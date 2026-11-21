package natsio

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pandops/flowai-journal/internal/rpc"
)

// Conn owns journal's broker connection, its bound durable consumer and
// its responders.
type Conn struct {
	URL     string
	Options []nats.Option
	Handler *rpc.Handler
	Timeout time.Duration
	Logger  *slog.Logger
	Super   *Supervisor

	mu       sync.Mutex
	nc       *nats.Conn
	consumer jetstream.Consumer
	subs     []*nats.Subscription
}

var errReconnecting = errors.New("nats: reconnecting")

// Dial is the Supervisor's Dialer: it opens the connection once and later
// waits for the client's own reconnects.
func (c *Conn) Dial(ctx context.Context) error {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()
	if nc != nil && !nc.IsClosed() {
		if nc.IsConnected() {
			return nil
		}
		return errReconnecting
	}
	opts := append([]nats.Option{}, c.Options...)
	opts = append(opts,
		nats.DisconnectErrHandler(func(*nats.Conn, error) { c.Super.SetConnected(false) }),
		nats.ReconnectHandler(func(*nats.Conn) { c.Super.SetConnected(true) }),
		nats.ClosedHandler(func(*nats.Conn) { c.Super.SetConnected(false) }),
	)
	nc, err := nats.Connect(c.URL, opts...)
	if err != nil {
		c.Logger.Warn("broker connection failed", "error", err)
		return err
	}
	for _, op := range rpc.Operations {
		op := op
		sub, err := nc.QueueSubscribe(rpc.Subject(op), QueueGroup, func(m *nats.Msg) { c.respond(op, m) })
		if err != nil {
			nc.Close()
			return err
		}
		c.subs = append(c.subs, sub)
	}
	c.mu.Lock()
	c.nc = nc
	c.mu.Unlock()
	c.Logger.Info("broker connected")
	return nil
}

func (c *Conn) respond(op string, m *nats.Msg) {
	if m.Reply == "" {
		return
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r := c.Handler.Serve(ctx, op, m.Header, m.Data)
	out := nats.NewMsg(m.Reply)
	out.Header.Set("status", strconv.Itoa(r.Status))
	for k, v := range r.Header {
		out.Header.Set(k, v)
	}
	out.Data = r.Body
	if err := m.RespondMsg(out); err != nil {
		c.Logger.Warn("reply failed", "operation", op, "error", err)
	}
}

// Connected reports the connection state.
func (c *Conn) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nc != nil && c.nc.IsConnected()
}

// BindConsumer looks up the durable consumer the deployment created; it
// never creates one.
func (c *Conn) BindConsumer(ctx context.Context) error {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()
	if nc == nil || !nc.IsConnected() {
		return errReconnecting
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	cons, err := js.Consumer(ctx, Stream, Durable)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.consumer = cons
	c.mu.Unlock()
	return nil
}

// ConsumerBound reports whether the durable consumer is bound.
func (c *Conn) ConsumerBound() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumer != nil
}

// Fetch implements Fetcher over the bound consumer.
func (c *Conn) Fetch(ctx context.Context, max int) ([]Message, error) {
	c.mu.Lock()
	cons := c.consumer
	c.mu.Unlock()
	if cons == nil {
		return nil, errReconnecting
	}
	batch, err := cons.Fetch(max, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		return nil, err
	}
	var out []Message
	for m := range batch.Messages() {
		out = append(out, jsMessage{m})
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// Close drains the connection.
func (c *Conn) Close() {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()
	if nc != nil {
		_ = nc.Drain()
	}
}

type jsMessage struct{ m jetstream.Msg }

func (j jsMessage) Subject() string             { return j.m.Subject() }
func (j jsMessage) Header() map[string][]string { return j.m.Headers() }
func (j jsMessage) Data() []byte                { return j.m.Data() }
func (j jsMessage) Ack() error                  { return j.m.Ack() }
func (j jsMessage) Nak(d time.Duration) error   { return j.m.NakWithDelay(d) }
func (j jsMessage) StreamSeq() uint64 {
	md, err := j.m.Metadata()
	if err != nil {
		return 0
	}
	return md.Sequence.Stream
}
