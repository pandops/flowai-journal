// Package natsio connects journal to NATS: a supervised TLS connection
// with user-JWT credentials, the bound durable consumer "journal" on
// ALL_EVENT, and queue-group responders on journal.ui.<operation>.sub.
// It contains no routing logic: the engine decides every disposition.
package natsio

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Durable is journal's durable consumer, created by the deployment.
const (
	Durable = "journal"
	Stream  = "ALL_EVENT"
	// QueueGroup is the responder queue group.
	QueueGroup = "journal"
	// InboxPrefix is journal's reply inbox prefix.
	InboxPrefix = "_INBOX.journal"
)

// Message is one delivered JetStream message.
type Message interface {
	Subject() string
	Header() map[string][]string
	Data() []byte
	StreamSeq() uint64
	Ack() error
	Nak(delay time.Duration) error
}

// Fetcher pulls the next messages of the durable consumer.
type Fetcher interface {
	Fetch(ctx context.Context, max int) ([]Message, error)
}

// Settle acknowledges or negatively acknowledges msg.
func Settle(msg Message, d engine.Disposition, nakDelay time.Duration) error {
	if d == engine.Ack {
		return msg.Ack()
	}
	return msg.Nak(nakDelay)
}

const batch = 64

// ConsumeLoop fetches and handles messages until ctx ends. A message is
// acknowledged only after the engine committed it; fetch errors (broker
// down) are retried after backoff.
func ConsumeLoop(ctx context.Context, f Fetcher, e *engine.Engine, backoff time.Duration) {
	if backoff <= 0 {
		backoff = time.Second
	}
	for ctx.Err() == nil {
		msgs, err := f.Fetch(ctx, batch)
		if err != nil {
			if !sleep(ctx, backoff) {
				return
			}
			continue
		}
		for _, m := range msgs {
			if ctx.Err() != nil {
				// Unsettled messages are redelivered after the ack wait.
				return
			}
			d := e.Handle(ctx, engine.Inbound{Subject: m.Subject(), Header: m.Header(), Data: m.Data(), StreamSeq: m.StreamSeq()})
			_ = Settle(m, d, backoff)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Dialer opens (or confirms) the broker connection.
type Dialer func(ctx context.Context) error

// Supervisor keeps the broker connection up: it dials until a dial
// succeeds and dials again whenever the connection is reported down.
type Supervisor struct {
	Dial    Dialer
	Backoff time.Duration

	mu        sync.Mutex
	connected bool
	attempts  int
	changed   chan struct{}
}

func (s *Supervisor) signal() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{}, 1)
	}
	return s.changed
}

// Run supervises until ctx ends.
func (s *Supervisor) Run(ctx context.Context) {
	backoff := s.Backoff
	if backoff <= 0 {
		backoff = time.Second
	}
	changed := s.signal()
	for ctx.Err() == nil {
		if s.Connected() {
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
			continue
		}
		s.mu.Lock()
		s.attempts++
		s.mu.Unlock()
		if err := s.Dial(ctx); err == nil {
			s.SetConnected(true)
			continue
		}
		if !sleep(ctx, backoff) {
			return
		}
	}
}

// Connected reports whether the connection is up.
func (s *Supervisor) Connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

// SetConnected records a connection change reported by the client.
func (s *Supervisor) SetConnected(up bool) {
	changed := s.signal()
	s.mu.Lock()
	s.connected = up
	s.mu.Unlock()
	select {
	case changed <- struct{}{}:
	default:
	}
}

// Attempts counts dial attempts.
func (s *Supervisor) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// ConsumerBinder binds the deployment-created durable consumer once the
// broker connection is up, retrying while it is missing. A dropped
// connection unbinds it; it is looked up again after reconnect.
type ConsumerBinder struct {
	Super   *Supervisor
	Lookup  func(ctx context.Context) error
	Backoff time.Duration
	// OnError reports a failed lookup (logging only).
	OnError func(error)

	bound atomic.Bool
}

// Run binds until ctx ends.
func (b *ConsumerBinder) Run(ctx context.Context) {
	backoff := b.Backoff
	if backoff <= 0 {
		backoff = time.Second
	}
	for ctx.Err() == nil {
		switch {
		case !b.Super.Connected():
			b.bound.Store(false)
		case !b.bound.Load():
			if err := b.Lookup(ctx); err != nil {
				if b.OnError != nil {
					b.OnError(err)
				}
			} else {
				b.bound.Store(true)
			}
		}
		if !sleep(ctx, backoff) {
			return
		}
	}
}

// Bound reports whether the durable consumer is bound over a live connection.
func (b *ConsumerBinder) Bound() bool { return b.bound.Load() && b.Super.Connected() }

// Readiness is journal's production readiness: the supervised broker
// connection, the bound durable consumer and the store.
type Readiness struct {
	Super  *Supervisor
	Binder *ConsumerBinder
	Store  func(ctx context.Context) bool
}

// BrokerConnected reports the supervised connection.
func (r *Readiness) BrokerConnected() bool { return r.Super.Connected() }

// ConsumerBound reports the durable consumer binding.
func (r *Readiness) ConsumerBound() bool { return r.Binder.Bound() }

// StoreReady reports the store.
func (r *Readiness) StoreReady(ctx context.Context) bool { return r.Store != nil && r.Store(ctx) }
