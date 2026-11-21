// Command journal runs the journal service: it consumes ALL_EVENT through
// the durable consumer "journal", stores every event once in its own
// PostgreSQL schema, answers API Gateway's journal.ui.* requests and serves
// the Gateway's team WebSocket stream.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandops/flowai-journal/internal/config"
	"github.com/pandops/flowai-journal/internal/cursor"
	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/httpedge"
	"github.com/pandops/flowai-journal/internal/natsio"
	"github.com/pandops/flowai-journal/internal/pgstore"
	"github.com/pandops/flowai-journal/internal/rpc"
)

func main() {
	path := flag.String("config", "/etc/flowai/config.yaml", "configuration file")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "journal")
	if err := run(*path, logger); err != nil {
		logger.Error("journal stopped", "error", err)
		os.Exit(1)
	}
}

func run(path string, logger *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	natsOpts, err := natsio.DialOptions(cfg.NATS)
	if err != nil {
		return err
	}
	var keys []cursor.Key
	for _, k := range cfg.CursorKeys {
		secret, err := hex.DecodeString(k.KeyHex)
		if err != nil {
			return errors.New("config: cursor_keys key_hex is not hex")
		}
		keys = append(keys, cursor.Key{ID: k.ID, Secret: secret})
	}
	keyring, err := cursor.NewKeyring(keys)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", cfg.PostgresDSN())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	store, err := pgstore.New(db, cfg.DB.Schema)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hub := httpedge.NewHub()
	eng := engine.New(store, engine.Options{OnCommit: hub.Notify, Logger: logger})
	handler := &rpc.Handler{Engine: eng, Cursor: keyring, Logger: logger}
	sup := &natsio.Supervisor{Backoff: 2 * time.Second}
	conn := &natsio.Conn{URL: cfg.NATS.URL, Options: natsOpts, Handler: handler, Timeout: cfg.NATS.RequestTimeout, Logger: logger, Super: sup}
	sup.Dial = conn.Dial
	var migrated atomic.Bool
	binder := &natsio.ConsumerBinder{Super: sup, Lookup: conn.BindConsumer, Backoff: time.Second, OnError: func(err error) {
		logger.Warn("bind durable consumer", "stream", natsio.Stream, "durable", natsio.Durable, "error", err)
	}}
	ready := &natsio.Readiness{Super: sup, Binder: binder, Store: func(ctx context.Context) bool {
		return migrated.Load() && eng.Ready(ctx)
	}}

	srv := &http.Server{
		Addr:              cfg.HTTP.Listen,
		Handler:           httpedge.New(httpedge.Options{Engine: eng, GatewayToken: cfg.GatewayToken, Readiness: ready, Wake: hub.Subscribe, Logger: logger}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	go func() {
		for ctx.Err() == nil {
			if err := pgstore.Migrate(ctx, db, cfg.DB.Schema); err != nil {
				logger.Warn("migrate schema", "error", err)
				if !wait(ctx, 2*time.Second) {
					return
				}
				continue
			}
			migrated.Store(true)
			logger.Info("schema migrated", "schema", cfg.DB.Schema)
			return
		}
	}()
	go sup.Run(ctx)
	go binder.Run(ctx)
	go func() {
		for wait(ctx, time.Second) {
			if migrated.Load() && binder.Bound() {
				natsio.ConsumeLoop(ctx, conn, eng, time.Second)
			}
		}
	}()
	go func() {
		for wait(ctx, 30*time.Second) {
			if migrated.Load() {
				if err := eng.ExpireHeld(ctx); err != nil {
					logger.Warn("expire held items", "error", err)
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	conn.Close()
	return nil
}

func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
