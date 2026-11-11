// Package startup wires the service together and stops it in order.
//
// Data directory:
//
//	inbox/       received files waiting for the archive
//	archive/     <table>/app=<app>/dt=<day>/hour=<hour>/<node>-<name>
//	quarantine/  received files that were not Parquet files of their table
//	state/       ledger.jsonl, urgent.jsonl
package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/server/indexer"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/metrics"
	"github.com/c12s/cosmicdust/internal/server/receiver"
	"github.com/c12s/cosmicdust/internal/server/retention"
	"github.com/c12s/cosmicdust/internal/server/servers"
	"github.com/c12s/cosmicdust/internal/server/urgent"
)

// App is the running service.
type App struct {
	config *configs.Config
	log    *slog.Logger

	ledger   *ledger.Ledger
	spool    *urgent.Spool
	receiver *receiver.Receiver
	archiver *archive.Archiver
	indexer  *indexer.Indexer
	cleaner  *retention.Cleaner

	ready  atomic.Bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	grpcAddr, httpAddr net.Addr

	// stopped in reverse order: first the servers, last the state
	shutdownProcesses []func(ctx context.Context)
}

// GRPCAddr is where the gRPC server listens, once started; nil without one.
func (a *App) GRPCAddr() net.Addr { return a.grpcAddr }

// HTTPAddr is where /metrics and /healthz are served, once started.
func (a *App) HTTPAddr() net.Addr { return a.httpAddr }

// NewAppWithConfig returns the service, not started.
func NewAppWithConfig(config *configs.Config) (*App, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(config.LogLevel)); err != nil {
		return nil, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return &App{config: config, log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))}, nil
}

// Logger is the service's logger.
func (a *App) Logger() *slog.Logger { return a.log }

// Start opens the state, starts the background work and then the servers. On
// an error what was started is stopped again.
func (a *App) Start() (err error) {
	defer func() {
		if err != nil {
			a.GracefulStop(context.Background())
		}
	}()
	c := a.config
	dir := func(name string) string { return filepath.Join(c.DataDir, name) }

	if a.ledger, err = ledger.Open(dir("state")); err != nil {
		return err
	}
	a.onStop(func(context.Context) { _ = a.ledger.Close() })

	var client *es.Client
	if c.EsAddress != "" {
		if client, err = es.New(c.EsAddress, es.Options{Username: c.EsUsername, Password: c.EsPassword,
			APIKey: c.EsAPIKey}); err != nil {
			return err
		}
	}
	wakeIndex := func() {}
	if a.spool, err = urgent.Open(dir("state"), func() { wakeIndex() }); err != nil {
		return err
	}
	a.onStop(func(context.Context) { _ = a.spool.Close() })
	if client != nil {
		a.indexer = indexer.New(client, a.ledger, a.spool, dir("archive"), indexer.Options{Prefix: c.EsIndexPrefix,
			Retention: c.Retention, Replicas: c.EsReplicas, Log: a.log.With("part", "indexer")})
		wakeIndex = a.indexer.Wake
	} else {
		// Archive only: nothing waits for an index, and urgent events stay in
		// the log of the spool.
		a.log.Warn("no ES_ADDRESS: archive only, nothing is indexed")
		wakeIndex = func() { _ = a.ledger.Indexed(a.ledger.PendingIndex()...) }
	}
	a.archiver = archive.New(dir("inbox"), dir("archive"), dir("quarantine"), a.ledger, wakeIndex,
		a.log.With("part", "archive"))
	a.archiver.Compaction(c.CompactAfter, c.CompactEvery)
	if a.receiver, err = receiver.Open(dir("inbox"), a.ledger, a.archiver.Wake); err != nil {
		return err
	}
	a.onStop(func(context.Context) { _ = a.receiver.Close() })
	a.cleaner = retention.New(dir("archive"), c.DedupWindow, c.Retention, a.ledger, a.log.With("part", "retention"))

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if err := metrics.Register(reg, metrics.Parts{Receiver: a.receiver, Spool: a.spool, Archiver: a.archiver,
		Indexer: a.indexer, Cleaner: a.cleaner, Ledger: a.ledger}); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.onStop(func(stop context.Context) {
		cancel()
		done := make(chan struct{})
		go func() { a.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-stop.Done():
			a.log.Warn("background work did not stop in time")
		}
	})
	a.wg.Go(func() { a.archiver.Run(ctx) })
	if a.indexer != nil {
		a.wg.Go(func() { a.indexer.Run(ctx) })
	}
	a.wg.Go(func() { a.cleaner.Run(ctx, c.CleanupEvery) })
	a.wg.Go(func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				a.receiver.Expire(c.PartialIdle)
			}
		}
	})
	wakeIndex()

	if err := a.startServers(reg); err != nil {
		return err
	}
	a.ready.Store(true)
	a.log.Info("started", "data", c.DataDir, "nats", c.NatsAddress, "grpc", c.GrpcAddress, "http", c.HttpAddress,
		"elasticsearch", c.EsAddress, "waiting_for_archive", len(a.ledger.PendingArchive()),
		"waiting_for_index", len(a.ledger.PendingIndex()))
	return nil
}

func (a *App) startServers(reg *prometheus.Registry) error {
	c := a.config
	if c.HttpAddress != "" {
		lis, err := net.Listen("tcp", c.HttpAddress)
		if err != nil {
			return err
		}
		a.httpAddr = lis.Addr()
		srv := servers.NewHTTPServer(c.HttpAddress, reg, a.ready.Load)
		go func() { _ = srv.Serve(lis) }()
		a.onStop(func(ctx context.Context) { _ = srv.Shutdown(ctx) })
	}
	if c.GrpcAddress != "" {
		lis, err := net.Listen("tcp", c.GrpcAddress)
		if err != nil {
			return err
		}
		a.grpcAddr = lis.Addr()
		srv := servers.NewGRPCServer(a.receiver, a.spool)
		go func() {
			if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				a.log.Error("gRPC server stopped", "err", err)
			}
		}()
		a.onStop(func(ctx context.Context) { stopGRPC(ctx, srv) })
	}
	if c.NatsAddress != "" {
		url := c.NatsAddress
		if !strings.Contains(url, "://") {
			url = "nats://" + url
		}
		nc, err := nats.Connect(url, nats.Name("cosmicdust-server"), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second))
		if err != nil {
			return fmt.Errorf("nats: %w", err)
		}
		a.onStop(func(context.Context) { nc.Close() })
		stop, err := servers.ServeNATS(nc, c.SubjectPrefix, a.receiver, a.spool)
		if err != nil {
			return fmt.Errorf("nats: %w", err)
		}
		a.onStop(func(context.Context) { _ = stop() })
	}
	return nil
}

func stopGRPC(ctx context.Context, srv *grpc.Server) {
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		srv.Stop()
	}
}

func (a *App) onStop(f func(ctx context.Context)) {
	a.shutdownProcesses = append(a.shutdownProcesses, f)
}

// GracefulStop stops the servers first, so that nothing new arrives, then
// the background work, then closes the state. Uploads in progress are
// dropped; their senders continue them after the restart.
func (a *App) GracefulStop(ctx context.Context) {
	a.ready.Store(false)
	for i := len(a.shutdownProcesses) - 1; i >= 0; i-- {
		a.shutdownProcesses[i](ctx)
	}
	a.shutdownProcesses = nil
	a.log.Info("stopped")
}
