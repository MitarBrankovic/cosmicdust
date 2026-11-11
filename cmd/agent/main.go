// Command cosmicdust-agent is the standalone agent: the node side of cosmicdust (package node)
// with its options from flags, an HTTP endpoint for /metrics and /healthz, and
// a memory limit, run until it is told to stop.
//
// Every flag can also be set through the environment, as COSMICDUST_ and the flag's
// name in upper case with underscores: -data-dir is COSMICDUST_DATA_DIR. A flag on
// the command line wins over the environment.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/metrics"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
	"github.com/c12s/cosmicdust/pkg/node"
)

// version is set at build time: -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cfg, err := parseConfig(os.Args[1:], os.LookupEnv, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cosmicdust-agent:", err)
		os.Exit(2)
	}
	if cfg.version {
		fmt.Println("cosmicdust-agent", version)
		return
	}
	log := newLogger(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, cfg); err != nil {
		log.Error("cosmicdust-agent failed", "err", err)
		os.Exit(1)
	}
}

func newLogger(cfg config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.logLevel}
	if cfg.logFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func run(ctx context.Context, log *slog.Logger, cfg config) error {
	applyMemoryLimit(log, cfg.memoryLimit)
	reg := metrics.NewRegistry(version)
	var n *node.Node
	endpoint := metrics.NewServer(metrics.Options{
		Addr:     cfg.listen,
		Gatherer: reg,
		Health:   func() error { return n.Health() },
		Pprof:    cfg.pprof,
		Logger:   log,
	})
	n, err := node.New(nodeOptions(cfg, log, reg, endpoint))
	if err != nil {
		return err
	}
	log.Info("cosmicdust-agent starting", "version", version, "node", n.ID(), "data_dir", cfg.dataDir,
		"gomemlimit", debug.SetMemoryLimit(-1))
	return n.Run(ctx)
}

// nodeOptions turns the configuration into the node's options. The HTTP
// endpoint runs as the node's last component, so that the last metrics can
// still be scraped while the rest stops.
func nodeOptions(cfg config, log *slog.Logger, reg prometheus.Registerer, endpoint cosmicdust.Component) node.Options {
	o := node.Options{
		DataDir:       cfg.dataDir,
		ID:            cfg.node,
		AppLabels:     cfg.appLabels,
		CgroupRoot:    cfg.cgroupRoot,
		SnapshotEvery: cfg.snapshotEvery,
		Tail: node.TailOptions{
			Poll: cfg.poll, PollMax: cfg.pollMax,
			ReadBuf: int(cfg.readBuf), MaxLine: int(cfg.maxLine), MaxMessage: int(cfg.maxMessage),
			SkipExited: cfg.skipExited,
		},
		WAL: node.WALOptions{SegmentBytes: int64(cfg.walSegmentBytes), SegmentAge: cfg.walSegmentAge},
		Store: node.StoreOptions{
			ConvertEvery: cfg.convertEvery,
			ConvertBytes: int64(cfg.convertBytes),
			SortBytes:    int(cfg.sortBytes),
			PageBytes:    int(cfg.pageBytes),
			CompactEvery: cfg.compactEvery,
			Reserve:      off(int64(cfg.diskReserve)),
			Quota:        off(int64(cfg.storeQuota)),
			WhenFull:     cfg.whenFull,
		},
		Components:      []cosmicdust.Component{endpoint},
		ShutdownTimeout: cfg.shutdownTimeout,
		Logger:          log,
		Registerer:      reg,
	}
	if cfg.docker {
		o.Docker = &node.DockerOptions{Host: cfg.dockerHost, Reconcile: off(cfg.dockerReconcile)}
	}
	if cfg.syncURL != "" {
		o.Sync = &node.SyncOptions{
			URL:        cfg.syncURL,
			Every:      cfg.syncEvery,
			ChunkBytes: int(cfg.syncChunk),
			Window:     cfg.syncWindow,
			AckTimeout: cfg.syncAckTimeout,
			RateBytes:  int64(cfg.syncRate),
			Urgent:     cfg.syncUrgent,
		}
	}
	if cfg.containerd {
		o.Containerd = &node.ContainerdOptions{
			Address:    cfg.containerdAddress,
			Namespaces: cfg.containerdNS,
			Reconcile:  off(cfg.containerdReconcile),
		}
	}
	return o
}

// off turns a flag's 0, which means none, into the node's negative: there a
// zero takes the default.
func off[T int64 | time.Duration](v T) T {
	if v == 0 {
		return -1
	}
	return v
}

// applyMemoryLimit sets the Go runtime's soft memory limit. Without one the
// garbage collector lets the heap double before it runs and resident memory
// follows. 64 MiB costs no CPU at 1,000 or 5,000 lines/s; 48 MiB saves ~11 MB
// and costs 12 % more CPU at 1,000 lines/s, 80 % more at 5,000.
// GOMEMLIMIT, if set, has already been applied by the runtime and wins.
func applyMemoryLimit(log *slog.Logger, limit size) {
	if v, ok := os.LookupEnv("GOMEMLIMIT"); ok {
		log.Debug("memory limit from GOMEMLIMIT", "value", v)
		return
	}
	if limit > 0 {
		debug.SetMemoryLimit(int64(limit))
	}
}
