// Package node runs the node side of cosmicdust inside another program: it follows
// the containers of Docker and containerd, reads their logs, writes lines and
// lifecycle events to a write-ahead log, turns that into hourly Parquet files
// on the local disk, and sends them to a control plane, which keeps them once.
//
// A program that already runs on every node, such as an edge agent, builds a
// Node from Options and runs it beside its own work:
//
//	n, err := node.New(node.Options{
//		DataDir:    "/var/lib/cosmicdust",
//		Docker:     &node.DockerOptions{},
//		Containerd: &node.ContainerdOptions{},
//		Sync:       &node.SyncOptions{URL: "nats://control-plane:4222"},
//	})
//	if err != nil {
//		return err
//	}
//	return n.Run(ctx) // until ctx ends; then every part stops in order
//
// The parts are the implementations in this module's internal packages, wired
// together here; package cosmicdust has the types and interfaces they share. The
// standalone daemon, cmd/agent, is this package plus flags, an HTTP endpoint
// for /metrics and /healthz, and a memory limit.
//
// # Memory
//
// The Go runtime's soft memory limit is the process's, so the embedding
// program sets it (debug.SetMemoryLimit or GOMEMLIMIT). Without one the heap
// may double before each collection; cosmicdust-agent sets 64 MiB. Most of what the node
// keeps live is its Parquet writers, a buffer of StoreOptions.PageBytes per
// column, and the rows waiting to be sorted, StoreOptions.SortBytes of lines.
package node

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/lifecycle"
	"github.com/c12s/cosmicdust/internal/nodeid"
	"github.com/c12s/cosmicdust/internal/runtime/containerd"
	"github.com/c12s/cosmicdust/internal/runtime/docker"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/internal/syncer"
	"github.com/c12s/cosmicdust/internal/tail"
	"github.com/c12s/cosmicdust/internal/transport"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure a Node. A zero field takes the default given with it,
// which is also cosmicdust-agent's.
type Options struct {
	// DataDir holds everything the node keeps: its identity, the container
	// state, the WAL and the store. Required.
	DataDir string

	// ID is the node's identity, stored with every row. Empty: the one kept
	// in DataDir/node-id, made there on first start.
	ID string

	// Docker and Containerd are the runtimes followed; nil leaves one out.
	// With both, Docker names its containers and knows their log files, and
	// containerd adds the ones Docker does not run (nerdctl, ctr). At least
	// one of them, or a Source, is needed.
	Docker     *DockerOptions
	Containerd *ContainerdOptions

	// Sources are further runtimes, such as one cosmicdust has no adapter for.
	Sources []Source

	// AppLabels are the container label keys that name the application, in
	// order of preference. Default: the Compose service, then the image.
	AppLabels []string

	// CgroupRoot is the cgroup v2 root, normally /sys/fs/cgroup, where the
	// kernel counts OOM kills per container. Empty: an OOM kill is known only
	// when the runtime reports it.
	CgroupRoot string

	// SnapshotEvery is how often the container state is saved to DataDir
	// when it has changed. Default 10 s.
	SnapshotEvery time.Duration

	Tail  TailOptions
	WAL   WALOptions
	Store StoreOptions

	// Sync sends the store's files to a control plane, and urgent events at
	// once. nil keeps everything on the node, within the store's quota.
	Sync *SyncOptions

	// Components run beside the node's own parts and stop after them, in the
	// order given: an HTTP endpoint, for one, so that the last metrics can
	// still be scraped.
	Components []cosmicdust.Component

	// ShutdownTimeout bounds the whole stop. Default 30 s.
	ShutdownTimeout time.Duration

	Logger     *slog.Logger          // default slog.Default()
	Registerer prometheus.Registerer // receives the cosmicdust_agent_* self-metrics; nil: none
}

// SyncOptions configure sending to the control plane.
type SyncOptions struct {
	// URL is the control plane: nats://host:4222 for NATS, the default
	// transport, or grpc://host:port. Ignored when Transport is set.
	URL string

	// Transport, if set, carries the files and urgent events instead of one
	// made from URL: one into an agent's own connection, for instance. The
	// node closes it.
	Transport cosmicdust.Transport

	// Every is the time between rounds; default 10 s. A row reaches the
	// control plane about half of it after its file is written.
	Every time.Duration

	// ChunkBytes (default 256 KiB), Window (NATS chunks in flight, default
	// 32), AckTimeout (NATS, the longest wait for the next ack, default 30 s)
	// and RateBytes (file bytes per second, default 0: no cap) shape the
	// transport made from URL.
	ChunkBytes int
	Window     int
	AckTimeout time.Duration
	RateBytes  int64

	// Urgent is how urgent events travel: "separate" (default) on a
	// connection of their own, "shared" on the files' one, "off" only with
	// the events table.
	Urgent string
}

// DockerOptions configure following dockerd.
type DockerOptions struct {
	// Host is dockerd's address. Empty: DOCKER_HOST or the default socket.
	Host string

	// Reconcile is how often to compare the tracked state with a listing,
	// besides after every connect. Default 0: only after a connect, because
	// a Docker listing costs dockerd and containerd ~10 ms of CPU per
	// container.
	Reconcile time.Duration
}

// ContainerdOptions configure following containerd. It needs access to
// containerd's socket, normally root.
type ContainerdOptions struct {
	// Address is the socket. Default /run/containerd/containerd.sock.
	Address string

	// Namespaces are the ones followed. Default "moby" (Docker's) and
	// "default" (nerdctl, ctr).
	Namespaces []string

	// Reconcile is how often to compare with a listing besides after every
	// connect; a containerd listing is cheap. Default 5 min; negative: only
	// after a connect.
	Reconcile time.Duration
}

// Source is a runtime given as a cosmicdust.EventSource.
type Source struct {
	cosmicdust.EventSource

	// Reconcile is how often to compare with a listing besides after every
	// connect; zero: only after a connect.
	Reconcile time.Duration
}

// TailOptions configure reading the logs.
type TailOptions struct {
	// Poll is how often a file with recent data is checked; PollMax how
	// rarely one that has been quiet that long is. Defaults 250 ms and 5 s.
	Poll, PollMax time.Duration

	ReadBuf    int // read buffer per followed file; default 16 KiB
	MaxLine    int // longest physical line kept, longer ones are skipped; default 1 MiB
	MaxMessage int // longest message kept, longer ones are cut; default 256 KiB

	// SkipExited leaves unread what containers that are not running when
	// first seen left in their logs. By default it is read once.
	SkipExited bool
}

// WALOptions configure the write-ahead log.
type WALOptions struct {
	// A segment is sealed, and becomes Parquet, when it reaches SegmentBytes
	// or has been open SegmentAge. Defaults 8 MiB and 5 s.
	SegmentBytes int64
	SegmentAge   time.Duration
}

// StoreOptions configure the Parquet store.
type StoreOptions struct {
	// ConvertEvery is how often sealed segments become Parquet. Default 10 s.
	ConvertEvery time.Duration

	// ConvertBytes is the log text one conversion takes; a backlog is
	// converted in pieces this size. Default 32 MiB.
	ConvertBytes int64

	// SortBytes is the log text sorted at a time while converting: rows
	// stream into the files, and this much waits in memory per hour being
	// written. Default 4 MiB.
	SortBytes int

	// PageBytes is the Parquet page size. A file being written keeps a
	// buffer this size for every column. Default 1 MiB.
	PageBytes int

	// CompactEvery is how often finished hours are looked at to merge their
	// files. Default 30 s.
	CompactEvery time.Duration

	// Reserve is the space kept free on the store's file system, on top of
	// room to convert the WAL that waits; default 64 MiB, negative: no check.
	// Quota is the most the store, WAL included, may hold; default 2 GiB,
	// negative: no quota.
	Reserve int64
	Quota   int64

	// WhenFull says what happens without room once synced files are gone:
	// "evict" (default) deletes the oldest unsynced log files, each recorded
	// as a logs_dropped event; "block" stops reading until there is room.
	WhenFull string
}

// Node is the node side of cosmicdust, assembled.
type Node struct {
	id    string
	agent *cosmicdust.Agent
	store *store.Store

	closeOnce sync.Once
	closers   []func() error
	closeErr  error
}

// New assembles a node: it opens the WAL and the store, repairing what a crash
// left, and prepares the runtime clients, which connect once Run starts.
func New(opts Options) (*Node, error) {
	if opts.DataDir == "" {
		return nil, errors.New("node: Options.DataDir is empty")
	}
	if opts.Docker == nil && opts.Containerd == nil && len(opts.Sources) == 0 {
		return nil, errors.New("node: no runtime to follow: set Docker, Containerd or Sources")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return nil, err
	}
	n := &Node{id: opts.ID}
	if n.id == "" {
		id, err := nodeid.Load(filepath.Join(opts.DataDir, "node-id"))
		if err != nil {
			return nil, fmt.Errorf("node: id: %w", err)
		}
		n.id = id
	}
	fail := func(err error) (*Node, error) {
		_ = n.Close()
		return nil, err
	}

	sources, err := n.sources(opts, log)
	if err != nil {
		return fail(err)
	}
	events := make(chan cosmicdust.Event, 1024)
	var w *wal.WAL
	tracker, err := lifecycle.New(lifecycle.Options{
		Sources:       sources,
		Out:           events,
		SnapshotPath:  filepath.Join(opts.DataDir, "lifecycle.json"),
		SnapshotEvery: opts.SnapshotEvery,
		CgroupRoot:    opts.CgroupRoot,
		Replay:        func(since time.Time, fn func(cosmicdust.Event)) error { return replayWAL(w, since, fn) },
		Logger:        log,
		Registerer:    opts.Registerer,
	})
	if err != nil {
		return fail(err)
	}

	// Lines and events land in the WAL with the positions reading resumes
	// from, in one fsync per block. A segment goes once the tracker's state
	// on disk has its events.
	w, err = wal.Open(wal.Options{
		Dir:          filepath.Join(opts.DataDir, "wal"),
		SegmentBytes: opts.WAL.SegmentBytes,
		SegmentAge:   opts.WAL.SegmentAge,
		Meta: func(id string) wal.Container {
			c, _ := tracker.Get(id)
			return wal.Container{Name: c.Name, Image: c.Image, App: c.App}
		},
		Keep:       tracker.CleanAt,
		Logger:     log,
		Registerer: opts.Registerer,
	})
	if err != nil {
		return fail(err)
	}
	n.closers = append(n.closers, w.Close) // Run closes it too; again is harmless

	// The way to the control plane, and urgent events taken on their way to
	// the WAL.
	tr, urgent, err := n.syncParts(opts, log)
	if err != nil {
		return fail(err)
	}
	var also []func(cosmicdust.Event)
	if urgent != nil {
		also = append(also, urgent.Push)
	}
	eventWriter := wal.NewEventWriter(w, events, log, also...)

	// Sealed segments become hourly Parquet files, each line with the state
	// its container had when the line was written.
	so := opts.Store
	st, err := store.Open(store.Options{
		Root: filepath.Join(opts.DataDir, "store"),
		Node: n.id,
		WAL:  w,
		StateAt: func(id string, ts time.Time) (cosmicdust.State, int) {
			s := tracker.At(id, ts)
			return s.State, s.Epoch
		},
		ConvertEvery: so.ConvertEvery,
		ConvertBytes: so.ConvertBytes,
		SortBytes:    so.SortBytes,
		PageBytes:    so.PageBytes,
		Compact:      store.CompactOptions{Every: so.CompactEvery},
		Reserve:      orDefault(so.Reserve, 64<<20),
		Quota:        orDefault(so.Quota, 2<<30),
		WhenFull:     so.WhenFull,
		Logger:       log,
		Registerer:   opts.Registerer,
	})
	if err != nil {
		return fail(err)
	}
	n.store = st

	to := opts.Tail
	tailer, err := tail.New(tail.Options{
		Containers: tracker,
		Sink:       w,
		Resume:     w.Position,
		Source: tail.SourceOptions{
			ReadBuf: to.ReadBuf, MaxLine: to.MaxLine, MaxMessage: to.MaxMessage, Logger: log,
		},
		Poll:       to.Poll,
		PollMax:    to.PollMax,
		SkipExited: to.SkipExited,
		Logger:     log,
		Registerer: opts.Registerer,
	})
	if err != nil {
		return fail(err)
	}

	// Data-flow order: the readers stop first, then what drains them, the
	// WAL after everything that writes into it, sending after the store, the
	// caller's parts last.
	components := []cosmicdust.Component{tailer, tracker, eventWriter, st, w}
	if tr != nil {
		engine, err := syncer.New(syncer.Options{Store: st, Transport: tr, Every: opts.Sync.Every,
			Logger: log, Registerer: opts.Registerer})
		if err != nil {
			return fail(err)
		}
		components = append(components, engine)
		if urgent != nil {
			components = append(components, urgent)
		}
	}
	components = append(components, opts.Components...)
	n.agent, err = cosmicdust.New(cosmicdust.Options{
		Node:            n.id,
		Components:      components,
		ShutdownTimeout: opts.ShutdownTimeout,
		Logger:          log,
		Registerer:      opts.Registerer,
	})
	if err != nil {
		return fail(err)
	}
	return n, nil
}

// sources makes the event sources the options ask for; their clients are
// closed with the node.
func (n *Node) sources(opts Options, log *slog.Logger) ([]lifecycle.Source, error) {
	var sources []lifecycle.Source
	if d := opts.Docker; d != nil {
		src, err := docker.New(docker.Options{Host: d.Host, AppLabels: opts.AppLabels, Logger: log})
		if err != nil {
			return nil, err
		}
		n.closers = append(n.closers, src.Close)
		sources = append(sources, lifecycle.Source{EventSource: src, Reconcile: max(d.Reconcile, 0)})
	}
	if c := opts.Containerd; c != nil {
		src, err := containerd.New(containerd.Options{
			Address:    c.Address,
			Namespaces: c.Namespaces,
			NoDocker:   opts.Docker == nil,
			AppLabels:  opts.AppLabels,
			Logger:     log,
		})
		if err != nil {
			return nil, err
		}
		n.closers = append(n.closers, src.Close)
		sources = append(sources, lifecycle.Source{EventSource: src, Reconcile: orDefault(c.Reconcile, 5*time.Minute)})
	}
	for i, s := range opts.Sources {
		if s.EventSource == nil {
			return nil, fmt.Errorf("node: Sources[%d] is nil", i)
		}
		sources = append(sources, lifecycle.Source{EventSource: s.EventSource, Reconcile: max(s.Reconcile, 0)})
	}
	return sources, nil
}

// syncParts makes the transport and the urgent channel the options ask for;
// nil and nil without sync. The transport is closed with the node.
func (n *Node) syncParts(opts Options, log *slog.Logger) (cosmicdust.Transport, *syncer.Urgent, error) {
	so := opts.Sync
	if so == nil {
		return nil, nil, nil
	}
	switch so.Urgent {
	case "", "separate", "shared", "off":
	default:
		return nil, nil, fmt.Errorf("node: Sync.Urgent %q is none of separate, shared, off", so.Urgent)
	}
	tr := so.Transport
	if tr == nil {
		if so.URL == "" {
			return nil, nil, errors.New("node: Sync needs a URL or a Transport")
		}
		var err error
		tr, err = transport.New(so.URL, transport.Options{
			Node: n.id, ChunkBytes: so.ChunkBytes, Window: so.Window, AckTimeout: so.AckTimeout,
			RateBytes: so.RateBytes, SharedUrgent: so.Urgent == "shared", Logger: log, Registerer: opts.Registerer,
		})
		if err != nil {
			return nil, nil, err
		}
	}
	n.closers = append(n.closers, tr.Close)
	if so.Urgent == "off" {
		return tr, nil, nil
	}
	uo := syncer.UrgentOptions{Transport: tr, Logger: log, Registerer: opts.Registerer}
	if opts.Docker != nil {
		uo.DockerNamespace = "moby"
	}
	urgent, err := syncer.NewUrgent(uo)
	if err != nil {
		return nil, nil, err
	}
	return tr, urgent, nil
}

// replayWAL hands the tracker the events of the blocks written to the WAL
// since a time, oldest first. A segment gone meanwhile had its events in the
// tracker's state on disk already; what precedes damage in a segment is
// delivered.
func replayWAL(w *wal.WAL, since time.Time, fn func(cosmicdust.Event)) error {
	for _, seg := range w.Sealed() {
		err := w.Read(seg, func(b *wal.Block) error {
			if b.At.Before(since) {
				return nil
			}
			for _, ev := range b.Events {
				fn(ev)
			}
			return nil
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) && !wal.IsCorrupt(err) {
			return err
		}
	}
	return nil
}

// orDefault reads a zero option as its default and a negative one as off.
func orDefault[T int64 | time.Duration](v, def T) T {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	}
	return v
}

// ID is the node's identity, as stored with every row.
func (n *Node) ID() string { return n.id }

// Store is the node's buffer of finished files, as a sync engine sees it.
func (n *Node) Store() cosmicdust.Store { return n.store }

// Health returns nil while every part runs, and otherwise says which do not.
func (n *Node) Health() error { return n.agent.Health() }

// Run starts every part and blocks until ctx ends or a part fails; either way
// it then stops them in order, releases what New opened and returns. A clean
// stop returns nil. A node runs once.
func (n *Node) Run(ctx context.Context) error {
	err := n.agent.Run(ctx)
	return errors.Join(err, n.Close())
}

// Close releases what New opened. Run does it on its way out; Close is for a
// node that is never run.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		var errs []error
		for i := len(n.closers) - 1; i >= 0; i-- {
			errs = append(errs, n.closers[i]())
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}
