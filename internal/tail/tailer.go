// Package tail reads container logs from the files the runtime writes and
// hands them on in batches, following the containers the lifecycle tracker
// knows.
package tail

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/lifecycle"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Containers is what the tailer needs from the lifecycle tracker.
type Containers interface {
	Get(id string) (lifecycle.Container, bool)
	Subscribe() *lifecycle.Changes
}

// Sink takes what the tailer reads. Add copies the records, whose lines are
// valid only during the call; Flush makes everything added durable. The tailer
// calls Flush once per pass over the files that were due, so one pass is one
// durable write.
type Sink interface {
	Add(recs []cosmicdust.Record) error
	Flush() error
}

// Discard is a Sink that keeps nothing.
type Discard struct{}

func (Discard) Add([]cosmicdust.Record) error { return nil }
func (Discard) Flush() error                  { return nil }

// Options configure a Tailer.
type Options struct {
	Containers Containers
	Sink       Sink // default Discard
	Source     SourceOptions

	// Resume says where to start a container the tailer has not read since
	// it started (the store knows); without it, at the oldest file on disk.
	Resume func(id string) (cosmicdust.Position, bool)

	// Poll is how often a file with recent data is checked; PollMax how
	// rarely one that has been quiet for PollMax is (polling every 250
	// ms cost ~0.06 % of a core per quiet file, 12.6 % at 200 containers).
	// Defaults 250 ms and 5 s.
	Poll, PollMax time.Duration

	// Batch is the records read at a time; PerPass bounds the records one
	// pass takes, so that a long backlog does not become one huge write: a
	// pass is a WAL block, and the WAL's buffers grow to the biggest one.
	// Defaults 1024 and 4096; a file whose budget ran out is read again in
	// the next pass, at once.
	Batch, PerPass int

	// SkipExited leaves the logs of containers that are not running when
	// first seen unread. By default what they left is read once.
	SkipExited bool

	Logger     *slog.Logger
	Registerer prometheus.Registerer

	rateWindow time.Duration // tests: how often retention is estimated; default 30 s
}

// Tailer follows the logs of the node's containers. It is a cosmicdust.Component.
//
// One goroutine serves every file: a queue of deadlines, an adaptive interval
// per file, and non-blocking reads. A goroutine and a timer per file kept the
// prototype awake 89 times a second with no container at all.
type Tailer struct {
	opts    Options
	src     *Source
	stats   *Stats
	log     *slog.Logger
	changes *lifecycle.Changes

	files       map[string]*followed           // by container ID
	queue       queue                          // files by due time
	positions   map[string]cosmicdust.Position // where to resume containers not followed now
	unsupported map[string]bool
	buf         []cosmicdust.Record
	read        []*followed // read in the current pass
	done        []*followed // drained in the current pass
	added       bool        // records added since the last Flush
	blocked     bool        // the sink has no room for now; reading waits for a Flush to succeed

	rateWindow time.Duration // how often the write rates, and with them retention, are estimated
	rateAt     time.Time     // when they last were
}

type followed struct {
	id        string
	ref       cosmicdust.ContainerRef
	r         *reader
	every     time.Duration
	lastData  time.Time
	due       time.Time
	index     int  // in the queue; -1 when not queued
	draining  bool // not running: read to the end, then close
	forget    bool // removed: drop its position too
	failing   bool // the last read failed
	pending   cosmicdust.Position
	committed cosmicdust.Position

	window  time.Time // start of the current rate window
	written int64     // bytes read in it
	rate    float64   // bytes per second over the last full window
	short   bool      // warned that the runtime keeps little of this log
}

// New returns a tailer. It subscribes to the tracker at once, so that nothing
// the tracker learns before Run starts is missed.
func New(opts Options) (*Tailer, error) {
	if opts.Containers == nil {
		return nil, errors.New("tail: Options.Containers is nil")
	}
	if opts.Sink == nil {
		opts.Sink = Discard{}
	}
	if opts.Poll <= 0 {
		opts.Poll = 250 * time.Millisecond
	}
	if opts.PollMax <= 0 {
		opts.PollMax = 5 * time.Second
	}
	if opts.Batch <= 0 {
		opts.Batch = 1024
	}
	if opts.PerPass <= 0 {
		opts.PerPass = 4096
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	opts.Source.Logger = opts.Logger
	stats := &Stats{}
	t := &Tailer{
		opts:        opts,
		src:         NewSource(opts.Source, stats),
		stats:       stats,
		log:         opts.Logger,
		changes:     opts.Containers.Subscribe(),
		files:       map[string]*followed{},
		positions:   map[string]cosmicdust.Position{},
		unsupported: map[string]bool{},
		buf:         make([]cosmicdust.Record, opts.Batch),
		rateWindow:  cmp.Or(opts.rateWindow, 30*time.Second),
		rateAt:      time.Now(),
	}
	stats.retention.Store(math.Float64bits(math.Inf(1)))
	if opts.Registerer != nil {
		for _, c := range stats.collectors() {
			if err := opts.Registerer.Register(c); err != nil {
				return nil, fmt.Errorf("tail: register metrics: %w", err)
			}
		}
	}
	return t, nil
}

func (t *Tailer) Name() string { return "tail" }

// Stats returns the tailer's counters.
func (t *Tailer) Stats() *Stats { return t.stats }

// Run follows the logs until ctx ends. A failing Sink stops it: without the
// store there is nowhere for the logs to go.
func (t *Tailer) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	defer t.closeAll()
	for {
		d := time.Hour
		switch {
		case t.blocked:
			d = time.Second // try the sink again
		case len(t.queue) > 0:
			d = max(0, time.Until(t.queue[0].due))
		}
		timer.Reset(d)
		select {
		case <-ctx.Done():
			return nil
		case <-t.changes.C():
			for _, id := range t.changes.Take() {
				if err := t.sync(id); err != nil {
					return err
				}
			}
		case <-timer.C:
		}
		if err := t.pass(); err != nil {
			return err
		}
	}
}

// sync brings one container's following in line with its state.
func (t *Tailer) sync(id string) error {
	c, known := t.opts.Containers.Get(id)
	f := t.files[id]
	if !known || c.State == cosmicdust.StateRemoved {
		// The descriptors still read the unlinked files to their end.
		if f != nil {
			f.draining, f.forget = true, true
			t.dueNow(f)
		} else {
			delete(t.positions, id)
		}
		delete(t.unsupported, id)
		return nil
	}
	if c.State == cosmicdust.StateCreated || c.State == cosmicdust.StateUnknown {
		return nil // no log yet: started containers are followed
	}
	if !Readable(c.ContainerRef) {
		if c.LogPath != "" && !t.unsupported[id] {
			t.unsupported[id] = true
			t.stats.unsupported.Add(1)
			t.log.Info("log is not a json-file or CRI file; not read", "container", short(id), "name", c.Name, "format", c.LogFormat)
		}
		return nil
	}
	running := c.State == cosmicdust.StateRunning
	if f != nil && f.ref.LogPath != c.LogPath {
		// A new file for the same container (the kubelet starts one per
		// restart): the old one is read to its end first.
		if err := t.drainNow(f); err != nil {
			return err
		}
		f = nil
	}
	if f == nil {
		_, seen := t.positions[id]
		if !running && t.opts.SkipExited && !seen {
			return nil
		}
		pos, ok := t.positions[id]
		if !ok && t.opts.Resume != nil {
			pos, _ = t.opts.Resume(id)
		}
		f = &followed{id: id, committed: pos, pending: pos, index: -1, window: time.Now()}
		t.files[id] = f
		t.stats.open.Add(1)
	}
	f.ref = c.ContainerRef
	f.draining, f.forget = !running, false
	if running {
		// A new or restarted container counts as having just had data, so it
		// is checked every Poll until it has been quiet for a while: backing
		// off at once put a new container's first line half a second late
		//.
		f.every, f.lastData = t.opts.Poll, time.Now()
	}
	t.dueNow(f)
	return nil
}

func (t *Tailer) dueNow(f *followed) {
	f.due = time.Now()
	if f.index >= 0 {
		heap.Fix(&t.queue, f.index)
	} else {
		heap.Push(&t.queue, f)
	}
}

// pass reads the files that are due, hands their records to the sink, and
// makes them durable with one Flush.
func (t *Tailer) pass() error {
	if t.blocked {
		// Nothing more is read until what was read last is stored: a full
		// disk stops the reading, never the agent. The lines wait in the
		// runtime's files.
		if err := t.flush(); err != nil || t.blocked {
			return err
		}
	}
	now := time.Now()
	if len(t.queue) == 0 || t.queue[0].due.After(now) {
		return nil
	}
	t.stats.passes.Add(1)
	budget := t.opts.PerPass
	for len(t.queue) > 0 && !t.queue[0].due.After(now) && budget > 0 && !t.blocked {
		f := heap.Pop(&t.queue).(*followed)
		from := f.pending
		got, more, err := t.readFile(f, budget)
		if errors.Is(err, errSink) {
			return err
		}
		f.written += advanced(from, f.pending)
		budget -= got
		t.read = append(t.read, f)
		busy := got > 0
		if busy {
			f.lastData = now
		}
		switch {
		case err != nil:
			t.stats.readErrors.Add(1)
			level := slog.LevelWarn
			if f.failing {
				level = slog.LevelDebug
			}
			t.log.Log(context.Background(), level, "cannot read a container log; trying again", "container", short(f.id), "name", f.ref.Name, "err", err)
			f.failing = true
			if f.r != nil {
				_ = f.r.Close()
				f.r = nil // reopened from its position
			}
			f.every = t.opts.PollMax
		case f.draining && !busy:
			t.done = append(t.done, f)
			continue
		default:
			f.failing = false
			f.every = nextPoll(f.every, t.opts.Poll, t.opts.PollMax, busy, now.Sub(f.lastData))
		}
		f.due = align(now.Add(f.every), t.opts.Poll)
		if more {
			f.due = now // the budget ran out, not the file
		}
		heap.Push(&t.queue, f)
	}
	if err := t.flush(); err != nil {
		return err
	}
	for _, f := range t.done {
		t.close(f)
	}
	t.done = t.done[:0]
	t.retention(now)
	return nil
}

// shortRetention is the least a runtime should keep of a log, in time at the
// rate it is written, before the tailer warns: a longer stop of the agent, or
// a longer catch-up after one, loses lines.
const shortRetention = 10 * time.Minute

// retention estimates every rateWindow how long a stop of the agent each
// followed log survives: what the runtime keeps of it over the rate it is
// written. Rotation sets that bound, not the agent: a 30 s stop lost 18,766
// lines of a container whose rotation kept 11 s. The rate is what
// reading went through, so it runs high while a backlog is read.
func (t *Tailer) retention(now time.Time) {
	if now.Sub(t.rateAt) < t.rateWindow {
		return
	}
	t.rateAt = now
	shortest := math.Inf(1)
	for _, f := range t.files {
		if d := now.Sub(f.window); d >= t.rateWindow {
			f.rate = float64(f.written) / d.Seconds()
			f.window, f.written = now, 0
		}
		if f.ref.LogKeep <= 0 || f.rate <= 0 || f.draining {
			continue
		}
		keeps := time.Duration(float64(f.ref.LogKeep) / f.rate * float64(time.Second))
		shortest = min(shortest, keeps.Seconds())
		switch {
		case keeps < shortRetention && !f.short:
			f.short = true
			t.log.Warn("the runtime keeps little of this container's log: a longer stop of the agent loses lines",
				"container", short(f.id), "name", f.ref.Name, "keeps", keeps.Round(time.Second),
				"log_bytes", f.ref.LogKeep, "bytes_per_second", int64(f.rate))
		case keeps > 2*shortRetention:
			f.short = false
		}
	}
	t.stats.retention.Store(math.Float64bits(shortest))
}

// advanced is how many bytes of the log reading went through: within one file
// the offsets tell; past a rotation, what it read of the new file.
func advanced(from, to cosmicdust.Position) int64 {
	if to.Inode == from.Inode && to.Head == from.Head {
		return max(0, to.Offset-from.Offset)
	}
	return to.Offset
}

var errSink = errors.New("tail: sink failed")

// readFile reads f up to budget records and adds them to the sink. more
// reports that the budget, not the file, ran out.
func (t *Tailer) readFile(f *followed, budget int) (got int, more bool, err error) {
	if f.r == nil {
		if f.r, err = t.src.open(f.ref, f.committed); err != nil {
			return 0, false, err
		}
	}
	for got < budget {
		buf := t.buf[:min(len(t.buf), budget-got)]
		n, err := f.r.Next(buf)
		if n > 0 {
			if aerr := t.opts.Sink.Add(buf[:n]); aerr != nil {
				if !isTemporary(aerr) {
					return got, false, fmt.Errorf("%w: %w", errSink, aerr)
				}
				// Taken, not yet stored: stop reading until a Flush goes through.
				t.block(aerr)
			}
			t.added = true
			got += n
			if t.blocked {
				f.pending = f.r.Position()
				return got, false, nil
			}
		}
		f.pending = f.r.Position()
		if err != nil {
			return got, false, err
		}
		if n < len(buf) {
			return got, false, nil
		}
	}
	return got, true, nil
}

// flush makes what the pass added durable; only then do the positions of the
// files read count as committed.
func (t *Tailer) flush() error {
	if t.added {
		if err := t.opts.Sink.Flush(); err != nil {
			if isTemporary(err) {
				t.block(err)
				return nil
			}
			return fmt.Errorf("%w: flush: %w", errSink, err)
		}
		t.added = false
	}
	if t.blocked {
		t.blocked = false
		t.log.Info("the store takes logs again; reading resumes")
	}
	for _, f := range t.read {
		f.committed = f.pending
	}
	t.read = t.read[:0]
	return nil
}

func (t *Tailer) block(err error) {
	if !t.blocked {
		t.stats.storeWaits.Add(1)
		t.log.Warn("the store has no room for now; reading waits", "err", err)
	}
	t.blocked = true
}

// isTemporary reports an error to wait out rather than stop on: no space now,
// perhaps some later.
func isTemporary(err error) bool {
	var t interface{ Temporary() bool }
	return errors.As(err, &t) && t.Temporary()
}

// drainNow reads f to its end at once and stops following it.
func (t *Tailer) drainNow(f *followed) error {
	if f.index >= 0 {
		heap.Remove(&t.queue, f.index)
	}
	for {
		got, more, err := t.readFile(f, t.opts.PerPass)
		t.read = append(t.read, f)
		if errors.Is(err, errSink) {
			return err
		}
		if err := t.flush(); err != nil {
			return err
		}
		if err != nil || (got == 0 && !more) {
			break
		}
	}
	t.close(f)
	return nil
}

// close stops following f, keeping where it ended unless it was removed.
func (t *Tailer) close(f *followed) {
	if f.r != nil {
		_ = f.r.Close()
		f.r = nil
	}
	delete(t.files, f.id)
	t.stats.open.Add(-1)
	if f.forget {
		delete(t.positions, f.id)
	} else {
		t.positions[f.id] = f.committed
	}
}

func (t *Tailer) closeAll() {
	for _, f := range t.files {
		if f.r != nil {
			_ = f.r.Close()
		}
	}
}

// align moves a deadline up to the next multiple of grid. Deadlines on one
// grid make the files that are due at about the same time one pass, and one
// pass is one write to the store: unaligned, ten files polled every 250 ms
// made ~40 passes and ~40 fsyncs a second.
func align(t time.Time, grid time.Duration) time.Time {
	a := t.Truncate(grid)
	if a.Before(t) {
		a = a.Add(grid)
	}
	return a
}

// nextPoll returns the interval until the next check: base after a check that
// found data and while the file has been quiet for less than max, otherwise
// twice the current one, capped at max. Holding base for a while keeps the
// first lines of a new container, and the next lines of a file that paused
// briefly, as fresh as on a busy file. With max <= base every check
// is base apart.
func nextPoll(cur, base, max time.Duration, busy bool, quiet time.Duration) time.Duration {
	if busy || max <= base || quiet < max {
		return base
	}
	return min(2*cur, max)
}

// queue orders followed files by due time.
type queue []*followed

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].due.Before(q[j].due) }
func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}
func (q *queue) Push(x any) {
	f := x.(*followed)
	f.index = len(*q)
	*q = append(*q, f)
}
func (q *queue) Pop() any {
	old := *q
	f := old[len(old)-1]
	old[len(old)-1] = nil
	f.index = -1
	*q = old[:len(old)-1]
	return f
}
