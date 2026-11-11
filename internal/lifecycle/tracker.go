// Package lifecycle keeps the state of every container on the node: derived
// from the events of every runtime, reconciled against their listings after
// every connect, kept on disk across restarts, and remembered over time so
// that a log line can be stored with the state its container had when the
// line was written.
package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Source is one runtime and how often to reconcile with it.
type Source struct {
	cosmicdust.EventSource

	// Reconcile is how often to compare with a fresh listing besides after
	// every connect; zero means only after a connect. Listing through
	// containerd is cheap; through Docker it cost ~10-12 ms of CPU per
	// container in each daemon, ~93 % of a core at 200 containers every 5 s
	//, so Docker is listed only after a connect.
	Reconcile time.Duration
}

// Options configure a Tracker.
type Options struct {
	Sources []Source

	// Out receives every event once applied, with the container's merged
	// state; nil discards them. The tracker blocks on it: events are data,
	// never dropped.
	Out chan<- cosmicdust.Event

	// SnapshotPath keeps the tracked state across restarts; empty keeps it
	// in memory only. It is written SnapshotDelay after a change (default
	// 1 s), and checked every SnapshotEvery (default 10 s). A kill -9 loses
	// at most the last SnapshotDelay of it: what changed then is inferred
	// again at the next start, as an event the store already has (with
	// a fixed 10 s, kills 2-4 s apart re-inferred every container).
	SnapshotPath  string
	SnapshotDelay time.Duration
	SnapshotEvery time.Duration

	// CgroupRoot is where the kernel's OOM counters are watched and read
	// (cgroup v2); empty disables the check.
	CgroupRoot string

	// Replay hands Run, before anything else, the events the tracker passed
	// on before a restart and wrote to the WAL since a moment: shortly
	// before the snapshot was written. They are applied again, not passed
	// on, so that the listing after the connect does not infer them a
	// second time (a kill -9 within the second after a change
	// did). The WAL keeps every event the state on disk does not have yet,
	// as CleanAt says. Older ones are in the snapshot, or were about a
	// container long removed and forgotten since.
	Replay func(since time.Time, fn func(cosmicdust.Event)) error

	// DockerNamespace is the containerd namespace of Docker's containers.
	// When a Docker source is present, containerd is not asked to describe
	// them: Docker has their names, labels and log paths. Default "moby".
	DockerNamespace string

	History         int           // transitions kept per container, default 64
	RemovedTTL      time.Duration // how long a removed container is remembered, default 1h
	DescribeWorkers int           // concurrent describes, default 4

	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

// Tracker follows the containers of one node. It is a cosmicdust.Component.
type Tracker struct {
	opts    Options
	log     *slog.Logger
	sources map[string]Source
	cg      cgroups
	m       *metrics

	// oom watches the cgroups of live incarnations; nil without a cgroup
	// root or inotify. notes are the kills its adds read, for the loop.
	oom   *oomWatch
	notes []oomNote

	cleanAt   atomic.Int64 // unix ns: every event applied before it is in the snapshot on disk
	replaying bool         // applying the WAL's events at the start

	mu         sync.RWMutex
	containers map[string]*container
	dirty      bool
	subs       []*Changes

	describeQ chan describeJob
}

// container is the tracker's account of one container.
type container struct {
	Ref      cosmicdust.ContainerRef `json:"ref"`
	MetaFrom string                  `json:"meta_from,omitempty"` // the runtime whose word Ref holds
	Views    map[string]*view        `json:"views"`
	History  history                 `json:"history,omitempty"`

	OOMEpoch     int       `json:"oom_epoch,omitempty"`     // last incarnation the OOM killer hit
	CheckedEpoch int       `json:"checked_epoch,omitempty"` // last incarnation whose cgroup was read at its end
	RemovedAt    time.Time `json:"removed_at,omitempty"`    // by the agent's clock

	described map[string]bool // runtimes already asked to describe it
}

// New returns a tracker; Run starts it.
func New(opts Options) (*Tracker, error) {
	if opts.SnapshotEvery <= 0 {
		opts.SnapshotEvery = 10 * time.Second
	}
	if opts.SnapshotDelay <= 0 {
		opts.SnapshotDelay = time.Second
	}
	if opts.DockerNamespace == "" {
		opts.DockerNamespace = "moby"
	}
	if opts.History <= 0 {
		opts.History = 64
	}
	if opts.RemovedTTL <= 0 {
		opts.RemovedTTL = time.Hour
	}
	if opts.DescribeWorkers <= 0 {
		opts.DescribeWorkers = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	t := &Tracker{
		opts:       opts,
		log:        opts.Logger,
		sources:    make(map[string]Source, len(opts.Sources)),
		cg:         cgroups{root: opts.CgroupRoot},
		containers: map[string]*container{},
		describeQ:  make(chan describeJob, 1024),
	}
	for _, s := range opts.Sources {
		rt := s.Runtime()
		if _, dup := t.sources[rt]; dup {
			return nil, fmt.Errorf("lifecycle: two sources for %s", rt)
		}
		t.sources[rt] = s
	}
	m, collectors := newMetrics(t)
	t.m = m
	if opts.Registerer != nil {
		for _, c := range collectors {
			if err := opts.Registerer.Register(c); err != nil {
				return nil, fmt.Errorf("lifecycle: register metrics: %w", err)
			}
		}
	}
	return t, nil
}

func (t *Tracker) Name() string { return "lifecycle" }

type listingResult struct {
	runtime, reason string
	l               cosmicdust.Listing
	err             error
	took            time.Duration
}

type describeJob struct{ runtime, id string }

type described struct {
	runtime string
	st      cosmicdust.ContainerState
	err     error
}

// Run follows the sources until ctx ends, then applies what they had already
// sent and writes the snapshot.
func (t *Tracker) Run(ctx context.Context) error {
	var written time.Time
	if t.opts.SnapshotPath != "" {
		cs, at, err := readSnapshot(t.opts.SnapshotPath)
		written = at
		if err != nil {
			t.log.Warn("cannot read the snapshot; starting from the listings alone", "err", err)
		} else {
			t.mu.Lock()
			t.containers = cs
			for id := range cs {
				t.notify(id)
			}
			t.mu.Unlock()
			t.log.Info("snapshot read", "containers", len(cs))
		}
	}
	if t.opts.Replay != nil {
		since := written
		if !since.IsZero() {
			since = since.Add(-time.Minute) // a clock that stepped back
		}
		t.replayAll(since)
	}

	wctx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	var wg sync.WaitGroup
	in := make(chan cosmicdust.Event, 1024)
	listings := make(chan listingResult, len(t.sources))
	results := make(chan described, 64)
	periodic := make(chan string, len(t.sources))
	fatal := make(chan error, len(t.sources)+1)
	notes := make(chan oomNote, 64)

	if t.cg.root != "" {
		w, err := newOOMWatch(t.cg, t.log)
		if err != nil {
			t.log.Warn("cannot watch the cgroups; OOM kills are read at the exits only", "err", err)
		} else {
			t.mu.Lock()
			t.oom = w
			t.mu.Unlock()
			wg.Go(func() {
				if err := w.run(wctx, notes); err != nil {
					fatal <- fmt.Errorf("cgroup watch: %w", err)
				}
			})
		}
	}
	for _, s := range t.sources {
		wg.Go(func() {
			if err := s.Watch(wctx, in); err != nil {
				fatal <- fmt.Errorf("%s events: %w", s.Runtime(), err)
			}
		})
		if s.Reconcile > 0 {
			wg.Go(func() {
				tick := time.NewTicker(s.Reconcile)
				defer tick.Stop()
				for {
					select {
					case <-wctx.Done():
						return
					case <-tick.C:
						select {
						case periodic <- s.Runtime():
						default: // one is already waiting
						}
					}
				}
			})
		}
	}
	for range t.opts.DescribeWorkers {
		wg.Go(func() { t.describeWorker(wctx, results) })
	}

	listing := map[string]bool{}   // a listing is running
	pending := map[string]string{} // asked for while one ran, with its reason
	connected := map[string]bool{} // has connected at least once
	startListing := func(rt, reason string) {
		if listing[rt] {
			pending[rt] = reason
			return
		}
		listing[rt] = true
		src := t.sources[rt]
		wg.Go(func() {
			start := time.Now()
			l, err := src.List(wctx)
			select {
			case listings <- listingResult{runtime: rt, reason: reason, l: l, err: err, took: time.Since(start)}:
			case <-wctx.Done():
			}
		})
	}

	snap := time.NewTicker(t.opts.SnapshotEvery)
	defer snap.Stop()
	save := time.NewTimer(time.Hour)
	save.Stop()
	defer save.Stop()
	saving := false // save is armed
	var err error
loop:
	for {
		if !saving && t.isDirty() {
			save.Reset(t.opts.SnapshotDelay)
			saving = true
		}
		select {
		case <-ctx.Done():
			break loop
		case err = <-fatal:
			break loop
		case ev := <-in:
			if ev.Kind == cosmicdust.KindResync {
				connected[ev.Runtime] = true
				t.m.resyncs.WithLabelValues(ev.Runtime, ev.Fields["reason"]).Inc()
				startListing(ev.Runtime, ev.Fields["reason"])
			}
			t.handle(ctx, ev)
		case r := <-listings:
			listing[r.runtime] = false
			t.reconcile(ctx, r)
			if reason, ok := pending[r.runtime]; ok {
				delete(pending, r.runtime)
				startListing(r.runtime, reason)
			}
		case d := <-results:
			t.describedResult(d)
		case n := <-notes:
			t.kernelOOM(ctx, n)
		case rt := <-periodic:
			if connected[rt] {
				startListing(rt, "periodic")
			}
		case <-save.C:
			saving = false
			t.persist()
		case <-snap.C:
			t.persist()
			t.prune()
		}
	}

	stopWorkers()
	wg.Wait()
	// What the sources sent before they stopped is applied and passed on;
	// the consumer of Out stops after the tracker.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for drained := false; !drained; {
		select {
		case ev := <-in:
			t.handle(dctx, ev)
		case n := <-notes:
			t.kernelOOM(dctx, n)
		default:
			drained = true
		}
	}
	t.persist()
	return err
}

// handle applies one event and passes it on.
func (t *Tracker) handle(ctx context.Context, ev cosmicdust.Event) {
	if ev.Inferred {
		t.m.inferred.WithLabelValues(ev.Runtime, ev.Kind.String()).Inc()
	} else {
		t.m.events.WithLabelValues(ev.Runtime, ev.Kind.String()).Inc()
	}
	out, job := t.apply(ev)
	if job != nil {
		select {
		case t.describeQ <- *job:
		default:
			// Two hundred containers started at once; this one is
			// described when it next starts or at the next listing.
			t.mu.Lock()
			if c := t.containers[job.id]; c != nil {
				delete(c.described, job.runtime)
			}
			t.mu.Unlock()
		}
	}
	if out.ContainerID != "" && t.log.Enabled(ctx, slog.LevelDebug) {
		attrs := []any{"runtime", out.Runtime, "action", out.Action, "container", short(out.ContainerID),
			"name", out.Name, "app", out.App, "state", out.State, "epoch", out.Epoch, "inferred", out.Inferred,
			"at", out.Time.UTC().Format(time.RFC3339Nano)}
		if out.Kind == cosmicdust.KindExit {
			attrs = append(attrs, "exit_code", codeOf(out.ExitCode), "oom_killed", out.OOMKilled)
		}
		t.log.Debug("event", attrs...)
	}
	if t.opts.Out != nil {
		select {
		case t.opts.Out <- out:
		case <-ctx.Done():
		}
	}
	t.flushNotes(ctx)
}

// flushNotes passes on the kills the cgroup watch read as it was added.
func (t *Tracker) flushNotes(ctx context.Context) {
	for len(t.notes) > 0 {
		n := t.notes[0]
		t.notes = t.notes[1:]
		t.kernelOOM(ctx, n)
	}
}

// oomAction names the tracker's own OOM events, from the cgroup watch.
const oomAction = "cgroup/oom_kill"

// kernelOOM takes the kernel's word, from the cgroup watch, that the OOM
// killer hit an incarnation, and passes it on as an event of its own: the
// runtimes may never report this kill, and their exit may have gone out
// already.
func (t *Tracker) kernelOOM(ctx context.Context, n oomNote) {
	t.mu.Lock()
	c := t.containers[n.id]
	if c == nil || !t.confirmOOM(c, n.epoch, "watch", n.kills) {
		t.mu.Unlock()
		return
	}
	st := merged(c.Views)
	state := st.State
	if st.Epoch < n.epoch {
		state = cosmicdust.StateRunning // a start not reported yet: the process ran
	}
	out := cosmicdust.Event{
		Time: n.at, Runtime: or(c.Ref.Runtime, c.MetaFrom), Namespace: c.Ref.Namespace, Action: oomAction,
		Kind: cosmicdust.KindOOM, ContainerID: n.id, Name: c.Ref.Name, Image: c.Ref.Image, App: c.Ref.App,
		Fields: map[string]string{"oom_kill": strconv.FormatInt(n.kills, 10)},
		State:  state, Epoch: n.epoch,
	}
	t.mu.Unlock()
	if t.opts.Out != nil {
		select {
		case t.opts.Out <- out:
		case <-ctx.Done():
		}
	}
}

// confirmOOM records the kernel's word that the OOM killer hit incarnation
// epoch, read by the cgroup watch or at a runtime's exit or OOM event. It
// reports whether that was news. Called with t.mu held.
func (t *Tracker) confirmOOM(c *container, epoch int, read string, kills int64) bool {
	if epoch <= 0 || c.OOMEpoch >= epoch {
		return false
	}
	c.OOMEpoch = epoch
	t.dirty = true
	t.m.oomKills.WithLabelValues(read).Inc()
	t.log.Info("OOM kill confirmed by the kernel", "container", short(c.Ref.ID), "name", c.Ref.Name,
		"epoch", epoch, "oom_kill", kills, "read", read)
	return true
}

// apply folds an event into its container and returns it with the merged
// state, and a describe to run if the container is new to its runtime.
func (t *Tracker) apply(ev cosmicdust.Event) (cosmicdust.Event, *describeJob) {
	if ev.ContainerID == "" {
		return ev, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.containers[ev.ContainerID]
	if c == nil {
		c = &container{Ref: cosmicdust.ContainerRef{ID: ev.ContainerID}, Views: map[string]*view{}}
		t.containers[ev.ContainerID] = c
	}
	if c.meta(ev.Runtime, cosmicdust.ContainerRef{Name: ev.Name, Image: ev.Image, App: ev.App, Namespace: ev.Namespace}) {
		t.notify(ev.ContainerID)
	}

	before := merged(c.Views)
	v := c.Views[ev.Runtime]
	if v == nil {
		v = &view{State: cosmicdust.StateUnknown}
		c.Views[ev.Runtime] = v
	}
	moved, late := v.apply(ev)
	if late && !t.replaying {
		t.m.late.WithLabelValues(ev.Runtime).Inc()
	}
	after := merged(c.Views)
	if moved {
		t.dirty = true
	}
	switch {
	case after.State != before.State || after.Epoch != before.Epoch:
		t.notify(ev.ContainerID)
		c.History = c.History.add(transition{At: ev.Time, State: after.State, Epoch: after.Epoch}, t.opts.History)
		c.RemovedAt = time.Time{}
		if after.State == cosmicdust.StateRemoved {
			c.RemovedAt = time.Now()
		}
	case moved && v.State == after.State && v.Epoch == after.Epoch:
		// A second witness of the same step; it may know better when.
		c.History.earlier(after.State, after.Epoch, ev.Time)
	}

	// The kernel's word on OOM, while the incarnation's cgroup still exists.
	// The watch reads it when the counter grows; with the systemd cgroup
	// driver the scope is gone by the time the exit is reported, and the
	// OOM event, sent while the process is being killed, is then the last
	// moment to ask (the read succeeded on /tasks/oom and on no plain
	// exit).
	if (ev.Kind == cosmicdust.KindExit || ev.Kind == cosmicdust.KindOOM) && !ev.Inferred && !t.replaying && c.CheckedEpoch < after.Epoch {
		if n, read := t.cg.oomKills(ev.ContainerID, c.Ref.Namespace); read {
			c.CheckedEpoch = after.Epoch
			if n > 0 {
				t.confirmOOM(c, after.Epoch, "event", n)
			}
			t.dirty = true
		} else if t.oom != nil && c.OOMEpoch < after.Epoch {
			if n, ok := t.oom.parentOOM(ev.ContainerID, after.Epoch); ok {
				t.confirmOOM(c, after.Epoch, "parent", n)
			}
		}
	}
	if ev.Kind == cosmicdust.KindExit && ev.OOMKilled && c.OOMEpoch < after.Epoch {
		c.OOMEpoch = after.Epoch // an inferred exit, from Docker's inspect
	}
	if t.oom != nil && !ev.Inferred {
		switch ev.Kind {
		case cosmicdust.KindRemove:
			t.oom.forget(ev.ContainerID)
		case cosmicdust.KindExit, cosmicdust.KindOOM:
		default:
			// Any word of a live incarnation. containerd's /tasks/create
			// comes when the cgroup exists and the process has not started;
			// that is the incarnation after the one the views have seen.
			epoch := after.Epoch
			if after.State != cosmicdust.StateRunning {
				epoch++
			}
			if n, ok := t.oom.add(ev.ContainerID, or(ev.Namespace, c.Ref.Namespace), epoch); ok {
				t.notes = append(t.notes, n)
			}
		}
	}

	out := ev
	out.Name, out.Image, out.App = or(c.Ref.Name, ev.Name), or(c.Ref.Image, ev.Image), or(c.Ref.App, ev.App)
	out.State, out.Epoch = after.State, after.Epoch
	if ev.Kind == cosmicdust.KindExit {
		out.OOMKilled = ev.OOMKilled || c.oomIn(after.Epoch)
	}

	// Described at a start: before it Docker has no log path for the
	// container. Again at a later start while the path
	// is still unknown.
	var job *describeJob
	if ev.Kind == cosmicdust.KindStart && !ev.Inferred && (!c.described[ev.Runtime] || c.Ref.LogPath == "") {
		_, docker := t.sources[cosmicdust.RuntimeDocker]
		skip := ev.Runtime == cosmicdust.RuntimeContainerd && docker && ev.Namespace == t.opts.DockerNamespace
		if !skip {
			if c.described == nil {
				c.described = map[string]bool{}
			}
			c.described[ev.Runtime] = true
			job = &describeJob{runtime: ev.Runtime, id: ev.ContainerID}
		}
	}
	return out, job
}

// oomIn reports whether the OOM killer hit incarnation epoch, by the kernel's
// counter or any runtime's word.
func (c *container) oomIn(epoch int) bool {
	if epoch > 0 && c.OOMEpoch == epoch {
		return true
	}
	for _, v := range c.Views {
		if v.Epoch == epoch && v.OOM {
			return true
		}
	}
	return false
}

// meta takes what runtime rt says about the container. Docker's word wins for
// the containers it runs: names and labels are set through it, and only it
// knows their log paths. Empty fields never overwrite. It reports whether
// what the tailer needs, the log's path and format, changed.
func (c *container) meta(rt string, r cosmicdust.ContainerRef) (logChanged bool) {
	path, format := c.Ref.LogPath, c.Ref.LogFormat
	if c.MetaFrom == cosmicdust.RuntimeDocker && rt != cosmicdust.RuntimeDocker {
		fill := func(dst *string, v string) {
			if *dst == "" {
				*dst = v
			}
		}
		fill(&c.Ref.Name, r.Name)
		fill(&c.Ref.Image, r.Image)
		fill(&c.Ref.App, r.App)
		fill(&c.Ref.Namespace, r.Namespace)
		return false
	}
	set := func(dst *string, v string) {
		if v != "" {
			*dst = v
		}
	}
	set(&c.Ref.Name, r.Name)
	set(&c.Ref.Image, r.Image)
	set(&c.Ref.App, r.App)
	set(&c.Ref.Namespace, r.Namespace)
	set(&c.Ref.LogPath, r.LogPath)
	set(&c.Ref.LogFormat, r.LogFormat)
	if r.LogPath != "" || r.LogFormat != "" {
		c.Ref.TTY, c.Ref.LogKeep = r.TTY, r.LogKeep // a description, which says either way
	}
	if rt == cosmicdust.RuntimeDocker || c.MetaFrom == "" {
		c.MetaFrom = rt
		c.Ref.Runtime = rt
	}
	return c.Ref.LogPath != path || c.Ref.LogFormat != format
}

func (t *Tracker) describeWorker(ctx context.Context, results chan<- described) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-t.describeQ:
			src, ok := t.sources[job.runtime]
			if !ok {
				continue
			}
			dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			st, err := src.Describe(dctx, job.id)
			cancel()
			select {
			case results <- described{runtime: job.runtime, st: st, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (t *Tracker) describedResult(d described) {
	if d.err != nil {
		// Gone before it could be described, most likely.
		t.log.Debug("describe failed", "runtime", d.runtime, "err", d.err)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.containers[d.st.ID]; c != nil {
		if c.meta(d.runtime, d.st.ContainerRef) {
			t.notify(d.st.ID)
		}
		t.dirty = true
	}
}

// reconcile applies what a listing implies.
func (t *Tracker) reconcile(ctx context.Context, r listingResult) {
	t.m.reconcileS.WithLabelValues(r.runtime).Observe(r.took.Seconds())
	if r.err != nil {
		if ctx.Err() == nil {
			t.log.Warn("cannot list containers", "runtime", r.runtime, "reason", r.reason, "err", r.err)
		}
		t.m.reconciles.WithLabelValues(r.runtime, "error").Inc()
		return
	}
	t.mu.RLock()
	views := make(map[string]viewRef, len(t.containers))
	for id, c := range t.containers {
		if v := c.Views[r.runtime]; v != nil {
			cp := *v
			views[id] = viewRef{v: &cp, namespace: c.Ref.Namespace}
		}
	}
	t.mu.RUnlock()

	evs := inferred(r.runtime, views, r.l)
	for _, ev := range evs {
		ev.Fields = map[string]string{"reason": r.reason}
		t.handle(ctx, ev)
	}
	t.mu.Lock()
	for _, cs := range r.l.Containers {
		if c := t.containers[cs.ID]; c != nil {
			if c.meta(r.runtime, cs.ContainerRef) {
				t.notify(cs.ID)
			}
			if c.described == nil {
				c.described = map[string]bool{}
			}
			c.described[r.runtime] = true
			// Running when the agent started, or while a stream was down.
			if st := merged(c.Views); t.oom != nil && cs.State == cosmicdust.StateRunning && st.State == cosmicdust.StateRunning {
				if n, ok := t.oom.add(cs.ID, or(cs.Namespace, c.Ref.Namespace), st.Epoch); ok {
					t.notes = append(t.notes, n)
				}
			}
		}
	}
	t.dirty = t.dirty || len(evs) > 0
	t.mu.Unlock()
	t.flushNotes(ctx)
	t.m.reconciles.WithLabelValues(r.runtime, "ok").Inc()
	t.log.Info("reconciled", "runtime", r.runtime, "reason", r.reason, "listed", len(r.l.Containers),
		"inferred", len(evs), "took", r.took.Round(time.Millisecond))
}

func (t *Tracker) isDirty() bool {
	if t.opts.SnapshotPath == "" {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dirty
}

// persist writes the snapshot if anything changed since the last one.
func (t *Tracker) persist() {
	if t.opts.SnapshotPath == "" {
		return
	}
	t.mu.Lock()
	at := time.Now()
	if !t.dirty {
		t.mu.Unlock()
		t.cleanAt.Store(at.UnixNano()) // everything applied is on disk
		return
	}
	data, err := encodeSnapshot(t.containers, at)
	t.dirty = false
	t.mu.Unlock()
	if err == nil {
		err = writeSnapshot(t.opts.SnapshotPath, data)
	}
	if err != nil {
		t.mu.Lock()
		t.dirty = true
		t.mu.Unlock()
		t.m.snapshots.WithLabelValues("error").Inc()
		t.log.Warn("cannot write the snapshot", "path", t.opts.SnapshotPath, "err", err)
		return
	}
	t.m.snapshots.WithLabelValues("ok").Inc()
	t.cleanAt.Store(at.UnixNano())
}

// CleanAt is when the state on disk last had every event the tracker had
// applied: events written to the WAL before it can go with their segment.
// Without a snapshot there is nothing to replay into, so it is now.
func (t *Tracker) CleanAt() time.Time {
	if t.opts.SnapshotPath == "" {
		return time.Now()
	}
	return time.Unix(0, t.cleanAt.Load())
}

// replayAll applies the events Options.Replay hands over.
func (t *Tracker) replayAll(since time.Time) {
	t.replaying = true
	defer func() { t.replaying = false }()
	n := 0
	err := t.opts.Replay(since, func(ev cosmicdust.Event) {
		if ev.ContainerID == "" {
			return // not about a container (logs_dropped)
		}
		n++
		if ev.Kind == cosmicdust.KindOOM && ev.Action == oomAction {
			// The tracker's own word, from the cgroup watch.
			t.mu.Lock()
			if c := t.containers[ev.ContainerID]; c != nil && c.OOMEpoch < ev.Epoch {
				c.OOMEpoch, t.dirty = ev.Epoch, true
			}
			t.mu.Unlock()
			return
		}
		if _, ok := t.sources[ev.Runtime]; ok {
			t.apply(ev)
		}
	})
	// A replayed start asked for a description, which is not run: the
	// listing or the next start describes the container as before.
	t.mu.Lock()
	for _, c := range t.containers {
		c.described = nil
	}
	t.mu.Unlock()
	t.m.replayed.Add(float64(n))
	if err != nil {
		t.log.Warn("cannot replay the WAL's events; the listings infer what they missed", "err", err)
	}
	if n > 0 {
		t.log.Info("events replayed from the WAL", "events", n)
	}
}

// prune forgets containers removed longer than RemovedTTL ago. Until then
// their state is still asked for: the tailer drains a removed container's
// file, and the store stamps those lines.
func (t *Tracker) prune() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.containers {
		if !c.RemovedAt.IsZero() && time.Since(c.RemovedAt) > t.opts.RemovedTTL {
			if t.oom != nil {
				t.oom.forget(id)
			}
			delete(t.containers, id)
			t.notify(id)
			t.dirty = true
		}
	}
}

// Status is a container's merged state at one moment.
type Status struct {
	State cosmicdust.State
	Epoch int
}

// At returns the state container id was in at ts: what a log line written at
// ts is stored with.
func (t *Tracker) At(id string, ts time.Time) Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c := t.containers[id]
	if c == nil {
		return Status{State: cosmicdust.StateUnknown}
	}
	s, e := c.History.at(ts)
	return Status{State: s, Epoch: e}
}

// Container is the tracker's account of one container.
type Container struct {
	cosmicdust.ContainerRef
	State     cosmicdust.State
	Epoch     int
	ExitCode  *int
	OOMKilled bool // in its current or last incarnation
}

// Get returns one container.
func (t *Tracker) Get(id string) (Container, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c := t.containers[id]
	if c == nil {
		return Container{}, false
	}
	return c.public(), true
}

// List returns every tracked container, by ID.
func (t *Tracker) List() []Container {
	t.mu.RLock()
	out := make([]Container, 0, len(t.containers))
	for _, c := range t.containers {
		out = append(out, c.public())
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *container) public() Container {
	st := merged(c.Views)
	return Container{ContainerRef: c.Ref, State: st.State, Epoch: st.Epoch, ExitCode: st.ExitCode, OOMKilled: c.oomIn(st.Epoch)}
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// codeOf makes an exit code printable: a number, or nothing.
func codeOf(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
