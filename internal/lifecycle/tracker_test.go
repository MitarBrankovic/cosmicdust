package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// fakeSource plays a runtime: a resync on connect, then whatever the test
// pushes; a scripted listing; scripted descriptions.
type fakeSource struct {
	rt     string
	events chan cosmicdust.Event

	mu        sync.Mutex
	listing   cosmicdust.Listing
	listErr   error
	lists     int
	describe  map[string]cosmicdust.ContainerState
	described []string
}

func newFake(rt string) *fakeSource {
	return &fakeSource{rt: rt, events: make(chan cosmicdust.Event, 16), describe: map[string]cosmicdust.ContainerState{}}
}

func (f *fakeSource) Runtime() string { return f.rt }

func (f *fakeSource) Watch(ctx context.Context, out chan<- cosmicdust.Event) error {
	mark := cosmicdust.Event{Time: time.Now(), Runtime: f.rt, Action: "resync", Kind: cosmicdust.KindResync,
		Fields: map[string]string{"reason": "start"}}
	select {
	case out <- mark:
	case <-ctx.Done():
		return nil
	}
	for {
		select {
		case e := <-f.events:
			select {
			case out <- e:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (f *fakeSource) List(ctx context.Context) (cosmicdust.Listing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	l := f.listing
	if l.At.IsZero() {
		l.At = time.Now()
	}
	return l, f.listErr
}

func (f *fakeSource) Describe(ctx context.Context, id string) (cosmicdust.ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.described = append(f.described, id)
	if st, ok := f.describe[id]; ok {
		return st, nil
	}
	return cosmicdust.ContainerState{}, errors.New("no such container")
}

type harness struct {
	t      *testing.T
	tr     *Tracker
	out    chan cosmicdust.Event
	reg    *prometheus.Registry
	cancel context.CancelFunc
	done   chan error
}

func start(t *testing.T, opts Options) *harness {
	t.Helper()
	out := make(chan cosmicdust.Event, 64)
	reg := prometheus.NewRegistry()
	opts.Out = out
	opts.Registerer = reg
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	tr, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, tr: tr, out: out, reg: reg, cancel: cancel, done: make(chan error, 1)}
	go func() { h.done <- tr.Run(ctx) }()
	t.Cleanup(func() { _ = h.stop() })
	return h
}

func (h *harness) stop() error {
	if h.cancel == nil {
		return nil
	}
	h.cancel()
	h.cancel = nil
	return <-h.done
}

// next returns the next event the tracker passed on.
func (h *harness) next() cosmicdust.Event {
	h.t.Helper()
	select {
	case e := <-h.out:
		return e
	case <-time.After(5 * time.Second):
		h.t.Fatal("no event from the tracker within 5s")
		return cosmicdust.Event{}
	}
}

// until skips events until one matches.
func (h *harness) until(match func(cosmicdust.Event) bool) cosmicdust.Event {
	h.t.Helper()
	for {
		if e := h.next(); match(e) {
			return e
		}
	}
}

func isKind(k cosmicdust.Kind, id string) func(cosmicdust.Event) bool {
	return func(e cosmicdust.Event) bool { return e.Kind == k && e.ContainerID == id }
}

// reconciled waits until each runtime's first listing has been applied: a
// fake's listing does not change with the events a test sends, and one
// applied after them would take their container for removed.
func (h *harness) reconciled(runtimes ...string) {
	h.t.Helper()
	waitFor(h.t, func() bool {
		for _, rt := range runtimes {
			if testutil.ToFloat64(h.tr.m.reconciles.WithLabelValues(rt, "ok")) < 1 {
				return false
			}
		}
		return true
	})
}

func TestTrackerFollowsBothRuntimes(t *testing.T) {
	d, c := newFake(docker), newFake(containerd)
	h := start(t, Options{Sources: []Source{{EventSource: d}, {EventSource: c}}})
	h.reconciled(docker, containerd)

	id := "c1"
	at0 := time.Now()
	c.events <- cosmicdust.Event{Time: at0, Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: id, Action: "/tasks/start"}
	if e := h.until(isKind(cosmicdust.KindStart, id)); e.State != cosmicdust.StateRunning || e.Epoch != 1 {
		t.Fatalf("after containerd's start: %s epoch %d", e.State, e.Epoch)
	}
	d.events <- cosmicdust.Event{Time: at0.Add(5 * time.Millisecond), Runtime: docker, Namespace: "moby", Kind: cosmicdust.KindStart,
		ContainerID: id, Action: "start", Name: "web", Image: "nginx:1.27", App: "nginx"}
	if e := h.until(isKind(cosmicdust.KindStart, id)); e.Epoch != 1 || e.Name != "web" {
		t.Fatalf("Docker's start of the same incarnation: epoch %d name %q", e.Epoch, e.Name)
	}
	c.events <- cosmicdust.Event{Time: at0.Add(time.Second), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindExit,
		ContainerID: id, Action: "/tasks/exit", ExitCode: code(137)}
	e := h.until(isKind(cosmicdust.KindExit, id))
	if e.State != cosmicdust.StateExited || e.Name != "web" || e.App != "nginx" {
		t.Fatalf("containerd's exit passed on as %s, name %q, app %q", e.State, e.Name, e.App)
	}

	got, ok := h.tr.Get(id)
	if !ok || got.State != cosmicdust.StateExited || got.Epoch != 1 || deref(got.ExitCode) != 137 || got.Runtime != docker {
		t.Fatalf("Get = %+v, %v", got, ok)
	}
	if st := h.tr.At(id, at0.Add(500*time.Millisecond)); st.State != cosmicdust.StateRunning || st.Epoch != 1 {
		t.Fatalf("At(mid-run) = %+v", st)
	}
	if st := h.tr.At(id, at0.Add(2*time.Second)); st.State != cosmicdust.StateExited {
		t.Fatalf("At(after the exit) = %+v", st)
	}
}

// After a kill -9 the snapshot can be older than the events already passed
// on; replayed from the WAL, they are in the state again, and the listing
// infers nothing a second time.
func TestTrackerReplaysWhatTheSnapshotMissed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifecycle.json")
	t0 := time.Now().Add(-time.Minute)
	ev := func(at time.Duration, k cosmicdust.Kind, action string, exit *int) cosmicdust.Event {
		return cosmicdust.Event{Time: t0.Add(at), Runtime: docker, Namespace: "moby", Kind: k, Action: action, ContainerID: "r1",
			Name: "app", ExitCode: exit}
	}
	// What was passed on before the crash; the snapshot had none of it.
	wal := []cosmicdust.Event{
		ev(0, cosmicdust.KindStart, "start", nil),
		ev(10*time.Second, cosmicdust.KindExit, "die", code(137)),
		{Time: t0.Add(10 * time.Second), Runtime: docker, Namespace: "moby", Kind: cosmicdust.KindOOM, Action: oomAction,
			ContainerID: "r1", Epoch: 1},
		{Time: t0.Add(11 * time.Second), Runtime: "cosmicdust", Action: "logs_dropped"},
	}
	d := newFake(docker)
	d.listing = cosmicdust.Listing{Containers: []cosmicdust.ContainerState{{
		ContainerRef: cosmicdust.ContainerRef{ID: "r1", Name: "app", Namespace: "moby"},
		State:        cosmicdust.StateExited, StartedAt: t0, FinishedAt: t0.Add(10 * time.Second), ExitCode: code(137),
	}}}
	h := start(t, Options{Sources: []Source{{EventSource: d}}, SnapshotPath: path,
		Replay: func(since time.Time, fn func(cosmicdust.Event)) error {
			if !since.IsZero() {
				t.Errorf("replay since %v without a snapshot, want all", since)
			}
			for _, e := range wal {
				fn(e)
			}
			return nil
		}})
	h.reconciled(docker)
	c, ok := h.tr.Get("r1")
	if !ok || c.State != cosmicdust.StateExited || c.Epoch != 1 || !c.OOMKilled {
		t.Fatalf("after the replay: %+v", c)
	}
	if n := testutil.ToFloat64(h.tr.m.inferred.WithLabelValues(docker, "exit")); n != 0 {
		t.Fatalf("%v exits inferred again", n)
	}
	if n := testutil.ToFloat64(h.tr.m.replayed); n != 3 {
		t.Fatalf("replayed %v, want 3", n)
	}
	for drained := false; !drained; {
		select {
		case e := <-h.out:
			if e.ContainerID == "r1" {
				t.Fatalf("passed on again: %+v", e)
			}
		default:
			drained = true
		}
	}
	// Saved, the state on disk has them: the WAL may let them go.
	waitFor(t, func() bool { return h.tr.CleanAt().After(t0.Add(time.Minute)) })
}

func TestTrackerReconcilesAtStart(t *testing.T) {
	d := newFake(docker)
	started := time.Now().Add(-time.Hour)
	d.listing = cosmicdust.Listing{Containers: []cosmicdust.ContainerState{{
		ContainerRef: cosmicdust.ContainerRef{ID: "old", Name: "db", Namespace: "moby", LogPath: "/var/lib/docker/containers/old/old-json.log"},
		State:        cosmicdust.StateRunning, StartedAt: started,
	}}}
	h := start(t, Options{Sources: []Source{{EventSource: d}}})
	e := h.until(isKind(cosmicdust.KindStart, "old"))
	if !e.Inferred || !e.Time.Equal(started) || e.State != cosmicdust.StateRunning {
		t.Fatalf("inferred start: %+v", e)
	}
	waitFor(t, func() bool {
		c, _ := h.tr.Get("old")
		return c.LogPath != ""
	})
	if st := h.tr.At("old", started.Add(time.Minute)); st.State != cosmicdust.StateRunning {
		t.Fatalf("a line written an hour ago is stored as %s", st.State)
	}
}

func TestTrackerDescribesNewContainers(t *testing.T) {
	d := newFake(docker)
	d.describe["n1"] = cosmicdust.ContainerState{ContainerRef: cosmicdust.ContainerRef{ID: "n1", LogPath: "/x/n1-json.log", LogFormat: "json-file", LogKeep: 30 << 20}}
	h := start(t, Options{Sources: []Source{{EventSource: d}}})
	h.reconciled(docker)
	d.events <- cosmicdust.Event{Time: time.Now(), Runtime: docker, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "n1", Action: "start"}
	h.until(isKind(cosmicdust.KindStart, "n1"))
	waitFor(t, func() bool {
		c, _ := h.tr.Get("n1")
		return c.LogPath == "/x/n1-json.log" && c.LogFormat == "json-file" && c.LogKeep == 30<<20
	})
}

func TestTrackerConfirmsOOMFromTheCgroup(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "system.slice", "docker-c9.scope")
	c := newFake(containerd)
	h := start(t, Options{Sources: []Source{{EventSource: c}}, CgroupRoot: root})
	h.reconciled(containerd)
	now := time.Now()
	c.events <- cosmicdust.Event{Time: now, Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "c9"}
	h.until(isKind(cosmicdust.KindStart, "c9"))
	// The cgroup was not there to watch at the start; at the exit it is.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte("low 0\nhigh 0\nmax 12\noom 1\noom_kill 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.events <- cosmicdust.Event{Time: now.Add(time.Second), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindExit, ContainerID: "c9", ExitCode: code(137)}
	if e := h.until(isKind(cosmicdust.KindExit, "c9")); !e.OOMKilled {
		t.Fatal("exit 137 with oom_kill 1 in the cgroup not marked as an OOM kill")
	}
	if n := testutil.ToFloat64(h.tr.m.oomKills.WithLabelValues("event")); n != 1 {
		t.Fatalf("oom_kills_total %v, want 1", n)
	}

	// kill -9: the same exit code, no kill in the counter.
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte("oom 0\noom_kill 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.events <- cosmicdust.Event{Time: now.Add(2 * time.Second), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "c9"}
	c.events <- cosmicdust.Event{Time: now.Add(3 * time.Second), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindExit, ContainerID: "c9", ExitCode: code(137)}
	if e := h.until(func(e cosmicdust.Event) bool { return e.Kind == cosmicdust.KindExit && e.Epoch == 2 }); e.OOMKilled {
		t.Fatal("kill -9 marked as an OOM kill")
	}
}

func TestTrackerSnapshotAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifecycle.json")
	d := newFake(docker)
	h := start(t, Options{Sources: []Source{{EventSource: d}}, SnapshotPath: path})
	h.reconciled(docker)
	t0 := time.Now().Add(-time.Minute)
	d.events <- cosmicdust.Event{Time: t0, Runtime: docker, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "s1", Name: "app"}
	h.until(isKind(cosmicdust.KindStart, "s1"))
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no snapshot after a clean stop: %v", err)
	}

	// While the agent was away the container stopped; the listing says so.
	d2 := newFake(docker)
	finished := t0.Add(30 * time.Second)
	d2.listing = cosmicdust.Listing{Containers: []cosmicdust.ContainerState{{
		ContainerRef: cosmicdust.ContainerRef{ID: "s1", Name: "app", Namespace: "moby"},
		State:        cosmicdust.StateExited, StartedAt: t0, FinishedAt: finished, ExitCode: code(0),
	}}}
	h2 := start(t, Options{Sources: []Source{{EventSource: d2}}, SnapshotPath: path})
	e := h2.until(isKind(cosmicdust.KindExit, "s1"))
	if !e.Inferred || !e.Time.Equal(finished) || e.Epoch != 1 {
		t.Fatalf("stopped while away: %+v", e)
	}
	if st := h2.tr.At("s1", t0.Add(10*time.Second)); st.State != cosmicdust.StateRunning || st.Epoch != 1 {
		t.Fatalf("history lost across the restart: %+v", st)
	}
}

func TestTrackerPeriodicReconcile(t *testing.T) {
	c := newFake(containerd)
	h := start(t, Options{Sources: []Source{{EventSource: c, Reconcile: 20 * time.Millisecond}}})
	h.until(func(e cosmicdust.Event) bool { return e.Kind == cosmicdust.KindResync })
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.lists >= 3
	})
	if n := testutil.ToFloat64(h.tr.m.resyncs.WithLabelValues(containerd, "start")); n != 1 {
		t.Fatalf("resyncs_total{reason=start} = %v, want 1", n)
	}
	if n := testutil.ToFloat64(h.tr.m.reconciles.WithLabelValues(containerd, "ok")); n < 3 {
		t.Fatalf("reconciliations_total{ok} = %v, want at least 3", n)
	}
}

func TestContainersMetric(t *testing.T) {
	d := newFake(docker)
	h := start(t, Options{Sources: []Source{{EventSource: d}}})
	h.reconciled(docker)
	d.events <- cosmicdust.Event{Time: time.Now(), Runtime: docker, Kind: cosmicdust.KindStart, ContainerID: "m1", Namespace: "moby"}
	h.until(isKind(cosmicdust.KindStart, "m1"))
	want := `
# HELP cosmicdust_agent_lifecycle_containers Tracked containers, by merged state.
# TYPE cosmicdust_agent_lifecycle_containers gauge
cosmicdust_agent_lifecycle_containers{state="created"} 0
cosmicdust_agent_lifecycle_containers{state="exited"} 0
cosmicdust_agent_lifecycle_containers{state="removed"} 0
cosmicdust_agent_lifecycle_containers{state="restarting"} 0
cosmicdust_agent_lifecycle_containers{state="running"} 1
cosmicdust_agent_lifecycle_containers{state="unknown"} 0
`
	if err := testutil.GatherAndCompare(h.reg, strings.NewReader(want), "cosmicdust_agent_lifecycle_containers"); err != nil {
		t.Fatal(err)
	}
}

func TestTrackerSurvivesAFailingListing(t *testing.T) {
	d := newFake(docker)
	d.listErr = errors.New("daemon busy")
	h := start(t, Options{Sources: []Source{{EventSource: d}}})
	h.until(func(e cosmicdust.Event) bool { return e.Kind == cosmicdust.KindResync })
	d.events <- cosmicdust.Event{Time: time.Now(), Runtime: docker, Kind: cosmicdust.KindStart, ContainerID: "x", Namespace: "moby"}
	if e := h.until(isKind(cosmicdust.KindStart, "x")); e.State != cosmicdust.StateRunning {
		t.Fatalf("after a failed listing the stream is not applied: %+v", e)
	}
	waitFor(t, func() bool {
		return testutil.ToFloat64(h.tr.m.reconciles.WithLabelValues(docker, "error")) == 1
	})
}

func TestNewRejectsTwoSourcesOfOneRuntime(t *testing.T) {
	if _, err := New(Options{Sources: []Source{{EventSource: newFake(docker)}, {EventSource: newFake(docker)}}}); err == nil {
		t.Fatal("New accepted two Docker sources")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubscribeSeesNewAndKnownContainers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifecycle.json")
	d := newFake(docker)
	d.describe["k1"] = cosmicdust.ContainerState{ContainerRef: cosmicdust.ContainerRef{ID: "k1", LogPath: "/x/k1-json.log", LogFormat: "json-file"}}
	h := start(t, Options{Sources: []Source{{EventSource: d}}, SnapshotPath: path})
	ch := h.tr.Subscribe()
	h.reconciled(docker)
	d.events <- cosmicdust.Event{Time: time.Now(), Runtime: docker, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "k1"}
	h.until(isKind(cosmicdust.KindStart, "k1"))
	// The state changed, then the log path arrived with the description:
	// the subscriber hears of k1, and Get then has the path.
	var heard []string
	waitFor(t, func() bool {
		select {
		case <-ch.C():
			heard = append(heard, ch.Take()...)
		default:
		}
		c, _ := h.tr.Get("k1")
		return c.LogPath != "" && slices.Contains(heard, "k1")
	})
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}

	// After a restart nothing changes for k1, yet a subscriber hears of it.
	h2 := start(t, Options{Sources: []Source{{EventSource: newFake(docker)}}, SnapshotPath: path})
	ch2 := h2.tr.Subscribe()
	var got []string
	waitFor(t, func() bool {
		select {
		case <-ch2.C():
			got = append(got, ch2.Take()...)
		default:
		}
		return slices.Contains(got, "k1")
	})
}

func TestChangesCoalesce(t *testing.T) {
	c := NewChanges()
	for range 100 {
		c.Add("a")
	}
	c.Add("b")
	<-c.C()
	got := c.Take()
	slices.Sort(got)
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("Take = %v, want a,b once each", got)
	}
	select {
	case <-c.C():
		t.Fatal("a second signal for changes already taken")
	default:
	}
}
