package tail

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/lifecycle"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// fakeContainers stands in for the lifecycle tracker.
type fakeContainers struct {
	mu sync.Mutex
	cs map[string]lifecycle.Container
	ch *lifecycle.Changes
}

func newContainers() *fakeContainers {
	return &fakeContainers{cs: map[string]lifecycle.Container{}, ch: lifecycle.NewChanges()}
}

func (f *fakeContainers) Get(id string) (lifecycle.Container, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.cs[id]
	return c, ok
}

func (f *fakeContainers) Subscribe() *lifecycle.Changes { return f.ch }

func (f *fakeContainers) set(id, path string, state cosmicdust.State) {
	f.setRef(cosmicdust.ContainerRef{ID: id, Name: id, LogPath: path, LogFormat: "json-file"}, state)
}

func (f *fakeContainers) setRef(ref cosmicdust.ContainerRef, state cosmicdust.State) {
	f.mu.Lock()
	f.cs[ref.ID] = lifecycle.Container{ContainerRef: ref, State: state}
	f.mu.Unlock()
	f.ch.Add(ref.ID)
}

func (f *fakeContainers) remove(id string) {
	f.mu.Lock()
	delete(f.cs, id)
	f.mu.Unlock()
	f.ch.Add(id)
}

// captureSink keeps what was flushed, line by line.
type captureSink struct {
	mu      sync.Mutex
	pending []string
	lines   []string
	flushes int
	addErr  error
}

func (s *captureSink) Add(recs []cosmicdust.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addErr != nil {
		return s.addErr
	}
	for _, r := range recs {
		s.pending = append(s.pending, string(r.Line))
	}
	return nil
}

func (s *captureSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, s.pending...)
	s.pending = s.pending[:0]
	s.flushes++
	return nil
}

func (s *captureSink) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

type tailHarness struct {
	t    *testing.T
	tl   *Tailer
	cs   *fakeContainers
	sink *captureSink
	done chan error
	stop context.CancelFunc
}

func startTailer(t *testing.T, opts Options) *tailHarness {
	t.Helper()
	h := &tailHarness{t: t, cs: newContainers(), sink: &captureSink{}, done: make(chan error, 1)}
	if opts.Containers == nil {
		opts.Containers = h.cs
	}
	if opts.Sink == nil {
		opts.Sink = h.sink
	}
	if opts.Poll == 0 {
		opts.Poll = 2 * time.Millisecond
	}
	if opts.PollMax == 0 {
		opts.PollMax = 20 * time.Millisecond
	}
	opts.Logger = slog.New(slog.DiscardHandler)
	tl, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	h.tl = tl
	ctx, cancel := context.WithCancel(context.Background())
	h.stop = cancel
	go func() { h.done <- tl.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-h.done
	})
	return h
}

// waitLines waits until the sink holds exactly line-from .. line-to.
func (h *tailHarness) waitLines(from, to int) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := h.sink.got()
		if len(got) >= to-from+1 {
			for i, l := range got {
				if n, err := strconv.Atoi(strings.TrimPrefix(l, "line-")); err != nil || n != from+i {
					h.t.Fatalf("line %d is %q, want line-%d (all: %v)", i, l, from+i, got)
				}
			}
			if len(got) != to-from+1 {
				h.t.Fatalf("got %d lines, want %d", len(got), to-from+1)
			}
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("got %d lines after 5s, want %d", len(got), to-from+1)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 5s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestTailerFollowsARunningContainer(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(5)
	h := startTailer(t, Options{})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	h.waitLines(1, 5)
	d.write(3)
	d.rotate()
	d.write(2)
	h.waitLines(1, 10)
	if n := h.tl.Stats().open.Load(); n != 1 {
		t.Fatalf("open files %d, want 1", n)
	}
}

func TestTailerWaitsForTheStart(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(2)
	h := startTailer(t, Options{})
	h.cs.set("c1", d.path, cosmicdust.StateCreated)
	time.Sleep(30 * time.Millisecond)
	if len(h.sink.got()) != 0 || h.tl.Stats().open.Load() != 0 {
		t.Fatal("a created container's log was opened")
	}
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	h.waitLines(1, 2)
}

// An exited container is read to its end and let go; when it starts again
// reading resumes where it stopped.
func TestTailerDrainsAnExitedContainerAndResumes(t *testing.T) {
	d := newFakeDocker(t, 5)
	h := startTailer(t, Options{})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	d.write(3)
	h.waitLines(1, 3)
	d.write(2)
	h.cs.set("c1", d.path, cosmicdust.StateExited)
	h.waitLines(1, 5)
	eventually(t, "the exited container's file closed", func() bool { return h.tl.Stats().open.Load() == 0 })

	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	d.write(3)
	h.waitLines(1, 8)
}

func TestTailerForgetsARemovedContainer(t *testing.T) {
	d := newFakeDocker(t, 5)
	h := startTailer(t, Options{})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	d.write(2)
	h.waitLines(1, 2)
	d.write(2)
	h.cs.remove("c1")
	h.waitLines(1, 4)
	eventually(t, "the removed container's file closed", func() bool { return h.tl.Stats().open.Load() == 0 })
}

func TestTailerSkipsWhatIsNotAFile(t *testing.T) {
	h := startTailer(t, Options{})
	h.cs.setRef(cosmicdust.ContainerRef{ID: "j", LogPath: "/dev/null", LogFormat: "journald"}, cosmicdust.StateRunning)
	eventually(t, "counted as unsupported", func() bool { return h.tl.Stats().unsupported.Load() == 1 })
	h.cs.setRef(cosmicdust.ContainerRef{ID: "j", LogPath: "/dev/null", LogFormat: "journald"}, cosmicdust.StateRunning)
	time.Sleep(20 * time.Millisecond)
	if n := h.tl.Stats().unsupported.Load(); n != 1 || h.tl.Stats().open.Load() != 0 {
		t.Fatalf("unsupported %d (counted once per container), open %d", n, h.tl.Stats().open.Load())
	}
}

// A long backlog is taken over several passes, each one flushed, so that it
// does not become one huge write.
func TestTailerBoundsAPass(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(5000)
	h := startTailer(t, Options{PerPass: 400, Batch: 64})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	h.waitLines(1, 5000)
	h.sink.mu.Lock()
	flushes := h.sink.flushes
	h.sink.mu.Unlock()
	if flushes < 5000/400 {
		t.Fatalf("%d flushes for 5000 lines at 400 a pass", flushes)
	}
}

func TestTailerUsesResume(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(10)
	pos := d.position(4)
	h := startTailer(t, Options{Resume: func(id string) (cosmicdust.Position, bool) { return pos, id == "c1" }})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	h.waitLines(5, 10)
}

func TestTailerStopsWhenTheSinkFails(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(1)
	h := startTailer(t, Options{})
	h.sink.addErr = errors.New("disk on fire")
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	select {
	case err := <-h.done:
		if !errors.Is(err, errSink) || !strings.Contains(err.Error(), "disk on fire") {
			t.Fatalf("Run returned %v", err)
		}
		h.done <- err // for the cleanup
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going with a failing sink")
	}
}

// A quiet file is checked less and less often: a fixed 2 ms poll would wake
// the tailer ~200 times in 400 ms.
func TestTailerBacksOffWhenQuiet(t *testing.T) {
	d := newFakeDocker(t, 5)
	h := startTailer(t, Options{Poll: 2 * time.Millisecond, PollMax: 50 * time.Millisecond})
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	d.write(1)
	h.waitLines(1, 1)
	time.Sleep(100 * time.Millisecond) // quiet past PollMax
	before := h.tl.Stats().passes.Load()
	time.Sleep(400 * time.Millisecond)
	if n := h.tl.Stats().passes.Load() - before; n > 20 {
		t.Fatalf("%d passes in 400 ms of quiet with PollMax 50 ms", n)
	}
	d.write(1) // still read, at most about PollMax late
	h.waitLines(1, 2)
}

// The kubelet starts a new file for every restart: the old one is read to its
// end, then the new one followed.
func TestTailerFollowsANewLogPath(t *testing.T) {
	d1, d2 := newFakeDocker(t, 5), newFakeDocker(t, 5)
	h := startTailer(t, Options{})
	h.cs.set("c1", d1.path, cosmicdust.StateRunning)
	d1.write(2)
	h.waitLines(1, 2)
	d1.write(1)
	d2.seq = 3
	d2.write(2)
	h.cs.set("c1", d2.path, cosmicdust.StateRunning)
	h.waitLines(1, 5)
}

// noSpaceSink refuses its first Flushes the way a full disk does.
type noSpaceSink struct {
	captureSink
	refuse int
}

type tempErr struct{}

func (tempErr) Error() string   { return "no space left on device" }
func (tempErr) Temporary() bool { return true }

func (s *noSpaceSink) Flush() error {
	s.mu.Lock()
	if s.refuse > 0 {
		s.refuse--
		s.mu.Unlock()
		return tempErr{}
	}
	s.mu.Unlock()
	return s.captureSink.Flush()
}

// A full disk stops the reading, never the agent; once a write goes through,
// every line arrives once.
func TestTailerWaitsOutAFullDisk(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(5)
	sink := &noSpaceSink{refuse: 3}
	h := startTailer(t, Options{Sink: sink})
	h.sink = &sink.captureSink
	h.cs.set("c1", d.path, cosmicdust.StateRunning)
	eventually(t, "reading stopped", func() bool { return h.tl.Stats().storeWaits.Load() == 1 })
	d.write(5)
	h.waitLines(1, 10)
	select {
	case err := <-h.done:
		t.Fatalf("the tailer stopped: %v", err)
	default:
	}
}

// The tailer estimates how long a stop of the agent the runtime's rotation
// leaves room for.
func TestTailerEstimatesRetention(t *testing.T) {
	d := newFakeDocker(t, 5)
	h := startTailer(t, Options{rateWindow: 20 * time.Millisecond})
	retention := func() float64 { return math.Float64frombits(h.tl.Stats().retention.Load()) }
	if !math.IsInf(retention(), 1) {
		t.Fatalf("retention %v with nothing followed, want +Inf", retention())
	}
	h.cs.setRef(cosmicdust.ContainerRef{ID: "c1", Name: "c1", LogPath: d.path, LogFormat: "json-file", LogKeep: 1 << 20}, cosmicdust.StateRunning)
	// ~60 bytes a line, ~10,000 lines a second: 1 MiB lasts well under a
	// minute.
	deadline := time.Now().Add(5 * time.Second)
	for r := retention(); math.IsInf(r, 1); r = retention() {
		if time.Now().After(deadline) {
			t.Fatal("no estimate within 5s")
		}
		d.write(50)
		time.Sleep(5 * time.Millisecond)
	}
	if r := retention(); r <= 0 || r > 60 {
		t.Fatalf("retention %v s, want under a minute", r)
	}
}

func TestAlign(t *testing.T) {
	base := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	grid := 250 * time.Millisecond
	cases := map[time.Duration]time.Duration{
		0: 0, time.Millisecond: grid, 249 * time.Millisecond: grid, grid: grid, grid + 1: 2 * grid,
	}
	for in, want := range cases {
		if got := align(base.Add(in), grid); !got.Equal(base.Add(want)) {
			t.Errorf("align(+%v) = +%v, want +%v", in, got.Sub(base), want)
		}
	}
}

// Files polled at the same interval share their passes: ten active files make
// about one pass per interval, not ten.
func TestTailerSharesPasses(t *testing.T) {
	h := startTailer(t, Options{Poll: 50 * time.Millisecond, PollMax: time.Second})
	var ds []*fakeDocker
	for i := range 10 {
		d := newFakeDocker(t, 5)
		ds = append(ds, d)
		h.cs.set("c"+strconv.Itoa(i), d.path, cosmicdust.StateRunning)
	}
	time.Sleep(200 * time.Millisecond) // discovered, settled on the grid
	before := h.tl.Stats().passes.Load()
	for range 20 { // one second of writing into every file
		for _, d := range ds {
			d.write(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := h.tl.Stats().passes.Load() - before; n > 30 {
		t.Fatalf("%d passes in ~1 s with 10 files on a 50 ms grid, want about 20", n)
	}
}
