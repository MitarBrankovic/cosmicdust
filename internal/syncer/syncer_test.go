package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/c12s/cosmicdust/internal/retry"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

// fakeStore is a cosmicdust.Store in memory.
type fakeStore struct {
	mu       sync.Mutex
	files    []cosmicdust.FileRef
	data     map[string][]byte
	claimed  map[string]bool
	synced   map[string]bool
	claimErr map[string]error
}

func newStore(n int, base time.Time) *fakeStore {
	s := &fakeStore{data: map[string][]byte{}, claimed: map[string]bool{}, synced: map[string]bool{}, claimErr: map[string]error{}}
	for i := range n {
		p := fmt.Sprintf("logs/dt=2026-10-22/hour=09/part-%d.parquet", i)
		d := bytes.Repeat([]byte{byte('a' + i)}, 100+i)
		s.files = append(s.files, cosmicdust.FileRef{Table: "logs", Path: p, Size: int64(len(d)),
			MinTime: base.Add(time.Duration(i) * time.Second), MaxTime: base.Add(time.Duration(i+1) * time.Second)})
		s.data[p] = d
	}
	return s
}

func (s *fakeStore) Unsynced(context.Context) ([]cosmicdust.FileRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []cosmicdust.FileRef
	for _, f := range s.files {
		if !s.synced[f.Path] && !s.claimed[f.Path] {
			out = append(out, f)
		}
	}
	return out, nil
}

func (s *fakeStore) Claim(ref cosmicdust.FileRef) (io.ReadCloser, func(cosmicdust.Ack) error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.claimErr[ref.Path]; err != nil {
		return nil, nil, err
	}
	if s.claimed[ref.Path] {
		return nil, nil, cosmicdust.ErrClaimed
	}
	s.claimed[ref.Path] = true
	return io.NopCloser(bytes.NewReader(s.data[ref.Path])), func(a cosmicdust.Ack) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.claimed, ref.Path)
		if a.Stored() {
			s.synced[ref.Path] = true
		}
		return nil
	}, nil
}

func (s *fakeStore) GC(int64) error { return nil }

func (s *fakeStore) unsynced() []string {
	refs, _ := s.Unsynced(context.Background())
	var out []string
	for _, r := range refs {
		out = append(out, r.Path)
	}
	return out
}

// fakeTransport records what it is sent and answers as told.
type fakeTransport struct {
	mu      sync.Mutex
	got     map[string][]byte
	order   []string
	answer  func(ref cosmicdust.FileRef, n int) (cosmicdust.Ack, error) // n: uploads of this file so far
	uploads map[string]int
	urgent  []cosmicdust.Event
	urgErr  func(n int) error
}

func newTransport() *fakeTransport {
	return &fakeTransport{got: map[string][]byte{}, uploads: map[string]int{}}
}

func (t *fakeTransport) Upload(_ context.Context, ref cosmicdust.FileRef, r io.Reader) (cosmicdust.Ack, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.uploads[ref.Path]++
	if t.answer != nil {
		if a, err := t.answer(ref, t.uploads[ref.Path]); err != nil || a.Status != cosmicdust.AckStored {
			return a, err
		}
	}
	if _, ok := t.got[ref.Path]; ok {
		return cosmicdust.Ack{Status: cosmicdust.AckDuplicate}, nil
	}
	t.got[ref.Path] = data
	t.order = append(t.order, ref.Path)
	return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
}

func (t *fakeTransport) Urgent(_ context.Context, e cosmicdust.Event) (cosmicdust.Ack, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.urgErr != nil {
		if err := t.urgErr(len(t.urgent)); err != nil {
			t.urgent = append(t.urgent, cosmicdust.Event{}) // count the attempt
			return cosmicdust.Ack{}, err
		}
	}
	t.urgent = append(t.urgent, e)
	return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
}

func (t *fakeTransport) Close() error { return nil }

func engine(t *testing.T, s cosmicdust.Store, tr cosmicdust.Transport, reg *prometheus.Registry) *Engine {
	t.Helper()
	e, err := New(Options{Store: s, Transport: tr, Every: 20 * time.Millisecond,
		Backoff: retry.Backoff{Min: 5 * time.Millisecond, Max: 20 * time.Millisecond}, Logger: quiet(), Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestRoundSendsEverythingInOrder(t *testing.T) {
	s, tr, reg := newStore(3, time.Now()), newTransport(), prometheus.NewRegistry()
	e := engine(t, s, tr, reg)
	if err := e.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{s.files[0].Path, s.files[1].Path, s.files[2].Path}
	if !slices.Equal(tr.order, want) {
		t.Fatalf("sent %v", tr.order)
	}
	for _, f := range s.files {
		if !bytes.Equal(tr.got[f.Path], s.data[f.Path]) {
			t.Fatalf("%s arrived different", f.Path)
		}
	}
	if left := s.unsynced(); len(left) != 0 {
		t.Fatalf("unsynced after the round: %v", left)
	}
	if got := testutil.ToFloat64(e.m.files.WithLabelValues("stored")); got != 3 {
		t.Errorf("stored %v", got)
	}
	if e.lag() != 0 || testutil.ToFloat64(e.m.backlogFiles) != 0 {
		t.Errorf("lag %v, backlog %v after everything was sent", e.lag(), testutil.ToFloat64(e.m.backlogFiles))
	}
}

// A failure to reach the control plane ends the round; what was not sent is
// released, stays unsynced and goes in the next round.
func TestFailureEndsTheRound(t *testing.T) {
	base := time.Now().Add(-time.Minute)
	s, tr := newStore(3, base), newTransport()
	down := errors.New("connection refused")
	tr.answer = func(ref cosmicdust.FileRef, n int) (cosmicdust.Ack, error) {
		if ref.Path == s.files[1].Path && n == 1 {
			return cosmicdust.Ack{}, down
		}
		return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
	}
	e := engine(t, s, tr, prometheus.NewRegistry())
	if err := e.round(context.Background()); !errors.Is(err, down) {
		t.Fatalf("round: %v", err)
	}
	if left := s.unsynced(); !slices.Equal(left, []string{s.files[1].Path, s.files[2].Path}) {
		t.Fatalf("unsynced %v", left)
	}
	if tr.uploads[s.files[2].Path] != 0 {
		t.Fatal("the round went on after the failure")
	}
	if lag := e.lag(); lag < 55 || lag > 70 { // the oldest unsent row is ~59 s old
		t.Errorf("lag %v", lag)
	}
	if testutil.ToFloat64(e.m.failures) != 1 {
		t.Errorf("failures %v", testutil.ToFloat64(e.m.failures))
	}
	if err := e.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left := s.unsynced(); len(left) != 0 {
		t.Fatalf("unsynced after the retry: %v", left)
	}
}

// A file the control plane refuses stays unsynced but holds nothing up.
func TestRefusedFileDoesNotBlock(t *testing.T) {
	s, tr := newStore(3, time.Now()), newTransport()
	tr.answer = func(ref cosmicdust.FileRef, _ int) (cosmicdust.Ack, error) {
		if ref.Path == s.files[0].Path {
			return cosmicdust.Ack{Status: cosmicdust.AckRejected, Reason: "bad checksum"}, nil
		}
		return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
	}
	e := engine(t, s, tr, prometheus.NewRegistry())
	for range 2 {
		if err := e.round(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if left := s.unsynced(); !slices.Equal(left, []string{s.files[0].Path}) {
		t.Fatalf("unsynced %v", left)
	}
	if got := testutil.ToFloat64(e.m.files.WithLabelValues("rejected")); got != 2 {
		t.Errorf("rejected %v, want one per round", got)
	}
}

// A file being merged, or gone since the listing, is left for later.
func TestClaimedOrGoneFilesWait(t *testing.T) {
	s, tr := newStore(3, time.Now()), newTransport()
	s.claimErr[s.files[0].Path] = cosmicdust.ErrClaimed
	s.claimErr[s.files[1].Path] = fmt.Errorf("merged: %w", fs.ErrNotExist)
	e := engine(t, s, tr, prometheus.NewRegistry())
	if err := e.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tr.order, []string{s.files[2].Path}) {
		t.Fatalf("sent %v", tr.order)
	}
}

// After a lost answer the same file goes again and is a duplicate: synced.
func TestDuplicateIsSynced(t *testing.T) {
	s, tr := newStore(1, time.Now()), newTransport()
	tr.got[s.files[0].Path] = s.data[s.files[0].Path]
	e := engine(t, s, tr, prometheus.NewRegistry())
	if err := e.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.unsynced()) != 0 || testutil.ToFloat64(e.m.files.WithLabelValues("duplicate")) != 1 {
		t.Fatal("a duplicate did not mark the file synced")
	}
}

// Run retries a failed round after a backoff, not after the next interval.
func TestRunRetries(t *testing.T) {
	s, tr := newStore(2, time.Now()), newTransport()
	tr.answer = func(_ cosmicdust.FileRef, n int) (cosmicdust.Ack, error) {
		if n <= 2 {
			return cosmicdust.Ack{}, errors.New("down")
		}
		return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
	}
	const every = 300 * time.Millisecond
	e, err := New(Options{Store: s, Transport: tr, Every: every,
		Backoff: retry.Backoff{Min: 5 * time.Millisecond, Max: 10 * time.Millisecond}, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- e.Run(ctx) }()
	for len(s.unsynced()) != 0 {
		time.Sleep(2 * time.Millisecond)
		if time.Since(start) > 2*every-50*time.Millisecond {
			// Retrying at the next interval would only finish after two.
			t.Fatalf("still unsynced after %v: %v", time.Since(start), s.unsynced())
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// reconnecting is a transport that says when its connection is back.
type reconnecting struct {
	*fakeTransport
	up chan struct{}
}

func (r reconnecting) Reconnected() <-chan struct{} { return r.up }

// A connection that comes back ends the wait: the backlog goes at once, not
// after the backoff.
func TestRunSendsWhenTheConnectionIsBack(t *testing.T) {
	s, tr := newStore(2, time.Now()), newTransport()
	var down atomic.Bool
	down.Store(true)
	tr.answer = func(cosmicdust.FileRef, int) (cosmicdust.Ack, error) {
		if down.Load() {
			return cosmicdust.Ack{}, errors.New("down")
		}
		return cosmicdust.Ack{Status: cosmicdust.AckStored}, nil
	}
	r := reconnecting{fakeTransport: tr, up: make(chan struct{}, 1)}
	reg := prometheus.NewRegistry()
	e, err := New(Options{Store: s, Transport: r, Every: 10 * time.Millisecond,
		Backoff: retry.Backoff{Min: 10 * time.Millisecond, Max: time.Hour}, Logger: quiet(), Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	failures := func() float64 { return testutil.ToFloat64(e.m.failures) }
	// An outage: the backoff grows past a second.
	waitUntil(t, func() bool { return failures() >= 8 })
	// The connection is back, the receiver not yet: one more failure, and
	// the next try comes at the start of the backoff, not seconds later.
	r.up <- struct{}{}
	waitUntil(t, func() bool { return failures() >= 9 })
	down.Store(false)
	start := time.Now()
	waitUntil(t, func() bool { return len(s.unsynced()) == 0 })
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("sent %v after the receiver was back", took)
	}
	if n := testutil.ToFloat64(e.m.reconnects); n != 1 {
		t.Errorf("reconnects %v, want 1", n)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func waitUntil(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("not within 5s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func exit(id string, epoch, code int) cosmicdust.Event {
	return cosmicdust.Event{Time: time.Now(), Kind: cosmicdust.KindExit, ContainerID: id, Epoch: epoch, ExitCode: &code}
}

func TestIsUrgent(t *testing.T) {
	inferred := exit("c", 1, 1)
	inferred.Inferred = true
	oom := exit("c", 1, 137)
	oom.OOMKilled = true
	for _, tc := range []struct {
		e    cosmicdust.Event
		want bool
	}{
		{exit("c", 1, 1), true},
		{exit("c", 1, 0), false},
		{oom, true},
		{cosmicdust.Event{Kind: cosmicdust.KindOOM, ContainerID: "c"}, true},
		{cosmicdust.Event{Kind: cosmicdust.KindStart, ContainerID: "c"}, false},
		{cosmicdust.Event{Kind: cosmicdust.KindExit, ContainerID: "c"}, false}, // no exit code
		{inferred, false},
	} {
		if got := IsUrgent(tc.e); got != tc.want {
			t.Errorf("%+v: %v", tc.e, got)
		}
	}
}

// With Docker followed, Docker's exit decides for its containers, and an exit
// after a kill Docker was asked for is a stop.
func TestUrgentStopIsNotACrash(t *testing.T) {
	u := urgent(t, newTransport(), UrgentOptions{DockerNamespace: "moby"})
	ev := func(rt, action string, k cosmicdust.Kind, epoch int, code *int) cosmicdust.Event {
		return cosmicdust.Event{Runtime: rt, Namespace: "moby", Action: action, Kind: k, ContainerID: "c1", Epoch: epoch, ExitCode: code}
	}
	c143, c137 := 143, 137
	// docker stop: containerd's exit first, then the kill, Docker's exit.
	u.Push(ev(cosmicdust.RuntimeContainerd, "/tasks/exit", cosmicdust.KindExit, 1, &c143))
	u.Push(ev(cosmicdust.RuntimeDocker, "kill", cosmicdust.KindOther, 1, nil))
	u.Push(ev(cosmicdust.RuntimeDocker, "die", cosmicdust.KindExit, 1, &c143))
	if n := len(u.ch); n != 0 {
		t.Fatalf("%d queued for a docker stop", n)
	}
	// A crash in the next incarnation: Docker's exit, without a kill.
	u.Push(ev(cosmicdust.RuntimeContainerd, "/tasks/exit", cosmicdust.KindExit, 2, &c137))
	u.Push(ev(cosmicdust.RuntimeDocker, "die", cosmicdust.KindExit, 2, &c137))
	if n := len(u.ch); n != 1 {
		t.Fatalf("%d queued for a crash, want 1", n)
	}
	// An OOM kill is urgent at containerd's word.
	oom := ev(cosmicdust.RuntimeContainerd, "/tasks/exit", cosmicdust.KindExit, 3, &c137)
	oom.OOMKilled = true
	u.Push(oom)
	if n := len(u.ch); n != 2 {
		t.Fatalf("%d queued, want the OOM kill too", n)
	}
	if got := testutil.ToFloat64(u.m.result.WithLabelValues("stopped")); got != 1 {
		t.Errorf("stopped %v, want 1", got)
	}

	// Docker not followed: containerd's exit is all there is.
	u = urgent(t, newTransport(), UrgentOptions{})
	u.Push(ev(cosmicdust.RuntimeContainerd, "/tasks/exit", cosmicdust.KindExit, 1, &c143))
	if n := len(u.ch); n != 1 {
		t.Fatalf("%d queued without Docker, want 1", n)
	}
}

func urgent(t *testing.T, tr cosmicdust.Transport, opts UrgentOptions) *Urgent {
	t.Helper()
	opts.Transport, opts.Logger = tr, quiet()
	u, err := NewUrgent(opts)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// Both runtimes report the same exit: one urgent event per incarnation.
func TestUrgentOncePerIncarnation(t *testing.T) {
	tr := newTransport()
	u := urgent(t, tr, UrgentOptions{})
	u.Push(exit("c1", 1, 1))                                                // containerd
	u.Push(exit("c1", 1, 1))                                                // Docker, later
	u.Push(cosmicdust.Event{Kind: cosmicdust.KindStart, ContainerID: "c1"}) // not urgent
	u.Push(exit("c1", 2, 1))                                                // the next incarnation
	u.Push(cosmicdust.Event{Kind: cosmicdust.KindRemove, ContainerID: "c1"})
	u.Push(exit("c1", 1, 1)) // a new container under the same ID starts counting again
	if n := len(u.ch); n != 3 {
		t.Fatalf("%d queued, want 3", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		tr.mu.Lock()
		n := len(tr.urgent)
		tr.mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sent", n)
		}
	}
	cancel()
	<-done
}

func TestUrgentGivesUpAndDrops(t *testing.T) {
	tr := newTransport()
	tr.urgErr = func(int) error { return errors.New("down") }
	u := urgent(t, tr, UrgentOptions{Queue: 1, Attempts: 3, Timeout: time.Second})
	u.Push(exit("a", 1, 1))
	u.Push(exit("b", 1, 1)) // the queue holds one
	if got := testutil.ToFloat64(u.m.result.WithLabelValues("dropped")); got != 1 {
		t.Fatalf("dropped %v", got)
	}
	u.send(context.Background(), <-u.ch)
	if got := testutil.ToFloat64(u.m.result.WithLabelValues("failed")); got != 1 || len(tr.urgent) != 3 {
		t.Fatalf("failed %v after %d attempts", got, len(tr.urgent))
	}
}
