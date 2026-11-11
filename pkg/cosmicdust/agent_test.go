package cosmicdust

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fake is a component whose behaviour a test scripts.
type fake struct {
	name    string
	started chan struct{}
	fail    error         // returned at once, before ctx ends, if set
	stopErr error         // returned when ctx ends
	hang    time.Duration // how long to keep running after ctx ends
	stopped func(name string)
}

func newFake(name string) *fake { return &fake{name: name, started: make(chan struct{})} }

func (f *fake) Name() string { return f.name }

func (f *fake) Run(ctx context.Context) error {
	close(f.started)
	if f.fail != nil {
		return f.fail
	}
	<-ctx.Done()
	time.Sleep(f.hang)
	if f.stopped != nil {
		f.stopped(f.name)
	}
	if f.stopErr != nil {
		return f.stopErr
	}
	return ctx.Err()
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newAgent(t *testing.T, opts Options) *Agent {
	t.Helper()
	if opts.Node == "" {
		opts.Node = "node-1"
	}
	opts.Logger = quiet()
	a, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewRejectsBadOptions(t *testing.T) {
	cases := map[string]Options{
		"no node":    {Components: []Component{newFake("a")}},
		"nil":        {Node: "n", Components: []Component{nil}},
		"no name":    {Node: "n", Components: []Component{newFake("")}},
		"duplicates": {Node: "n", Components: []Component{newFake("a"), newFake("a")}},
	}
	for name, opts := range cases {
		if _, err := New(opts); err == nil {
			t.Errorf("%s: New accepted it", name)
		}
	}
}

func TestStopsInOrderOnCancel(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}
	comps := []*fake{newFake("tailer"), newFake("store"), newFake("http")}
	// The first one takes a while to stop; the rest must still wait for it.
	comps[0].hang = 50 * time.Millisecond
	var list []Component
	for _, c := range comps {
		c.stopped = record
		list = append(list, c)
	}
	a := newAgent(t, Options{Components: list})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	for _, c := range comps {
		<-c.started
	}
	if err := a.Health(); err != nil {
		t.Fatalf("health while running: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("clean stop returned %v", err)
	}
	if got := strings.Join(order, ","); got != "tailer,store,http" {
		t.Fatalf("stop order %s, want tailer,store,http", got)
	}
	if err := a.Health(); err == nil {
		t.Fatal("health is nil after the stop")
	}
}

func TestComponentFailureStopsTheRest(t *testing.T) {
	boom := errors.New("boom")
	ok1, bad, ok2 := newFake("a"), newFake("b"), newFake("c")
	bad.fail = boom
	a := newAgent(t, Options{Components: []Component{ok1, bad, ok2}})

	err := a.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want it to wrap boom", err)
	}
	if !strings.Contains(err.Error(), "b: boom") {
		t.Fatalf("error %q does not name the component", err)
	}
}

func TestEarlyNilReturnIsAFailure(t *testing.T) {
	quitter := &earlyNil{}
	a := newAgent(t, Options{Components: []Component{quitter, newFake("other")}})
	err := a.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "quitter") {
		t.Fatalf("Run returned %v, want an error naming quitter", err)
	}
}

type earlyNil struct{}

func (earlyNil) Name() string                  { return "quitter" }
func (earlyNil) Run(ctx context.Context) error { return nil }

func TestStopErrorIsReported(t *testing.T) {
	c := newFake("store")
	c.stopErr = errors.New("flush failed")
	a := newAgent(t, Options{Components: []Component{c}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-c.started
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("Run returned %v, want the stop error", err)
	}
}

func TestShutdownTimeout(t *testing.T) {
	stuck, after := newFake("stuck"), newFake("after")
	stuck.hang = time.Second
	a := newAgent(t, Options{Components: []Component{stuck, after}, ShutdownTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-stuck.started
	<-after.started
	start := time.Now()
	cancel()
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "stuck did not stop") {
		t.Fatalf("Run returned %v, want a timeout naming stuck", err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("Run waited %s past a 50ms timeout", took)
	}
}

func TestRunsOnce(t *testing.T) {
	a := newAgent(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Run(ctx); err == nil {
		t.Fatal("second Run succeeded")
	}
}

func TestComponentUpMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newFake("tailer")
	a := newAgent(t, Options{Components: []Component{c}, Registerer: reg})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-c.started

	want := `
# HELP cosmicdust_agent_component_up 1 while a component runs, 0 once it has returned.
# TYPE cosmicdust_agent_component_up gauge
cosmicdust_agent_component_up{component="tailer"} %s
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(strings.Replace(want, "%s", "1", 1))); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
	if err := testutil.GatherAndCompare(reg, strings.NewReader(strings.Replace(want, "%s", "0", 1))); err != nil {
		t.Fatal(err)
	}
}

func TestHealthBeforeRun(t *testing.T) {
	a := newAgent(t, Options{})
	if err := a.Health(); err == nil {
		t.Fatal("health is nil before Run")
	}
}

func TestAck(t *testing.T) {
	for s, want := range map[AckStatus]bool{AckStored: true, AckDuplicate: true, AckRejected: false, 0: false} {
		if got := (Ack{Status: s}).Stored(); got != want {
			t.Errorf("Ack{%d}.Stored() = %v", s, got)
		}
	}
}
