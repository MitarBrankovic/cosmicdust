package node_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/servertest"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
	"github.com/c12s/cosmicdust/pkg/node"
)

// fakeRuntime plays a runtime with one running container, whose json-file log
// the test writes, and passes on the events a test pushes.
type fakeRuntime struct {
	c    cosmicdust.ContainerState
	push chan cosmicdust.Event
}

func (f *fakeRuntime) Runtime() string { return "fake" }

func (f *fakeRuntime) Watch(ctx context.Context, out chan<- cosmicdust.Event) error {
	mark := cosmicdust.Event{Time: time.Now(), Runtime: "fake", Action: "resync", Kind: cosmicdust.KindResync,
		Fields: map[string]string{"reason": "start"}}
	select {
	case out <- mark:
	case <-ctx.Done():
		return nil
	}
	for {
		select {
		case e := <-f.push:
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

func (f *fakeRuntime) List(context.Context) (cosmicdust.Listing, error) {
	return cosmicdust.Listing{At: time.Now(), Containers: []cosmicdust.ContainerState{f.c}}, nil
}

func (f *fakeRuntime) Describe(_ context.Context, id string) (cosmicdust.ContainerState, error) {
	if id != f.c.ID {
		return cosmicdust.ContainerState{}, errors.New("no such container")
	}
	return f.c, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// oneContainer writes a json-file log of lines for container c1 and returns a
// runtime that runs it.
func oneContainer(t *testing.T, dir string, lines int) *fakeRuntime {
	t.Helper()
	logPath := filepath.Join(dir, "c1-json.log")
	var b strings.Builder
	start := time.Now().Add(-time.Minute).UTC()
	for i := range lines {
		ts := start.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)
		fmt.Fprintf(&b, `{"log":"line %d\n","stream":"stdout","time":"%s"}`+"\n", i, ts)
	}
	if err := os.WriteFile(logPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return &fakeRuntime{push: make(chan cosmicdust.Event, 4), c: cosmicdust.ContainerState{
		ContainerRef: cosmicdust.ContainerRef{ID: "c1", Name: "web", Image: "nginx:1", Runtime: "fake",
			Namespace: "default", LogPath: logPath, LogFormat: "json-file"},
		State:     cosmicdust.StateRunning,
		StartedAt: start.Add(-time.Second),
	}}
}

// TestNode runs the whole node side on a fake runtime: lines a container
// wrote reach a Parquet file the sync engine would see.
func TestNode(t *testing.T) {
	dir := t.TempDir()
	const lines = 100
	rt := oneContainer(t, dir, lines)

	reg := prometheus.NewRegistry()
	n, err := node.New(node.Options{
		DataDir:    filepath.Join(dir, "data"),
		Sources:    []node.Source{{EventSource: rt}},
		WAL:        node.WALOptions{SegmentAge: 100 * time.Millisecond},
		Store:      node.StoreOptions{ConvertEvery: 100 * time.Millisecond, Reserve: -1, Quota: -1},
		Logger:     quiet(),
		Registerer: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Health(); err == nil {
		t.Error("healthy before Run")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	var rows int64
	deadline := time.Now().Add(20 * time.Second)
	for rows < lines {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d rows in the store after 20 s", rows, lines)
		}
		time.Sleep(50 * time.Millisecond)
		refs, err := n.Store().Unsynced(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows = 0
		for _, r := range refs {
			if r.Table == "logs" {
				rows += r.Rows
			}
		}
	}
	if rows != lines {
		t.Errorf("%d rows in the store, want %d", rows, lines)
	}
	if err := n.Health(); err != nil {
		t.Errorf("Health: %v", err)
	}
	if got := value(t, reg, "cosmicdust_agent_tail_messages_total"); got != lines {
		t.Errorf("cosmicdust_agent_tail_messages_total = %v, want %d", got, lines)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	id, err := os.ReadFile(filepath.Join(dir, "data", "node-id"))
	if err != nil || strings.TrimSpace(string(id)) != n.ID() {
		t.Errorf("node-id file %q (%v), node %q", id, err, n.ID())
	}
}

// TestNodeSyncs adds a control plane: the files reach it and are synced, and a
// container that dies with an error is reported at once.
func TestNodeSyncs(t *testing.T) {
	dir := t.TempDir()
	rt := oneContainer(t, dir, 50)
	cp, err := servertest.Open(filepath.Join(dir, "cp"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Close() }()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := servertest.NewGRPCServer(cp)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	n, err := node.New(node.Options{
		DataDir: filepath.Join(dir, "data"),
		Sources: []node.Source{{EventSource: rt}},
		WAL:     node.WALOptions{SegmentAge: 100 * time.Millisecond},
		Store:   node.StoreOptions{ConvertEvery: 100 * time.Millisecond, Reserve: -1, Quota: -1},
		Sync:    &node.SyncOptions{URL: "grpc://" + lis.Addr().String(), Every: 100 * time.Millisecond},
		Logger:  quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	code := 1
	rt.push <- cosmicdust.Event{Time: time.Now(), Runtime: "fake", Namespace: "default", Action: "die",
		Kind: cosmicdust.KindExit, ContainerID: "c1", ExitCode: &code}

	deadline := time.Now().Add(20 * time.Second)
	for {
		refs, err := n.Store().Unsynced(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if cp.Stats.FilesStored.Load() >= 2 && len(refs) == 0 && cp.Stats.Events.Load() == 1 {
			break // a logs file and an events file, both synced; the urgent event
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 20 s: %d files at the control plane, %d unsynced, %d urgent events",
				cp.Stats.FilesStored.Load(), len(refs), cp.Stats.Events.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// value reads one counter from the registry.
func value(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name && len(mf.GetMetric()) == 1 {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("%s is not registered", name)
	return 0
}

func TestNewChecks(t *testing.T) {
	if _, err := node.New(node.Options{Docker: &node.DockerOptions{}}); err == nil {
		t.Error("New without DataDir succeeded")
	}
	if _, err := node.New(node.Options{DataDir: t.TempDir()}); err == nil {
		t.Error("New without a runtime succeeded")
	}
	_, err := node.New(node.Options{DataDir: t.TempDir(), Sources: []node.Source{{}}, Logger: quiet()})
	if err == nil {
		t.Error("New with a nil source succeeded")
	}
}

// TestCloseUnrun releases a node that never ran: its WAL can be opened again.
func TestCloseUnrun(t *testing.T) {
	dir := t.TempDir()
	opts := node.Options{DataDir: dir, Sources: []node.Source{{EventSource: &fakeRuntime{}}}, Logger: quiet()}
	n, err := node.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	n2, err := node.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if n2.ID() != n.ID() {
		t.Errorf("node id changed: %q, then %q", n.ID(), n2.ID())
	}
	_ = n2.Close()
}
