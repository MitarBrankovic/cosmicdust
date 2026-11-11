//go:build integration

// The tailer against the node's dockerd, through the lifecycle tracker. Docker's
// log files are root's:
//
//	sudo -E env PATH=$PATH go test -tags integration ./internal/tail/
package tail

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/lifecycle"
	dockersource "github.com/c12s/cosmicdust/internal/runtime/docker"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

type partsSink struct {
	mu    sync.Mutex
	recs  []cosmicdust.Record // Line copied
	added []cosmicdust.Record
}

func (s *partsSink) Add(recs []cosmicdust.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		r.Line = append([]byte(nil), r.Line...)
		s.added = append(s.added, r)
	}
	return nil
}

func (s *partsSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append(s.recs, s.added...)
	s.added = s.added[:0]
	return nil
}

func (s *partsSink) of(id string) []cosmicdust.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []cosmicdust.Record
	for _, r := range s.recs {
		if r.ContainerID == id {
			out = append(out, r)
		}
	}
	return out
}

func TestIntegrationTailer(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Docker's log files are root's")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("no docker daemon:", err)
	}
	log := slog.New(slog.DiscardHandler)
	src, err := dockersource.New(dockersource.Options{Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	tr, err := lifecycle.New(lifecycle.Options{Sources: []lifecycle.Source{{EventSource: src}}, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	sink := &partsSink{}
	tl, err := New(Options{Containers: tr, Sink: sink, Poll: 50 * time.Millisecond, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { _ = tr.Run(ctx) })
	wg.Go(func() { _ = tl.Run(ctx) })
	defer func() { cancel(); wg.Wait() }()

	const name, lines = "it-tail", 3000
	_ = exec.Command("docker", "rm", "-f", name).Run()
	// The other containers on the machine are followed too: count from them.
	base := settled(t, tl)
	defer func() { _ = exec.Command("docker", "rm", "-f", name).Run() }()
	// The container writes once the tailer holds its log file, still empty,
	// so that the rotations happen while the file is followed: written before
	// the first read, every file is read as on first sight and no rotation is
	// followed (seen once on a fast runner).
	script := fmt.Sprintf(`while [ ! -e /tmp/go ]; do sleep 0.1; done; i=1; while [ $i -le %d ]; do echo line-$i; i=$((i+1)); done; head -c 40000 /dev/zero | tr '\0' x; echo; sleep 60`, lines)
	out, err := exec.Command("docker", "run", "-d", "--name", name,
		"--log-opt", "max-size=20k", "--log-opt", "max-file=30", "alpine", "sh", "-c", script).Output()
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	ids := strings.Fields(string(out))
	id := ids[len(ids)-1]

	deadline := time.Now().Add(30 * time.Second)
	for tl.Stats().open.Load() <= base {
		if time.Now().After(deadline) {
			t.Fatal("the container is not followed after 30 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Two passes later the follow has opened the file.
	for passes := tl.Stats().passes.Load(); tl.Stats().passes.Load() < passes+2; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no pass over the followed file after 30 s")
		}
	}
	if err := exec.Command("docker", "exec", name, "touch", "/tmp/go").Run(); err != nil {
		t.Fatalf("docker exec: %v", err)
	}
	var recs []cosmicdust.Record
	for len(recs) < lines+1 {
		if time.Now().After(deadline) {
			// Enough to tell a slow discovery from a lost file next time.
			st := tl.Stats()
			c, known := tr.Get(id)
			t.Fatalf("%d of %d messages after 30 s; container known %v (%s, log %q); files %d, passes %d, "+
				"rotations %d, lost files %d, read errors %d, store waits %d",
				len(recs), lines+1, known, c.State, c.LogPath, st.open.Load(), st.passes.Load(),
				st.rotations.Load(), st.lostFiles.Load(), st.readErrors.Load(), st.storeWaits.Load())
		}
		time.Sleep(100 * time.Millisecond)
		recs = sink.of(id)
	}
	for i, r := range recs[:lines] {
		if want := fmt.Sprintf("line-%d", i+1); string(r.Line) != want {
			t.Fatalf("message %d is %q, want %q", i, r.Line, want)
		}
	}
	long := recs[lines]
	if len(long.Line) != 40000 || long.Parts != 3 || long.Truncated {
		t.Fatalf("the long line: %d bytes in %d parts, truncated %v", len(long.Line), long.Parts, long.Truncated)
	}
	if len(recs) != lines+1 {
		t.Fatalf("%d messages, want %d", len(recs), lines+1)
	}
	if tl.Stats().rotations.Load() == 0 {
		t.Fatal("no rotation followed; the test did not test that")
	}
	t.Logf("%d messages, %d rotations, %d slow decodes", len(recs), tl.Stats().rotations.Load(), tl.Stats().slowDecodes.Load())

	// Stopped: the rest is read and the file let go. The wait counts from the
	// stop, which can itself be slow on a busy daemon.
	stopStart := time.Now()
	if err := exec.Command("docker", "stop", "-t", "1", name).Run(); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	for tl.Stats().open.Load() > base {
		if time.Since(stopped) > 10*time.Second {
			st := tl.Stats()
			c, known := tr.Get(id)
			t.Fatalf("the stopped container's log is still followed 10 s after docker stop (which took %s); "+
				"container known %v (%s); files %d, passes %d, read errors %d",
				stopped.Sub(stopStart).Round(time.Millisecond), known, c.State, st.open.Load(), st.passes.Load(),
				st.readErrors.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// settled waits until the number of followed logs has held for a second and
// returns it: the containers already on the machine, read or being read.
func settled(t *testing.T, tl *Tailer) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	n, since := tl.Stats().open.Load(), time.Now()
	for time.Since(since) < time.Second {
		if time.Now().After(deadline) {
			t.Fatal("the followed logs did not settle in 30 s")
		}
		time.Sleep(20 * time.Millisecond)
		if m := tl.Stats().open.Load(); m != n {
			n, since = m, time.Now()
		}
	}
	return n
}
