package wal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/lifecycle"
	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/internal/tail"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// The child process of TestKillNine: the tailer reading one container's log
// into the WAL, until it is killed or told to stop.
func TestMain(m *testing.M) {
	if dir := os.Getenv("COSMICDUST_CRASH_WAL"); dir != "" {
		if err := child(dir, os.Getenv("COSMICDUST_CRASH_LOG")); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type oneContainer struct {
	ref cosmicdust.ContainerRef
	ch  *lifecycle.Changes
}

func (o *oneContainer) Get(id string) (lifecycle.Container, bool) {
	return lifecycle.Container{ContainerRef: o.ref, State: cosmicdust.StateRunning}, id == o.ref.ID
}

func (o *oneContainer) Subscribe() *lifecycle.Changes {
	o.ch.Add(o.ref.ID)
	return o.ch
}

func child(dir, logPath string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.DiscardHandler)
	w, err := wal.Open(wal.Options{Dir: dir, SegmentBytes: 64 << 10, Logger: log})
	if err != nil {
		return err
	}
	tl, err := tail.New(tail.Options{
		Containers: &oneContainer{ref: cosmicdust.ContainerRef{ID: "c", LogPath: logPath, LogFormat: "json-file"}, ch: lifecycle.NewChanges()},
		Sink:       w,
		Resume:     w.Position,
		Poll:       5 * time.Millisecond,
		PollMax:    20 * time.Millisecond,
		Batch:      64,
		Logger:     log,
	})
	if err != nil {
		return err
	}
	go func() { _ = w.Run(ctx) }()
	if err := tl.Run(ctx); err != nil {
		return err
	}
	return w.Close()
}

// fakeDocker writes json-file lines and rotates by rename, as dockerd does.
type fakeDocker struct {
	path string
	seq  int
	f    *os.File
	size int
	rot  int
}

func (d *fakeDocker) write(t *testing.T, n int) {
	for range n {
		d.seq++
		b, _ := json.Marshal(map[string]any{"log": fmt.Sprintf("line-%d\n", d.seq), "stream": "stdout", "time": time.Now().UTC()})
		b = append(b, '\n')
		if _, err := d.f.Write(b); err != nil {
			t.Fatal(err)
		}
		d.size += len(b)
		if d.size > 32<<10 { // rotate at 32 KiB, keep every file: this test is about crashes, not retention
			d.rot++
			_ = d.f.Close()
			for i := d.rot; i >= 1; i-- {
				_ = os.Rename(fmt.Sprintf("%s.%d", d.path, i), fmt.Sprintf("%s.%d", d.path, i+1))
			}
			if err := os.Rename(d.path, d.path+".1"); err != nil {
				t.Fatal(err)
			}
			var err error
			if d.f, err = os.OpenFile(d.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
				t.Fatal(err)
			}
			d.size = 0
		}
	}
}

// The claim of the store: data and resume position in one fsync give
// a restart without gaps and without duplicates. The writer keeps writing and
// rotating while the agent is killed with SIGKILL again and again.
func TestKillNine(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns and kills processes for a few seconds")
	}
	root := t.TempDir()
	walDir, logPath := filepath.Join(root, "wal"), filepath.Join(root, "c-json.log")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	d := &fakeDocker{path: logPath, f: f}
	defer func() { _ = d.f.Close() }()

	start := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run", "^$")
		cmd.Env = append(os.Environ(), "COSMICDUST_CRASH_WAL="+walDir, "COSMICDUST_CRASH_LOG="+logPath)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}

	const total = 20000
	seed := uint64(time.Now().UnixNano())
	if v, err := strconv.ParseUint(os.Getenv("COSMICDUST_CRASH_SEED"), 10, 64); err == nil {
		seed = v // to replay a failure
	}
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed>>32))
	kills := 0
	cmd := start()
	for d.seq < total {
		d.write(t, min(400, total-d.seq))
		time.Sleep(20 * time.Millisecond)
		if rng.IntN(4) == 0 {
			_ = cmd.Process.Kill() // SIGKILL: no flush, no close, no deferred anything
			_ = cmd.Wait()
			kills++
			cmd = start()
		}
	}
	time.Sleep(1500 * time.Millisecond) // let the last run catch up
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the last run did not stop cleanly: %v", err)
	}

	w, err := wal.Open(wal.Options{Dir: walDir, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	var got []int
	for _, seg := range w.Sealed() {
		if err := w.Read(seg, func(b *wal.Block) error {
			for _, r := range b.Logs {
				n, err := strconv.Atoi(strings.TrimPrefix(r.Line, "line-"))
				if err != nil {
					return fmt.Errorf("not a numbered line: %q", r.Line)
				}
				got = append(got, n)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i, n := range got {
		if n != i+1 {
			t.Fatalf("after %d kills: line %d is line-%d (lines read %d of %d): a gap or a duplicate", kills, i, n, len(got), total)
		}
	}
	if len(got) != total {
		t.Fatalf("after %d kills: %d of %d lines", kills, len(got), total)
	}
	t.Logf("%d lines exactly once through %d kills and %d rotations", total, kills, d.rot)
}
