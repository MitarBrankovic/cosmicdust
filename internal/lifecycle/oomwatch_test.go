package lifecycle

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// fakeCgroup makes the systemd-driver cgroup of a Docker container under root
// and returns its memory.events.
func fakeCgroup(t *testing.T, root, id string, kills int) string {
	t.Helper()
	dir := filepath.Join(root, "system.slice", "docker-"+id+".scope")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "memory.events")
	setKills(t, p, kills)
	return p
}

func setKills(t *testing.T, path string, kills int) {
	t.Helper()
	n := strconv.Itoa(kills)
	data := []byte("low 0\nhigh 0\nmax 3\noom " + n + "\noom_kill " + n + "\n")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func nextNote(t *testing.T, notes <-chan oomNote) oomNote {
	t.Helper()
	select {
	case n := <-notes:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("no note within 5s")
		return oomNote{}
	}
}

func TestOOMWatch(t *testing.T) {
	root := t.TempDir()
	w, err := newOOMWatch(cgroups{root: root}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	notes := make(chan oomNote, 8)
	done := make(chan error, 1)
	go func() { done <- w.run(ctx, notes) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	// Watched from incarnation 1: a kill is a note, the same count again is
	// not.
	p1 := fakeCgroup(t, root, "c1", 0)
	if _, ok := w.add("c1", "moby", 1); ok {
		t.Fatal("a note for a counter at zero")
	}
	setKills(t, p1, 1)
	if n := nextNote(t, notes); n.id != "c1" || n.epoch != 1 || n.kills != 1 {
		t.Fatalf("note %+v", n)
	}
	setKills(t, p1, 1)
	setKills(t, p1, 2)
	if n := nextNote(t, notes); n.kills != 2 {
		t.Fatalf("note %+v, want the second kill", n)
	}

	// A kill before the watch, when the event that asked for it was taken
	// late: it is the incarnation's.
	p2 := fakeCgroup(t, root, "c2", 1)
	if n, ok := w.add("c2", "moby", 1); !ok || n.kills != 1 || n.epoch != 1 {
		t.Fatalf("a kill before the watch: %+v, %v", n, ok)
	}
	// Watched already, for this incarnation: nothing to add.
	if _, ok := w.add("c2", "moby", 1); ok {
		t.Fatal("added twice")
	}
	// The same cgroup for a later incarnation: what it counted so far is
	// the earlier one's.
	if _, ok := w.add("c2", "moby", 2); ok {
		t.Fatal("the earlier incarnation's kill reported for the later one")
	}
	setKills(t, p2, 2)
	if n := nextNote(t, notes); n.id != "c2" || n.epoch != 2 || n.kills != 2 {
		t.Fatalf("note %+v", n)
	}

	// No cgroup, no watch.
	if _, ok := w.add("c3", "moby", 1); ok || w.len() != 2 {
		t.Fatalf("watches %d, want 2", w.len())
	}

	// Forgotten: its kills are not news.
	w.forget("c1")
	setKills(t, p1, 3)
	setKills(t, p2, 3)
	if n := nextNote(t, notes); n.id != "c2" {
		t.Fatalf("note %+v from a forgotten watch", n)
	}

	// A removed cgroup takes its watch with it.
	if err := os.RemoveAll(filepath.Dir(p2)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w.len() == 0 })
}

// The tracker takes the parent's word when the container's cgroup is gone by
// the exit.
func TestTrackerTakesTheParentsWord(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "system.slice", "memory.events")
	p := fakeCgroup(t, root, "c9", 0)
	setKills(t, parent, 2)
	c := newFake(containerd)
	h := start(t, Options{Sources: []Source{{EventSource: c}}, CgroupRoot: root})
	h.reconciled(containerd)
	now := time.Now()
	c.events <- cosmicdust.Event{Time: now, Runtime: containerd, Namespace: "moby", Action: "/tasks/create", ContainerID: "c9"}
	c.events <- cosmicdust.Event{Time: now.Add(time.Millisecond), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindStart, ContainerID: "c9"}
	h.until(isKind(cosmicdust.KindStart, "c9"))
	if err := os.WriteFile(p, []byte("max 3\noom 0\noom_kill 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		h.tr.mu.RLock()
		w := h.tr.oom
		h.tr.mu.RUnlock()
		w.mu.Lock()
		defer w.mu.Unlock()
		wt := w.watches[w.byID["c9"]]
		return wt != nil && !wt.limitAt.IsZero()
	})
	if err := os.RemoveAll(filepath.Dir(p)); err != nil {
		t.Fatal(err)
	}
	setKills(t, parent, 3)
	c.events <- cosmicdust.Event{Time: now.Add(time.Second), Runtime: containerd, Namespace: "moby", Kind: cosmicdust.KindExit, ContainerID: "c9", ExitCode: code(137)}
	if e := h.until(isKind(cosmicdust.KindExit, "c9")); !e.OOMKilled {
		t.Fatal("the parent's kill is not on the exit")
	}
	if n := testutil.ToFloat64(h.tr.m.oomKills.WithLabelValues("parent")); n != 1 {
		t.Fatalf("oom_kills_total{read=parent} %v, want 1", n)
	}
}

// A kill the container's own counter was gone before it could tell is read
// from the parent's, if the container was at its limit just before.
func TestOOMWatchParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "system.slice", "memory.events")
	w, err := newOOMWatch(cgroups{root: root}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx, make(chan oomNote, 8)) }()

	p := fakeCgroup(t, root, "c1", 0)
	setKills(t, parent, 4) // kills of other containers before
	if _, ok := w.add("c1", "moby", 1); ok {
		t.Fatal("a note for a counter at zero")
	}
	if _, ok := w.parentOOM("c1", 1); ok {
		t.Fatal("never at its limit, yet the parent's kills were taken for it")
	}

	// At its limit: max grows, no kill yet.
	if err := os.WriteFile(p, []byte("max 7\noom 0\noom_kill 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.watches[w.byID["c1"]].parentKills == 4
	})
	// Killed, and the cgroup gone before the next notification.
	if err := os.RemoveAll(filepath.Dir(p)); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.parentOOM("c1", 1); ok {
		t.Fatal("the parent's counter did not grow, yet a kill was read from it")
	}
	setKills(t, parent, 5)
	if _, ok := w.parentOOM("c1", 2); ok {
		t.Fatal("taken for another incarnation")
	}
	if n, ok := w.parentOOM("c1", 1); !ok || n != 5 {
		t.Fatalf("parent's kill not read: %d, %v", n, ok)
	}
	if _, ok := w.parentOOM("c1", 1); ok {
		t.Fatal("the same kill taken twice")
	}
}

func TestTrackerWatchesTheCgroupForOOM(t *testing.T) {
	root := t.TempDir()
	p := fakeCgroup(t, root, "c9", 0)
	c := newFake(containerd)
	h := start(t, Options{Sources: []Source{{EventSource: c}}, CgroupRoot: root})
	h.reconciled(containerd)
	now := time.Now()
	ev := func(at time.Duration, action string, kind cosmicdust.Kind, exit *int) cosmicdust.Event {
		return cosmicdust.Event{Time: now.Add(at), Runtime: containerd, Namespace: "moby", Action: action, Kind: kind,
			ContainerID: "c9", ExitCode: exit}
	}

	// The cgroup is watched from the task's create. The OOM killer hits,
	// no runtime says so, and the scope is gone before the exit.
	c.events <- ev(0, "/tasks/create", cosmicdust.KindOther, nil)
	c.events <- ev(time.Millisecond, "/tasks/start", cosmicdust.KindStart, nil)
	h.until(isKind(cosmicdust.KindStart, "c9"))
	setKills(t, p, 1)
	if e := h.until(isKind(cosmicdust.KindOOM, "c9")); e.Action != "cgroup/oom_kill" || e.Epoch != 1 || e.State != cosmicdust.StateRunning {
		t.Fatalf("OOM event %+v", e)
	}
	if err := os.RemoveAll(filepath.Dir(p)); err != nil {
		t.Fatal(err)
	}
	c.events <- ev(time.Second, "/tasks/exit", cosmicdust.KindExit, code(137))
	if e := h.until(isKind(cosmicdust.KindExit, "c9")); !e.OOMKilled {
		t.Fatal("the watch's kill is not on the exit")
	}
	if n := testutil.ToFloat64(h.tr.m.oomKills.WithLabelValues("watch")); n != 1 {
		t.Fatalf("oom_kills_total{read=watch} %v, want 1", n)
	}

	// A restart: the next task's create is the next incarnation's, and a
	// kill read before its start is still that incarnation's.
	p = fakeCgroup(t, root, "c9", 0)
	c.events <- ev(2*time.Second, "/tasks/create", cosmicdust.KindOther, nil)
	h.until(func(e cosmicdust.Event) bool { return e.Action == "/tasks/create" && e.Epoch == 1 })
	setKills(t, p, 1)
	if e := h.until(isKind(cosmicdust.KindOOM, "c9")); e.Epoch != 2 || e.State != cosmicdust.StateRunning {
		t.Fatalf("OOM event %+v, want epoch 2", e)
	}
	c.events <- ev(2*time.Second+time.Millisecond, "/tasks/start", cosmicdust.KindStart, nil)
	c.events <- ev(3*time.Second, "/tasks/exit", cosmicdust.KindExit, code(137))
	if e := h.until(isKind(cosmicdust.KindExit, "c9")); !e.OOMKilled || e.Epoch != 2 {
		t.Fatalf("exit %+v, want an OOM kill in epoch 2", e)
	}

	// kill -9: the same code, the counter does not move.
	fakeCgroup(t, root, "c9", 0)
	c.events <- ev(4*time.Second, "/tasks/create", cosmicdust.KindOther, nil)
	c.events <- ev(4*time.Second+time.Millisecond, "/tasks/start", cosmicdust.KindStart, nil)
	c.events <- ev(5*time.Second, "/tasks/exit", cosmicdust.KindExit, code(137))
	if e := h.until(isKind(cosmicdust.KindExit, "c9")); e.OOMKilled || e.Epoch != 3 {
		t.Fatalf("exit %+v: kill -9 marked as an OOM kill", e)
	}
	c.events <- ev(6*time.Second, "/containers/delete", cosmicdust.KindRemove, nil)
	h.until(isKind(cosmicdust.KindRemove, "c9"))
	waitFor(t, func() bool {
		h.tr.mu.RLock()
		defer h.tr.mu.RUnlock()
		return h.tr.oom.len() == 0
	})
}
