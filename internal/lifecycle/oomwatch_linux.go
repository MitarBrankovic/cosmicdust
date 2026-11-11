package lifecycle

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Reading the counter at the exit is too late with the systemd driver, whose
// scope is gone by then, and the runtimes' OOM events can be missing: of
// 400 containers killed within milliseconds of their start, containerd - and
// Docker, which hears it from containerd - missed 9, and one such miss failed
// CI. So the tracker watches memory.events of every live incarnation with
// inotify. The kernel marks the file modified
// when one of its counters grows, and the counter is read at that moment,
// while the cgroup still exists.
//
// A watch is added at the first word of a live incarnation. containerd's
// /tasks/create is the earliest: the task's cgroup exists, its process has
// not started. The kernel keeps a watch for as long as it is not removed, so
// it is removed with the next incarnation's, which has a cgroup of its own,
// and with the container.
//
// The kernel notifies a file at most every ~10 ms and holds back what changes
// in between. A container that reaches its limit is notified at once (max);
// the kill that follows a few milliseconds later waits for the next
// notification, and a small container is gone by then, its cgroup with it:
// the watch missed 29 of 100 such kills so. So when a read shows the
// container at its limit, the watch also notes the parent cgroup's oom_kill,
// which counts the kills of all its children and outlives them; if the
// container's cgroup is gone when its exit is reported and the parent's
// counter has grown since, the kill was its (parentOOM).

// oomWatch is one inotify instance for the memory.events files of all
// watched containers.
type oomWatch struct {
	cg  cgroups
	log *slog.Logger
	f   *os.File // the instance, non-blocking: closing it ends run

	mu      sync.Mutex
	watches map[int32]*watched // by watch descriptor
	byID    map[string]int32
	gone    map[string]*watched // cgroups gone a moment ago at their limit, for parentOOM
}

// watched is the cgroup of one incarnation.
type watched struct {
	id     string
	path   string    // its memory.events
	parent string    // the parent cgroup's
	epoch  int       // the incarnation
	seen   int64     // oom_kill already reported, or there before the incarnation
	last   memEvents // as last read

	limitAt     time.Time // last read at its limit
	parentKills int64     // the parent's oom_kill then
}

// oomNote is a grown oom_kill counter: the OOM killer hit incarnation epoch.
type oomNote struct {
	id    string
	epoch int
	kills int64
	at    time.Time
}

func newOOMWatch(cg cgroups, log *slog.Logger) (*oomWatch, error) {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("inotify: %w", err)
	}
	return &oomWatch{
		cg:      cg,
		log:     log,
		f:       os.NewFile(uintptr(fd), "inotify"),
		watches: map[int32]*watched{},
		byID:    map[string]int32{},
		gone:    map[string]*watched{},
	}, nil
}

// add watches the cgroup of a container's incarnation epoch and reads the
// counter at once: the event that asked for the watch can come after the
// process was killed, within milliseconds of its start. The note is returned
// rather than sent, because the caller is the loop that takes the notes. A
// cgroup watched already for an earlier incarnation is the same cgroup still
// there; what it counted is that incarnation's.
func (w *oomWatch) add(id, namespace string, epoch int) (oomNote, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if wt := w.watches[w.byID[id]]; wt != nil && wt.id == id && wt.epoch >= epoch {
		return oomNote{}, false
	}
	path, wd, ok := w.watch(id, namespace)
	if !ok {
		return oomNote{}, false
	}
	if old, ok := w.byID[id]; ok && old != wd {
		w.remove(old) // the previous incarnation's cgroup
	}
	w.byID[id] = wd
	wt := w.watches[wd]
	if wt != nil && wt.id == id {
		// The same cgroup for a later incarnation: what it counted
		// belongs to the earlier one.
		wt.epoch, wt.limitAt = epoch, time.Time{}
		if ev, err := readMemEvents(wt.path); err == nil {
			wt.seen, wt.last = ev.oomKill, ev
		}
		return oomNote{}, false
	}
	wt = &watched{id: id, path: path, epoch: epoch,
		parent: filepath.Join(filepath.Dir(filepath.Dir(path)), "memory.events")}
	w.watches[wd] = wt
	w.log.Debug("cgroup watched", "container", short(id), "epoch", epoch, "path", path)
	return w.read(wt, time.Now())
}

// watch adds an inotify watch on the memory.events of a container's cgroup,
// wherever its runtime and driver put it.
func (w *oomWatch) watch(id, namespace string) (path string, wd int32, ok bool) {
	for _, p := range w.cg.eventFiles(id, namespace) {
		var n int
		var err error
		if !w.control(func(fd int) { n, err = syscall.InotifyAddWatch(fd, p, syscall.IN_MODIFY) }) {
			return "", 0, false // closed
		}
		if err == nil {
			return p, int32(n), true
		}
	}
	return "", 0, false
}

// forget stops watching a container.
func (w *oomWatch) forget(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if wd, ok := w.byID[id]; ok {
		w.remove(wd)
	}
	delete(w.gone, id)
}

func (w *oomWatch) remove(wd int32) {
	if wt := w.watches[wd]; wt != nil && w.byID[wt.id] == wd {
		delete(w.byID, wt.id)
	}
	delete(w.watches, wd)
	// The kernel answers with IN_IGNORED, for a descriptor no longer known.
	w.control(func(fd int) { _, _ = syscall.InotifyRmWatch(fd, uint32(wd)) })
}

// control runs f with the instance's descriptor; false once it is closed.
func (w *oomWatch) control(f func(fd int)) bool {
	rc, err := w.f.SyscallConn()
	if err != nil {
		return false
	}
	return rc.Control(func(fd uintptr) { f(int(fd)) }) == nil
}

func (w *oomWatch) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.watches)
}

// run reads the instance until ctx ends and sends the counters that grew.
func (w *oomWatch) run(ctx context.Context, out chan<- oomNote) error {
	stop := context.AfterFunc(ctx, func() { _ = w.f.Close() })
	defer stop()
	buf := make([]byte, 4096)
	var notes []oomNote
	for {
		n, err := w.f.Read(buf)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return nil
			}
			return err
		}
		notes = w.events(buf[:n], notes[:0])
		for _, note := range notes {
			select {
			case out <- note:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// events reads the counters of the cgroups the events in b are about.
func (w *oomWatch) events(b []byte, notes []oomNote) []oomNote {
	at := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(b) >= syscall.SizeofInotifyEvent {
		wd := int32(binary.NativeEndian.Uint32(b[0:]))
		mask := binary.NativeEndian.Uint32(b[4:])
		size := syscall.SizeofInotifyEvent + int(binary.NativeEndian.Uint32(b[12:]))
		if size > len(b) {
			break
		}
		b = b[size:]
		switch {
		case mask&syscall.IN_Q_OVERFLOW != 0:
			// The queue was full and events were dropped: read them all.
			for _, wt := range w.watches {
				if note, ok := w.read(wt, at); ok {
					notes = append(notes, note)
				}
			}
		case mask&syscall.IN_IGNORED != 0:
			// The cgroup is gone; its exit may not have been taken yet.
			if wt := w.watches[wd]; wt != nil {
				if w.byID[wt.id] == wd {
					delete(w.byID, wt.id)
				}
				if !wt.limitAt.IsZero() {
					w.gone[wt.id] = wt
				}
			}
			delete(w.watches, wd)
		case mask&syscall.IN_MODIFY != 0:
			if wt := w.watches[wd]; wt != nil {
				if note, ok := w.read(wt, at); ok {
					notes = append(notes, note)
				}
			}
		}
	}
	return notes
}

// read reports the counter if it grew. A cgroup already gone reads as no
// change: what is lost then is lost to the exit's read too.
func (w *oomWatch) read(wt *watched, at time.Time) (oomNote, bool) {
	ev, err := readMemEvents(wt.path)
	if err != nil {
		w.log.Debug("cgroup read failed", "container", short(wt.id), "epoch", wt.epoch, "err", err)
		return oomNote{}, false
	}
	if ev != wt.last {
		w.log.Debug("cgroup memory events", "container", short(wt.id), "epoch", wt.epoch,
			"max", ev.max, "oom", ev.oom, "oom_kill", ev.oomKill)
	}
	atLimit := ev.max > wt.last.max || ev.oom > wt.last.oom
	wt.last = ev
	if ev.oomKill > wt.seen {
		wt.seen = ev.oomKill
		return oomNote{id: wt.id, epoch: wt.epoch, kills: ev.oomKill, at: at}, true
	}
	if atLimit {
		if pe, err := readMemEvents(wt.parent); err == nil {
			wt.limitAt, wt.parentKills = at, pe.oomKill
		}
	}
	return oomNote{}, false
}

// parentWindow is how long after a container was last seen at its limit the
// parent's counter may speak for it.
const parentWindow = 5 * time.Second

// parentOOM is the last word on an incarnation whose cgroup is gone: the
// parent's oom_kill has grown since the incarnation was last read at its
// limit, a moment before. The kill is taken once.
func (w *oomWatch) parentOOM(id string, epoch int) (int64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for g, wt := range w.gone {
		if time.Since(wt.limitAt) > parentWindow {
			delete(w.gone, g)
		}
	}
	wt := w.watches[w.byID[id]]
	if wt == nil || wt.id != id || wt.epoch != epoch {
		wt = w.gone[id]
	}
	if wt == nil || wt.epoch != epoch || wt.limitAt.IsZero() || time.Since(wt.limitAt) > parentWindow {
		return 0, false
	}
	pe, err := readMemEvents(wt.parent)
	if err != nil || pe.oomKill <= wt.parentKills {
		return 0, false
	}
	w.log.Debug("OOM kill read from the parent cgroup", "container", short(id), "epoch", epoch,
		"parent_oom_kill", pe.oomKill, "before", wt.parentKills, "at_limit", time.Since(wt.limitAt).Round(time.Millisecond))
	wt.parentKills = pe.oomKill
	return pe.oomKill, true
}
