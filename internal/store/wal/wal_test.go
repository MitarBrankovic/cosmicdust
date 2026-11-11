package wal

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

func open(t *testing.T, dir string, opts ...func(*Options)) *WAL {
	t.Helper()
	o := Options{Dir: dir, Logger: quiet(), Meta: func(id string) Container {
		return Container{Name: "name-" + id, Image: "img-" + id, App: "app-" + id}
	}}
	for _, f := range opts {
		f(&o)
	}
	w, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

var t0 = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

// recs makes n records of container id, numbered from first, each one 100
// bytes further into its log.
func recs(id string, first, n int) []cosmicdust.Record {
	out := make([]cosmicdust.Record, n)
	for i := range out {
		k := first + i
		off := int64(k * 100)
		out[i] = cosmicdust.Record{
			Time: t0.Add(time.Duration(k) * time.Millisecond), ReadAt: t0.Add(time.Duration(k)*time.Millisecond + 250*time.Millisecond),
			ContainerID: id, Stream: "stdout", Source: "file-json", Line: fmt.Appendf(nil, "%s-%d", id, k),
			Start:  cosmicdust.Position{Inode: 7, Head: 99, Offset: off},
			Commit: cosmicdust.Position{Inode: 7, Head: 99, Offset: off + 100},
		}
	}
	return out
}

// lines reads every sealed segment back as "id-k" strings, in order.
func lines(t *testing.T, w *WAL) []string {
	t.Helper()
	var out []string
	for _, seg := range w.Sealed() {
		err := w.Read(seg, func(b *Block) error {
			for _, r := range b.Logs {
				out = append(out, r.Line)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func seal(t *testing.T, w *WAL) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size > 0 {
		if err := w.roll(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	w := open(t, t.TempDir())
	code := 137
	ev := cosmicdust.Event{Time: t0, Runtime: "containerd", Namespace: "moby", Action: "/tasks/exit", Kind: cosmicdust.KindExit,
		ContainerID: "c1", Name: "web", Image: "nginx", App: "shop.web", State: cosmicdust.StateExited, Epoch: 2,
		ExitCode: &code, OOMKilled: true, Inferred: true, Fields: map[string]string{"pid": "7", "signal": "9"}}
	zero := 0
	ev2 := cosmicdust.Event{Time: t0, Runtime: "docker", Action: "resync", Kind: cosmicdust.KindResync, ExitCode: &zero}
	r := recs("c1", 1, 2)
	r[1].Stream, r[1].Source, r[1].Parts, r[1].Truncated = "weird", "somewhere", 3, true
	if err := w.Add(r); err != nil {
		t.Fatal(err)
	}
	if err := w.AddEvents([]cosmicdust.Event{ev, ev2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w)

	var got *Block
	if err := w.Read(w.Sealed()[0], func(b *Block) error { got = b; return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got.Containers) != 1 || got.Containers[0] != (Container{ID: "c1", Name: "name-c1", Image: "img-c1", App: "app-c1"}) {
		t.Fatalf("containers %+v", got.Containers)
	}
	if p := got.Positions["c1"]; p.Inode != 7 || p.Head != 99 || p.Offset != 300 {
		t.Fatalf("position %+v", p)
	}
	l := got.Logs
	if len(l) != 2 || l[0].Line != "c1-1" || !l[0].TS.Equal(r[0].Time) || !l[0].ReadAt.Equal(r[0].ReadAt) ||
		l[0].Stream != "stdout" || l[0].Source != "file-json" || l[0].Offset != 100 || l[0].Container != 0 {
		t.Fatalf("row 0 %+v", l[0])
	}
	if l[1].Stream != "weird" || l[1].Source != "somewhere" || l[1].Parts != 3 || !l[1].Truncated {
		t.Fatalf("row 1 %+v", l[1])
	}
	e := got.Events
	if len(e) != 2 || e[0].Action != "/tasks/exit" || *e[0].ExitCode != 137 || !e[0].OOMKilled || !e[0].Inferred ||
		e[0].Fields["signal"] != "9" || e[0].State != cosmicdust.StateExited || e[0].Epoch != 2 || e[0].App != "shop.web" || !e[0].Time.Equal(t0) {
		t.Fatalf("event 0 %+v", e[0])
	}
	if e[1].ExitCode == nil || *e[1].ExitCode != 0 || e[1].Kind != cosmicdust.KindResync {
		t.Fatalf("event 1: exit code 0 must stay 0, not none: %+v", e[1])
	}
}

// The records' lines alias a reader's buffer: the WAL must have its copy.
func TestAddCopiesTheLines(t *testing.T) {
	w := open(t, t.TempDir())
	r := recs("c1", 1, 1)
	if err := w.Add(r); err != nil {
		t.Fatal(err)
	}
	copy(r[0].Line, "XXXX")
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w)
	if got := lines(t, w); len(got) != 1 || got[0] != "c1-1" {
		t.Fatalf("lines %v", got)
	}
}

func TestPositionsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Add(append(recs("a", 1, 3), recs("b", 1, 1)...)); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.Position("a"); ok {
		t.Fatal("a position before the block was written")
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(recs("a", 4, 2)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if p, _ := w.Position("a"); p.Offset != 600 {
		t.Fatalf("a at %d, want 600", p.Offset)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := open(t, dir)
	if p, ok := w2.Position("a"); !ok || p.Offset != 600 || p.Inode != 7 || p.Head != 99 {
		t.Fatalf("a after the restart: %+v %v", p, ok)
	}
	if p, _ := w2.Position("b"); p.Offset != 200 {
		t.Fatalf("b after the restart at %d, want 200", p.Offset)
	}
	if got := lines(t, w2); strings.Join(got, ",") != "a-1,a-2,a-3,b-1,a-4,a-5" {
		t.Fatalf("lines %v", got)
	}
}

// A crash in the middle of a write leaves half a block at the end of the
// newest segment: cut off at recovery, the blocks before it kept.
func TestTornTailIsCut(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	for i := range 3 {
		if err := w.Add(recs("a", 1+i*10, 10)); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	seg := w.segmentPath(w.id)
	_ = w.Close()
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0x32, 0x42, 0x57, 0x4e, 1, 0, 0, 0, 200, 0}) // a header and then nothing
	_ = f.Close()

	w2 := open(t, dir)
	if n := testutil.ToFloat64(w2.m.tornTails); n != 1 {
		t.Fatalf("torn tails %v, want 1", n)
	}
	if p, _ := w2.Position("a"); p.Offset != 3100 {
		t.Fatalf("position %d after the cut, want 3100", p.Offset)
	}
	if got := lines(t, w2); len(got) != 30 || got[29] != "a-30" {
		t.Fatalf("%d lines, last %q", len(got), got[len(got)-1])
	}
}

// Damage in an older segment cannot be a crash: it is reported, and what
// comes before it is still read.
func TestCorruptOlderSegment(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	for i := range 2 {
		if err := w.Add(recs("a", 1+i*10, 10)); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	first := w.id
	seal(t, w)
	if err := w.Add(recs("a", 21, 10)); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	data, err := os.ReadFile(w.segmentPath(first))
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-5] ^= 0xff // in the second block's payload
	if err := os.WriteFile(w.segmentPath(first), data, 0o600); err != nil {
		t.Fatal(err)
	}
	w2 := open(t, dir)
	if n := testutil.ToFloat64(w2.m.corrupt); n != 1 {
		t.Fatalf("corrupt segments %v, want 1", n)
	}
	var got int
	err = w2.Read(first, func(b *Block) error { got += len(b.Logs); return nil })
	if !IsCorrupt(err) || got != 10 {
		t.Fatalf("read %d rows, err %v; want the first block's 10 and a corruption", got, err)
	}
}

// The segment ages by moving its start back, not by sleeping: with an age of
// 20 ms, a loaded machine (one fsync of 20 ms) sealed it before its age.
func TestSegmentsSealBySizeAndAge(t *testing.T) {
	w := open(t, t.TempDir(), func(o *Options) { o.SegmentBytes = 1000; o.SegmentAge = time.Minute })
	if err := w.Add(recs("a", 1, 100)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(w.Sealed()) != 1 {
		t.Fatalf("%d sealed after a block over the size, want 1", len(w.Sealed()))
	}
	if err := w.Add(recs("a", 101, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	w.tick()
	if len(w.Sealed()) != 1 {
		t.Fatal("sealed before its age")
	}
	w.opened = w.opened.Add(-time.Minute)
	w.tick()
	if len(w.Sealed()) != 2 {
		t.Fatalf("%d sealed after the age, want 2", len(w.Sealed()))
	}
	w.tick() // an empty open segment is not sealed
	if len(w.Sealed()) != 2 {
		t.Fatal("an empty segment was sealed")
	}
}

func TestReleaseKeepsPositionsInTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Add(append(recs("a", 1, 5), recs("b", 1, 2)...)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w)
	if err := w.Add(recs("a", 6, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w)
	sealed := w.Sealed()
	if err := w.Release(sealed[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(w.segmentPath(sealed[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released segment still there: %v", err)
	}
	if err := w.Release(sealed[0]); err != nil {
		t.Fatalf("releasing twice: %v", err)
	}
	if err := w.Release(w.id); err == nil {
		t.Fatal("released the open segment")
	}
	_ = w.Close()

	// b's only block is gone; its position lives on in the checkpoint.
	w2 := open(t, dir)
	if p, _ := w2.Position("b"); p.Offset != 300 {
		t.Fatalf("b at %d after the release and a restart, want 300", p.Offset)
	}
	if p, _ := w2.Position("a"); p.Offset != 700 {
		t.Fatalf("a at %d, want 700 (from the segment still on disk)", p.Offset)
	}
	if got := lines(t, w2); strings.Join(got, ",") != "a-6" {
		t.Fatalf("lines %v", got)
	}
}

// Released, but the process died before the file was deleted.
// A segment written after what Keep says is saved elsewhere stays past its
// release, until Keep passes it, or for KeepAtMost.
func TestKeepHoldsReleasedSegments(t *testing.T) {
	var keep atomic.Int64
	w := open(t, t.TempDir(), func(o *Options) {
		o.Keep = func() time.Time { return time.Unix(0, keep.Load()) }
		o.KeepAtMost = time.Hour
	})
	for i := range 2 {
		if err := w.Add(recs("a", 1+i, 1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		seal(t, w)
		if i == 0 {
			keep.Store(time.Now().UnixNano()) // the first segment is saved, the second not
			time.Sleep(time.Millisecond)
		}
	}
	sealed := w.Sealed()
	if err := w.Release(sealed[1]); err != nil {
		t.Fatal(err)
	}
	if got := w.Sealed(); len(got) != 1 || got[0] != sealed[1] {
		t.Fatalf("on disk after the release: %v, want only %d", got, sealed[1])
	}
	w.tick()
	if len(w.Sealed()) != 1 {
		t.Fatal("released before Keep passed it")
	}
	keep.Store(time.Now().UnixNano())
	w.tick()
	if got := w.Sealed(); len(got) != 0 {
		t.Fatalf("still held after Keep passed it: %v", got)
	}

	// Never saved: held for KeepAtMost only.
	w2 := open(t, t.TempDir(), func(o *Options) {
		o.Keep = func() time.Time { return time.Time{} }
		o.KeepAtMost = 20 * time.Millisecond
	})
	if err := w2.Add(recs("a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w2.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w2)
	if err := w2.Release(w2.Sealed()[0]); err != nil {
		t.Fatal(err)
	}
	if len(w2.Sealed()) != 1 {
		t.Fatal("not held")
	}
	time.Sleep(30 * time.Millisecond)
	w2.tick()
	if got := w2.Sealed(); len(got) != 0 {
		t.Fatalf("held past KeepAtMost: %v", got)
	}
}

func TestReleasedSegmentLeftBehindIsDeleted(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Add(recs("a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seal(t, w)
	seg := w.Sealed()[0]
	data, _ := os.ReadFile(w.segmentPath(seg))
	if err := w.Release(seg); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if err := os.WriteFile(w.segmentPath(seg), data, 0o600); err != nil {
		t.Fatal(err)
	}
	w2 := open(t, dir)
	if len(w2.Sealed()) != 0 {
		t.Fatalf("sealed %v, want none", w2.Sealed())
	}
	if _, err := os.Stat(w.segmentPath(seg)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the released segment was not deleted")
	}
}

// A full disk: half a block reached the file before the write failed. It is
// cut back, the block waits, and the next Flush writes it once.
func TestNoSpaceIsUndoneAndRetried(t *testing.T) {
	w := open(t, t.TempDir())
	if err := w.Add(recs("a", 1, 5)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	before := w.size
	fail := true
	w.failWrite = func(f *os.File, b []byte) (int, error) {
		if !fail {
			return 0, nil
		}
		n, _ := f.Write(b[:len(b)/2])
		return n, &os.PathError{Op: "write", Path: f.Name(), Err: syscall.ENOSPC}
	}
	if err := w.Add(recs("a", 6, 5)); err != nil {
		t.Fatal(err)
	}
	err := w.Flush()
	if !errors.Is(err, ErrNoSpace) || !isTemporary(err) {
		t.Fatalf("Flush returned %v, want a temporary ErrNoSpace", err)
	}
	if st, _ := w.f.Stat(); st.Size() != before {
		t.Fatalf("segment is %d bytes after the failed write, want %d", st.Size(), before)
	}
	if p, _ := w.Position("a"); p.Offset != 600 {
		t.Fatalf("position moved to %d by a block not written", p.Offset)
	}
	w.tick() // still full
	fail = false
	w.tick() // room again: the waiting block goes out
	if p, _ := w.Position("a"); p.Offset != 1100 {
		t.Fatalf("position %d after the retry, want 1100", p.Offset)
	}
	seal(t, w)
	if got := lines(t, w); len(got) != 10 || got[9] != "a-10" {
		t.Fatalf("lines %v: each line once, in order", got)
	}
	if n := testutil.ToFloat64(w.m.noSpace); n != 2 {
		t.Fatalf("no space %v, want 2", n)
	}
}

func TestBigPendingBlockIsWrittenAtOnce(t *testing.T) {
	w := open(t, t.TempDir(), func(o *Options) { o.BlockBytes = 500 })
	if err := w.Add(recs("a", 1, 100)); err != nil {
		t.Fatal(err)
	}
	if !w.p.empty() {
		t.Fatal("a block over BlockBytes stayed pending")
	}
	if p, _ := w.Position("a"); p.Offset != 10100 {
		t.Fatalf("position %d", p.Offset)
	}
}

func TestClosedWAL(t *testing.T) {
	w := open(t, t.TempDir())
	if err := w.Add(recs("a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(recs("a", 2, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Add after Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestDecodeRejectsDamage(t *testing.T) {
	w := open(t, t.TempDir())
	if err := w.Add(recs("a", 1, 3)); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	_, _ = w.frameBlock(t0)
	payload := append([]byte(nil), w.payload...)
	w.mu.Unlock()
	if _, err := decodeBlock(payload); err != nil {
		t.Fatalf("intact payload: %v", err)
	}
	for _, cut := range []int{1, 5, len(payload) / 2, len(payload) - 1} {
		if _, err := decodeBlock(payload[:cut]); !errors.Is(err, errInvalid) {
			t.Errorf("payload cut at %d: %v", cut, err)
		}
	}
	if _, err := decodeBlock(append(payload, 0)); !errors.Is(err, errInvalid) {
		t.Error("a trailing byte was accepted")
	}
	bad := append([]byte(nil), payload...)
	bad[0] = 9 // version
	if _, err := decodeBlock(bad); !errors.Is(err, errInvalid) {
		t.Error("an unknown version was accepted")
	}
}

func TestEncodingAllocatesNothingOnceWarm(t *testing.T) {
	w := open(t, t.TempDir())
	r := recs("a", 1, 100)
	warm := func() {
		_ = w.Add(r)
		w.mu.Lock()
		_, _ = w.frameBlock(t0)
		w.p.reset()
		w.mu.Unlock()
	}
	warm()
	// The test's Meta builds three strings per block; nothing may grow
	// with the records.
	if allocs := testing.AllocsPerRun(20, warm); allocs > 4 {
		t.Fatalf("%v allocations per block of 100 records", allocs)
	}
}

var _ io.Closer = (*WAL)(nil)
