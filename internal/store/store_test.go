package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

// hour0 is an hour long past, so that compaction takes it.
var hour0 = time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)

type env struct {
	t    *testing.T
	dir  string
	w    *wal.WAL
	s    *Store
	opts func(*Options)
}

func newEnv(t *testing.T, opts ...func(*Options)) *env {
	e := &env{t: t, dir: t.TempDir()}
	e.opts = func(o *Options) {
		for _, f := range opts {
			f(o)
		}
	}
	e.open()
	t.Cleanup(func() { _ = e.w.Close() })
	return e
}

func (e *env) open() {
	e.t.Helper()
	w, err := wal.Open(wal.Options{Dir: filepath.Join(e.dir, "wal"), Logger: quiet(),
		Meta: func(id string) wal.Container {
			return wal.Container{Name: "name-" + id, Image: "img-" + id, App: "app-" + id}
		}})
	if err != nil {
		e.t.Fatal(err)
	}
	o := Options{Root: filepath.Join(e.dir, "store"), Node: "node-1", WAL: w, Logger: quiet(),
		StateAt: func(id string, ts time.Time) (cosmicdust.State, int) { return cosmicdust.StateRunning, 3 }}
	e.opts(&o)
	s, err := Open(o)
	if err != nil {
		e.t.Fatal(err)
	}
	e.w, e.s = w, s
}

func (e *env) reopen() {
	e.t.Helper()
	_ = e.w.Close()
	e.open()
}

// write puts n lines of container id into the WAL, k-th line at ts(k), and
// seals the segment.
func (e *env) write(id string, first, n int, ts func(k int) time.Time) {
	e.t.Helper()
	recs := make([]cosmicdust.Record, n)
	for i := range recs {
		k := first + i
		recs[i] = cosmicdust.Record{Time: ts(k), ReadAt: ts(k).Add(time.Second), ContainerID: id, Stream: "stdout",
			Source: "file-json", Line: fmt.Appendf(nil, "%s-%d", id, k),
			Start: cosmicdust.Position{Inode: 1, Head: 2, Offset: int64(k * 10)}, Commit: cosmicdust.Position{Inode: 1, Head: 2, Offset: int64(k*10 + 10)}}
	}
	if err := e.w.Add(recs); err != nil {
		e.t.Fatal(err)
	}
	e.flushSeal()
}

func (e *env) event(id, action string, ts time.Time) {
	e.t.Helper()
	code := 0
	if err := e.w.AddEvents([]cosmicdust.Event{{Time: ts, Runtime: "docker", Action: action, Kind: cosmicdust.KindExit,
		ContainerID: id, Name: "n", App: "a", State: cosmicdust.StateExited, Epoch: 1, ExitCode: &code,
		Fields: map[string]string{"signal": "9"}}}); err != nil {
		e.t.Fatal(err)
	}
	e.flushSeal()
}

func (e *env) flushSeal() {
	e.t.Helper()
	if err := e.w.Flush(); err != nil {
		e.t.Fatal(err)
	}
	if err := e.w.Seal(); err != nil {
		e.t.Fatal(err)
	}
}

func at(k int) time.Time { return hour0.Add(time.Duration(k) * time.Second) }

func (e *env) convert() int {
	e.t.Helper()
	n, err := e.s.convert()
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// logLines reads every committed log file back as "line" strings, in file
// and row order.
func (e *env) logLines() []string {
	e.t.Helper()
	var out []string
	for _, r := range e.rows() {
		out = append(out, r.Line)
	}
	return out
}

func (e *env) rows() []LogRow {
	e.t.Helper()
	dirs, err := e.s.partitionDirs(tableLogs)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []LogRow
	for _, dir := range dirs {
		m, err := loadManifest(dir)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, f := range m.Files {
			rows, err := parquet.ReadFile[LogRow](filepath.Join(dir, f.Name))
			if err != nil {
				e.t.Fatal(err)
			}
			if int64(len(rows)) != f.Rows {
				e.t.Fatalf("%s: %d rows, manifest says %d", f.Name, len(rows), f.Rows)
			}
			out = append(out, rows...)
		}
	}
	return out
}

func TestConvert(t *testing.T) {
	e := newEnv(t)
	e.write("b", 1, 3, at)
	e.write("a", 1, 2, at)
	e.write("a", 3, 1, func(k int) time.Time { return hour0.Add(time.Hour + time.Duration(k)) }) // the next hour
	e.event("a", "die", at(9))
	if n := e.convert(); n != 4 {
		t.Fatalf("converted %d segments, want 4", n)
	}
	if len(e.w.Sealed()) != 0 {
		t.Fatalf("segments left after the conversion: %v", e.w.Sealed())
	}
	if p, ok := e.w.Position("b"); !ok || p.Offset != 40 {
		t.Fatalf("b's resume position after the release: %+v %v", p, ok)
	}

	rows := e.rows()
	var lines []string
	for _, r := range rows {
		lines = append(lines, r.Line)
	}
	// Within the first hour: by container, then time.
	if got := strings.Join(lines, ","); got != "a-1,a-2,b-1,b-2,b-3,a-3" {
		t.Fatalf("rows %s", got)
	}
	r := rows[0]
	if r.Node != "node-1" || r.App != "app-a" || r.Name != "name-a" || r.Image != "img-a" || r.State != "running" ||
		r.Epoch != 3 || r.Stream != "stdout" || r.Source != "file-json" || r.Offset != 10 || !r.TS.Equal(at(1)) {
		t.Fatalf("row %+v", r)
	}

	dirs, _ := e.s.partitionDirs(tableEvents)
	if len(dirs) != 1 {
		t.Fatalf("event partitions %v", dirs)
	}
	m, _ := loadManifest(dirs[0])
	evs, err := parquet.ReadFile[EventRow](filepath.Join(dirs[0], m.Files[0].Name))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Action != "die" || evs[0].ExitCode != 0 || evs[0].Fields != `{"signal":"9"}` ||
		evs[0].Kind != "exit" || evs[0].State != "exited" || evs[0].Node != "node-1" {
		t.Fatalf("events %+v", evs)
	}
	if e.convert() != 0 {
		t.Fatal("a second conversion found work")
	}
}

// A crash anywhere in a conversion must leave every row once in the store.
func TestConversionCrashes(t *testing.T) {
	for _, step := range []string{"files-written", "committed", "manifests-updated"} {
		t.Run(step, func(t *testing.T) {
			e := newEnv(t)
			e.write("a", 1, 5, at)
			e.write("b", 1, 5, at)
			e.s.crash = step
			if _, err := e.s.convert(); !errors.Is(err, errCrash) {
				t.Fatalf("convert: %v", err)
			}
			e.reopen() // the repair runs here
			e.write("a", 6, 2, at)
			e.convert()
			got := e.logLines()
			slices.Sort(got)
			want := []string{"a-1", "a-2", "a-3", "a-4", "a-5", "a-6", "a-7", "b-1", "b-2", "b-3", "b-4", "b-5"}
			if !slices.Equal(got, want) {
				t.Fatalf("after a crash at %s: %v", step, got)
			}
			if len(e.w.Sealed()) != 0 {
				t.Fatalf("segments left: %v", e.w.Sealed())
			}
			for _, dir := range must(e.s.partitionDirs(tableLogs)) {
				for _, f := range must(os.ReadDir(dir)) {
					if strings.HasSuffix(f.Name(), ".tmp") {
						t.Fatalf("a temporary file survived: %s", f.Name())
					}
				}
			}
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestConversionIsBounded(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.ConvertBytes = 1 })
	for i := range 3 {
		e.write("a", 1+i*10, 10, at)
	}
	if n := e.convert(); n != 1 {
		t.Fatalf("one conversion took %d segments with a 1-byte budget, want 1", n)
	}
	if n := e.convert(); n != 1 {
		t.Fatalf("the next took %d", n)
	}
}

// With a tiny SortBytes every row is written as it comes: the file keeps the
// WAL's order, and every row is still there once.
func TestConversionStreams(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.SortBytes = 1 })
	e.write("b", 1, 3, at)
	e.write("a", 1, 3, at)
	e.convert()
	if got := strings.Join(e.logLines(), ","); got != "b-1,b-2,b-3,a-1,a-2,a-3" {
		t.Fatalf("rows %s", got)
	}
}

// Rows that move through more hours than a conversion keeps open: the least
// recently used hour's file is finished, and the hour gets another file when
// its rows come again.
func TestConversionManyHours(t *testing.T) {
	e := newEnv(t)
	hour := func(h int) func(k int) time.Time {
		return func(k int) time.Time { return hour0.Add(time.Duration(h)*time.Hour + time.Duration(k)*time.Second) }
	}
	for h := range maxOpenParts + 2 {
		e.write("a", 1+h*10, 2, hour(h))
	}
	e.write("a", 100, 2, hour(0)) // the first hour again, long closed
	e.convert()
	got := e.logLines()
	if len(got) != 2*(maxOpenParts+3) {
		t.Fatalf("%d rows, want %d: %v", len(got), 2*(maxOpenParts+3), got)
	}
	dirs := must(e.s.partitionDirs(tableLogs))
	if len(dirs) != maxOpenParts+2 {
		t.Fatalf("%d hours, want %d", len(dirs), maxOpenParts+2)
	}
	if m := must(loadManifest(dirs[0])); len(m.Files) != 2 {
		t.Fatalf("the first hour has %d files, want 2", len(m.Files))
	}
}

func TestRawSizeMatchesEncoding(t *testing.T) {
	ts := time.Date(2026, 10, 21, 8, 0, 0, 120000000, time.UTC)
	logs := []LogRow{
		{TS: ts, ReadAt: ts.Add(time.Millisecond), Node: "n", ContainerID: "c", Name: "x", Stream: "stdout", State: "running",
			Line: "plain", Source: "file-json"},
		{TS: ts.Truncate(time.Second), ReadAt: ts.In(time.FixedZone("x", 3600)), Node: "n", App: "shop.api", ContainerID: "c",
			Name: "x", Image: "nginx:1.27", Stream: "stderr", State: "exited", Epoch: 12, Line: "esc \"q\" \\ <a&b> \t\x01 ćao 😀   \xff",
			Source: "file-cri", Inode: 99, Head: 1 << 60, Offset: 12345, Parts: 3, Truncated: true},
	}
	for _, r := range logs {
		b, _ := json.Marshal(r)
		if got, want := r.rawSize(), int64(len(b)+1); got != want {
			t.Errorf("log row %q: counted %d, encoded %d", r.Line, got, want)
		}
	}
	events := []EventRow{
		{TS: ts, Node: "n", ContainerID: "c", Name: "x", Runtime: "docker", Action: "die", Kind: "exit", State: "exited",
			Epoch: 1, ExitCode: -1},
		{TS: ts, Node: "n", App: "a", ContainerID: "c", Name: "x", Image: "i", Runtime: "containerd", Namespace: "moby",
			Action: "/tasks/exit", Kind: "exit", State: "exited", Epoch: 2, ExitCode: 137, OOMKilled: true, Inferred: true,
			Fields: `{"pid":"7"}`},
	}
	for _, r := range events {
		b, _ := json.Marshal(r)
		if got, want := r.rawSize(), int64(len(b)+1); got != want {
			t.Errorf("event row %q: counted %d, encoded %d", r.Action, got, want)
		}
	}
}

// compactable leaves n converted files of a past hour in the store.
func compactable(t *testing.T, e *env, n int) {
	t.Helper()
	for i := range n {
		// Interleaved containers, so the merge has something to sort.
		e.write("b", 1+i*2, 2, func(k int) time.Time { return at(k * 2) })
		e.write("a", 1+i*2, 2, func(k int) time.Time { return at(k*2 + 1) })
		e.convert()
	}
}

func TestCompaction(t *testing.T) {
	e := newEnv(t)
	compactable(t, e, 4)
	before := e.rows()
	if err := e.s.compactAll(time.Now()); err != nil {
		t.Fatal(err)
	}
	dirs := must(e.s.partitionDirs(tableLogs))
	m := must(loadManifest(dirs[0]))
	if len(m.Files) != 1 || !strings.HasPrefix(m.Files[0].Name, "compact-") {
		t.Fatalf("after compaction: %+v", m.Files)
	}
	if entries := must(os.ReadDir(dirs[0])); len(entries) != 2 { // the file and the manifest
		t.Fatalf("files left in the partition: %d", len(entries))
	}
	after := e.rows()
	if len(after) != len(before) {
		t.Fatalf("%d rows after, %d before", len(after), len(before))
	}
	for i := 1; i < len(after); i++ {
		a, b := after[i-1], after[i]
		if a.ContainerID > b.ContainerID || (a.ContainerID == b.ContainerID && a.TS.After(b.TS)) {
			t.Fatalf("row %d out of order: %s %v after %s %v", i, b.ContainerID, b.TS, a.ContainerID, a.TS)
		}
	}
	var raw int64
	for _, f := range m.Files {
		raw += f.BytesRaw
	}
	if raw == 0 || m.Files[0].Segments[0] == 0 {
		t.Fatalf("manifest entry %+v", m.Files[0])
	}
}

func TestCompactionLeavesTheCurrentHourAndSyncedFiles(t *testing.T) {
	e := newEnv(t)
	compactable(t, e, 3)
	// Not over yet, by the clock compaction is given.
	if err := e.s.compactAll(hour0.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	dir := must(e.s.partitionDirs(tableLogs))[0]
	if n := len(must(loadManifest(dir)).Files); n != 3 {
		t.Fatalf("an hour not over was compacted: %d files", n)
	}
	refs := must(e.s.Unsynced(context.Background()))
	for _, ref := range refs[:2] { // sync two of the three
		r, done, err := e.s.Claim(ref)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Close()
		if err := done(cosmicdust.Ack{Status: cosmicdust.AckStored}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.s.compactAll(time.Now()); err != nil {
		t.Fatal(err)
	}
	if n := len(must(loadManifest(dir)).Files); n != 3 {
		t.Fatalf("synced files were compacted: %d files", n)
	}
}

// A file that went to the control plane once may be there, unsynced only
// because the answer was lost: merged, its rows would arrive again in the
// merged file. Compaction leaves it alone, after a restart too.
func TestCompactionLeavesOfferedFiles(t *testing.T) {
	e := newEnv(t)
	compactable(t, e, 3)
	refs := must(e.s.Unsynced(context.Background()))
	r, done, err := e.s.Claim(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if err := done(cosmicdust.Ack{}); err != nil { // the upload failed
		t.Fatal(err)
	}
	e.reopen()
	if err := e.s.compactAll(time.Now()); err != nil {
		t.Fatal(err)
	}
	dir := must(e.s.partitionDirs(tableLogs))[0]
	files := must(loadManifest(dir)).Files
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name)
	}
	offered := path.Base(refs[0].Path)
	if len(files) != 2 || !slices.Contains(names, offered) {
		t.Fatalf("after compaction %v, want the offered %s and one merged file", names, offered)
	}
	if left := must(e.s.Unsynced(context.Background())); len(left) != 2 {
		t.Fatalf("%d unsynced, want the offered file and the merged one", len(left))
	}
}

func TestCompactionCrashes(t *testing.T) {
	for _, step := range []string{"compact-written", "compact-committed"} {
		t.Run(step, func(t *testing.T) {
			e := newEnv(t)
			compactable(t, e, 3)
			want := e.logLines()
			slices.Sort(want)
			e.s.crash = step
			if err := e.s.compactAll(time.Now()); !errors.Is(err, errCrash) {
				t.Fatalf("compact: %v", err)
			}
			e.s.crash = ""
			e.reopen()
			got := e.logLines()
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("after a crash at %s: %d rows, want %d", step, len(got), len(want))
			}
			dir := must(e.s.partitionDirs(tableLogs))[0]
			listed := len(must(loadManifest(dir)).Files)
			if entries := len(must(os.ReadDir(dir))); entries != listed+1 {
				t.Fatalf("%d files on disk, %d listed: orphans left", entries-1, listed)
			}
		})
	}
}

func TestSyncSide(t *testing.T) {
	e := newEnv(t)
	e.write("a", 1, 3, at)
	e.event("a", "die", at(5))
	e.convert()
	refs := must(e.s.Unsynced(context.Background()))
	if len(refs) != 2 || refs[0].Table != tableEvents || refs[1].Table != tableLogs {
		t.Fatalf("unsynced %+v", refs)
	}
	r, done, err := e.s.Claim(refs[1])
	if err != nil {
		t.Fatal(err)
	}
	data := must(io.ReadAll(r))
	_ = r.Close()
	if int64(len(data)) != refs[1].Size || !bytes.HasPrefix(data, []byte("PAR1")) {
		t.Fatalf("read %d bytes, the ref says %d", len(data), refs[1].Size)
	}
	if _, _, err := e.s.Claim(refs[1]); !errors.Is(err, cosmicdust.ErrClaimed) {
		t.Fatalf("a second claim: %v", err)
	}
	if n := len(must(e.s.Unsynced(context.Background()))); n != 1 {
		t.Fatalf("a claimed file listed as unsynced: %d", n)
	}
	if err := done(cosmicdust.Ack{}); err != nil { // the send failed
		t.Fatal(err)
	}
	if n := len(must(e.s.Unsynced(context.Background()))); n != 2 {
		t.Fatalf("after a failed send: %d unsynced, want 2", n)
	}
	for _, ref := range refs {
		_, done, err := e.s.Claim(ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := done(cosmicdust.Ack{Status: cosmicdust.AckDuplicate}); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(must(e.s.Unsynced(context.Background()))); n != 0 {
		t.Fatalf("after the acks: %d unsynced", n)
	}
	if err := e.s.GC(1); err != nil {
		t.Fatal(err)
	}
	if rows := e.rows(); len(rows) != 0 {
		t.Fatalf("GC under a 1-byte quota left %d synced rows", len(rows))
	}
	if _, _, err := e.s.Claim(cosmicdust.FileRef{Table: "logs", Path: "../../etc/passwd"}); err == nil {
		t.Fatal("claimed a path outside the store")
	}
}

// Out of room: synced files first, then the oldest unsynced logs, each one
// recorded; events are never dropped.
func TestEvict(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.Reserve = 4000 })
	compactable(t, e, 3)
	e.event("a", "die", at(9))
	e.convert()
	var logRefs []cosmicdust.FileRef
	for _, r := range must(e.s.Unsynced(context.Background())) {
		if r.Table == tableLogs {
			logRefs = append(logRefs, r)
		}
	}
	newest := logRefs[len(logRefs)-1]
	_, done, _ := e.s.Claim(newest)
	_ = done(cosmicdust.Ack{Status: cosmicdust.AckStored})

	// With nothing free, deleting the synced file is not enough.
	e.s.free = func() (int64, error) { return 0, nil }
	gaps, err := e.s.checkSpace(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) == 0 {
		t.Fatal("nothing evicted")
	}
	g := gaps[0]
	if g.Action != "logs_dropped" || g.Fields["table"] != "logs" || g.Fields["rows"] != "4" ||
		g.Fields["file"] != filepath.Base(logRefs[0].Path) {
		t.Fatalf("first gap %+v, want the oldest log file %s", g, logRefs[0].Path)
	}
	var events int
	for _, dir := range must(e.s.partitionDirs(tableEvents)) {
		events += len(must(loadManifest(dir)).Files)
	}
	if events != 1 {
		t.Fatal("an events file was deleted")
	}
	for _, r := range e.rows() {
		if r.Line == "b-1" {
			t.Fatal("the oldest lines survived the eviction")
		}
	}
}

// Freeing the synced files is enough: nothing unsynced goes.
func TestSyncedGoFirst(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.Reserve = 1000 })
	compactable(t, e, 3)
	refs := must(e.s.Unsynced(context.Background()))
	_, done, _ := e.s.Claim(refs[len(refs)-1])
	_ = done(cosmicdust.Ack{Status: cosmicdust.AckStored})
	e.s.free = func() (int64, error) { return 900, nil }
	gaps, err := e.s.checkSpace(time.Now())
	if err != nil || len(gaps) != 0 {
		t.Fatalf("gaps %v, err %v: the synced file should have been enough", gaps, err)
	}
	if len(must(e.s.Unsynced(context.Background()))) != len(refs)-1 {
		t.Fatal("an unsynced file went")
	}
}

func TestBlock(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.Reserve = 1000; o.WhenFull = "block" })
	compactable(t, e, 2)
	e.s.free = func() (int64, error) { return 10, nil }
	gaps, err := e.s.checkSpace(time.Now())
	if err != nil || len(gaps) != 0 {
		t.Fatalf("block dropped %d files: %v", len(gaps), err)
	}
	if !e.s.full {
		t.Fatal("not held")
	}
	if err := e.w.Add([]cosmicdust.Record{{ContainerID: "a", Time: at(1), Line: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := e.w.Flush(); !errors.Is(err, wal.ErrNoSpace) {
		t.Fatalf("Flush while held: %v", err)
	}
	e.s.free = func() (int64, error) { return 1 << 40, nil }
	if _, err := e.s.checkSpace(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := e.w.Flush(); err != nil {
		t.Fatalf("Flush after room came back: %v", err)
	}
}

func TestQuota(t *testing.T) {
	e := newEnv(t)
	compactable(t, e, 4)
	size := must(e.s.sizeOnDisk())
	e.s.opts.Quota = size / 2
	gaps, err := e.s.checkSpace(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) == 0 {
		t.Fatal("over the quota and nothing dropped")
	}
	if after := must(e.s.sizeOnDisk()) + e.w.DiskBytes(); after > e.s.opts.Quota {
		t.Fatalf("%d bytes left, quota %d", after, e.s.opts.Quota)
	}
}
