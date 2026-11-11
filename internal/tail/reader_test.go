package tail

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// fakeDocker writes a json-file log the way dockerd does, rotation by rename
// included, so the reader can be tested without a daemon.
type fakeDocker struct {
	t    *testing.T
	path string
	keep int // rotated files kept: max-file minus one
	seq  int
}

func newFakeDocker(t *testing.T, keep int) *fakeDocker {
	t.Helper()
	d := &fakeDocker{t: t, path: filepath.Join(t.TempDir(), "c-json.log"), keep: keep}
	if err := os.WriteFile(d.path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

func entry(stream, text string, final bool) string {
	if final {
		text += "\n"
	}
	b, _ := json.Marshal(jsonFileEntry{Log: text, Stream: stream, Time: time.Now().UTC()})
	return string(b) + "\n"
}

func (d *fakeDocker) raw(s string) {
	d.t.Helper()
	f, err := os.OpenFile(d.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		d.t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(s); err != nil {
		d.t.Fatal(err)
	}
}

// write appends n numbered lines, line-<seq>.
func (d *fakeDocker) write(n int) {
	d.t.Helper()
	var b strings.Builder
	for range n {
		d.seq++
		b.WriteString(entry("stdout", fmt.Sprintf("line-%d", d.seq), true))
	}
	d.raw(b.String())
}

// rotate does what dockerd does when the file reaches max-size.
func (d *fakeDocker) rotate() {
	d.t.Helper()
	for i := d.keep; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", d.path, i)
		if i == d.keep {
			_ = os.Remove(src)
			continue
		}
		_ = os.Rename(src, fmt.Sprintf("%s.%d", d.path, i+1))
	}
	if err := os.Rename(d.path, d.path+".1"); err != nil {
		d.t.Fatal(err)
	}
	if err := os.WriteFile(d.path, nil, 0o644); err != nil {
		d.t.Fatal(err)
	}
}

// position is a resume position just past the n-th line of the live file.
func (d *fakeDocker) position(n int) cosmicdust.Position {
	d.t.Helper()
	of, err := openAt(d.path, true, newReadBuf(4096))
	if err != nil {
		d.t.Fatal(err)
	}
	defer of.close()
	data, err := os.ReadFile(d.path)
	if err != nil {
		d.t.Fatal(err)
	}
	off := 0
	for range n {
		i := bytes.IndexByte(data[off:], '\n')
		if i < 0 {
			d.t.Fatalf("file has fewer than %d lines", n)
		}
		off += i + 1
	}
	p := of.positionAt(int64(off))
	if p.Head == 0 {
		d.t.Fatal("the live file has no first line to fingerprint")
	}
	return p
}

func testSource(st *Stats) *Source {
	return NewSource(SourceOptions{ReadBuf: 4096, Logger: slog.New(slog.DiscardHandler)}, st)
}

func open(t *testing.T, d *fakeDocker, st *Stats, from cosmicdust.Position) *reader {
	t.Helper()
	r, err := testSource(st).open(cosmicdust.ContainerRef{ID: "c", LogPath: d.path, LogFormat: "json-file"}, from)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// rec is a record with its line copied out of the reader's arena.
type rec struct {
	cosmicdust.Record
	text string
}

// next reads everything there is now, a few records at a time, as the tailer
// does.
func next(t *testing.T, r *reader) []rec {
	t.Helper()
	var out []rec
	buf := make([]cosmicdust.Record, 7) // small, so that reads stop mid-file
	for {
		n, err := r.Next(buf)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range buf[:n] {
			out = append(out, rec{Record: x, text: string(x.Line)})
		}
		if n == 0 {
			return out
		}
	}
}

// expectRange asserts the records are exactly line-from .. line-to, in order.
func expectRange(t *testing.T, recs []rec, from, to int) {
	t.Helper()
	if len(recs) != to-from+1 {
		var got []string
		for _, r := range recs {
			got = append(got, r.text)
		}
		t.Fatalf("got %d records %v, want line-%d..line-%d", len(recs), got, from, to)
	}
	for i, r := range recs {
		n, err := strconv.Atoi(strings.TrimPrefix(r.text, "line-"))
		if err != nil || n != from+i {
			t.Fatalf("record %d is %q, want line-%d", i, r.text, from+i)
		}
	}
}

// A rotation between the listing of the rotated files and their opens moves
// the file a resume position points into to a name the listing does not have.
// It must not look rotated out of existence: the kill -9 test once lost the
// rest of such a file, 406 lines.
func TestResumeWhileRotating(t *testing.T) {
	d := newFakeDocker(t, 1000)
	d.write(10)
	p := d.position(3)
	d.rotate()
	d.write(10)
	d.rotate()
	d.write(10) // the position's file is .2 now
	st := &Stats{}
	r := open(t, d, st, p)
	rotated := false
	r.plan.listed = func() {
		if !rotated { // between the first listing and the opens: it becomes .3
			rotated = true
			d.rotate()
			d.write(5)
		}
	}
	expectRange(t, next(t, r), 4, d.seq)
	if n := st.lostFiles.Load(); n != 0 {
		t.Fatalf("%d files counted as lost", n)
	}
}

// A listing taken in the middle of a rotation has the position's file under
// its new name and the next newer file still under its old one, which the rest
// of the rotation gives to the file after it. Opened by the listed names, the
// file in between was skipped: the kill -9 test lost one whole file, 411 lines.
func TestListingInTheMiddleOfARotation(t *testing.T) {
	d := newFakeDocker(t, 1000)
	d.write(10)
	p := d.position(10)
	d.rotate()
	d.write(10) // the position's file is .1; lines 11-20 are in the live one
	// The next rotation, up to its middle: .1 has become .2, the live file has
	// not become .1 yet.
	if err := os.Rename(d.path+".1", d.path+".2"); err != nil {
		t.Fatal(err)
	}
	st := &Stats{}
	r := open(t, d, st, p)
	finished := false
	r.plan.listed = func() {
		if !finished { // the rest of the rotation, between the listing and the opens
			finished = true
			if err := os.Rename(d.path, d.path+".1"); err != nil {
				t.Fatal(err)
			}
			d.write(5)
		}
	}
	expectRange(t, next(t, r), 11, d.seq)
	if n := st.lostFiles.Load(); n != 0 {
		t.Fatalf("%d files counted as lost", n)
	}
}

// Readers of one source share the read buffer: taking turns a few records at a
// time, stopping mid-file and at half-written lines, each still reads its own
// file once and in order.
func TestReadersShareBuffer(t *testing.T) {
	src := NewSource(SourceOptions{ReadBuf: 64, Logger: slog.New(slog.DiscardHandler)}, nil)
	var ds []*fakeDocker
	var rs []*reader
	for i := range 3 {
		d := newFakeDocker(t, 1000)
		r, err := src.open(cosmicdust.ContainerRef{ID: fmt.Sprintf("c%d", i), LogPath: d.path, LogFormat: "json-file"}, cosmicdust.Position{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		ds, rs = append(ds, d), append(rs, r)
	}
	const rounds = 30
	finish := `-written\n","stream":"stdout","time":"2026-10-21T10:00:00Z"}` + "\n"
	half := make([]bool, len(ds)) // a line was started and not finished
	got := make([][]rec, len(rs))
	buf := make([]cosmicdust.Record, 3)
	for round := range rounds {
		for i, d := range ds {
			if half[i] {
				d.raw(finish)
				half[i] = false
			}
			if round%11 == 5 {
				d.rotate()
			}
			d.write(1 + (round+i)%5)
			if round%7 == i {
				d.raw(`{"log":"half`) // finished in the next round
				half[i] = true
			}
		}
		for i, r := range rs {
			n, err := r.Next(buf)
			if err != nil {
				t.Fatal(err)
			}
			for _, x := range buf[:n] {
				got[i] = append(got[i], rec{Record: x, text: string(x.Line)})
			}
		}
	}
	for i, r := range rs {
		if half[i] {
			ds[i].raw(finish)
		}
		got[i] = append(got[i], next(t, r)...)
		var lines []rec
		halves, want := 0, 0
		for round := range rounds {
			if round%7 == i {
				want++
			}
		}
		for _, x := range got[i] {
			if x.text == "half-written" {
				halves++
				continue
			}
			lines = append(lines, x)
		}
		if halves != want {
			t.Errorf("reader %d: %d half-written lines put together, want %d", i, halves, want)
		}
		expectRange(t, lines, 1, ds[i].seq)
	}
}

// The core claim of the reader, without goroutines or timers: rotations between
// reads lose nothing and duplicate nothing, several at once included.
func TestFollowAcrossRotations(t *testing.T) {
	d := newFakeDocker(t, 1000)
	st := &Stats{}
	r := open(t, d, st, cosmicdust.Position{})
	var got []rec
	for b := range 40 {
		d.write(25)
		if b%3 == 2 {
			d.rotate()
		}
		if b%7 == 6 {
			d.rotate() // two rotations before the next read
		}
		if b%2 == 1 {
			got = append(got, next(t, r)...)
		}
	}
	got = append(got, next(t, r)...)
	expectRange(t, got, 1, 1000)
	if st.rotations.Load() == 0 || st.lostFiles.Load() != 0 {
		t.Fatalf("rotations %d lost %d", st.rotations.Load(), st.lostFiles.Load())
	}
}

func TestFollowTruncation(t *testing.T) {
	d := newFakeDocker(t, 5)
	st := &Stats{}
	r := open(t, d, st, cosmicdust.Position{})
	d.write(3)
	expectRange(t, next(t, r), 1, 3)
	if err := os.Truncate(d.path, 0); err != nil {
		t.Fatal(err)
	}
	d.write(2)
	expectRange(t, next(t, r), 4, 5)
	if st.truncations.Load() != 1 {
		t.Fatalf("truncations %d, want 1", st.truncations.Load())
	}
}

func TestResumeInTheSameFile(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(10)
	expectRange(t, next(t, open(t, d, &Stats{}, d.position(4))), 5, 10)
}

// The agent was down while the file rotated twice: the position's file is now
// .2, read from the offset, then .1, then the live one.
func TestResumeAfterRotationsWhileDown(t *testing.T) {
	d := newFakeDocker(t, 10)
	d.write(10)
	resume := d.position(6)
	d.rotate()
	d.write(5)
	d.rotate()
	d.write(5)
	st := &Stats{}
	expectRange(t, next(t, open(t, d, st, resume)), 7, 20)
	if st.lostFiles.Load() != 0 {
		t.Fatalf("lost files %d", st.lostFiles.Load())
	}
}

// Down longer than the retention: the position's file was deleted. What is
// still on disk is read, and the loss counted, not hidden.
func TestResumeWhenTheFileWasRotatedAway(t *testing.T) {
	d := newFakeDocker(t, 1) // max-file=2
	d.write(10)
	resume := d.position(6)
	d.rotate()
	d.write(5)
	d.rotate()
	d.write(5)
	st := &Stats{}
	expectRange(t, next(t, open(t, d, st, resume)), 11, 20)
	if st.lostFiles.Load() != 1 {
		t.Fatalf("lost files %d, want 1", st.lostFiles.Load())
	}
}

// Seen for the first time: every file still on disk is read, oldest first.
func TestFirstSightReadsTheRotatedFilesToo(t *testing.T) {
	d := newFakeDocker(t, 3)
	d.write(4)
	d.rotate()
	d.write(4)
	d.rotate()
	d.write(4)
	expectRange(t, next(t, open(t, d, &Stats{}, cosmicdust.Position{})), 1, 12)
}

func TestResumeWhenTheFirstLineIsLongerThanTheFingerprint(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.raw(entry("stdout", strings.Repeat("z", 5000), true))
	d.write(5)
	resume := d.position(3) // after the long line, line-1 and line-2
	d.rotate()
	d.write(2)
	st := &Stats{}
	expectRange(t, next(t, open(t, d, st, resume)), 3, 7)
	if st.lostFiles.Load() != 0 {
		t.Fatalf("lost files %d", st.lostFiles.Load())
	}
}

// The kernel reuses a freed inode: same inode, another first line, is not
// the file the position was in.
func TestReusedInodeIsNotTheSameFile(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(10)
	resume := d.position(6)
	resume.Head ^= 0xdeadbeef
	st := &Stats{}
	expectRange(t, next(t, open(t, d, st, resume)), 1, 10)
	if st.lostFiles.Load() != 1 {
		t.Fatalf("lost files %d, want 1", st.lostFiles.Load())
	}
}

// While stdout has a part pending, a stderr message must not move the resume
// position past that part.
func TestPendingPartHoldsThePosition(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.raw(entry("stdout", "first half ", false))
	d.raw(entry("stderr", "an error", true))
	r := open(t, d, &Stats{}, cosmicdust.Position{})
	recs := next(t, r)
	if len(recs) != 1 || recs[0].Stream != "stderr" || recs[0].Commit.Offset != 0 || r.Position().Offset != 0 {
		t.Fatalf("records %+v, position %v", recs, r.Position())
	}
	d.raw(entry("stdout", "second half", true))
	recs = next(t, r)
	fi, _ := os.Stat(d.path)
	if len(recs) != 1 || recs[0].text != "first half second half" || recs[0].Parts != 2 ||
		recs[0].Commit.Offset != fi.Size() || r.Position().Offset != fi.Size() {
		t.Fatalf("joined %+v, position %v", recs, r.Position())
	}
}

// A removed container takes its directory with it; the descriptor still reads
// the unlinked file to its end.
func TestUnlinkedFileIsReadToItsEnd(t *testing.T) {
	d := newFakeDocker(t, 5)
	r := open(t, d, &Stats{}, cosmicdust.Position{})
	d.write(3)
	expectRange(t, next(t, r), 1, 3)
	d.write(2)
	if err := os.Remove(d.path); err != nil {
		t.Fatal(err)
	}
	expectRange(t, next(t, r), 4, 5)
}

func TestTornLineAtTheEndOfARotatedFile(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(1)
	resume := d.position(0)
	d.raw(`{"log":"torn in the mid`)
	d.rotate()
	d.write(1)
	st := &Stats{}
	expectRange(t, next(t, open(t, d, st, resume)), 1, 2)
	if st.tornLines.Load() != 1 {
		t.Fatalf("torn %d, want 1", st.tornLines.Load())
	}
}

func TestLineOverTheLimitIsSkipped(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.raw(entry("stdout", strings.Repeat("x", 500), true))
	d.write(1)
	st := &Stats{}
	src := NewSource(SourceOptions{ReadBuf: 16, MaxLine: 100, Logger: slog.New(slog.DiscardHandler)}, st)
	r, err := src.open(cosmicdust.ContainerRef{ID: "c", LogPath: d.path, LogFormat: "json-file"}, cosmicdust.Position{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	expectRange(t, next(t, r), 1, 1)
	if st.overlongLines.Load() != 1 {
		t.Fatalf("over long %d, want 1", st.overlongLines.Load())
	}
}

func TestALogThatDoesNotExistYet(t *testing.T) {
	d := newFakeDocker(t, 5)
	if err := os.Remove(d.path); err != nil {
		t.Fatal(err)
	}
	r := open(t, d, &Stats{}, cosmicdust.Position{})
	if recs := next(t, r); len(recs) != 0 {
		t.Fatalf("records from a missing file: %v", recs)
	}
	d.write(2)
	expectRange(t, next(t, r), 1, 2)
}

func TestUnfinishedLineWaitsForTheRest(t *testing.T) {
	d := newFakeDocker(t, 5)
	r := open(t, d, &Stats{}, cosmicdust.Position{})
	whole := entry("stdout", "line-1", true)
	d.raw(whole[:20])
	if recs := next(t, r); len(recs) != 0 {
		t.Fatalf("a half-written line was returned: %v", recs)
	}
	d.raw(whole[20:])
	expectRange(t, next(t, r), 1, 1)
}

// Records carry the position of their first byte, which is how the control
// plane drops a line read twice.
func TestRecordsCarryTheirPosition(t *testing.T) {
	d := newFakeDocker(t, 5)
	d.write(3)
	recs := next(t, open(t, d, &Stats{}, cosmicdust.Position{}))
	again := next(t, open(t, d, &Stats{}, cosmicdust.Position{}))
	for i := range recs {
		if recs[i].Start != again[i].Start || recs[i].Start.Inode == 0 || recs[i].Start.Head == 0 {
			t.Fatalf("record %d at %+v, read again at %+v", i, recs[i].Start, again[i].Start)
		}
	}
	if recs[0].Start.Offset != 0 || recs[1].Start.Offset != recs[0].Commit.Offset {
		t.Fatalf("offsets %d, %d after commit %d", recs[0].Start.Offset, recs[1].Start.Offset, recs[0].Commit.Offset)
	}
}

func TestRotatedFilesOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.log")
	for _, name := range []string{"0.log.1", "0.log.3", "0.log.2", "0.log.4.gz", "0.log.unrelated"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, compressed, err := rotatedFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, filepath.Base(f.path))
	}
	if strings.Join(got, " ") != "0.log.3 0.log.2 0.log.1" || compressed != 1 {
		t.Fatalf("docker order %v compressed %d", got, compressed)
	}
	dir2 := t.TempDir()
	for _, name := range []string{"0.log.20261021-120500", "0.log.20261021-110000"} {
		if err := os.WriteFile(filepath.Join(dir2, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, _, err = rotatedFiles(filepath.Join(dir2, "0.log"))
	if err != nil || len(files) != 2 || filepath.Base(files[0].path) != "0.log.20261021-110000" {
		t.Fatalf("kubelet order %+v %v", files, err)
	}
}

func TestOpenRejectsWhatIsNotAFile(t *testing.T) {
	src := testSource(&Stats{})
	for _, c := range []cosmicdust.ContainerRef{{ID: "a", LogPath: "/x", LogFormat: "local"}, {ID: "b", LogFormat: "json-file"}} {
		if _, err := src.Open(t.Context(), c, cosmicdust.Position{}); err == nil {
			t.Errorf("opened %+v", c)
		}
	}
}

var _ io.Closer = (*reader)(nil)
var _ cosmicdust.LogSource = (*Source)(nil)
