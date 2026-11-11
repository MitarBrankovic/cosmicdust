package tail

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func pos(off int64) cosmicdust.Position { return cosmicdust.Position{Inode: 7, Offset: off} }

func newJSON(limit int) *jsonFileDecoder {
	return &jsonFileDecoder{assembler: newAssembler(limit), slow: &atomic.Int64{}}
}

const ts = "2026-10-21T08:00:00.123456789Z"

func TestJSONFileWholeLine(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	m, ok, err := d.decode([]byte(line("hello\r\n", "stdout", ts, "")), pos(0), &arena)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if string(m.text) != "hello" || m.stream != "stdout" || m.parts != 1 || m.start != pos(0) {
		t.Fatalf("message %+v", m)
	}
}

// dockerd splits anything longer than 16 KiB into entries without a newline.
func TestJSONFilePartsAreJoined(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	for i, part := range []string{"aaa", "bbb", "ccc\n"} {
		m, ok, err := d.decode([]byte(line(part, "stdout", ts, "")), pos(int64(i*100)), &arena)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if ok {
				t.Fatalf("part %d completed a message", i)
			}
			if p, pending := d.oldestPending(); !pending || p != pos(0) {
				t.Fatalf("pending start %v %v, want offset 0", p, pending)
			}
			continue
		}
		if !ok || string(m.text) != "aaabbbccc" || m.parts != 3 || m.start != pos(0) {
			t.Fatalf("joined message %+v", m)
		}
	}
	if _, pending := d.oldestPending(); pending {
		t.Fatal("still pending after the last part")
	}
}

func TestPartsOfTwoStreamsInterleave(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	feed := func(text, stream string, off int64) (message, bool) {
		m, ok, err := d.decode([]byte(line(text, stream, ts, "")), pos(off), &arena)
		if err != nil {
			t.Fatal(err)
		}
		return m, ok
	}
	feed("out-1 ", "stdout", 0)
	feed("err-1 ", "stderr", 100)
	if p, _ := d.oldestPending(); p.Offset != 0 {
		t.Fatalf("oldest pending at %d, want 0", p.Offset)
	}
	m, ok := feed("err-2\n", "stderr", 200)
	if !ok || string(m.text) != "err-1 err-2" {
		t.Fatalf("stderr message %q", m.text)
	}
	if p, _ := d.oldestPending(); p.Offset != 0 {
		t.Fatalf("stdout's part still pending, oldest should be 0, got %d", p.Offset)
	}
	m, ok = feed("out-2\n", "stdout", 300)
	if !ok || string(m.text) != "out-1 out-2" {
		t.Fatalf("stdout message %q", m.text)
	}
}

func TestAssemblerLimitTruncates(t *testing.T) {
	d := newJSON(10)
	var arena []byte
	m, ok, _ := d.decode([]byte(line(strings.Repeat("x", 25)+"\n", "stdout", ts, "")), pos(0), &arena)
	if !ok || len(m.text) != 10 || !m.truncated {
		t.Fatalf("whole line: %d bytes truncated=%v", len(m.text), m.truncated)
	}
	for _, part := range []string{strings.Repeat("a", 6), strings.Repeat("b", 6)} {
		if _, ok, _ := d.decode([]byte(line(part, "stdout", ts, "")), pos(10), &arena); ok {
			t.Fatal("a part completed a message")
		}
	}
	m, ok, _ = d.decode([]byte(line("cc\n", "stdout", ts, "")), pos(20), &arena)
	if !ok || string(m.text) != "aaaaaabbbb" || !m.truncated || m.parts != 3 {
		t.Fatalf("parts: %q truncated=%v parts=%d", m.text, m.truncated, m.parts)
	}
}

func TestAssemblerReset(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	_, _, _ = d.decode([]byte(line("half", "stdout", ts, "")), pos(0), &arena)
	d.reset()
	if _, pending := d.oldestPending(); pending {
		t.Fatal("pending after reset")
	}
	m, ok, _ := d.decode([]byte(line("new\n", "stdout", ts, "")), pos(50), &arena)
	if !ok || string(m.text) != "new" {
		t.Fatalf("after reset: %q", m.text)
	}
}

func TestJSONFileSlowPath(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	m, ok, err := d.decode([]byte(`{"stream":"stderr","time":"`+ts+`","log":"reordered\n"}`), pos(0), &arena)
	if err != nil || !ok || string(m.text) != "reordered" || m.stream != "stderr" {
		t.Fatalf("slow path: %q %v %v", m.text, ok, err)
	}
	if d.slow.Load() != 1 {
		t.Fatalf("slow decodes %d, want 1", d.slow.Load())
	}
	if _, _, err := d.decode([]byte(`{"log": broken`), pos(0), &arena); err == nil {
		t.Fatal("malformed line accepted")
	}
}

// What the arena holds is only what the records reference: a part moved to
// its pending buffer leaves nothing behind.
func TestArenaHoldsOnlyMessages(t *testing.T) {
	d := newJSON(0)
	var arena []byte
	_, _, _ = d.decode([]byte(line("part ", "stdout", ts, "")), pos(0), &arena)
	if len(arena) != 0 {
		t.Fatalf("a pending part left %q in the arena", arena)
	}
	m1, _, _ := d.decode([]byte(line("one\n", "stderr", ts, "")), pos(1), &arena)
	m2, _, _ := d.decode([]byte(line("end\n", "stdout", ts, "")), pos(2), &arena)
	if string(m1.text) != "one" || string(m2.text) != "part end" || string(arena) != "onepart end" {
		t.Fatalf("m1 %q m2 %q arena %q", m1.text, m2.text, arena)
	}
}

func TestCRIDecoder(t *testing.T) {
	d := &criDecoder{newAssembler(0)}
	var arena []byte
	m, ok, err := d.decode([]byte("2026-10-21T10:00:00.5+02:00 stdout F hello world"), pos(0), &arena)
	if err != nil || !ok || string(m.text) != "hello world" || m.stream != "stdout" || m.time.UTC().Hour() != 8 {
		t.Fatalf("full line: %+v %v %v", m, ok, err)
	}
	if _, ok, _ := d.decode([]byte(ts+" stderr P first "), pos(10), &arena); ok {
		t.Fatal("a P part completed a message")
	}
	m, ok, _ = d.decode([]byte(ts+" stderr F:x second"), pos(20), &arena)
	if !ok || string(m.text) != "first second" || m.parts != 2 || m.start != pos(10) {
		t.Fatalf("joined: %+v", m)
	}
	m, ok, _ = d.decode([]byte(ts+" stdout F"), pos(30), &arena)
	if !ok || len(m.text) != 0 {
		t.Fatalf("empty message: %+v", m)
	}
}

func TestCRIDecoderRejects(t *testing.T) {
	d := &criDecoder{newAssembler(0)}
	var arena []byte
	for _, raw := range []string{"no-spaces", ts + " stdin F x", ts + " stdout X x", "yesterday stdout F x", ts} {
		if _, _, err := d.decode([]byte(raw), pos(0), &arena); !errors.Is(err, errCRIFormat) {
			t.Errorf("%q: err %v", raw, err)
		}
	}
}
