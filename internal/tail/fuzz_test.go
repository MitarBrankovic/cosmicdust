package tail

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// The decoders read what containers write, which is anything. go test runs the
// seeds; go test -fuzz runs them for as long as one lets it, e.g.
//
//	go test -run '^$' -fuzz FuzzParseJSONFile -fuzztime 2m ./internal/tail/

// The hand-written json-file parser must agree with encoding/json on every
// line it accepts: same text, same stream, same time. Lines it refuses go to
// encoding/json anyway, so a refusal is never wrong, only slower.
func FuzzParseJSONFile(f *testing.F) {
	for _, seed := range []string{
		`{"log":"hello\n","stream":"stdout","time":"2026-10-21T08:00:00.123456789Z"}`,
		`{"log":"tab\tquote\" back\\ slash\/ é中😀\n","stream":"stderr","time":"2026-10-21T08:00:00Z"}`,
		`{"log":"part without newline","stream":"stdout","attrs":{"tag":"x","n":{"a":"}"}},"time":"2026-10-21T08:00:00.5Z"}`,
		`{"log":"","stream":"stdout","time":"2026-10-21T08:00:00.000000001Z"}`,
		`{"stream":"stdout","time":"2026-10-21T08:00:00Z"}`,
		`{"log":"x","stream":"stdin","time":"2026-10-21T08:00:00Z"}`,
		`{"log":"x","stream":"stdout","time":"2026-10-21T08:00:00+01:00"}`,
		`{"log":"\ud800","stream":"stdout","time":"2026-10-21T08:00:00Z"}`,
		`{"log":"x\u0000y","stream":"stdout","time":"2026-10-21T08:00:00Z"}`,
		`not json`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		prefix := []byte("kept")
		out, stream, ts, ok := parseJSONFile(prefix, line)
		if !bytes.HasPrefix(out, prefix) {
			t.Fatalf("the buffer before the text changed: %q", out)
		}
		if !ok {
			if len(out) != len(prefix) {
				t.Fatalf("a refused line left %q behind", out[len(prefix):])
			}
			return
		}
		var e jsonFileEntry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("accepted %q, which encoding/json refuses: %v", line, err)
		}
		if !utf8.Valid(line) {
			return // encoding/json replaces invalid bytes; dockerd never writes them
		}
		if got := string(out[len(prefix):]); got != e.Log || stream != e.Stream || !ts.Equal(e.Time) {
			t.Fatalf("%q: text %q stream %q time %v; encoding/json: %q %q %v", line, got, stream, ts, e.Log,
				e.Stream, e.Time)
		}
	})
}

// The CRI decoder never panics, and a whole message that is one line is
// exactly the text after the tag.
func FuzzCRIDecoder(f *testing.F) {
	for _, seed := range []string{
		"2026-10-21T08:00:00.123456789Z stdout F hello",
		"2026-10-21T08:00:00Z stderr P part one ",
		"2026-10-21T08:00:00Z stdout F:extra text with spaces  ",
		"2026-10-21T08:00:00Z stdout F",
		"2026-10-21T08:00:00Z stdout X bad tag",
		"bad",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		d := &criDecoder{newAssembler(1 << 16)}
		var arena []byte
		m, complete, err := d.decode(raw, cosmicdust.Position{Offset: 1}, &arena)
		if err != nil || !complete {
			return
		}
		fields := strings.SplitN(string(raw), " ", 4)
		want := ""
		if len(fields) == 4 {
			want = fields[3]
		}
		if len(want) > 1<<16 {
			want = want[:1<<16]
		}
		if string(m.text) != want {
			t.Fatalf("%q: message %q, want %q", raw, m.text, want)
		}
	})
}

// hasControl, eight bytes at a time, finds what a byte at a time finds.
func FuzzHasControl(f *testing.F) {
	for _, seed := range []string{"", "plain text", "tab\there", "x\x1fy", "\x7f\x80\xff\x20", "0123456789abcdef\x00"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, p []byte) {
		want := false
		for _, c := range p {
			if c < 0x20 {
				want = true
			}
		}
		if got := hasControl(p); got != want {
			t.Fatalf("%q: %v, want %v", p, got, want)
		}
	})
}

// parseTime reads RFC 3339 in UTC with up to nine fraction digits; whatever
// it accepts, time.Parse reads the same.
func FuzzParseTime(f *testing.F) {
	for _, seed := range []string{"2026-10-21T08:00:00Z", "2026-10-21T08:00:00.123456789Z", "2026-02-30T08:00:00Z",
		"2026-10-21T24:00:00Z", "2026-10-21T08:00:00.1234567891Z", "0000-01-01T00:00:00Z"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, ok := parseTime([]byte(s))
		if !ok {
			return
		}
		want, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("parseTime accepted %q, time.Parse refuses: %v", s, err)
		}
		if !got.Equal(want) {
			t.Fatalf("%q: %v, time.Parse %v", s, got, want)
		}
	})
}
