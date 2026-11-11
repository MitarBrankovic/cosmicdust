package tail

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// line builds a json-file line the way dockerd writes it.
func line(log, stream, ts string, attrs string) string {
	b, _ := json.Marshal(log) // encoding/json escapes as dockerd does, <>& included
	s := `{"log":` + string(b) + `,"stream":"` + stream + `"`
	if attrs != "" {
		s += `,"attrs":` + attrs
	}
	return s + `,"time":"` + ts + `"}`
}

func TestParseJSONFile(t *testing.T) {
	ts := "2026-10-21T08:00:00.123456789Z"
	want := time.Date(2026, 10, 21, 8, 0, 0, 123456789, time.UTC)
	cases := []struct {
		name, raw, text, stream string
	}{
		{"plain", line("hello\n", "stdout", ts, ""), "hello\n", "stdout"},
		{"stderr", line("oops\n", "stderr", ts, ""), "oops\n", "stderr"},
		{"escapes", line("a \"q\" \\ \t tab\r\n", "stdout", ts, ""), "a \"q\" \\ \t tab\r\n", "stdout"},
		{"html escaped by dockerd", line("<a href='x'>&</a>\n", "stdout", ts, ""), "<a href='x'>&</a>\n", "stdout"},
		{"control characters", line("bell\a nul\x00\n", "stdout", ts, ""), "bell\a nul\x00\n", "stdout"},
		{"unicode", line("ćirilica ћирилица 😀\n", "stdout", ts, ""), "ćirilica ћирилица 😀\n", "stdout"},
		{"line separator", line("a b\n", "stdout", ts, ""), "a b\n", "stdout"},
		{"a part, no newline", line("first 16 KiB", "stdout", ts, ""), "first 16 KiB", "stdout"},
		{"attrs", line("x\n", "stdout", ts, `{"tag":"a}b\"{","app":"web"}`), "x\n", "stdout"},
		{"empty message", line("", "stdout", ts, ""), "", "stdout"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := []byte("prefix|")
			out, stream, got, ok := parseJSONFile(dst, []byte(c.raw))
			if !ok {
				t.Fatalf("not parsed: %s", c.raw)
			}
			if string(out) != "prefix|"+c.text || stream != c.stream || !got.Equal(want) {
				t.Fatalf("got %q %q %v", out, stream, got)
			}
		})
	}
}

func TestParseJSONFileUnicodeEscapes(t *testing.T) {
	raw := `{"log":"ć😀<� lone \ud800 end\n","stream":"stdout","time":"2026-10-21T08:00:00Z"}`
	out, _, ts, ok := parseJSONFile(nil, []byte(raw))
	if !ok {
		t.Fatal("not parsed")
	}
	var e jsonFileEntry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	if string(out) != e.Log {
		t.Fatalf("fast %q, encoding/json %q", out, e.Log)
	}
	if !ts.Equal(time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("time %v", ts)
	}
}

func TestParseJSONFileRejects(t *testing.T) {
	ts := "2026-10-21T08:00:00Z"
	for _, raw := range []string{
		`{"stream":"stdout","log":"x\n","time":"` + ts + `"}`, // another order
		`{"log":"x\n","stream":"stdin","time":"` + ts + `"}`,
		`{"log":"x\n","stream":"stdout"}`,
		`{"log":"x\n","stream":"stdout","time":"` + ts + `","extra":1}`,
		`{"log":"bad \x escape","stream":"stdout","time":"` + ts + `"}`,
		`{"log":"unterminated,"stream":"stdout","time":"` + ts + `"}`,
		`{"log":"x\n","stream":"stdout","time":"yesterday"}`,
		`not json at all`,
		``,
	} {
		dst := []byte("keep")
		out, _, _, ok := parseJSONFile(dst, []byte(raw))
		if ok {
			t.Errorf("accepted %s", raw)
		}
		if string(out) != "keep" {
			t.Errorf("a rejected line left %q behind", out)
		}
	}
}

func TestParseTime(t *testing.T) {
	cases := map[string]time.Time{
		"2026-10-21T08:00:00Z":           time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC),
		"2026-10-21T08:00:00.5Z":         time.Date(2026, 10, 21, 8, 0, 0, 500000000, time.UTC),
		"2026-10-21T08:00:00.000000001Z": time.Date(2026, 10, 21, 8, 0, 0, 1, time.UTC),
		"2026-10-21T10:00:00.25+02:00":   time.Date(2026, 10, 21, 8, 0, 0, 250000000, time.UTC), // the kubelet's local time
		"2026-10-21T08:00:00.123456789Z": time.Date(2026, 10, 21, 8, 0, 0, 123456789, time.UTC),
	}
	for in, want := range cases {
		got, ok := parseTime([]byte(in))
		if !ok || !got.Equal(want) {
			t.Errorf("parseTime(%s) = %v %v, want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"2026-13-01T00:00:00Z", "2026-10-21T08:00:00.Z", "2026-10-21 08:00:00Z"} {
		if _, ok := parseTime([]byte(in)); ok {
			t.Errorf("parseTime(%s) accepted", in)
		}
	}
}

func TestParseJSONFileDoesNotAllocate(t *testing.T) {
	raw := []byte(line("GET /api/v1/items?page=3 200 12ms \"Mozilla/5.0\"\n", "stdout", "2026-10-21T08:00:00.123456789Z", ""))
	dst := make([]byte, 0, 4096)
	allocs := testing.AllocsPerRun(100, func() {
		_, _, _, _ = parseJSONFile(dst[:0], raw)
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per line, want 0", allocs)
	}
}

var benchLine = []byte(line(strings.Repeat("x", 40)+" GET /api/v1/items?page=3 200 12ms \"Mozilla/5.0\"\n",
	"stdout", "2026-10-21T08:00:00.123456789Z", ""))

func BenchmarkJSONFileFast(b *testing.B) {
	dst := make([]byte, 0, 4096)
	b.SetBytes(int64(len(benchLine)))
	b.ReportAllocs()
	for b.Loop() {
		_, _, _, _ = parseJSONFile(dst[:0], benchLine)
	}
}

func BenchmarkJSONFileEncodingJSON(b *testing.B) {
	b.SetBytes(int64(len(benchLine)))
	b.ReportAllocs()
	for b.Loop() {
		var e jsonFileEntry
		_ = json.Unmarshal(benchLine, &e)
	}
}
