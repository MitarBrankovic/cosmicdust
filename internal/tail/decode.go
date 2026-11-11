package tail

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// message is one complete log message as a decoder hands it to the reader.
type message struct {
	stream    string
	time      time.Time
	text      []byte // in the reader's arena
	start     cosmicdust.Position
	parts     int
	truncated bool
}

// decoder turns the physical lines of one log format into messages. Both
// formats split a long message into several physical lines, so a decoder
// keeps state between calls.
type decoder interface {
	// decode takes one physical line, without its newline, and where it
	// starts. A completed message's text is appended to *arena.
	decode(raw []byte, at cosmicdust.Position, arena *[]byte) (message, bool, error)

	// oldestPending is the start of the oldest part still waiting for the
	// rest of its message. A resume position must never pass it, or a restart
	// would begin in the middle of that message.
	oldestPending() (cosmicdust.Position, bool)

	// reset drops pending parts: the file was truncated under us and the rest
	// of those messages can no longer arrive.
	reset()
}

// assembler joins the parts of a message per stream; stdout and stderr parts
// can interleave in one file.
type assembler struct {
	limit   int // longest message kept; 0 means no limit
	seq     uint64
	pending map[string]*partial
}

type partial struct {
	seq       uint64 // arrival order, to find the oldest
	start     cosmicdust.Position
	time      time.Time
	buf       []byte
	parts     int
	truncated bool
}

func newAssembler(limit int) *assembler {
	return &assembler{limit: limit, pending: map[string]*partial{}}
}

// add takes one part, whose text the caller has appended to *arena at mark.
// A whole message - the common case - stays where it is. A part moves out of
// the arena into its pending buffer until the last part comes; the limit keeps
// one container from growing the agent without bound, cutting the message and
// counting the rest of its parts without keeping them.
func (a *assembler) add(stream string, t time.Time, arena *[]byte, mark int, final bool, at cosmicdust.Position) (message, bool) {
	text := (*arena)[mark:]
	p, open := a.pending[stream]
	if !open && final {
		m := message{stream: stream, time: t, text: text, start: at, parts: 1}
		if a.limit > 0 && len(text) > a.limit {
			m.text, m.truncated = text[:a.limit], true
			*arena = (*arena)[:mark+a.limit]
		}
		return m, true
	}
	if !open {
		a.seq++
		p = &partial{seq: a.seq, start: at, time: t}
		a.pending[stream] = p
	}
	p.parts++
	if !p.truncated {
		take := len(text)
		if a.limit > 0 && len(p.buf)+take > a.limit {
			take = a.limit - len(p.buf)
			p.truncated = true
		}
		p.buf = append(p.buf, text[:take]...)
	}
	*arena = (*arena)[:mark]
	if !final {
		return message{}, false
	}
	delete(a.pending, stream)
	start := len(*arena)
	*arena = append(*arena, p.buf...)
	return message{stream: stream, time: p.time, text: (*arena)[start:], start: p.start, parts: p.parts, truncated: p.truncated}, true
}

func (a *assembler) oldestPending() (cosmicdust.Position, bool) {
	var oldest *partial
	for _, p := range a.pending {
		if oldest == nil || p.seq < oldest.seq {
			oldest = p
		}
	}
	if oldest == nil {
		return cosmicdust.Position{}, false
	}
	return oldest.start, true
}

func (a *assembler) reset() { clear(a.pending) }

// jsonFileDecoder reads Docker's json-file format.
type jsonFileDecoder struct {
	*assembler
	slow *atomic.Int64 // lines that needed encoding/json
}

type jsonFileEntry struct {
	Log    string    `json:"log"`
	Stream string    `json:"stream"`
	Time   time.Time `json:"time"`
}

func (d *jsonFileDecoder) decode(raw []byte, at cosmicdust.Position, arena *[]byte) (message, bool, error) {
	mark := len(*arena)
	out, stream, ts, ok := parseJSONFile(*arena, raw)
	if !ok {
		var e jsonFileEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return message{}, false, fmt.Errorf("json-file entry: %w", err)
		}
		out, stream, ts = append(out[:mark], e.Log...), e.Stream, e.Time
		if d.slow != nil {
			d.slow.Add(1)
		}
	}
	// Docker ends every complete message with a newline and splits anything
	// longer than 16 KiB into entries without one: no newline means more
	// follows.
	final := len(out) > mark && out[len(out)-1] == '\n'
	if final {
		out = out[:len(out)-1]
		if len(out) > mark && out[len(out)-1] == '\r' {
			out = out[:len(out)-1]
		}
	}
	*arena = out
	m, ok := d.add(stream, ts, arena, mark, final, at)
	return m, ok, nil
}

// criDecoder reads the format the kubelet writes,
//
//	<RFC3339Nano> <stdout|stderr> <F|P>[:flags] <text>
//
// where P marks a part and F the part that completes the message.
type criDecoder struct{ *assembler }

var errCRIFormat = errors.New("not a CRI log line")

func (d *criDecoder) decode(raw []byte, at cosmicdust.Position, arena *[]byte) (message, bool, error) {
	ts, rest, ok := bytes.Cut(raw, []byte{' '})
	if !ok {
		return message{}, false, errCRIFormat
	}
	streamB, rest, ok := bytes.Cut(rest, []byte{' '})
	if !ok {
		return message{}, false, errCRIFormat
	}
	// The text may be empty, and then there may be no separator after the tag.
	tag, text, _ := bytes.Cut(rest, []byte{' '})

	t, ok := parseTime(ts)
	if !ok {
		return message{}, false, fmt.Errorf("CRI timestamp %q: %w", ts, errCRIFormat)
	}
	var stream string
	switch string(streamB) {
	case "stdout":
		stream = "stdout"
	case "stderr":
		stream = "stderr"
	default:
		return message{}, false, fmt.Errorf("CRI stream %q: %w", streamB, errCRIFormat)
	}
	flag, _, _ := bytes.Cut(tag, []byte{':'})
	var final bool
	switch string(flag) {
	case "F":
		final = true
	case "P":
	default:
		return message{}, false, fmt.Errorf("CRI tag %q: %w", tag, errCRIFormat)
	}
	mark := len(*arena)
	*arena = append(*arena, text...)
	m, ok := d.add(stream, t, arena, mark, final, at)
	return m, ok, nil
}
