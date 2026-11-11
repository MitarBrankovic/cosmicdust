package store

import (
	"strconv"
	"time"
	"unicode/utf8"
)

// LogRow is one log line as it is stored. The struct tags are the Parquet
// schema: dictionary encoding for the columns with few distinct values, delta
// for the ones that grow steadily. App, name, image, state and epoch repeat
// from row to row, so after dictionary and run-length encoding they cost next
// to nothing; the text is 82 % of the space.
//
// State and epoch are the container's when the line was written, not when it
// was stored: stamped at storage time, 39 % of the lines of a backlog stored
// after a crash were marked exited although their containers ran.
//
// Inode, head and offset are where the line starts in its log file: with them
// the control plane drops a line it already has without guessing.
type LogRow struct {
	TS          time.Time `parquet:"ts,timestamp(nanosecond),delta" json:"ts"`
	ReadAt      time.Time `parquet:"read_at,timestamp(nanosecond),delta" json:"read_at"`
	Node        string    `parquet:"node,dict" json:"node"`
	App         string    `parquet:"app,dict" json:"app,omitempty"`
	ContainerID string    `parquet:"container_id,dict" json:"container_id"`
	Name        string    `parquet:"name,dict" json:"name"`
	Image       string    `parquet:"image,dict" json:"image,omitempty"`
	Stream      string    `parquet:"stream,dict" json:"stream"`
	State       string    `parquet:"state,dict" json:"state"`
	Epoch       int32     `parquet:"epoch,dict" json:"epoch"`
	Line        string    `parquet:"line" json:"line"`
	Source      string    `parquet:"source,dict" json:"source"`
	Inode       uint64    `parquet:"inode,dict" json:"inode,omitempty"`
	Head        uint64    `parquet:"head,dict" json:"head,omitempty"`
	Offset      int64     `parquet:"offset,delta" json:"offset,omitempty"`
	Parts       int32     `parquet:"parts" json:"parts,omitempty"`
	Truncated   bool      `parquet:"truncated" json:"truncated,omitempty"`
}

// EventRow is one lifecycle event, with the state it left its container in,
// or one of the store's own: logs_dropped, when it had to delete unsynced
// lines to make room.
type EventRow struct {
	TS          time.Time `parquet:"ts,timestamp(nanosecond),delta" json:"ts"`
	Node        string    `parquet:"node,dict" json:"node"`
	App         string    `parquet:"app,dict" json:"app,omitempty"`
	ContainerID string    `parquet:"container_id,dict" json:"container_id"`
	Name        string    `parquet:"name,dict" json:"name"`
	Image       string    `parquet:"image,dict" json:"image,omitempty"`
	Runtime     string    `parquet:"runtime,dict" json:"runtime"`
	Namespace   string    `parquet:"namespace,dict" json:"namespace,omitempty"`
	Action      string    `parquet:"action,dict" json:"action"`
	Kind        string    `parquet:"kind,dict" json:"kind"`
	State       string    `parquet:"state,dict" json:"state"`
	Epoch       int32     `parquet:"epoch,dict" json:"epoch"`
	ExitCode    int32     `parquet:"exit_code" json:"exit_code"` // -1 when the event carries none
	OOMKilled   bool      `parquet:"oom_killed" json:"oom_killed,omitempty"`
	Inferred    bool      `parquet:"inferred" json:"inferred,omitempty"`
	Fields      string    `parquet:"fields" json:"fields,omitempty"` // a JSON object
}

// The size of rows as JSON Lines, counted instead of encoded: bytes_raw in the
// manifest, the baseline of every compression ratio. It must be the exact
// number json.Encoder writes (TestRawSizeMatchesEncoding); encoding every row
// once more only to count it took 8 % of the prototype agent's CPU.

func (r *LogRow) rawSize() int64 {
	var o jsonObject
	o.field("ts", timeJSONSize(r.TS))
	o.field("read_at", timeJSONSize(r.ReadAt))
	o.field("node", stringJSONSize(r.Node))
	if r.App != "" {
		o.field("app", stringJSONSize(r.App))
	}
	o.field("container_id", stringJSONSize(r.ContainerID))
	o.field("name", stringJSONSize(r.Name))
	if r.Image != "" {
		o.field("image", stringJSONSize(r.Image))
	}
	o.field("stream", stringJSONSize(r.Stream))
	o.field("state", stringJSONSize(r.State))
	o.field("epoch", intJSONSize(int64(r.Epoch)))
	o.field("line", stringJSONSize(r.Line))
	o.field("source", stringJSONSize(r.Source))
	if r.Inode != 0 {
		o.field("inode", uintJSONSize(r.Inode))
	}
	if r.Head != 0 {
		o.field("head", uintJSONSize(r.Head))
	}
	if r.Offset != 0 {
		o.field("offset", intJSONSize(r.Offset))
	}
	if r.Parts != 0 {
		o.field("parts", intJSONSize(int64(r.Parts)))
	}
	if r.Truncated {
		o.field("truncated", 4)
	}
	return o.size()
}

func (r *EventRow) rawSize() int64 {
	var o jsonObject
	o.field("ts", timeJSONSize(r.TS))
	o.field("node", stringJSONSize(r.Node))
	if r.App != "" {
		o.field("app", stringJSONSize(r.App))
	}
	o.field("container_id", stringJSONSize(r.ContainerID))
	o.field("name", stringJSONSize(r.Name))
	if r.Image != "" {
		o.field("image", stringJSONSize(r.Image))
	}
	o.field("runtime", stringJSONSize(r.Runtime))
	if r.Namespace != "" {
		o.field("namespace", stringJSONSize(r.Namespace))
	}
	o.field("action", stringJSONSize(r.Action))
	o.field("kind", stringJSONSize(r.Kind))
	o.field("state", stringJSONSize(r.State))
	o.field("epoch", intJSONSize(int64(r.Epoch)))
	o.field("exit_code", intJSONSize(int64(r.ExitCode)))
	if r.OOMKilled {
		o.field("oom_killed", 4)
	}
	if r.Inferred {
		o.field("inferred", 4)
	}
	if r.Fields != "" {
		o.field("fields", stringJSONSize(r.Fields))
	}
	return o.size()
}

// jsonObject adds up an object field by field: braces, keys, values, commas
// and the newline Encode ends a value with.
type jsonObject struct {
	n      int64
	fields int
}

func (o *jsonObject) field(key string, value int64) {
	o.n += int64(len(key)) + 3 + value // "key":
	o.fields++
}

func (o *jsonObject) size() int64 { return o.n + 2 + int64(max(0, o.fields-1)) + 1 }

// timeJSONSize is the length of the quoted RFC 3339 form MarshalJSON writes:
// 19 characters for date and time, the fraction without trailing zeros, and Z
// or ±hh:mm.
func timeJSONSize(t time.Time) int64 {
	if y := t.Year(); y < 0 || y > 9999 {
		var buf [64]byte
		return int64(len(t.AppendFormat(buf[:0], time.RFC3339Nano))) + 2
	}
	n := int64(2 + 19)
	if ns := t.Nanosecond(); ns != 0 {
		digits := int64(9)
		for ns%10 == 0 {
			ns /= 10
			digits--
		}
		n += 1 + digits
	}
	if _, off := t.Zone(); off == 0 {
		n++
	} else {
		n += 6
	}
	return n
}

func intJSONSize(v int64) int64 {
	var buf [24]byte
	return int64(len(strconv.AppendInt(buf[:0], v, 10)))
}

func uintJSONSize(v uint64) int64 {
	var buf [24]byte
	return int64(len(strconv.AppendUint(buf[:0], v, 10)))
}

// asciiJSONSize is what each ASCII byte becomes inside a JSON string with
// HTML escaping on, as json.Encoder has it by default.
var asciiJSONSize = func() (t [utf8.RuneSelf]uint8) {
	for c := range t {
		switch {
		case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
			t[c] = 2
		case c < 0x20 || c == '<' || c == '>' || c == '&':
			t[c] = 6
		default:
			t[c] = 1
		}
	}
	return t
}()

// stringJSONSize counts U+2028 and U+2029 at six bytes (escaped) and a byte of
// invalid UTF-8 at three (replaced by U+FFFD).
func stringJSONSize(s string) int64 {
	n := int64(2)
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			n += int64(asciiJSONSize[c])
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			n += 3
		case r == ' ' || r == ' ':
			n += 6
		default:
			n += int64(size)
		}
		i += size
	}
	return n
}

// partition is where a row belongs: the hour of its runtime timestamp in UTC.
// By the time the line was written, not when it was read, so a line read
// late, after the agent was down, still lands in its own hour.
func partition(ts time.Time) string { return ts.UTC().Format(partitionLayout) }

const partitionLayout = "dt=2006-01-02/hour=15"

// partitionEnd is the end of the hour a partition stands for.
func partitionEnd(part string) (time.Time, error) {
	t, err := time.Parse(partitionLayout, part)
	if err != nil {
		return time.Time{}, err
	}
	return t.Add(time.Hour), nil
}
