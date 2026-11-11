package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// A block on disk is a header and a payload:
//
//	magic  uint32  "CDW1"
//	flags  uint32  bit 0: the payload is s2-compressed
//	length uint32  bytes of the stored payload
//	crc    uint32  CRC-32C of the stored payload
//
// so that a crash in the middle of a write is found and cut off. The payload
// is binary (JSON cost five to eight times more to write and to read, a
// third of the prototype agent's CPU at 5,000 lines/s); every integer is a varint:
//
//	version     uvarint, 1
//	at          varint, unix ns when the block was written
//	containers  uvarint n, then per container: id, name, image, app
//	positions   uvarint n, then per entry: container index, inode, head
//	            (uvarints), offset (varint)
//	events      uvarint n, then per event (below)
//	logs        uvarint n, then per row (below)
//
// A container's name, image and application are written once per block, at
// the time its lines were read, so the conversion has them even when the
// tracker has forgotten a container removed long ago. Its state at the time
// of each line is not: that is decided at conversion, when the events that
// came late have arrived.
//
// Positions are where reading each container's log resumes once the block is
// on disk. Data and position in one fsync give a restart without gaps and
// without duplicates (no duplicate after 39 kills).
//
// Log row: ts (varint ns), read_at (varint ns after ts), container index,
// stream and source (one byte each: a known value, or 255 and a string), line,
// inode, head (uvarints), offset (varint), parts (uvarint), truncated (byte).
//
// Event: ts (varint ns), runtime, namespace, action (strings), kind (uvarint),
// container id, name, image, app, state (strings), epoch (varint), exit code
// (varint, -1 for none), flags (byte: 1 OOM-killed, 2 inferred), fields
// (uvarint n, then key and value strings).
//
// A string is a uvarint length and the bytes.

const (
	magic         = 0x43445731 // "CDW1"
	headerSize    = 16
	flagS2        = 1
	formatVersion = 1
)

var errInvalid = errors.New("invalid WAL block")

const other = 255 // a stream or source byte followed by the value as a string

var streams = [...]string{"stdout", "stderr"}
var sources = [...]string{"file-json", "file-cri", "api"}

func code(v string, known []string) byte {
	for i, k := range known {
		if v == k {
			return byte(i)
		}
	}
	return other
}

type encoder struct{ buf []byte }

func (e *encoder) uvarint(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *encoder) varint(v int64)   { e.buf = binary.AppendVarint(e.buf, v) }
func (e *encoder) byte(b byte)      { e.buf = append(e.buf, b) }

func (e *encoder) str(s string) {
	e.uvarint(uint64(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *encoder) bytes(b []byte) {
	e.uvarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *encoder) coded(v string, known []string) {
	c := code(v, known)
	e.byte(c)
	if c == other {
		e.str(v)
	}
}

// encodeRecord appends one log row; container is its index in the block.
func (e *encoder) record(r *cosmicdust.Record, container uint64) {
	ts := r.Time.UnixNano()
	e.varint(ts)
	e.varint(r.ReadAt.UnixNano() - ts)
	e.uvarint(container)
	e.coded(r.Stream, streams[:])
	e.coded(r.Source, sources[:])
	e.bytes(r.Line)
	e.uvarint(r.Start.Inode)
	e.uvarint(r.Start.Head)
	e.varint(r.Start.Offset)
	e.uvarint(uint64(max(r.Parts, 0)))
	if r.Truncated {
		e.byte(1)
	} else {
		e.byte(0)
	}
}

func (e *encoder) event(ev *cosmicdust.Event) {
	e.varint(ev.Time.UnixNano())
	e.str(ev.Runtime)
	e.str(ev.Namespace)
	e.str(ev.Action)
	e.uvarint(uint64(ev.Kind))
	e.str(ev.ContainerID)
	e.str(ev.Name)
	e.str(ev.Image)
	e.str(ev.App)
	e.str(string(ev.State))
	e.varint(int64(ev.Epoch))
	code := int64(-1)
	if ev.ExitCode != nil {
		code = int64(*ev.ExitCode)
	}
	e.varint(code)
	var flags byte
	if ev.OOMKilled {
		flags |= 1
	}
	if ev.Inferred {
		flags |= 2
	}
	e.byte(flags)
	e.uvarint(uint64(len(ev.Fields)))
	for k, v := range ev.Fields {
		e.str(k)
		e.str(v)
	}
}

// Container is a container as a block records it.
type Container struct {
	ID, Name, Image, App string
}

// Row is a log line as a block holds it.
type Row struct {
	TS, ReadAt time.Time
	Container  int // index into Block.Containers
	Stream     string
	Source     string
	Line       string // a slice of the block's payload
	Inode      uint64
	Head       uint64
	Offset     int64
	Parts      int
	Truncated  bool
}

// Block is one block as read back.
type Block struct {
	At         time.Time
	Containers []Container
	Positions  map[string]cosmicdust.Position // by container ID; Path is not kept
	Events     []cosmicdust.Event
	Logs       []Row
}

type decoder struct {
	s   string // the whole payload: lines are slices of it, one allocation per block
	off int
	err error
}

// decodeBlock reads a payload the caller hands over: a buffer of its own that
// nobody writes to again. The block's lines are slices of it, viewed as a
// string without a copy, which halved what reading a segment allocates.
func decodeBlock(payload []byte) (*Block, error) {
	d := decoder{s: unsafe.String(unsafe.SliceData(payload), len(payload))}
	if v := d.uvarint(); d.err == nil && v != formatVersion {
		return nil, fmt.Errorf("%w: format version %d", errInvalid, v)
	}
	b := &Block{At: time.Unix(0, d.varint())}

	b.Containers = make([]Container, d.count())
	for i := range b.Containers {
		c := &b.Containers[i]
		c.ID, c.Name, c.Image, c.App = d.clone(), d.clone(), d.clone(), d.clone()
	}

	n := d.count()
	b.Positions = make(map[string]cosmicdust.Position, n)
	for range n {
		i := d.index(len(b.Containers))
		p := cosmicdust.Position{Inode: d.uvarint(), Head: d.uvarint(), Offset: d.varint()}
		if d.err == nil {
			b.Positions[b.Containers[i].ID] = p
		}
	}

	if n := d.count(); n > 0 {
		b.Events = make([]cosmicdust.Event, n)
		for i := range b.Events {
			ev := &b.Events[i]
			ev.Time = time.Unix(0, d.varint())
			ev.Runtime, ev.Namespace, ev.Action = d.clone(), d.clone(), d.clone()
			ev.Kind = cosmicdust.Kind(d.uvarint())
			ev.ContainerID, ev.Name, ev.Image, ev.App = d.clone(), d.clone(), d.clone(), d.clone()
			ev.State = cosmicdust.State(d.clone())
			ev.Epoch = d.int()
			if code := d.varint(); code >= 0 {
				c := int(code)
				ev.ExitCode = &c
			}
			flags := d.byte()
			ev.OOMKilled, ev.Inferred = flags&1 != 0, flags&2 != 0
			if nf := d.count(); nf > 0 {
				ev.Fields = make(map[string]string, nf)
				for range nf {
					k := d.clone()
					ev.Fields[k] = d.clone()
				}
			}
		}
	}

	if n := d.count(); n > 0 {
		b.Logs = make([]Row, n)
		for i := range b.Logs {
			r := &b.Logs[i]
			ts := d.varint()
			r.TS = time.Unix(0, ts)
			r.ReadAt = time.Unix(0, ts+d.varint())
			r.Container = d.index(len(b.Containers))
			r.Stream = d.coded(streams[:])
			r.Source = d.coded(sources[:])
			r.Line = d.str()
			r.Inode, r.Head, r.Offset = d.uvarint(), d.uvarint(), d.varint()
			r.Parts = d.small()
			r.Truncated = d.byte() == 1
		}
	}

	if d.err == nil && d.off != len(d.s) {
		d.err = fmt.Errorf("%d bytes after the last row", len(d.s)-d.off)
	}
	if d.err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalid, d.err)
	}
	return b, nil
}

var errShort = errors.New("payload cut short")

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	var v uint64
	var shift uint
	for i := 0; ; i++ {
		if d.off >= len(d.s) || i == binary.MaxVarintLen64 {
			d.err = errShort
			return 0
		}
		c := d.s[d.off]
		d.off++
		if c < 0x80 {
			return v | uint64(c)<<shift
		}
		v |= uint64(c&0x7f) << shift
		shift += 7
	}
}

func (d *decoder) varint() int64 {
	u := d.uvarint()
	v := int64(u >> 1)
	if u&1 != 0 {
		v = ^v
	}
	return v
}

func (d *decoder) int() int {
	v := d.varint()
	if v < math.MinInt32 || v > math.MaxInt32 {
		if d.err == nil {
			d.err = fmt.Errorf("value %d out of range", v)
		}
		return 0
	}
	return int(v)
}

// small is a uvarint that must fit an int32.
func (d *decoder) small() int {
	v := d.uvarint()
	if v > math.MaxInt32 {
		if d.err == nil {
			d.err = fmt.Errorf("value %d out of range", v)
		}
		return 0
	}
	return int(v)
}

// count is a length that must fit in what is left of the payload, so that a
// corrupt count cannot make the decoder allocate gigabytes.
func (d *decoder) count() int {
	n := d.uvarint()
	if d.err == nil && n > uint64(len(d.s)-d.off) {
		d.err = fmt.Errorf("count %d larger than the rest of the block", n)
	}
	if d.err != nil {
		return 0
	}
	return int(n)
}

func (d *decoder) index(limit int) int {
	i := d.uvarint()
	if d.err == nil && i >= uint64(limit) {
		d.err = fmt.Errorf("container %d of %d", i, limit)
	}
	if d.err != nil {
		return 0
	}
	return int(i)
}

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if d.off >= len(d.s) {
		d.err = errShort
		return 0
	}
	c := d.s[d.off]
	d.off++
	return c
}

func (d *decoder) str() string {
	n := d.count()
	if d.err != nil {
		return ""
	}
	s := d.s[d.off : d.off+n]
	d.off += n
	return s
}

// clone is a string that may outlive the block, as a map key or in an event:
// as a slice of the payload it would keep the whole block alive.
func (d *decoder) clone() string { return strings.Clone(d.str()) }

func (d *decoder) coded(known []string) string {
	c := d.byte()
	if c == other {
		return d.clone()
	}
	if int(c) >= len(known) {
		if d.err == nil {
			d.err = fmt.Errorf("code %d", c)
		}
		return ""
	}
	return known[c]
}
