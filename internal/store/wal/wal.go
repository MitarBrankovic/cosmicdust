// Package wal is the store's first layer: log records and lifecycle events land
// here in blocks, one fsync each, together with where reading each container's
// log resumes, and are later rewritten into Parquet by whoever consumes the
// sealed segments.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/fsutil"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure a WAL.
type Options struct {
	Dir string

	// A segment is sealed, and handed to the consumer, when it reaches
	// SegmentBytes or has been open SegmentAge. Defaults 8 MiB and 5 s.
	SegmentBytes int64
	SegmentAge   time.Duration

	// BlockBytes caps a block: Add writes the pending one at once past it.
	// Default 4 MiB.
	BlockBytes int

	// Meta names a container (name, image, application) for the blocks that
	// hold its lines; nil records IDs only.
	Meta func(id string) Container

	// Forget drops a resume position not moved for this long, at a release.
	// Default 7 days.
	Forget time.Duration

	// Keep holds released segments back: one with a block written after
	// the time Keep returns stays until Keep has passed it, but no longer
	// than KeepAtMost (default 10 min) from its last block or from the
	// WAL's opening. The lifecycle tracker saves its state a moment after
	// the events it passes on, and replays them from here after a crash;
	// an event that had gone with its segment was inferred a second time
	//. nil holds nothing back.
	Keep       func() time.Time
	KeepAtMost time.Duration

	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

// ErrNoSpace is wrapped by a write that failed for lack of disk space. The
// block was cut back off the segment and waits in memory for the next Flush:
// nothing is lost and nothing is written twice.
var ErrNoSpace = errors.New("wal: no space left on the device")

var errHeld = errors.New("wal: held until the store has room")

// Hold stops writing blocks while the store has no room (its policy is to
// block rather than evict, or eviction found nothing more to delete): Flush
// then fails the way a full disk does, the tailer waits, and events wait in
// memory. Released, the waiting block goes out with the next tick.
func (w *WAL) Hold(on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.held = on
}

// DiskBytes is the WAL on disk: what waits for conversion, the open segment
// included. The store keeps room to convert it.
func (w *WAL) DiskBytes() int64 {
	w.mu.Lock()
	ids := append(slices.Clone(w.sealed), w.id)
	w.mu.Unlock()
	var n int64
	for _, id := range ids {
		if st, err := os.Stat(w.segmentPath(id)); err == nil {
			n += st.Size()
		}
	}
	return n
}

// ErrClosed is returned once the WAL is closed.
var ErrClosed = errors.New("wal: closed")

type noSpace struct{ err error }

func (e noSpace) Error() string   { return e.err.Error() }
func (e noSpace) Unwrap() []error { return []error{ErrNoSpace, e.err} }
func (e noSpace) Temporary() bool { return true }

// WAL is safe for concurrent use: the tailer adds records, the event writer
// events, the consumer reads and releases sealed segments.
type WAL struct {
	opts Options
	log  *slog.Logger
	m    *metrics

	mu          sync.Mutex
	closed      bool
	f           *os.File
	id          uint64 // the open segment
	size        int64
	opened      time.Time
	rollPending bool // a roll after a full segment failed; tick retries
	failed      bool // the last write failed; the pending block waits
	held        bool // the store has no room: blocks wait in memory (Hold)

	p       pending
	payload []byte // reused from block to block (half the gain without it)
	frame   []byte
	order   []uint64

	// failWrite, in tests, stands in front of the segment's Write to make it
	// fail the way a full disk does.
	failWrite func(f *os.File, b []byte) (int, error)

	positions map[string]position            // as of the last block written
	released  uint64                         // segments up to this one are gone
	release   uint64                         // the consumer is done with segments up to this one
	written   map[uint64]time.Time           // when each segment on disk had its last block
	start     time.Time                      // when the WAL was opened
	base      map[string]position            // positions as of released
	deltas    map[uint64]map[string]position // what each segment still on disk moved
	sealed    []uint64                       // sealed, not released, in order
}

type position struct {
	Inode  uint64    `json:"inode"`
	Head   uint64    `json:"head"`
	Offset int64     `json:"offset"`
	At     time.Time `json:"at"` // when the block that set it was written
}

func (p position) public() cosmicdust.Position {
	return cosmicdust.Position{Inode: p.Inode, Head: p.Head, Offset: p.Offset}
}

// pending is the block being put together.
type pending struct {
	ids        map[string]uint64
	containers []Container
	positions  map[uint64]cosmicdust.Position // by container index
	logs       encoder
	nLogs      int
	events     encoder
	nEvents    int
}

func (p *pending) empty() bool { return p.nLogs == 0 && p.nEvents == 0 && len(p.positions) == 0 }

func (p *pending) reset() {
	clear(p.ids)
	p.containers = p.containers[:0]
	clear(p.positions)
	p.logs.buf, p.nLogs = p.logs.buf[:0], 0
	p.events.buf, p.nEvents = p.events.buf[:0], 0
}

func (p *pending) container(id string, meta func(string) Container) uint64 {
	if i, ok := p.ids[id]; ok {
		return i
	}
	c := Container{ID: id}
	if meta != nil {
		c = meta(id)
		c.ID = id
	}
	i := uint64(len(p.containers))
	p.ids[id] = i
	p.containers = append(p.containers, c)
	return i
}

const checkpointFile = "checkpoint.json"

type checkpoint struct {
	Version   int                 `json:"version"`
	Released  uint64              `json:"released"`
	Positions map[string]position `json:"positions"`
}

// Open recovers the WAL in opts.Dir and starts a new segment. It never appends
// to a segment it found: the newest one may just have been cut back, and a
// fresh segment keeps "sealed" simple - every segment but the open one is
// final.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("wal: Options.Dir is empty")
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 8 << 20
	}
	if opts.SegmentAge <= 0 {
		opts.SegmentAge = 5 * time.Second
	}
	if opts.BlockBytes <= 0 {
		opts.BlockBytes = 4 << 20
	}
	if opts.Forget <= 0 {
		opts.Forget = 7 * 24 * time.Hour
	}
	if opts.KeepAtMost <= 0 {
		opts.KeepAtMost = 10 * time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(opts.Dir, 0o750); err != nil {
		return nil, err
	}
	w := &WAL{
		opts:      opts,
		log:       opts.Logger,
		positions: map[string]position{},
		base:      map[string]position{},
		deltas:    map[uint64]map[string]position{},
		written:   map[uint64]time.Time{},
		start:     time.Now(),
		p:         pending{ids: map[string]uint64{}, positions: map[uint64]cosmicdust.Position{}},
	}
	m, collectors := newMetrics(w)
	w.m = m
	if opts.Registerer != nil {
		for _, c := range collectors {
			if err := opts.Registerer.Register(c); err != nil {
				return nil, fmt.Errorf("wal: register metrics: %w", err)
			}
		}
	}
	if err := w.recover(); err != nil {
		return nil, err
	}
	next := w.released + 1
	if len(w.sealed) > 0 {
		next = w.sealed[len(w.sealed)-1] + 1
	}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) segmentPath(id uint64) string {
	return filepath.Join(w.opts.Dir, fmt.Sprintf("%020d.wal", id))
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".wal")
		if !ok {
			continue
		}
		if id, err := strconv.ParseUint(name, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// recover reads the checkpoint and every segment after it. The newest segment
// may end in half a block if the process died while writing; it is cut back to
// its last whole block. A bad block in an older segment cannot come from a
// crash - those were complete and synced before the next one was opened - so
// it is corruption: reported, counted, and the segment kept up to that point.
func (w *WAL) recover() error {
	cp, err := readCheckpoint(filepath.Join(w.opts.Dir, checkpointFile))
	if err != nil {
		return err
	}
	w.released, w.base = cp.Released, cp.Positions
	ids, err := listSegments(w.opts.Dir)
	if err != nil {
		return err
	}
	for i, id := range ids {
		path := w.segmentPath(id)
		if id <= w.released {
			// Released, but the process died before it was deleted.
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		delta := map[string]position{}
		valid, err := readSegment(path, func(b *Block) error {
			for c, p := range b.Positions {
				delta[c] = position{Inode: p.Inode, Head: p.Head, Offset: p.Offset, At: b.At}
			}
			return nil
		})
		if err == nil && valid == 0 {
			// An empty segment: opened, never written. Nothing to keep.
			if err := os.Remove(path); err != nil {
				return err
			}
			continue
		}
		switch {
		case err == nil:
		case !errors.Is(err, errInvalid):
			return err
		case i == len(ids)-1:
			w.m.tornTails.Inc()
			w.log.Warn("cutting a torn block off the newest WAL segment", "segment", id, "valid_bytes", valid, "reason", err)
			if err := truncateSync(path, valid); err != nil {
				return err
			}
		default:
			w.m.corrupt.Inc()
			w.log.Error("corrupt WAL segment; what follows the damage is lost", "segment", id, "valid_bytes", valid, "reason", err)
		}
		w.deltas[id] = delta
		w.sealed = append(w.sealed, id)
		if fi, err := os.Stat(path); err == nil {
			w.written[id] = fi.ModTime()
		}
	}
	w.release = w.released
	maps.Copy(w.positions, w.base)
	for _, id := range w.sealed {
		maps.Copy(w.positions, w.deltas[id])
	}
	if len(ids) > 0 || len(w.base) > 0 {
		w.log.Info("WAL recovered", "segments", len(w.sealed), "released", w.released, "positions", len(w.positions))
	}
	return nil
}

func readCheckpoint(path string) (checkpoint, error) {
	cp := checkpoint{Positions: map[string]position{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cp, nil
	}
	if err != nil {
		return cp, err
	}
	if err := json.Unmarshal(data, &cp); err != nil {
		return cp, fmt.Errorf("wal checkpoint %s: %w", path, err)
	}
	if cp.Positions == nil {
		cp.Positions = map[string]position{}
	}
	return cp, nil
}

func truncateSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

// openSegment creates the next segment and makes it the open one. On an error
// the open segment stays, and a half-made file is removed, so that the next
// attempt - after the disk has room again - can create it.
func (w *WAL) openSegment(id uint64) error {
	path := w.segmentPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := syncDir(w.opts.Dir); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	w.f, w.id, w.size, w.opened = f, id, 0, time.Now()
	return nil
}

// roll opens the next segment before it closes the current one: with a full
// disk the WAL keeps a segment to write to.
func (w *WAL) roll() error {
	old, oldID := w.f, w.id
	if err := w.openSegment(w.id + 1); err != nil {
		return err
	}
	w.rollPending = false
	w.sealed = append(w.sealed, oldID)
	return old.Close()
}

// Add takes log records into the pending block, copying them: their lines are
// only valid during the call. A pending block past Options.BlockBytes is
// written at once.
func (w *WAL) Add(recs []cosmicdust.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	for i := range recs {
		r := &recs[i]
		c := w.p.container(r.ContainerID, w.opts.Meta)
		w.p.logs.record(r, c)
		w.p.nLogs++
		w.p.positions[c] = r.Commit
	}
	if len(w.p.logs.buf) >= w.opts.BlockBytes && !w.failed {
		return w.flushLocked()
	}
	return nil
}

// AddEvents takes lifecycle events into the pending block.
func (w *WAL) AddEvents(evs []cosmicdust.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	for i := range evs {
		w.p.events.event(&evs[i])
		w.p.nEvents++
	}
	return nil
}

// Flush writes the pending block and syncs it. When it returns nil, the
// records, the events and the resume positions in it survive a crash.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.flushLocked()
}

func (w *WAL) flushLocked() error {
	if w.p.empty() {
		return nil
	}
	if w.held {
		w.failed = true
		return noSpace{err: errHeld}
	}
	now := time.Now()
	frame, raw := w.frameBlock(now)
	if err := w.writeFrame(frame); err != nil {
		return w.undo(err)
	}
	start := time.Now()
	if err := w.f.Sync(); err != nil {
		return w.undo(err)
	}
	w.m.fsync.Observe(time.Since(start).Seconds())
	w.size += int64(len(frame))
	w.failed = false
	w.written[w.id] = now

	delta := w.deltas[w.id]
	if delta == nil {
		delta = map[string]position{}
		w.deltas[w.id] = delta
	}
	for c, p := range w.p.positions {
		pos := position{Inode: p.Inode, Head: p.Head, Offset: p.Offset, At: now}
		id := w.p.containers[c].ID
		w.positions[id] = pos
		delta[id] = pos
	}
	w.m.blocks.Inc()
	w.m.bytes.Add(float64(len(frame)))
	w.m.payload.Add(float64(raw))
	w.m.logs.Add(float64(w.p.nLogs))
	w.m.events.Add(float64(w.p.nEvents))
	w.p.reset()
	w.trim(raw)

	if w.size >= w.opts.SegmentBytes {
		// The block is durable whatever happens now; a failed roll must not
		// make the caller write it again. tick tries again.
		if err := w.roll(); err != nil {
			w.rollPending = true
			w.log.Warn("cannot open the next WAL segment; will retry", "err", err)
		}
	}
	return nil
}

// trim lets go of the block buffers when they are far bigger than the block
// just written needed: a block of a backlog, or of a pass that came late, grew
// them, and they stayed at that size. At 5,000 lines/s, where a block is ~400
// KB, the three held ~10 MB.
func (w *WAL) trim(raw int) {
	limit := max(4*raw, 1<<20)
	if cap(w.payload) > limit {
		w.payload = nil
	}
	if cap(w.frame) > limit {
		w.frame = nil
	}
	if cap(w.p.logs.buf) > limit {
		w.p.logs.buf = nil
	}
}

func (w *WAL) writeFrame(b []byte) error {
	if w.failWrite != nil {
		if _, err := w.failWrite(w.f, b); err != nil {
			return err
		}
	}
	_, err := w.f.Write(b)
	return err
}

// frameBlock builds header and payload, s2-compressed straight behind the
// header, in buffers kept from one block to the next.
func (w *WAL) frameBlock(at time.Time) ([]byte, int) {
	e := encoder{buf: w.payload[:0]}
	e.uvarint(formatVersion)
	e.varint(at.UnixNano())
	e.uvarint(uint64(len(w.p.containers)))
	for _, c := range w.p.containers {
		e.str(c.ID)
		e.str(c.Name)
		e.str(c.Image)
		e.str(c.App)
	}
	w.order = w.order[:0] // in order: the same content, the same block
	for c := range w.p.positions {
		w.order = append(w.order, c)
	}
	slices.Sort(w.order)
	e.uvarint(uint64(len(w.order)))
	for _, c := range w.order {
		p := w.p.positions[c]
		e.uvarint(c)
		e.uvarint(p.Inode)
		e.uvarint(p.Head)
		e.varint(p.Offset)
	}
	e.uvarint(uint64(w.p.nEvents))
	e.buf = append(e.buf, w.p.events.buf...)
	e.uvarint(uint64(w.p.nLogs))
	e.buf = append(e.buf, w.p.logs.buf...)
	w.payload = e.buf

	need := headerSize + s2.MaxEncodedLen(len(w.payload))
	if cap(w.frame) < need {
		w.frame = make([]byte, need)
	}
	body := s2.Encode(w.frame[headerSize:need], w.payload)
	frame := w.frame[:headerSize+len(body)]
	binary.LittleEndian.PutUint32(frame[0:], magic)
	binary.LittleEndian.PutUint32(frame[4:], flagS2)
	binary.LittleEndian.PutUint32(frame[8:], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[12:], crc32.Checksum(body, castagnoli))
	return frame, len(w.payload)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// undo cuts the open segment back to its last whole block after a failed
// write: a torn block in the middle of a segment would cost every block after
// it at recovery. The pending block stays for the next Flush.
func (w *WAL) undo(err error) error {
	w.failed = true
	if terr := w.f.Truncate(w.size); terr != nil {
		err = fmt.Errorf("%w (and cutting the segment back failed: %v)", err, terr)
	} else if _, serr := w.f.Seek(w.size, io.SeekStart); serr != nil {
		err = fmt.Errorf("%w (and seeking back failed: %v)", err, serr)
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		w.m.noSpace.Inc()
		return noSpace{err: fmt.Errorf("wal: write segment %d: %w", w.id, err)}
	}
	return fmt.Errorf("wal: write segment %d: %w", w.id, err)
}

// tick seals a segment that is old enough, so that a quiet node still hands
// its lines on within SegmentAge, and retries a block that could not be
// written.
func (w *WAL) tick() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if w.failed && !w.p.empty() {
		if err := w.flushLocked(); err != nil {
			w.log.Debug("the pending WAL block still cannot be written", "err", err)
			return
		}
	}
	if w.size > 0 && (w.rollPending || time.Since(w.opened) >= w.opts.SegmentAge) {
		if err := w.roll(); err != nil {
			w.rollPending = true
			w.log.Warn("cannot seal the WAL segment; will retry", "err", err)
		}
	}
	if w.release > w.released {
		if err := w.releaseLocked(); err != nil {
			w.log.Warn("cannot release WAL segments; will retry", "err", err)
		}
	}
}

// Seal closes the open segment for writing and starts the next, so that what
// it holds can be converted now rather than when it is old enough. An empty
// segment is left open.
func (w *WAL) Seal() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.size == 0 {
		return nil
	}
	return w.roll()
}

// Position is where reading container id's log resumes: past the last record
// written to the WAL. false: the WAL has never held a line of it.
func (w *WAL) Position(id string) (cosmicdust.Position, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.positions[id]
	return p.public(), ok
}

// Sealed lists the segments that will not change any more and have not been
// released, oldest first.
func (w *WAL) Sealed() []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.sealed)
}

// Read calls fn for every block of a sealed segment, in order. A block that
// does not check out ends the reading with an error wrapping errInvalid;
// everything before it has been delivered.
func (w *WAL) Read(segment uint64, fn func(*Block) error) error {
	_, err := readSegment(w.segmentPath(segment), fn)
	return err
}

// IsCorrupt reports whether an error from Read is a damaged block, as opposed
// to a failure to read the file or an error from fn.
func IsCorrupt(err error) bool { return errors.Is(err, errInvalid) }

// Release deletes the segments up to through: the consumer has committed what
// they held. Before they go, the resume positions as of through are written
// to the checkpoint, so that a restart still finds them. Segments Keep holds
// back go later, from tick.
func (w *WAL) Release(through uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if through <= w.release {
		return nil
	}
	if through >= w.id {
		return fmt.Errorf("wal: release through %d: segment %d is still open", through, w.id)
	}
	w.release = through
	return w.releaseLocked()
}

// releaseLocked deletes what Release asked for, as far as Keep lets it.
func (w *WAL) releaseLocked() error {
	through := w.release
	if w.opts.Keep != nil {
		keep, now := w.opts.Keep(), time.Now()
		for _, id := range w.sealed {
			if id > through {
				break
			}
			last := w.written[id]
			if last.After(keep) && now.Sub(later(last, w.start)) < w.opts.KeepAtMost {
				through = id - 1
				break
			}
		}
	}
	if through <= w.released {
		return nil
	}
	base := maps.Clone(w.base)
	var gone []uint64
	for _, id := range w.sealed {
		if id > through {
			break
		}
		maps.Copy(base, w.deltas[id])
		gone = append(gone, id)
	}
	cutoff := time.Now().Add(-w.opts.Forget)
	maps.DeleteFunc(base, func(_ string, p position) bool { return p.At.Before(cutoff) })

	data, err := json.Marshal(checkpoint{Version: 1, Released: through, Positions: base})
	if err != nil {
		return err
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(w.opts.Dir, checkpointFile), data, 0o600); err != nil {
		return err
	}
	for _, id := range gone {
		if err := os.Remove(w.segmentPath(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		delete(w.deltas, id)
		delete(w.written, id)
	}
	if err := syncDir(w.opts.Dir); err != nil {
		return err
	}
	w.base, w.released = base, through
	w.sealed = w.sealed[len(gone):]
	w.m.released.Add(float64(len(gone)))
	return nil
}

// Close writes the pending block and closes the open segment. A block that
// cannot be written is lost with the process, as on a crash: its lines are
// read again after the restart, from the last position written.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	err := w.flushLocked()
	w.closed = true
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if w.size == 0 && err == nil {
		// Never written: the next start would only find it empty.
		err = os.Remove(w.segmentPath(w.id))
	}
	return err
}

// readSegment calls fn for every valid block and returns how many bytes of the
// file are valid. It stops at the first block that is short, fails its
// checksum or does not decode, and reports that with errInvalid.
func readSegment(path string, fn func(*Block) error) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var off int64
	for off < int64(len(data)) {
		rest := data[off:]
		if len(rest) < headerSize {
			return off, fmt.Errorf("%w: %d bytes of a header", errInvalid, len(rest))
		}
		if binary.LittleEndian.Uint32(rest[0:]) != magic {
			return off, fmt.Errorf("%w: bad magic at %d", errInvalid, off)
		}
		flags := binary.LittleEndian.Uint32(rest[4:])
		length := int64(binary.LittleEndian.Uint32(rest[8:]))
		sum := binary.LittleEndian.Uint32(rest[12:])
		if int64(len(rest)) < headerSize+length {
			return off, fmt.Errorf("%w: payload cut short at %d", errInvalid, off)
		}
		payload := rest[headerSize : headerSize+length]
		if crc32.Checksum(payload, castagnoli) != sum {
			return off, fmt.Errorf("%w: checksum mismatch at %d", errInvalid, off)
		}
		if flags&flagS2 != 0 {
			if payload, err = s2.Decode(nil, payload); err != nil {
				return off, fmt.Errorf("%w: s2: %v", errInvalid, err)
			}
		}
		b, err := decodeBlock(payload)
		if err != nil {
			return off, err
		}
		if err := fn(b); err != nil {
			return off, err
		}
		off += headerSize + length
	}
	return off, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// segmentsOnDisk counts the segment files, for the metric.
func (w *WAL) segmentsOnDisk() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.sealed) + 1
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
