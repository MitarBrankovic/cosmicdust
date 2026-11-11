package tail

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Source opens container logs from the files the runtime writes: json-file
// (Docker, nerdctl) and CRI (the kubelet). A file is ~2.4x cheaper for the node
// than the Docker API, resumes to the byte, and keeps long lines whole; the
// FIFO the runtime writes into is not used, because a slow reader there would
// block the container's writes.
//
// The readers of one Source share a read buffer, so they are not safe for
// concurrent use with each other; a record's Line, which points into the
// reader's own arena, stays valid until that reader's next Next.
type Source struct {
	opts  SourceOptions
	stats *Stats
	buf   *readBuf
}

// SourceOptions configure how files are read.
type SourceOptions struct {
	ReadBuf    int // the read buffer, shared by every file; default 16 KiB
	MaxLine    int // longest physical line kept; longer ones are skipped and counted. Default 1 MiB
	MaxMessage int // longest message kept; longer ones are cut. Default 256 KiB
	Logger     *slog.Logger
}

// NewSource returns a Source counting into stats (nil: a private one).
func NewSource(opts SourceOptions, stats *Stats) *Source {
	if opts.ReadBuf <= 0 {
		opts.ReadBuf = 16 << 10
	}
	if opts.MaxLine <= 0 {
		opts.MaxLine = 1 << 20
	}
	if opts.MaxMessage <= 0 {
		opts.MaxMessage = 256 << 10
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if stats == nil {
		stats = &Stats{}
	}
	return &Source{opts: opts, stats: stats, buf: newReadBuf(opts.ReadBuf)}
}

// ErrUnsupported is returned for a log that is not a file this source reads:
// another driver (local, journald, ...) or no path yet.
var ErrUnsupported = errors.New("log is not a json-file or CRI file")

// Readable reports whether Open can read c's log.
func Readable(c cosmicdust.ContainerRef) bool {
	return c.LogPath != "" && (c.LogFormat == "json-file" || c.LogFormat == "cri")
}

// Open starts reading c's log at from, or, with a zero position, at the start
// of the oldest of its files still on disk. Nothing is opened until the first
// Next: the file may not exist yet.
func (s *Source) Open(ctx context.Context, c cosmicdust.ContainerRef, from cosmicdust.Position) (cosmicdust.LogReader, error) {
	return s.open(c, from)
}

func (s *Source) open(c cosmicdust.ContainerRef, from cosmicdust.Position) (*reader, error) {
	if !Readable(c) {
		return nil, fmt.Errorf("%s (%q): %w", c.ID, c.LogFormat, ErrUnsupported)
	}
	r := &reader{
		ref:     c,
		maxLine: s.opts.MaxLine,
		stats:   s.stats,
		log:     s.opts.Logger.With("container", short(c.ID), "name", c.Name),
		now:     time.Now,
	}
	asm := newAssembler(s.opts.MaxMessage)
	if c.LogFormat == "cri" {
		r.source, r.dec = "file-cri", &criDecoder{asm}
	} else {
		r.source, r.dec = "file-json", &jsonFileDecoder{assembler: asm, slow: &s.stats.slowDecodes}
	}
	r.plan = plan{path: c.LogPath, buf: s.buf, stats: s.stats,
		warnLost: func(resume *cosmicdust.Position, remaining int) {
			r.log.Warn("resume position was rotated away; reading every file still on disk",
				"inode", resume.Inode, "offset", resume.Offset, "files", remaining)
		}}
	if from != (cosmicdust.Position{}) {
		p := from
		r.resume = &p
	}
	return r, nil
}

// reader follows one container's log across rotations. It is not safe for
// concurrent use.
type reader struct {
	ref     cosmicdust.ContainerRef
	source  string
	dec     decoder
	plan    plan
	maxLine int
	stats   *Stats
	log     *slog.Logger
	now     func() time.Time

	resume *cosmicdust.Position // where to start, until the files are opened
	files  []*openFile          // oldest first; the last one is the live file
	arena  []byte               // the text of the records the last Next returned
}

var (
	errUnfinished  = errors.New("line not finished yet")
	errLineTooLong = errors.New("line longer than the limit")
)

// Next fills buf with the messages completed since the last call and returns
// how many. It never waits: with nothing new it returns 0. The records' Line
// fields point into the reader's buffer and are valid until the next call.
func (r *reader) Next(buf []cosmicdust.Record) (int, error) {
	// A batch from a backlog grows the arena to its size; once the file reads
	// little again the arena goes, or every container that ever had a backlog
	// would keep a full batch of text.
	if cap(r.arena) > 64<<10 && len(r.arena) < cap(r.arena)/4 {
		r.arena = nil
	}
	r.arena = r.arena[:0]
	if r.files == nil {
		files, err := r.plan.open(r.resume, false)
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil // created, not written yet; or between a rename and its create
		}
		if err != nil {
			return 0, err
		}
		r.files, r.resume = files, nil
	}
	n := 0
	for n < len(buf) {
		cur := r.files[0]
		got, err := r.drain(cur, buf[n:])
		n += got
		if err != nil || n == len(buf) {
			return n, err
		}
		if !cur.live {
			// A rotated file left from before: it will not grow again.
			r.finish(cur)
			r.files = r.files[1:]
			continue
		}
		switch r.check(cur) {
		case stateRotated:
			// What was written between our last read and the rename is still
			// in the old file: read it once more before letting go.
			got, err := r.drain(cur, buf[n:])
			n += got
			if err != nil || n == len(buf) {
				return n, err
			}
			end := cur.position()
			// Usually the new plan is the old file, read to its end, and the
			// new live one; after several rotations the files in between come
			// too. The old file stays open until the new plan holds its own
			// descriptors, so its inode cannot be handed out meanwhile.
			next, err := r.plan.open(&end, true)
			if errors.Is(err, fs.ErrNotExist) {
				return n, nil // renamed, not created yet: the next call finds it
			}
			if err != nil {
				return n, fmt.Errorf("follow the rotation of %s: %w", r.ref.LogPath, err)
			}
			r.stats.rotations.Add(1)
			r.finish(cur)
			r.files = next
			continue
		case stateTruncated:
			r.log.Info("log truncated, reading from the start")
			r.stats.truncations.Add(1)
			r.dec.reset()
			cur.seek(0)
			continue
		}
		return n, nil
	}
	return n, nil
}

// Position is where a restart may resume once everything Next has returned is
// stored: past the last message, but never past the start of a message still
// waiting for its parts.
func (r *reader) Position() cosmicdust.Position {
	if p, pending := r.dec.oldestPending(); pending {
		return p
	}
	if len(r.files) == 0 {
		if r.resume != nil {
			return *r.resume
		}
		return cosmicdust.Position{}
	}
	return r.files[0].position()
}

// Close releases the open files.
func (r *reader) Close() error {
	closeAll(r.files)
	r.files = nil
	return nil
}

type fileState int

const (
	stateIdle fileState = iota
	stateRotated
	stateTruncated
	stateMissing
)

func (r *reader) check(cur *openFile) fileState {
	fi, err := os.Stat(r.ref.LogPath)
	if err != nil {
		// The container is gone, or a rotation is between its rename and its
		// create. Either way the descriptor still reads what was written.
		return stateMissing
	}
	if inodeOf(fi) != cur.inode {
		return stateRotated
	}
	if cur.size() < cur.offset {
		return stateTruncated
	}
	return stateIdle
}

// finish closes a file that will not grow again. Bytes after its last newline
// are a line torn by a writer that died mid-write: reported, not guessed at.
func (r *reader) finish(cur *openFile) {
	if cur.size() > cur.offset {
		r.stats.tornLines.Add(1)
		r.log.Warn("torn line at the end of a rotated file", "file", cur.path, "bytes", cur.size()-cur.offset)
	}
	cur.close()
}

// drain decodes complete lines of cur into buf until the file ends or buf is
// full. At the end it rewinds to the start of an unfinished line, if any.
func (r *reader) drain(cur *openFile, buf []cosmicdust.Record) (int, error) {
	n := 0
	for n < len(buf) {
		lineStart := cur.offset
		raw, err := r.readLine(cur)
		switch {
		case err == nil:
		case errors.Is(err, errUnfinished):
			cur.seek(lineStart)
			return n, nil
		case errors.Is(err, io.EOF):
			return n, nil
		case errors.Is(err, errLineTooLong):
			r.stats.overlongLines.Add(1)
			continue
		default:
			return n, fmt.Errorf("read %s: %w", cur.path, err)
		}
		r.stats.lines.Add(1)
		r.stats.bytes.Add(cur.offset - lineStart)

		msg, ok, err := r.dec.decode(raw, cur.positionAt(lineStart), &r.arena)
		if err != nil {
			r.stats.malformedLines.Add(1)
			continue
		}
		if !ok {
			continue
		}
		rec := &buf[n]
		*rec = cosmicdust.Record{
			Time:        msg.time,
			ReadAt:      r.now(),
			ContainerID: r.ref.ID,
			Stream:      msg.stream,
			Source:      r.source,
			Line:        msg.text,
			Start:       msg.start,
			Truncated:   msg.truncated,
			Commit:      cur.position(),
		}
		if msg.parts > 1 {
			rec.Parts = msg.parts
		}
		if msg.truncated {
			r.stats.truncatedMessages.Add(1)
		}
		if p, pending := r.dec.oldestPending(); pending {
			rec.Commit = p
		}
		r.stats.messages.Add(1)
		n++
	}
	return n, nil
}

// readLine returns the next complete physical line, without its newline, and
// advances the offset past it. At the end of the file it returns io.EOF if
// nothing was read and errUnfinished if a line had started; the offset then
// stays, so the caller can come back for the whole line. A line longer than
// the read buffer is put together up to maxLine; past that the rest is read
// and dropped, and errLineTooLong returned.
func (r *reader) readLine(cur *openFile) ([]byte, error) {
	br, err := cur.buf.reader(cur)
	if err != nil {
		return nil, err
	}
	var acc []byte
	var consumed int64
	tooLong := false
	for {
		chunk, err := br.ReadSlice('\n')
		consumed += int64(len(chunk))
		if errors.Is(err, bufio.ErrBufferFull) {
			if !tooLong {
				if len(acc)+len(chunk) > r.maxLine {
					tooLong, acc = true, nil
				} else {
					acc = append(acc, chunk...)
				}
			}
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && consumed > 0 {
				return nil, errUnfinished
			}
			return nil, err
		}
		cur.offset += consumed
		if tooLong {
			return nil, errLineTooLong
		}
		if acc != nil {
			acc = append(acc, chunk...)
			return acc[:len(acc)-1], nil
		}
		return chunk[:len(chunk)-1], nil
	}
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
