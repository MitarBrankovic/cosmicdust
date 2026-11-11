package store

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// errNoSpace marks a conversion or compaction that could not write its files
// for lack of space, before anything was committed: the WAL segments stay, and
// the same work is tried again once there is room.
var errNoSpace = errors.New("store: no space for the files")

// maxOpenParts bounds the files a conversion writes at once. A conversion's
// rows fall into one hour, two at the turn of one; a backlog read from an old
// container's log moves through many hours, one after another.
const maxOpenParts = 4

// convert rewrites sealed WAL segments into Parquet, in order, up to
// ConvertBytes of lines at a time. It returns how many segments it consumed.
//
// Rows stream through. Each partition gathers them until their lines reach
// SortBytes; they are then sorted by container and time and written to the
// partition's file. Memory is bounded by that and by the open files, not by
// the conversion: holding a whole conversion's rows, 10 s at 5,000 lines/s,
// kept ~17 MB live and the garbage collector running most of the time.
// A file whose rows fit SortBytes, every file at 1,000 lines/s, is sorted as a
// whole; a bigger one is a few sorted runs.
func (s *Store) convert() (int, error) {
	var todo []uint64
	for _, id := range s.opts.WAL.Sealed() {
		if id > s.state.Converted {
			todo = append(todo, id)
		}
	}
	if len(todo) == 0 {
		return 0, nil
	}
	start := time.Now()
	c := &conversion{s: s, first: todo[0], events: s.eventRows}
	defer c.release()
	var lineBytes int64
	taken := 0
	for _, id := range todo {
		if taken > 0 && lineBytes >= s.opts.ConvertBytes {
			break
		}
		err := s.opts.WAL.Read(id, func(b *wal.Block) error {
			for i := range b.Logs {
				r := s.logRow(b, &b.Logs[i])
				lineBytes += int64(len(r.Line))
				if err := c.add(r); err != nil {
					return err
				}
			}
			for i := range b.Events {
				r := s.eventRow(&b.Events[i])
				p := partition(r.TS)
				c.events[p] = append(c.events[p], r)
			}
			return nil
		})
		if err != nil {
			if !wal.IsCorrupt(err) {
				return c.undo(err)
			}
			// The WAL reported it at recovery; what is readable is kept.
			s.log.Error("converting the readable part of a damaged WAL segment", "segment", id, "err", err)
		}
		taken++
	}
	todo = todo[:taken]
	last := todo[len(todo)-1]

	// 1. The files. Until the commit they are not part of the store: if one
	// cannot be written, the ones before it go too, or they would fill a
	// full disk further.
	files, err := c.finish(last)
	if err != nil {
		return c.undo(err)
	}
	if err := s.step("files-written"); err != nil {
		return 0, err
	}

	// 2-4 under the lock, so that compaction and sync never see half of them.
	if err := s.commitConversion(last, files); err != nil {
		return 0, err
	}
	// 5. The WAL no longer needs the segments.
	if err := s.opts.WAL.Release(last); err != nil {
		return 0, err
	}

	var rows int64
	for _, f := range files {
		rows += f.File.Rows
		s.m.addStored(f.File.Bytes, f.File.BytesRaw)
		s.m.rowsStored.WithLabelValues(f.Table).Add(float64(f.File.Rows))
	}
	s.m.conversions.Inc()
	s.m.convertSeconds.Observe(time.Since(start).Seconds())
	s.log.Debug("converted", "segments", len(todo), "files", len(files), "rows", rows, "took", time.Since(start).Round(time.Millisecond))
	return len(todo), nil
}

// conversion is the files of one convert call.
type conversion struct {
	s      *Store
	first  uint64
	n      int         // files started, for their names
	open   []*convPart // being written, least recently used first
	done   []*convPart // in place under their names, not yet committed
	events map[string][]EventRow
	extra  []string // event files in place, for undo
}

// convPart is a log file being written: the rows of one partition waiting to
// be sorted, and the file they go to.
type convPart struct {
	part  string
	rows  []LogRow
	bytes int
	w     *partWriter[LogRow]
	file  manifestFile
}

// name is the next file's: the conversion's first segment and a sequence
// number, so that converting the same segments again gives the same names.
func (c *conversion) name() string {
	c.n++
	return fmt.Sprintf("part-%020d-%03d.parquet", c.first, c.n)
}

// add puts a row into its partition's file; the partition's rows are written
// once their lines reach SortBytes.
func (c *conversion) add(r LogRow) error {
	p, err := c.part(partition(r.TS))
	if err != nil {
		return err
	}
	p.rows = append(p.rows, r)
	p.bytes += len(r.Line)
	if p.bytes >= c.s.opts.SortBytes {
		return p.flush()
	}
	return nil
}

// part returns the open file of a partition. Opening one when maxOpenParts
// are open closes the least recently used; its partition, should rows for it
// come again, gets another file.
func (c *conversion) part(part string) (*convPart, error) {
	for i, p := range c.open {
		if p.part == part {
			if i != len(c.open)-1 {
				copy(c.open[i:], c.open[i+1:])
				c.open[len(c.open)-1] = p
			}
			return p, nil
		}
	}
	if len(c.open) == maxOpenParts {
		if err := c.close(); err != nil {
			return nil, err
		}
	}
	w, err := newPartWriter[LogRow](c.s, c.s.partDir(tableLogs, part), c.name())
	if err != nil {
		return nil, err
	}
	p := &convPart{part: part, w: w, rows: c.s.takeRows()}
	c.open = append(c.open, p)
	return p, nil
}

// close finishes the least recently used open file.
func (c *conversion) close() error {
	p := c.open[0]
	if err := p.flush(); err != nil {
		return err
	}
	mf, err := p.w.commit()
	if err != nil {
		p.w = nil // commit cleaned up after itself
		c.open = c.open[1:]
		c.s.putRows(p.rows)
		return err
	}
	p.w, p.file = nil, mf
	c.s.putRows(p.rows)
	p.rows = nil
	c.open = c.open[1:]
	c.done = append(c.done, p)
	return nil
}

// flush sorts the rows gathered by container and time, the order compaction
// keeps (worth +9 % on a real hour of logs), and writes them.
func (p *convPart) flush() error {
	if len(p.rows) == 0 {
		return nil
	}
	slices.SortStableFunc(p.rows, func(a, b LogRow) int {
		if c := cmp.Compare(a.ContainerID, b.ContainerID); c != 0 {
			return c
		}
		return a.TS.Compare(b.TS)
	})
	err := p.w.write(p.rows)
	clear(p.rows) // their lines point into WAL blocks, which can go now
	p.rows, p.bytes = p.rows[:0], 0
	return err
}

// finish writes what is left: the open log files, then the events, which are
// few and wait in memory. It returns every file of the conversion.
func (c *conversion) finish(last uint64) ([]pendingFile, error) {
	for len(c.open) > 0 {
		if err := c.close(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(c.done, func(a, b *convPart) int {
		return cmp.Or(cmp.Compare(a.part, b.part), cmp.Compare(a.file.Name, b.file.Name))
	})
	files := make([]pendingFile, 0, len(c.done)+len(c.events))
	for _, p := range c.done {
		p.file.Segments = [2]uint64{c.first, last}
		files = append(files, pendingFile{Table: tableLogs, Partition: p.part, File: p.file})
	}
	for _, part := range slices.Sorted(maps.Keys(c.events)) {
		rows := c.events[part]
		if len(rows) == 0 {
			continue
		}
		slices.SortStableFunc(rows, func(a, b EventRow) int { return a.TS.Compare(b.TS) })
		dir := c.s.partDir(tableEvents, part)
		mf, err := writeAll(c.s, dir, c.name(), rows)
		if err != nil {
			return nil, err
		}
		c.extra = append(c.extra, filepath.Join(dir, mf.Name))
		mf.Segments = [2]uint64{c.first, last}
		files = append(files, pendingFile{Table: tableEvents, Partition: part, File: mf})
	}
	return files, nil
}

// undo removes every file the conversion wrote, before its commit.
func (c *conversion) undo(err error) (int, error) {
	for _, p := range c.open {
		p.w.abort()
		c.s.putRows(p.rows)
	}
	c.open = nil
	for _, p := range c.done {
		_ = os.Remove(filepath.Join(c.s.partDir(tableLogs, p.part), p.file.Name))
	}
	for _, path := range c.extra {
		_ = os.Remove(path)
	}
	if isNoSpace(err) {
		return 0, fmt.Errorf("%w: %w", errNoSpace, err)
	}
	return 0, err
}

// release clears the event rows the conversion gathered, so that the WAL
// blocks they point into can be collected, keeping the arrays.
func (c *conversion) release() {
	for p, rows := range c.events {
		clear(rows)
		if len(rows) == 0 {
			delete(c.events, p) // an hour this conversion did not touch
		} else {
			c.events[p] = rows[:0]
		}
	}
}

// takeRows and putRows keep row arrays from one conversion to the next.
func (s *Store) takeRows() []LogRow {
	if n := len(s.rowArrays); n > 0 {
		rows := s.rowArrays[n-1]
		s.rowArrays = s.rowArrays[:n-1]
		return rows
	}
	return nil
}

func (s *Store) putRows(rows []LogRow) {
	if rows != nil && len(s.rowArrays) < maxOpenParts {
		clear(rows)
		s.rowArrays = append(s.rowArrays, rows[:0])
	}
}

func (s *Store) commitConversion(last uint64, files []pendingFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Converted = last
	s.state.Pending = files
	if err := s.saveState(); err != nil {
		return err
	}
	if err := s.step("committed"); err != nil {
		return err
	}
	if err := s.applyPending(); err != nil {
		return err
	}
	return s.step("manifests-updated")
}

func writeAll[T row](s *Store, dir, name string, rows []T) (manifestFile, error) {
	w, err := newPartWriter[T](s, dir, name)
	if err != nil {
		return manifestFile{}, err
	}
	if err := w.write(rows); err != nil {
		w.abort()
		return manifestFile{}, err
	}
	return w.commit()
}

// logRow turns a WAL row into a stored one. The container's state is decided
// now, seconds after the line was read, when the events that came late are
// in: Docker reports an exit ~400 ms after containerd.
func (s *Store) logRow(b *wal.Block, r *wal.Row) LogRow {
	c := b.Containers[r.Container]
	row := LogRow{
		TS: r.TS.UTC(), ReadAt: r.ReadAt.UTC(), Node: s.opts.Node, App: c.App, ContainerID: c.ID, Name: c.Name,
		Image: c.Image, Stream: r.Stream, State: string(cosmicdust.StateUnknown), Line: r.Line, Source: r.Source,
		Inode: r.Inode, Head: r.Head, Offset: r.Offset, Parts: int32(r.Parts), Truncated: r.Truncated,
	}
	if s.opts.StateAt != nil {
		st, epoch := s.opts.StateAt(c.ID, r.TS)
		row.State, row.Epoch = string(st), int32(epoch)
	}
	return row
}

func (s *Store) eventRow(e *cosmicdust.Event) EventRow {
	row := EventRow{
		TS: e.Time.UTC(), Node: s.opts.Node, App: e.App, ContainerID: e.ContainerID, Name: e.Name, Image: e.Image,
		Runtime: e.Runtime, Namespace: e.Namespace, Action: e.Action, Kind: e.Kind.String(), State: string(e.State),
		Epoch: int32(e.Epoch), ExitCode: -1, OOMKilled: e.OOMKilled, Inferred: e.Inferred,
	}
	if e.ExitCode != nil {
		row.ExitCode = int32(*e.ExitCode)
	}
	if len(e.Fields) > 0 {
		b, _ := json.Marshal(e.Fields)
		row.Fields = string(b)
	}
	return row
}
