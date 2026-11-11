package store

import (
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Compaction answers the small-files problem the Spark world knows: a
// conversion every few seconds leaves hundreds of small files per hour, each
// with its own dictionaries and footer; they compress ~30 % worse and cost
// more to list and to sync. Once an hour is over (or has too many
// files), its unsynced files are merged into one.
//
// Only unsynced files are merged: merging a synced file with an unsynced one
// leaves a file that is neither - marking it synced loses rows, marking it
// unsynced sends rows again. Files the sync engine has claimed are left alone,
// and so are files it ever offered: such a file may be at the control plane
// with its answer lost to a crash.
//
// Every converted file is sorted by container and time, so the merge reads
// the files side by side and writes one sorted file, a few hundred rows of
// each in memory: a first version read the whole hour and sorted it,
// 1.7 GB for one hour.
//
// The manifest is the commit point: the merged file is written first, the
// manifest swaps the old entries for it in one atomic write, and only then are
// the old files deleted. A crash before the swap leaves the merged file an
// orphan, after it the old files; Open removes orphans either way.

// compactChunk is how many rows of each file are held at a time.
const compactChunk = 512

// compactAll merges the partitions that qualify.
func (s *Store) compactAll(now time.Time) error {
	for _, table := range []string{tableLogs, tableEvents} {
		dirs, err := s.partitionDirs(table)
		if err != nil {
			return err
		}
		for _, dir := range dirs {
			if err := s.maybeCompact(table, dir, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) maybeCompact(table, dir string, now time.Time) error {
	s.mu.Lock()
	m, err := loadManifest(dir)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	var eligible []manifestFile
	for _, f := range m.Files {
		// Only files that never went out: one offered may be at the control
		// plane already, unsynced only because the answer was lost.
		if f.SyncedAt == nil && f.OfferedAt == nil && s.final(f) && s.claims[fileKey(table, m.Partition, f.Name)] == "" {
			eligible = append(eligible, f)
		}
	}
	hourOver := false
	if end, err := partitionEnd(m.Partition); err == nil {
		hourOver = now.After(end.Add(s.opts.Compact.Grace))
	}
	tooMany := len(eligible) >= s.opts.Compact.MaxFiles
	finished := hourOver && len(eligible) >= s.opts.Compact.MinFiles
	if len(eligible) < 2 || (!tooMany && !finished) {
		s.mu.Unlock()
		return nil
	}
	// The merged file is written before the old ones go: without room for
	// both, compaction would only fail half way.
	if ok, err := s.roomFor(eligible); err != nil || !ok {
		s.mu.Unlock()
		if err == nil {
			s.m.compactSkipped.Inc()
		}
		return err
	}
	keys := make([]string, 0, len(eligible))
	for _, f := range eligible {
		k := fileKey(table, m.Partition, f.Name)
		s.claim(k, "compaction")
		keys = append(keys, k)
	}
	s.mu.Unlock()
	defer s.release(keys...)

	if table == tableLogs {
		return compactFiles(s, dir, m.Partition, eligible, func(a, b *LogRow) int {
			if c := cmp.Compare(a.ContainerID, b.ContainerID); c != 0 {
				return c
			}
			return a.TS.Compare(b.TS)
		})
	}
	return compactFiles(s, dir, m.Partition, eligible, func(a, b *EventRow) int { return a.TS.Compare(b.TS) })
}

func compactFiles[T row](s *Store, dir, part string, files []manifestFile, cmpRows func(a, b *T) int) error {
	start := time.Now()
	slices.SortFunc(files, func(a, b manifestFile) int { return cmp.Compare(a.Segments[0], b.Segments[0]) })
	first, last := files[0].Segments[0], files[0].Segments[1]
	var bytesBefore int64
	for _, f := range files {
		first, last = min(first, f.Segments[0]), max(last, f.Segments[1])
		bytesBefore += f.Bytes
	}
	name := fmt.Sprintf("compact-%020d-%020d.parquet", first, last)

	w, err := newPartWriter[T](s, dir, name)
	if err != nil {
		return err
	}
	if err := mergeFiles(w, dir, files, cmpRows); err != nil {
		w.abort()
		if isNoSpace(err) {
			return fmt.Errorf("%w: %w", errNoSpace, err)
		}
		return fmt.Errorf("compact %s: %w", part, err)
	}
	mf, err := w.commit()
	if err != nil {
		if isNoSpace(err) {
			return fmt.Errorf("%w: %w", errNoSpace, err)
		}
		return err
	}
	mf.Segments = [2]uint64{first, last}
	if err := s.step("compact-written"); err != nil {
		return err
	}

	merged := make(map[string]bool, len(files))
	for _, f := range files {
		merged[f.Name] = true
	}
	committed, err := s.swapInManifest(dir, merged, mf)
	if !committed {
		_ = os.Remove(filepath.Join(dir, name)) // the manifest does not list it: ours to remove
		return err
	}
	if err != nil {
		// Committed, then stopped: the old files are orphans now, and the next
		// start removes them. Removing anything here could only do harm.
		return err
	}
	for n := range merged {
		if err := os.Remove(filepath.Join(dir, n)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	s.m.compactions.Inc()
	s.m.bytesCompacted.Add(float64(mf.Bytes))
	s.log.Info("compacted", "partition", part, "files", len(files), "rows", mf.Rows,
		"bytes_before", bytesBefore, "bytes_after", mf.Bytes, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// mergeFiles writes the rows of every file into w in cmpRows order, taking
// the smallest head of the files each time.
func mergeFiles[T row](w *partWriter[T], dir string, files []manifestFile, cmpRows func(a, b *T) int) error {
	h := &rowHeap[T]{cmp: cmpRows}
	defer func() {
		for _, r := range h.readers {
			r.close()
		}
	}()
	for _, f := range files {
		r, err := openRows[T](filepath.Join(dir, f.Name), compactChunk)
		if err != nil {
			return err
		}
		h.readers = append(h.readers, r)
		if row := r.next(); row != nil {
			h.items = append(h.items, heapItem[T]{row: *row, src: len(h.readers) - 1})
		} else if err := r.failed(); err != nil {
			return err
		}
	}
	heap.Init(h)
	out := make([]T, 0, compactChunk)
	flush := func() error {
		if len(out) == 0 {
			return nil
		}
		err := w.write(out)
		out = out[:0]
		return err
	}
	for h.Len() > 0 {
		it := &h.items[0]
		out = append(out, it.row)
		if len(out) == cap(out) {
			if err := flush(); err != nil {
				return err
			}
		}
		r := h.readers[it.src]
		if next := r.next(); next != nil {
			it.row = *next
			heap.Fix(h, 0)
		} else {
			if err := r.failed(); err != nil {
				return err
			}
			heap.Pop(h)
		}
	}
	return flush()
}

type heapItem[T row] struct {
	row T
	src int
}

type rowHeap[T row] struct {
	items   []heapItem[T]
	readers []*rowReader[T]
	cmp     func(a, b *T) int
}

func (h *rowHeap[T]) Len() int { return len(h.items) }
func (h *rowHeap[T]) Less(i, j int) bool {
	if c := h.cmp(&h.items[i].row, &h.items[j].row); c != 0 {
		return c < 0
	}
	return h.items[i].src < h.items[j].src // older file first among equals
}
func (h *rowHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *rowHeap[T]) Push(x any)    { h.items = append(h.items, x.(heapItem[T])) }
func (h *rowHeap[T]) Pop() any {
	it := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return it
}

// swapInManifest replaces the merged entries with the compacted file. The
// manifest is read again under the lock: a conversion may have added a file to
// the partition meanwhile, and that entry must survive. It reports false, and
// changes nothing, if a merged file has left the manifest in the meantime.
func (s *Store) swapInManifest(dir string, merged map[string]bool, mf manifestFile) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := loadManifest(dir)
	if err != nil {
		return false, err
	}
	kept := m.Files[:0:0]
	found := 0
	for _, f := range m.Files {
		if merged[f.Name] {
			found++
			continue
		}
		kept = append(kept, f)
	}
	if found != len(merged) {
		s.log.Warn("manifest changed under a compaction; dropping its result", "dir", dir)
		return false, nil
	}
	m.Files = kept
	m.put(mf)
	if err := saveManifest(dir, m); err != nil {
		return false, err
	}
	return true, s.step("compact-committed")
}
