package store

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// What the store does when its disk fills, or the store reaches its quota.
// An earlier version stopped on the first "no space left", and a restart with
// the disk still full stopped again five seconds later: a crash loop that
// collects nothing. "Never delete what is not synced" cannot be kept;
// "never lose anything without a trace" can:
//
//   - What must stay free is the reserve plus the WAL that waits for its
//     conversion: a conversion writes Parquet before it releases segments,
//     and with the reserve alone a disk full of WAL could never be converted,
//     synced or freed.
//   - Below that, synced files go first, oldest first: the control plane has
//     them.
//   - Then WhenFull: "evict" deletes the oldest unsynced log files - never
//     events - and records each as a logs_dropped event with its time range
//     and row count, so the control plane learns exactly what is missing;
//     "block" deletes nothing and stops the reading until there is room, and
//     the loss happens at the source, when the runtime rotates unread lines
//     away, without a trace.
//   - A quota on the store's size works the same way.

func freeBytes(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * st.Bsize, nil
}

func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// checkSpace enforces the reserve and the quota. It returns logs_dropped
// events for what it evicted; they go to the WAL like any other event.
func (s *Store) checkSpace(now time.Time) ([]cosmicdust.Event, error) {
	var gaps []cosmicdust.Event
	hold := false

	if s.opts.Reserve > 0 {
		free, err := s.free()
		if err != nil {
			return nil, fmt.Errorf("free space: %w", err)
		}
		need := s.opts.Reserve + s.walBytes()
		if free < need {
			// Free twice the shortfall, so as not to delete one file per check.
			g, freed, err := s.freeUp(2*(need-free), s.opts.WhenFull == "evict", now)
			gaps = append(gaps, g...)
			if err != nil {
				return gaps, err
			}
			if free+freed < need {
				hold = true
			}
		}
	}
	if s.opts.Quota > 0 {
		s.mu.Lock()
		size, err := s.sizeOnDisk()
		s.mu.Unlock()
		if err != nil {
			return gaps, err
		}
		size += s.walBytes()
		if size > s.opts.Quota {
			g, freed, err := s.freeUp(size-s.opts.Quota, s.opts.WhenFull == "evict", now)
			gaps = append(gaps, g...)
			if err != nil {
				return gaps, err
			}
			if size-freed > s.opts.Quota {
				hold = true
			}
		}
	}

	// Held, the WAL refuses blocks and the tailer waits; events wait in
	// memory. Evicting usually makes room before it comes to that.
	s.opts.WAL.Hold(hold)
	switch {
	case hold && !s.full:
		s.m.fullTimes.Inc()
		s.log.Warn("no room: reading stops until there is", "reserve", s.opts.Reserve, "quota", s.opts.Quota, "when_full", s.opts.WhenFull)
	case !hold && s.full:
		s.log.Info("room again: reading resumes")
	}
	s.full = hold
	s.m.held.Set(boolFloat(hold))
	return gaps, nil
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// freeUp deletes files until need bytes are gone or nothing more may go:
// synced files of both tables first, oldest first; then, if evict, unsynced log
// files nobody is sending or compacting, oldest first. It returns a
// logs_dropped event for every unsynced file it deleted.
//
// The manifest goes first and the file after it: a crash in between leaves an
// orphan file, which the next start removes.
func (s *Store) freeUp(need int64, evict bool, now time.Time) ([]cosmicdust.Event, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type cand struct {
		dir, table, part string
		f                manifestFile
		synced           bool
	}
	var cands []cand
	for _, table := range []string{tableEvents, tableLogs} {
		dirs, err := s.partitionDirs(table)
		if err != nil {
			return nil, 0, err
		}
		for _, dir := range dirs {
			m, err := loadManifest(dir)
			if err != nil {
				return nil, 0, err
			}
			for _, f := range m.Files {
				synced := f.SyncedAt != nil
				free := s.claims[fileKey(table, m.Partition, f.Name)] == ""
				if free && s.final(f) && (synced || (evict && table == tableLogs)) {
					cands = append(cands, cand{dir, table, m.Partition, f, synced})
				}
			}
		}
	}
	slices.SortFunc(cands, func(a, b cand) int {
		if a.synced != b.synced {
			if a.synced {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(a.part, b.part); c != 0 {
			return c
		}
		return cmp.Compare(a.f.Segments[0], b.f.Segments[0])
	})

	var freed int64
	var gaps []cosmicdust.Event
	for _, c := range cands {
		if freed >= need {
			break
		}
		if err := s.dropFile(c.dir, c.f.Name); err != nil {
			return gaps, freed, err
		}
		freed += c.f.Bytes
		if c.synced {
			s.m.deleted.WithLabelValues("true").Inc()
			continue
		}
		s.m.deleted.WithLabelValues("false").Inc()
		s.m.droppedRows.Add(float64(c.f.Rows))
		// What is missing, exactly: which file, how many rows, which time span.
		gaps = append(gaps, cosmicdust.Event{
			Time: now, Runtime: "cosmicdust", Action: "logs_dropped", Kind: cosmicdust.KindOther,
			Fields: map[string]string{
				"table": c.table, "partition": c.part, "file": c.f.Name,
				"rows": strconv.FormatInt(c.f.Rows, 10), "bytes": strconv.FormatInt(c.f.Bytes, 10),
				"min_ts": c.f.MinTS.Format(time.RFC3339Nano), "max_ts": c.f.MaxTS.Format(time.RFC3339Nano),
			},
		})
		s.log.Warn("dropped unsynced logs to make room", "partition", c.part, "file", c.f.Name, "rows", c.f.Rows)
	}
	return gaps, freed, nil
}

// dropFile takes a file out of its manifest, then deletes it. The caller holds
// mu.
func (s *Store) dropFile(dir, name string) error {
	m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	kept := m.Files[:0]
	for _, f := range m.Files {
		if f.Name != name {
			kept = append(kept, f)
		}
	}
	m.Files = kept
	if err := saveManifest(dir, m); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// roomFor reports whether files fit on the disk a second time with the
// reserve left over, as a compaction needs. The caller holds mu.
func (s *Store) roomFor(files []manifestFile) (bool, error) {
	if s.opts.Reserve <= 0 {
		return true, nil
	}
	var need int64
	for _, f := range files {
		need += f.Bytes
	}
	free, err := s.free()
	if err != nil {
		return false, err
	}
	return free >= need+s.opts.Reserve, nil
}
