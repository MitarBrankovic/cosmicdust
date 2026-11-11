package archive

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// Nodes send every few seconds, so an application's hour ends up as hundreds
// of small files, each with a footer and dictionaries of its own: the archive
// of a test was 30 % larger than the node's store. Once an hour is over
// and every file of it is indexed, its files are merged into one.
//
// The merged file names its inputs in its metadata. It is written next to its
// name, fsynced and renamed before an input is removed, so a crash leaves the
// inputs and perhaps the merged file; the next pass then removes the inputs
// the merged file names, and nothing is in the archive twice for longer than
// that. Files that arrive for an hour already merged - a node that was away -
// are merged with it at a later pass.

const (
	compactedPrefix = "compacted-"
	compactedFrom   = "cosmicdust.compacted_from"
)

// Compaction sets when an hour's files are merged - once the hour ended more
// than after ago - and how often the archiver looks; 0 turns merging off.
func (a *Archiver) Compaction(after, every time.Duration) {
	a.compactAfter, a.compactEvery = after, every
}

// compactDue merges if the time has come; the archiver calls it between
// files, so that nothing else writes the archive meanwhile.
func (a *Archiver) compactDue(now time.Time) {
	if a.compactEvery <= 0 || now.Sub(a.lastCompaction) < a.compactEvery {
		return
	}
	a.lastCompaction = now
	if _, err := a.Compact(now); err != nil {
		a.log.Warn("merging archive hours failed; trying again later", "err", err)
	}
}

// Compact merges the files of every application's hour that ended more than
// the compaction delay before now and has nothing waiting for the index. It
// returns how many hours it merged.
func (a *Archiver) Compact(now time.Time) (int, error) {
	pending := map[string]bool{}
	for _, rel := range a.ledger.PendingIndex() {
		pending[rel] = true
	}
	merged := 0
	var errs []error
	for _, table := range []string{api.TableLogs, api.TableEvents} {
		hours, err := filepath.Glob(filepath.Join(a.root, table, "app=*", "dt=*", "hour=*"))
		if err != nil {
			return merged, err
		}
		for _, dir := range hours {
			end, ok := hourEnd(dir)
			if !ok || now.Sub(end) < a.compactAfter {
				continue
			}
			did, err := a.compactHour(table, dir, pending)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if did {
				merged++
			}
		}
	}
	a.Stats.Compactions.Add(int64(merged))
	return merged, errors.Join(errs...)
}

// hourEnd is when the hour of a .../dt=<day>/hour=<hour> directory ended.
func hourEnd(dir string) (time.Time, bool) {
	day, err := time.Parse("2006-01-02", strings.TrimPrefix(filepath.Base(filepath.Dir(dir)), "dt="))
	if err != nil {
		return time.Time{}, false
	}
	h, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(dir), "hour="))
	if err != nil || h < 0 || h > 23 {
		return time.Time{}, false
	}
	return day.Add(time.Duration(h+1) * time.Hour), true
}

func (a *Archiver) compactHour(table, dir string, pending map[string]bool) (bool, error) {
	names, err := parquetFiles(dir)
	if err != nil {
		return false, err
	}
	// A merge a crash cut short: its inputs go now.
	for _, name := range names {
		if !strings.HasPrefix(name, compactedPrefix) {
			continue
		}
		inputs, err := compactedInputs(filepath.Join(dir, name))
		if err != nil {
			continue // not readable: left as it is
		}
		for _, in := range inputs {
			if in != name && slices.Contains(names, in) {
				if err := os.Remove(filepath.Join(dir, in)); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return false, err
				}
				a.Stats.Compacted.Add(1)
			}
		}
	}
	if names, err = parquetFiles(dir); err != nil || len(names) < 2 {
		return false, err
	}
	rel, err := filepath.Rel(a.root, dir)
	if err != nil {
		return false, err
	}
	for _, name := range names {
		if pending[filepath.ToSlash(filepath.Join(rel, name))] {
			return false, nil // not all indexed yet
		}
	}

	h := fnv.New64a()
	for _, name := range names {
		_, _ = h.Write([]byte(name + "\n"))
	}
	out := fmt.Sprintf("%s%016x.parquet", compactedPrefix, h.Sum64())
	switch table {
	case api.TableLogs:
		err = merge[store.LogRow](dir, names, out, a.batch, 1<<20)
	case api.TableEvents:
		err = merge[store.EventRow](dir, names, out, a.batch, 64<<10)
	}
	var bad *BadFileError
	if errors.As(err, &bad) {
		// An input that cannot be read stays as it is, and so does the hour.
		a.log.Warn("an archive file cannot be read; its hour is not merged", "file", bad.Path, "err", bad.Err)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, name := range names {
		if name == out {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
		a.Stats.Compacted.Add(1)
	}
	return true, syncDir(dir)
}

// parquetFiles lists the Parquet files of a directory, by name.
func parquetFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".parquet") {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// compactedInputs reads the inputs a merged file names.
func compactedInputs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return nil, err
	}
	v, ok := pf.Lookup(compactedFrom)
	if !ok {
		return nil, nil
	}
	return strings.Split(v, ","), nil
}

// merge writes the rows of inputs, in their order, into out in dir.
func merge[T Row](dir string, inputs []string, out string, batch, page int) (err error) {
	o, err := create[T](dir, out, page, parquet.KeyValueMetadata(compactedFrom, strings.Join(inputs, ",")))
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = o.f.Close()
			_ = os.Remove(o.tmp)
		}
	}()
	buf := make([]T, batch)
	for _, in := range inputs {
		r, err := openRows[T](filepath.Join(dir, in))
		if err != nil {
			return err
		}
		for {
			n, rerr := r.read(buf)
			if n > 0 {
				if _, err := o.w.Write(buf[:n]); err != nil {
					r.close()
					return fmt.Errorf("write %s: %w", o.rel, err)
				}
			}
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				r.close()
				return rerr
			}
		}
		r.close()
	}
	return o.commit()
}
