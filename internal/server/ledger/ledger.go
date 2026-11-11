// Package ledger is the service's memory across restarts: which files arrived,
// so that one sent again is recognised after its inbox copy is gone; which of
// them are archived; which archive files are in the search index. It is a file
// of JSON lines, appended to and fsynced before anything that depends on it is
// answered, replayed at start, and rewritten when it holds mostly history.
package ledger

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// FileID is what a sender declared of a file's content.
type FileID struct {
	Size int64
	CRC  uint32
}

// Received describes a file that arrived whole and is durable in the inbox.
type Received struct {
	File         string // inbox path: node=<node>/<table>/<partition>/<name>
	ID           FileID
	Rows         int64
	MinTS, MaxTS int64 // unix nanoseconds, the file's oldest and newest row
}

type entry struct {
	Op      string   `json:"op"`
	At      int64    `json:"at"`
	File    string   `json:"file,omitempty"`
	Size    int64    `json:"size,omitempty"`
	CRC     uint32   `json:"crc32c,omitempty"`
	Rows    int64    `json:"rows,omitempty"`
	MinTS   int64    `json:"min_ts,omitempty"`
	MaxTS   int64    `json:"max_ts,omitempty"`
	Outputs []string `json:"outputs,omitempty"` // archived: the archive files written; indexed: those now in the index
}

const (
	opReceived = "received"
	opArchived = "archived" // with an empty file: outputs still to index, after a compaction
	opIndexed  = "indexed"
)

type file struct {
	id       FileID
	at       int64 // when it arrived
	seq      uint64
	archived bool
}

// Ledger is safe for concurrent use.
type Ledger struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	lines   int
	files   map[string]*file
	seq     uint64
	pending map[string]uint64 // archive files not yet in the index, by order of archiving
	pseq    uint64

	now        func() time.Time
	sync       func(*os.File) error // fsync; tests that are not about durability skip it
	minCompact int                  // fewer lines are never worth a rewrite
}

// Open replays the ledger in dir. A last line torn by a crash is dropped: it
// was never fsynced, so nothing was answered on it.
func Open(dir string) (*Ledger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Ledger{path: filepath.Join(dir, "ledger.jsonl"), files: map[string]*file{}, pending: map[string]uint64{},
		now: time.Now, sync: (*os.File).Sync, minCompact: 1000}
	data, err := os.ReadFile(l.path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	good := 0
	for good < len(data) {
		i := bytes.IndexByte(data[good:], '\n')
		if i < 0 {
			break
		}
		var e entry
		if err := json.Unmarshal(data[good:good+i], &e); err != nil {
			if good+i+1 == len(data) {
				break // the last line, torn
			}
			return nil, fmt.Errorf("ledger %s: line %d: %w", l.path, l.lines+1, err)
		}
		l.apply(e)
		l.lines++
		good += i + 1
	}
	if l.f, err = os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if good < len(data) {
		if err := l.f.Truncate(int64(good)); err != nil {
			_ = l.f.Close()
			return nil, err
		}
	}
	if _, err := l.f.Seek(int64(good), 0); err != nil {
		_ = l.f.Close()
		return nil, err
	}
	return l, nil
}

func (l *Ledger) apply(e entry) {
	switch e.Op {
	case opReceived:
		l.seq++
		l.files[e.File] = &file{id: FileID{Size: e.Size, CRC: e.CRC}, at: e.At, seq: l.seq}
	case opArchived:
		if f := l.files[e.File]; f != nil {
			f.archived = true
		}
		for _, o := range e.Outputs {
			l.pseq++
			l.pending[o] = l.pseq
		}
	case opIndexed:
		for _, o := range e.Outputs {
			delete(l.pending, o)
		}
	}
}

func (l *Ledger) append(e entry) error {
	e.At = l.now().UnixNano()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	if err := l.sync(l.f); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	l.lines++
	l.apply(e)
	return nil
}

// Known returns the content a file arrived with, if it did.
func (l *Ledger) Known(file string) (FileID, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f := l.files[file]; f != nil {
		return f.id, true
	}
	return FileID{}, false
}

// Received records a file that arrived. It returns once the record is
// durable; the sender may be answered then.
func (l *Ledger) Received(r Received) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.append(entry{Op: opReceived, File: r.File, Size: r.ID.Size, CRC: r.ID.CRC, Rows: r.Rows, MinTS: r.MinTS,
		MaxTS: r.MaxTS})
}

// Archived records that a received file's rows are in the archive files
// outputs, which are now to be indexed.
func (l *Ledger) Archived(file string, outputs []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.append(entry{Op: opArchived, File: file, Outputs: outputs})
}

// Indexed records that archive files are in the search index.
func (l *Ledger) Indexed(outputs ...string) error {
	if len(outputs) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.append(entry{Op: opIndexed, Outputs: outputs})
}

// PendingArchive lists the received files not archived yet, oldest first.
func (l *Ledger) PendingArchive() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for name, f := range l.files {
		if !f.archived {
			out = append(out, name)
		}
	}
	slices.SortFunc(out, func(a, b string) int { return cmp.Compare(l.files[a].seq, l.files[b].seq) })
	return out
}

// PendingIndex lists the archive files not in the index yet, in the order
// they were archived.
func (l *Ledger) PendingIndex() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.pending))
	for o := range l.pending {
		out = append(out, o)
	}
	slices.SortFunc(out, func(a, b string) int { return cmp.Compare(l.pending[a], l.pending[b]) })
	return out
}

// Forget drops archive files from the work still to do: the retention
// removed them before they were indexed.
func (l *Ledger) Forget(outputs ...string) error { return l.Indexed(outputs...) }

// MaybeCompact rewrites the ledger once most of it is history, forgetting the
// archived files that arrived before horizon: a second upload of one of those
// is no longer recognised.
func (l *Ledger) MaybeCompact(horizon time.Time) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lines < l.minCompact || l.lines < 2*l.liveLocked(horizon) {
		return false, nil
	}
	return true, l.compactLocked(horizon)
}

// Compact rewrites the ledger now.
func (l *Ledger) Compact(horizon time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.compactLocked(horizon)
}

func (l *Ledger) keep(f *file, horizon time.Time) bool {
	return !f.archived || f.at >= horizon.UnixNano()
}

// liveLocked is how many lines a compacted ledger would have.
func (l *Ledger) liveLocked(horizon time.Time) int {
	n := 1
	for _, f := range l.files {
		if l.keep(f, horizon) {
			n++
			if f.archived {
				n++
			}
		}
	}
	return n
}

func (l *Ledger) compactLocked(horizon time.Time) error {
	names := make([]string, 0, len(l.files))
	for name, f := range l.files {
		if l.keep(f, horizon) {
			names = append(names, name)
		}
	}
	slices.SortFunc(names, func(a, b string) int { return cmp.Compare(l.files[a].seq, l.files[b].seq) })
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	lines := 0
	put := func(e entry) error {
		lines++
		return enc.Encode(e)
	}
	for _, name := range names {
		f := l.files[name]
		if err := put(entry{Op: opReceived, At: f.at, File: name, Size: f.id.Size, CRC: f.id.CRC}); err != nil {
			return err
		}
		if f.archived {
			if err := put(entry{Op: opArchived, At: f.at, File: name}); err != nil {
				return err
			}
		}
	}
	pending := make([]string, 0, len(l.pending))
	for o := range l.pending {
		pending = append(pending, o)
	}
	slices.SortFunc(pending, func(a, b string) int { return cmp.Compare(l.pending[a], l.pending[b]) })
	for chunk := range slices.Chunk(pending, 500) {
		if err := put(entry{Op: opArchived, At: l.now().UnixNano(), Outputs: chunk}); err != nil {
			return err
		}
	}

	tmp := l.path + ".tmp"
	if err := writeSynced(tmp, buf.Bytes()); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(l.path)); err != nil {
		return err
	}
	_ = l.f.Close()
	f, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return err
	}
	l.f, l.lines = f, lines
	for name, f := range l.files {
		if !l.keep(f, horizon) {
			delete(l.files, name)
		}
	}
	return nil
}

// Close closes the ledger.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

func writeSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
