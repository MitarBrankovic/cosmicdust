// Package store is the second layer of the node's storage: rows from sealed
// WAL segments are rewritten into Parquet files, one directory per table and
// hour, Hive style,
//
//	<root>/logs/dt=2026-10-21/hour=14/part-<first>-<last>.parquet
//	<root>/events/dt=2026-10-21/hour=14/...
//
// with a manifest per partition that lists its files and whether each has
// been sent to the control plane yet. On the node the files are a buffer and
// the unit of sending, not an archive: a file is sent once, acknowledged once
// and deleted once.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/fsutil"
	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

const (
	tableLogs    = "logs"
	tableEvents  = "events"
	stateFile    = "store.json"
	manifestName = "manifest.json"
)

// Options configure a Store.
type Options struct {
	Root string // <data-dir>/store
	Node string // stored with every row
	WAL  *wal.WAL

	// StateAt is the container's state and epoch when a line was written:
	// the lifecycle tracker's At. nil leaves them unknown.
	StateAt func(id string, ts time.Time) (cosmicdust.State, int)

	// ConvertEvery is how often sealed WAL segments are converted (default
	// 10 s). One conversion takes segments until their lines reach
	// ConvertBytes (default 32 MiB), so a backlog after a long stop is
	// converted in bounded pieces, not all at once. Rows stream
	// into the files; what waits in memory to be sorted is at most SortBytes
	// of lines per partition (default 4 MiB).
	ConvertEvery time.Duration
	ConvertBytes int64
	SortBytes    int

	// RowGroupRows (default 100 000) and PageBytes (default 1 MiB) shape the
	// Parquet files; zstd at its fastest level.
	RowGroupRows int64
	PageBytes    int

	Compact CompactOptions

	// Reserve is the space to keep free on the store's file system, on top of
	// room to convert the WAL that is waiting; 0 disables the check.
	// Quota is the most the store, WAL included, may hold; 0 disables it.
	// When either is passed, synced files go first, oldest first; then
	// WhenFull decides: "evict" deletes the oldest unsynced log files -
	// never events - each recorded as a logs_dropped event, "block" stops
	// the reading until there is room again.
	Reserve  int64
	Quota    int64
	WhenFull string

	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

// CompactOptions configure compaction.
type CompactOptions struct {
	Every    time.Duration // how often partitions are looked at; default 30 s
	MinFiles int           // merge a finished hour with at least this many files; default 2
	MaxFiles int           // merge at once past this many; default 200
	Grace    time.Duration // how long after its end an hour may still get rows; default 2 min
}

// Store keeps the node's Parquet files. It is a cosmicdust.Component (Run) and a
// cosmicdust.Store (Unsynced, Claim, GC).
type Store struct {
	opts Options
	log  *slog.Logger
	m    *metrics

	// mu guards state, claims and every read-modify-write of a manifest.
	mu     sync.Mutex
	state  state
	claims map[string]string // by file key: who is working with it (sync, compaction)

	// free reports the bytes available on the store's file system; walBytes
	// the WAL on disk. Replaced in tests.
	free     func() (int64, error)
	walBytes func() int64
	full     bool // the last space check found too little room

	// crash, in tests, stops a conversion or compaction at the named step as
	// if the process had died there.
	crash string

	// Kept from one conversion to the next, used by the Run goroutine only:
	// the encoder, the writers and the rows' backing arrays.
	codec        *zstdCodec
	logWriters   writerPool[LogRow]
	eventWriters writerPool[EventRow]
	rowArrays    [][]LogRow
	eventRows    map[string][]EventRow
}

// state is store.json, the single commit point of a conversion:
//
//  1. write every Parquet file (temporary name, fsync, rename, fsync dir)
//  2. write store.json: "segments up to N are converted into these files"
//  3. add those files to their partitions' manifests
//  4. clear the list in store.json
//  5. release the WAL segments
//
// Before step 2 the new files are orphans and the segments are still there,
// so a restart deletes the files and converts again. After it, a restart
// finishes steps 3-5. Names come from the segment range, so converting the
// same segments twice gives the same files, not copies.
type state struct {
	Converted uint64        `json:"converted"`
	Pending   []pendingFile `json:"pending,omitempty"`
}

type pendingFile struct {
	Table     string       `json:"table"`
	Partition string       `json:"partition"`
	File      manifestFile `json:"file"`
}

type manifestFile struct {
	Name      string     `json:"name"`
	Segments  [2]uint64  `json:"segments"` // first and last WAL segment its rows came from
	Rows      int64      `json:"rows"`
	BytesRaw  int64      `json:"bytes_raw"` // the same rows as JSON Lines
	Bytes     int64      `json:"bytes"`     // the Parquet file
	MinTS     time.Time  `json:"min_ts"`
	MaxTS     time.Time  `json:"max_ts"`
	CRC32C    uint32     `json:"crc32c"`
	CreatedAt time.Time  `json:"created_at"`
	SyncedAt  *time.Time `json:"synced_at"`

	// OfferedAt is when the file first went to the control plane. From then
	// on it may be there even unsynced - stored, with the answer lost to a
	// crash - so compaction leaves it alone: merged, its rows would arrive a
	// second time in the merged file.
	OfferedAt *time.Time `json:"offered_at,omitempty"`
}

type manifest struct {
	Table     string         `json:"table"`
	Partition string         `json:"partition"`
	Files     []manifestFile `json:"files"`
}

// Open loads store.json and repairs what a crash left behind.
func Open(opts Options) (*Store, error) {
	if opts.Root == "" || opts.WAL == nil {
		return nil, errors.New("store: Options.Root and Options.WAL are required")
	}
	if opts.ConvertEvery <= 0 {
		opts.ConvertEvery = 10 * time.Second
	}
	if opts.ConvertBytes <= 0 {
		opts.ConvertBytes = 32 << 20
	}
	if opts.SortBytes <= 0 {
		opts.SortBytes = 4 << 20
	}
	if opts.RowGroupRows <= 0 {
		opts.RowGroupRows = 100_000
	}
	if opts.PageBytes <= 0 {
		opts.PageBytes = 1 << 20
	}
	if opts.Compact.Every <= 0 {
		opts.Compact.Every = 30 * time.Second
	}
	if opts.Compact.MinFiles <= 0 {
		opts.Compact.MinFiles = 2
	}
	if opts.Compact.MaxFiles <= 0 {
		opts.Compact.MaxFiles = 200
	}
	if opts.Compact.Grace <= 0 {
		opts.Compact.Grace = 2 * time.Minute
	}
	switch opts.WhenFull {
	case "":
		opts.WhenFull = "evict"
	case "evict", "block":
	default:
		return nil, fmt.Errorf("store: WhenFull %q is neither evict nor block", opts.WhenFull)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(opts.Root, 0o750); err != nil {
		return nil, err
	}
	codec, err := newZstdCodec(opts.PageBytes)
	if err != nil {
		return nil, err
	}
	s := &Store{opts: opts, log: opts.Logger, claims: map[string]string{}, codec: codec,
		eventRows: map[string][]EventRow{}}
	s.free = func() (int64, error) { return freeBytes(opts.Root) }
	s.walBytes = opts.WAL.DiskBytes
	m, collectors := newMetrics(s)
	s.m = m
	if opts.Registerer != nil {
		for _, c := range collectors {
			if err := opts.Registerer.Register(c); err != nil {
				return nil, fmt.Errorf("store: register metrics: %w", err)
			}
		}
	}
	data, err := os.ReadFile(filepath.Join(opts.Root, stateFile))
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(data, &s.state); err != nil {
			return nil, fmt.Errorf("store: %s: %w", stateFile, err)
		}
	}
	if err := s.repair(); err != nil {
		return nil, err
	}
	return s, nil
}

var errCrash = errors.New("simulated crash")

func (s *Store) step(name string) error {
	if s.crash == name {
		return errCrash
	}
	return nil
}

// repair finishes a conversion that got past its commit point, deletes files
// that never reached it, and releases WAL segments already converted.
func (s *Store) repair() error {
	if len(s.state.Pending) > 0 {
		s.log.Info("finishing a committed conversion", "files", len(s.state.Pending))
		if err := s.applyPending(); err != nil {
			return err
		}
	}
	if err := s.removeOrphans(); err != nil {
		return err
	}
	if s.state.Converted > 0 && slices.Contains(s.opts.WAL.Sealed(), s.state.Converted) {
		return s.opts.WAL.Release(s.state.Converted)
	}
	return nil
}

// removeOrphans deletes temporary files, and Parquet files no manifest lists:
// both are what a crash leaves between writing a file and committing it.
func (s *Store) removeOrphans() error {
	for _, table := range []string{tableLogs, tableEvents} {
		dirs, err := s.partitionDirs(table)
		if err != nil {
			return err
		}
		for _, dir := range dirs {
			m, err := loadManifest(dir)
			if err != nil {
				return err
			}
			listed := make(map[string]bool, len(m.Files))
			for _, f := range m.Files {
				listed[f.Name] = true
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			for _, e := range entries {
				name := e.Name()
				if strings.HasSuffix(name, ".tmp") || (strings.HasSuffix(name, ".parquet") && !listed[name]) {
					s.m.orphans.Inc()
					s.log.Info("removing a file no manifest lists", "file", filepath.Join(dir, name))
					if err := os.Remove(filepath.Join(dir, name)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// partitionDirs lists the hour directories of a table, oldest first.
func (s *Store) partitionDirs(table string) ([]string, error) {
	root := filepath.Join(s.opts.Root, table)
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), "hour=") {
			dirs = append(dirs, path)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	slices.Sort(dirs)
	return dirs, err
}

func (s *Store) partDir(table, part string) string {
	return filepath.Join(s.opts.Root, table, filepath.FromSlash(part))
}

func (s *Store) applyPending() error {
	for _, p := range s.state.Pending {
		dir := s.partDir(p.Table, p.Partition)
		m, err := loadManifest(dir)
		if err != nil {
			return err
		}
		m.Table, m.Partition = p.Table, p.Partition
		m.put(p.File)
		if err := saveManifest(dir, m); err != nil {
			return err
		}
	}
	s.state.Pending = nil
	return s.saveState()
}

func (s *Store) saveState() error {
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	return fsutil.WriteFileAtomic(filepath.Join(s.opts.Root, stateFile), data, 0o600)
}

// put adds f, replacing an entry of the same name: applying the same
// conversion twice leaves one entry.
func (m *manifest) put(f manifestFile) {
	for i := range m.Files {
		if m.Files[i].Name == f.Name {
			m.Files[i] = f
			return
		}
	}
	m.Files = append(m.Files, f)
	slices.SortFunc(m.Files, func(a, b manifestFile) int { return strings.Compare(a.Name, b.Name) })
}

func loadManifest(dir string) (manifest, error) {
	var m manifest
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("store: manifest in %s: %w", dir, err)
	}
	return m, nil
}

func saveManifest(dir string, m manifest) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return fsutil.WriteFileAtomic(filepath.Join(dir, manifestName), data, 0o600)
}

func fileKey(table, part, name string) string { return table + "/" + part + "/" + name }

// claim marks a file as taken by owner; false if someone has it. The caller
// holds mu.
func (s *Store) claim(key, owner string) bool {
	if _, held := s.claims[key]; held {
		return false
	}
	s.claims[key] = owner
	return true
}

func (s *Store) release(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.claims, k)
	}
}

// final reports whether a file's rows are past the commit point.
func (s *Store) final(f manifestFile) bool { return f.Segments[1] <= s.state.Converted }

// sizeOnDisk adds up the store's Parquet files, as the manifests list them.
// The caller holds mu.
func (s *Store) sizeOnDisk() (int64, error) {
	var n int64
	for _, table := range []string{tableLogs, tableEvents} {
		dirs, err := s.partitionDirs(table)
		if err != nil {
			return 0, err
		}
		for _, dir := range dirs {
			m, err := loadManifest(dir)
			if err != nil {
				return 0, err
			}
			for _, f := range m.Files {
				n += f.Bytes
			}
		}
	}
	return n, nil
}

func (s *Store) Name() string { return "store" }
