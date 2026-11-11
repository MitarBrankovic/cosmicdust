// Package servertest is a stand-in for the control plane's receiving side: it
// takes the files and urgent events nodes send, over NATS and gRPC, and keeps
// them under node=<node>/<table>/<partition>/. It exists to test the sync
// protocol end to end, not as a control plane: one process, one lock, no
// authentication.
package servertest

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/pkg/api"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Receiver is the protocol's receiving side, shared by the NATS and the gRPC
// server. A file arrives as chunks in order into a .part file, is checked
// against the size and CRC-32C its sender declared, fsynced and renamed into
// place. A file it already holds with the same content is a duplicate at the
// probe: that makes every upload idempotent. An upload that broke off is
// continued: the probe answers how much of the file arrived.
type Receiver struct {
	dir      string
	mu       sync.Mutex
	partials map[string]*partial
	owners   map[string]uint64 // the upload attempt a probe handed a file to
	known    map[string]fileID
	events   map[string]bool
	evf      *os.File
	filesLog *os.File
	now      func() time.Time

	Stats Stats
}

type fileID struct {
	crc  uint32
	size int64
}

type partial struct {
	f        *os.File
	tmp, dst string
	want     fileID // what the sender declared
	size     int64
	crc      hash.Hash32
	touched  time.Time
}

// Stats counts what the receiver did.
type Stats struct {
	FilesStored, BytesStored, Duplicates, Conflicts atomic.Int64
	Resumed, BadOffsets, CRCFailures, Discarded     atomic.Int64
	Superseded, Events, EventsDuplicate             atomic.Int64
}

// Open opens the receiver's directory. Files stored before are known again;
// parts of uploads cut off by a restart are removed, and their senders start
// them over.
func Open(dir string) (*Receiver, error) {
	r := &Receiver{dir: dir, partials: map[string]*partial{}, owners: map[string]uint64{}, known: map[string]fileID{},
		events: map[string]bool{}, now: time.Now}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		switch {
		case strings.HasSuffix(path, ".part"):
			return os.Remove(path)
		case strings.HasPrefix(rel, "node="):
			id, err := checksum(path)
			if err != nil {
				return err
			}
			r.known[filepath.ToSlash(rel)] = id
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := r.loadEvents(); err != nil {
		return nil, err
	}
	if r.filesLog, err = os.OpenFile(filepath.Join(dir, "files.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		_ = r.evf.Close()
		return nil, err
	}
	return r, nil
}

// Known reports how many files the receiver holds.
func (r *Receiver) Known() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.known)
}

// Has reports whether the receiver holds a node's file, by its store path
// (<table>/<partition>/<name>).
func (r *Receiver) Has(node, storePath string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.known["node="+node+"/"+storePath]
	return ok
}

// storedFile is a line of files.jsonl: received_at minus max_ts is how old the
// newest row of the file was when it arrived.
type storedFile struct {
	ReceivedAt time.Time `json:"received_at"`
	Node       string    `json:"node"`
	Table      string    `json:"table"`
	Partition  string    `json:"partition"`
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	Rows       int64     `json:"rows"`
	MinTS      time.Time `json:"min_ts"`
	MaxTS      time.Time `json:"max_ts"`
}

var (
	safeName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	safePart = regexp.MustCompile(`^dt=[0-9a-z-]+/hour=[0-9]{2}$`)
)

// path is where a file goes, relative to the receiver's directory. Every
// part is checked, so that no sender can write outside it.
func path(ref *api.FileRef) (string, error) {
	if ref == nil || !safeName.MatchString(ref.Node) || !safeName.MatchString(ref.Table) ||
		!safePart.MatchString(ref.Partition) || !safeName.MatchString(ref.Name) || ref.Size < 0 {
		return "", errors.New("bad file reference")
	}
	return "node=" + ref.Node + "/" + ref.Table + "/" + ref.Partition + "/" + ref.Name, nil
}

func refuse(format string, args ...any) *api.Ack {
	return &api.Ack{Error: fmt.Sprintf(format, args...)}
}

// Chunk takes one chunk or probe and answers it.
func (r *Receiver) Chunk(c *api.Chunk) *api.Ack {
	rel, err := path(c.GetFile())
	if err != nil {
		return refuse("%v", err)
	}
	want := fileID{crc: c.File.Crc32C, size: c.File.Size}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.known[rel]; ok && id == want && (c.Probe || c.Offset == 0) {
		r.Stats.Duplicates.Add(1)
		return &api.Ack{Ok: true, Duplicate: true}
	}
	p := r.partials[rel]
	if p != nil && p.want != want {
		r.discard(rel, p) // another content under the same name: start over
		p = nil
	}
	if c.Probe {
		if c.Upload != 0 {
			r.owners[rel] = c.Upload // from now on the file is this attempt's
		}
		if p == nil {
			return &api.Ack{Ok: true}
		}
		r.Stats.Resumed.Add(1)
		return &api.Ack{Ok: true, Have: p.size}
	}
	if o := r.owners[rel]; o != 0 && c.Upload != 0 && c.Upload != o {
		// A chunk of an attempt that broke off, late: a gRPC server still
		// takes what the cancelled stream had buffered.
		r.Stats.Superseded.Add(1)
		return refuse("upload %x of the file is superseded by %x", c.Upload, o)
	}
	if c.Offset == 0 {
		if _, ok := r.known[rel]; ok {
			r.Stats.Conflicts.Add(1) // same name, other content: the new upload replaces it
		}
		if p != nil {
			r.discard(rel, p)
		}
		if p, err = r.start(rel, want); err != nil {
			return refuse("%v", err)
		}
	}
	if p == nil || p.size != c.Offset {
		r.Stats.BadOffsets.Add(1)
		var have int64
		if p != nil {
			have = p.size
		}
		return refuse("chunk at offset %d, have %d bytes", c.Offset, have)
	}
	if _, err := p.f.Write(c.Data); err != nil {
		r.discard(rel, p)
		return refuse("%v", err)
	}
	_, _ = p.crc.Write(c.Data)
	p.size += int64(len(c.Data))
	p.touched = r.now()
	if !c.Last {
		return &api.Ack{Ok: true}
	}
	return r.finish(rel, p, c.File)
}

func (r *Receiver) start(rel string, want fileID) (*partial, error) {
	dst := filepath.Join(r.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(dst + ".part")
	if err != nil {
		return nil, err
	}
	p := &partial{f: f, tmp: dst + ".part", dst: dst, want: want, crc: crc32.New(castagnoli), touched: r.now()}
	r.partials[rel] = p
	return p, nil
}

// finish checks a complete file and makes it durable under its name.
func (r *Receiver) finish(rel string, p *partial, ref *api.FileRef) *api.Ack {
	delete(r.partials, rel)
	if p.size != ref.Size || p.crc.Sum32() != ref.Crc32C {
		r.Stats.CRCFailures.Add(1)
		_ = p.f.Close()
		_ = os.Remove(p.tmp)
		return refuse("got %d bytes with CRC %08x, expected %d with %08x", p.size, p.crc.Sum32(), ref.Size, ref.Crc32C)
	}
	if err := p.f.Sync(); err != nil {
		return refuse("%v", err)
	}
	if err := p.f.Close(); err != nil {
		return refuse("%v", err)
	}
	if err := os.Rename(p.tmp, p.dst); err != nil {
		return refuse("%v", err)
	}
	if err := syncDir(filepath.Dir(p.dst)); err != nil {
		return refuse("%v", err)
	}
	r.known[rel] = p.want
	delete(r.owners, rel)
	if b, err := json.Marshal(storedFile{ReceivedAt: r.now().UTC(), Node: ref.Node, Table: ref.Table,
		Partition: ref.Partition, Name: ref.Name, Size: ref.Size, Rows: ref.Rows,
		MinTS: time.Unix(0, ref.MinTs).UTC(), MaxTS: time.Unix(0, ref.MaxTs).UTC()}); err == nil {
		_, _ = r.filesLog.Write(append(b, '\n'))
	}
	r.Stats.FilesStored.Add(1)
	r.Stats.BytesStored.Add(p.size)
	return &api.Ack{Ok: true, Stored: true}
}

func (r *Receiver) discard(rel string, p *partial) {
	_ = p.f.Close()
	_ = os.Remove(p.tmp)
	delete(r.partials, rel)
	r.Stats.Discarded.Add(1)
}

// Expire drops uploads nobody has continued for longer than idle, and what
// probes handed over for files no upload has started.
func (r *Receiver) Expire(idle time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rel, p := range r.partials {
		if r.now().Sub(p.touched) > idle {
			r.discard(rel, p)
		}
	}
	for rel := range r.owners {
		if r.partials[rel] == nil {
			delete(r.owners, rel)
		}
	}
}

// --- urgent events ---

func eventKey(e *api.Event) string {
	return fmt.Sprintf("%s/%d/%s/%s", e.Node, e.Ts, e.ContainerId, e.Action)
}

type storedEvent struct {
	ReceivedAt time.Time  `json:"received_at"`
	Event      *api.Event `json:"event"`
}

func (r *Receiver) eventsPath() string { return filepath.Join(r.dir, "urgent-events.jsonl") }

func (r *Receiver) loadEvents() error {
	data, err := os.ReadFile(r.eventsPath())
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, l := range strings.Split(string(data), "\n") {
		var se storedEvent
		if l != "" && json.Unmarshal([]byte(l), &se) == nil && se.Event != nil {
			r.events[eventKey(se.Event)] = true
		}
	}
	r.evf, err = os.OpenFile(r.eventsPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	return err
}

// Event takes one urgent event and answers it; one already taken is a
// duplicate.
func (r *Receiver) Event(e *api.Event) *api.Ack {
	if e == nil || !safeName.MatchString(e.Node) {
		return refuse("bad event")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := eventKey(e)
	if r.events[k] {
		r.Stats.EventsDuplicate.Add(1)
		return &api.Ack{Ok: true, Duplicate: true}
	}
	b, err := json.Marshal(storedEvent{ReceivedAt: r.now().UTC(), Event: e})
	if err != nil {
		return refuse("%v", err)
	}
	if _, err := r.evf.Write(append(b, '\n')); err != nil {
		return refuse("%v", err)
	}
	r.events[k] = true
	r.Stats.Events.Add(1)
	return &api.Ack{Ok: true, Stored: true}
}

// Close drops the uploads in progress and closes the logs.
func (r *Receiver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rel, p := range r.partials {
		r.discard(rel, p)
	}
	return errors.Join(r.evf.Close(), r.filesLog.Close())
}

func checksum(path string) (fileID, error) {
	f, err := os.Open(path)
	if err != nil {
		return fileID{}, err
	}
	defer func() { _ = f.Close() }()
	h := crc32.New(castagnoli)
	n, err := io.Copy(h, f)
	return fileID{crc: h.Sum32(), size: n}, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
