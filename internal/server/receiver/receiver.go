// Package receiver takes the files nodes send. A file arrives as chunks in
// order into a .part file in the inbox, is checked against the size and
// CRC-32C its sender declared, fsynced, renamed into place and recorded in the
// ledger; only then is the sender told it is stored, and may delete its copy.
//
// Every upload is idempotent: a file the ledger knows with the same content is
// a duplicate at the probe, also after its inbox copy has been archived and
// removed. An upload that broke off is continued: the probe answers how much
// of the file arrived, and hands the file to the probing attempt, so that late
// chunks of the attempt that broke off cannot move the copy from under it.
package receiver

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/pkg/api"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Receiver is shared by the NATS and the gRPC server.
type Receiver struct {
	inbox    string
	ledger   *ledger.Ledger
	stored   func() // a file arrived: wakes the archiver
	mu       sync.Mutex
	partials map[string]*partial
	owners   map[string]uint64 // the upload attempt a probe handed a file to
	now      func() time.Time

	Stats Stats
}

type partial struct {
	f        *os.File
	tmp, dst string
	want     ledger.FileID // what the sender declared
	size     int64
	crc      hash.Hash32
	touched  time.Time
}

// Stats counts what the receiver did.
type Stats struct {
	FilesStored, BytesStored, Duplicates, Conflicts atomic.Int64
	Resumed, BadOffsets, CRCFailures, Discarded     atomic.Int64
	Superseded, Refused                             atomic.Int64
}

// Open opens the inbox. Parts of uploads a restart cut off are removed, and
// their senders start them over; so is every file the ledger does not list as
// waiting for the archive: one archived already, or one renamed into place
// just before a crash and never recorded, and so never acknowledged.
func Open(inbox string, l *ledger.Ledger, stored func()) (*Receiver, error) {
	if err := os.MkdirAll(inbox, 0o755); err != nil {
		return nil, err
	}
	if stored == nil {
		stored = func() {}
	}
	r := &Receiver{inbox: inbox, ledger: l, stored: stored, partials: map[string]*partial{},
		owners: map[string]uint64{}, now: time.Now}
	waiting := map[string]bool{}
	for _, f := range l.PendingArchive() {
		waiting[f] = true
	}
	err := filepath.WalkDir(inbox, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(inbox, path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".part") || !waiting[filepath.ToSlash(rel)] {
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

var (
	safeName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	safePart = regexp.MustCompile(`^dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}$`)
)

// Path is where a file goes, relative to the inbox:
// node=<node>/<table>/<partition>/<name>. Every part is checked, so that no
// sender can write outside the inbox.
func Path(ref *api.FileRef) (string, error) {
	if ref == nil || !safeName.MatchString(ref.Node) || strings.HasPrefix(ref.Node, ".") ||
		(ref.Table != api.TableLogs && ref.Table != api.TableEvents) || !safePart.MatchString(ref.Partition) ||
		!safeName.MatchString(ref.Name) || strings.HasPrefix(ref.Name, ".") || ref.Size < 0 {
		return "", errors.New("bad file reference")
	}
	return "node=" + ref.Node + "/" + ref.Table + "/" + ref.Partition + "/" + ref.Name, nil
}

// ParsePath splits an inbox path into its parts.
func ParsePath(rel string) (node, table, partition, name string, err error) {
	parts := strings.Split(rel, "/")
	if len(parts) != 5 || !strings.HasPrefix(parts[0], "node=") {
		return "", "", "", "", fmt.Errorf("not an inbox path: %q", rel)
	}
	return strings.TrimPrefix(parts[0], "node="), parts[1], parts[2] + "/" + parts[3], parts[4], nil
}

func refuse(format string, args ...any) *api.Ack {
	return &api.Ack{Error: fmt.Sprintf(format, args...)}
}

// Chunk takes one chunk or probe and answers it.
func (r *Receiver) Chunk(c *api.Chunk) *api.Ack {
	rel, err := Path(c.GetFile())
	if err != nil {
		r.Stats.Refused.Add(1)
		return refuse("%v", err)
	}
	want := ledger.FileID{Size: c.File.Size, CRC: c.File.Crc32C}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.ledger.Known(rel); ok && id == want && (c.Probe || c.Offset == 0) {
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
		if _, ok := r.ledger.Known(rel); ok {
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

func (r *Receiver) start(rel string, want ledger.FileID) (*partial, error) {
	dst := filepath.Join(r.inbox, filepath.FromSlash(rel))
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

// finish checks a complete file, makes it durable under its name and records
// it in the ledger.
func (r *Receiver) finish(rel string, p *partial, ref *api.FileRef) *api.Ack {
	delete(r.partials, rel)
	if p.size != ref.Size || p.crc.Sum32() != ref.Crc32C {
		r.Stats.CRCFailures.Add(1)
		_ = p.f.Close()
		_ = os.Remove(p.tmp)
		return refuse("got %d bytes with CRC %08x, expected %d with %08x", p.size, p.crc.Sum32(), ref.Size, ref.Crc32C)
	}
	fail := func(err error) *api.Ack {
		_ = p.f.Close()
		_ = os.Remove(p.tmp)
		return refuse("%v", err)
	}
	if err := p.f.Sync(); err != nil {
		return fail(err)
	}
	if err := p.f.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(p.tmp, p.dst); err != nil {
		return fail(err)
	}
	if err := syncDir(filepath.Dir(p.dst)); err != nil {
		return refuse("%v", err)
	}
	if err := r.ledger.Received(ledger.Received{File: rel, ID: p.want, Rows: ref.Rows, MinTS: ref.MinTs,
		MaxTS: ref.MaxTs}); err != nil {
		// Not recorded, so not stored: the file goes at the next start, and
		// the sender, without an answer, sends it again.
		return refuse("%v", err)
	}
	delete(r.owners, rel)
	r.Stats.FilesStored.Add(1)
	r.Stats.BytesStored.Add(p.size)
	r.stored()
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

// Uploads is how many uploads are in progress.
func (r *Receiver) Uploads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.partials)
}

// Close drops the uploads in progress; their senders continue them after a
// restart, from the start.
func (r *Receiver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rel, p := range r.partials {
		r.discard(rel, p)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
