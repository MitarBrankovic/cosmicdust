// Package urgent keeps the urgent events nodes send - a container that died
// with an error or was killed for memory - until the indexer has them in the
// search index. An event is appended to a spool file and fsynced before the
// node is answered; the indexer reads the spool on from where it got to.
//
// The same events reach the archive later with the events table: the spool is
// the fast way to the index, not the record.
package urgent

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/pkg/api"
)

// Spool is safe for concurrent use; Read and Done are for one reader.
type Spool struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	size    int64
	done    int64                // bytes the indexer has finished with
	recent  map[string]time.Time // events answered lately: a resend is a duplicate
	notify  func()
	now     func() time.Time
	sync    func(*os.File) error // fsync; tests that are not about durability skip it
	emptyAt int64                // a spool the indexer has finished with is emptied past this size

	Stats Stats
}

// Stats counts the events the spool took.
type Stats struct {
	Events, Duplicates, Refused atomic.Int64
}

// defaultEmptyAt is the size past which a spool the indexer has finished with is
// emptied.
const defaultEmptyAt = 1 << 20

var safeName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Open opens the spool in dir; notify is called after each event taken.
func Open(dir string, notify func()) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if notify == nil {
		notify = func() {}
	}
	s := &Spool{path: filepath.Join(dir, "urgent.jsonl"), recent: map[string]time.Time{}, notify: notify,
		now: time.Now, sync: (*os.File).Sync, emptyAt: defaultEmptyAt}
	var err error
	if s.f, err = os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		return nil, err
	}
	fi, err := s.f.Stat()
	if err != nil {
		_ = s.f.Close()
		return nil, err
	}
	s.size = fi.Size()
	if b, err := os.ReadFile(s.offsetPath()); err == nil {
		if n, err := strconv.ParseInt(string(b), 10, 64); err == nil && n >= 0 && n <= s.size {
			s.done = n
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = s.f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Spool) offsetPath() string { return s.path + ".offset" }

func key(e *api.Event) string {
	return fmt.Sprintf("%s/%d/%s/%s/%s", e.Node, e.Ts, e.ContainerId, e.Runtime, e.Action)
}

// Spooled is an event as the spool keeps it: with the time it arrived.
type Spooled struct {
	ReceivedAt int64      `json:"received_at"` // unix nanoseconds
	Event      *api.Event `json:"event"`
}

// Add takes one urgent event and answers it. The answer is stored once the
// event is durable in the spool.
func (s *Spool) Add(e *api.Event) *api.Ack {
	if e == nil || !safeName.MatchString(e.Node) || e.Ts <= 0 {
		s.Stats.Refused.Add(1)
		return &api.Ack{Error: "bad event"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	k := key(e)
	if _, ok := s.recent[k]; ok {
		s.Stats.Duplicates.Add(1)
		return &api.Ack{Ok: true, Duplicate: true}
	}
	b, err := json.Marshal(Spooled{ReceivedAt: now.UnixNano(), Event: e})
	if err != nil {
		return &api.Ack{Error: err.Error()}
	}
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return &api.Ack{Error: err.Error()}
	}
	if err := s.sync(s.f); err != nil {
		return &api.Ack{Error: err.Error()}
	}
	s.size += int64(len(b)) + 1
	s.recent[k] = now
	if len(s.recent) > 10000 {
		for k, t := range s.recent {
			if now.Sub(t) > time.Hour {
				delete(s.recent, k)
			}
		}
	}
	s.Stats.Events.Add(1)
	s.notify()
	return &api.Ack{Ok: true, Stored: true}
}

// Read returns up to max events the indexer has not finished with, and the
// offset to report to Done once they are indexed.
func (s *Spool) Read(max int) ([]Spooled, int64, error) {
	s.mu.Lock()
	from, size := s.done, s.size
	s.mu.Unlock()
	if from >= size {
		return nil, from, nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, from, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return nil, from, err
	}
	br := bufio.NewReader(io.LimitReader(f, size-from))
	var out []Spooled
	end := from
	for len(out) < max {
		line, err := br.ReadBytes('\n')
		if err != nil {
			break // the end, or a line still being written
		}
		end += int64(len(line))
		var sp Spooled
		if json.Unmarshal(line, &sp) != nil || sp.Event == nil {
			continue // not an event; nothing to index
		}
		out = append(out, sp)
	}
	return out, end, nil
}

// Done records that everything before offset is indexed. A spool the indexer
// has finished with is emptied once it is big.
func (s *Spool) Done(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset <= s.done {
		return nil
	}
	s.done = offset
	if s.done == s.size && s.size > s.emptyAt {
		if err := s.f.Truncate(0); err != nil {
			return err
		}
		s.done, s.size = 0, 0
	}
	// Not fsynced: an offset lost in a crash only indexes events again, and
	// the index keeps one copy of each.
	return os.WriteFile(s.offsetPath(), []byte(strconv.FormatInt(s.done, 10)), 0o644)
}

// Backlog is how many bytes of events wait for the index.
func (s *Spool) Backlog() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size - s.done
}

// Close closes the spool.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
