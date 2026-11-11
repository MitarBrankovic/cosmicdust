package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/c12s/cosmicdust/internal/fsutil"
)

// The tracked state is written to disk periodically, not only on a clean
// stop: after a kill -9 a snapshot written at exit does not exist, and
// reconciliation then cannot tell what changed while the agent was away.
// It is written only when something changed, so an idle node does not
// write at all.

const snapshotVersion = 1

type snapshotFile struct {
	Version    int                   `json:"version"`
	Written    time.Time             `json:"written"`
	Containers map[string]*container `json:"containers"`
}

// encodeSnapshot serialises the containers; the caller holds the lock.
func encodeSnapshot(cs map[string]*container, now time.Time) ([]byte, error) {
	return json.Marshal(snapshotFile{Version: snapshotVersion, Written: now, Containers: cs})
}

func writeSnapshot(path string, data []byte) error {
	return fsutil.WriteFileAtomic(path, data, 0o600)
}

// readSnapshot returns the containers kept at path and when they were
// written; none, and a zero time, if there is no file.
func readSnapshot(path string) (map[string]*container, time.Time, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]*container{}, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	var f snapshotFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, time.Time{}, fmt.Errorf("snapshot %s: %w", path, err)
	}
	if f.Version != snapshotVersion {
		return nil, time.Time{}, fmt.Errorf("snapshot %s: version %d, want %d", path, f.Version, snapshotVersion)
	}
	if f.Containers == nil {
		f.Containers = map[string]*container{}
	}
	for id, c := range f.Containers {
		if c == nil {
			delete(f.Containers, id)
			continue
		}
		if c.Views == nil {
			c.Views = map[string]*view{}
		}
		c.Ref.ID = id
	}
	return f.Containers, f.Written, nil
}
