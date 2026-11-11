// Package nodeid keeps a node's identity stable across restarts.
//
// Every row the agent stores carries the node it came from, and the control
// plane deduplicates by node and position, so the identity must not change
// when the agent, the container it runs in, or the machine restarts. A hostname
// is not good enough (containers get a random one), and an identity handed out
// by an orchestrator may be per boot. The agent therefore makes its own once,
// keeps it in its data directory, and sends any external identity alongside it
// as metadata.
package nodeid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/c12s/cosmicdust/internal/fsutil"
)

// Load returns the identity kept in path, creating it first if the file does
// not exist yet.
func Load(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(b))
		if id == "" {
			return "", fmt.Errorf("node id file %s is empty", path)
		}
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	id, err := generate()
	if err != nil {
		return "", err
	}
	// Atomically: a half-written identity would be read back as a different
	// node.
	if err := fsutil.WriteFileAtomic(path, []byte(id+"\n"), 0o644); err != nil {
		return "", err
	}
	return id, nil
}

// generate returns 128 random bits in hex, the size of a UUID without its
// dashes: unique enough across any fleet, short enough for a Parquet
// dictionary.
func generate() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
