package store

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// The side of the store the sync engine sees: the files to send, a claim on
// one while it is being sent, and the control plane's answer. A claim
// keeps compaction and eviction away from the file; a stored answer marks it
// synced, and from then on the reserve and the quota may delete it.

// Unsynced lists the committed files the control plane does not have and
// nobody is working with, oldest first: events before logs within an hour, so
// that the record of what happened goes first.
func (s *Store) Unsynced(ctx context.Context) ([]cosmicdust.FileRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []cosmicdust.FileRef
	for _, table := range []string{tableEvents, tableLogs} {
		dirs, err := s.partitionDirs(table)
		if err != nil {
			return nil, err
		}
		for _, dir := range dirs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			m, err := loadManifest(dir)
			if err != nil {
				return nil, err
			}
			for _, f := range m.Files {
				if f.SyncedAt == nil && s.final(f) && s.claims[fileKey(table, m.Partition, f.Name)] == "" {
					out = append(out, cosmicdust.FileRef{
						Table: table, Path: path.Join(table, m.Partition, f.Name), Size: f.Bytes, CRC: f.CRC32C,
						Rows: f.Rows, MinTime: f.MinTS, MaxTime: f.MaxTS,
					})
				}
			}
		}
	}
	slices.SortStableFunc(out, func(a, b cosmicdust.FileRef) int {
		pa, pb := path.Dir(strings.TrimPrefix(a.Path, a.Table+"/")), path.Dir(strings.TrimPrefix(b.Path, b.Table+"/"))
		return cmp.Compare(pa, pb)
	})
	return out, nil
}

// Claim opens a file for sending. done must be called once, with the control
// plane's answer or a zero Ack if the sending failed; a stored answer marks
// the file synced. The first claim of a file records it as offered, before it
// leaves (OfferedAt).
func (s *Store) Claim(ref cosmicdust.FileRef) (io.ReadCloser, func(cosmicdust.Ack) error, error) {
	table, part, name, err := splitRef(ref)
	if err != nil {
		return nil, nil, err
	}
	key := fileKey(table, part, name)
	s.mu.Lock()
	err = s.offer(table, part, name)
	if err == nil && !s.claim(key, "sync") {
		err = cosmicdust.ErrClaimed
	}
	s.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(filepath.Join(s.opts.Root, filepath.FromSlash(ref.Path)))
	if err != nil {
		s.release(key)
		return nil, nil, err
	}
	done := func(ack cosmicdust.Ack) error {
		defer s.release(key)
		if !ack.Stored() {
			return nil
		}
		return s.markSynced(table, part, name, time.Now())
	}
	return f, done, nil
}

// offer records, once, that a file goes to the control plane. s.mu is held.
func (s *Store) offer(table, part, name string) error {
	dir := s.partDir(table, part)
	m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(m.Files, func(f manifestFile) bool { return f.Name == name })
	if i < 0 {
		return fmt.Errorf("store: %s/%s/%s is not in its manifest: %w", table, part, name, fs.ErrNotExist)
	}
	if m.Files[i].OfferedAt != nil || s.claims[fileKey(table, part, name)] != "" {
		return nil // offered before, or being merged: Claim answers that
	}
	t := time.Now().UTC()
	m.Files[i].OfferedAt = &t
	return saveManifest(dir, m)
}

func (s *Store) markSynced(table, part, name string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.partDir(table, part)
	m, err := loadManifest(dir)
	if err != nil {
		return err
	}
	for i := range m.Files {
		if m.Files[i].Name == name {
			t := at.UTC()
			m.Files[i].SyncedAt = &t
			s.m.synced.Inc()
			return saveManifest(dir, m)
		}
	}
	// The claim kept compaction and eviction away: a bug, not a race.
	return fmt.Errorf("store: synced file %s/%s/%s is no longer in its manifest", table, part, name)
}

// GC deletes synced files, oldest first, until the store, WAL included, holds
// at most quota bytes.
func (s *Store) GC(quota int64) error {
	if quota <= 0 {
		return nil
	}
	s.mu.Lock()
	size, err := s.sizeOnDisk()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	size += s.walBytes()
	if size <= quota {
		return nil
	}
	_, _, err = s.freeUp(size-quota, false, time.Now())
	return err
}

func splitRef(ref cosmicdust.FileRef) (table, part, name string, err error) {
	clean := path.Clean(ref.Path)
	parts := strings.Split(clean, "/")
	if len(parts) != 4 || (parts[0] != tableLogs && parts[0] != tableEvents) || strings.Contains(clean, "..") {
		return "", "", "", fmt.Errorf("store: %q is not a file of the store", ref.Path)
	}
	return parts[0], parts[1] + "/" + parts[2], parts[3], nil
}

var _ cosmicdust.Store = (*Store)(nil)
