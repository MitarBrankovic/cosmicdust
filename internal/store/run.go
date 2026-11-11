package store

import (
	"context"
	"errors"
	"time"
)

// Run converts, compacts and keeps the disk in check until ctx ends. One
// goroutine does all three: the reading never waits for them - it writes into
// the WAL, which simply holds more until a conversion catches up - so there is
// no need to run them side by side and lock them against each other. It stops
// before the WAL, which it releases segments of.
func (s *Store) Run(ctx context.Context) error {
	convert := time.NewTicker(s.opts.ConvertEvery)
	defer convert.Stop()
	compact := time.NewTicker(s.opts.Compact.Every)
	defer compact.Stop()
	space := time.NewTicker(time.Second)
	defer space.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-space.C:
			if err := s.space(); err != nil {
				return err
			}
		case <-convert.C:
			// A backlog is converted a piece at a time, each piece committed.
			for {
				n, err := s.convert()
				if errors.Is(err, errNoSpace) {
					s.log.Warn("no room to convert; the WAL waits", "err", err)
					break
				}
				if err != nil {
					return err
				}
				if n == 0 || ctx.Err() != nil {
					break
				}
				if err := s.space(); err != nil {
					return err
				}
			}
		case <-compact.C:
			if err := s.compactAll(time.Now()); err != nil {
				if !errors.Is(err, errNoSpace) {
					return err
				}
				s.log.Warn("no room to compact", "err", err)
			}
		}
	}
}

// space enforces the reserve and the quota, and records what it evicted.
func (s *Store) space() error {
	gaps, err := s.checkSpace(time.Now())
	if len(gaps) > 0 {
		// Events are never dropped: if the WAL cannot take them now it keeps
		// them pending and writes them once it can.
		if aerr := s.opts.WAL.AddEvents(gaps); aerr != nil {
			return aerr
		}
		_ = s.opts.WAL.Flush()
	}
	return err
}
