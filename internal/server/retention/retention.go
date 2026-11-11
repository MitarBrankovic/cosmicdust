// Package retention deletes what is past its retention in the archive: an
// application's hour directory goes once its hour ended longer ago than the
// application's archive period. Partitioned by time like this, deleting is
// removing directories, never rewriting files. Elasticsearch deletes its own
// data: every data stream has the lifecycle of its application.
package retention

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/pkg/api"
)

// Cleaner sweeps the archive.
type Cleaner struct {
	root   string
	dedup  time.Duration // how long the ledger remembers received files
	policy *configs.Retention
	ledger *ledger.Ledger
	log    *slog.Logger
	now    func() time.Time

	Stats Stats
}

// Stats counts what the cleaner did.
type Stats struct {
	Sweeps, Partitions, Errors atomic.Int64
}

// New returns a cleaner of the archive under root; dedup is how long the
// ledger remembers a received file, to recognise it when it is sent again.
func New(root string, dedup time.Duration, policy *configs.Retention, l *ledger.Ledger, log *slog.Logger) *Cleaner {
	if dedup <= 0 {
		dedup = 7 * 24 * time.Hour
	}
	return &Cleaner{root: root, dedup: dedup, policy: policy, ledger: l, log: log, now: time.Now}
}

// Run sweeps at the start and then every interval, until ctx ends.
func (c *Cleaner) Run(ctx context.Context, every time.Duration) {
	for {
		if _, err := c.Sweep(); err != nil {
			c.Stats.Errors.Add(1)
			c.log.Warn("retention sweep failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

// Sweep removes the archive's expired hours and returns how many. It also
// lets the ledger forget the received files older than the dedup window.
func (c *Cleaner) Sweep() (int, error) {
	c.Stats.Sweeps.Add(1)
	now := c.now()
	removed := 0
	var errs []error
	for _, table := range []string{api.TableLogs, api.TableEvents} {
		apps, err := os.ReadDir(filepath.Join(c.root, table))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, appDir := range apps {
			escaped, ok := strings.CutPrefix(appDir.Name(), "app=")
			if !ok || !appDir.IsDir() {
				continue
			}
			keep := c.policy.For(archive.UnescapeApp(escaped), table).Archive.D()
			appPath := filepath.Join(c.root, table, appDir.Name())
			n, err := sweepApp(appPath, now, keep)
			removed += n
			if err != nil {
				errs = append(errs, err)
			}
			_ = os.Remove(appPath) // only if empty
		}
	}
	c.Stats.Partitions.Add(int64(removed))
	if removed > 0 {
		c.log.Info("retention removed archive hours", "hours", removed)
	}
	if _, err := c.ledger.MaybeCompact(now.Add(-c.dedup)); err != nil {
		errs = append(errs, err)
	}
	return removed, errors.Join(errs...)
}

// sweepApp removes the hours of one application that ended more than keep
// ago, and the days left empty.
func sweepApp(appPath string, now time.Time, keep time.Duration) (int, error) {
	days, err := os.ReadDir(appPath)
	if err != nil {
		return 0, err
	}
	removed := 0
	var errs []error
	for _, dayDir := range days {
		day, err := time.Parse("2006-01-02", strings.TrimPrefix(dayDir.Name(), "dt="))
		if err != nil || !dayDir.IsDir() || !strings.HasPrefix(dayDir.Name(), "dt=") {
			continue
		}
		dayPath := filepath.Join(appPath, dayDir.Name())
		if now.Sub(day.Add(time.Hour)) <= keep {
			continue // even its first hour is kept: the whole day is
		}
		hours, err := os.ReadDir(dayPath)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, hourDir := range hours {
			h, err := strconv.Atoi(strings.TrimPrefix(hourDir.Name(), "hour="))
			if err != nil || !hourDir.IsDir() || !strings.HasPrefix(hourDir.Name(), "hour=") || h < 0 || h > 23 {
				continue
			}
			if now.Sub(day.Add(time.Duration(h+1)*time.Hour)) <= keep {
				continue
			}
			if err := os.RemoveAll(filepath.Join(dayPath, hourDir.Name())); err != nil {
				errs = append(errs, err)
				continue
			}
			removed++
		}
		_ = os.Remove(dayPath) // only if empty
	}
	return removed, errors.Join(errs...)
}
