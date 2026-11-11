package retention

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/ledger"
)

func TestSweepKeepsEachApplicationsPeriod(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "archive")
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	policy := configs.DefaultRetention() // logs 30 days in the archive
	policy.Apps = map[string]configs.Policy{"short/app": {Logs: configs.Window{Archive: configs.Days(2)}}}

	now := time.Date(2026, 10, 29, 12, 30, 0, 0, time.UTC)
	file := func(table, app, partition string) string {
		rel := archive.Path(table, app, partition, "n1-p.parquet")
		path := filepath.Join(root, filepath.FromSlash(rel))
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	gone := []string{
		file("logs", "short/app", "dt=2026-10-27/hour=10"), // ended 2 days and 1.5 hours ago
		file("logs", "default", "dt=2026-09-28/hour=23"),   // ended 30 days and 12.5 hours ago
	}
	kept := []string{
		file("logs", "short/app", "dt=2026-10-27/hour=12"),   // ended 1 day 23.5 hours ago
		file("logs", "default", "dt=2026-09-29/hour=12"),     // ended under 30 days ago
		file("events", "short/app", "dt=2026-10-01/hour=00"), // events keep the default 90 days
	}
	c := New(root, 7*24*time.Hour, policy, l, slog.New(slog.DiscardHandler))
	c.now = func() time.Time { return now }
	removed, err := c.Sweep()
	if err != nil || removed != 2 {
		t.Fatalf("removed %d, %v", removed, err)
	}
	for _, p := range gone {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("kept %s", p)
		}
	}
	for _, p := range kept {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("removed %s", p)
		}
	}
	// Days and applications left empty go too.
	if _, err := os.Stat(filepath.Join(root, "logs", "app=default", "dt=2026-09-28")); !os.IsNotExist(err) {
		t.Error("an empty day is left")
	}
}
