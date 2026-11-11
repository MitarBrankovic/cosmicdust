package ledger

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func open(t *testing.T, dir string) *Ledger {
	t.Helper()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// What the ledger says after a restart is what it said before: the files that
// arrived, which wait for the archive, which archive files wait for the index.
func TestReplayAfterRestart(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir)
	must(t, l.Received(Received{File: "node=a/logs/dt=2026-10-29/hour=10/p1.parquet", ID: FileID{Size: 10, CRC: 1}}))
	must(t, l.Received(Received{File: "node=a/logs/dt=2026-10-29/hour=10/p2.parquet", ID: FileID{Size: 20, CRC: 2}}))
	must(t, l.Received(Received{File: "node=b/events/dt=2026-10-29/hour=10/p1.parquet", ID: FileID{Size: 30, CRC: 3}}))
	must(t, l.Archived("node=a/logs/dt=2026-10-29/hour=10/p1.parquet", []string{"x/1", "y/1"}))
	must(t, l.Indexed("x/1"))
	_ = l.Close()

	l2 := open(t, dir)
	if id, ok := l2.Known("node=a/logs/dt=2026-10-29/hour=10/p2.parquet"); !ok || id != (FileID{Size: 20, CRC: 2}) {
		t.Fatalf("known %v %v", id, ok)
	}
	if got := l2.PendingArchive(); !slices.Equal(got, []string{"node=a/logs/dt=2026-10-29/hour=10/p2.parquet",
		"node=b/events/dt=2026-10-29/hour=10/p1.parquet"}) {
		t.Fatalf("pending archive %v", got)
	}
	if got := l2.PendingIndex(); !slices.Equal(got, []string{"y/1"}) {
		t.Fatalf("pending index %v", got)
	}
}

// A crash in the middle of an append leaves a torn last line; it was never
// fsynced, so it is dropped, and the ledger goes on after it.
func TestTornLastLine(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir)
	must(t, l.Received(Received{File: "f1", ID: FileID{Size: 1}}))
	_ = l.Close()
	f, err := os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	must(t, err)
	_, _ = f.WriteString(`{"op":"received","fi`)
	_ = f.Close()

	l2 := open(t, dir)
	must(t, l2.Received(Received{File: "f2", ID: FileID{Size: 2}}))
	_ = l2.Close()
	l3 := open(t, dir)
	if _, ok := l3.Known("f1"); !ok {
		t.Fatal("f1 lost")
	}
	if _, ok := l3.Known("f2"); !ok {
		t.Fatal("f2, written after the torn line, lost")
	}

	// A broken line in the middle is not a torn tail: the ledger refuses to
	// guess.
	_ = l3.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "ledger.jsonl"))
	must(t, os.WriteFile(filepath.Join(dir, "ledger.jsonl"), append([]byte("{broken\n"), data...), 0o644))
	if _, err := Open(dir); err == nil {
		t.Fatal("a broken line in the middle accepted")
	}
}

// Compaction forgets archived files older than the horizon, and keeps
// everything still to do: files to archive, archive files to index.
func TestCompactionKeepsTheWork(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir)
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	l.sync = func(*os.File) error { return nil }
	l.minCompact = 100
	for i := range 300 {
		file := fmt.Sprintf("old/%d", i)
		must(t, l.Received(Received{File: file, ID: FileID{Size: int64(i)}}))
		must(t, l.Archived(file, []string{"arch/" + file}))
		if i != 7 {
			must(t, l.Indexed("arch/"+file))
		}
	}
	now = now.Add(48 * time.Hour)
	must(t, l.Received(Received{File: "new/archived", ID: FileID{Size: 1}}))
	must(t, l.Archived("new/archived", []string{"arch/new"}))
	must(t, l.Received(Received{File: "new/waiting", ID: FileID{Size: 2}}))

	compacted, err := l.MaybeCompact(now.Add(-24 * time.Hour))
	if err != nil || !compacted {
		t.Fatalf("compacted %v, %v", compacted, err)
	}
	check := func(l *Ledger, waiting ...string) {
		t.Helper()
		if _, ok := l.Known("old/5"); ok {
			t.Error("an archived file older than the horizon still known")
		}
		if _, ok := l.Known("new/archived"); !ok {
			t.Error("a recent archived file forgotten")
		}
		if got := l.PendingArchive(); !slices.Equal(got, waiting) {
			t.Errorf("pending archive %v, want %v", got, waiting)
		}
		if got := l.PendingIndex(); !slices.Equal(got, []string{"arch/old/7", "arch/new"}) {
			t.Errorf("pending index %v", got)
		}
	}
	check(l, "new/waiting")
	if l.lines > 10 {
		t.Errorf("%d lines after compaction", l.lines)
	}
	must(t, l.Received(Received{File: "after", ID: FileID{Size: 3}}))
	_ = l.Close()
	check(open(t, dir), "new/waiting", "after")
}
