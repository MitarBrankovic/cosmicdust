package archive

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/store"
)

var t0 = time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)

// writeNodeFile writes a Parquet file as a node would: lines of three
// applications interleaved, one of them without an application.
func writeNodeFile(t *testing.T, path string, n int) []store.LogRow {
	t.Helper()
	apps := []string{"shop.api", "billing/v2", ""}
	rows := make([]store.LogRow, n)
	for i := range rows {
		rows[i] = store.LogRow{TS: t0.Add(time.Duration(i) * time.Millisecond), Node: "n1", App: apps[i%3],
			ContainerID: fmt.Sprintf("c%d", i%3), Line: fmt.Sprintf("line-%d", i), Inode: 7, Head: 9, Offset: int64(i * 80)}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[store.LogRow](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	return rows
}

func readAll(t *testing.T, path string) []store.LogRow {
	t.Helper()
	var out []store.LogRow
	if err := ReadRows(path, 100, func(rows []store.LogRow) error {
		out = append(out, rows...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestArchiveSplitsByApplication(t *testing.T) {
	dir := t.TempDir()
	inbox, root := filepath.Join(dir, "inbox"), filepath.Join(dir, "archive")
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	file := "node=n1/logs/dt=2026-10-29/hour=08/p1.parquet"
	rows := writeNodeFile(t, filepath.Join(inbox, file), 1000)
	if err := l.Received(ledger.Received{File: file}); err != nil {
		t.Fatal(err)
	}
	woken := 0
	a := New(inbox, root, filepath.Join(dir, "quarantine"), l, func() { woken++ }, slog.New(slog.DiscardHandler))
	a.batch = 64 // several batches per file

	// A crash after the archive files were written and before the ledger knew:
	// archived again, the same files, the same rows.
	if _, _, err := split[store.LogRow](filepath.Join(inbox, file), root, "logs", "dt=2026-10-29/hour=08",
		"n1-p1.parquet", 64, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := a.Archive(file); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"logs/app=_unknown/dt=2026-10-29/hour=08/n1-p1.parquet",
		"logs/app=billing%2Fv2/dt=2026-10-29/hour=08/n1-p1.parquet",
		"logs/app=shop.api/dt=2026-10-29/hour=08/n1-p1.parquet",
	}
	if got := l.PendingIndex(); !slices.Equal(got, want) {
		t.Fatalf("archive files %v", got)
	}
	total := 0
	for _, rel := range want {
		_, app, _, _, err := ParsePath(rel)
		if err != nil {
			t.Fatal(err)
		}
		got := readAll(t, filepath.Join(root, filepath.FromSlash(rel)))
		var expect []store.LogRow
		for _, r := range rows {
			if r.App == app {
				expect = append(expect, r)
			}
		}
		if len(got) != len(expect) {
			t.Fatalf("%s: %d rows, want %d", rel, len(got), len(expect))
		}
		for i := range got {
			if got[i].Line != expect[i].Line || !got[i].TS.Equal(expect[i].TS) || got[i].Offset != expect[i].Offset {
				t.Fatalf("%s row %d: %+v, want %+v", rel, i, got[i], expect[i])
			}
		}
		total += len(got)
	}
	if total != len(rows) {
		t.Fatalf("%d rows archived of %d", total, len(rows))
	}
	if _, err := os.Stat(filepath.Join(inbox, file)); !os.IsNotExist(err) {
		t.Fatal("the archived file is still in the inbox")
	}
	if len(l.PendingArchive()) != 0 || woken != 1 {
		t.Fatalf("pending %v, indexer woken %d", l.PendingArchive(), woken)
	}
	tmps, _ := filepath.Glob(filepath.Join(root, "logs/*/*/*/*.tmp"))
	if len(tmps) != 0 {
		t.Fatalf("temporary files left: %v", tmps)
	}
}

func TestEmptyFileArchivesToNothing(t *testing.T) {
	dir := t.TempDir()
	inbox := filepath.Join(dir, "inbox")
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	file := "node=n1/events/dt=2026-10-29/hour=08/e.parquet"
	path := filepath.Join(inbox, file)
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	f, _ := os.Create(path)
	w := parquet.NewGenericWriter[store.EventRow](f)
	_ = w.Close()
	_ = f.Close()
	_ = l.Received(ledger.Received{File: file})
	a := New(inbox, filepath.Join(dir, "archive"), filepath.Join(dir, "quarantine"), l, nil, slog.New(slog.DiscardHandler))
	if err := a.Archive(file); err != nil {
		t.Fatal(err)
	}
	if len(l.PendingArchive()) != 0 || len(l.PendingIndex()) != 0 {
		t.Fatalf("pending %v %v", l.PendingArchive(), l.PendingIndex())
	}
}

// A received file that is not Parquet - the node sent what it had, the CRC
// matches - cannot be archived however often it is tried: it goes to
// quarantine, and the files behind it go on.
func TestBadFileGoesToQuarantine(t *testing.T) {
	dir := t.TempDir()
	inbox := filepath.Join(dir, "inbox")
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	bad := "node=n1/logs/dt=2026-10-29/hour=08/bad.parquet"
	_ = os.MkdirAll(filepath.Join(inbox, filepath.Dir(bad)), 0o755)
	if err := os.WriteFile(filepath.Join(inbox, bad), []byte("PAR1 but not really"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = l.Received(ledger.Received{File: bad})
	good := "node=n1/logs/dt=2026-10-29/hour=08/good.parquet"
	writeNodeFile(t, filepath.Join(inbox, good), 10)
	_ = l.Received(ledger.Received{File: good})

	a := New(inbox, filepath.Join(dir, "archive"), filepath.Join(dir, "quarantine"), l, nil, slog.New(slog.DiscardHandler))
	for _, f := range l.PendingArchive() {
		if err := a.Archive(f); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine", bad)); err != nil {
		t.Fatalf("not in quarantine: %v", err)
	}
	if a.Stats.Quarantined.Load() != 1 || len(l.PendingArchive()) != 0 || len(l.PendingIndex()) != 3 {
		t.Fatalf("quarantined %d, pending %v / %v", a.Stats.Quarantined.Load(), l.PendingArchive(), l.PendingIndex())
	}
}

func TestPaths(t *testing.T) {
	for _, app := range []string{"shop.api", "org/ns/app", "", "a b", "..", "_unknown2"} {
		rel := Path("events", app, "dt=2026-10-29/hour=23", "n1-p.parquet")
		table, got, part, name, err := ParsePath(rel)
		if err != nil || table != "events" || got != app || part != "dt=2026-10-29/hour=23" || name != "n1-p.parquet" {
			t.Errorf("%q: %q -> %q %q %q %q %v", app, rel, table, got, part, name, err)
		}
	}
}
