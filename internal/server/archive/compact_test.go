package archive

import (
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/store"
)

// writeArchiveFile writes n lines of node into an archive file and returns
// their texts.
func writeArchiveFile(t *testing.T, root, rel, node string, first, n int) []string {
	t.Helper()
	rows := make([]store.LogRow, n)
	var lines []string
	for i := range rows {
		line := fmt.Sprintf("%s-line-%d", node, first+i)
		rows[i] = store.LogRow{TS: t0.Add(time.Duration(first+i) * time.Millisecond), Node: node, App: "a",
			ContainerID: "c-" + node, Line: line, Inode: 1, Head: 2, Offset: int64(first+i) * 10}
		lines = append(lines, line)
	}
	path := filepath.Join(root, filepath.FromSlash(rel))
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
	return lines
}

func compactFixture(t *testing.T) (*Archiver, *ledger.Ledger, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	root := filepath.Join(dir, "archive")
	a := New(filepath.Join(dir, "inbox"), root, filepath.Join(dir, "quarantine"), l, nil, slog.New(slog.DiscardHandler))
	a.Compaction(2*time.Hour, time.Minute)
	a.batch = 7
	return a, l, root
}

func lines(t *testing.T, root, hour string) (names []string, texts []string) {
	t.Helper()
	files, err := parquetFiles(filepath.Join(root, filepath.FromSlash(hour)))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		for _, r := range readAll(t, filepath.Join(root, filepath.FromSlash(hour), name)) {
			texts = append(texts, r.Line)
		}
	}
	return files, texts
}

func TestCompactionMergesAnIndexedHour(t *testing.T) {
	a, l, root := compactFixture(t)
	hour := "logs/app=a/dt=2026-10-29/hour=08"
	byName := map[string][]string{}
	for i, node := range []string{"n1", "n1", "n2", "n1", "n2"} {
		name := fmt.Sprintf("%s-p%d.parquet", node, i)
		byName[name] = writeArchiveFile(t, root, hour+"/"+name, node, i*100, 10)
	}
	var want []string // a merge reads its inputs by name
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		want = append(want, byName[name]...)
	}
	// One file of another hour waits for the index; one hour is too recent.
	writeArchiveFile(t, root, "logs/app=a/dt=2026-10-29/hour=09/n1-p1.parquet", "n1", 0, 3)
	writeArchiveFile(t, root, "logs/app=a/dt=2026-10-29/hour=09/n1-p2.parquet", "n1", 3, 3)
	_ = l.Archived("x", []string{"logs/app=a/dt=2026-10-29/hour=09/n1-p2.parquet"})
	writeArchiveFile(t, root, "logs/app=a/dt=2026-10-29/hour=11/n1-p1.parquet", "n1", 0, 3)
	writeArchiveFile(t, root, "logs/app=a/dt=2026-10-29/hour=11/n1-p2.parquet", "n1", 3, 3)

	now := time.Date(2026, 10, 29, 13, 30, 0, 0, time.UTC) // hour 11 ended 1.5 h ago
	merged, err := a.Compact(now)
	if err != nil || merged != 1 {
		t.Fatalf("merged %d hours, %v", merged, err)
	}
	names, texts := lines(t, root, hour)
	if len(names) != 1 || !slices.Equal(texts, want) {
		t.Fatalf("after merging: %v with %d lines", names, len(texts))
	}
	inputs, err := compactedInputs(filepath.Join(root, filepath.FromSlash(hour), names[0]))
	if err != nil || len(inputs) != 5 {
		t.Fatalf("the merged file names %v, %v", inputs, err)
	}
	for _, h := range []string{"hour=09", "hour=11"} {
		if names, _ := lines(t, root, "logs/app=a/dt=2026-10-29/"+h); len(names) != 2 {
			t.Errorf("%s merged: %v", h, names)
		}
	}

	// A file that arrives late for the hour is merged with it later.
	want = append(want, writeArchiveFile(t, root, hour+"/n3-p9.parquet", "n3", 0, 4)...)
	if merged, err := a.Compact(now); err != nil || merged != 1 {
		t.Fatalf("merging the late file: %d, %v", merged, err)
	}
	names, texts = lines(t, root, hour)
	if len(names) != 1 || len(texts) != len(want) {
		t.Fatalf("after the late file: %v with %d lines, want %d", names, len(texts), len(want))
	}
	if a.Stats.Compacted.Load() != 7 {
		t.Errorf("files merged away %d", a.Stats.Compacted.Load())
	}
}

// A crash after the merged file was renamed into place and before its inputs
// were removed: the next pass removes them, and merges nothing twice.
func TestCompactionFinishesAnInterruptedMerge(t *testing.T) {
	a, _, root := compactFixture(t)
	hour := "logs/app=a/dt=2026-10-29/hour=08"
	dir := filepath.Join(root, filepath.FromSlash(hour))
	inputs := []string{"n1-p1.parquet", "n1-p2.parquet"}
	writeArchiveFile(t, root, hour+"/"+inputs[0], "n1", 0, 5)
	writeArchiveFile(t, root, hour+"/"+inputs[1], "n1", 5, 5)
	if err := merge[store.LogRow](dir, inputs, "compacted-0000000000000001.parquet", 4, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Compact(time.Date(2026, 10, 29, 13, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	names, texts := lines(t, root, hour)
	if len(names) != 1 || names[0] != "compacted-0000000000000001.parquet" || len(texts) != 10 {
		t.Fatalf("after finishing the merge: %v with %d lines", names, len(texts))
	}
}
