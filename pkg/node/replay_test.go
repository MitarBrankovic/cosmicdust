package node

import (
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/store/wal"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// The tracker gets back the events of the blocks written since the time it
// asks for, oldest first.
func TestReplayWALSince(t *testing.T) {
	w, err := wal.Open(wal.Options{Dir: t.TempDir(), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	add := func(id string) {
		t.Helper()
		ev := cosmicdust.Event{Time: time.Now(), Runtime: cosmicdust.RuntimeDocker, Kind: cosmicdust.KindStart, ContainerID: id}
		if err := w.AddEvents([]cosmicdust.Event{ev}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	add("old")
	time.Sleep(10 * time.Millisecond)
	since := time.Now()
	add("new1")
	add("new2")
	if err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	replay := func(since time.Time) []string {
		var got []string
		if err := replayWAL(w, since, func(e cosmicdust.Event) { got = append(got, e.ContainerID) }); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := replay(since); !slices.Equal(got, []string{"new1", "new2"}) {
		t.Fatalf("since %v: %v", since, got)
	}
	if got := replay(time.Time{}); !slices.Equal(got, []string{"old", "new1", "new2"}) {
		t.Fatalf("all: %v", got)
	}
}
