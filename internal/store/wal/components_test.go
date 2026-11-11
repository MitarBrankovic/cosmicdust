package wal

import (
	"context"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func TestEventWriterWritesWhatArrived(t *testing.T) {
	w := open(t, t.TempDir())
	in := make(chan cosmicdust.Event, 16)
	ew := NewEventWriter(w, in, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ew.Run(ctx) }()
	for i := range 3 {
		in <- cosmicdust.Event{Time: t0.Add(time.Duration(i)), Runtime: "docker", Action: "start", Kind: cosmicdust.KindStart, ContainerID: "c1"}
	}
	in <- cosmicdust.Event{Time: t0, Runtime: "docker", Action: "die", Kind: cosmicdust.KindExit, ContainerID: "c1"}
	cancel() // what is still in the channel is written on the way out
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	seal(t, w)
	var n int
	for _, seg := range w.Sealed() {
		if err := w.Read(seg, func(b *Block) error { n += len(b.Events); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if n != 4 {
		t.Fatalf("%d events in the WAL, want 4", n)
	}
}
