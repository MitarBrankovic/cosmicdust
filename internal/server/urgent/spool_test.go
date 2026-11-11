package urgent

import (
	"os"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/api"
)

func event(node string, ts int64) *api.Event {
	return &api.Event{Node: node, Ts: ts, ContainerId: "c1", Runtime: "docker", Action: "die", Kind: "exit", ExitCode: 3}
}

func TestSpoolKeepsEventsUntilDone(t *testing.T) {
	dir := t.TempDir()
	notified := 0
	s, err := Open(dir, func() { notified++ })
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Now().UnixNano()
	if ack := s.Add(event("n1", ts)); !ack.Stored {
		t.Fatalf("first %+v", ack)
	}
	if ack := s.Add(event("n1", ts)); !ack.Duplicate {
		t.Fatalf("second %+v", ack)
	}
	if ack := s.Add(event("../n1", ts)); ack.Ok {
		t.Fatal("bad node accepted")
	}
	s.Add(event("n1", ts+1))
	if notified != 2 {
		t.Fatalf("notified %d", notified)
	}
	evs, end, err := s.Read(1)
	if err != nil || len(evs) != 1 || evs[0].Event.Ts != ts || evs[0].ReceivedAt == 0 {
		t.Fatalf("read %v %v", evs, err)
	}
	if err := s.Done(end); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// After a restart the reader goes on from where it was done.
	s2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	evs, end, err = s2.Read(10)
	if err != nil || len(evs) != 1 || evs[0].Event.Ts != ts+1 {
		t.Fatalf("after a restart %v %v", evs, err)
	}
	if err := s2.Done(end); err != nil {
		t.Fatal(err)
	}
	if s2.Backlog() != 0 {
		t.Fatalf("backlog %d", s2.Backlog())
	}
}

// A spool the reader has finished with is emptied once it is big, and goes on
// from the start.
func TestSpoolEmptiesWhenDone(t *testing.T) {
	s, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	s.sync = func(*os.File) error { return nil }
	s.emptyAt = 64 << 10
	ts := time.Now().UnixNano()
	for i := range 6000 {
		s.Add(event("n1", ts+int64(i)))
	}
	total := 0
	for {
		evs, end, err := s.Read(1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 0 {
			break
		}
		total += len(evs)
		if err := s.Done(end); err != nil {
			t.Fatal(err)
		}
	}
	if total != 6000 || s.size != 0 {
		t.Fatalf("read %d, spool size %d", total, s.size)
	}
	s.Add(event("n1", ts-1))
	if evs, _, _ := s.Read(10); len(evs) != 1 || evs[0].Event.Ts != ts-1 {
		t.Fatalf("after emptying %v", evs)
	}
}
