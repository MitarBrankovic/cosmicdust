package retry

import (
	"context"
	"testing"
	"time"
)

func TestBackoffDoublesToTheCeiling(t *testing.T) {
	b := Backoff{Min: time.Second, Max: 8 * time.Second}
	now := time.Unix(0, 0)
	var got []time.Duration
	for range 6 {
		got = append(got, b.Next(now))
	}
	want := []time.Duration{1, 2, 4, 8, 8, 8}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("pauses %v, want %v seconds", got, want)
		}
	}
}

func TestBackoffStartsOverAfterAStableConnection(t *testing.T) {
	b := Backoff{Min: time.Second, Max: 30 * time.Second, Stable: time.Minute}
	now := time.Unix(0, 0)
	b.Next(now)
	b.Next(now)
	if d := b.Next(now); d != 4*time.Second {
		t.Fatalf("third pause %v, want 4s", d)
	}
	// A connection that held for less than a minute: keep doubling.
	b.Connected(now)
	if d := b.Next(now.Add(10 * time.Second)); d != 8*time.Second {
		t.Fatalf("after a short connection %v, want 8s", d)
	}
	// One that held for a minute: the next failure is a first one.
	b.Connected(now)
	if d := b.Next(now.Add(time.Minute)); d != time.Second {
		t.Fatalf("after a stable connection %v, want 1s", d)
	}
}

func TestSleep(t *testing.T) {
	if !Sleep(context.Background(), time.Millisecond) {
		t.Fatal("Sleep did not wait")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Fatal("Sleep waited past a cancelled context")
	}
}
