// Package retry paces reconnects: a pause that doubles after every failure up
// to a ceiling, and starts over once a connection has held for a while.
package retry

import (
	"context"
	"time"
)

// Backoff is not safe for concurrent use; each reconnect loop owns one.
type Backoff struct {
	Min, Max time.Duration
	// Stable is how long a connection must hold before the next failure
	// counts as a first one again. Zero means Max.
	Stable time.Duration

	next      time.Duration
	connected time.Time
}

// Connected records that a connection was made.
func (b *Backoff) Connected(now time.Time) { b.connected = now }

// Next returns the pause before the next attempt and doubles the one after it.
func (b *Backoff) Next(now time.Time) time.Duration {
	stable := b.Stable
	if stable == 0 {
		stable = b.Max
	}
	if b.next == 0 || (!b.connected.IsZero() && now.Sub(b.connected) >= stable) {
		b.next = b.Min
	}
	b.connected = time.Time{}
	d := b.next
	b.next = min(2*b.next, b.Max)
	return d
}

// Sleep waits d or until ctx ends, and reports whether it waited the full d.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
