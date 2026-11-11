package wal

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Run makes the WAL a cosmicdust.Component: it seals the open segment once it is old
// enough and retries a block that could not be written, until ctx ends; then
// it writes what is pending and closes. The components that write into the WAL
// must stop before it.
func (w *WAL) Run(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return w.Close()
		case <-t.C:
			w.tick()
		}
	}
}

func (w *WAL) Name() string { return "wal" }

// isTemporary reports an error a writer waits out rather than stops on: no
// space now, perhaps some later.
func isTemporary(err error) bool {
	var t interface{ Temporary() bool }
	return errors.As(err, &t) && t.Temporary()
}

// EventWriter writes the tracker's events into the WAL: whatever has arrived
// by the time it looks goes into one block. Events are never dropped: a block
// that cannot be written for lack of space waits in the WAL, which retries.
type EventWriter struct {
	w    *WAL
	in   <-chan cosmicdust.Event
	log  *slog.Logger
	buf  []cosmicdust.Event
	also []func(cosmicdust.Event)
}

// NewEventWriter returns the component; in is the tracker's Out. Every event
// is also passed to each of also as it is taken, such as the urgent channel's
// Push; they must not block.
func NewEventWriter(w *WAL, in <-chan cosmicdust.Event, log *slog.Logger, also ...func(cosmicdust.Event)) *EventWriter {
	if log == nil {
		log = slog.Default()
	}
	return &EventWriter{w: w, in: in, log: log, also: also}
}

func (e *EventWriter) add(ev cosmicdust.Event) {
	e.buf = append(e.buf, ev)
	for _, f := range e.also {
		f(ev)
	}
}

func (e *EventWriter) Name() string { return "events" }

// Run writes events until ctx ends, then what is still in the channel. It
// stops after the tracker, which drains into the channel as it stops.
func (e *EventWriter) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			e.take()
			return e.write()
		case ev := <-e.in:
			e.add(ev)
			e.take()
			if err := e.write(); err != nil {
				return err
			}
		}
	}
}

// take adds what is already waiting, without blocking.
func (e *EventWriter) take() {
	for len(e.buf) < 1024 {
		select {
		case ev := <-e.in:
			e.add(ev)
		default:
			return
		}
	}
}

func (e *EventWriter) write() error {
	if len(e.buf) == 0 {
		return nil
	}
	if err := e.w.AddEvents(e.buf); err != nil {
		return err
	}
	e.buf = e.buf[:0]
	if err := e.w.Flush(); err != nil {
		if isTemporary(err) {
			e.log.Warn("events wait in memory: the WAL has no space", "err", err)
			return nil
		}
		return err
	}
	return nil
}
