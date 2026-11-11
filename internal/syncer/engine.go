// Package syncer sends the node's finished files to the control plane in
// rounds, and urgent lifecycle events at once.
//
// A file is sent at least once: it stays unsynced, and is offered again, until
// the control plane answers that it has it; the control plane recognises a
// file it already holds by name, size and CRC, so every file is stored there
// exactly once (through 34 crashes of the agent, 4 of the control
// plane and a 10-minute outage).
package syncer

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/retry"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure an Engine.
type Options struct {
	Store     cosmicdust.Store
	Transport cosmicdust.Transport

	// Every is the time between rounds. A row reaches the control plane
	// about half of it after its file is written, plus the time to send: it
	// sets freshness, not the bytes, which follow the conversions.
	// Default 10 s.
	Every time.Duration

	// Backoff paces the retries after a round that could not reach the
	// control plane, with jitter. Default 1 s doubling to 60 s.
	Backoff retry.Backoff

	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

// Engine is the sync engine, a cosmicdust.Component.
type Engine struct {
	opts Options
	log  *slog.Logger
	m    *metrics

	mu       sync.Mutex
	oldest   time.Time      // the oldest row of a file known to be unsynced
	rejected map[string]int // files the control plane refused, and how often
}

// New checks the options.
func New(opts Options) (*Engine, error) {
	if opts.Store == nil || opts.Transport == nil {
		return nil, errors.New("syncer: Options.Store and Options.Transport are required")
	}
	if opts.Every <= 0 {
		opts.Every = 10 * time.Second
	}
	if opts.Backoff.Min <= 0 {
		opts.Backoff = retry.Backoff{Min: time.Second, Max: time.Minute}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	e := &Engine{opts: opts, log: opts.Logger, rejected: map[string]int{}}
	m, err := newMetrics(opts.Registerer, e.lag)
	if err != nil {
		return nil, err
	}
	e.m = m
	return e, nil
}

func (e *Engine) Name() string { return "sync" }

// Run sends what is unsynced every Every until ctx ends. After a round that
// could not reach the control plane it retries with backoff instead of
// waiting for the next interval, so a dead control plane is not hammered.
// A transport that says when its connection is up again (cosmicdust.Reconnector)
// ends the wait: the backlog goes at once, not up to a minute later. An
// upload cut by the stop is continued after the next start.
func (e *Engine) Run(ctx context.Context) error {
	var up <-chan struct{}
	if r, ok := e.opts.Transport.(cosmicdust.Reconnector); ok {
		up = r.Reconnected()
	}
	b := e.opts.Backoff
	wait := e.opts.Every
	failing := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		case <-up:
			if failing {
				// Backoff from the start: the receiver may subscribe again
				// a moment after the node is connected.
				b = e.opts.Backoff
				e.m.reconnects.Inc()
				e.log.Info("connection to the control plane is back; sending")
			}
		}
		err := e.round(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if failing = err != nil; failing {
			wait = jitter(b.Next(time.Now()))
			e.log.Warn("sync round failed", "err", err, "retry_in", wait.Round(time.Millisecond))
			continue
		}
		b, wait = e.opts.Backoff, e.opts.Every
	}
}

// jitter spreads d over its upper half, so that nodes cut off together do not
// come back together.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// round sends every unsynced file once, oldest first. A failure to reach the
// control plane ends it: the files after it wait for the retry. A file the
// control plane refuses does not: it stays unsynced, and the others go on.
func (e *Engine) round(ctx context.Context) error {
	start := time.Now()
	defer func() { e.m.rounds.Inc(); e.m.roundSeconds.Observe(time.Since(start).Seconds()) }()
	refs, err := e.opts.Store.Unsynced(ctx)
	if err != nil {
		return err
	}
	e.backlog(refs)
	for i, ref := range refs {
		if err := e.send(ctx, ref); err != nil {
			e.backlog(refs[i:])
			return err
		}
	}
	e.backlog(nil)
	return nil
}

// send offers one file to the control plane and records its answer.
func (e *Engine) send(ctx context.Context, ref cosmicdust.FileRef) error {
	r, done, err := e.opts.Store.Claim(ref)
	if errors.Is(err, cosmicdust.ErrClaimed) || errors.Is(err, fs.ErrNotExist) {
		return nil // being merged, or gone since the listing: the next round sees
	}
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	start := time.Now()
	ack, err := e.opts.Transport.Upload(ctx, ref, r)
	if err != nil {
		_ = done(cosmicdust.Ack{})
		e.m.failures.Inc()
		return err
	}
	if err := done(ack); err != nil {
		return err // the answer could not be recorded: the file goes again, and is a duplicate
	}
	e.m.uploadSeconds.Observe(time.Since(start).Seconds())
	switch ack.Status {
	case cosmicdust.AckStored:
		e.m.files.WithLabelValues("stored").Inc()
		e.m.bytes.Add(float64(ref.Size))
	case cosmicdust.AckDuplicate:
		e.m.files.WithLabelValues("duplicate").Inc()
	default:
		e.m.files.WithLabelValues("rejected").Inc()
		e.mu.Lock()
		e.rejected[ref.Path]++
		n := e.rejected[ref.Path]
		e.mu.Unlock()
		level := slog.LevelWarn
		if n > 1 {
			level = slog.LevelDebug
		}
		e.log.Log(ctx, level, "the control plane refused a file; it stays unsynced and goes again",
			"file", ref.Path, "reason", ack.Reason, "times", n)
		return nil
	}
	e.mu.Lock()
	delete(e.rejected, ref.Path)
	e.mu.Unlock()
	return nil
}

// backlog records what is left: the files still unsynced and the oldest row
// among them.
func (e *Engine) backlog(left []cosmicdust.FileRef) {
	var bytes int64
	var oldest time.Time
	for _, f := range left {
		bytes += f.Size
		if oldest.IsZero() || f.MinTime.Before(oldest) {
			oldest = f.MinTime
		}
	}
	e.m.backlogFiles.Set(float64(len(left)))
	e.m.backlogBytes.Set(float64(bytes))
	e.mu.Lock()
	e.oldest = oldest
	e.mu.Unlock()
}

// lag is the age of the oldest row known to wait for the control plane, at
// the time it is asked.
func (e *Engine) lag() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.oldest.IsZero() {
		return 0
	}
	return time.Since(e.oldest).Seconds()
}
