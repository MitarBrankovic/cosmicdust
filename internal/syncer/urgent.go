package syncer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/retry"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// IsUrgent says whether an event should not wait for the next file: a
// container that died with a non-zero exit code, or that the OOM killer hit.
// Starts, clean exits, removals and events inferred from a listing - old news
// by definition - wait.
func IsUrgent(e cosmicdust.Event) bool {
	if e.Inferred || e.ContainerID == "" {
		return false
	}
	switch e.Kind {
	case cosmicdust.KindOOM:
		return true
	case cosmicdust.KindExit:
		return e.OOMKilled || (e.ExitCode != nil && *e.ExitCode != 0)
	}
	return false
}

// UrgentOptions configure an Urgent.
type UrgentOptions struct {
	Transport cosmicdust.Transport

	// Queue is how many urgent events may wait to be sent; beyond it Push
	// drops them (they still go with the events table). Default 256.
	Queue int

	// MaxAge is how long an event is worth sending at once. While the
	// control plane cannot be reached, the events wait in the queue in
	// order and go out when it is back; one that has waited longer than
	// MaxAge is given up. Default 10 min. Timeout bounds one attempt,
	// default 10 s.
	MaxAge  time.Duration
	Timeout time.Duration

	// DockerNamespace is the containerd namespace of the containers Docker
	// runs, when Docker is followed; empty when it is not. For those
	// containers Docker's own exit decides whether the exit is urgent: it
	// comes after any kill Docker was asked for (docker stop, kill, restart,
	// rm -f, compose down), and an exit after one is a stop, not a crash.
	// containerd reports the exit first and cannot tell.
	DockerNamespace string

	Logger     *slog.Logger
	Registerer prometheus.Registerer

	// For tests: the clock the age of an event is read from, and the pauses
	// between attempts (default 250 ms doubling to 5 s).
	now     func() time.Time
	backoff retry.Backoff
}

// Urgent sends urgent lifecycle events at once, one at a time, in order. An
// event the control plane cannot be reached for is tried again, with backoff,
// until it is delivered or older than MaxAge, and the events behind it wait:
// an outage delays them, it does not cost them their place. It is a shortcut,
// not the record: every event is also in the WAL and goes out with the events
// table, so a full queue or a control plane that stays away longer drops the
// shortcut, never the event. Both runtimes report the same exit, ~100-400 ms
// apart, so one event per container incarnation is sent.
type Urgent struct {
	opts UrgentOptions
	log  *slog.Logger
	ch   chan queued
	m    *urgentMetrics

	mu    sync.Mutex
	sent  map[string]int // container -> the last incarnation sent
	asked map[string]int // container -> the last incarnation Docker was asked to signal
}

// NewUrgent checks the options.
func NewUrgent(opts UrgentOptions) (*Urgent, error) {
	if opts.Transport == nil {
		return nil, errors.New("syncer: UrgentOptions.Transport is required")
	}
	if opts.Queue <= 0 {
		opts.Queue = 256
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 10 * time.Minute
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.backoff.Min <= 0 {
		// A short ceiling: an attempt at a control plane that is away costs
		// nothing on the network, and the events of an outage should arrive
		// within seconds of its end.
		opts.backoff = retry.Backoff{Min: 250 * time.Millisecond, Max: 5 * time.Second}
	}
	m, err := newUrgentMetrics(opts.Registerer)
	if err != nil {
		return nil, err
	}
	return &Urgent{opts: opts, log: opts.Logger, ch: make(chan queued, opts.Queue), m: m,
		sent: map[string]int{}, asked: map[string]int{}}, nil
}

// queued is an urgent event and when it joined the queue.
type queued struct {
	e  cosmicdust.Event
	at time.Time
}

func (u *Urgent) Name() string { return "urgent" }

// Push takes every event the tracker passes on and queues the urgent ones,
// without ever blocking the caller.
func (u *Urgent) Push(e cosmicdust.Event) {
	u.mu.Lock()
	switch {
	case e.Kind == cosmicdust.KindRemove:
		delete(u.sent, e.ContainerID)
		delete(u.asked, e.ContainerID)
	case e.Runtime == cosmicdust.RuntimeDocker && e.Action == "kill" && !e.Inferred:
		u.asked[e.ContainerID] = e.Epoch
	}
	if !u.urgent(e) {
		u.mu.Unlock()
		return
	}
	if last, ok := u.sent[e.ContainerID]; ok && e.Epoch <= last {
		u.mu.Unlock()
		return // the other runtime's word on the same incarnation
	}
	u.sent[e.ContainerID] = e.Epoch
	u.mu.Unlock()
	select {
	case u.ch <- queued{e: e, at: u.opts.now()}:
	default:
		u.m.result.WithLabelValues("dropped").Inc()
	}
}

// urgent is IsUrgent with what only the stream tells: whether Docker was
// asked to end the incarnation. A signal that does not end a container
// (docker kill -s HUP) counts too; a crash after one, in the same
// incarnation, waits for the events table.
func (u *Urgent) urgent(e cosmicdust.Event) bool {
	if !IsUrgent(e) {
		return false
	}
	if e.Kind != cosmicdust.KindExit || e.OOMKilled || u.opts.DockerNamespace == "" || e.Namespace != u.opts.DockerNamespace {
		return true
	}
	if e.Runtime != cosmicdust.RuntimeDocker {
		return false // Docker's own exit follows, after any kill it was asked for
	}
	if epoch, ok := u.asked[e.ContainerID]; ok && epoch == e.Epoch {
		u.m.result.WithLabelValues("stopped").Inc()
		return false
	}
	return true
}

// Run sends queued events until ctx ends.
func (u *Urgent) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case q := <-u.ch:
			u.send(ctx, q)
		}
	}
}

// send delivers one event, or gives it up once it has waited MaxAge. The
// events queued behind it wait meanwhile: what keeps this one from arriving
// keeps them too.
func (u *Urgent) send(ctx context.Context, q queued) {
	e, b := q.e, u.opts.backoff
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, u.opts.Timeout)
		ack, err := u.opts.Transport.Urgent(actx, e)
		cancel()
		if err == nil {
			switch ack.Status {
			case cosmicdust.AckStored:
				u.m.result.WithLabelValues("stored").Inc()
				u.m.latency.Observe(time.Since(e.Time).Seconds())
			case cosmicdust.AckDuplicate:
				u.m.result.WithLabelValues("duplicate").Inc()
			default:
				u.m.result.WithLabelValues("rejected").Inc()
				u.log.Warn("the control plane refused an urgent event", "container", e.Name, "action", e.Action, "reason", ack.Reason)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if attempt == 1 {
			u.log.Warn("urgent event not delivered, waiting for the control plane", "container", e.Name,
				"action", e.Action, "err", err)
		}
		if u.opts.now().Sub(q.at) >= u.opts.MaxAge {
			u.m.result.WithLabelValues("failed").Inc()
			u.log.Warn("urgent event given up; it goes with the events table", "container", e.Name, "action", e.Action,
				"attempts", attempt)
			return
		}
		if !retry.Sleep(ctx, jitter(b.Next(time.Now()))) {
			return
		}
	}
}
