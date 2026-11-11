package cosmicdust

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Component is one long-running part of the agent: the lifecycle tracker, the
// tailer, the converter, the sync engine, the HTTP endpoint.
type Component interface {
	// Name labels the component in logs and in cosmicdust_agent_component_up.
	Name() string

	// Run works until ctx ends and then returns, nil or ctx.Err() on a clean
	// stop. Returning early, with or without an error, stops the agent.
	Run(ctx context.Context) error
}

// Options configure an Agent.
type Options struct {
	// Node is the node's stable identity, stored with every row.
	Node string

	// Components run together. They stop in the order given, each after the
	// one before it has returned, so list them in data-flow order: sources
	// first, then the parts that drain them, then whatever has to outlive
	// them (the HTTP endpoint, so the last metrics can still be scraped).
	Components []Component

	// ShutdownTimeout bounds the whole stop. Default 30s.
	ShutdownTimeout time.Duration

	// Logger defaults to slog.Default().
	Logger *slog.Logger

	// Registerer receives the agent's self-metrics; nil means none.
	Registerer prometheus.Registerer
}

// Agent runs the components of one node.
type Agent struct {
	node       string
	components []Component
	timeout    time.Duration
	log        *slog.Logger
	up         *prometheus.GaugeVec

	ran atomic.Bool
	mu  sync.Mutex
	// running is nil until Run starts; then it holds every component that
	// has not returned.
	running map[string]bool
}

// New checks the options and returns an agent ready to Run.
func New(opts Options) (*Agent, error) {
	if opts.Node == "" {
		return nil, errors.New("cosmicdust: Options.Node is empty")
	}
	seen := make(map[string]bool, len(opts.Components))
	for i, c := range opts.Components {
		if c == nil {
			return nil, fmt.Errorf("cosmicdust: component %d is nil", i)
		}
		name := c.Name()
		if name == "" {
			return nil, fmt.Errorf("cosmicdust: component %d has no name", i)
		}
		if seen[name] {
			return nil, fmt.Errorf("cosmicdust: two components are named %q", name)
		}
		seen[name] = true
	}
	a := &Agent{
		node:       opts.Node,
		components: opts.Components,
		timeout:    opts.ShutdownTimeout,
		log:        opts.Logger,
	}
	if a.timeout <= 0 {
		a.timeout = 30 * time.Second
	}
	if a.log == nil {
		a.log = slog.Default()
	}
	a.up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cosmicdust_agent", Name: "component_up",
		Help: "1 while a component runs, 0 once it has returned.",
	}, []string{"component"})
	if opts.Registerer != nil {
		if err := opts.Registerer.Register(a.up); err != nil {
			return nil, fmt.Errorf("cosmicdust: register metrics: %w", err)
		}
	}
	return a, nil
}

// Node returns the node identity the agent runs with.
func (a *Agent) Node() string { return a.node }

// Health returns nil while every component runs, and otherwise says which do
// not.
func (a *Agent) Health() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running == nil {
		return errors.New("not started")
	}
	var down []string
	for _, c := range a.components {
		if !a.running[c.Name()] {
			down = append(down, c.Name())
		}
	}
	if len(down) > 0 {
		return fmt.Errorf("not running: %s", strings.Join(down, ", "))
	}
	return nil
}

func (a *Agent) setRunning(name string, up bool) {
	a.mu.Lock()
	a.running[name] = up
	a.mu.Unlock()
	v := 0.0
	if up {
		v = 1
	}
	a.up.WithLabelValues(name).Set(v)
}

type runner struct {
	c      Component
	cancel context.CancelFunc
	done   chan struct{}
	err    error // set before done closes
}

// Run starts every component and blocks until ctx ends or a component returns
// on its own; either way it then stops the components in order and returns. A
// clean stop returns nil. If the stop takes longer than ShutdownTimeout, Run
// cancels whatever is left and returns without waiting for it: the process is
// about to exit, and a stuck component must not hold it.
//
// An agent runs once.
func (a *Agent) Run(ctx context.Context) error {
	if !a.ran.CompareAndSwap(false, true) {
		return errors.New("cosmicdust: agent already ran")
	}
	a.mu.Lock()
	a.running = make(map[string]bool, len(a.components))
	a.mu.Unlock()

	runners := make([]*runner, len(a.components))
	returned := make(chan int, len(a.components))
	for i, c := range a.components {
		// The agent, not the caller's context, decides when each component
		// stops; values still pass through.
		cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		r := &runner{c: c, cancel: cancel, done: make(chan struct{})}
		runners[i] = r
		a.setRunning(c.Name(), true)
		go func() {
			defer close(r.done)
			r.err = c.Run(cctx)
			a.setRunning(c.Name(), false)
			returned <- i
		}()
	}
	a.log.Info("agent started", "node", a.node, "components", len(runners))

	var errs []error
	failed := -1
	select {
	case <-ctx.Done():
		a.log.Info("agent stopping", "reason", context.Cause(ctx))
	case i := <-returned:
		failed = i
		r := runners[i]
		err := r.err
		if err == nil {
			err = errors.New("returned before the agent stopped")
		}
		errs = append(errs, fmt.Errorf("%s: %w", r.c.Name(), err))
		a.log.Error("component failed, agent stopping", "component", r.c.Name(), "err", err)
	}

	deadline := time.NewTimer(a.timeout)
	defer deadline.Stop()
	for i, r := range runners {
		r.cancel()
		select {
		case <-r.done:
		case <-deadline.C:
			for _, rest := range runners[i:] {
				rest.cancel()
			}
			errs = append(errs, fmt.Errorf("%s did not stop within %s", r.c.Name(), a.timeout))
			return fmt.Errorf("cosmicdust: %w", errors.Join(errs...))
		}
		if i != failed && r.err != nil && !errors.Is(r.err, context.Canceled) {
			errs = append(errs, fmt.Errorf("%s: stop: %w", r.c.Name(), r.err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cosmicdust: %w", errors.Join(errs...))
	}
	a.log.Info("agent stopped")
	return nil
}
