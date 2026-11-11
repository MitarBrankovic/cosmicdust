// Package containerd is the event source for containerd: task and container
// events from its event service, listings through its container and task
// services.
//
// containerd sees an exit ~350-420 ms before Docker does and stamps it with
// the moment the process ended, and listing through it is ~50x
// cheaper than through Docker, so it serves as the
// periodic safety net. It knows a Docker container only while it runs: in
// Docker's namespace the container object is created and deleted with every
// incarnation.
//
// Access: /run/containerd/containerd.sock is root:root 0660 with no group to
// join, so this source needs root.
package containerd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	apievents "github.com/containerd/containerd/api/events"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/events"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	typeurl "github.com/containerd/typeurl/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/c12s/cosmicdust/internal/retry"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure the source.
type Options struct {
	// Address is containerd's socket. Default /run/containerd/containerd.sock.
	Address string

	// Namespaces are the ones followed. Default "moby" (Docker) and
	// "default" (nerdctl, ctr).
	Namespaces []string

	// DockerNamespace is where Docker runs its containers. There the
	// container object lives for one incarnation, so its create and delete
	// are not the container's, and a stopped container is not listed. Empty
	// when Docker is not followed: containerd is then the only witness and
	// its objects are taken at face value. Default "moby".
	DockerNamespace string
	// NoDocker sets DockerNamespace to empty rather than its default.
	NoDocker bool

	// AppLabels are the label keys that name a container's application.
	AppLabels []string

	// Backoff paces resubscribing. Zero: 1s doubling to 30s.
	Backoff retry.Backoff

	Logger *slog.Logger
}

// Source reads one containerd.
type Source struct {
	cli    *containerd.Client
	opts   Options
	log    *slog.Logger
	wanted map[string]bool
}

// New returns a source. The gRPC connection is made lazily and remade by gRPC
// itself after containerd restarts; only the subscription has to be renewed.
func New(opts Options) (*Source, error) {
	if opts.Address == "" {
		opts.Address = "/run/containerd/containerd.sock"
	}
	if len(opts.Namespaces) == 0 {
		opts.Namespaces = []string{"moby", "default"}
	}
	switch {
	case opts.NoDocker:
		opts.DockerNamespace = ""
	case opts.DockerNamespace == "":
		opts.DockerNamespace = "moby"
	}
	if opts.Backoff.Min == 0 {
		opts.Backoff = retry.Backoff{Min: time.Second, Max: 30 * time.Second}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	cli, err := containerd.New(opts.Address)
	if err != nil {
		return nil, fmt.Errorf("containerd client (%s): %w", opts.Address, err)
	}
	wanted := make(map[string]bool, len(opts.Namespaces))
	for _, ns := range opts.Namespaces {
		wanted[ns] = true
	}
	return &Source{cli: cli, opts: opts, log: opts.Logger.With("runtime", cosmicdust.RuntimeContainerd), wanted: wanted}, nil
}

// Close releases the client.
func (s *Source) Close() error { return s.cli.Close() }

func (s *Source) Runtime() string { return cosmicdust.RuntimeContainerd }

// subscribeFilters ask for the events that carry state, and /tasks/create,
// the moment a task's cgroup exists, for the tracker's watch of its OOM
// counter. Exec processes start and end with every Docker health check;
// their exits come on /tasks/exit too and are dropped in event().
var subscribeFilters = []string{
	`topic=="/tasks/create"`,
	`topic=="/tasks/start"`,
	`topic=="/tasks/exit"`,
	`topic=="/tasks/oom"`,
	`topic=="/tasks/paused"`,
	`topic=="/tasks/resumed"`,
	`topic~="/containers/.*"`,
}

func (s *Source) Watch(ctx context.Context, out chan<- cosmicdust.Event) error {
	b := s.opts.Backoff
	reason, lastErr := "start", error(nil)
	var down time.Time
	var said string // the last error logged at warning level
	// The call needs a namespace, but the stream is not scoped to it:
	// one subscription delivers every namespace, filtered in event().
	nctx := namespaces.WithNamespace(ctx, s.opts.Namespaces[0])
	for {
		if _, err := s.cli.Version(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			pause := b.Next(time.Now())
			level := slog.LevelWarn
			if err.Error() == said {
				level = slog.LevelDebug // the same refusal every half minute is not news
			}
			said = err.Error()
			s.log.Log(ctx, level, "containerd unreachable; retrying (root is needed for its socket)", "err", err, "in", pause)
			if down.IsZero() {
				down, lastErr = time.Now(), err
			}
			if !retry.Sleep(ctx, pause) {
				return nil
			}
			continue
		}
		sctx, cancel := context.WithCancel(nctx)
		envelopes, errs := s.cli.Subscribe(sctx, subscribeFilters...)
		b.Connected(time.Now())
		said = ""

		mark := cosmicdust.Event{
			Time: time.Now(), Runtime: cosmicdust.RuntimeContainerd, Action: "resync",
			Kind: cosmicdust.KindResync, Fields: map[string]string{"reason": reason},
		}
		if !down.IsZero() {
			mark.Fields["down"] = time.Since(down).Round(time.Millisecond).String()
		}
		if lastErr != nil {
			mark.Fields["error"] = lastErr.Error()
		}
		if !send(ctx, out, mark) {
			cancel()
			return nil
		}
		s.log.Info("containerd events subscribed", "reason", reason, "namespaces", s.opts.Namespaces)

		err := s.forward(ctx, envelopes, errs, out)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		// A restart of containerd ends the subscription for good; without
		// this loop the tracker would show a long-gone container running
		// forever.
		reason, lastErr, down = "reconnect", err, time.Now()
		pause := b.Next(time.Now())
		s.log.Warn("containerd events stream broke; resubscribing", "err", err, "in", pause)
		if !retry.Sleep(ctx, pause) {
			return nil
		}
	}
}

func (s *Source) forward(ctx context.Context, envelopes <-chan *events.Envelope, errs <-chan error, out chan<- cosmicdust.Event) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case env, ok := <-envelopes:
			if !ok {
				return errors.New("event stream closed")
			}
			ev, keep, err := s.event(env)
			if err != nil {
				s.log.Warn("cannot decode event", "topic", env.Topic, "err", err)
				continue
			}
			if keep && !send(ctx, out, ev) {
				return nil
			}
		case err, ok := <-errs:
			if !ok || err == nil {
				return errors.New("event stream closed")
			}
			if isShutdown(ctx, err) {
				return nil
			}
			return err
		}
	}
}

func send(ctx context.Context, out chan<- cosmicdust.Event, ev cosmicdust.Event) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// isShutdown reports whether an error is our own stop rather than a failure.
// With a deadline, gRPC can tear the stream down a moment before the context
// reports done, so the status code is checked too (one run in five).
func isShutdown(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

// event decodes one envelope. keep is false for events of namespaces not
// followed and for exec processes.
func (s *Source) event(env *events.Envelope) (ev cosmicdust.Event, keep bool, err error) {
	if !s.wanted[env.Namespace] {
		return cosmicdust.Event{}, false, nil
	}
	ev = cosmicdust.Event{
		Time:      env.Timestamp,
		Runtime:   cosmicdust.RuntimeContainerd,
		Namespace: env.Namespace,
		Action:    env.Topic,
		Kind:      cosmicdust.KindOther,
	}
	if env.Event == nil {
		return ev, false, nil
	}
	decoded, err := typeurl.UnmarshalAny(env.Event)
	if err != nil {
		return ev, false, err
	}
	docker := env.Namespace == s.opts.DockerNamespace
	switch e := decoded.(type) {
	case *apievents.ContainerCreate:
		// No labels here, hence no application yet: the tracker describes
		// the container, and the description has them.
		ev.ContainerID = e.GetID()
		ev.Image = e.GetImage()
		if !docker {
			ev.Kind = cosmicdust.KindCreate
		}
	case *apievents.ContainerUpdate:
		ev.ContainerID = e.GetID()
		ev.Image = e.GetImage()
	case *apievents.ContainerDelete:
		ev.ContainerID = e.GetID()
		if !docker {
			ev.Kind = cosmicdust.KindRemove
		}
	case *apievents.TaskCreate:
		// No change of state: the process exists, not started yet.
		ev.ContainerID = e.GetContainerID()
		ev.Fields = map[string]string{"pid": strconv.FormatUint(uint64(e.GetPid()), 10)}
	case *apievents.TaskStart:
		ev.ContainerID = e.GetContainerID()
		ev.Kind = cosmicdust.KindStart
		ev.Fields = map[string]string{"pid": strconv.FormatUint(uint64(e.GetPid()), 10)}
	case *apievents.TaskExit:
		// An exec process exits on the same topic, with its own ID. Taking
		// that for the container's exit marks a running container exited
		// at every health check.
		if id := e.GetID(); id != "" && id != e.GetContainerID() {
			return ev, false, nil
		}
		ev.ContainerID = e.GetContainerID()
		ev.Kind = cosmicdust.KindExit
		code := int(e.GetExitStatus())
		ev.ExitCode = &code
		// When the process ended, by the shim's clock, rather than when
		// the event was published.
		if t := e.GetExitedAt(); t != nil && !t.AsTime().IsZero() {
			ev.Time = t.AsTime()
		}
		ev.Fields = map[string]string{"pid": strconv.FormatUint(uint64(e.GetPid()), 10)}
	case *apievents.TaskOOM:
		ev.ContainerID = e.GetContainerID()
		ev.Kind = cosmicdust.KindOOM
	case *apievents.TaskPaused:
		ev.ContainerID = e.GetContainerID()
	case *apievents.TaskResumed:
		ev.ContainerID = e.GetContainerID()
	default:
		return ev, false, nil
	}
	return ev, ev.ContainerID != "", nil
}

// List reports the containers of the followed namespaces: two calls per
// namespace, one for the containers and one for all their tasks, rather than
// three per container.
func (s *Source) List(ctx context.Context) (cosmicdust.Listing, error) {
	l := cosmicdust.Listing{At: time.Now()}
	if s.opts.DockerNamespace != "" {
		l.RunningOnly = map[string]bool{s.opts.DockerNamespace: true}
	}
	for _, ns := range s.opts.Namespaces {
		nctx := namespaces.WithNamespace(ctx, ns)
		cs, err := s.cli.ContainerService().List(nctx)
		if err != nil {
			return cosmicdust.Listing{}, fmt.Errorf("list containers (namespace %s): %w", ns, err)
		}
		tasks, err := s.cli.TaskService().List(nctx, &tasksapi.ListTasksRequest{})
		if err != nil {
			return cosmicdust.Listing{}, fmt.Errorf("list tasks (namespace %s): %w", ns, err)
		}
		byID := tasksByContainer(tasks.GetTasks())
		for _, c := range cs {
			l.Containers = append(l.Containers, s.state(ns, c, byID[c.ID]))
		}
	}
	return l, nil
}

// tasksByContainer keys a namespace's tasks by their container. containerd's
// task list names a task's init process by ID, the container's ID, and leaves
// ContainerID empty: keyed by ContainerID, no container had a task, and every
// one a listing showed was only created - a running nerdctl or kubelet
// container was not seen running after a restart of the agent, nor an exited
// one exited.
func tasksByContainer(tasks []*tasktypes.Process) map[string]*tasktypes.Process {
	byID := make(map[string]*tasktypes.Process, len(tasks))
	for _, p := range tasks {
		id := p.GetContainerID()
		if id == "" {
			id = p.GetID()
		}
		byID[id] = p
	}
	return byID
}

// Describe reports one container from whichever followed namespace has it.
func (s *Source) Describe(ctx context.Context, id string) (cosmicdust.ContainerState, error) {
	for _, ns := range s.opts.Namespaces {
		nctx := namespaces.WithNamespace(ctx, ns)
		c, err := s.cli.ContainerService().Get(nctx, id)
		if err != nil {
			continue
		}
		var p *tasktypes.Process
		if res, err := s.cli.TaskService().Get(nctx, &tasksapi.GetRequest{ContainerID: id}); err == nil {
			p = res.GetProcess()
		}
		return s.state(ns, c, p), nil
	}
	return cosmicdust.ContainerState{}, fmt.Errorf("container %s not found in %v", id, s.opts.Namespaces)
}

// state maps a container and its task, if it has one.
func (s *Source) state(ns string, c containers.Container, p *tasktypes.Process) cosmicdust.ContainerState {
	st := cosmicdust.ContainerState{
		ContainerRef: cosmicdust.ContainerRef{
			ID:        c.ID,
			Name:      nameOf(c.Labels),
			Image:     c.Image,
			App:       cosmicdust.AppOf(c.Labels, c.Image, s.opts.AppLabels),
			Runtime:   cosmicdust.RuntimeContainerd,
			Namespace: ns,
		},
		State: cosmicdust.StateCreated,
	}
	if dir := c.Labels["nerdctl/state-dir"]; dir != "" {
		// nerdctl keeps a json-file log next to its state, named like
		// Docker's.
		st.LogPath = dir + "/" + c.ID + "-json.log"
		st.LogFormat = "json-file"
	}
	if ns == s.opts.DockerNamespace {
		// The object is made for this incarnation, a few milliseconds
		// before its task starts.
		st.StartedAt = c.CreatedAt
	}
	if p == nil {
		return st
	}
	switch p.GetStatus() {
	case tasktypes.Status_RUNNING, tasktypes.Status_PAUSED, tasktypes.Status_PAUSING:
		st.State = cosmicdust.StateRunning
	case tasktypes.Status_STOPPED:
		st.State = cosmicdust.StateExited
		code := int(p.GetExitStatus())
		st.ExitCode = &code
		if t := p.GetExitedAt(); t != nil {
			st.FinishedAt = t.AsTime()
		}
	}
	return st
}

// nameOf finds a human name where a client left one: nerdctl, or kubelet.
func nameOf(labels map[string]string) string {
	for _, k := range []string{"nerdctl/name", "io.kubernetes.container.name"} {
		if v := labels[k]; v != "" {
			return v
		}
	}
	return ""
}
