// Package docker is the event source for dockerd: container events from its
// /events stream, and listings and descriptions through list and inspect.
//
// Docker knows what containerd forgets: a stopped container, its name, labels
// and log path. It also sees an exit ~350-420 ms after containerd does,
// which is why the tracker merges the two rather than picking one.
//
// Access: the Docker socket is root:docker 0660, so the docker group is enough.
package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/events"
	"github.com/moby/moby/client"

	"github.com/c12s/cosmicdust/internal/retry"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure the source.
type Options struct {
	// Host is the daemon's address, e.g. unix:///var/run/docker.sock; empty
	// means DOCKER_HOST or the default socket.
	Host string

	// Namespace is the containerd namespace Docker runs its containers in,
	// recorded with every event so that the tracker sees one container
	// whichever runtime reports it. Default "moby".
	Namespace string

	// AppLabels are the label keys that name a container's application
	// (cosmicdust.AppOf).
	AppLabels []string

	// InspectWorkers bounds concurrent inspects in List. Default 4.
	InspectWorkers int

	// Backoff paces reconnects. Zero: 1s doubling to 30s.
	Backoff retry.Backoff

	Logger *slog.Logger
}

// Source reads one dockerd.
type Source struct {
	cli  *client.Client
	opts Options
	log  *slog.Logger
}

// New returns a source; it connects only when used.
func New(opts Options) (*Source, error) {
	copts := []client.Opt{client.FromEnv}
	if opts.Host != "" {
		copts = append(copts, client.WithHost(opts.Host))
	}
	cli, err := client.New(copts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = "moby"
	}
	if opts.InspectWorkers <= 0 {
		opts.InspectWorkers = 4
	}
	if opts.Backoff.Min == 0 {
		opts.Backoff = retry.Backoff{Min: time.Second, Max: 30 * time.Second}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Source{cli: cli, opts: opts, log: opts.Logger.With("runtime", cosmicdust.RuntimeDocker)}, nil
}

// Close releases the client.
func (s *Source) Close() error { return s.cli.Close() }

func (s *Source) Runtime() string { return cosmicdust.RuntimeDocker }

// watchedActions are the container events asked for. Everything else (exec
// events from every health check, attach, resize, top, ...) would only be
// filtered out here, after dockerd had spent the work of sending it.
var watchedActions = []string{
	"create", "start", "restart", "die", "oom", "kill", "stop",
	"pause", "unpause", "destroy", "rename", "health_status",
}

// replayWindow is how far back a reconnect asks dockerd to replay. Two
// events can be published in the opposite order of their timestamps, so the
// newest one delivered is not a safe point to resume from; the ring of
// recent events drops what is delivered twice.
const replayWindow = 2 * time.Second

func (s *Source) Watch(ctx context.Context, out chan<- cosmicdust.Event) error {
	b := s.opts.Backoff
	seen := newRecent(1024)
	var newest int64 // TimeNano of the newest event delivered
	reason, lastErr := "start", error(nil)
	var down time.Time
	var said string // the last error logged at warning level
	for {
		if _, err := s.cli.Ping(ctx, client.PingOptions{}); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			pause := b.Next(time.Now())
			level := slog.LevelWarn
			if err.Error() == said {
				level = slog.LevelDebug
			}
			said = err.Error()
			s.log.Log(ctx, level, "docker unreachable; retrying", "err", err, "in", pause)
			if down.IsZero() {
				down, lastErr = time.Now(), err
			}
			if !retry.Sleep(ctx, pause) {
				return nil
			}
			continue
		}

		opts := client.EventsListOptions{
			Filters: make(client.Filters).Add("type", "container").Add("event", watchedActions...),
		}
		if newest != 0 {
			opts.Since = sinceArg(time.Unix(0, newest).Add(-replayWindow))
		}
		sctx, cancel := context.WithCancel(ctx)
		res := s.cli.Events(sctx, opts)
		b.Connected(time.Now())
		said = ""

		mark := cosmicdust.Event{
			Time: time.Now(), Runtime: cosmicdust.RuntimeDocker, Namespace: s.opts.Namespace,
			Action: "resync", Kind: cosmicdust.KindResync, Fields: map[string]string{"reason": reason},
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
		s.log.Info("docker events subscribed", "reason", reason)

		err := s.forward(ctx, res, out, seen, &newest)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		reason, lastErr, down = "reconnect", err, time.Now()
		pause := b.Next(time.Now())
		s.log.Warn("docker events stream broke; reconnecting", "err", err, "in", pause)
		if !retry.Sleep(ctx, pause) {
			return nil
		}
	}
}

// forward passes events on until the stream breaks.
func (s *Source) forward(ctx context.Context, res client.EventsResult, out chan<- cosmicdust.Event, seen *recent, newest *int64) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-res.Messages:
			if !ok {
				return errors.New("event stream closed")
			}
			t := eventTime(m).UnixNano()
			if !seen.add(fmt.Sprintf("%d/%s/%s", t, m.Actor.ID, m.Action)) {
				continue // replayed after a reconnect
			}
			if t > *newest {
				*newest = t
			}
			if !send(ctx, out, s.event(m)) {
				return nil
			}
		case err, ok := <-res.Err:
			if !ok || err == nil {
				return errors.New("event stream closed")
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

// sinceArg formats a time the way the events API takes it.
func sinceArg(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

func eventTime(m events.Message) time.Time {
	if m.TimeNano != 0 {
		return time.Unix(0, m.TimeNano)
	}
	return time.Unix(m.Time, 0)
}

// event normalises one message. The actor's attributes hold the container's
// labels next to name, image and exitCode, so the application is known from
// the first event, without an inspect.
func (s *Source) event(m events.Message) cosmicdust.Event {
	attrs := m.Actor.Attributes
	ev := cosmicdust.Event{
		Time:        eventTime(m),
		Runtime:     cosmicdust.RuntimeDocker,
		Namespace:   s.opts.Namespace,
		Action:      string(m.Action),
		Kind:        kindOf(m.Action),
		ContainerID: m.Actor.ID,
		Name:        attrs["name"],
		Image:       attrs["image"],
		App:         cosmicdust.AppOf(attrs, attrs["image"], s.opts.AppLabels),
	}
	if ev.Kind == cosmicdust.KindExit {
		if code, err := strconv.Atoi(attrs["exitCode"]); err == nil {
			ev.ExitCode = &code
		}
	}
	for _, k := range []string{"signal", "oldName"} {
		if v, ok := attrs[k]; ok {
			if ev.Fields == nil {
				ev.Fields = map[string]string{}
			}
			ev.Fields[k] = v
		}
	}
	return ev
}

func kindOf(a events.Action) cosmicdust.Kind {
	switch a {
	case events.ActionCreate:
		return cosmicdust.KindCreate
	case events.ActionStart:
		return cosmicdust.KindStart
	case events.ActionDie:
		return cosmicdust.KindExit
	case events.ActionOOM:
		return cosmicdust.KindOOM
	case events.ActionRestart:
		// Sent after the new start (daemon containerRestart): the start
		// already moved the state.
		return cosmicdust.KindRestart
	case events.ActionDestroy:
		return cosmicdust.KindRemove
	}
	return cosmicdust.KindOther
}

// List reports every container dockerd knows, stopped ones included.
//
// A listing has neither times nor log paths, so every container is inspected
// as well, a few at a time. On Docker 29 with the containerd image store the
// listing costs dockerd and containerd ~10-12 ms of CPU each per container in
// the answer and an inspect ~2 ms: the inspects add a
// fifth. The tracker lists Docker only after a connect, not periodically.
func (s *Source) List(ctx context.Context) (cosmicdust.Listing, error) {
	at := time.Now()
	res, err := s.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return cosmicdust.Listing{}, err
	}
	states := make([]cosmicdust.ContainerState, len(res.Items))
	work := make(chan int)
	var wg sync.WaitGroup
	for range min(s.opts.InspectWorkers, len(res.Items)) {
		wg.Go(func() {
			for i := range work {
				item := res.Items[i]
				if st, err := s.Describe(ctx, item.ID); err == nil {
					states[i] = st
				} else {
					// Removed in the meantime, most likely: the summary
					// is still the truth as of the listing.
					states[i] = s.fromSummary(item)
				}
			}
		})
	}
	for i := range res.Items {
		work <- i
	}
	close(work)
	wg.Wait()
	return cosmicdust.Listing{At: at, Containers: states}, ctx.Err()
}

// Describe inspects one container.
func (s *Source) Describe(ctx context.Context, id string) (cosmicdust.ContainerState, error) {
	res, err := s.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return cosmicdust.ContainerState{}, err
	}
	return s.fromInspect(res.Container), nil
}

// logKeep is how many bytes of a json-file log Docker keeps: max-size times
// max-file, the daemon's defaults included (inspect has them merged). With
// compress the rotated files are gzipped, which the tailer does not read, so
// the current file is all that counts. 0: not rotated, Docker's default.
func logKeep(lc container.LogConfig) int64 {
	if lc.Type != "json-file" {
		return 0
	}
	size, ok := parseSize(lc.Config["max-size"])
	if !ok || size <= 0 {
		return 0
	}
	files := int64(1)
	if n, err := strconv.ParseInt(lc.Config["max-file"], 10, 64); err == nil && n > 0 {
		files = n
	}
	if lc.Config["compress"] == "true" {
		files = 1
	}
	return size * files
}

// parseSize reads a size the way Docker reads max-size: a number and an
// optional k, m, g, t or p, in powers of 1024, with an optional "b" or "ib".
func parseSize(s string) (int64, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(strings.TrimSuffix(s, "b"), "i")
	mult := int64(1)
	if n := len(s); n > 0 {
		if i := strings.IndexByte("kmgtp", s[n-1]); i >= 0 {
			mult = 1 << (10 * (i + 1))
			s = strings.TrimSpace(s[:n-1])
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	return int64(v * float64(mult)), true
}

func (s *Source) fromInspect(c container.InspectResponse) cosmicdust.ContainerState {
	st := cosmicdust.ContainerState{ContainerRef: cosmicdust.ContainerRef{
		ID:        c.ID,
		Name:      strings.TrimPrefix(c.Name, "/"),
		Runtime:   cosmicdust.RuntimeDocker,
		Namespace: s.opts.Namespace,
		LogPath:   c.LogPath,
	}}
	var labels map[string]string
	if c.Config != nil {
		st.Image = c.Config.Image
		st.TTY = c.Config.Tty
		labels = c.Config.Labels
	}
	if c.HostConfig != nil {
		st.LogFormat = c.HostConfig.LogConfig.Type
		st.LogKeep = logKeep(c.HostConfig.LogConfig)
	}
	st.App = cosmicdust.AppOf(labels, st.Image, s.opts.AppLabels)
	st.Restarts = c.RestartCount
	st.State = cosmicdust.StateUnknown
	if c.State != nil {
		st.State = stateOf(c.State.Status)
		st.StartedAt = parseTime(c.State.StartedAt)
		st.OOMKilled = c.State.OOMKilled
		if !c.State.Running {
			st.FinishedAt = parseTime(c.State.FinishedAt)
			if !st.FinishedAt.IsZero() {
				code := c.State.ExitCode
				st.ExitCode = &code
			}
		}
	}
	return st
}

func (s *Source) fromSummary(c container.Summary) cosmicdust.ContainerState {
	name := ""
	if len(c.Names) > 0 {
		name = strings.TrimPrefix(c.Names[0], "/")
	}
	return cosmicdust.ContainerState{
		ContainerRef: cosmicdust.ContainerRef{
			ID: c.ID, Name: name, Image: c.Image,
			App:     cosmicdust.AppOf(c.Labels, c.Image, s.opts.AppLabels),
			Runtime: cosmicdust.RuntimeDocker, Namespace: s.opts.Namespace,
		},
		State: stateOf(c.State),
	}
}

func stateOf(s container.ContainerState) cosmicdust.State {
	switch s {
	case container.StateCreated:
		return cosmicdust.StateCreated
	case container.StateRunning, container.StatePaused:
		return cosmicdust.StateRunning // a paused container still holds its log open
	case container.StateRestarting:
		return cosmicdust.StateRestarting
	case container.StateExited, container.StateDead:
		return cosmicdust.StateExited
	case container.StateRemoving:
		return cosmicdust.StateRemoved
	}
	return cosmicdust.StateUnknown
}

// parseTime reads the daemon's RFC 3339 times; "never" is the zero time
// 0001-01-01T00:00:00Z, which parses to the zero time.Time as well.
func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// recent remembers the last n keys added, to drop events a reconnect replays.
type recent struct {
	keys  map[string]struct{}
	order []string
	next  int
}

func newRecent(n int) *recent {
	return &recent{keys: make(map[string]struct{}, n), order: make([]string, n)}
}

// add reports whether key is new, and remembers it.
func (r *recent) add(key string) bool {
	if _, ok := r.keys[key]; ok {
		return false
	}
	if old := r.order[r.next]; old != "" {
		delete(r.keys, old)
	}
	r.order[r.next] = key
	r.next = (r.next + 1) % len(r.order)
	r.keys[key] = struct{}{}
	return true
}
