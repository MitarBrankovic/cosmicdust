package containerd

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apievents "github.com/containerd/containerd/api/events"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/events"
	typeurl "github.com/containerd/typeurl/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func source(t *testing.T, opts Options) *Source {
	t.Helper()
	// No socket is touched until the source is used.
	opts.Address = "/nonexistent/containerd.sock"
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func envelope(t *testing.T, ns, topic string, payload proto.Message, at time.Time) *events.Envelope {
	t.Helper()
	any, err := typeurl.MarshalAny(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &events.Envelope{Timestamp: at, Namespace: ns, Topic: topic, Event: any}
}

func TestEvent(t *testing.T) {
	published := time.Date(2026, 10, 20, 12, 0, 1, 0, time.UTC)
	exited := published.Add(-80 * time.Millisecond)
	s := source(t, Options{})
	cases := []struct {
		name     string
		env      *events.Envelope
		keep     bool
		kind     cosmicdust.Kind
		id       string
		exitCode *int
		at       time.Time
	}{
		{"a task's create carries no state", envelope(t, "moby", "/tasks/create", &apievents.TaskCreate{ContainerID: "c1", Pid: 7}, published),
			true, cosmicdust.KindOther, "c1", nil, published},
		{"start", envelope(t, "moby", "/tasks/start", &apievents.TaskStart{ContainerID: "c1", Pid: 7}, published),
			true, cosmicdust.KindStart, "c1", nil, published},
		{
			// The time is the shim's, when the process ended, not the
			// publication's.
			"exit of the container's process",
			envelope(t, "moby", "/tasks/exit", &apievents.TaskExit{ContainerID: "c1", ID: "c1", Pid: 7, ExitStatus: 137, ExitedAt: timestamppb.New(exited)}, published),
			true, cosmicdust.KindExit, "c1", ptr(137), exited,
		},
		{
			// A health check's exec ends on the same topic.
			"exit of an exec process is dropped",
			envelope(t, "moby", "/tasks/exit", &apievents.TaskExit{ContainerID: "c1", ID: "health-1a2b", Pid: 99, ExitStatus: 0, ExitedAt: timestamppb.New(exited)}, published),
			false, cosmicdust.KindOther, "", nil, time.Time{},
		},
		{"oom", envelope(t, "moby", "/tasks/oom", &apievents.TaskOOM{ContainerID: "c1"}, published),
			true, cosmicdust.KindOOM, "c1", nil, published},
		{
			// In Docker's namespace the object lives for one incarnation.
			"create in Docker's namespace is not the container's",
			envelope(t, "moby", "/containers/create", &apievents.ContainerCreate{ID: "c1", Image: "docker.io/library/alpine:latest"}, published),
			true, cosmicdust.KindOther, "c1", nil, published,
		},
		{"delete in Docker's namespace is not a removal",
			envelope(t, "moby", "/containers/delete", &apievents.ContainerDelete{ID: "c1"}, published),
			true, cosmicdust.KindOther, "c1", nil, published},
		{"create elsewhere is a create",
			envelope(t, "default", "/containers/create", &apievents.ContainerCreate{ID: "n1", Image: "docker.io/library/alpine:latest"}, published),
			true, cosmicdust.KindCreate, "n1", nil, published},
		{"delete elsewhere is a removal",
			envelope(t, "default", "/containers/delete", &apievents.ContainerDelete{ID: "n1"}, published),
			true, cosmicdust.KindRemove, "n1", nil, published},
		{"a namespace not followed is dropped",
			envelope(t, "k8s.io", "/tasks/start", &apievents.TaskStart{ContainerID: "p1"}, published),
			false, cosmicdust.KindOther, "", nil, time.Time{}},
		{"paused carries no state", envelope(t, "moby", "/tasks/paused", &apievents.TaskPaused{ContainerID: "c1"}, published),
			true, cosmicdust.KindOther, "c1", nil, published},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, keep, err := s.event(tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if keep != tc.keep {
				t.Fatalf("keep = %v, want %v", keep, tc.keep)
			}
			if !keep {
				return
			}
			if ev.Kind != tc.kind || ev.ContainerID != tc.id || !ev.Time.Equal(tc.at) || ev.Runtime != cosmicdust.RuntimeContainerd {
				t.Errorf("got %s %q at %v, want %s %q at %v", ev.Kind, ev.ContainerID, ev.Time, tc.kind, tc.id, tc.at)
			}
			if (ev.ExitCode == nil) != (tc.exitCode == nil) || (ev.ExitCode != nil && *ev.ExitCode != *tc.exitCode) {
				t.Errorf("exit code %v, want %v", ev.ExitCode, tc.exitCode)
			}
		})
	}
}

func TestEventWithoutDocker(t *testing.T) {
	// When Docker is not followed, containerd is the only witness and its
	// objects are taken at face value, Docker's namespace included.
	s := source(t, Options{NoDocker: true})
	ev, keep, err := s.event(envelope(t, "moby", "/containers/delete", &apievents.ContainerDelete{ID: "c1"}, time.Now()))
	if err != nil || !keep || ev.Kind != cosmicdust.KindRemove {
		t.Fatalf("got %s keep %v err %v, want a removal", ev.Kind, keep, err)
	}
}

func TestState(t *testing.T) {
	created := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	exited := created.Add(time.Minute)
	s := source(t, Options{AppLabels: []string{"app"}})
	cases := []struct {
		name  string
		ns    string
		c     containers.Container
		p     *tasktypes.Process
		state cosmicdust.State
		check func(t *testing.T, st cosmicdust.ContainerState)
	}{
		{"running", "moby", containers.Container{ID: "c1", Image: "nginx:1.27", CreatedAt: created},
			&tasktypes.Process{ContainerID: "c1", Status: tasktypes.Status_RUNNING}, cosmicdust.StateRunning,
			func(t *testing.T, st cosmicdust.ContainerState) {
				if !st.StartedAt.Equal(created) {
					t.Errorf("in Docker's namespace the object's creation dates the incarnation; got %v", st.StartedAt)
				}
				if st.App != "nginx" {
					t.Errorf("app %q", st.App)
				}
			}},
		{"paused counts as running", "moby", containers.Container{ID: "c1"},
			&tasktypes.Process{Status: tasktypes.Status_PAUSED}, cosmicdust.StateRunning, nil},
		{"stopped", "default", containers.Container{ID: "n1", CreatedAt: created, Labels: map[string]string{"app": "billing", "nerdctl/name": "bill", "nerdctl/state-dir": "/var/lib/nerdctl/1935db59/containers/default/n1"}},
			&tasktypes.Process{Status: tasktypes.Status_STOPPED, ExitStatus: 3, ExitedAt: timestamppb.New(exited)}, cosmicdust.StateExited,
			func(t *testing.T, st cosmicdust.ContainerState) {
				if st.ExitCode == nil || *st.ExitCode != 3 || !st.FinishedAt.Equal(exited) {
					t.Errorf("exit %v at %v", st.ExitCode, st.FinishedAt)
				}
				if !st.StartedAt.IsZero() {
					t.Errorf("outside Docker's namespace the creation is not a start; got %v", st.StartedAt)
				}
				if st.Name != "bill" || st.App != "billing" || st.LogPath != "/var/lib/nerdctl/1935db59/containers/default/n1/n1-json.log" || st.LogFormat != "json-file" {
					t.Errorf("name %q app %q log %q %q", st.Name, st.App, st.LogPath, st.LogFormat)
				}
			}},
		{"no task", "default", containers.Container{ID: "n2"}, nil, cosmicdust.StateCreated, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := s.state(tc.ns, tc.c, tc.p)
			if st.State != tc.state || st.Runtime != cosmicdust.RuntimeContainerd || st.Namespace != tc.ns {
				t.Fatalf("state %s runtime %s namespace %s", st.State, st.Runtime, st.Namespace)
			}
			if tc.check != nil {
				tc.check(t, st)
			}
		})
	}
}

// containerd's task list has the container's ID in ID and ContainerID empty
// (seen on containerd 2.3); a process that does carry ContainerID is keyed by
// it.
func TestTasksByContainer(t *testing.T) {
	byID := tasksByContainer([]*tasktypes.Process{
		{ID: "n1", Status: tasktypes.Status_STOPPED, ExitStatus: 2},
		{ID: "n2", Status: tasktypes.Status_RUNNING},
		{ID: "exec-1", ContainerID: "n3", Status: tasktypes.Status_RUNNING},
	})
	if p := byID["n1"]; p == nil || p.GetExitStatus() != 2 {
		t.Fatalf("n1: %v", p)
	}
	if p := byID["n2"]; p == nil || p.GetStatus() != tasktypes.Status_RUNNING {
		t.Fatalf("n2: %v", p)
	}
	if p := byID["n3"]; p == nil || p.GetID() != "exec-1" {
		t.Fatalf("n3: %v", p)
	}
	if _, ok := byID[""]; ok {
		t.Fatal("a task keyed by an empty container ID")
	}
}

func TestIsShutdown(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	bg := context.Background()
	cases := []struct {
		ctx  context.Context
		err  error
		want bool
	}{
		{cancelled, errors.New("anything"), true},
		{bg, fmt.Errorf("subscribe: %w", context.Canceled), true},
		{bg, fmt.Errorf("subscribe: %w", context.DeadlineExceeded), true},
		// gRPC can give up on its own clock before the context says done.
		{bg, status.Error(codes.DeadlineExceeded, "stream terminated"), true},
		{bg, status.Error(codes.Canceled, "context canceled"), true},
		{bg, status.Error(codes.Unavailable, "error reading from server: EOF"), false},
		{bg, errors.New("EOF"), false},
	}
	for i, c := range cases {
		if got := isShutdown(c.ctx, c.err); got != c.want {
			t.Errorf("case %d (%v): isShutdown = %v", i, c.err, got)
		}
	}
}

func ptr(i int) *int { return &i }
