package docker

import (
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/events"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

func source(t *testing.T) *Source {
	t.Helper()
	// No daemon is touched until the source is used.
	s, err := New(Options{Host: "unix:///nonexistent/docker.sock", AppLabels: []string{"c12s.app"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestEvent(t *testing.T) {
	s := source(t)
	at := time.Date(2026, 10, 20, 12, 0, 1, 123456789, time.UTC)
	msg := func(a events.Action, attrs map[string]string) events.Message {
		return events.Message{Type: events.ContainerEventType, Action: a, TimeNano: at.UnixNano(), Time: at.Unix(),
			Actor: events.Actor{ID: "c1", Attributes: attrs}}
	}
	labels := map[string]string{"name": "web", "image": "nginx:1.27",
		"com.docker.compose.project": "shop", "com.docker.compose.service": "web"}

	ev := s.event(msg(events.ActionStart, labels))
	if ev.Kind != cosmicdust.KindStart || ev.ContainerID != "c1" || ev.Name != "web" || ev.Image != "nginx:1.27" ||
		ev.App != "shop.web" || ev.Namespace != "moby" || !ev.Time.Equal(at) {
		t.Fatalf("start: %+v", ev)
	}

	die := s.event(msg(events.ActionDie, map[string]string{"name": "web", "exitCode": "137"}))
	if die.Kind != cosmicdust.KindExit || die.ExitCode == nil || *die.ExitCode != 137 {
		t.Fatalf("die: %+v", die)
	}
	kill := s.event(msg(events.ActionKill, map[string]string{"signal": "9"}))
	if kill.Kind != cosmicdust.KindOther || kill.Fields["signal"] != "9" {
		t.Fatalf("kill: %+v", kill)
	}
	if app := s.event(msg(events.ActionCreate, map[string]string{"c12s.app": "billing", "image": "x"})).App; app != "billing" {
		t.Fatalf("app from the configured label: %q", app)
	}
	old := events.Message{Action: events.ActionStart, Time: at.Unix(), Actor: events.Actor{ID: "c1"}}
	if got := s.event(old).Time; !got.Equal(time.Unix(at.Unix(), 0)) {
		t.Fatalf("a message without nanoseconds: %v", got)
	}
}

func TestKindOf(t *testing.T) {
	cases := map[events.Action]cosmicdust.Kind{
		events.ActionCreate: cosmicdust.KindCreate, events.ActionStart: cosmicdust.KindStart, events.ActionDie: cosmicdust.KindExit,
		events.ActionOOM: cosmicdust.KindOOM, events.ActionRestart: cosmicdust.KindRestart, events.ActionDestroy: cosmicdust.KindRemove,
		events.ActionKill: cosmicdust.KindOther, events.ActionStop: cosmicdust.KindOther, events.ActionPause: cosmicdust.KindOther,
		"health_status: unhealthy": cosmicdust.KindOther,
	}
	for a, want := range cases {
		if got := kindOf(a); got != want {
			t.Errorf("kindOf(%q) = %s, want %s", a, got, want)
		}
	}
}

func TestFromInspect(t *testing.T) {
	s := source(t)
	started := "2026-10-20T12:00:00.123456789Z"
	finished := "2026-10-20T12:05:00.5Z"
	c := container.InspectResponse{
		ID: "c1", Name: "/web", LogPath: "/var/lib/docker/containers/c1/c1-json.log", RestartCount: 2,
		State:      &container.State{Status: container.StateExited, ExitCode: 137, OOMKilled: true, StartedAt: started, FinishedAt: finished},
		Config:     &container.Config{Image: "nginx:1.27", Tty: true, Labels: map[string]string{"c12s.app": "billing"}},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "json-file"}},
	}
	st := s.fromInspect(c)
	if st.ID != "c1" || st.Name != "web" || st.Image != "nginx:1.27" || st.App != "billing" || !st.TTY ||
		st.LogFormat != "json-file" || st.LogPath == "" || st.Restarts != 2 || st.Runtime != cosmicdust.RuntimeDocker {
		t.Fatalf("ref: %+v", st.ContainerRef)
	}
	if st.State != cosmicdust.StateExited || st.ExitCode == nil || *st.ExitCode != 137 || !st.OOMKilled ||
		st.StartedAt.Format(time.RFC3339Nano) != started || st.FinishedAt.Format(time.RFC3339Nano) != finished {
		t.Fatalf("state: %+v", st)
	}

	never := container.InspectResponse{ID: "c2", Name: "/new",
		State: &container.State{Status: container.StateCreated, StartedAt: "0001-01-01T00:00:00Z", FinishedAt: "0001-01-01T00:00:00Z"}}
	st = s.fromInspect(never)
	if st.State != cosmicdust.StateCreated || !st.StartedAt.IsZero() || !st.FinishedAt.IsZero() || st.ExitCode != nil {
		t.Fatalf("created, never started: %+v", st)
	}

	running := container.InspectResponse{ID: "c3", State: &container.State{Status: container.StateRunning, Running: true, StartedAt: started, FinishedAt: finished}}
	if st := s.fromInspect(running); st.ExitCode != nil || !st.FinishedAt.IsZero() {
		t.Fatalf("a running container's previous exit leaked into its state: %+v", st)
	}
}

func TestLogKeep(t *testing.T) {
	cases := []struct {
		lc   container.LogConfig
		want int64
	}{
		{container.LogConfig{Type: "json-file"}, 0}, // not rotated: Docker's default
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "10m"}}, 10 << 20},
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "10m", "max-file": "3"}}, 30 << 20},
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "512KiB", "max-file": "2"}}, 1 << 20},
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "1g", "max-file": "5", "compress": "true"}}, 1 << 30},
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "-1"}}, 0},
		{container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "lots"}}, 0},
		{container.LogConfig{Type: "local", Config: map[string]string{"max-size": "20m"}}, 0},
	}
	for _, tc := range cases {
		if got := logKeep(tc.lc); got != tc.want {
			t.Errorf("logKeep(%+v) = %d, want %d", tc.lc, got, tc.want)
		}
	}
}

func TestStateOf(t *testing.T) {
	cases := map[container.ContainerState]cosmicdust.State{
		container.StateCreated: cosmicdust.StateCreated, container.StateRunning: cosmicdust.StateRunning,
		container.StatePaused: cosmicdust.StateRunning, container.StateRestarting: cosmicdust.StateRestarting,
		container.StateExited: cosmicdust.StateExited, container.StateDead: cosmicdust.StateExited,
		container.StateRemoving: cosmicdust.StateRemoved, "weird": cosmicdust.StateUnknown,
	}
	for in, want := range cases {
		if got := stateOf(in); got != want {
			t.Errorf("stateOf(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestSinceArg(t *testing.T) {
	if got := sinceArg(time.Unix(1700000000, 5)); got != "1700000000.000000005" {
		t.Fatal(got)
	}
}

func TestRecent(t *testing.T) {
	r := newRecent(3)
	for i := range 3 {
		if !r.add(fmt.Sprint(i)) {
			t.Fatalf("%d not new", i)
		}
	}
	if r.add("1") {
		t.Fatal("a repeated key counted as new")
	}
	r.add("3") // evicts "0"
	if !r.add("0") {
		t.Fatal("an evicted key still remembered")
	}
}
