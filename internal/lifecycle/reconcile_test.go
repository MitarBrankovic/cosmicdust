package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// render prints inferred events as "kind@second.ms" for comparison.
func render(evs []cosmicdust.Event) string {
	var b []string
	for _, e := range evs {
		s := fmt.Sprintf("%s:%s@%s", e.ContainerID, e.Kind, e.Time.Format("05.000"))
		if e.ExitCode != nil {
			s += fmt.Sprintf("(%d)", *e.ExitCode)
		}
		if !e.Inferred {
			s += "!not-inferred"
		}
		b = append(b, s)
	}
	return strings.Join(b, " ")
}

func cs(id string, st cosmicdust.State, started, finished time.Time, exit *int) cosmicdust.ContainerState {
	return cosmicdust.ContainerState{
		ContainerRef: cosmicdust.ContainerRef{ID: id, Namespace: "moby"},
		State:        st, StartedAt: started, FinishedAt: finished, ExitCode: exit,
	}
}

func vw(st cosmicdust.State, epoch int, t time.Time) viewRef {
	return viewRef{v: &view{State: st, Epoch: epoch, At: t}, namespace: "moby"}
}

func TestInferred(t *testing.T) {
	listedAt := at(30, 0)
	cases := []struct {
		name    string
		views   map[string]viewRef
		listed  []cosmicdust.ContainerState
		running bool // the listing has running containers only
		want    string
	}{
		{
			name:   "first sight of a running container, dated by the runtime",
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, at(10, 0), time.Time{}, nil)},
			want:   "a:start@10.000",
		},
		{
			name:   "first sight of a stopped container: its last incarnation, start and exit",
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateExited, at(10, 0), at(20, 0), code(2))},
			want:   "a:start@10.000 a:exit@20.000(2)",
		},
		{
			name:   "no times in the listing: stamped at the listing",
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, time.Time{}, time.Time{}, nil)},
			want:   "a:start@30.000",
		},
		{
			name:   "stopped while away",
			views:  map[string]viewRef{"a": vw(cosmicdust.StateRunning, 1, at(10, 0))},
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateExited, at(10, 0), at(20, 0), code(137))},
			want:   "a:exit@20.000(137)",
		},
		{
			name:   "restarted while away: the missed exit lands just before the start",
			views:  map[string]viewRef{"a": vw(cosmicdust.StateRunning, 1, at(10, 0))},
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, at(25, 0), time.Time{}, nil)},
			want:   "a:exit@24.999 a:start@25.000",
		},
		{
			name:   "a whole incarnation while away",
			views:  map[string]viewRef{"a": vw(cosmicdust.StateExited, 1, at(10, 0))},
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateExited, at(15, 0), at(20, 0), code(0))},
			want:   "a:start@15.000 a:exit@20.000(0)",
		},
		{
			name:   "started again while away",
			views:  map[string]viewRef{"a": vw(cosmicdust.StateExited, 1, at(10, 0))},
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, at(15, 0), time.Time{}, nil)},
			want:   "a:start@15.000",
		},
		{
			name:   "nothing changed",
			views:  map[string]viewRef{"a": vw(cosmicdust.StateRunning, 1, at(10, 0))},
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, at(9, 990), time.Time{}, nil)},
			want:   "",
		},
		{
			name:  "removed while away",
			views: map[string]viewRef{"a": vw(cosmicdust.StateExited, 1, at(10, 0)), "b": vw(cosmicdust.StateRemoved, 1, at(11, 0))},
			want:  "a:remove@30.000",
		},
		{
			// containerd in Docker's namespace: missing means stopped.
			name:    "running-only listing: a missing running container has exited",
			views:   map[string]viewRef{"a": vw(cosmicdust.StateRunning, 1, at(10, 0)), "b": vw(cosmicdust.StateExited, 1, at(11, 0))},
			running: true,
			want:    "a:exit@30.000",
		},
		{
			name:    "running-only listing: an object between incarnations says nothing",
			views:   map[string]viewRef{"a": vw(cosmicdust.StateExited, 1, at(10, 0))},
			listed:  []cosmicdust.ContainerState{cs("a", cosmicdust.StateCreated, at(29, 0), time.Time{}, nil)},
			running: true,
			want:    "",
		},
		{
			// A clock that runs ahead must not put an inferred event after
			// events the stream delivers during the listing.
			name:   "never stamped after the listing",
			listed: []cosmicdust.ContainerState{cs("a", cosmicdust.StateRunning, at(45, 0), time.Time{}, nil)},
			want:   "a:start@30.000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := cosmicdust.Listing{At: listedAt, Containers: tc.listed}
			if tc.running {
				l.RunningOnly = map[string]bool{"moby": true}
			}
			if got := render(inferred(containerd, tc.views, l)); got != tc.want {
				t.Errorf("inferred\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// The listing runs while the stream is open: what the stream delivered after
// the listing started must survive what the listing implies.
func TestInferredNeverOverridesTheStream(t *testing.T) {
	v := &view{State: cosmicdust.StateUnknown}
	v.apply(ev(at(31, 0), docker, cosmicdust.KindStart)) // arrived during the listing
	l := cosmicdust.Listing{At: at(30, 0)}               // started before it; the container is not in it
	for _, e := range inferred(docker, map[string]viewRef{"c1": {v: v, namespace: "moby"}}, l) {
		v.apply(e)
	}
	if v.State != cosmicdust.StateRunning || v.Epoch != 1 {
		t.Fatalf("view %s epoch %d after reconciliation, want running 1", v.State, v.Epoch)
	}
}
