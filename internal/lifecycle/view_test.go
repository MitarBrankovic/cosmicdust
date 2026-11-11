package lifecycle

import (
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

const (
	docker     = cosmicdust.RuntimeDocker
	containerd = cosmicdust.RuntimeContainerd
)

// at is second n of an imaginary minute, plus ms milliseconds, so a test can
// put events out of order by giving a smaller n.
func at(n, ms int) time.Time {
	return time.Date(2026, 10, 20, 12, 0, n, ms*int(time.Millisecond), time.UTC)
}

func ev(t time.Time, rt string, k cosmicdust.Kind, code ...int) cosmicdust.Event {
	e := cosmicdust.Event{Time: t, Runtime: rt, Namespace: "moby", Action: k.String(), Kind: k, ContainerID: "c1"}
	if len(code) > 0 {
		e.ExitCode = &code[0]
	}
	return e
}

func code(i int) *int { return &i }

// Single-source cases, on one view.
func TestViewTransitions(t *testing.T) {
	cases := []struct {
		name      string
		events    []cosmicdust.Event
		wantState cosmicdust.State
		wantEpoch int
		wantExit  *int
		wantOOM   bool
		wantLate  int
	}{
		{
			name: "run to completion",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindCreate), ev(at(2, 0), docker, cosmicdust.KindStart),
				ev(at(3, 0), docker, cosmicdust.KindExit, 0), ev(at(4, 0), docker, cosmicdust.KindRemove),
			},
			wantState: cosmicdust.StateRemoved, wantEpoch: 1, wantExit: code(0),
		},
		{
			name: "restart policy: every start after an exit is a new epoch",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindCreate), ev(at(2, 0), docker, cosmicdust.KindStart),
				ev(at(3, 0), docker, cosmicdust.KindExit, 1), ev(at(4, 0), docker, cosmicdust.KindStart),
				ev(at(5, 0), docker, cosmicdust.KindExit, 1), ev(at(6, 0), docker, cosmicdust.KindStart),
			},
			wantState: cosmicdust.StateRunning, wantEpoch: 3,
		},
		{
			name: "a repeated start does not invent an epoch",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(2, 0), docker, cosmicdust.KindStart),
			},
			wantState: cosmicdust.StateRunning, wantEpoch: 1,
		},
		{
			name: "OOM is remembered and the exit still lands",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(2, 0), docker, cosmicdust.KindOOM),
				ev(at(3, 0), docker, cosmicdust.KindExit, 137),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(137), wantOOM: true,
		},
		{
			// Without the stamp rule this invents a restart.
			name: "a late start does not move the state back to running",
			events: []cosmicdust.Event{
				ev(at(5, 0), containerd, cosmicdust.KindExit, 0),
				ev(at(4, 0), containerd, cosmicdust.KindStart),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(0), wantLate: 1,
		},
		{
			// The agent was down when it started: the incarnation still ran.
			name: "an exit without its start counts the incarnation",
			events: []cosmicdust.Event{
				ev(at(1, 0), containerd, cosmicdust.KindCreate), ev(at(2, 0), containerd, cosmicdust.KindExit, 2),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(2),
		},
		{
			name: "an OOM before an exit without its start is that incarnation's",
			events: []cosmicdust.Event{
				ev(at(1, 0), containerd, cosmicdust.KindOOM), ev(at(2, 0), containerd, cosmicdust.KindExit, 137),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(137), wantOOM: true,
		},
		{
			name: "a restart policy seen from its first exit",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindExit, 1), ev(at(2, 0), docker, cosmicdust.KindStart),
				ev(at(3, 0), docker, cosmicdust.KindExit, 1), ev(at(4, 0), docker, cosmicdust.KindStart),
				ev(at(5, 0), docker, cosmicdust.KindExit, 1),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 3, wantExit: code(1),
		},
		{
			name: "a second word on the same exit is not an incarnation",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(2, 0), docker, cosmicdust.KindExit, 1),
				ev(at(2, 0), docker, cosmicdust.KindExit, 1),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(1),
		},
		{
			name: "events sharing a stamp both apply",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindCreate), ev(at(1, 0), docker, cosmicdust.KindStart),
			},
			wantState: cosmicdust.StateRunning, wantEpoch: 1,
		},
		{
			// Docker sends "restart" after the new start: it must not leave
			// the container looking like it is between incarnations.
			name: "Docker's restart is informational",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(2, 0), docker, cosmicdust.KindExit, 137),
				ev(at(3, 0), docker, cosmicdust.KindStart), ev(at(3, 1), docker, cosmicdust.KindRestart),
			},
			wantState: cosmicdust.StateRunning, wantEpoch: 2,
		},
		{
			name: "a start clears the previous exit code",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(2, 0), docker, cosmicdust.KindExit, 3),
				ev(at(3, 0), docker, cosmicdust.KindStart),
			},
			wantState: cosmicdust.StateRunning, wantEpoch: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := &view{State: cosmicdust.StateUnknown}
			late := 0
			for _, e := range tc.events {
				if _, l := v.apply(e); l {
					late++
				}
			}
			if v.State != tc.wantState || v.Epoch != tc.wantEpoch || v.OOM != tc.wantOOM || late != tc.wantLate {
				t.Errorf("got %s epoch %d oom %v late %d; want %s epoch %d oom %v late %d",
					v.State, v.Epoch, v.OOM, late, tc.wantState, tc.wantEpoch, tc.wantOOM, tc.wantLate)
			}
			if !sameCode(v.ExitCode, tc.wantExit) {
				t.Errorf("exit code %v, want %v", deref(v.ExitCode), deref(tc.wantExit))
			}
		})
	}
}

func sameCode(a, b *int) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// applyAll feeds events to views by runtime and returns the merge.
func applyAll(events ...cosmicdust.Event) status {
	views := map[string]*view{}
	for _, e := range events {
		v := views[e.Runtime]
		if v == nil {
			v = &view{State: cosmicdust.StateUnknown}
			views[e.Runtime] = v
		}
		v.apply(e)
	}
	return merged(views)
}

func TestMergeTwoRuntimes(t *testing.T) {
	cases := []struct {
		name      string
		events    []cosmicdust.Event
		wantState cosmicdust.State
		wantEpoch int
		wantExit  *int
	}{
		{
			// Observed: Docker reported the start 7 ms after containerd
			// reported the exit. One state machine would count a restart.
			name: "Docker's start after containerd's exit is not a restart",
			events: []cosmicdust.Event{
				ev(at(10, 700), containerd, cosmicdust.KindStart),
				ev(at(10, 782), containerd, cosmicdust.KindExit, 3),
				ev(at(10, 789), docker, cosmicdust.KindStart),
				ev(at(11, 132), docker, cosmicdust.KindExit, 3),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(3),
		},
		{
			name: "containerd's exit counts before Docker's die arrives",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(1, 10), containerd, cosmicdust.KindStart),
				ev(at(5, 0), containerd, cosmicdust.KindExit, 137),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(137),
		},
		{
			// Observed: four incarnations; containerd's object for each one
			// is created and deleted (KindOther in Docker's namespace).
			name: "restart policy seen by both",
			events: func() []cosmicdust.Event {
				var out []cosmicdust.Event
				out = append(out, ev(at(0, 0), docker, cosmicdust.KindCreate))
				for i := range 4 {
					s := 1 + 2*i
					out = append(out,
						ev(at(s, 0), containerd, cosmicdust.KindStart), ev(at(s, 5), docker, cosmicdust.KindStart),
						ev(at(s+1, 0), containerd, cosmicdust.KindExit, 1), ev(at(s+1, 400), docker, cosmicdust.KindExit, 1))
				}
				return out
			}(),
			wantState: cosmicdust.StateExited, wantEpoch: 4, wantExit: code(1),
		},
		{
			// Docker knows the stopped container; containerd's view in
			// Docker's namespace never says removed.
			name: "removed only when Docker says so",
			events: []cosmicdust.Event{
				ev(at(1, 0), containerd, cosmicdust.KindStart), ev(at(1, 5), docker, cosmicdust.KindStart),
				ev(at(2, 0), containerd, cosmicdust.KindExit, 0), ev(at(2, 400), docker, cosmicdust.KindExit, 0),
				ev(at(3, 0), docker, cosmicdust.KindRemove),
			},
			wantState: cosmicdust.StateRemoved, wantEpoch: 1, wantExit: code(0),
		},
		{
			name: "the exit code comes from whichever runtime has it",
			events: []cosmicdust.Event{
				ev(at(1, 0), docker, cosmicdust.KindStart), ev(at(1, 5), containerd, cosmicdust.KindStart),
				ev(at(2, 0), containerd, cosmicdust.KindExit), // no code
				ev(at(2, 400), docker, cosmicdust.KindExit, 42),
			},
			wantState: cosmicdust.StateExited, wantEpoch: 1, wantExit: code(42),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := applyAll(tc.events...)
			if st.State != tc.wantState || st.Epoch != tc.wantEpoch || !sameCode(st.ExitCode, tc.wantExit) {
				t.Errorf("got %s epoch %d exit %v; want %s epoch %d exit %v",
					st.State, st.Epoch, deref(st.ExitCode), tc.wantState, tc.wantEpoch, deref(tc.wantExit))
			}
		})
	}
}

func TestMergeOfNothing(t *testing.T) {
	if st := merged(nil); st.State != cosmicdust.StateUnknown || st.Epoch != 0 {
		t.Fatalf("merged(nil) = %+v", st)
	}
}

func TestHistory(t *testing.T) {
	var h history
	h = h.add(transition{At: at(1, 0), State: cosmicdust.StateRunning, Epoch: 1}, 0)
	h = h.add(transition{At: at(5, 0), State: cosmicdust.StateRunning, Epoch: 2}, 0)
	h = h.add(transition{At: at(3, 0), State: cosmicdust.StateExited, Epoch: 1}, 0) // out of order
	cases := []struct {
		ts    time.Time
		state cosmicdust.State
		epoch int
	}{
		{at(0, 0), cosmicdust.StateUnknown, 0},
		{at(1, 0), cosmicdust.StateRunning, 1}, // a line at the instant of the start
		{at(2, 0), cosmicdust.StateRunning, 1},
		{at(4, 0), cosmicdust.StateExited, 1},
		{at(9, 0), cosmicdust.StateRunning, 2},
	}
	for _, c := range cases {
		if s, e := h.at(c.ts); s != c.state || e != c.epoch {
			t.Errorf("at(%v) = %s %d, want %s %d", c.ts.Format("05.000"), s, e, c.state, c.epoch)
		}
	}
	for i := 10; i < 20; i++ {
		h = h.add(transition{At: at(i, 0), State: cosmicdust.StateRunning, Epoch: i}, 4)
	}
	if len(h) != 4 || h[0].Epoch != 16 {
		t.Fatalf("bounded history kept %d, oldest epoch %d; want 4 from 16", len(h), h[0].Epoch)
	}
}

func TestHistoryEarlier(t *testing.T) {
	var h history
	h = h.add(transition{At: at(1, 0), State: cosmicdust.StateRunning, Epoch: 1}, 0)
	h = h.add(transition{At: at(30, 0), State: cosmicdust.StateExited, Epoch: 1}, 0) // stamped at a listing
	h.earlier(cosmicdust.StateExited, 1, at(20, 0))                                  // Docker knows it stopped at 20
	if s, _ := h.at(at(25, 0)); s != cosmicdust.StateExited {
		t.Fatalf("at 25 the container is %s, want exited", s)
	}
	h.earlier(cosmicdust.StateExited, 1, at(0, 0)) // never before the start
	if s, _ := h.at(at(10, 0)); s != cosmicdust.StateRunning {
		t.Fatalf("at 10 the container is %s, want running", s)
	}
	h.earlier(cosmicdust.StateExited, 1, at(40, 0)) // later: no change
	if h[1].At != at(20, 0) {
		t.Fatalf("exit moved to %v", h[1].At)
	}
}
