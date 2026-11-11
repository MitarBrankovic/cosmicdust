package lifecycle

import (
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// view is what one runtime has said about one container.
//
// Each runtime gets its own view because they report the same lifecycle from
// different heights and at different times. containerd sees an exit ~400 ms
// before Docker, and Docker has been seen to report a start after containerd
// had already reported the exit. Folded into one state machine, that
// late start would find the container exited and invent a restart. Within one
// runtime the order is sound, so each view counts its own incarnations, and
// the container's state is the merge of the views (merged).
type view struct {
	State    cosmicdust.State `json:"state"`
	Epoch    int              `json:"epoch"` // incarnations seen: +1 for every start that did not find it running
	At       time.Time        `json:"at"`    // stamp of the event that set State
	ExitCode *int             `json:"exit_code,omitempty"`
	OOM      bool             `json:"oom,omitempty"` // an OOM event in this incarnation
}

// apply folds one event into the view. moved reports a change of state or
// epoch; late, an event stamped before the one that set the current state.
//
// The state follows the newest event by its stamp, not by arrival: containerd
// has delivered /tasks/delete 1 ms before the /tasks/exit it belongs to.
// A late event does not move the state, or an exit followed by a late
// start would invent a restart. Events with equal stamps all apply, or a fast
// create and start in the same instant would lose one.
func (v *view) apply(ev cosmicdust.Event) (moved, late bool) {
	switch ev.Kind {
	case cosmicdust.KindCreate, cosmicdust.KindStart, cosmicdust.KindExit, cosmicdust.KindRemove, cosmicdust.KindOOM:
	default:
		return false, false
	}
	if !v.At.IsZero() && ev.Time.Before(v.At) {
		return false, true
	}
	if ev.Kind == cosmicdust.KindOOM {
		// Says nothing about the state; the exit that follows does.
		v.OOM = true
		return false, false
	}
	before, epoch := v.State, v.Epoch
	v.At = ev.Time
	switch ev.Kind {
	case cosmicdust.KindCreate:
		if v.State == "" || v.State == cosmicdust.StateUnknown || v.State == cosmicdust.StateRemoved {
			v.State = cosmicdust.StateCreated
		}
	case cosmicdust.KindStart:
		// A start that does not find the container running is a new
		// incarnation: its log lines, read position and buffers must not be
		// mixed with the previous one's.
		if v.State != cosmicdust.StateRunning {
			v.Epoch++
			v.ExitCode = nil
			v.OOM = false
		}
		v.State = cosmicdust.StateRunning
	case cosmicdust.KindExit:
		if v.State != cosmicdust.StateRunning && v.State != cosmicdust.StateRestarting && v.State != cosmicdust.StateExited {
			// The exit of an incarnation whose start this view did not see:
			// the agent was down, or a listing came between the create and
			// the start. It ran, so it counts; under the previous epoch, a
			// restart policy's exits came out one short. An OOM event
			// before it was this incarnation's and stays.
			v.Epoch++
			v.ExitCode = nil
		}
		v.State = cosmicdust.StateExited
		if ev.ExitCode != nil {
			code := *ev.ExitCode
			v.ExitCode = &code
		}
	case cosmicdust.KindRemove:
		v.State = cosmicdust.StateRemoved
	}
	return v.State != before || v.Epoch != epoch, false
}

// phase orders the states within one incarnation. A container only moves
// forward through them until its next start.
func phase(s cosmicdust.State) int {
	switch s {
	case cosmicdust.StateCreated:
		return 1
	case cosmicdust.StateRunning:
		return 2
	case cosmicdust.StateExited, cosmicdust.StateRestarting:
		return 3
	case cosmicdust.StateRemoved:
		return 4
	}
	return 0
}

// ahead reports whether a is further along the lifecycle than b: a later
// incarnation, or the same one in a later phase.
func ahead(a, b *view) bool {
	if a.Epoch != b.Epoch {
		return a.Epoch > b.Epoch
	}
	return phase(a.State) > phase(b.State)
}

// status is the merged state of a container.
type status struct {
	State    cosmicdust.State
	Epoch    int
	ExitCode *int
}

// merged is the furthest view: whichever runtime saw the container get
// further is right, because a runtime that has not reported a step yet is
// late rather than wrong. When containerd reports an exit, Docker's view
// still says running for another ~400 ms; when Docker's late start arrives
// after containerd's exit, containerd's view is already further. Views that
// lag after a gap in their stream catch up at the next reconciliation.
func merged(views map[string]*view) status {
	var best *view
	for _, rt := range []string{cosmicdust.RuntimeContainerd, cosmicdust.RuntimeDocker} {
		if v := views[rt]; v != nil && (best == nil || ahead(v, best)) {
			best = v
		}
	}
	for rt, v := range views { // a runtime not named above, for tests
		if rt != cosmicdust.RuntimeContainerd && rt != cosmicdust.RuntimeDocker && (best == nil || ahead(v, best)) {
			best = v
		}
	}
	if best == nil {
		return status{State: cosmicdust.StateUnknown}
	}
	st := status{State: best.State, Epoch: best.Epoch, ExitCode: best.ExitCode}
	if st.State == "" {
		st.State = cosmicdust.StateUnknown
	}
	if st.ExitCode == nil && phase(st.State) >= phase(cosmicdust.StateExited) {
		// The other runtime may have the code for the same incarnation.
		for _, v := range views {
			if v.Epoch == st.Epoch && v.ExitCode != nil {
				st.ExitCode = v.ExitCode
				break
			}
		}
	}
	return st
}
