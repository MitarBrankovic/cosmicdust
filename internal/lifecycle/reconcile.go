package lifecycle

import (
	"sort"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Events are not durable: no runtime replays what happened while nobody
// listened, so after every connect the tracker compares one runtime's view of
// every container with that runtime's listing and derives the events that
// must have happened in between. They are marked inferred, so a
// consumer can tell a witnessed fact from a reconstruction, and they go
// through the same path as witnessed events.
//
// Two rules keep a reconstruction from overriding the stream, which is open
// while the listing runs:
//   - an inferred event is stamped no later than the listing's start; an
//     event the stream delivered meanwhile is newer and wins (view.apply);
//   - where the runtime says when (Docker's inspect: StartedAt, FinishedAt),
//     the event carries that time, so the history, and with it the state of
//     every log line written while the agent was away, is right.

// inferred derives the events that bring one runtime's views in line with its
// listing. views maps container IDs to that runtime's view and namespace.
func inferred(runtime string, views map[string]viewRef, l cosmicdust.Listing) []cosmicdust.Event {
	var out []cosmicdust.Event
	listed := make(map[string]bool, len(l.Containers))
	for _, cs := range l.Containers {
		listed[cs.ID] = true
		out = append(out, catchUp(runtime, views[cs.ID].v, cs, l)...)
	}
	// Containers this runtime knew that its listing does not have.
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids) // a stable order for tests and logs
	for _, id := range ids {
		vr := views[id]
		if listed[id] || vr.v == nil {
			continue
		}
		mk := func(k cosmicdust.Kind) cosmicdust.Event {
			return cosmicdust.Event{
				Time: l.At, Runtime: runtime, Namespace: vr.namespace, Action: "reconcile/" + k.String(),
				Kind: k, ContainerID: id, Inferred: true,
			}
		}
		switch {
		case l.RunningOnly[vr.namespace]:
			// Here a missing container has stopped; whether it is also
			// gone is for another runtime to say.
			if vr.v.State == cosmicdust.StateRunning {
				out = append(out, mk(cosmicdust.KindExit))
			}
		case vr.v.State != cosmicdust.StateRemoved:
			out = append(out, mk(cosmicdust.KindRemove))
		}
	}
	return out
}

// viewRef is one runtime's view of a container, with the namespace that
// decides what its absence from a listing means.
type viewRef struct {
	v         *view
	namespace string
}

// catchUp derives the events that bring view v (nil if the runtime never
// reported the container) to the state cs the runtime lists.
func catchUp(runtime string, v *view, cs cosmicdust.ContainerState, l cosmicdust.Listing) []cosmicdust.Event {
	// Never later than the listing: whatever the stream delivered since is
	// newer.
	stamp := func(t time.Time) time.Time {
		if t.IsZero() || t.After(l.At) {
			return l.At
		}
		return t
	}
	mk := func(k cosmicdust.Kind, t time.Time) cosmicdust.Event {
		ev := cosmicdust.Event{
			Time: stamp(t), Runtime: runtime, Namespace: cs.Namespace, Action: "reconcile/" + k.String(),
			Kind: k, ContainerID: cs.ID, Name: cs.Name, Image: cs.Image, App: cs.App, Inferred: true,
		}
		if k == cosmicdust.KindExit {
			ev.ExitCode = cs.ExitCode
			ev.OOMKilled = cs.OOMKilled
		}
		return ev
	}
	// exitBefore stands for an exit the runtime did not report and nothing
	// dates: just before the next start, the latest it can have been, and
	// not before the view's last word, or the view would take it for late.
	exitBefore := func(t time.Time) cosmicdust.Event {
		at := stamp(t).Add(-time.Millisecond)
		if at.Before(v.At) {
			at = v.At
		}
		ev := mk(cosmicdust.KindExit, at)
		ev.ExitCode, ev.OOMKilled = nil, false
		return ev
	}

	known := v != nil && v.State != "" && v.State != cosmicdust.StateUnknown
	// A start the view has not seen: the listing's incarnation began after
	// the view's last word.
	newStart := known && !cs.StartedAt.IsZero() && cs.StartedAt.After(v.At)

	switch cs.State {
	case cosmicdust.StateCreated:
		if l.RunningOnly[cs.Namespace] {
			return nil // an object between two incarnations, not a fact
		}
		if !known {
			return []cosmicdust.Event{mk(cosmicdust.KindCreate, time.Time{})}
		}

	case cosmicdust.StateRunning:
		switch {
		case !known || v.State == cosmicdust.StateCreated:
			return []cosmicdust.Event{mk(cosmicdust.KindStart, cs.StartedAt)}
		case v.State == cosmicdust.StateRunning && newStart:
			// Restarted while away: the exit and the start were missed.
			return []cosmicdust.Event{exitBefore(cs.StartedAt), mk(cosmicdust.KindStart, cs.StartedAt)}
		case v.State == cosmicdust.StateExited || v.State == cosmicdust.StateRestarting:
			return []cosmicdust.Event{mk(cosmicdust.KindStart, cs.StartedAt)}
		}

	case cosmicdust.StateExited, cosmicdust.StateRestarting:
		switch {
		case !known || v.State == cosmicdust.StateCreated:
			if !cs.StartedAt.IsZero() {
				return []cosmicdust.Event{mk(cosmicdust.KindStart, cs.StartedAt), mk(cosmicdust.KindExit, cs.FinishedAt)}
			}
			return []cosmicdust.Event{mk(cosmicdust.KindExit, cs.FinishedAt)}
		case v.State == cosmicdust.StateRunning && newStart:
			return []cosmicdust.Event{exitBefore(cs.StartedAt), mk(cosmicdust.KindStart, cs.StartedAt), mk(cosmicdust.KindExit, cs.FinishedAt)}
		case v.State == cosmicdust.StateRunning:
			return []cosmicdust.Event{mk(cosmicdust.KindExit, cs.FinishedAt)}
		case v.State == cosmicdust.StateExited && newStart:
			// A whole incarnation came and went while away.
			return []cosmicdust.Event{mk(cosmicdust.KindStart, cs.StartedAt), mk(cosmicdust.KindExit, cs.FinishedAt)}
		}

	case cosmicdust.StateRemoved:
		if !known || v.State != cosmicdust.StateRemoved {
			return []cosmicdust.Event{mk(cosmicdust.KindRemove, time.Time{})}
		}
	}
	return nil
}
