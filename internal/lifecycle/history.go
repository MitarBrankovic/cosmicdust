package lifecycle

import (
	"sort"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// transition is one change of a container's merged state.
type transition struct {
	At    time.Time        `json:"at"`
	State cosmicdust.State `json:"state"`
	Epoch int              `json:"epoch"`
}

// history is a container's transitions in time order. It is what makes the
// state stored with a log line right: the first store stamped each line with
// the state at the moment it was stored, and after a crash, with the backlog
// stored once the containers had exited, 39 % of the lines were marked
// exited although each was written while its container ran.
type history []transition

// add inserts t in time order. Transitions almost always come in order; one
// that does not still goes where its stamp says. Past max entries the oldest
// go: a container in a crash loop must not grow without bound.
func (h history) add(t transition, max int) history {
	i := sort.Search(len(h), func(i int) bool { return h[i].At.After(t.At) })
	h = append(h, transition{})
	copy(h[i+1:], h[i:])
	h[i] = t
	if max > 0 && len(h) > max {
		h = append(h[:0], h[len(h)-max:]...)
	}
	return h
}

// earlier moves the latest transition into (state, epoch) back to t when
// another runtime witnessed it earlier. A reconstruction from containerd's
// listing is stamped at the listing; Docker's, from inspect, carries the time
// the container really stopped. It never moves a transition before the one
// preceding it.
func (h history) earlier(state cosmicdust.State, epoch int, t time.Time) {
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].State == state && h[i].Epoch == epoch {
			if t.Before(h[i].At) && (i == 0 || t.After(h[i-1].At)) {
				h[i].At = t
			}
			return
		}
	}
}

// at returns the state in force at ts: the last transition stamped no later
// than ts, or unknown before the first one.
func (h history) at(ts time.Time) (cosmicdust.State, int) {
	i := sort.Search(len(h), func(i int) bool { return h[i].At.After(ts) })
	if i == 0 {
		return cosmicdust.StateUnknown, 0
	}
	return h[i-1].State, h[i-1].Epoch
}
