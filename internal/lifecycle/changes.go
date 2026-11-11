package lifecycle

import "sync"

// Changes tells a subscriber which containers changed: their merged state or
// epoch, or what the tracker knows about their log. Entries are coalesced per
// container, so a burst of events for one container is one entry, and the
// subscriber reads the container's current state with Get. Nothing the
// subscriber does can block the tracker.
type Changes struct {
	mu    sync.Mutex
	ids   map[string]struct{}
	ready chan struct{}
}

// NewChanges returns an empty feed; the tracker makes them with Subscribe,
// others (a test's stand-in for the tracker) with this.
func NewChanges() *Changes {
	return &Changes{ids: map[string]struct{}{}, ready: make(chan struct{}, 1)}
}

// C receives a value whenever there are changes to Take.
func (c *Changes) C() <-chan struct{} { return c.ready }

// Take returns the containers changed since the last Take, in no order.
func (c *Changes) Take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.ids))
	for id := range c.ids {
		out = append(out, id)
	}
	clear(c.ids)
	return out
}

// Add marks id as changed.
func (c *Changes) Add(id string) {
	c.mu.Lock()
	c.ids[id] = struct{}{}
	c.mu.Unlock()
	select {
	case c.ready <- struct{}{}:
	default:
	}
}

// Subscribe returns a feed of changed containers. It starts with every
// container the tracker knows, and when the tracker reads its snapshot, every
// container in it: one whose state did not change while the agent was away
// must still be picked up.
func (t *Tracker) Subscribe() *Changes {
	c := NewChanges()
	t.mu.Lock()
	t.subs = append(t.subs, c)
	for id := range t.containers {
		c.Add(id)
	}
	t.mu.Unlock()
	return c
}

// notify tells every subscriber that id changed; the caller holds t.mu.
func (t *Tracker) notify(id string) {
	for _, s := range t.subs {
		s.Add(id)
	}
}
