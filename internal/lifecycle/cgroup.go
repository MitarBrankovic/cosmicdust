package lifecycle

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Neither runtime is a reliable witness of an OOM kill: containerd has
// long-standing reports of TaskOOM dropped or duplicated (issues 8893, 4547),
// Docker's "oom" depends on the daemon noticing in time, and exit code 137 is
// the same for an OOM kill and a kill -9. The kernel keeps a counter
// per cgroup that only grows, so an OOM event is a hint and memory.events is
// the evidence. The cgroup goes away with the incarnation, so it is watched
// while the incarnation lives (oomwatch_linux.go), and read again at the first
// exit or OOM event, for an incarnation the watch missed.

// cgroupLayouts are the places a container's cgroup (v2) is found, by
// runtime and cgroup driver; %[1]s is the container ID, %[2]s its namespace.
var cgroupLayouts = []string{
	"system.slice/docker-%[1]s.scope",         // Docker, systemd driver
	"docker/%[1]s",                            // Docker, cgroupfs driver
	"system.slice/cri-containerd-%[1]s.scope", // containerd CRI, systemd driver
	"system.slice/nerdctl-%[1]s.scope",        // nerdctl, systemd driver
	"%[2]s/%[1]s",                             // nerdctl and ctr, cgroupfs driver
}

// cgroups reads memory.events under a cgroup v2 root.
type cgroups struct {
	root string // "" disables the check
}

// oomKills returns the oom_kill counter of a container's cgroup. read is false
// when the cgroup is not there (any more), which is "too late to know", not
// "no OOM".
func (c cgroups) oomKills(id, namespace string) (n int64, read bool) {
	for _, p := range c.eventFiles(id, namespace) {
		if n, err := readOOMKills(p); err == nil {
			return n, true
		}
	}
	return 0, false
}

// eventFiles are the places memory.events of a container's cgroup can be.
func (c cgroups) eventFiles(id, namespace string) []string {
	if c.root == "" || id == "" || strings.ContainsAny(id, "/.") {
		return nil
	}
	ps := make([]string, len(cgroupLayouts))
	for i, layout := range cgroupLayouts {
		ps[i] = filepath.Join(c.root, fmt.Sprintf(layout, id, namespace), "memory.events")
	}
	return ps
}

// memEvents are the counters of memory.events the tracker reads: how often
// the cgroup reached its limit (max), how often that ended in the OOM killer
// being invoked (oom), and how many of its processes it killed (oom_kill).
type memEvents struct {
	max, oom, oomKill int64
}

func readOOMKills(path string) (int64, error) {
	ev, err := readMemEvents(path)
	return ev.oomKill, err
}

// readMemEvents parses memory.events, one "key value" per line.
func readMemEvents(path string) (memEvents, error) {
	var ev memEvents
	f, err := os.Open(path)
	if err != nil {
		return ev, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		var p *int64
		switch k {
		case "max":
			p = &ev.max
		case "oom":
			p = &ev.oom
		case "oom_kill":
			p = &ev.oomKill
		default:
			continue
		}
		if *p, err = strconv.ParseInt(v, 10, 64); err != nil {
			return ev, err
		}
	}
	return ev, sc.Err()
}
