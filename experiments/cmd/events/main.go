// Command events checks the lifecycle events of a node against what a script
// made happen on it.
//
//	events -server 127.0.0.1:50051 -truth truth.txt node-1
//
// The truth has a container per line,
//
//	<name> <exits> <state>
//
// with the exits of the container's incarnations in their order, each as
// <code>:<1 if the kernel killed it for memory, else 0> and separated by
// semicolons ("-" for a container that never ended), and the state the
// container is left in. A container without a name of its own goes by its ID.
//
// The node's events are read through the server's query API. An exit counts
// as recorded when there is an exit event of that incarnation, from either
// runtime, that is not older than the incarnation's start; its code as right
// when one of them carries it; the kill for memory as right when the events
// say what the truth says. A second exit event of the same runtime and
// incarnation is a duplicate. The state is right when the container's last
// event leaves it there.
//
// A value per line is printed, as "<metric> <value>":
//
//	containers 11
//	exits_expected 13
//	exits_recorded 13
//	recorded_pct 100.0
//	codes_right_pct 100.0     of the exits recorded
//	oom_right_pct 100.0       of the exits recorded
//	duplicates 0
//	exit_events 27
//	inferred_pct 0.0          exit events no runtime reported: concluded from a listing
//	final_right_pct 100.0
//
// and with -detail a line per container is written to that file.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/c12s/cosmicdust/pkg/api"
)

type exit struct {
	code int32
	oom  bool
}

// container is a line of the truth.
type container struct {
	name  string
	exits []exit
	final string
}

func readTruth(path string) ([]container, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []container
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s: %q is not <name> <exits> <state>", path, sc.Text())
		}
		c := container{name: fields[0], final: fields[2]}
		if fields[1] != "-" {
			for _, e := range strings.Split(fields[1], ";") {
				code, oom, ok := strings.Cut(e, ":")
				n, err := strconv.ParseInt(code, 10, 32)
				if !ok || err != nil || (oom != "0" && oom != "1") {
					return nil, fmt.Errorf("%s: %q is not <code>:<0 or 1>", path, e)
				}
				c.exits = append(c.exits, exit{code: int32(n), oom: oom == "1"})
			}
		}
		out = append(out, c)
	}
	return out, sc.Err()
}

// fetch reads every event of a node.
func fetch(ctx context.Context, q api.QueryClient, node string) ([]*api.LifecycleEvent, error) {
	var out []*api.LifecycleEvent
	req := &api.GetEventsRequest{Node: node, PageSize: 1000}
	for {
		page, err := q.GetEvents(ctx, req)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Events...)
		if page.NextPageToken == "" {
			return out, nil
		}
		req.PageToken = page.NextPageToken
	}
}

// byContainer groups the events by the name their container goes by: its
// own, the one another event of the same container has, or else its ID.
func byContainer(events []*api.LifecycleEvent) map[string][]*api.LifecycleEvent {
	names := map[string]string{}
	for _, e := range events {
		if e.Name != "" {
			names[e.ContainerId] = strings.TrimPrefix(e.Name, "/")
		}
	}
	out := map[string][]*api.LifecycleEvent{}
	for _, e := range events {
		name, ok := names[e.ContainerId]
		if !ok {
			name = e.ContainerId
		}
		out[name] = append(out[name], e)
	}
	for _, evs := range out {
		slices.SortStableFunc(evs, func(a, b *api.LifecycleEvent) int {
			return a.GetTime().AsTime().Compare(b.GetTime().AsTime())
		})
	}
	return out
}

// result is what came of one container, or of all of them together.
type result struct {
	containers, expected, recorded, codes, ooms, duplicates, exitEvents, inferred, finals int
}

func (r *result) add(o result) {
	r.containers += o.containers
	r.expected += o.expected
	r.recorded += o.recorded
	r.codes += o.codes
	r.ooms += o.ooms
	r.duplicates += o.duplicates
	r.exitEvents += o.exitEvents
	r.inferred += o.inferred
	r.finals += o.finals
}

// check compares a container's events with its truth, and says the state
// its last event left it in.
func check(c container, events []*api.LifecycleEvent) (result, string) {
	r := result{containers: 1, expected: len(c.exits)}
	type incarnation struct {
		runtime string
		epoch   int32
	}
	seen := map[incarnation]int{}
	var exits []*api.LifecycleEvent
	for _, e := range events {
		if e.Kind != "exit" {
			continue
		}
		exits = append(exits, e)
		r.exitEvents++
		if e.Inferred {
			r.inferred++
		}
		k := incarnation{e.Runtime, e.Epoch}
		if seen[k]++; seen[k] == 2 {
			r.duplicates++
		}
	}
	// When each incarnation started, as far as the events say: an exit event
	// older than that is of an earlier incarnation, whatever its number.
	started := map[int32]time.Time{}
	for _, e := range events {
		if e.Kind != "start" {
			continue
		}
		if at, ok := started[e.Epoch]; !ok || e.GetTime().AsTime().Before(at) {
			started[e.Epoch] = e.GetTime().AsTime()
		}
	}
	for i, want := range c.exits {
		recorded, code, oom := false, false, false
		for _, e := range exits {
			if int(e.Epoch) != i+1 {
				continue
			}
			if at, ok := started[e.Epoch]; ok && e.GetTime().AsTime().Before(at) {
				continue
			}
			recorded = true
			code = code || (e.ExitCode != nil && e.GetExitCode() == want.code)
			oom = oom || e.OomKilled
		}
		if !recorded {
			continue
		}
		r.recorded++
		if code {
			r.codes++
		}
		if oom == want.oom {
			r.ooms++
		}
	}
	last := "none"
	if len(events) > 0 {
		last = events[len(events)-1].State
	}
	if last == c.final {
		r.finals = 1
	}
	return r, last
}

func pct(part, whole int) string {
	if whole == 0 {
		return ""
	}
	return strconv.FormatFloat(100*float64(part)/float64(whole), 'f', 1, 64)
}

func main() {
	var (
		server  = flag.String("server", "127.0.0.1:50051", "the server's gRPC address")
		truth   = flag.String("truth", "", `what happened: a file of "<name> <exits> <state>" lines`)
		detail  = flag.String("detail", "", "write a line per container to this file")
		timeout = flag.Duration("timeout", time.Minute, "give up after this long")
	)
	flag.Parse()
	if *truth == "" || flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: events -truth <file> [flags] <node>")
		os.Exit(2)
	}
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "events:", err)
		os.Exit(2)
	}
	containers, err := readTruth(*truth)
	if err != nil {
		fail(err)
	}
	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	events, err := fetch(ctx, api.NewQueryClient(conn), flag.Arg(0))
	if err != nil {
		fail(err)
	}
	grouped := byContainer(events)

	var total result
	var lines strings.Builder
	lines.WriteString("container exits_expected exits_recorded codes_right oom_right duplicates exit_events inferred state_expected state_seen\n")
	for _, c := range containers {
		r, last := check(c, grouped[c.name])
		total.add(r)
		fmt.Fprintf(&lines, "%s %d %d %d %d %d %d %d %s %s\n", c.name, r.expected, r.recorded, r.codes, r.ooms,
			r.duplicates, r.exitEvents, r.inferred, c.final, last)
	}
	if *detail != "" {
		if err := os.WriteFile(*detail, []byte(lines.String()), 0o644); err != nil {
			fail(err)
		}
	}
	fmt.Println("containers", total.containers)
	fmt.Println("exits_expected", total.expected)
	fmt.Println("exits_recorded", total.recorded)
	fmt.Println("recorded_pct", pct(total.recorded, total.expected))
	fmt.Println("codes_right_pct", pct(total.codes, total.recorded))
	fmt.Println("oom_right_pct", pct(total.ooms, total.recorded))
	fmt.Println("duplicates", total.duplicates)
	fmt.Println("exit_events", total.exitEvents)
	fmt.Println("inferred_pct", pct(total.inferred, total.exitEvents))
	fmt.Println("final_right_pct", pct(total.finals, total.containers))
}
