// Command verify reads back, through the server's query API, the lines the
// generators of an experiment wrote, and says whether every one of them is
// there exactly once.
//
//	verify -server 127.0.0.1:50051 -generators 5 -count 6000 node-1 node-2
//
// A generator numbers its lines from 1 (experiments/cmd/loggen), so for each
// of a node's generators the lines 1..count must each come back once. The
// generators are the ones whose id contains -match; -also names more of them,
// with a count of their own:
//
//	verify -generators 5 -count 6000 -also /b=5x18000 node-1
//
// Lines of anything else are counted apart. One line per node is printed:
//
//	node=node-1 generators=5 lines=30000 missing=0 duplicate=0 other=0 pages=31 seconds=1.2
//
// and the exit status is 1 if a node misses a line or has one twice.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/c12s/cosmicdust/pkg/api"
)

// parse takes "gen=<id> seq=<n> ..." apart.
func parse(line string) (gen string, seq int, ok bool) {
	rest, found := strings.CutPrefix(line, "gen=")
	if !found {
		return "", 0, false
	}
	gen, rest, found = strings.Cut(rest, " seq=")
	if !found {
		return "", 0, false
	}
	num, _, _ := strings.Cut(rest, " ")
	seq, err := strconv.Atoi(num)
	return gen, seq, err == nil
}

// A group is the generators of a node that wrote the same number of lines:
// the ones whose id contains match.
type group struct {
	match      string
	generators int
	count      int
}

// parseGroup reads "<match>=<generators>x<count>".
func parseGroup(v string) (group, error) {
	match, rest, ok := strings.Cut(v, "=")
	gens, count, ok2 := strings.Cut(rest, "x")
	g := group{match: match}
	var err1, err2 error
	g.generators, err1 = strconv.Atoi(gens)
	g.count, err2 = strconv.Atoi(count)
	if !ok || !ok2 || match == "" || err1 != nil || err2 != nil || g.generators < 1 || g.count < 1 {
		return g, fmt.Errorf("%q is not <match>=<generators>x<count>", v)
	}
	return g, nil
}

type result struct {
	generators, lines, missing, duplicate, other, pages int
}

func check(ctx context.Context, q api.QueryClient, node string, groups []group, page int) (result, error) {
	var r result
	seen := map[string][]bool{}
	of := func(gen string) int {
		for i, g := range groups {
			if strings.Contains(gen, g.match) {
				return i
			}
		}
		return -1
	}
	req := &api.GetLogsRequest{Node: node, PageSize: int32(page)}
	for {
		out, err := q.GetLogs(ctx, req)
		if err != nil {
			return r, err
		}
		r.pages++
		for _, l := range out.Lines {
			gen, seq, ok := parse(l.Line)
			g := -1
			if ok {
				g = of(gen)
			}
			if g < 0 || seq < 1 || seq > groups[g].count {
				r.other++
				continue
			}
			if seen[gen] == nil {
				seen[gen] = make([]bool, groups[g].count+1)
			}
			if seen[gen][seq] {
				r.duplicate++
				continue
			}
			seen[gen][seq] = true
			r.lines++
		}
		if out.NextPageToken == "" {
			break
		}
		req.PageToken = out.NextPageToken
	}
	r.generators = len(seen)
	// What should be there: every group's lines, of as many generators as
	// expected - or as were seen, if there were more.
	found := make([]int, len(groups))
	for gen := range seen {
		found[of(gen)]++
	}
	want := 0
	for i, g := range groups {
		want += max(g.generators, found[i]) * g.count
	}
	r.missing = want - r.lines
	return r, nil
}

func main() {
	var (
		server     = flag.String("server", "127.0.0.1:50051", "the server's gRPC address")
		generators = flag.Int("generators", 1, "generators expected on every node")
		count      = flag.Int("count", 0, "lines every generator wrote")
		match      = flag.String("match", "/c", "a generator is a line source whose id contains this")
		page       = flag.Int("page", 1000, "lines asked for at a time")
		timeout    = flag.Duration("timeout", 10*time.Minute, "give up after this long")
		also       []group
	)
	flag.Func("also", "more generators of every node, as <match>=<generators>x<count>; may be repeated", func(v string) error {
		g, err := parseGroup(v)
		if err == nil {
			also = append(also, g)
		}
		return err
	})
	flag.Parse()
	if *count <= 0 || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: verify -count <lines per generator> [flags] <node>...")
		os.Exit(2)
	}
	groups := append([]group{{match: *match, generators: *generators, count: *count}}, also...)
	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(2)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	exact := true
	for _, node := range flag.Args() {
		start := time.Now()
		r, err := check(ctx, api.NewQueryClient(conn), node, groups, *page)
		if err != nil {
			fmt.Fprintf(os.Stderr, "verify: node %s: %v\n", node, err)
			os.Exit(2)
		}
		fmt.Printf("node=%s generators=%d lines=%d missing=%d duplicate=%d other=%d pages=%d seconds=%.1f\n",
			node, r.generators, r.lines, r.missing, r.duplicate, r.other, r.pages, time.Since(start).Seconds())
		if r.missing != 0 || r.duplicate != 0 {
			exact = false
		}
	}
	if !exact {
		os.Exit(1)
	}
}
