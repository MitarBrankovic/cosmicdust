// Command verify reads back, through the server's query API, the lines the
// generators of an experiment wrote, and says whether every one of them is
// there exactly once.
//
//	verify -server 127.0.0.1:50051 -generators 5 -count 6000 node-1 node-2
//
// A generator numbers its lines from 1 (experiments/cmd/loggen), so for each
// of a node's generators the lines 1..count must each come back once. The
// generators are the ones whose id contains -match; lines of anything else
// are counted apart. One line per node is printed:
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

type result struct {
	generators, lines, missing, duplicate, other, pages int
}

func check(ctx context.Context, q api.QueryClient, node, match string, generators, count, page int) (result, error) {
	var r result
	seen := map[string][]bool{}
	req := &api.GetLogsRequest{Node: node, PageSize: int32(page)}
	for {
		out, err := q.GetLogs(ctx, req)
		if err != nil {
			return r, err
		}
		r.pages++
		for _, l := range out.Lines {
			gen, seq, ok := parse(l.Line)
			if !ok || !strings.Contains(gen, match) || seq < 1 || seq > count {
				r.other++
				continue
			}
			if seen[gen] == nil {
				seen[gen] = make([]bool, count+1)
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
	r.missing = max(generators, len(seen))*count - r.lines
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
	)
	flag.Parse()
	if *count <= 0 || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: verify -count <lines per generator> [flags] <node>...")
		os.Exit(2)
	}
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
		r, err := check(ctx, api.NewQueryClient(conn), node, *match, *generators, *count, *page)
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
