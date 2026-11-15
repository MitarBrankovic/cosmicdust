// Command loggen writes numbered log lines at a steady rate. Every line
// carries the generator id and a sequence number that grows by one, so a
// reader can prove it lost nothing and read nothing twice, and a nanosecond
// timestamp, so the reader can measure how late it was.
//
//	gen=<id> seq=<n> ts=<unix nanoseconds> pad=<filler>
//	gen=<id> seq=<n> ts=<unix nanoseconds> msg=<line from a real log>   (with -replay)
//
// It exists because flog cannot hold a set rate (1 ms of delay gives about
// 600 lines per second, not 1000) and does not number its lines. The filler
// is a run of one letter, which compresses far better than any real log; for
// measuring storage and compression, -replay takes the text from a real log
// file instead, starting at a place that depends on the id so that containers
// do not all write the same lines.
//
// With -exit it ends with that exit code once it has written its lines, to
// stand in for a container that fails.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// loadReplay reads up to limit lines of a log file to cycle through.
func loadReplay(path string, limit int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() && len(lines) < limit {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s has no lines", path)
	}
	return lines, sc.Err()
}

func main() {
	host, _ := os.Hostname()
	var (
		id          = flag.String("id", host, "generator id written on every line")
		rate        = flag.Int("rate", 1000, "lines per second")
		count       = flag.Int("count", 0, "stop after this many lines (0 = until stopped)")
		size        = flag.Int("size", 200, "approximate length of a line in bytes")
		bigEvery    = flag.Int("big-every", 0, "make every n-th line huge (0 = never)")
		bigSize     = flag.Int("big-size", 1<<20, "length of a huge line in bytes")
		stderrEvery = flag.Int("stderr-every", 0, "send every n-th line to stderr (0 = never)")
		hold        = flag.Bool("hold", false, "after count lines, keep running until stopped")
		replay      = flag.String("replay", "", "take the text of each line from this log file instead of filler")
		replayMax   = flag.Int("replay-max", 60000, "read at most this many lines of the replay file")
		raw         = flag.Bool("raw", false, "with -replay, write the replayed lines as they are, without id, sequence and time")
		exit        = flag.Int("exit", 0, "exit with this code once count lines are written")
	)
	flag.Parse()

	var lines []string
	var next int
	if *replay != "" {
		var err error
		if lines, err = loadReplay(*replay, *replayMax); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		h := fnv.New32a()
		_, _ = h.Write([]byte(*id))
		next = int(h.Sum32()) % len(lines)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	out := bufio.NewWriterSize(os.Stdout, 64*1024)
	errw := bufio.NewWriterSize(os.Stderr, 64*1024)
	defer func() { _ = out.Flush(); _ = errw.Flush() }()

	// The fixed part of a line is about 60 bytes; the filler makes up the rest.
	pad := strings.Repeat("x", max(1, *size-60))
	big := strings.Repeat("y", max(1, *bigSize))

	// Pace by debt rather than by sleeping per line: every tick, write however
	// many lines the rate says should exist by now. That holds the average
	// exactly and does not drift with scheduling noise. The tick follows the
	// rate (one line's time, between 5 and 100 ms): a fixed 5 ms tick woke
	// every generator 200 times a second whatever its rate, and 200 quiet
	// generators kept ~8 cores of the benchmark VM busy.
	start := time.Now()
	every := time.Second / time.Duration(max(1, *rate))
	tick := time.NewTicker(min(max(every, 5*time.Millisecond), 100*time.Millisecond))
	defer tick.Stop()

	seq := 0
	for {
		due := int(time.Since(start).Seconds() * float64(*rate))
		if *count > 0 {
			due = min(due, *count)
		}
		for seq < due {
			seq++
			w := out
			if *stderrEvery > 0 && seq%*stderrEvery == 0 {
				w = errw
			}
			if lines != nil {
				if *raw {
					_, _ = fmt.Fprintln(w, lines[next])
				} else {
					_, _ = fmt.Fprintf(w, "gen=%s seq=%d ts=%d msg=%s\n", *id, seq, time.Now().UnixNano(), lines[next])
				}
				next = (next + 1) % len(lines)
				continue
			}
			filler := pad
			if *bigEvery > 0 && seq%*bigEvery == 0 {
				filler = big
			}
			_, _ = fmt.Fprintf(w, "gen=%s seq=%d ts=%d pad=%s\n", *id, seq, time.Now().UnixNano(), filler)
		}
		_ = out.Flush()
		_ = errw.Flush()

		if *count > 0 && seq >= *count {
			if *hold {
				<-ctx.Done() // done writing: wait without waking up
			}
			if *exit != 0 {
				os.Exit(*exit) // everything is flushed above
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
