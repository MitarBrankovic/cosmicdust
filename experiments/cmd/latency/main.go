// Command latency says how late the lines of a run were, from what the server
// recorded of them itself.
//
//	latency -data <the server's data directory> [-exits <file>]
//
// The server's ledger (state/ledger.jsonl) has the moment every file of a
// node arrived, the archive files its rows went into, and the moment those
// were in the search index; the archive files have every line with the time
// the node's runtime took it. A generator's line - one that begins with
// -match - was late at the server by the time from the one to the arrival of
// its file, and late for the search by the time to the indexing of its
// archive file.
//
// -exits names a file with a container per line, "<name> <when its process
// ended>", the time as the runtime prints it (RFC 3339). An urgent event the
// server took for one of them (state/urgent.jsonl) was late by the time from
// that end to its arrival.
//
// A value per line is printed, as "<series> <metric> <value>":
//
//	server p50_s 9.312       half of the lines were at the server within this
//	server p95_s 15.704
//	server p99_s 20.611
//	server max_s 23.702
//	search p50_s 10.105      the same, to the search index
//	...
//	urgent p50_s 0.301       urgent events, to the server
//	urgent max_s 0.342
//	urgent events_timed 11
//	agent log_files 18       files of log lines the server received
//	agent lines_timed 180011
//
// The exit status is 1 if there is no line to time, or if a line of the
// archive is not in the index.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/receiver"
	"github.com/c12s/cosmicdust/internal/server/urgent"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// entry is a line of the server's ledger, as far as it matters here.
type entry struct {
	Op      string   `json:"op"`
	At      int64    `json:"at"` // unix nanoseconds
	File    string   `json:"file"`
	Outputs []string `json:"outputs"`
}

// record is what the ledger says of the files of log lines.
type record struct {
	received map[string]int64  // a received file: when it arrived
	source   map[string]string // an archive file: the received file its rows came in
	indexed  map[string]int64  // an archive file: when it was in the search index
}

// lines calls fn with every line of a file of JSON lines.
func lines(path string, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 1 {
			if ferr := fn(line); ferr != nil {
				return ferr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func readLedger(path string) (*record, error) {
	rec := &record{received: map[string]int64{}, source: map[string]string{}, indexed: map[string]int64{}}
	err := lines(path, func(line []byte) error {
		var e entry
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		switch e.Op {
		case "received":
			if _, table, _, _, err := receiver.ParsePath(e.File); err != nil || table != api.TableLogs {
				return nil
			}
			if _, again := rec.received[e.File]; !again {
				rec.received[e.File] = e.At
			}
		case "archived":
			for _, o := range e.Outputs {
				if e.File != "" {
					rec.source[o] = e.File
				}
			}
		case "indexed":
			for _, o := range e.Outputs {
				if _, again := rec.indexed[o]; !again {
					rec.indexed[o] = e.At
				}
			}
		}
		return nil
	})
	return rec, err
}

// quantile is the q-th quantile of sorted values, between the two nearest.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	at := q * float64(len(sorted)-1)
	lo := int(at)
	if lo+1 >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	return sorted[lo] + (at-float64(lo))*(sorted[lo+1]-sorted[lo])
}

func seconds(from, to int64) float64 { return float64(to-from) / 1e9 }

// readExits reads "<name> <time>" lines.
func readExits(path string) (map[string]int64, error) {
	exits := map[string]int64{}
	err := lines(path, func(line []byte) error {
		name, when, ok := strings.Cut(strings.TrimSpace(string(line)), " ")
		if !ok {
			return nil
		}
		t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(when))
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		exits[strings.TrimPrefix(name, "/")] = t.UnixNano()
		return nil
	})
	return exits, err
}

func run(data, match, exitsPath string) error {
	rec, err := readLedger(filepath.Join(data, "state", "ledger.jsonl"))
	if err != nil {
		return err
	}
	outputs := make([]string, 0, len(rec.source))
	for o, file := range rec.source {
		if _, ok := rec.received[file]; ok {
			outputs = append(outputs, o)
		}
	}
	slices.Sort(outputs)

	var server, search []float64
	unindexed := 0
	for _, o := range outputs {
		arrived := rec.received[rec.source[o]]
		indexed, isIndexed := rec.indexed[o]
		path := filepath.Join(data, "archive", filepath.FromSlash(o))
		err := archive.ReadRows(path, 4096, func(rows []store.LogRow) error {
			for i := range rows {
				if !strings.HasPrefix(rows[i].Line, match) {
					continue
				}
				written := rows[i].TS.UnixNano()
				server = append(server, seconds(written, arrived))
				if isIndexed {
					search = append(search, seconds(written, indexed))
				} else {
					unindexed++
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if len(server) == 0 {
		return errors.New("the archive has no line to time")
	}
	for _, s := range []struct {
		name   string
		values []float64
	}{{"server", server}, {"search", search}} {
		slices.Sort(s.values)
		if len(s.values) == 0 {
			continue
		}
		fmt.Printf("%s p50_s %.3f\n", s.name, quantile(s.values, 0.50))
		fmt.Printf("%s p95_s %.3f\n", s.name, quantile(s.values, 0.95))
		fmt.Printf("%s p99_s %.3f\n", s.name, quantile(s.values, 0.99))
		fmt.Printf("%s max_s %.3f\n", s.name, s.values[len(s.values)-1])
	}

	if exitsPath != "" {
		exits, err := readExits(exitsPath)
		if err != nil {
			return err
		}
		var late []float64
		err = lines(filepath.Join(data, "state", "urgent.jsonl"), func(line []byte) error {
			var sp urgent.Spooled
			if json.Unmarshal(line, &sp) != nil || sp.Event == nil {
				return nil
			}
			if ended, ok := exits[strings.TrimPrefix(sp.Event.GetName(), "/")]; ok {
				late = append(late, seconds(ended, sp.ReceivedAt))
			}
			return nil
		})
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		slices.Sort(late)
		if len(late) > 0 {
			fmt.Printf("urgent p50_s %.3f\n", quantile(late, 0.50))
			fmt.Printf("urgent max_s %.3f\n", late[len(late)-1])
		}
		fmt.Printf("urgent events_timed %d\n", len(late))
	}
	fmt.Printf("agent log_files %d\n", len(rec.received))
	fmt.Printf("agent lines_timed %d\n", len(server))
	if unindexed > 0 {
		return fmt.Errorf("%d lines of the archive are not in the search index", unindexed)
	}
	return nil
}

func main() {
	var (
		data  = flag.String("data", "", "the server's data directory")
		match = flag.String("match", "gen=", "a line to time begins with this")
		exits = flag.String("exits", "", `a file of "<container name> <when its process ended>" lines, for the urgent events`)
	)
	flag.Parse()
	if *data == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: latency -data <the server's data directory> [-exits <file>]")
		os.Exit(2)
	}
	if err := run(*data, *match, *exits); err != nil {
		fmt.Fprintln(os.Stderr, "latency:", err)
		os.Exit(1)
	}
}
