// Command walcat prints what a WAL directory holds, one JSON object per log
// row or event, oldest first. It reads a copy of the state: run it on a stopped
// agent's directory, or on a copy of a running one's.
//
//	walcat [-logs] [-events] [-positions] <data-dir>/wal
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/c12s/cosmicdust/internal/store/wal"
)

func main() {
	logs := flag.Bool("logs", true, "print log rows")
	events := flag.Bool("events", true, "print events")
	positions := flag.Bool("positions", false, "print the resume positions of every block")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: walcat [-logs] [-events] [-positions] <wal directory>")
		os.Exit(2)
	}
	if err := run(flag.Arg(0), *logs, *events, *positions); err != nil {
		fmt.Fprintln(os.Stderr, "walcat:", err)
		os.Exit(1)
	}
}

func run(dir string, logs, events, positions bool) error {
	// Opening recovers the directory and seals what it finds; nothing is
	// released, so the segments stay as they are for the agent.
	w, err := wal.Open(wal.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))})
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	out := bufio.NewWriter(os.Stdout)
	defer func() { _ = out.Flush() }()
	enc := json.NewEncoder(out)
	for _, seg := range w.Sealed() {
		err := w.Read(seg, func(b *wal.Block) error {
			if positions {
				if err := enc.Encode(map[string]any{"segment": seg, "block_at": b.At.UTC(), "positions": b.Positions}); err != nil {
					return err
				}
			}
			if events {
				for _, e := range b.Events {
					if err := enc.Encode(map[string]any{"segment": seg, "event": e}); err != nil {
						return err
					}
				}
			}
			if logs {
				for _, r := range b.Logs {
					c := b.Containers[r.Container]
					row := map[string]any{
						"segment": seg, "ts": r.TS.UTC().Format(time.RFC3339Nano), "container": c.ID, "name": c.Name,
						"app": c.App, "stream": r.Stream, "line": r.Line, "inode": r.Inode, "offset": r.Offset,
					}
					if r.Parts > 1 {
						row["parts"] = r.Parts
					}
					if err := enc.Encode(row); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg, err)
		}
	}
	return nil
}
