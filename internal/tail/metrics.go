package tail

import (
	"math"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Stats are the tailer's counters. They are plain atomics on the hot path and
// become Prometheus metrics only when scraped.
type Stats struct {
	lines             atomic.Int64 // physical lines read
	bytes             atomic.Int64 // bytes of those lines, newlines included
	messages          atomic.Int64 // messages handed on
	slowDecodes       atomic.Int64 // lines the fast json-file parser left to encoding/json
	malformedLines    atomic.Int64
	overlongLines     atomic.Int64
	truncatedMessages atomic.Int64
	tornLines         atomic.Int64
	rotations         atomic.Int64
	truncations       atomic.Int64
	lostFiles         atomic.Int64 // resume positions whose file was rotated away
	compressedSkipped atomic.Int64
	passes            atomic.Int64 // wake-ups of the scheduler with files due
	readErrors        atomic.Int64
	unsupported       atomic.Int64  // containers whose log is not a file the tailer reads
	storeWaits        atomic.Int64  // times reading stopped because the sink had no room
	open              atomic.Int64  // files being followed
	retention         atomic.Uint64 // float64 bits: the shortest stop of the agent that loses lines, in seconds
}

// Messages returns how many messages have been handed on.
func (s *Stats) Messages() int64 { return s.messages.Load() }

func (s *Stats) collectors() []prometheus.Collector {
	counter := func(name, help string, v *atomic.Int64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "tail", Name: name, Help: help},
			func() float64 { return float64(v.Load()) })
	}
	return []prometheus.Collector{
		counter("lines_total", "Physical log lines read.", &s.lines),
		counter("bytes_total", "Bytes of log lines read, newlines included.", &s.bytes),
		counter("messages_total", "Messages handed on, after joining the parts of long ones.", &s.messages),
		counter("slow_decodes_total", "json-file lines not in dockerd's shape, decoded with encoding/json.", &s.slowDecodes),
		counter("malformed_lines_total", "Lines that could not be decoded, skipped.", &s.malformedLines),
		counter("overlong_lines_total", "Physical lines over the limit, skipped.", &s.overlongLines),
		counter("truncated_messages_total", "Messages cut at the limit.", &s.truncatedMessages),
		counter("torn_lines_total", "Unfinished lines at the end of a rotated file.", &s.tornLines),
		counter("rotations_total", "Rotations followed.", &s.rotations),
		counter("truncations_total", "Files found shorter than where reading stopped.", &s.truncations),
		counter("lost_files_total", "Resume positions whose file had been rotated away; lines in it are lost.", &s.lostFiles),
		counter("compressed_skipped_total", "Compressed rotated files seen and skipped.", &s.compressedSkipped),
		counter("passes_total", "Wake-ups of the tailer with files due.", &s.passes),
		counter("read_errors_total", "Reads that failed; the file is reopened from its position.", &s.readErrors),
		counter("unsupported_total", "Containers whose log is not a json-file or CRI file, not read.", &s.unsupported),
		counter("store_waits_total", "Times reading stopped because the store had no room; it resumes once a write goes through.", &s.storeWaits),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "cosmicdust_agent", Subsystem: "tail", Name: "files",
			Help: "Container logs being followed."}, func() float64 { return float64(s.open.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "cosmicdust_agent", Subsystem: "tail", Name: "retention_seconds",
			Help: "The shortest stop of the agent that loses lines: of the followed logs, what the runtime keeps over the rate it is written. +Inf when no rotated log is written to."},
			func() float64 { return math.Float64frombits(s.retention.Load()) }),
	}
}
