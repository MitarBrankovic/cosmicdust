package wal

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	blocks    prometheus.Counter
	bytes     prometheus.Counter // written, after s2
	payload   prometheus.Counter // before s2
	logs      prometheus.Counter
	events    prometheus.Counter
	fsync     prometheus.Histogram
	noSpace   prometheus.Counter
	tornTails prometheus.Counter
	corrupt   prometheus.Counter
	released  prometheus.Counter
}

func newMetrics(w *WAL) (*metrics, []prometheus.Collector) {
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "wal", Name: name, Help: help})
	}
	m := &metrics{
		blocks:  c("blocks_total", "Blocks written, one fsync each."),
		bytes:   c("bytes_total", "Bytes written to WAL segments, after s2."),
		payload: c("payload_bytes_total", "Bytes of block payloads before s2."),
		logs:    c("log_records_total", "Log records written."),
		events:  c("events_total", "Lifecycle events written."),
		fsync: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "cosmicdust_agent", Subsystem: "wal", Name: "fsync_seconds",
			Help:    "Time to sync a block.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}),
		noSpace:   c("no_space_total", "Writes that failed for lack of space; the block waited in memory."),
		tornTails: c("torn_tails_total", "Torn blocks cut off the newest segment at recovery."),
		corrupt:   c("corrupt_segments_total", "Segments found damaged before their end at recovery."),
		released:  c("released_segments_total", "Segments deleted after their consumer committed them."),
	}
	segments := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "cosmicdust_agent", Subsystem: "wal", Name: "segments",
		Help: "Segments on disk, the open one included."}, func() float64 { return float64(w.segmentsOnDisk()) })
	return m, []prometheus.Collector{m.blocks, m.bytes, m.payload, m.logs, m.events, m.fsync, m.noSpace,
		m.tornTails, m.corrupt, m.released, segments}
}
