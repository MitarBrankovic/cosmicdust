package store

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	conversions    prometheus.Counter
	convertSeconds prometheus.Histogram
	rowsStored     *prometheus.CounterVec // table
	compactions    prometheus.Counter
	compactSkipped prometheus.Counter
	bytesCompacted prometheus.Counter
	orphans        prometheus.Counter
	synced         prometheus.Counter
	deleted        *prometheus.CounterVec // synced or unsynced
	droppedRows    prometheus.Counter
	fullTimes      prometheus.Counter
	held           prometheus.Gauge

	stored, raw atomic.Int64 // Parquet bytes and the same rows as JSON Lines, by conversions
}

func (m *metrics) addStored(stored, raw int64) {
	m.stored.Add(stored)
	m.raw.Add(raw)
}

func newMetrics(s *Store) (*metrics, []prometheus.Collector) {
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "store", Name: name, Help: help})
	}
	m := &metrics{
		conversions: c("conversions_total", "WAL-to-Parquet conversions committed."),
		convertSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "cosmicdust_agent", Subsystem: "store",
			Name: "conversion_seconds", Help: "Time a conversion took.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}}),
		rowsStored: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "store",
			Name: "rows_total", Help: "Rows written to Parquet by conversions, by table."}, []string{"table"}),
		compactions:    c("compactions_total", "Partitions compacted."),
		compactSkipped: c("compactions_skipped_total", "Compactions put off for lack of room for a second copy."),
		bytesCompacted: c("compacted_bytes_total", "Parquet bytes written by compaction."),
		orphans:        c("orphans_removed_total", "Files a crash left between writing and committing, removed at start."),
		synced:         c("synced_files_total", "Files the control plane acknowledged."),
		deleted: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "store",
			Name: "deleted_files_total", Help: "Files deleted for room, by whether they were synced."}, []string{"synced"}),
		droppedRows: c("dropped_rows_total", "Unsynced log rows deleted for room, each file recorded as a logs_dropped event."),
		fullTimes:   c("full_total", "Times reading stopped for lack of room."),
		held: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "cosmicdust_agent", Subsystem: "store", Name: "held",
			Help: "1 while the store has no room and reading waits."}),
	}
	m.deleted.WithLabelValues("true")
	m.deleted.WithLabelValues("false")
	gaugeFunc := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "cosmicdust_agent", Subsystem: "store", Name: name, Help: help}, f)
	}
	counterFunc := func(name, help string, v *atomic.Int64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "store", Name: name, Help: help},
			func() float64 { return float64(v.Load()) })
	}
	return m, []prometheus.Collector{m.conversions, m.convertSeconds, m.rowsStored, m.compactions, m.compactSkipped,
		m.bytesCompacted, m.orphans, m.synced, m.deleted, m.droppedRows, m.fullTimes, m.held,
		counterFunc("bytes_total", "Parquet bytes written by conversions.", &m.stored),
		counterFunc("raw_bytes_total", "The same rows as JSON Lines: the baseline of the compression ratio.", &m.raw),
		gaugeFunc("compression_ratio", "Rows as JSON Lines over their Parquet bytes, for the conversions since start.", func() float64 {
			if st := m.stored.Load(); st > 0 {
				return float64(m.raw.Load()) / float64(st)
			}
			return 0
		}),
	}
}
