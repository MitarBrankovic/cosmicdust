package syncer

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	rounds        prometheus.Counter
	roundSeconds  prometheus.Histogram
	uploadSeconds prometheus.Histogram
	files         *prometheus.CounterVec
	bytes         prometheus.Counter
	failures      prometheus.Counter
	reconnects    prometheus.Counter
	backlogFiles  prometheus.Gauge
	backlogBytes  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, lag func() float64) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: "cosmicdust_agent", Subsystem: "sync", Name: name, Help: help}
	}
	buckets := []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}
	m := &metrics{
		rounds: prometheus.NewCounter(prometheus.CounterOpts(opts("rounds_total", "Sync rounds."))),
		roundSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "cosmicdust_agent", Subsystem: "sync",
			Name: "round_seconds", Help: "Time a round took.", Buckets: buckets}),
		uploadSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "cosmicdust_agent", Subsystem: "sync",
			Name: "upload_seconds", Help: "Time one file took to be answered.", Buckets: buckets}),
		files: prometheus.NewCounterVec(prometheus.CounterOpts(opts("files_total",
			"Files answered by the control plane, by answer: stored, duplicate (stored before) or rejected.")), []string{"result"}),
		bytes: prometheus.NewCounter(prometheus.CounterOpts(opts("bytes_total", "Bytes of the files stored."))),
		failures: prometheus.NewCounter(prometheus.CounterOpts(opts("failures_total",
			"Uploads that did not reach an answer; each ends its round."))),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts(opts("reconnects_total",
			"Rounds started early, after failed ones, because the transport's connection came back."))),
		backlogFiles: prometheus.NewGauge(prometheus.GaugeOpts(opts("backlog_files", "Files left unsynced by the last round."))),
		backlogBytes: prometheus.NewGauge(prometheus.GaugeOpts(opts("backlog_bytes", "Their bytes."))),
	}
	for _, r := range []string{"stored", "duplicate", "rejected"} {
		m.files.WithLabelValues(r)
	}
	lagGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts("lag_seconds",
		"Age of the oldest row in a file known to wait for the control plane; 0 when none does.")), lag)
	return m, register(reg, m.rounds, m.roundSeconds, m.uploadSeconds, m.files, m.bytes, m.failures,
		m.reconnects, m.backlogFiles, m.backlogBytes, lagGauge)
}

type urgentMetrics struct {
	result  *prometheus.CounterVec
	latency prometheus.Histogram
}

func newUrgentMetrics(reg prometheus.Registerer) (*urgentMetrics, error) {
	m := &urgentMetrics{
		result: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "sync", Name: "urgent_total",
			Help: "Urgent events, by result: stored, duplicate, rejected, failed (given up after waiting too long), dropped (queue full), or stopped (an exit Docker was asked for, not sent)."},
			[]string{"result"}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "cosmicdust_agent", Subsystem: "sync",
			Name: "urgent_latency_seconds", Help: "From the runtime's time of an urgent event to its answer.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 600}}),
	}
	for _, r := range []string{"stored", "duplicate", "rejected", "failed", "dropped", "stopped"} {
		m.result.WithLabelValues(r)
	}
	return m, register(reg, m.result, m.latency)
}

func register(reg prometheus.Registerer, cs ...prometheus.Collector) error {
	if reg == nil {
		return nil
	}
	for _, c := range cs {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("syncer: register metrics: %w", err)
		}
	}
	return nil
}
