package lifecycle

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

type metrics struct {
	events     *prometheus.CounterVec // runtime, kind: as the sources sent them
	inferred   *prometheus.CounterVec // runtime, kind
	late       *prometheus.CounterVec // runtime
	resyncs    *prometheus.CounterVec // runtime, reason
	reconciles *prometheus.CounterVec // runtime, outcome
	reconcileS *prometheus.HistogramVec
	oomKills   *prometheus.CounterVec // read: watch, event, parent
	replayed   prometheus.Counter
	snapshots  *prometheus.CounterVec // outcome
}

func newMetrics(t *Tracker) (*metrics, []prometheus.Collector) {
	ns, sub := "cosmicdust_agent", "lifecycle"
	m := &metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "events_total",
			Help: "Events the runtimes sent, by runtime and kind."}, []string{"runtime", "kind"}),
		inferred: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "inferred_events_total",
			Help: "Events derived by reconciliation, by runtime and kind."}, []string{"runtime", "kind"}),
		late: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "late_events_total",
			Help: "Events stamped before the one that set the state they met; recorded, not applied."}, []string{"runtime"}),
		resyncs: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "resyncs_total",
			Help: "Connects of an event stream, by runtime and reason (start, reconnect)."}, []string{"runtime", "reason"}),
		reconciles: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "reconciliations_total",
			Help: "Listings compared with the tracked state, by runtime and outcome."}, []string{"runtime", "outcome"}),
		reconcileS: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "listing_seconds",
			Help:    "Time a runtime took to list its containers.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}}, []string{"runtime"}),
		oomKills: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "oom_kills_total",
			Help: "Incarnations the kernel's counter says the OOM killer hit, by where it was read: the cgroup watch, at a runtime's exit or OOM event, or the parent cgroup's, for a cgroup gone by the exit."},
			[]string{"read"}),
		replayed: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "replayed_events_total",
			Help: "Events read back from the WAL at the start and applied again, for what the snapshot did not have yet."}),
		snapshots: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "snapshot_writes_total",
			Help: "Writes of the tracked state to disk, by outcome."}, []string{"outcome"}),
	}
	watches := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "cgroup_watches",
		Help: "Cgroups whose memory.events is watched for OOM kills."}, func() float64 {
		t.mu.RLock()
		w := t.oom
		t.mu.RUnlock()
		if w == nil {
			return 0
		}
		return float64(w.len())
	})
	return m, []prometheus.Collector{m.events, m.inferred, m.late, m.resyncs, m.reconciles, m.reconcileS,
		m.oomKills, watches, m.replayed, m.snapshots, containersCollector{t}}
}

// containersCollector counts tracked containers by state at scrape time, so
// nothing is kept up to date between scrapes.
type containersCollector struct{ t *Tracker }

var containersDesc = prometheus.NewDesc("cosmicdust_agent_lifecycle_containers",
	"Tracked containers, by merged state.", []string{"state"}, nil)

func (c containersCollector) Describe(ch chan<- *prometheus.Desc) { ch <- containersDesc }

func (c containersCollector) Collect(ch chan<- prometheus.Metric) {
	counts := map[cosmicdust.State]int{}
	c.t.mu.RLock()
	for _, ct := range c.t.containers {
		counts[merged(ct.Views).State]++
	}
	c.t.mu.RUnlock()
	for _, s := range []cosmicdust.State{cosmicdust.StateUnknown, cosmicdust.StateCreated, cosmicdust.StateRunning, cosmicdust.StateExited, cosmicdust.StateRestarting, cosmicdust.StateRemoved} {
		ch <- prometheus.MustNewConstMetric(containersDesc, prometheus.GaugeValue, float64(counts[s]), string(s))
	}
}
