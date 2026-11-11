// Package metrics exposes what the service's parts count, for Prometheus:
// what arrived, what waits for the archive and the index, and whether
// Elasticsearch answers.
package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/indexer"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/query"
	"github.com/c12s/cosmicdust/internal/server/receiver"
	"github.com/c12s/cosmicdust/internal/server/retention"
	"github.com/c12s/cosmicdust/internal/server/urgent"
)

// Parts are the service's parts that count something. Indexer is nil without
// Elasticsearch.
type Parts struct {
	Receiver *receiver.Receiver
	Spool    *urgent.Spool
	Archiver *archive.Archiver
	Indexer  *indexer.Indexer
	Cleaner  *retention.Cleaner
	Ledger   *ledger.Ledger
	Query    *query.Service
}

const namespace = "cosmicdust_server"

// Register adds the parts' metrics to reg.
func Register(reg prometheus.Registerer, p Parts) error {
	var cs []prometheus.Collector
	counter := func(name, help string, v *atomic.Int64, labels prometheus.Labels) {
		cs = append(cs, prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help,
			ConstLabels: labels}, func() float64 { return float64(v.Load()) }))
	}
	gauge := func(name, help string, f func() float64) {
		cs = append(cs, prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, f))
	}

	r := &p.Receiver.Stats
	counter("files_received_total", "Files received whole and recorded; their senders were told they are stored.", &r.FilesStored, nil)
	counter("bytes_received_total", "Bytes of the files received.", &r.BytesStored, nil)
	counter("files_duplicate_total", "Uploads of files already received, answered as duplicates.", &r.Duplicates, nil)
	counter("files_conflict_total", "Files received again under a known name with other content; the new content replaced it.", &r.Conflicts, nil)
	counter("uploads_resumed_total", "Probes that let an upload that broke off continue.", &r.Resumed, nil)
	counter("chunks_refused_total", "Chunks refused, by reason.", &r.BadOffsets, prometheus.Labels{"reason": "offset"})
	counter("chunks_refused_total", "Chunks refused, by reason.", &r.Superseded, prometheus.Labels{"reason": "superseded"})
	counter("chunks_refused_total", "Chunks refused, by reason.", &r.Refused, prometheus.Labels{"reason": "reference"})
	counter("files_crc_failures_total", "Files whose size or CRC differed from what their sender declared.", &r.CRCFailures, nil)
	gauge("uploads_in_progress", "Uploads begun and not finished.", func() float64 { return float64(p.Receiver.Uploads()) })

	s := &p.Spool.Stats
	counter("urgent_events_total", "Urgent events received.", &s.Events, nil)
	counter("urgent_events_duplicate_total", "Urgent events received again.", &s.Duplicates, nil)
	gauge("urgent_backlog_bytes", "Bytes of urgent events not in the index yet.", func() float64 { return float64(p.Spool.Backlog()) })

	a := &p.Archiver.Stats
	counter("archive_files_total", "Received files moved into the archive.", &a.Files, nil)
	counter("archive_rows_total", "Rows moved into the archive.", &a.Rows, nil)
	counter("archive_outputs_total", "Archive files written: one per application of each received file.", &a.Outputs, nil)
	counter("archive_failures_total", "Attempts to archive that failed and are tried again.", &a.Failures, nil)
	counter("archive_quarantined_total", "Received files that were not Parquet files of their table, set aside.", &a.Quarantined, nil)
	counter("archive_compactions_total", "Hours of an application whose archive files were merged into one.", &a.Compactions, nil)
	counter("archive_files_merged_total", "Archive files merged into others and removed.", &a.Compacted, nil)
	gauge("archive_backlog_files", "Received files waiting for the archive.", func() float64 { return float64(len(p.Ledger.PendingArchive())) })

	if x := p.Indexer; x != nil {
		st := &x.Stats
		counter("index_documents_total", "Documents sent to Elasticsearch, by outcome.", &st.Created, prometheus.Labels{"result": "created"})
		counter("index_documents_total", "Documents sent to Elasticsearch, by outcome.", &st.Duplicates, prometheus.Labels{"result": "duplicate"})
		counter("index_documents_total", "Documents sent to Elasticsearch, by outcome.", &st.Refused, prometheus.Labels{"result": "refused"})
		counter("index_retried_total", "Documents Elasticsearch could not take at once and that were sent again.", &st.Retried, nil)
		counter("index_bulks_total", "Bulk requests sent.", &st.Bulks, nil)
		counter("index_files_total", "Archive files indexed.", &st.Files, nil)
		counter("index_unreadable_total", "Archive files that could not be read and stay unindexed.", &st.Unreadable, nil)
		counter("index_urgent_total", "Urgent events indexed.", &st.Urgent, nil)
		gauge("index_backlog_files", "Archive files waiting for the index.", func() float64 { return float64(len(p.Ledger.PendingIndex())) })
		gauge("elasticsearch_up", "1 if the last attempt to index reached Elasticsearch.", func() float64 {
			if st.Up.Load() {
				return 1
			}
			return 0
		})
	}

	c := &p.Cleaner.Stats
	counter("retention_hours_removed_total", "Hours of an application's archive removed past its retention.", &c.Partitions, nil)
	counter("retention_sweeps_total", "Sweeps of the archive.", &c.Sweeps, nil)

	if q := p.Query; q != nil {
		counter("queries_total", "Calls of the query API.", &q.Stats.Requests, nil)
		counter("query_failures_total", "Calls of the query API answered with an error, a bad request included.", &q.Stats.Failures, nil)
	}

	for _, col := range cs {
		if err := reg.Register(col); err != nil {
			return err
		}
	}
	return nil
}
