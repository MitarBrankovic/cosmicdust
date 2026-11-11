// Package indexer puts the archive into Elasticsearch, where it can be
// searched: each archive file into the data stream of its application and
// table, and the urgent events into a stream of their own.
//
// The node is acknowledged once a file is in the inbox, not once it is
// indexed, so Elasticsearch being down or slow does not reach the nodes: the
// indexer catches up from the archive. Every document is created with an id
// derived from its row, so indexing a file again - after a crash, or a file
// sent twice - adds nothing: Elasticsearch answers 409 and the copy is dropped.
package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/urgent"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

var esDuration = es.FormatDuration

// Indexer works through the archive files the ledger lists as not indexed,
// in the order they were archived, and through the urgent events spool.
type Indexer struct {
	es        *es.Client
	ledger    *ledger.Ledger
	spool     *urgent.Spool
	root      string
	prefix    string
	retention *configs.Retention
	replicas  int
	log       *slog.Logger
	wake      chan struct{}

	setUp      bool
	ready      map[string]bool // data streams created and given their retention
	maxDocs    int
	maxBytes   int
	refusedLog int

	Stats Stats
}

// Stats counts what the indexer did.
type Stats struct {
	Created, Duplicates, Refused, Retried atomic.Int64
	Bulks, Files, Urgent, Unreadable      atomic.Int64
	Up                                    atomic.Bool // the last attempt reached Elasticsearch
}

// Options configure an indexer.
type Options struct {
	Prefix    string             // data streams are <prefix>-logs-<app>, ...
	Retention *configs.Retention // how long each stream keeps its data
	Replicas  int                // replicas of the streams' indices; -1: the cluster's default
	Log       *slog.Logger
}

// New returns an indexer from the archive under root into client.
func New(client *es.Client, l *ledger.Ledger, s *urgent.Spool, root string, o Options) *Indexer {
	if o.Retention == nil {
		o.Retention = configs.DefaultRetention()
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return &Indexer{es: client, ledger: l, spool: s, root: root, prefix: o.Prefix, retention: o.Retention,
		replicas: o.Replicas, log: o.Log, wake: make(chan struct{}, 1), ready: map[string]bool{},
		maxDocs: 5000, maxBytes: 5 << 20}
}

// Wake tells the indexer there is something new.
func (x *Indexer) Wake() {
	select {
	case x.wake <- struct{}{}:
	default:
	}
}

// Run indexes until ctx ends. While Elasticsearch cannot be reached it tries
// again after a pause that doubles up to a minute; the archive keeps growing
// meanwhile, and nothing waits for the index.
func (x *Indexer) Run(ctx context.Context) {
	pause, failing := time.Second, false
	for {
		err := x.round(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			x.Stats.Up.Store(false)
			level := slog.LevelWarn
			if failing {
				level = slog.LevelDebug
			}
			x.log.Log(ctx, level, "cannot index; trying again", "err", err, "in", pause)
			failing = true
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
			pause = min(2*pause, time.Minute)
			continue
		}
		if failing {
			x.log.Info("indexing again")
		}
		x.Stats.Up.Store(true)
		failing, pause = false, time.Second
		select {
		case <-ctx.Done():
			return
		case <-x.wake:
		case <-time.After(30 * time.Second):
		}
	}
}

func (x *Indexer) round(ctx context.Context) error {
	if !x.setUp {
		if err := x.setup(ctx); err != nil {
			return err
		}
		x.setUp = true
	}
	if err := x.urgent(ctx); err != nil {
		return err
	}
	return x.files(ctx)
}

// setup puts the index templates and gives the streams that exist the
// retention the configuration now says - it may have changed since they were
// created.
func (x *Indexer) setup(ctx context.Context) error {
	def := x.retention.Default
	for name, t := range map[string]map[string]any{
		x.prefix + "-logs":   template(x.prefix+"-logs-*", def.Logs.Search.D(), x.replicas, logMappings()),
		x.prefix + "-events": template(x.prefix+"-events-*", def.Events.Search.D(), x.replicas, eventMappings(false)),
		x.prefix + "-urgent": template(UrgentStream(x.prefix), x.retention.Urgent.D(), x.replicas, eventMappings(true)),
	} {
		if err := x.es.PutIndexTemplate(ctx, name, t); err != nil {
			return fmt.Errorf("index template %s: %w", name, err)
		}
	}
	streams, err := x.es.DataStreams(ctx, x.prefix+"-*")
	if err != nil {
		return err
	}
	for _, s := range streams {
		retention, ok := x.retentionOf(s.Name)
		if !ok {
			continue
		}
		if err := x.es.PutLifecycle(ctx, s.Name, retention); err != nil {
			return fmt.Errorf("retention of %s: %w", s.Name, err)
		}
		x.ready[s.Name] = true
	}
	return nil
}

// retentionOf is how long a stream keeps its data, by its name: the
// application in the name is matched against the configured ones.
func (x *Indexer) retentionOf(stream string) (time.Duration, bool) {
	if stream == UrgentStream(x.prefix) {
		return x.retention.Urgent.D(), true
	}
	rest, ok := strings.CutPrefix(stream, x.prefix+"-")
	if !ok {
		return 0, false
	}
	table, app, ok := strings.Cut(rest, "-")
	if !ok || (table != api.TableLogs && table != api.TableEvents) {
		return 0, false
	}
	for configured := range x.retention.Apps {
		if streamApp(configured) == app {
			return x.retention.For(configured, table).Search.D(), true
		}
	}
	return x.retention.For("", table).Search.D(), true
}

// ensure creates a data stream and gives it its retention, once.
func (x *Indexer) ensure(ctx context.Context, stream string) error {
	if x.ready[stream] {
		return nil
	}
	if err := x.es.CreateDataStream(ctx, stream); err != nil {
		return fmt.Errorf("create %s: %w", stream, err)
	}
	if retention, ok := x.retentionOf(stream); ok {
		if err := x.es.PutLifecycle(ctx, stream, retention); err != nil {
			return fmt.Errorf("retention of %s: %w", stream, err)
		}
	}
	x.ready[stream] = true
	return nil
}

// errFlush marks a failure to index, as opposed to one to read the archive.
type errFlush struct{ err error }

func (e errFlush) Error() string { return e.err.Error() }
func (e errFlush) Unwrap() error { return e.err }

func (x *Indexer) files(ctx context.Context) error {
	pending := x.ledger.PendingIndex()
	if len(pending) == 0 {
		return nil
	}
	b := &batch{}
	for _, rel := range pending {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		table, app, _, _, err := archive.ParsePath(rel)
		if err != nil {
			x.log.Error("not an archive file; skipped", "file", rel, "err", err)
			_ = x.ledger.Forget(rel)
			continue
		}
		path := filepath.Join(x.root, filepath.FromSlash(rel))
		if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
			// The retention removed it before it was indexed: Elasticsearch
			// was away for longer than the archive keeps logs.
			_ = x.ledger.Forget(rel)
			continue
		}
		stream := StreamName(x.prefix, table, app)
		if err := x.ensure(ctx, stream); err != nil {
			return err
		}
		mark := b.mark()
		switch table {
		case api.TableLogs:
			err = archive.ReadRows(path, 1024, func(rows []store.LogRow) error {
				for i := range rows {
					if err := x.add(ctx, b, stream, LogID(&rows[i]), logDocOf(&rows[i])); err != nil {
						return err
					}
				}
				return nil
			})
		case api.TableEvents:
			err = archive.ReadRows(path, 1024, func(rows []store.EventRow) error {
				for i := range rows {
					r := &rows[i]
					id := EventID(r.Node, r.ContainerID, r.Runtime, r.Action, r.TS.UnixNano(), r.Epoch)
					if err := x.add(ctx, b, stream, id, eventDocOf(r)); err != nil {
						return err
					}
				}
				return nil
			})
		}
		var flushErr errFlush
		switch {
		case errors.As(err, &flushErr):
			return flushErr.err
		case err != nil:
			// An archive file that cannot be read stays in the archive; its
			// rows that were read are indexed, the rest are not.
			x.Stats.Unreadable.Add(1)
			x.log.Error("cannot read an archive file; it stays unindexed", "file", rel, "err", err)
			b.rewind(mark)
			_ = x.ledger.Forget(rel)
			continue
		}
		b.done = append(b.done, rel)
	}
	return x.flush(ctx, b)
}

// add puts a document into the batch, and sends the batch once it is full.
func (x *Indexer) add(ctx context.Context, b *batch, stream, id string, doc any) error {
	if err := b.add(stream, id, doc); err != nil {
		return err
	}
	if len(b.spans) >= x.maxDocs || b.buf.Len() >= x.maxBytes {
		if err := x.flush(ctx, b); err != nil {
			return errFlush{err}
		}
	}
	return nil
}

func (x *Indexer) urgent(ctx context.Context) error {
	if x.spool == nil {
		return nil
	}
	stream := UrgentStream(x.prefix)
	for {
		evs, end, err := x.spool.Read(500)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			return nil
		}
		if err := x.ensure(ctx, stream); err != nil {
			return err
		}
		b := &batch{}
		for _, sp := range evs {
			e := sp.Event
			if err := b.add(stream, EventID(e.Node, e.ContainerId, e.Runtime, e.Action, e.Ts, e.Epoch),
				urgentDocOf(e, time.Unix(0, sp.ReceivedAt))); err != nil {
				return err
			}
		}
		if err := x.flush(ctx, b); err != nil {
			return err
		}
		if err := x.spool.Done(end); err != nil {
			return err
		}
		x.Stats.Urgent.Add(int64(len(evs)))
	}
}

// flush sends the batch. Documents Elasticsearch could not take for now (429,
// 5xx) go again, a few times; documents it refused for good (a mapping error)
// are counted and dropped, so that one bad row cannot stop the index. The
// files the batch completed are then recorded as indexed.
func (x *Indexer) flush(ctx context.Context, b *batch) error {
	body, spans := b.buf.Bytes(), b.spans
	for attempt := 1; len(spans) > 0; attempt++ {
		items, err := x.es.Bulk(ctx, body)
		x.Stats.Bulks.Add(1)
		if err != nil {
			return err
		}
		if len(items) != len(spans) {
			return fmt.Errorf("bulk: %d answers to %d documents", len(items), len(spans))
		}
		var retry []span
		for i, it := range items {
			switch {
			case it.Status == 200 || it.Status == 201:
				x.Stats.Created.Add(1)
			case it.Status == 409:
				x.Stats.Duplicates.Add(1)
			case it.Status == 429 || it.Status >= 500:
				retry = append(retry, spans[i])
			default:
				x.Stats.Refused.Add(1)
				level := slog.LevelWarn
				if x.refusedLog++; x.refusedLog > 10 {
					level = slog.LevelDebug
				}
				x.log.Log(ctx, level, "Elasticsearch refused a document; dropped", "status", it.Status, "type",
					it.Type, "reason", it.Reason)
			}
		}
		if len(retry) == 0 {
			break
		}
		if attempt == 8 {
			return fmt.Errorf("bulk: %d documents still not taken after %d attempts", len(retry), attempt)
		}
		x.Stats.Retried.Add(int64(len(retry)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
		}
		var next bytes.Buffer
		spans = spans[:0:0]
		for _, s := range retry {
			start := next.Len()
			next.Write(body[s.start:s.end])
			spans = append(spans, span{start, next.Len()})
		}
		body = next.Bytes()
	}
	if err := x.ledger.Indexed(b.done...); err != nil {
		return err
	}
	x.Stats.Files.Add(int64(len(b.done)))
	b.reset()
	return nil
}

// batch is a bulk request being built: an action and a document per row.
type batch struct {
	buf     bytes.Buffer
	spans   []span   // each action with its document, for sending some again
	done    []string // archive files all of whose documents are in this or an earlier batch
	flushes int
}

type span struct{ start, end int }

type action struct {
	Create struct {
		Index string `json:"_index"`
		ID    string `json:"_id"`
	} `json:"create"`
}

func (b *batch) add(stream, id string, doc any) error {
	start := b.buf.Len()
	var a action
	a.Create.Index, a.Create.ID = stream, id
	enc := json.NewEncoder(&b.buf) // Encode ends each value with the newline the bulk API wants
	enc.SetEscapeHTML(false)
	if err := enc.Encode(a); err != nil {
		b.buf.Truncate(start)
		return err
	}
	if err := enc.Encode(doc); err != nil {
		b.buf.Truncate(start)
		return err
	}
	b.spans = append(b.spans, span{start, b.buf.Len()})
	return nil
}

// mark and rewind drop what one file added, when it turns out unreadable.
type mark struct{ bytes, spans, flushes int }

func (b *batch) mark() mark { return mark{b.buf.Len(), len(b.spans), b.flushes} }

func (b *batch) rewind(m mark) {
	if m.flushes != b.flushes {
		// The batch was sent while the file was read: what it holds now is
		// all the file's.
		m.bytes, m.spans = 0, 0
	}
	b.buf.Truncate(m.bytes)
	b.spans = b.spans[:m.spans]
}

func (b *batch) reset() {
	b.buf.Reset()
	b.spans = b.spans[:0]
	b.done = b.done[:0]
	b.flushes++
}
