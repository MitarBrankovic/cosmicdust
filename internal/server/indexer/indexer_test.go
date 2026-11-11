package indexer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/urgent"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// fakeES answers the requests the indexer makes, keeps the documents by
// stream and id, and can refuse: tooMany items once each with 429, or
// everything with 503 while down.
type fakeES struct {
	mu         sync.Mutex
	docs       map[string]map[string]json.RawMessage // stream -> id -> document
	templates  map[string]bool
	lifecycles map[string]string
	tooMany    int
	down       bool
	bulks      int
}

func newFakeES() *fakeES {
	return &fakeES{docs: map[string]map[string]json.RawMessage{}, templates: map[string]bool{},
		lifecycles: map[string]string{}}
}

func (f *fakeES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		http.Error(w, `{"error":{"type":"unavailable","reason":"down"}}`, http.StatusServiceUnavailable)
		return
	}
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_index_template/"):
		f.templates[strings.TrimPrefix(r.URL.Path, "/_index_template/")] = true
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/_lifecycle"):
		var l struct {
			Retention string `json:"data_retention"`
		}
		_ = json.Unmarshal(body, &l)
		f.lifecycles[strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/_data_stream/"), "/_lifecycle")] = l.Retention
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_data_stream/"):
		name := strings.TrimPrefix(r.URL.Path, "/_data_stream/")
		if f.docs[name] != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"type":"resource_already_exists_exception","reason":"exists"}}`)
			return
		}
		f.docs[name] = map[string]json.RawMessage{}
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_data_stream/"):
		var out struct {
			DataStreams []map[string]string `json:"data_streams"`
		}
		for name := range f.docs {
			out.DataStreams = append(out.DataStreams, map[string]string{"name": name})
		}
		_ = json.NewEncoder(w).Encode(out)
	case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
		f.bulks++
		var items []string
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var a action
			if err := json.Unmarshal(sc.Bytes(), &a); err != nil || !sc.Scan() {
				http.Error(w, "bad bulk", http.StatusBadRequest)
				return
			}
			doc := json.RawMessage(append([]byte(nil), sc.Bytes()...))
			stream := f.docs[a.Create.Index]
			switch {
			case stream == nil:
				items = append(items, `{"create":{"status":404,"error":{"type":"index_not_found_exception","reason":"no stream"}}}`)
			case f.tooMany > 0:
				f.tooMany--
				items = append(items, `{"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"busy"}}}`)
			case stream[a.Create.ID] != nil:
				items = append(items, `{"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"exists"}}}`)
			default:
				stream[a.Create.ID] = doc
				items = append(items, `{"create":{"status":201}}`)
			}
		}
		_, _ = fmt.Fprintf(w, `{"errors":true,"items":[%s]}`, strings.Join(items, ","))
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
	}
}

func (f *fakeES) count(stream string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.docs[stream])
}

type fixture struct {
	root   string
	ledger *ledger.Ledger
	spool  *urgent.Spool
	es     *fakeES
	x      *Indexer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	fx := &fixture{root: filepath.Join(dir, "archive"), es: newFakeES()}
	var err error
	if fx.ledger, err = ledger.Open(filepath.Join(dir, "state")); err != nil {
		t.Fatal(err)
	}
	if fx.spool, err = urgent.Open(filepath.Join(dir, "state"), nil); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(fx.es)
	t.Cleanup(func() {
		srv.Close()
		_ = fx.spool.Close()
		_ = fx.ledger.Close()
	})
	client, err := es.New(srv.URL, es.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ret := configs.DefaultRetention()
	ret.Apps = map[string]configs.Policy{"shop.api": {Logs: configs.Window{Search: configs.Days(3)}}}
	fx.x = New(client, fx.ledger, fx.spool, fx.root, Options{Prefix: "cosmicdust", Retention: ret, Replicas: -1,
		Log: slog.New(slog.DiscardHandler)})
	return fx
}

var t0 = time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)

// archiveLogs writes an archive file of n lines of app and records it as
// archived, waiting for the index.
func (fx *fixture) archiveLogs(t *testing.T, app, name string, n int) string {
	t.Helper()
	rows := make([]store.LogRow, n)
	for i := range rows {
		rows[i] = store.LogRow{TS: t0.Add(time.Duration(i)), Node: "n1", App: app, ContainerID: "c-" + name,
			Line: fmt.Sprintf("%s line %d", name, i), Inode: 11, Head: 22, Offset: int64(i) * 100}
	}
	rel := archive.Path(api.TableLogs, app, "dt=2026-10-29/hour=08", "n1-"+name)
	writeParquet(t, filepath.Join(fx.root, rel), rows)
	if err := fx.ledger.Archived("node=n1/logs/dt=2026-10-29/hour=08/"+name, []string{rel}); err != nil {
		t.Fatal(err)
	}
	return rel
}

func writeParquet[T archive.Row](t *testing.T, path string, rows []T) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[T](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

func TestIndexesTheArchiveOnce(t *testing.T) {
	fx := newFixture(t)
	fx.x.maxDocs = 700 // several bulks, flushed in the middle of files
	fx.archiveLogs(t, "shop.api", "p1.parquet", 1000)
	fx.archiveLogs(t, "Billing Service", "p1.parquet", 500)
	events := []store.EventRow{
		{TS: t0, Node: "n1", App: "shop.api", ContainerID: "c1", Runtime: "docker", Action: "die", Kind: "exit",
			ExitCode: 3, Fields: `{"signal":"9"}`},
		{TS: t0, Node: "cosmicdust-node", Runtime: "cosmicdust", Action: "logs_dropped", ExitCode: -1, Fields: "not json"},
	}
	rel := archive.Path(api.TableEvents, "shop.api", "dt=2026-10-29/hour=08", "n1-e1.parquet")
	writeParquet(t, filepath.Join(fx.root, rel), events)
	_ = fx.ledger.Archived("node=n1/events/dt=2026-10-29/hour=08/e1.parquet", []string{rel})

	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	for stream, want := range map[string]int{"cosmicdust-logs-shop.api": 1000, "cosmicdust-logs-billing-service": 500,
		"cosmicdust-events-shop.api": 2} {
		if got := fx.es.count(stream); got != want {
			t.Errorf("%s: %d documents, want %d", stream, got, want)
		}
	}
	if p := fx.ledger.PendingIndex(); len(p) != 0 {
		t.Fatalf("still pending: %v", p)
	}
	for _, name := range []string{"cosmicdust-logs", "cosmicdust-events", "cosmicdust-urgent"} {
		if !fx.es.templates[name] {
			t.Errorf("template %s not put", name)
		}
	}
	for stream, want := range map[string]string{"cosmicdust-logs-shop.api": "3d", "cosmicdust-logs-billing-service": "7d",
		"cosmicdust-events-shop.api": "30d"} {
		if got := fx.es.lifecycles[stream]; got != want {
			t.Errorf("%s keeps %q, want %q", stream, got, want)
		}
	}
	// A document carries what Kibana shows: the time, the line, where it came from.
	for _, doc := range fx.es.docs["cosmicdust-events-shop.api"] {
		var d map[string]any
		_ = json.Unmarshal(doc, &d)
		switch d["action"] {
		case "die":
			if d["exit_code"] != 3.0 || d["fields"].(map[string]any)["signal"] != "9" {
				t.Errorf("die event %v", d)
			}
		case "logs_dropped":
			if _, ok := d["exit_code"]; ok || d["fields"] != nil {
				t.Errorf("logs_dropped event %v", d)
			}
		}
	}

	// The same files again - a crash after indexing and before the ledger
	// knew - add nothing.
	fx.archiveLogs(t, "shop.api", "p1.parquet", 1000)
	before := fx.x.Stats.Created.Load()
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fx.x.Stats.Created.Load() != before || fx.x.Stats.Duplicates.Load() != 1000 || fx.es.count("cosmicdust-logs-shop.api") != 1000 {
		t.Fatalf("created %d, duplicates %d, documents %d", fx.x.Stats.Created.Load()-before,
			fx.x.Stats.Duplicates.Load(), fx.es.count("cosmicdust-logs-shop.api"))
	}
}

// Documents Elasticsearch cannot take for now go again; while it is down
// nothing is recorded as indexed, and once it is back everything is.
func TestBusyOrDownElasticsearch(t *testing.T) {
	fx := newFixture(t)
	fx.archiveLogs(t, "a", "p1.parquet", 300)
	fx.es.tooMany = 120
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fx.es.count("cosmicdust-logs-a") != 300 || fx.x.Stats.Retried.Load() != 120 {
		t.Fatalf("documents %d, retried %d", fx.es.count("cosmicdust-logs-a"), fx.x.Stats.Retried.Load())
	}

	fx.archiveLogs(t, "a", "p2.parquet", 200)
	fx.es.down = true
	if err := fx.x.round(context.Background()); err == nil {
		t.Fatal("no error while Elasticsearch is down")
	}
	if p := fx.ledger.PendingIndex(); len(p) != 1 {
		t.Fatalf("pending while down: %v", p)
	}
	fx.es.down = false
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fx.es.count("cosmicdust-logs-a") != 500 || len(fx.ledger.PendingIndex()) != 0 {
		t.Fatalf("documents %d, pending %v", fx.es.count("cosmicdust-logs-a"), fx.ledger.PendingIndex())
	}
}

func TestUrgentEvents(t *testing.T) {
	fx := newFixture(t)
	ts := t0.UnixNano()
	e := &api.Event{Node: "n1", Ts: ts, ContainerId: "c1", Runtime: "containerd", Action: "/tasks/exit", Kind: "exit",
		ExitCode: 137, OomKilled: true, App: "shop.api", SentAt: ts + 1000}
	fx.spool.Add(e)
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Sent again after a restart of the service, which forgot it had it.
	_ = fx.spool.Close()
	spool, err := urgent.Open(filepath.Dir(filepath.Join(fx.root, "..", "state", "x")), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = spool.Close() }()
	spool.Add(e)
	fx.x.spool = spool
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fx.es.count("cosmicdust-urgent") != 1 || fx.es.lifecycles["cosmicdust-urgent"] != "7d" {
		t.Fatalf("urgent documents %d, retention %q", fx.es.count("cosmicdust-urgent"), fx.es.lifecycles["cosmicdust-urgent"])
	}
	for _, doc := range fx.es.docs["cosmicdust-urgent"] {
		var d map[string]any
		_ = json.Unmarshal(doc, &d)
		if d["received_at"] == nil || d["sent_at"] == nil || d["oom_killed"] != true {
			t.Fatalf("urgent document %v", d)
		}
	}
}

// An archive file that cannot be read is skipped, and does not hold up the
// files behind it; one the retention removed is forgotten.
func TestUnreadableOrRemovedArchiveFiles(t *testing.T) {
	fx := newFixture(t)
	bad := archive.Path(api.TableLogs, "a", "dt=2026-10-29/hour=08", "n1-bad.parquet")
	_ = os.MkdirAll(filepath.Join(fx.root, filepath.Dir(bad)), 0o755)
	if err := os.WriteFile(filepath.Join(fx.root, bad), []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = fx.ledger.Archived("node=n1/logs/dt=2026-10-29/hour=08/bad.parquet", []string{bad})
	_ = fx.ledger.Archived("node=n1/logs/dt=2026-10-29/hour=08/gone.parquet",
		[]string{archive.Path(api.TableLogs, "a", "dt=2026-10-29/hour=08", "n1-gone.parquet")})
	fx.archiveLogs(t, "a", "good.parquet", 10)
	if err := fx.x.round(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fx.es.count("cosmicdust-logs-a") != 10 || fx.x.Stats.Unreadable.Load() != 1 || len(fx.ledger.PendingIndex()) != 0 {
		t.Fatalf("documents %d, unreadable %d, pending %v", fx.es.count("cosmicdust-logs-a"), fx.x.Stats.Unreadable.Load(),
			fx.ledger.PendingIndex())
	}
}

func TestNamesAndIDs(t *testing.T) {
	for app, want := range map[string]string{"shop.api": "cosmicdust-logs-shop.api", "Billing Service": "cosmicdust-logs-billing-service",
		"": "cosmicdust-logs-_unknown", "org/ns/app": "cosmicdust-logs-org-ns-app"} {
		if got := StreamName("cosmicdust", "logs", app); got != want {
			t.Errorf("%q: %q, want %q", app, got, want)
		}
	}
	if long := StreamName("cosmicdust", "logs", strings.Repeat("x", 300)); len(long) > 255 {
		t.Errorf("a stream name of %d bytes", len(long))
	}
	a := store.LogRow{Node: "n1", ContainerID: "c", Inode: 1, Head: 2, Offset: 3, Line: "x"}
	b := a
	b.Line, b.TS = "another text, same place", t0
	if LogID(&a) != LogID(&b) {
		t.Error("the id depends on more than the position")
	}
	b.Offset = 4
	if LogID(&a) == LogID(&b) {
		t.Error("two positions, one id")
	}
	if es.FormatDuration(36*time.Hour) != "36h" || es.FormatDuration(7*24*time.Hour) != "7d" {
		t.Error("durations for Elasticsearch")
	}
}
