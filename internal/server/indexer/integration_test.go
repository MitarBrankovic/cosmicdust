//go:build integration

// The indexer against a real Elasticsearch:
//
//	ES_ADDRESS=http://localhost:9200 go test -tags integration ./internal/server/indexer/
//
// Without one it is skipped, unless ES_REQUIRED is set (CI).
package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/server/archive"
	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/urgent"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

func TestIntegrationElasticsearch(t *testing.T) {
	skip := t.Skip
	if os.Getenv("ES_REQUIRED") != "" {
		skip = t.Fatal
	}
	addr := os.Getenv("ES_ADDRESS")
	if addr == "" {
		skip("ES_ADDRESS not set")
	}
	ctx := context.Background()
	client, err := es.New(addr, es.Options{})
	if err != nil {
		t.Fatal(err)
	}
	version, err := client.Version(ctx)
	if err != nil {
		skip("no Elasticsearch at", addr, err)
	}
	prefix := fmt.Sprintf("it%d", time.Now().UnixNano()%1_000_000)
	t.Cleanup(func() {
		_ = client.DeleteDataStreams(ctx, prefix+"-*")
		_ = client.DeleteIndexTemplates(ctx, prefix+"-*") // after the streams, which use them
	})

	dir := t.TempDir()
	root := filepath.Join(dir, "archive")
	l, err := ledger.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	spool, err := urgent.Open(filepath.Join(dir, "state"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = spool.Close() }()
	ret := configs.DefaultRetention()
	ret.Apps = map[string]configs.Policy{"shop.api": {Logs: configs.Window{Search: configs.Days(3)}}}
	x := New(client, l, spool, root, Options{Prefix: prefix, Retention: ret, Replicas: 0, Log: slog.New(slog.DiscardHandler)})

	const n = 2500
	rows := make([]store.LogRow, n)
	for i := range rows {
		rows[i] = store.LogRow{TS: t0.Add(time.Duration(i) * time.Microsecond), ReadAt: t0, Node: "n1", App: "shop.api",
			ContainerID: "c1", Name: "web", Stream: "stdout", State: "running", Epoch: 1, Source: "file-json",
			Line: fmt.Sprintf("GET /item/%d 200", i), Inode: 1 << 63, Head: 99, Offset: int64(i) * 50}
	}
	logs := archive.Path(api.TableLogs, "shop.api", "dt=2026-10-29/hour=08", "n1-p1.parquet")
	writeParquet(t, filepath.Join(root, logs), rows)
	events := archive.Path(api.TableEvents, "shop.api", "dt=2026-10-29/hour=08", "n1-e1.parquet")
	writeParquet(t, filepath.Join(root, events), []store.EventRow{
		{TS: t0, Node: "n1", App: "shop.api", ContainerID: "c1", Runtime: "docker", Action: "die", Kind: "exit",
			State: "exited", ExitCode: 3, Fields: `{"signal":"15","nested":{"a":1}}`},
	})
	_ = l.Archived("node=n1/logs/dt=2026-10-29/hour=08/p1.parquet", []string{logs, events})
	spool.Add(&api.Event{Node: "n1", Ts: t0.UnixNano(), ContainerId: "c1", Runtime: "docker", Action: "die",
		Kind: "exit", ExitCode: 3, App: "shop.api", SentAt: t0.UnixNano()})

	for range 2 { // the second time adds nothing
		_ = l.Archived("node=n1/logs/dt=2026-10-29/hour=08/p1.parquet", []string{logs, events})
		if err := x.round(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Refresh(ctx, prefix+"-*"); err != nil {
		t.Fatal(err)
	}
	for pattern, want := range map[string]int64{prefix + "-logs-shop.api": n, prefix + "-events-shop.api": 1,
		prefix + "-urgent": 1} {
		got, err := client.Count(ctx, pattern, "")
		if err != nil || got != want {
			t.Errorf("%s: %d documents, %v; want %d", pattern, got, err, want)
		}
	}
	if got, err := client.Count(ctx, prefix+"-logs-*", `message:"/item/42"`); err != nil || got != 1 {
		t.Errorf("full-text search: %d, %v", got, err)
	}
	if x.Stats.Refused.Load() != 0 || x.Stats.Duplicates.Load() != n+1 {
		t.Errorf("refused %d, duplicates %d", x.Stats.Refused.Load(), x.Stats.Duplicates.Load())
	}
	streams, err := client.DataStreams(ctx, prefix+"-*")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range streams {
		want := map[string]string{prefix + "-logs-shop.api": "3d", prefix + "-events-shop.api": "30d",
			prefix + "-urgent": "7d"}[s.Name]
		if s.Lifecycle == nil || !s.Lifecycle.Enabled || s.Lifecycle.DataRetention != want {
			t.Errorf("%s: lifecycle %+v, want retention %s", s.Name, s.Lifecycle, want)
		}
	}
	t.Logf("Elasticsearch %s: %d streams, %d bulks", version, len(streams), x.Stats.Bulks.Load())
}
