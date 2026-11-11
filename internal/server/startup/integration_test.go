//go:build integration

// The whole service against a real Elasticsearch: files in over gRPC, read
// back through the query API.
//
//	ES_ADDRESS=http://localhost:9200 go test -tags integration -run Integration ./internal/server/startup/
//
// Without one it is skipped, unless ES_REQUIRED is set (CI).
package startup

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

func parquetOf[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestIntegrationQuery(t *testing.T) {
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
	if _, err := client.Version(ctx); err != nil {
		skip("no Elasticsearch at", addr, err)
	}
	prefix := fmt.Sprintf("itq%d", time.Now().UnixNano()%1_000_000)
	t.Cleanup(func() {
		_ = client.DeleteDataStreams(ctx, prefix+"-*")
		_ = client.DeleteIndexTemplates(ctx, prefix+"-*") // after the streams, which use them
	})

	app := start(t, &configs.Config{SubjectPrefix: "cosmicdust", GrpcAddress: "127.0.0.1:0", HttpAddress: "127.0.0.1:0",
		DataDir: t.TempDir(), EsAddress: addr, EsIndexPrefix: prefix, EsReplicas: 0, PartialIdle: time.Minute,
		CleanupEvery: time.Hour, LogLevel: "error", Retention: configs.DefaultRetention()})
	defer app.GracefulStop(ctx)

	// Node n1 runs a web container (1,200 lines on stdout) and a database
	// (300 lines on stderr, ten minutes later); node n2 one container.
	const web, db, other = 1200, 300, 50
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	read := t0.Add(30 * time.Minute) // when the agent read the lines: it always sets it
	var n1, n2 []store.LogRow
	for i := range web {
		n1 = append(n1, store.LogRow{TS: t0.Add(time.Duration(i) * time.Millisecond), ReadAt: read, Node: "n1", App: "shop",
			ContainerID: "c-web", Name: "web", Image: "shop:1", Stream: "stdout", State: "running", Epoch: 1,
			Line: fmt.Sprintf("web %d GET /item/%d 200", i, i), Inode: 7, Head: 8, Offset: int64(i) * 40})
	}
	for i := range db {
		n1 = append(n1, store.LogRow{TS: t0.Add(10*time.Minute + time.Duration(i)*time.Millisecond), ReadAt: read, Node: "n1",
			App: "shop", ContainerID: "c-db", Name: "db", Image: "postgres:17", Stream: "stderr", State: "running",
			Epoch: 1, Line: fmt.Sprintf("db checkpoint %d", i), Inode: 9, Head: 10, Offset: int64(i) * 30})
	}
	for i := range other {
		n2 = append(n2, store.LogRow{TS: t0.Add(time.Duration(i) * time.Second), ReadAt: read, Node: "n2", App: "billing",
			ContainerID: "c-bill", Name: "billing", Stream: "stdout", Line: fmt.Sprintf("invoice %d", i), Inode: 1,
			Head: 2, Offset: int64(i) * 20})
	}
	events := []store.EventRow{
		{TS: t0.Add(-time.Second), Node: "n1", App: "shop", ContainerID: "c-web", Name: "web", Image: "shop:1",
			Runtime: "docker", Action: "start", Kind: "start", State: "running", Epoch: 1, ExitCode: -1},
		{TS: t0.Add(20 * time.Minute), Node: "n1", App: "shop", ContainerID: "c-web", Name: "web", Image: "shop:1",
			Runtime: "containerd", Action: "/tasks/exit", Kind: "exit", State: "exited", Epoch: 1, ExitCode: 3},
		{TS: t0.Add(20*time.Minute + 400*time.Millisecond), Node: "n1", App: "shop", ContainerID: "c-web", Name: "web",
			Image: "shop:1", Runtime: "docker", Action: "die", Kind: "exit", State: "exited", Epoch: 1, ExitCode: 3},
	}
	upload := func(node, table, name string, data []byte) {
		t.Helper()
		ref := &api.FileRef{Node: node, Table: table, Partition: "dt=" + t0.Format("2006-01-02") + "/hour=" + t0.Format("15"),
			Name: name, Crc32C: crc32.Checksum(data, castagnoli), Size: int64(len(data)), Rows: 1}
		if ack := uploadGRPC(t, app.GRPCAddr(), ref, data); !ack.Stored {
			t.Fatalf("upload of %s: %+v", name, ack)
		}
	}
	upload("n1", api.TableLogs, "n1-logs.parquet", parquetOf(t, n1))
	upload("n2", api.TableLogs, "n2-logs.parquet", parquetOf(t, n2))
	upload("n1", api.TableEvents, "n1-events.parquet", parquetOf(t, events))

	conn, err := grpc.NewClient(app.GRPCAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	q := api.NewQueryClient(conn)

	// Indexing follows the upload; a refresh makes it searchable.
	var nodes *api.ListNodesResponse
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		_ = client.Refresh(ctx, prefix+"-*")
		if nodes, err = q.ListNodes(ctx, &api.ListNodesRequest{}); err != nil {
			t.Fatal(err)
		}
		if len(nodes.Nodes) == 2 && nodes.Nodes[0].LogLines == web+db && nodes.Nodes[0].Events == int64(len(events)) &&
			nodes.Nodes[1].LogLines == other {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not indexed in time: %v", nodes)
		}
	}

	// Nodes: by name, with their counts and their first and last time, which
	// an aggregation gives to the millisecond.
	a, b := nodes.Nodes[0], nodes.Nodes[1]
	if a.Node != "n1" || b.Node != "n2" || b.Events != 0 {
		t.Errorf("nodes %v", nodes)
	}
	if first, last := a.First.AsTime(), a.Last.AsTime(); !first.Equal(t0.Add(-time.Second)) ||
		!last.Equal(t0.Add(20*time.Minute+400*time.Millisecond)) {
		t.Errorf("n1 from %v to %v, want %v to %v", first, last, t0.Add(-time.Second), t0.Add(20*time.Minute+400*time.Millisecond))
	}
	// A period includes its start and excludes its end.
	firstHundred := &api.TimeRange{From: timestamppb.New(t0), To: timestamppb.New(t0.Add(100 * time.Millisecond))}
	if in, err := q.ListNodes(ctx, &api.ListNodesRequest{Period: firstHundred}); err != nil || len(in.Nodes) != 2 ||
		in.Nodes[0].LogLines != 100 || in.Nodes[0].Events != 0 || in.Nodes[1].LogLines != 1 {
		t.Errorf("nodes in the first 100 ms: %v %v", in, err)
	}

	// Containers of a node: the most recently active first.
	cs, err := q.ListContainers(ctx, &api.ListContainersRequest{Node: "n1"})
	if err != nil || len(cs.Containers) != 2 {
		t.Fatalf("containers: %v %v", cs, err)
	}
	if w, d := cs.Containers[0], cs.Containers[1]; w.ContainerId != "c-web" || w.Name != "web" || w.Image != "shop:1" ||
		w.App != "shop" || w.LogLines != web || w.Events != 3 || d.ContainerId != "c-db" || d.Name != "db" ||
		d.Image != "postgres:17" || d.LogLines != db || d.Events != 0 {
		t.Errorf("containers %v", cs)
	}

	// Every line of a container, a page at a time: none missing, none twice,
	// in order - with pages that leave a rest and with pages that fit exactly.
	for _, size := range []int32{500, 400} {
		var got []string
		var last time.Time
		req := &api.GetLogsRequest{Node: "n1", Container: "web", PageSize: size}
		for pages := 0; ; pages++ {
			out, err := q.GetLogs(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range out.Lines {
				if ts := l.Time.AsTime(); ts.Before(last) {
					t.Fatalf("page size %d: %v after %v", size, ts, last)
				} else {
					last = ts
				}
				got = append(got, l.Line)
			}
			if out.NextPageToken == "" {
				break
			}
			if pages > 10 {
				t.Fatalf("page size %d: no end", size)
			}
			req.PageToken = out.NextPageToken
		}
		if len(got) != web {
			t.Fatalf("page size %d: %d lines, want %d", size, len(got), web)
		}
		for i, line := range got {
			if want := fmt.Sprintf("web %d GET /item/%d 200", i, i); line != want {
				t.Fatalf("page size %d: line %d is %q, want %q", size, i, line, want)
			}
		}
	}

	// Filters: by container ID, by stream, by application, by words, by period.
	count := func(req *api.GetLogsRequest) int {
		t.Helper()
		req.PageSize = 1000
		out, err := q.GetLogs(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return len(out.Lines)
	}
	for name, tc := range map[string]struct {
		req  *api.GetLogsRequest
		want int
	}{
		"a container by ID":      {&api.GetLogsRequest{Node: "n1", Container: "c-db"}, db},
		"a stream":               {&api.GetLogsRequest{Node: "n1", Stream: "stderr"}, db},
		"an application":         {&api.GetLogsRequest{Node: "n2", App: "billing"}, other},
		"another application":    {&api.GetLogsRequest{Node: "n2", App: "shop"}, 0},
		"words":                  {&api.GetLogsRequest{Node: "n1", Contains: "/item/17"}, 1},
		"a period":               {&api.GetLogsRequest{Node: "n1", Period: firstHundred}, 100},
		"a node that sent none":  {&api.GetLogsRequest{Node: "n3"}, 0},
		"a container of another": {&api.GetLogsRequest{Node: "n2", Container: "web"}, 0},
	} {
		if got := count(tc.req); got != tc.want {
			t.Errorf("%s: %d lines, want %d", name, got, tc.want)
		}
	}

	// Events of a node, oldest first, and one kind of them.
	evs, err := q.GetEvents(ctx, &api.GetEventsRequest{Node: "n1"})
	if err != nil || len(evs.Events) != 3 || evs.NextPageToken != "" {
		t.Fatalf("events: %v %v", evs, err)
	}
	if s, x := evs.Events[0], evs.Events[1]; s.Kind != "start" || s.ExitCode != nil || x.Kind != "exit" ||
		x.Runtime != "containerd" || x.ExitCode == nil || *x.ExitCode != 3 || !x.Time.AsTime().Equal(t0.Add(20*time.Minute)) ||
		x.Name != "web" || x.State != "exited" {
		t.Errorf("events %v", evs)
	}
	if exits, err := q.GetEvents(ctx, &api.GetEventsRequest{Node: "n1", Kind: "exit", PageSize: 1}); err != nil ||
		len(exits.Events) != 1 || exits.NextPageToken == "" {
		t.Errorf("exits, a page of one: %v %v", exits, err)
	} else if rest, err := q.GetEvents(ctx, &api.GetEventsRequest{Node: "n1", Kind: "exit", PageSize: 1,
		PageToken: exits.NextPageToken}); err != nil || len(rest.Events) != 1 || rest.Events[0].Runtime != "docker" {
		t.Errorf("exits, the second page: %v %v", rest, err)
	}
}
