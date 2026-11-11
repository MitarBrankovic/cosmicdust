package startup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"

	"github.com/c12s/cosmicdust/internal/server/configs"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// fakeES keeps what is bulk indexed, by stream and id.
type fakeES struct {
	mu   sync.Mutex
	docs map[string]map[string]bool
}

func (f *fakeES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
		var items []string
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var a struct {
				Create struct {
					Index string `json:"_index"`
					ID    string `json:"_id"`
				} `json:"create"`
			}
			_ = json.Unmarshal(sc.Bytes(), &a)
			sc.Scan()
			if f.docs[a.Create.Index] == nil {
				f.docs[a.Create.Index] = map[string]bool{}
			}
			status := 201
			if f.docs[a.Create.Index][a.Create.ID] {
				status = 409
			}
			f.docs[a.Create.Index][a.Create.ID] = true
			items = append(items, fmt.Sprintf(`{"create":{"status":%d}}`, status))
		}
		_, _ = fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_data_stream/"):
		_, _ = io.WriteString(w, `{"data_streams":[]}`)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_search"):
		_, _ = io.WriteString(w, `{"hits":{"hits":[{"_source":{"@timestamp":"2026-10-29T08:00:00Z","node":"n1",`+
			`"container_id":"c1","message":"line-0"},"sort":[1,"c1",5,6,0]}]}}`)
	default: // templates, streams, lifecycles
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	}
}

func (f *fakeES) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for stream, ids := range f.docs {
		if strings.HasPrefix(stream, prefix) {
			n += len(ids)
		}
	}
	return n
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// nodeFile is a Parquet file as a node sends it: n lines of a container, of
// two applications.
func nodeFile(t *testing.T, container string, n int) []byte {
	t.Helper()
	rows := make([]store.LogRow, n)
	t0 := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i] = store.LogRow{TS: t0.Add(time.Duration(i)), Node: "n1", App: []string{"shop.api", "billing"}[i%2],
			ContainerID: container, Line: fmt.Sprintf("line-%d", i), Inode: 5, Head: 6, Offset: int64(i) * 10}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[store.LogRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func refOf(name string, data []byte) *api.FileRef {
	return &api.FileRef{Node: "n1", Table: api.TableLogs, Partition: "dt=2026-10-29/hour=08", Name: name,
		Crc32C: crc32.Checksum(data, castagnoli), Size: int64(len(data)), Rows: 1}
}

func start(t *testing.T, cfg *configs.Config) *App {
	t.Helper()
	app, err := NewAppWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	return app
}

// uploadGRPC sends a file as the cosmicdust node's gRPC transport does: a probe,
// then a stream of chunks.
func uploadGRPC(t *testing.T, addr net.Addr, ref *api.FileRef, data []byte) *api.Ack {
	t.Helper()
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	c := api.NewSyncClient(conn)
	ctx := context.Background()
	ack, err := c.Probe(ctx, &api.Chunk{File: ref, Probe: true, Upload: 1})
	if err != nil || ack.Duplicate || !ack.Ok {
		return ack
	}
	stream, err := c.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(data); off += 1000 {
		end := min(off+1000, len(data))
		if err := stream.Send(&api.Chunk{File: ref, Offset: int64(off), Data: data[off:end], Last: end == len(data),
			Upload: 1}); err != nil {
			break // answered early; CloseAndRecv has it
		}
	}
	ack, err = stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	return ack
}

// uploadNATS sends a file as the cosmicdust node's NATS transport does: a probe
// and chunks, each a request with its own reply.
func uploadNATS(t *testing.T, nc *nats.Conn, ref *api.FileRef, data []byte) *api.Ack {
	t.Helper()
	request := func(c *api.Chunk) *api.Ack {
		b, _ := proto.Marshal(c)
		m, err := nc.Request(api.FilesSubject("cosmicdust", ref.Node), b, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var ack api.Ack
		if err := proto.Unmarshal(m.Data, &ack); err != nil {
			t.Fatal(err)
		}
		return &ack
	}
	if ack := request(&api.Chunk{File: ref, Probe: true, Upload: 2}); ack.Duplicate || !ack.Ok {
		return ack
	}
	var ack *api.Ack
	for off := 0; off < len(data); off += 1000 {
		end := min(off+1000, len(data))
		if ack = request(&api.Chunk{File: ref, Offset: int64(off), Data: data[off:end], Last: end == len(data),
			Upload: 2}); !ack.Ok {
			break
		}
	}
	return ack
}

// services are the gRPC services the server names over reflection.
func services(t *testing.T, conn *grpc.ClientConn) []string {
	t.Helper()
	stream, err := reflectpb.NewServerReflectionClient(conn).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.CloseSend() }()
	if err := stream.Send(&reflectpb.ServerReflectionRequest{
		MessageRequest: &reflectpb.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		names = append(names, s.GetName())
	}
	return names
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s", what)
		}
	}
}

func TestService(t *testing.T) {
	es := &fakeES{docs: map[string]map[string]bool{}}
	esSrv := httptest.NewServer(es)
	defer esSrv.Close()
	cfg := &configs.Config{SubjectPrefix: "cosmicdust", GrpcAddress: "127.0.0.1:0", HttpAddress: "127.0.0.1:0",
		DataDir: t.TempDir(), EsAddress: esSrv.URL, EsIndexPrefix: "cosmicdust", EsReplicas: -1, PartialIdle: time.Minute,
		CleanupEvery: time.Hour, LogLevel: "error", Retention: configs.DefaultRetention()}
	var nc *nats.Conn
	if bin, err := exec.LookPath("nats-server"); err == nil {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		cmd := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprint(port))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
		cfg.NatsAddress = fmt.Sprintf("127.0.0.1:%d", port)
		if nc, err = nats.Connect("nats://"+cfg.NatsAddress, nats.RetryOnFailedConnect(true)); err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		waitFor(t, "NATS", nc.IsConnected)
	} else {
		t.Log("no nats-server on PATH: gRPC only")
	}

	app := start(t, cfg)
	data := nodeFile(t, "c1", 500)
	if ack := uploadGRPC(t, app.GRPCAddr(), refOf("p1.parquet", data), data); !ack.Stored {
		t.Fatalf("upload over gRPC: %+v", ack)
	}
	conn, _ := grpc.NewClient(app.GRPCAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() { _ = conn.Close() }()
	ev := &api.Event{Node: "n1", Ts: time.Now().UnixNano(), ContainerId: "c1", Runtime: "docker", Action: "die",
		Kind: "exit", ExitCode: 1, SentAt: time.Now().UnixNano()}
	if ack, err := api.NewSyncClient(conn).Urgent(context.Background(), ev); err != nil || !ack.Stored {
		t.Fatalf("urgent event: %+v %v", ack, err)
	}
	want := 500
	if nc != nil {
		data2 := nodeFile(t, "c2", 300)
		if ack := uploadNATS(t, nc, refOf("p2.parquet", data2), data2); !ack.Stored {
			t.Fatalf("upload over NATS: %+v", ack)
		}
		want += 300
	}
	waitFor(t, "the index", func() bool { return es.count("cosmicdust-logs-") == want && es.count("cosmicdust-urgent") == 1 })

	// The query API is on the same server, and reflection names both services.
	logs, err := api.NewQueryClient(conn).GetLogs(context.Background(), &api.GetLogsRequest{Node: "n1"})
	if err != nil || len(logs.GetLines()) != 1 || logs.Lines[0].Line != "line-0" {
		t.Fatalf("query: %v %v", logs, err)
	}
	if got := services(t, conn); !slices.Contains(got, "cosmicdust.v1.Query") || !slices.Contains(got, "cosmicdust.v1.Sync") {
		t.Errorf("reflection names %v", got)
	}

	resp, err := http.Get("http://" + app.HTTPAddr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(metrics), "cosmicdust_server_files_received_total") ||
		!strings.Contains(string(metrics), "cosmicdust_server_queries_total 1") ||
		!strings.Contains(string(metrics), "cosmicdust_server_elasticsearch_up 1") {
		t.Errorf("metrics:\n%s", metrics)
	}
	if resp, err := http.Get("http://" + app.HTTPAddr().String() + "/healthz"); err != nil || resp.StatusCode != 200 {
		t.Errorf("healthz %v %v", resp, err)
	}
	app.GracefulStop(context.Background())

	// After a restart the file is known: a duplicate, and nothing new is
	// indexed.
	app2 := start(t, cfg)
	defer app2.GracefulStop(context.Background())
	if ack := uploadGRPC(t, app2.GRPCAddr(), refOf("p1.parquet", data), data); !ack.Duplicate {
		t.Fatalf("after a restart: %+v, want a duplicate", ack)
	}
	time.Sleep(200 * time.Millisecond)
	if n := es.count("cosmicdust-logs-"); n != want {
		t.Fatalf("%d documents after the restart, want %d", n, want)
	}
}
