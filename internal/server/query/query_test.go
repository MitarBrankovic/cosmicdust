package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/pkg/api"
)

// fakeES answers searches with what the test queued and keeps what it was
// asked.
type fakeES struct {
	mu      sync.Mutex
	paths   []string
	bodies  []map[string]any
	raw     []string // the bodies as sent
	answers []string // one per request, in order; the last one repeats
	status  int
}

func (f *fakeES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	f.paths = append(f.paths, r.Method+" "+r.URL.RequestURI())
	f.bodies = append(f.bodies, body)
	f.raw = append(f.raw, string(data))
	w.Header().Set("Content-Type", "application/json")
	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, `{"error":{"type":"search_phase_execution_exception","reason":"all shards failed"}}`)
		return
	}
	answer := `{"hits":{"hits":[]}}`
	if n := len(f.answers); n > 0 {
		answer = f.answers[min(len(f.bodies)-1, n-1)]
	}
	_, _ = io.WriteString(w, answer)
}

func (f *fakeES) last() (string, map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paths[len(f.paths)-1], f.bodies[len(f.bodies)-1]
}

func service(t *testing.T, f *fakeES) *Service {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client, err := es.New(srv.URL, es.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return New(client, "pfx")
}

// filtersOf are the filters of a request's query, as JSON texts.
func filtersOf(t *testing.T, body map[string]any) []string {
	t.Helper()
	fs, ok := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	if !ok {
		t.Fatalf("no bool filter in %v", body["query"])
	}
	out := make([]string, len(fs))
	for i, f := range fs {
		b, _ := json.Marshal(f)
		out[i] = string(b)
	}
	return out
}

func code(err error) codes.Code { return status.Code(err) }

var t0 = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func TestGetLogsFiltersSortsAndPages(t *testing.T) {
	f := &fakeES{answers: []string{
		`{"hits":{"hits":[
		  {"_source":{"@timestamp":"2026-11-11T10:00:00.000000001Z","node":"n1","app":"shop","container_id":"c1","name":"web",
		     "image":"shop:1","stream":"stdout","state":"running","epoch":2,"message":"GET /a 200","offset":0},
		   "sort":[1794391200000000001,"c1",18446744073709551615,99,0]},
		  {"_source":{"@timestamp":"2026-11-11T10:00:00.5Z","node":"n1","container_id":"c1","message":"GET /b 500","truncated":true},
		   "sort":[1794391200500000000,"c1",18446744073709551615,99,50]}]}}`,
		`{"hits":{"hits":[
		  {"_source":{"@timestamp":"2026-11-11T10:00:01Z","node":"n1","container_id":"c1","message":"last"},
		   "sort":[1794391201000000000,"c1",18446744073709551615,99,100]}]}}`,
	}}
	s := service(t, f)
	ctx := context.Background()
	req := &api.GetLogsRequest{Node: "n1", App: "shop", Container: "web", Stream: "stdout", Contains: "GET 200",
		Period:   &api.TimeRange{From: timestamppb.New(t0), To: timestamppb.New(t0.Add(time.Hour))},
		PageSize: 2}
	out, err := s.GetLogs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	path, body := f.last()
	if !strings.HasPrefix(path, "POST /pfx-logs-%2A/_search?") || !strings.Contains(path, "ignore_unavailable=true") {
		t.Errorf("request %s", path)
	}
	want := []string{
		`{"term":{"node":"n1"}}`,
		`{"range":{"@timestamp":{"format":"strict_date_optional_time_nanos","gte":"2026-11-11T10:00:00Z","lt":"2026-11-11T11:00:00Z"}}}`,
		`{"term":{"app":"shop"}}`,
		`{"bool":{"minimum_should_match":1,"should":[{"term":{"container_id":"web"}},{"term":{"name":"web"}}]}}`,
		`{"term":{"stream":"stdout"}}`,
		`{"match":{"message":{"operator":"and","query":"GET 200"}}}`,
	}
	if got := filtersOf(t, body); !reflect.DeepEqual(got, want) {
		t.Errorf("filters\n got %v\nwant %v", got, want)
	}
	if body["size"] != float64(2) || len(body["sort"].([]any)) != 5 || body["search_after"] != nil {
		t.Errorf("size %v, sort %v, search_after %v", body["size"], body["sort"], body["search_after"])
	}

	if len(out.Lines) != 2 || out.NextPageToken == "" {
		t.Fatalf("%d lines, token %q", len(out.Lines), out.NextPageToken)
	}
	first := out.Lines[0]
	if !first.Time.AsTime().Equal(t0.Add(time.Nanosecond)) || first.Node != "n1" || first.App != "shop" ||
		first.ContainerId != "c1" || first.Name != "web" || first.Image != "shop:1" || first.Stream != "stdout" ||
		first.Line != "GET /a 200" || first.State != "running" || first.Epoch != 2 || first.Truncated {
		t.Errorf("first line %v", first)
	}
	if !out.Lines[1].Truncated || out.Lines[1].Line != "GET /b 500" {
		t.Errorf("second line %v", out.Lines[1])
	}

	// The next page starts after the last row of this one, by the very values
	// Elasticsearch sorted it with: a number above 2^53 must come back exact.
	req.PageToken = out.NextPageToken
	out, err = s.GetLogs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	const after = `[1794391200500000000,"c1",18446744073709551615,99,50]`
	if token, _ := base64.RawURLEncoding.DecodeString(req.PageToken); string(token) != after {
		t.Errorf("token %s", token)
	}
	f.mu.Lock()
	sent := f.raw[1]
	f.mu.Unlock()
	if !strings.Contains(sent, `"search_after":`+after) {
		t.Errorf("the second request does not continue after the last row: %s", sent)
	}
	if len(out.Lines) != 1 || out.NextPageToken != "" {
		t.Errorf("last page: %d lines, token %q", len(out.Lines), out.NextPageToken)
	}
	if s.Stats.Requests.Load() != 2 || s.Stats.Failures.Load() != 0 {
		t.Errorf("requests %d, failures %d", s.Stats.Requests.Load(), s.Stats.Failures.Load())
	}
}

func TestPageSize(t *testing.T) {
	f := &fakeES{}
	s := service(t, f)
	for size, want := range map[int32]float64{0: defaultPage, -5: defaultPage, 7: 7, 5000: maxPage} {
		if _, err := s.GetLogs(context.Background(), &api.GetLogsRequest{Node: "n1", PageSize: size}); err != nil {
			t.Fatal(err)
		}
		if _, body := f.last(); body["size"] != want {
			t.Errorf("page_size %d: size %v, want %v", size, body["size"], want)
		}
	}
}

func TestBadRequests(t *testing.T) {
	f := &fakeES{}
	s := service(t, f)
	ctx := context.Background()
	eventToken := base64.RawURLEncoding.EncodeToString([]byte(`[1,"c1"]`)) // too few values for either sort
	backwards := &api.TimeRange{From: timestamppb.New(t0), To: timestamppb.New(t0)}
	for name, call := range map[string]func() error{
		"logs without a node":       func() error { _, err := s.GetLogs(ctx, &api.GetLogsRequest{}); return err },
		"events without a node":     func() error { _, err := s.GetEvents(ctx, &api.GetEventsRequest{}); return err },
		"containers without a node": func() error { _, err := s.ListContainers(ctx, &api.ListContainersRequest{}); return err },
		"a token that is not one": func() error {
			_, err := s.GetLogs(ctx, &api.GetLogsRequest{Node: "n1", PageToken: "not base64!"})
			return err
		},
		"a token of another call": func() error {
			_, err := s.GetEvents(ctx, &api.GetEventsRequest{Node: "n1", PageToken: eventToken})
			return err
		},
		"a period that ends where it starts": func() error {
			_, err := s.ListNodes(ctx, &api.ListNodesRequest{Period: backwards})
			return err
		},
	} {
		if err := call(); code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	f.mu.Lock()
	asked := len(f.paths)
	f.mu.Unlock()
	if asked != 0 {
		t.Errorf("%d bad requests reached Elasticsearch", asked)
	}
	if s.Stats.Requests.Load() != 6 || s.Stats.Failures.Load() != 6 {
		t.Errorf("requests %d, failures %d", s.Stats.Requests.Load(), s.Stats.Failures.Load())
	}
}

func TestGetEvents(t *testing.T) {
	f := &fakeES{answers: []string{`{"hits":{"hits":[
	  {"_source":{"@timestamp":"2026-11-11T10:00:00Z","node":"n1","app":"shop","container_id":"c1","name":"web","image":"shop:1",
	     "runtime":"docker","action":"die","kind":"exit","state":"exited","epoch":3,"exit_code":137,"oom_killed":true},
	   "sort":[1,"c1","docker","die",3]},
	  {"_source":{"@timestamp":"2026-11-11T10:00:02Z","node":"n1","container_id":"c1","runtime":"containerd",
	     "action":"/tasks/start","kind":"start","epoch":4,"inferred":true},
	   "sort":[2,"c1","containerd","/tasks/start",4]}]}}`}}
	s := service(t, f)
	out, err := s.GetEvents(context.Background(), &api.GetEventsRequest{Node: "n1", Container: "c1", Kind: "exit"})
	if err != nil {
		t.Fatal(err)
	}
	path, body := f.last()
	if !strings.HasPrefix(path, "POST /pfx-events-%2A/_search?") {
		t.Errorf("request %s", path)
	}
	if got := filtersOf(t, body); len(got) != 3 || got[2] != `{"term":{"kind":"exit"}}` {
		t.Errorf("filters %v", got)
	}
	if len(out.Events) != 2 || out.NextPageToken != "" {
		t.Fatalf("%d events, token %q", len(out.Events), out.NextPageToken)
	}
	died, started := out.Events[0], out.Events[1]
	if died.ExitCode == nil || *died.ExitCode != 137 || !died.OomKilled || died.Inferred || died.Runtime != "docker" ||
		died.Action != "die" || died.Kind != "exit" || died.State != "exited" || died.Epoch != 3 ||
		!died.Time.AsTime().Equal(t0) {
		t.Errorf("exit %v", died)
	}
	if started.ExitCode != nil || !started.Inferred || started.Action != "/tasks/start" {
		t.Errorf("start %v", started)
	}
}

func TestListNodes(t *testing.T) {
	f := &fakeES{answers: []string{`{"hits":{"hits":[]},"aggregations":{"by":{"buckets":[
	  {"key":"n1","doc_count":1010,"first":{"value":1.0,"value_as_string":"2026-11-11T10:00:00.000Z"},
	   "last":{"value":2.0,"value_as_string":"2026-11-11T10:30:00.250Z"},"events":{"doc_count":10}},
	  {"key":"n2","doc_count":3,"first":{"value":null},"last":{"value":null},"events":{"doc_count":3}}]}}}`}}
	s := service(t, f)
	out, err := s.ListNodes(context.Background(), &api.ListNodesRequest{
		Period: &api.TimeRange{From: timestamppb.New(t0)}})
	if err != nil {
		t.Fatal(err)
	}
	path, body := f.last()
	if !strings.HasPrefix(path, "POST /pfx-logs-%2A%2Cpfx-events-%2A/_search?") {
		t.Errorf("request %s", path)
	}
	by := body["aggs"].(map[string]any)["by"].(map[string]any)
	if terms := by["terms"].(map[string]any); terms["field"] != "node" || terms["size"] != float64(maxBuckets) {
		t.Errorf("terms %v", terms)
	}
	if got := filtersOf(t, body); len(got) != 1 || !strings.Contains(got[0], `"gte":"2026-11-11T10:00:00Z"`) ||
		strings.Contains(got[0], `"lt"`) {
		t.Errorf("filters %v", got)
	}
	if body["size"] != float64(0) {
		t.Errorf("size %v", body["size"])
	}
	if len(out.Nodes) != 2 {
		t.Fatalf("%d nodes", len(out.Nodes))
	}
	n1, n2 := out.Nodes[0], out.Nodes[1]
	if n1.Node != "n1" || n1.LogLines != 1000 || n1.Events != 10 || !n1.First.AsTime().Equal(t0) ||
		!n1.Last.AsTime().Equal(t0.Add(30*time.Minute+250*time.Millisecond)) {
		t.Errorf("n1 %v", n1)
	}
	if n2.LogLines != 0 || n2.Events != 3 || n2.First != nil || n2.Last != nil {
		t.Errorf("n2 %v", n2)
	}

	// No period at all: every document.
	if _, err := s.ListNodes(context.Background(), &api.ListNodesRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, body := f.last(); body["query"].(map[string]any)["match_all"] == nil {
		t.Errorf("query without a period: %v", body["query"])
	}
}

func TestListContainers(t *testing.T) {
	f := &fakeES{answers: []string{`{"aggregations":{"by":{"buckets":[
	  {"key":"c2","doc_count":50,"first":{"value_as_string":"2026-11-11T10:10:00.000Z"},
	   "last":{"value_as_string":"2026-11-11T10:20:00.000Z"},"events":{"doc_count":2},
	   "about":{"hits":{"hits":[{"_source":{"name":"db","image":"postgres:17","app":"shop"}}]}}},
	  {"key":"c1","doc_count":7,"first":{"value_as_string":"2026-11-11T10:00:00.000Z"},
	   "last":{"value_as_string":"2026-11-11T10:05:00.000Z"},"events":{"doc_count":7},"about":{"hits":{"hits":[]}}}]}}}`}}
	s := service(t, f)
	out, err := s.ListContainers(context.Background(), &api.ListContainersRequest{Node: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	_, body := f.last()
	by := body["aggs"].(map[string]any)["by"].(map[string]any)
	terms := by["terms"].(map[string]any)
	if terms["field"] != "container_id" || terms["order"].(map[string]any)["last"] != "desc" {
		t.Errorf("terms %v", terms)
	}
	if by["aggs"].(map[string]any)["about"] == nil {
		t.Errorf("no top hit asked for: %v", by["aggs"])
	}
	if got := filtersOf(t, body); len(got) != 1 || got[0] != `{"term":{"node":"n1"}}` {
		t.Errorf("filters %v", got)
	}
	if len(out.Containers) != 2 {
		t.Fatalf("%d containers", len(out.Containers))
	}
	db, other := out.Containers[0], out.Containers[1]
	if db.ContainerId != "c2" || db.Name != "db" || db.Image != "postgres:17" || db.App != "shop" || db.LogLines != 48 ||
		db.Events != 2 || !db.Last.AsTime().Equal(t0.Add(20*time.Minute)) {
		t.Errorf("c2 %v", db)
	}
	if other.ContainerId != "c1" || other.Name != "" || other.LogLines != 0 || other.Events != 7 {
		t.Errorf("c1 %v", other)
	}
}

func TestElasticsearchFailures(t *testing.T) {
	ctx := context.Background()

	// Without Elasticsearch there is nothing to ask.
	none := New(nil, "pfx")
	if _, err := none.ListNodes(ctx, &api.ListNodesRequest{}); code(err) != codes.FailedPrecondition {
		t.Errorf("no Elasticsearch: %v", err)
	}

	// It answers with an error: the service's failure.
	f := &fakeES{status: http.StatusServiceUnavailable}
	s := service(t, f)
	_, err := s.GetLogs(ctx, &api.GetLogsRequest{Node: "n1"})
	if code(err) != codes.Internal || !strings.Contains(err.Error(), "all shards failed") {
		t.Errorf("an error answer: %v", err)
	}

	// It is not there.
	srv := httptest.NewServer(http.NotFoundHandler())
	client, _ := es.New(srv.URL, es.Options{})
	srv.Close()
	gone := New(client, "pfx")
	if _, err := gone.GetEvents(ctx, &api.GetEventsRequest{Node: "n1"}); code(err) != codes.Unavailable {
		t.Errorf("unreachable: %v", err)
	}

	// The caller gave up.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.GetLogs(cancelled, &api.GetLogsRequest{Node: "n1"}); code(err) != codes.Canceled {
		t.Errorf("cancelled: %v", err)
	}
	if s.Stats.Failures.Load() != 2 {
		t.Errorf("failures %d", s.Stats.Failures.Load())
	}
}
