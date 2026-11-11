// Package query answers the server's query API from the search index: the
// nodes that sent something, the containers seen on a node, and a node's log
// lines and lifecycle events, a page at a time.
//
// The index is the only source. What an application's search retention has
// already deleted is in the archive and is not read here, and a server that
// runs without Elasticsearch refuses every call.
package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/c12s/cosmicdust/internal/server/es"
	"github.com/c12s/cosmicdust/internal/server/indexer"
	"github.com/c12s/cosmicdust/pkg/api"
)

const (
	defaultPage = 100
	maxPage     = 1000

	// maxBuckets bounds the nodes and the containers of one answer.
	maxBuckets = 10000
)

// Searcher is the part of the Elasticsearch client the service uses.
type Searcher interface {
	Search(ctx context.Context, pattern string, body, out any) error
}

// Stats count what the service was asked.
type Stats struct {
	Requests atomic.Int64
	Failures atomic.Int64 // answered with an error, a bad request included
}

// Service implements api.QueryServer.
type Service struct {
	api.UnimplementedQueryServer

	es           Searcher
	logs, events string // the index patterns of the two tables

	Stats Stats
}

// New returns the service over the data streams the indexer writes under
// prefix. With a nil Searcher - a server without Elasticsearch - every call
// is refused.
func New(s Searcher, prefix string) *Service {
	return &Service{es: s, logs: indexer.StreamPattern(prefix, api.TableLogs),
		events: indexer.StreamPattern(prefix, api.TableEvents)}
}

// The sort of a page is also what identifies a row within a node, so that no
// two rows compare equal and continuing after the last one neither skips nor
// repeats: for a line where it starts in its log file, for an event what the
// runtime reported.
var (
	logSort = []any{
		map[string]any{"@timestamp": "asc"},
		map[string]any{"container_id": "asc"},
		map[string]any{"inode": map[string]any{"order": "asc", "missing": "_first"}},
		map[string]any{"head": map[string]any{"order": "asc", "missing": "_first"}},
		map[string]any{"offset": "asc"},
	}
	eventSort = []any{
		map[string]any{"@timestamp": "asc"},
		map[string]any{"container_id": map[string]any{"order": "asc", "missing": "_first"}},
		map[string]any{"runtime": map[string]any{"order": "asc", "missing": "_first"}},
		map[string]any{"action": "asc"},
		map[string]any{"epoch": "asc"},
	}
)

func term(field, value string) map[string]any {
	return map[string]any{"term": map[string]any{field: value}}
}

// container matches a container by its ID or by its name.
func container(v string) map[string]any {
	return map[string]any{"bool": map[string]any{
		"should":               []any{term("container_id", v), term("name", v)},
		"minimum_should_match": 1,
	}}
}

func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// period turns a time range into a filter; nil for an open range.
func period(r *api.TimeRange) (map[string]any, error) {
	rng := map[string]any{}
	for key, ts := range map[string]*timestamppb.Timestamp{"gte": r.GetFrom(), "lt": r.GetTo()} {
		if ts == nil {
			continue
		}
		if err := ts.CheckValid(); err != nil {
			return nil, invalid("period: %v", err)
		}
		rng[key] = ts.AsTime().UTC().Format(time.RFC3339Nano)
	}
	if len(rng) == 0 {
		return nil, nil
	}
	if r.GetFrom() != nil && r.GetTo() != nil && !r.GetFrom().AsTime().Before(r.GetTo().AsTime()) {
		return nil, invalid("period: from is not before to")
	}
	rng["format"] = "strict_date_optional_time_nanos"
	return map[string]any{"range": map[string]any{"@timestamp": rng}}, nil
}

// filters are what every call but ListNodes starts from: one node, a period.
func filters(node string, r *api.TimeRange) ([]any, error) {
	if node == "" {
		return nil, invalid("node is required")
	}
	fs := []any{term("node", node)}
	p, err := period(r)
	if err != nil {
		return nil, err
	}
	if p != nil {
		fs = append(fs, p)
	}
	return fs, nil
}

func boolFilter(fs []any) map[string]any {
	if len(fs) == 0 {
		return map[string]any{"match_all": map[string]any{}}
	}
	return map[string]any{"bool": map[string]any{"filter": fs}}
}

// search runs one request. Elasticsearch answering with an error is the
// service's failure, not the caller's; not reaching it is "unavailable".
func (s *Service) search(ctx context.Context, pattern string, body, out any) error {
	if s.es == nil {
		return status.Error(codes.FailedPrecondition, "the server runs without Elasticsearch: nothing is indexed to query")
	}
	err := s.es.Search(ctx, pattern, body, out)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	var e *es.Error
	if errors.As(err, &e) {
		return status.Errorf(codes.Internal, "elasticsearch: %d %s: %s", e.Status, e.Type, e.Reason)
	}
	return status.Errorf(codes.Unavailable, "elasticsearch: %v", err)
}

// done counts a call and passes its error on.
func (s *Service) done(err error) error {
	s.Stats.Requests.Add(1)
	if err != nil {
		s.Stats.Failures.Add(1)
	}
	return err
}

type hit[T any] struct {
	Source T                 `json:"_source"`
	Sort   []json.RawMessage `json:"sort"`
}

// page runs one page of a sorted search: size rows after the row the token
// names. The token of the next page is the sort values of the last row,
// as Elasticsearch returned them; it is empty when the page is not full.
func page[T any](ctx context.Context, s *Service, pattern string, fs, sort []any, size int32, token string) ([]T, string, error) {
	switch {
	case size <= 0:
		size = defaultPage
	case size > maxPage:
		size = maxPage
	}
	body := map[string]any{"size": size, "track_total_hits": false, "query": boolFilter(fs), "sort": sort}
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		var after []json.RawMessage
		if err != nil || json.Unmarshal(raw, &after) != nil || len(after) != len(sort) {
			return nil, "", invalid("page_token is not one this call returned")
		}
		body["search_after"] = after
	}
	var out struct {
		Hits struct {
			Hits []hit[T] `json:"hits"`
		} `json:"hits"`
	}
	if err := s.search(ctx, pattern, body, &out); err != nil {
		return nil, "", err
	}
	hits := out.Hits.Hits
	rows := make([]T, len(hits))
	for i := range hits {
		rows[i] = hits[i].Source
	}
	next := ""
	if len(hits) == int(size) {
		raw, err := json.Marshal(hits[len(hits)-1].Sort)
		if err != nil {
			return nil, "", status.Errorf(codes.Internal, "page token: %v", err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	return rows, next, nil
}

// stamp reads a time as the indexer wrote it.
func stamp(s string) *timestamppb.Timestamp {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(t)
}

// logSource and eventSource are the fields of the indexer's documents the
// API returns.
type logSource struct {
	Timestamp   string `json:"@timestamp"`
	Node        string `json:"node"`
	App         string `json:"app"`
	ContainerID string `json:"container_id"`
	Name        string `json:"name"`
	Image       string `json:"image"`
	Stream      string `json:"stream"`
	State       string `json:"state"`
	Epoch       int32  `json:"epoch"`
	Message     string `json:"message"`
	Truncated   bool   `json:"truncated"`
}

type eventSource struct {
	Timestamp   string `json:"@timestamp"`
	Node        string `json:"node"`
	App         string `json:"app"`
	ContainerID string `json:"container_id"`
	Name        string `json:"name"`
	Image       string `json:"image"`
	Runtime     string `json:"runtime"`
	Action      string `json:"action"`
	Kind        string `json:"kind"`
	State       string `json:"state"`
	Epoch       int32  `json:"epoch"`
	ExitCode    *int32 `json:"exit_code"`
	OOMKilled   bool   `json:"oom_killed"`
	Inferred    bool   `json:"inferred"`
}

// GetLogs returns log lines of a node, oldest first.
func (s *Service) GetLogs(ctx context.Context, r *api.GetLogsRequest) (*api.GetLogsResponse, error) {
	out, err := s.getLogs(ctx, r)
	return out, s.done(err)
}

func (s *Service) getLogs(ctx context.Context, r *api.GetLogsRequest) (*api.GetLogsResponse, error) {
	fs, err := filters(r.GetNode(), r.GetPeriod())
	if err != nil {
		return nil, err
	}
	if v := r.GetApp(); v != "" {
		fs = append(fs, term("app", v))
	}
	if v := r.GetContainer(); v != "" {
		fs = append(fs, container(v))
	}
	if v := r.GetStream(); v != "" {
		fs = append(fs, term("stream", v))
	}
	if v := r.GetContains(); v != "" {
		fs = append(fs, map[string]any{"match": map[string]any{"message": map[string]any{"query": v, "operator": "and"}}})
	}
	rows, next, err := page[logSource](ctx, s, s.logs, fs, logSort, r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	out := &api.GetLogsResponse{Lines: make([]*api.LogLine, len(rows)), NextPageToken: next}
	for i, d := range rows {
		out.Lines[i] = &api.LogLine{Time: stamp(d.Timestamp), Node: d.Node, App: d.App, ContainerId: d.ContainerID,
			Name: d.Name, Image: d.Image, Stream: d.Stream, Line: d.Message, State: d.State, Epoch: d.Epoch,
			Truncated: d.Truncated}
	}
	return out, nil
}

// GetEvents returns lifecycle events of a node, oldest first.
func (s *Service) GetEvents(ctx context.Context, r *api.GetEventsRequest) (*api.GetEventsResponse, error) {
	out, err := s.getEvents(ctx, r)
	return out, s.done(err)
}

func (s *Service) getEvents(ctx context.Context, r *api.GetEventsRequest) (*api.GetEventsResponse, error) {
	fs, err := filters(r.GetNode(), r.GetPeriod())
	if err != nil {
		return nil, err
	}
	if v := r.GetContainer(); v != "" {
		fs = append(fs, container(v))
	}
	if v := r.GetKind(); v != "" {
		fs = append(fs, term("kind", v))
	}
	rows, next, err := page[eventSource](ctx, s, s.events, fs, eventSort, r.GetPageSize(), r.GetPageToken())
	if err != nil {
		return nil, err
	}
	out := &api.GetEventsResponse{Events: make([]*api.LifecycleEvent, len(rows)), NextPageToken: next}
	for i, d := range rows {
		out.Events[i] = &api.LifecycleEvent{Time: stamp(d.Timestamp), Node: d.Node, App: d.App,
			ContainerId: d.ContainerID, Name: d.Name, Image: d.Image, Runtime: d.Runtime, Action: d.Action, Kind: d.Kind,
			State: d.State, Epoch: d.Epoch, ExitCode: d.ExitCode, OomKilled: d.OOMKilled, Inferred: d.Inferred}
	}
	return out, nil
}

// A summary counts the documents of both tables in one request: an event has
// an action, a line has none. The first and the last time come from an
// aggregation, which Elasticsearch computes to the millisecond.
func summaryAggs() map[string]any {
	return map[string]any{
		"first":  map[string]any{"min": map[string]any{"field": "@timestamp"}},
		"last":   map[string]any{"max": map[string]any{"field": "@timestamp"}},
		"events": map[string]any{"filter": map[string]any{"exists": map[string]any{"field": "action"}}},
	}
}

type aggTime struct {
	Text string `json:"value_as_string"`
}

type bucket struct {
	Key    string  `json:"key"`
	Docs   int64   `json:"doc_count"`
	First  aggTime `json:"first"`
	Last   aggTime `json:"last"`
	Events struct {
		Docs int64 `json:"doc_count"`
	} `json:"events"`
	About struct {
		Hits struct {
			Hits []struct {
				Source struct {
					Name  string `json:"name"`
					Image string `json:"image"`
					App   string `json:"app"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	} `json:"about"`
}

// summarize groups the documents of both tables matching fs by field.
func (s *Service) summarize(ctx context.Context, fs []any, field string, terms, aggs map[string]any) ([]bucket, error) {
	terms["field"], terms["size"] = field, maxBuckets
	body := map[string]any{"size": 0, "track_total_hits": false, "query": boolFilter(fs),
		"aggs": map[string]any{"by": map[string]any{"terms": terms, "aggs": aggs}}}
	var out struct {
		Aggregations struct {
			By struct {
				Buckets []bucket `json:"buckets"`
			} `json:"by"`
		} `json:"aggregations"`
	}
	if err := s.search(ctx, s.logs+","+s.events, body, &out); err != nil {
		return nil, err
	}
	return out.Aggregations.By.Buckets, nil
}

// ListNodes lists the nodes that sent logs or events in a period.
func (s *Service) ListNodes(ctx context.Context, r *api.ListNodesRequest) (*api.ListNodesResponse, error) {
	out, err := s.listNodes(ctx, r)
	return out, s.done(err)
}

func (s *Service) listNodes(ctx context.Context, r *api.ListNodesRequest) (*api.ListNodesResponse, error) {
	var fs []any
	p, err := period(r.GetPeriod())
	if err != nil {
		return nil, err
	}
	if p != nil {
		fs = append(fs, p)
	}
	buckets, err := s.summarize(ctx, fs, "node", map[string]any{"order": map[string]any{"_key": "asc"}}, summaryAggs())
	if err != nil {
		return nil, err
	}
	out := &api.ListNodesResponse{Nodes: make([]*api.NodeSummary, len(buckets))}
	for i, b := range buckets {
		out.Nodes[i] = &api.NodeSummary{Node: b.Key, LogLines: b.Docs - b.Events.Docs, Events: b.Events.Docs,
			First: stamp(b.First.Text), Last: stamp(b.Last.Text)}
	}
	return out, nil
}

// ListContainers lists the containers seen on a node in a period, the most
// recently active first.
func (s *Service) ListContainers(ctx context.Context, r *api.ListContainersRequest) (*api.ListContainersResponse, error) {
	out, err := s.listContainers(ctx, r)
	return out, s.done(err)
}

func (s *Service) listContainers(ctx context.Context, r *api.ListContainersRequest) (*api.ListContainersResponse, error) {
	fs, err := filters(r.GetNode(), r.GetPeriod())
	if err != nil {
		return nil, err
	}
	aggs := summaryAggs()
	// What the container is called: its newest document says.
	aggs["about"] = map[string]any{"top_hits": map[string]any{"size": 1, "_source": []string{"name", "image", "app"},
		"sort": []any{map[string]any{"@timestamp": "desc"}}}}
	buckets, err := s.summarize(ctx, fs, "container_id", map[string]any{"order": map[string]any{"last": "desc"}}, aggs)
	if err != nil {
		return nil, err
	}
	out := &api.ListContainersResponse{Containers: make([]*api.ContainerSummary, len(buckets))}
	for i, b := range buckets {
		c := &api.ContainerSummary{ContainerId: b.Key, LogLines: b.Docs - b.Events.Docs, Events: b.Events.Docs,
			First: stamp(b.First.Text), Last: stamp(b.Last.Text)}
		if hits := b.About.Hits.Hits; len(hits) > 0 {
			c.Name, c.Image, c.App = hits[0].Source.Name, hits[0].Source.Image, hits[0].Source.App
		}
		out.Containers[i] = c
	}
	return out, nil
}
