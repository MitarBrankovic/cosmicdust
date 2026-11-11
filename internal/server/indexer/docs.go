package indexer

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"strings"
	"time"

	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// Streams are named <prefix>-logs-<app>, <prefix>-events-<app> and
// <prefix>-urgent: one data stream per application and table, so that each has
// its own retention, and one for the urgent copies of events.

// StreamName is the data stream of an application's table.
func StreamName(prefix, table, app string) string {
	return prefix + "-" + table + "-" + streamApp(app)
}

// StreamPattern matches the data streams of a table, of every application.
func StreamPattern(prefix, table string) string { return prefix + "-" + table + "-*" }

// UrgentStream is the data stream of urgent events.
func UrgentStream(prefix string) string { return prefix + "-urgent" }

// streamApp makes an application name fit a data stream name: lowercase, no
// characters Elasticsearch forbids, bounded length. Distinct applications may
// end up in one stream; they keep their own names in the documents.
func streamApp(app string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(app) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	s := b.String()
	if s == "" {
		return "_unknown"
	}
	if len(s) > 180 {
		s = s[:160] + "-" + idOf("app", app)
	}
	return s
}

// The id of a document is a hash of what identifies its row, so a row
// indexed twice - after a crash, or a file sent twice - is one document:
// the second create is answered 409 and dropped.

func idOf(parts ...any) string {
	h := fnv.New128a()
	var buf [8]byte
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			_, _ = h.Write([]byte(v))
		case int64:
			binary.LittleEndian.PutUint64(buf[:], uint64(v))
			_, _ = h.Write(buf[:])
		case uint64:
			binary.LittleEndian.PutUint64(buf[:], v)
			_, _ = h.Write(buf[:])
		case int32:
			binary.LittleEndian.PutUint64(buf[:], uint64(v))
			_, _ = h.Write(buf[:])
		}
		_, _ = h.Write([]byte{0})
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// LogID identifies a line by where it starts in its log file on its node.
func LogID(r *store.LogRow) string {
	if r.Inode == 0 && r.Head == 0 {
		// A line without a position in a file (another source): its time and
		// text are all there is.
		return idOf("log0", r.Node, r.ContainerID, r.TS.UnixNano(), r.Stream, r.Line)
	}
	return idOf("log", r.Node, r.ContainerID, r.Inode, r.Head, r.Offset)
}

// EventID identifies a lifecycle event by what the runtime reported.
func EventID(node, container, runtime, action string, ts int64, epoch int32) string {
	return idOf("event", node, container, runtime, action, ts, epoch)
}

type logDoc struct {
	Timestamp   string `json:"@timestamp"`
	ReadAt      string `json:"read_at,omitempty"`
	Node        string `json:"node"`
	App         string `json:"app,omitempty"`
	ContainerID string `json:"container_id"`
	Name        string `json:"name,omitempty"`
	Image       string `json:"image,omitempty"`
	Stream      string `json:"stream,omitempty"`
	State       string `json:"state,omitempty"`
	Epoch       int32  `json:"epoch"`
	Source      string `json:"source,omitempty"`
	Message     string `json:"message"`
	Inode       uint64 `json:"inode,omitempty"`
	Head        uint64 `json:"head,omitempty"`
	Offset      int64  `json:"offset"`
	Parts       int32  `json:"parts,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func logDocOf(r *store.LogRow) logDoc {
	return logDoc{Timestamp: timestamp(r.TS), ReadAt: timestamp(r.ReadAt), Node: r.Node, App: r.App,
		ContainerID: r.ContainerID, Name: r.Name, Image: r.Image, Stream: r.Stream, State: r.State, Epoch: r.Epoch,
		Source: r.Source, Message: r.Line, Inode: r.Inode, Head: r.Head, Offset: r.Offset, Parts: r.Parts,
		Truncated: r.Truncated}
}

type eventDoc struct {
	Timestamp   string          `json:"@timestamp"`
	Node        string          `json:"node"`
	App         string          `json:"app,omitempty"`
	ContainerID string          `json:"container_id,omitempty"`
	Name        string          `json:"name,omitempty"`
	Image       string          `json:"image,omitempty"`
	Runtime     string          `json:"runtime,omitempty"`
	Namespace   string          `json:"namespace,omitempty"`
	Action      string          `json:"action"`
	Kind        string          `json:"kind,omitempty"`
	State       string          `json:"state,omitempty"`
	Epoch       int32           `json:"epoch"`
	ExitCode    *int32          `json:"exit_code,omitempty"`
	OOMKilled   bool            `json:"oom_killed,omitempty"`
	Inferred    bool            `json:"inferred,omitempty"`
	Fields      json.RawMessage `json:"fields,omitempty"`

	// The urgent copies only: when the node sent it and when it arrived.
	SentAt     string `json:"sent_at,omitempty"`
	ReceivedAt string `json:"received_at,omitempty"`
}

func exitCode(c int32) *int32 {
	if c < 0 {
		return nil // the event carried none
	}
	return &c
}

func eventDocOf(r *store.EventRow) eventDoc {
	d := eventDoc{Timestamp: timestamp(r.TS), Node: r.Node, App: r.App, ContainerID: r.ContainerID, Name: r.Name,
		Image: r.Image, Runtime: r.Runtime, Namespace: r.Namespace, Action: r.Action, Kind: r.Kind, State: r.State,
		Epoch: r.Epoch, ExitCode: exitCode(r.ExitCode), OOMKilled: r.OOMKilled, Inferred: r.Inferred}
	if f := strings.TrimSpace(r.Fields); strings.HasPrefix(f, "{") && json.Valid([]byte(f)) {
		d.Fields = json.RawMessage(f)
	}
	return d
}

func urgentDocOf(e *api.Event, received time.Time) eventDoc {
	return eventDoc{Timestamp: timestamp(time.Unix(0, e.Ts)), Node: e.Node, App: e.App, ContainerID: e.ContainerId,
		Name: e.Name, Image: e.Image, Runtime: e.Runtime, Namespace: e.Namespace, Action: e.Action, Kind: e.Kind,
		Epoch: e.Epoch, ExitCode: exitCode(e.ExitCode), OOMKilled: e.OomKilled,
		SentAt: timestamp(time.Unix(0, e.SentAt)), ReceivedAt: timestamp(received)}
}

// The index templates: one per table and one for urgent events, each with
// its default retention; streams of applications with a retention of their
// own get it when they are created.

func keyword() map[string]any { return map[string]any{"type": "keyword"} }

func template(pattern string, retention time.Duration, replicas int, props map[string]any) map[string]any {
	settings := map[string]any{"number_of_shards": 1}
	if replicas >= 0 {
		settings["number_of_replicas"] = replicas
	}
	return map[string]any{
		"index_patterns": []string{pattern},
		"data_stream":    map[string]any{},
		"priority":       500,
		"template": map[string]any{
			"settings":  settings,
			"mappings":  map[string]any{"dynamic": false, "properties": props},
			"lifecycle": map[string]any{"data_retention": esDuration(retention)},
		},
		"_meta": map[string]any{"managed_by": "cosmicdust-server"},
	}
}

func logMappings() map[string]any {
	return map[string]any{
		"@timestamp": map[string]any{"type": "date_nanos"}, "read_at": map[string]any{"type": "date_nanos"},
		"node": keyword(), "app": keyword(), "container_id": keyword(), "name": keyword(), "image": keyword(),
		"stream": keyword(), "state": keyword(), "source": keyword(),
		"epoch":   map[string]any{"type": "integer"},
		"message": map[string]any{"type": "match_only_text"},
		"inode":   map[string]any{"type": "unsigned_long"}, "head": map[string]any{"type": "unsigned_long"},
		"offset": map[string]any{"type": "long"}, "parts": map[string]any{"type": "integer"},
		"truncated": map[string]any{"type": "boolean"},
	}
}

func eventMappings(urgent bool) map[string]any {
	m := map[string]any{
		"@timestamp": map[string]any{"type": "date_nanos"},
		"node":       keyword(), "app": keyword(), "container_id": keyword(), "name": keyword(), "image": keyword(),
		"runtime": keyword(), "namespace": keyword(), "action": keyword(), "kind": keyword(), "state": keyword(),
		"epoch": map[string]any{"type": "integer"}, "exit_code": map[string]any{"type": "integer"},
		"oom_killed": map[string]any{"type": "boolean"}, "inferred": map[string]any{"type": "boolean"},
		"fields": map[string]any{"type": "flattened"},
	}
	if urgent {
		m["sent_at"] = map[string]any{"type": "date_nanos"}
		m["received_at"] = map[string]any{"type": "date_nanos"}
	}
	return m
}
