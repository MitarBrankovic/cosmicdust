// Package servers puts the receiver on NATS and gRPC, the two transports the
// nodes use, the query API on the same gRPC server, and serves the metrics
// over HTTP.
package servers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	"github.com/c12s/cosmicdust/internal/server/receiver"
	"github.com/c12s/cosmicdust/internal/server/urgent"
	"github.com/c12s/cosmicdust/pkg/api"
)

// QueueGroup is the NATS queue group of the service's subscriptions.
const QueueGroup = "cosmicdust-server"

// ServeNATS answers chunks on <prefix>.files.* and urgent events on
// <prefix>.urgent.*. A subscription delivers in order, which keeps a file's
// chunks in order. stop unsubscribes.
//
// Both subscriptions are in one queue group, but more than one instance
// would split a file's chunks between them: one instance per NATS server.
func ServeNATS(nc *nats.Conn, prefix string, r *receiver.Receiver, s *urgent.Spool) (stop func() error, err error) {
	files, err := nc.QueueSubscribe(api.FilesSubject(prefix, "*"), QueueGroup, func(m *nats.Msg) {
		var c api.Chunk
		ack := &api.Ack{Error: "bad chunk"}
		if proto.Unmarshal(m.Data, &c) == nil {
			ack = r.Chunk(&c)
		}
		respond(m, ack)
	})
	if err != nil {
		return nil, err
	}
	events, err := nc.QueueSubscribe(api.UrgentSubject(prefix, "*"), QueueGroup, func(m *nats.Msg) {
		var e api.Event
		ack := &api.Ack{Error: "bad event"}
		if proto.Unmarshal(m.Data, &e) == nil {
			ack = s.Add(&e)
		}
		respond(m, ack)
	})
	if err != nil {
		_ = files.Unsubscribe()
		return nil, err
	}
	return func() error { return errors.Join(files.Unsubscribe(), events.Unsubscribe()) }, nil
}

func respond(m *nats.Msg, ack *api.Ack) {
	if b, err := proto.Marshal(ack); err == nil {
		_ = m.Respond(b)
	}
}

// NewGRPCServer is the gRPC server: the receiver for the nodes - a probe, then
// one client stream per file, answered once, early for a duplicate or a
// refused chunk - and the query API for whoever reads what they sent. It
// answers reflection, so that a client needs no proto files to call it.
func NewGRPCServer(r *receiver.Receiver, s *urgent.Spool, q api.QueryServer) *grpc.Server {
	srv := grpc.NewServer()
	api.RegisterSyncServer(srv, &grpcServer{r: r, s: s})
	api.RegisterQueryServer(srv, q)
	reflection.Register(srv)
	return srv
}

type grpcServer struct {
	api.UnimplementedSyncServer
	r *receiver.Receiver
	s *urgent.Spool
}

func (g *grpcServer) Probe(_ context.Context, c *api.Chunk) (*api.Ack, error) {
	return g.r.Chunk(&api.Chunk{File: c.GetFile(), Probe: true, Upload: c.GetUpload()}), nil
}

func (g *grpcServer) Upload(stream api.Sync_UploadServer) error {
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&api.Ack{Error: "the stream ended before the last chunk"})
		}
		if err != nil {
			return err
		}
		if c.Probe {
			return stream.SendAndClose(&api.Ack{Error: "probe on the upload stream"})
		}
		ack := g.r.Chunk(c)
		if !ack.Ok || ack.Duplicate || ack.Stored {
			return stream.SendAndClose(ack)
		}
	}
}

func (g *grpcServer) Urgent(_ context.Context, e *api.Event) (*api.Ack, error) {
	return g.s.Add(e), nil
}

// NewHTTPServer serves /metrics from reg and /healthz, which answers 200 once
// ready says so.
func NewHTTPServer(addr string, reg *prometheus.Registry, ready func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
