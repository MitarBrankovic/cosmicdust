package servertest

import (
	"context"
	"errors"
	"io"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/c12s/cosmicdust/pkg/api"
)

// ServeNATS answers chunks on <subject>.files.* and urgent events on
// <subject>.urgent.*, in one queue group so that more receivers could share
// the load. A subscription delivers in order, which keeps a file's chunks in
// order. stop unsubscribes.
func ServeNATS(nc *nats.Conn, subject string, r *Receiver) (stop func() error, err error) {
	files, err := nc.QueueSubscribe(api.FilesSubject(subject, "*"), "servertest", func(m *nats.Msg) {
		var c api.Chunk
		ack := refuse("bad chunk")
		if proto.Unmarshal(m.Data, &c) == nil {
			ack = r.Chunk(&c)
		}
		respond(m, ack)
	})
	if err != nil {
		return nil, err
	}
	urgent, err := nc.QueueSubscribe(api.UrgentSubject(subject, "*"), "servertest", func(m *nats.Msg) {
		var e api.Event
		ack := refuse("bad event")
		if proto.Unmarshal(m.Data, &e) == nil {
			ack = r.Event(&e)
		}
		respond(m, ack)
	})
	if err != nil {
		_ = files.Unsubscribe()
		return nil, err
	}
	return func() error { return errors.Join(files.Unsubscribe(), urgent.Unsubscribe()) }, nil
}

func respond(m *nats.Msg, ack *api.Ack) {
	if b, err := proto.Marshal(ack); err == nil {
		_ = m.Respond(b)
	}
}

// NewGRPCServer is the receiver as a gRPC server: one client stream per file,
// answered once - early for a duplicate or a refused chunk.
func NewGRPCServer(r *Receiver) *grpc.Server {
	s := grpc.NewServer()
	api.RegisterSyncServer(s, &grpcServer{r: r})
	return s
}

type grpcServer struct {
	api.UnimplementedSyncServer
	r *Receiver
}

func (s *grpcServer) Probe(_ context.Context, c *api.Chunk) (*api.Ack, error) {
	return s.r.Chunk(&api.Chunk{File: c.GetFile(), Probe: true, Upload: c.GetUpload()}), nil
}

func (s *grpcServer) Upload(stream api.Sync_UploadServer) error {
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(refuse("the stream ended before the last chunk"))
		}
		if err != nil {
			return err
		}
		if c.Probe {
			return stream.SendAndClose(refuse("probe on the upload stream"))
		}
		ack := s.r.Chunk(c)
		if !ack.Ok || ack.Duplicate || ack.Stored {
			return stream.SendAndClose(ack)
		}
	}
}

func (s *grpcServer) Urgent(_ context.Context, e *api.Event) (*api.Ack, error) {
	return s.r.Event(e), nil
}
