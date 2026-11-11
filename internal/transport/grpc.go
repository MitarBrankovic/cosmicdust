package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/c12s/cosmicdust/pkg/api"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// GRPC sends a file as a probe and then one client stream of chunks; HTTP/2
// flow control does what the window does for NATS (the same throughput
// at 100 ms). The receiver may answer early, which ends the stream.
type GRPC struct {
	opts           Options
	files, urgent  *grpc.ClientConn
	fileC, urgentC api.SyncClient
	cnt            *counter
	lim            *limiter
	up             up
	stopWatch      context.CancelFunc
}

// NewGRPC prepares connections to addr (host:port). The one for files is made
// at once and kept up; the urgent one when used. The connections are not
// encrypted.
func NewGRPC(addr string, opts Options) (*GRPC, error) {
	if err := opts.defaults(); err != nil {
		return nil, err
	}
	cnt := &counter{}
	// After a failure gRPC reconnects in the background, by default backing
	// off to two minutes; at most 10 s here, so that a control plane that
	// is back is found soon. An attempt is a TCP connect.
	connect := backoff.DefaultConfig
	connect.MaxDelay = 10 * time.Second
	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, a string) (net.Conn, error) { return cnt.dial(ctx, "tcp", a) }),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: connect, MinConnectTimeout: 20 * time.Second}),
	}
	files, err := grpc.NewClient(addr, dial...)
	if err != nil {
		return nil, fmt.Errorf("grpc: %w", err)
	}
	urgent := files
	if !opts.SharedUrgent {
		if urgent, err = grpc.NewClient(addr, dial...); err != nil {
			_ = files.Close()
			return nil, fmt.Errorf("grpc: %w", err)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	t := &GRPC{opts: opts, files: files, urgent: urgent, cnt: cnt, lim: newLimiter(opts.RateBytes),
		fileC: api.NewSyncClient(files), urgentC: api.NewSyncClient(urgent), up: newUp(), stopWatch: stop}
	go watchReady(ctx, files, t.up)
	if err := cnt.register(opts.Registerer, "grpc"); err != nil {
		_ = t.Close()
		return nil, err
	}
	return t, nil
}

// watchReady keeps conn connected and signals every time it becomes ready.
// gRPC leaves a connection that broke idle until the next call; here it is
// made again at once, and retried while the control plane is down, as the
// NATS client does, so that a control plane that is back is noticed.
func watchReady(ctx context.Context, conn *grpc.ClientConn, u up) {
	for s := conn.GetState(); ; s = conn.GetState() {
		switch s {
		case connectivity.Idle:
			conn.Connect()
		case connectivity.Ready:
			u.signal()
		}
		if !conn.WaitForStateChange(ctx, s) {
			return
		}
	}
}

// Reconnected receives a value when the connection for files is made, the
// first time or again.
func (t *GRPC) Reconnected() <-chan struct{} { return t.up }

// Upload sends one file: a probe, then a stream of the chunks from where the
// receiver's copy ends.
func (t *GRPC) Upload(ctx context.Context, ref cosmicdust.FileRef, r io.Reader) (cosmicdust.Ack, error) {
	pref, err := fileRef(t.opts.Node, ref)
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	id := attempt()
	t.cnt.probes.Add(1)
	a, err := t.fileC.Probe(ctx, &api.Chunk{File: pref, Probe: true, Upload: id})
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("grpc: probe: %w", err)
	}
	if a.Duplicate || a.Stored || !a.Ok {
		return ackOf(a), nil
	}
	start := resumeAt(a.Have, pref.Size)
	if err := skip(r, start); err != nil {
		return cosmicdust.Ack{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := t.fileC.Upload(ctx)
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("grpc: %w", err)
	}
	err = chunks(r, pref, id, start, t.opts.ChunkBytes, t.lim, func(c *api.Chunk) error {
		t.cnt.chunks.Add(1)
		if err := stream.Send(c); err != nil {
			if errors.Is(err, io.EOF) {
				return errDone // the receiver has answered; CloseAndRecv gets it
			}
			return fmt.Errorf("grpc: %w", err)
		}
		return nil
	})
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	a, err = stream.CloseAndRecv()
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("grpc: %w", err)
	}
	return ackOf(a), nil
}

// Urgent sends one event and waits for its answer.
func (t *GRPC) Urgent(ctx context.Context, e cosmicdust.Event) (cosmicdust.Ack, error) {
	t.cnt.ev.Add(1)
	a, err := t.urgentC.Urgent(ctx, event(t.opts.Node, e))
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("grpc: %w", err)
	}
	return ackOf(a), nil
}

// Close closes the connections.
func (t *GRPC) Close() error {
	t.stopWatch()
	var err error
	if t.urgent != t.files {
		err = t.urgent.Close()
	}
	return errors.Join(err, t.files.Close())
}

var (
	_ cosmicdust.Transport   = (*GRPC)(nil)
	_ cosmicdust.Reconnector = (*GRPC)(nil)
)
