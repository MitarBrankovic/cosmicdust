// Package transport carries a node's finished files and urgent events to the
// control plane, over NATS or gRPC, as a cosmicdust.Transport. The sync engine does
// not know which one it has: both speak the protocol of package api.
//
// A file goes in chunks after a probe, which tells whether the receiver is
// there, whether it already has the file, and how much of it it holds from an
// upload that broke off: the upload continues from there instead of starting
// over, which on a slow lossy link is what lets a file arrive at all.
// Urgent events go on a connection of their own, so that they do not wait
// behind a file in the socket buffers (p95 2-22 s on the same connection,
// 0.3-0.5 s on their own).
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/pkg/api"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Options configure a transport. Zero values take the defaults.
type Options struct {
	// Node is the node's identity, sent with every file and event. Required.
	Node string

	// ChunkBytes is the size of a file chunk; default 256 KiB. On a narrow
	// link it is also a latency: a chunk on its way holds the link.
	ChunkBytes int

	// Window is how many chunks NATS keeps in flight before it waits for an
	// ack, after a first one has come back; default 32, about the
	// bandwidth-delay product of 100 ms at 30 MB/s. gRPC has HTTP/2
	// flow control instead.
	Window int

	// AckTimeout is how long NATS waits for the next ack. Every ack is
	// progress and starts the wait again, so a slow link only needs time for
	// one chunk; default 30 s (a fixed 5 s per chunk never got a 4
	// MB file through 1 Mbit/s with 5 % loss).
	AckTimeout time.Duration

	// RateBytes caps the file bytes sent per second, a token bucket with one
	// second of burst; 0: no cap. Urgent events are never held back.
	RateBytes int64

	// SharedUrgent sends urgent events on the files' connection instead of
	// one of their own.
	SharedUrgent bool

	// Subject is the NATS subject prefix: files go on <Subject>.files.<node>,
	// urgent events on <Subject>.urgent.<node>. Default "cosmicdust".
	Subject string

	Logger     *slog.Logger
	Registerer prometheus.Registerer
}

func (o *Options) defaults() error {
	if o.Node == "" || strings.ContainsAny(o.Node, ".*> \t\r\n") {
		return fmt.Errorf("transport: node %q cannot name a subject", o.Node)
	}
	if o.ChunkBytes <= 0 {
		o.ChunkBytes = 256 << 10
	}
	if o.Window <= 0 {
		o.Window = 32
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = 30 * time.Second
	}
	if o.Subject == "" {
		o.Subject = "cosmicdust"
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return nil
}

// up tells the sync engine that the connection for files came up
// (cosmicdust.Reconnector). A signal nobody took is not repeated.
type up chan struct{}

func newUp() up { return make(up, 1) }

func (u up) signal() {
	select {
	case u <- struct{}{}:
	default:
	}
}

// New makes the transport a URL names: nats://host:port for NATS,
// grpc://host:port for gRPC.
func New(rawURL string, opts Options) (cosmicdust.Transport, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("transport: %q: %w", rawURL, err)
	}
	switch u.Scheme {
	case "nats", "tls":
		return NewNATS(rawURL, opts)
	case "grpc":
		return NewGRPC(u.Host, opts)
	}
	return nil, fmt.Errorf("transport: %q is neither nats:// nor grpc://", rawURL)
}

// fileRef turns a store file into the protocol's reference. The store's path
// is <table>/dt=<day>/hour=<hour>/<name>.
func fileRef(node string, ref cosmicdust.FileRef) (*api.FileRef, error) {
	parts := strings.Split(ref.Path, "/")
	if len(parts) != 4 || parts[0] != ref.Table {
		return nil, fmt.Errorf("transport: %q is not a store file", ref.Path)
	}
	return &api.FileRef{
		Node: node, Table: ref.Table, Partition: parts[1] + "/" + parts[2], Name: parts[3],
		Crc32C: ref.CRC, Size: ref.Size, Rows: ref.Rows,
		MinTs: ref.MinTime.UnixNano(), MaxTs: ref.MaxTime.UnixNano(),
	}, nil
}

func event(node string, e cosmicdust.Event) *api.Event {
	code := int32(-1)
	if e.ExitCode != nil {
		code = int32(*e.ExitCode)
	}
	return &api.Event{
		Node: node, Ts: e.Time.UnixNano(), Runtime: e.Runtime, Namespace: e.Namespace,
		ContainerId: e.ContainerID, Name: e.Name, Image: e.Image, App: e.App,
		Action: e.Action, Kind: e.Kind.String(), ExitCode: code, OomKilled: e.OOMKilled,
		Epoch: int32(e.Epoch), SentAt: time.Now().UnixNano(),
	}
}

// ackOf reads the receiver's answer. A refusal is an answer, not a failure of
// the transport: the file stays unsynced and is offered again.
func ackOf(a *api.Ack) cosmicdust.Ack {
	switch {
	case a.GetDuplicate():
		return cosmicdust.Ack{Status: cosmicdust.AckDuplicate}
	case a.GetStored():
		return cosmicdust.Ack{Status: cosmicdust.AckStored}
	}
	reason := a.GetError()
	if reason == "" {
		reason = "refused without a reason"
	}
	return cosmicdust.Ack{Status: cosmicdust.AckRejected, Reason: reason}
}

// skip moves r to offset, by seeking when it can.
func skip(r io.Reader, offset int64) error {
	if offset == 0 {
		return nil
	}
	if s, ok := r.(io.Seeker); ok {
		_, err := s.Seek(offset, io.SeekStart)
		return err
	}
	_, err := io.CopyN(io.Discard, r, offset)
	return err
}

// attempt names one upload of a file (api.Chunk.upload), never 0.
func attempt() uint64 { return rand.Uint64() | 1 }

// chunks reads r from offset into chunks of at most size bytes, of the upload
// attempt, and calls send for each, in order. The last one is flagged; a file
// that ends at offset is one empty last chunk, which completes an upload whose
// data had all arrived. send may return errDone to end early without an error.
func chunks(r io.Reader, ref *api.FileRef, upload uint64, offset int64, size int, lim *limiter, send func(*api.Chunk) error) error {
	buf := make([]byte, size)
	next := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	for {
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return err
		}
		// Read one chunk ahead to know whether this one is the last. A read
		// error there ends the upload: taking it for the end of the file
		// would send a short file as complete (an earlier version did).
		last := err != nil
		var m int
		var nerr error
		if !last {
			m, nerr = io.ReadFull(r, next)
			if nerr != nil && !errors.Is(nerr, io.ErrUnexpectedEOF) && !errors.Is(nerr, io.EOF) {
				return nerr
			}
			last = m == 0
		}
		lim.wait(n)
		c := &api.Chunk{File: ref, Offset: offset, Data: buf[:n], Last: last, Upload: upload}
		if serr := send(c); serr != nil {
			if errors.Is(serr, errDone) {
				return nil
			}
			return serr
		}
		if last {
			return nil
		}
		offset += int64(n)
		buf, next = next, buf
		n, err = m, nerr
	}
}

// errDone ends an upload the receiver has answered for good.
var errDone = errors.New("transport: answered")

// resumeAt is where an upload continues: after what the receiver holds, if
// that is a part of this file.
func resumeAt(have, size int64) int64 {
	if have > 0 && have <= size {
		return have
	}
	return 0
}

// limiter paces file bytes: a token bucket holding at most one second of
// tokens, so that a catch-up after an outage cannot flood a narrow link. A nil
// limiter does not limit.
type limiter struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	tokens float64
	last   time.Time
	now    func() time.Time
	sleep  func(time.Duration)
}

func newLimiter(bytesPerSecond int64) *limiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	return &limiter{rate: float64(bytesPerSecond), tokens: float64(bytesPerSecond), now: time.Now, sleep: time.Sleep}
}

func (l *limiter) wait(n int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	now := l.now()
	if !l.last.IsZero() {
		l.tokens = min(l.rate, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	}
	l.last = now
	l.tokens -= float64(n)
	var d time.Duration
	if l.tokens < 0 {
		d = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	if d > 0 {
		l.sleep(d)
	}
}

// counter counts the bytes a transport's connections write and read, framing
// and acks included, and the messages it sends.
type counter struct {
	out, in            atomic.Int64
	chunks, probes, ev atomic.Int64
}

func (c *counter) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, c: c}, nil
}

// Dial makes counter a nats.CustomDialer.
func (c *counter) Dial(network, addr string) (net.Conn, error) {
	return c.dial(context.Background(), network, addr)
}

type countingConn struct {
	net.Conn
	c *counter
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.c.out.Add(int64(n))
	return n, err
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.c.in.Add(int64(n))
	return n, err
}

func (c *counter) register(reg prometheus.Registerer, kind string) error {
	if reg == nil {
		return nil
	}
	labels := prometheus.Labels{"transport": kind}
	fn := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "cosmicdust_agent", Subsystem: "transport", Name: name,
			Help: help, ConstLabels: labels}, f)
	}
	for _, col := range []prometheus.Collector{
		fn("sent_bytes_total", "Bytes written to the control plane's connections, framing included.",
			func() float64 { return float64(c.out.Load()) }),
		fn("received_bytes_total", "Bytes read from the control plane's connections.",
			func() float64 { return float64(c.in.Load()) }),
		fn("chunks_total", "File chunks sent.", func() float64 { return float64(c.chunks.Load()) }),
		fn("probes_total", "Probes sent, one before every file.", func() float64 { return float64(c.probes.Load()) }),
		fn("urgent_total", "Urgent events sent.", func() float64 { return float64(c.ev.Load()) }),
	} {
		if err := reg.Register(col); err != nil {
			return fmt.Errorf("transport: register metrics: %w", err)
		}
	}
	return nil
}
