package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/c12s/cosmicdust/pkg/api"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// NATS sends every chunk as a message on <subject>.files.<node> with a reply
// subject of its own and takes the acks in order, up to Options.Window chunks
// in flight once the probe has been answered. NATS keeps the order of one
// publisher's messages to one subscriber, so the receiver sees a file's chunks
// in order.
type NATS struct {
	opts                        Options
	files, urgent               *nats.Conn
	filesSubject, urgentSubject string
	cnt                         *counter
	lim                         *limiter
	up                          up
}

// NewNATS connects to the server at url, in the background: a server that is
// down is a failed upload, retried later, not a failed start.
func NewNATS(url string, opts Options) (*NATS, error) {
	if err := opts.defaults(); err != nil {
		return nil, err
	}
	cnt := &counter{}
	connect := func(role string, more ...nats.Option) (*nats.Conn, error) {
		return nats.Connect(url, append([]nats.Option{
			nats.Name("cosmicdust-" + role + "-" + opts.Node),
			nats.SetCustomDialer(cnt),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
		}, more...)...)
	}
	u := newUp()
	files, err := connect("files",
		nats.ConnectHandler(func(*nats.Conn) { u.signal() }),
		nats.ReconnectHandler(func(*nats.Conn) { u.signal() }))
	if err != nil {
		return nil, fmt.Errorf("nats: %w", err)
	}
	urgent := files
	if !opts.SharedUrgent {
		if urgent, err = connect("urgent"); err != nil {
			files.Close()
			return nil, fmt.Errorf("nats: %w", err)
		}
	}
	t := &NATS{opts: opts, files: files, urgent: urgent, cnt: cnt, lim: newLimiter(opts.RateBytes), up: u,
		filesSubject: api.FilesSubject(opts.Subject, opts.Node), urgentSubject: api.UrgentSubject(opts.Subject, opts.Node)}
	if err := cnt.register(opts.Registerer, "nats"); err != nil {
		_ = t.Close()
		return nil, err
	}
	return t, nil
}

// Upload sends one file: a probe, then the chunks from where the receiver's
// copy ends.
func (t *NATS) Upload(ctx context.Context, ref cosmicdust.FileRef, r io.Reader) (cosmicdust.Ack, error) {
	pref, err := fileRef(t.opts.Node, ref)
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	// With RetryOnFailedConnect the connection is made in the background:
	// wait for it a little rather than fail an upload a millisecond early.
	if !connected(ctx, t.files, t.opts.AckTimeout) {
		return cosmicdust.Ack{}, errors.New("nats: not connected")
	}
	inbox := t.files.NewInbox()
	sub, err := t.files.SubscribeSync(inbox)
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("nats: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	outstanding := 0
	publish := func(c *api.Chunk) error {
		data, err := proto.Marshal(c)
		if err != nil {
			return err
		}
		if err := t.files.PublishMsg(&nats.Msg{Subject: t.filesSubject, Reply: inbox, Data: data}); err != nil {
			return fmt.Errorf("nats: %w", err)
		}
		outstanding++
		return nil
	}
	next := func() (*api.Ack, error) {
		actx, cancel := context.WithTimeout(ctx, t.opts.AckTimeout)
		defer cancel()
		m, err := sub.NextMsgWithContext(actx)
		if err != nil {
			return nil, fmt.Errorf("nats: waiting for an ack: %w", err)
		}
		outstanding--
		if len(m.Data) == 0 && m.Header.Get("Status") == "503" {
			return nil, nats.ErrNoResponders // nobody listens on the subject
		}
		var a api.Ack
		if err := proto.Unmarshal(m.Data, &a); err != nil {
			return nil, fmt.Errorf("nats: ack: %w", err)
		}
		return &a, nil
	}

	// The probe: the broker takes messages whether or not a receiver
	// listens, and without it every retry during an outage sent a chunk -
	// often a whole small file - for nothing.
	id := attempt()
	t.cnt.probes.Add(1)
	if err := publish(&api.Chunk{File: pref, Probe: true, Upload: id}); err != nil {
		return cosmicdust.Ack{}, err
	}
	a, err := next()
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	if a.Duplicate || a.Stored || !a.Ok {
		return ackOf(a), nil
	}
	start := resumeAt(a.Have, pref.Size)
	if err := skip(r, start); err != nil {
		return cosmicdust.Ack{}, err
	}
	var final *api.Ack
	err = chunks(r, pref, id, start, t.opts.ChunkBytes, t.lim, func(c *api.Chunk) error {
		if err := publish(c); err != nil {
			return err
		}
		t.cnt.chunks.Add(1)
		for outstanding >= t.opts.Window || (c.Last && outstanding > 0) {
			a, err := next()
			if err != nil {
				return err
			}
			if a.Duplicate || a.Stored || !a.Ok {
				final = a
				return errDone
			}
		}
		return nil
	})
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	if final == nil {
		return cosmicdust.Ack{}, errors.New("nats: the upload ended without an answer")
	}
	return ackOf(final), nil
}

// Urgent sends one event and waits for its answer.
func (t *NATS) Urgent(ctx context.Context, e cosmicdust.Event) (cosmicdust.Ack, error) {
	data, err := proto.Marshal(event(t.opts.Node, e))
	if err != nil {
		return cosmicdust.Ack{}, err
	}
	t.cnt.ev.Add(1)
	m, err := t.urgent.RequestWithContext(ctx, t.urgentSubject, data)
	if err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("nats: %w", err)
	}
	var a api.Ack
	if err := proto.Unmarshal(m.Data, &a); err != nil {
		return cosmicdust.Ack{}, fmt.Errorf("nats: ack: %w", err)
	}
	return ackOf(&a), nil
}

// Close closes the connections.
func (t *NATS) Close() error {
	if t.urgent != t.files {
		t.urgent.Close()
	}
	t.files.Close()
	return nil
}

// Reconnected receives a value when the connection for files is made, the
// first time or again: the client tries every second.
func (t *NATS) Reconnected() <-chan struct{} { return t.up }

func connected(ctx context.Context, nc *nats.Conn, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for !nc.IsConnected() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

var (
	_ cosmicdust.Transport   = (*NATS)(nil)
	_ cosmicdust.Reconnector = (*NATS)(nil)
)
