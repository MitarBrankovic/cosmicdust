package cosmicdust

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrClaimed is Store.Claim's answer for a file someone else is working with,
// such as compaction.
var ErrClaimed = errors.New("cosmicdust: the file is being worked with")

// EventSource is one container runtime: Docker or containerd.
//
// Events are only a hint. A subscription can drop (a containerd restart ends
// it), nothing replays what happened while nobody listened, and events can
// arrive out of order. The tracked state is therefore reconciled against List:
// after every connect, and periodically as a safety net where listing is cheap.
type EventSource interface {
	// Runtime names the source: RuntimeDocker or RuntimeContainerd.
	Runtime() string

	// Watch sends the runtime's container events to out until ctx ends,
	// reconnecting on its own when the stream breaks. After every connect,
	// the first one included, it sends an event of KindResync before any
	// other, so that the receiver reconciles while the new stream is already
	// open: a change during the listing is then seen twice rather than
	// missed. Watch returns nil when ctx ends, and an error only for a fault
	// that retrying cannot fix.
	Watch(ctx context.Context, out chan<- Event) error

	// List reports the containers the runtime knows, as of Listing.At.
	List(ctx context.Context) (Listing, error)

	// Describe reports one container, with what a listing may leave out: the
	// log path and format, the times of its last incarnation.
	Describe(ctx context.Context, id string) (ContainerState, error)
}

// Listing is what a runtime reported about its containers at one moment.
type Listing struct {
	// At is when the listing started. Inferred events are stamped no later,
	// so they never override an event the stream delivered meanwhile.
	At         time.Time
	Containers []ContainerState

	// RunningOnly holds the namespaces in which the runtime lists running
	// containers only. containerd deletes the object of a Docker container
	// when it stops, so in Docker's namespace a container missing from its
	// listing has stopped, not necessarily gone. Anywhere else a missing
	// container has been removed.
	RunningOnly map[string]bool
}

// LogSource opens the log of one container: the runtime's json-file or CRI
// file (the primary path), or the Docker API for other log drivers.
type LogSource interface {
	// Open starts reading at from, or at the start of the current file when
	// from is zero.
	Open(ctx context.Context, c ContainerRef, from Position) (LogReader, error)
}

// LogReader reads one container's log, following rotation.
type LogReader interface {
	// Next fills buf with the records read so far and returns how many,
	// without waiting: 0 and a nil error mean nothing new. The records' Line
	// fields stay valid until the next call.
	Next(buf []Record) (int, error)

	// Position is where a restart may resume once everything Next returned
	// is stored: past the last record, never past the start of a message
	// still waiting for its parts.
	Position() Position

	Close() error
}

// Store is the node's buffer of finished files, as the sync engine sees it:
// hourly Parquet files with a manifest. Records and events reach it through
// the WAL, where data and the positions it came from become durable in one
// fsync.
type Store interface {
	// Unsynced lists finished files the control plane has not acknowledged.
	Unsynced(ctx context.Context) ([]FileRef, error)

	// Claim opens a file for sending and keeps compaction away from it until
	// done is called with the control plane's answer; a stored Ack marks the
	// file synced. It fails with ErrClaimed for a file someone else is
	// working with, and with an error wrapping fs.ErrNotExist for one that
	// is gone (merged or deleted since it was listed): both are for later,
	// not failures.
	Claim(ref FileRef) (r io.ReadCloser, done func(Ack) error, err error)

	// GC deletes synced files, oldest first, until the store is under quota
	// bytes.
	GC(quota int64) error
}

// Codec compresses one block. s2 for WAL blocks, zstd for Parquet columns and
// for the urgent stream.
type Codec interface {
	Compress(dst, src []byte) []byte
	Decompress(dst, src []byte) ([]byte, error)
}

// Transport carries files and urgent events to the control plane: NATS by
// default, gRPC as a second implementation. The sync engine does not know which
// one it has.
type Transport interface {
	// Upload sends one file in chunks, resuming after the last acknowledged
	// chunk if a previous attempt broke off, and returns the control plane's
	// answer for the whole file.
	Upload(ctx context.Context, ref FileRef, r io.Reader) (Ack, error)

	// Urgent sends one event at once, over its own connection so that it does
	// not wait behind a file chunk. It is a shortcut: the event also reaches
	// the control plane with the events table.
	Urgent(ctx context.Context, e Event) (Ack, error)

	Close() error
}

// Reconnector is a Transport that says when its connection for files is up
// again, so that the sync engine sends at once rather than waiting out its
// backoff, up to a minute after a long outage.
type Reconnector interface {
	// Reconnected receives a value whenever the connection comes up; one
	// that is not taken is not repeated.
	Reconnected() <-chan struct{}
}
