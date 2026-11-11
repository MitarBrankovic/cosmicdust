package cosmicdust

import (
	"strconv"
	"time"
)

// Runtimes an event or a container can come from.
const (
	RuntimeDocker     = "docker"
	RuntimeContainerd = "containerd"
)

// State is the container state the agent derives from events and
// reconciliation. It is smaller than what a runtime reports: these are the
// states a log collector has to tell apart to know whether to keep reading a
// log, close it, or start reading a new incarnation.
type State string

const (
	StateUnknown    State = "unknown"
	StateCreated    State = "created"
	StateRunning    State = "running"
	StateExited     State = "exited"
	StateRestarting State = "restarting"
	StateRemoved    State = "removed"
)

// Kind is the transition an event stands for. Each runtime adapter maps its
// own action names onto it, so the lifecycle tracker never has to know them.
type Kind int

const (
	KindOther   Kind = iota // carries no state change (kill, stop, pause, health status, ...)
	KindCreate              // the container exists
	KindStart               // an incarnation started
	KindExit                // the incarnation's process ended
	KindOOM                 // the kernel's OOM killer hit the container; the exit follows
	KindRestart             // Docker's "restart", sent after the new start: informational
	KindRemove              // the container is gone

	// KindResync is not about a container. A source sends it after every
	// connect, the first one included: what happened while nobody listened
	// is lost, and the tracker reconciles against the runtime's listing.
	KindResync
)

var kindNames = [...]string{"other", "create", "start", "exit", "oom", "restart", "remove", "resync"}

func (k Kind) String() string {
	if k >= 0 && int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Position identifies a byte in a log file by inode rather than by name. Names
// move during rotation, inodes do not, so a position that has to survive a
// restart carries the inode; the path is only a hint for where to look first.
//
// An inode alone is not enough: the kernel hands a freed inode to the next file
// created, so after a restart "the same inode" can be a different file. Head is
// a fingerprint of the file's first line, which never changes once written.
// Inode and Head together identify the file, and with Offset every line, which
// is what makes deduplication on the control plane exact.
type Position struct {
	Path   string
	Inode  uint64
	Head   uint64
	Offset int64
}

// ContainerRef is what the agent needs to know to read one container's log and
// to label its rows.
type ContainerRef struct {
	ID        string
	Name      string
	Image     string
	App       string // for retention on the control plane: from a label, the compose service, or the image
	Runtime   string // RuntimeDocker or RuntimeContainerd
	Namespace string // containerd namespace; "moby" for containers run by Docker
	LogPath   string
	LogFormat string // "json-file", "cri", or the name of a driver whose log is not a file
	TTY       bool   // with a terminal the runtime does not split stdout from stderr

	// LogKeep is how much of the log the runtime keeps before it deletes the
	// oldest part: max-size times max-file of Docker's json-file. A stop of
	// the agent longer than LogKeep over the rate the log is written loses
	// lines. 0: not rotated, or not known (the kubelet's setting).
	LogKeep int64
}

// ContainerState is one container as a runtime reports it on a listing or
// when asked about it: the ground truth reconciliation compares the tracked
// state against. Times are zero where the runtime does not say.
type ContainerState struct {
	ContainerRef
	State      State
	StartedAt  time.Time // start of the current or the last incarnation
	FinishedAt time.Time // end of the last incarnation, if it has ended
	ExitCode   *int      // of the last incarnation, if it has ended
	OOMKilled  bool
	Restarts   int // the runtime's own count, informational
}

// Event is one lifecycle event, normalised so that one from Docker and one from
// containerd have the same shape.
type Event struct {
	Time        time.Time // as stamped by the runtime
	Runtime     string
	Namespace   string
	Action      string // the runtime's own name: "die", "/tasks/exit", ...
	Kind        Kind
	ContainerID string
	Name        string
	Image       string
	App         string
	ExitCode    *int
	OOMKilled   bool // on an exit: the kernel's counter says the OOM killer hit this incarnation
	Fields      map[string]string

	// Inferred marks an event no runtime sent: reconciliation derived it
	// from a listing, after a gap in the stream.
	Inferred bool

	// State and Epoch are the container's after the event, as the tracker
	// derived it from every source; sources leave them empty.
	State State
	Epoch int
}

// Record is one log message on its way from a source to the store. Node, app,
// image and container state are not here: the store adds them when it writes
// the row, from the tracked state at the time of the line.
type Record struct {
	Time        time.Time // as stamped by the runtime
	ReadAt      time.Time // when the agent read it
	ContainerID string
	Stream      string // stdout or stderr
	Source      string // file-json, file-cri or api

	// Line aliases the reader's buffer and is valid only until the next call to
	// LogReader.Next. Whoever keeps it longer copies it; the store does, into
	// its WAL block, which is how the hot path avoids a string per line.
	Line []byte

	// Start is the first byte of the first physical line of the message; zero
	// for the API source, which has no notion of a position.
	Start     Position
	Parts     int  // more than one when the runtime split the message
	Truncated bool // longer than the agent's line limit

	// Commit is where a restart may resume once this record is on disk. It is
	// not always the end of the record: while another stream of the same file
	// still has parts pending, resuming after this record would skip them.
	Commit Position
}

// FileRef is one finished file in the local store, the unit of sending: it is
// sent once, acknowledged once and deleted once.
type FileRef struct {
	Table   string // "logs" or "events"
	Path    string // relative to the store root: logs/dt=2026-10-20/hour=14/<name>.parquet
	Size    int64
	CRC     uint32 // with Path, what the control plane deduplicates on
	Rows    int64
	MinTime time.Time
	MaxTime time.Time
}

// AckStatus is the control plane's answer to one file or event.
type AckStatus int

const (
	AckStored    AckStatus = iota + 1 // stored now
	AckDuplicate                      // stored before; as good as AckStored for the node
	AckRejected                       // refused this time, e.g. a CRC mismatch; Reason says why. The data stays and goes again
)

// Ack is the control plane's answer. A node deletes data only after
// AckStored or AckDuplicate.
type Ack struct {
	Status AckStatus
	Reason string
}

// Stored reports whether the control plane has the data, so the node may
// forget it.
func (a Ack) Stored() bool { return a.Status == AckStored || a.Status == AckDuplicate }
