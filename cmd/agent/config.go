package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type config struct {
	node            string
	dataDir         string
	listen          string
	pprof           bool
	shutdownTimeout time.Duration
	memoryLimit     size
	logLevel        slog.Level
	logFormat       string
	version         bool

	docker              bool
	dockerHost          string
	dockerReconcile     time.Duration
	containerd          bool
	containerdAddress   string
	containerdNS        list
	containerdReconcile time.Duration
	appLabels           list
	cgroupRoot          string
	snapshotEvery       time.Duration

	poll       time.Duration
	pollMax    time.Duration
	readBuf    size
	maxLine    size
	maxMessage size
	skipExited bool

	walSegmentBytes size
	walSegmentAge   time.Duration
	convertEvery    time.Duration
	compactEvery    time.Duration
	convertBytes    size
	sortBytes       size
	pageBytes       size
	diskReserve     size
	storeQuota      size
	whenFull        string

	syncURL        string
	syncEvery      time.Duration
	syncChunk      size
	syncWindow     int
	syncAckTimeout time.Duration
	syncRate       size
	syncUrgent     string
}

// envPrefix is prepended to a flag's name, upper-cased with dashes turned into
// underscores, to give the environment variable that sets it: -data-dir is
// COSMICDUST_DATA_DIR. In a container the rest of the stack is configured through
// the environment, and the daemon fits in the same way.
const envPrefix = "COSMICDUST_"

// parseConfig reads flags, then the environment for every flag not given on
// the command line, then defaults. lookupEnv is os.LookupEnv outside tests.
func parseConfig(args []string, lookupEnv func(string) (string, bool), errOut io.Writer) (config, error) {
	var cfg config
	cfg.memoryLimit = 64 << 20
	fs := flag.NewFlagSet("cosmicdust-agent", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.StringVar(&cfg.node, "node", "", "node identity stored with every row (empty: kept in <data-dir>/node-id, made on first start)")
	fs.StringVar(&cfg.dataDir, "data-dir", "/var/lib/cosmicdust", "directory for the node identity, the WAL and the local store")
	fs.StringVar(&cfg.listen, "listen", ":9101", "address of /metrics and /healthz")
	fs.BoolVar(&cfg.pprof, "pprof", false, "serve /debug/pprof/ on the same address")
	fs.DurationVar(&cfg.shutdownTimeout, "shutdown-timeout", 30*time.Second, "longest a clean stop may take")
	fs.Var(&cfg.memoryLimit, "memory-limit", "soft memory limit for the Go runtime, e.g. 64MiB (0: none; GOMEMLIMIT, if set, wins)")
	fs.TextVar(&cfg.logLevel, "log-level", slog.LevelInfo, "debug, info, warn or error")
	fs.StringVar(&cfg.logFormat, "log-format", "text", "text or json")
	fs.BoolVar(&cfg.version, "version", false, "print the version and exit")

	fs.BoolVar(&cfg.docker, "docker", true, "follow dockerd")
	fs.StringVar(&cfg.dockerHost, "docker-host", "", "dockerd's address (empty: DOCKER_HOST or the default socket)")
	fs.DurationVar(&cfg.dockerReconcile, "docker-reconcile", 0, "also reconcile with Docker this often (0: only after a connect; a listing costs dockerd and containerd ~10 ms of CPU per container)")
	fs.BoolVar(&cfg.containerd, "containerd", true, "follow containerd (needs root for its socket)")
	fs.StringVar(&cfg.containerdAddress, "containerd-address", "/run/containerd/containerd.sock", "containerd's socket")
	cfg.containerdNS = list{"moby", "default"}
	fs.Var(&cfg.containerdNS, "containerd-namespaces", "containerd namespaces to follow, comma separated")
	fs.DurationVar(&cfg.containerdReconcile, "containerd-reconcile", 5*time.Minute, "also reconcile with containerd this often (0: only after a connect)")
	fs.Var(&cfg.appLabels, "app-labels", "container label keys that name the application, comma separated, e.g. org,namespace,app (default: the Compose service, then the image)")
	fs.StringVar(&cfg.cgroupRoot, "cgroup-root", "/sys/fs/cgroup", "cgroup v2 root, for the kernel's OOM counters (empty: no check)")
	fs.DurationVar(&cfg.snapshotEvery, "snapshot-every", 10*time.Second, "write the container state to <data-dir> this often when it changed")

	fs.DurationVar(&cfg.poll, "poll", 250*time.Millisecond, "check a log file with recent data this often")
	fs.DurationVar(&cfg.pollMax, "poll-max", 5*time.Second, "check a log file that has been quiet this long this rarely")
	cfg.readBuf, cfg.maxLine, cfg.maxMessage = 16<<10, 1<<20, 256<<10
	fs.Var(&cfg.readBuf, "read-buf", "read buffer per followed log file")
	fs.Var(&cfg.maxLine, "max-line", "longest physical log line kept; longer ones are skipped")
	fs.Var(&cfg.maxMessage, "max-message", "longest log message kept; longer ones are cut")
	fs.BoolVar(&cfg.skipExited, "skip-exited", false, "do not read what containers that are not running when first seen left in their logs")

	cfg.walSegmentBytes = 8 << 20
	fs.Var(&cfg.walSegmentBytes, "wal-segment-bytes", "seal a WAL segment at this size")
	fs.DurationVar(&cfg.walSegmentAge, "wal-segment-age", 5*time.Second, "or when it has been open this long")
	fs.DurationVar(&cfg.convertEvery, "convert-every", 10*time.Second, "turn sealed WAL segments into Parquet this often")
	fs.DurationVar(&cfg.compactEvery, "compact-every", 30*time.Second, "look for finished hours to compact this often")
	cfg.convertBytes, cfg.sortBytes, cfg.pageBytes = 32<<20, 4<<20, 1<<20
	fs.Var(&cfg.convertBytes, "convert-bytes", "log text one conversion takes; a backlog is converted in pieces this size")
	fs.Var(&cfg.sortBytes, "sort-bytes", "log text sorted at a time while converting; this much waits in memory per hour being written")
	fs.Var(&cfg.pageBytes, "parquet-page-bytes", "Parquet page size; every column of a file being written keeps a buffer this size")
	cfg.diskReserve, cfg.storeQuota = 64<<20, 2<<30
	fs.Var(&cfg.diskReserve, "disk-reserve", "keep this much free on the store's disk, besides room to convert the WAL (0: no check)")
	fs.Var(&cfg.storeQuota, "store-quota", "most the store may hold, WAL included (0: no quota)")
	fs.StringVar(&cfg.whenFull, "when-full", "evict", "out of room with nothing synced to delete: evict (drop the oldest unsynced logs, each recorded as a logs_dropped event) or block (stop reading)")

	fs.StringVar(&cfg.syncURL, "sync-url", "", "the control plane: nats://host:4222 or grpc://host:port (empty: no sync, the store keeps everything within its quota)")
	fs.DurationVar(&cfg.syncEvery, "sync-every", 10*time.Second, "send the finished files this often")
	cfg.syncChunk = 256 << 10
	fs.Var(&cfg.syncChunk, "sync-chunk-bytes", "a file goes in chunks of this size")
	fs.IntVar(&cfg.syncWindow, "sync-window", 32, "NATS: chunks in flight before waiting for an ack")
	fs.DurationVar(&cfg.syncAckTimeout, "sync-ack-timeout", 30*time.Second, "NATS: the longest wait for the next ack")
	fs.Var(&cfg.syncRate, "sync-rate", "cap on the file bytes sent per second (0: none); urgent events are never held back")
	fs.StringVar(&cfg.syncUrgent, "sync-urgent", "separate", "urgent events (a container that died with an error or for memory): separate (their own connection), shared (the files' one) or off (only with the events table)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if err := fromEnv(fs, lookupEnv); err != nil {
		return cfg, err
	}
	if cfg.logFormat != "text" && cfg.logFormat != "json" {
		return cfg, fmt.Errorf("-log-format: %q is neither text nor json", cfg.logFormat)
	}
	if cfg.dataDir == "" {
		return cfg, errors.New("-data-dir is empty")
	}
	if !cfg.docker && !cfg.containerd {
		return cfg, errors.New("neither -docker nor -containerd: nothing to follow")
	}
	if cfg.whenFull != "evict" && cfg.whenFull != "block" {
		return cfg, fmt.Errorf("-when-full: %q is neither evict nor block", cfg.whenFull)
	}
	switch cfg.syncUrgent {
	case "separate", "shared", "off":
	default:
		return cfg, fmt.Errorf("-sync-urgent: %q is none of separate, shared, off", cfg.syncUrgent)
	}
	if cfg.containerd && len(cfg.containerdNS) == 0 {
		return cfg, errors.New("-containerd-namespaces is empty")
	}
	return cfg, nil
}

// list is a comma-separated flag value.
type list []string

func (l *list) Set(v string) error {
	*l = nil
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			*l = append(*l, s)
		}
	}
	return nil
}

func (l list) String() string { return strings.Join(l, ",") }

// fromEnv sets every flag not given on the command line from its environment
// variable, if that is set.
func fromEnv(fs *flag.FlagSet, lookupEnv func(string) (string, bool)) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	var errs []error
	fs.VisitAll(func(f *flag.Flag) {
		if given[f.Name] || f.Name == "version" {
			return
		}
		key := envName(f.Name)
		if v, ok := lookupEnv(key); ok {
			if err := f.Value.Set(v); err != nil {
				errs = append(errs, fmt.Errorf("%s=%q: %w", key, v, err))
			}
		}
	})
	return errors.Join(errs...)
}

func envName(flagName string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// size is a byte count written as a plain number or with a binary suffix,
// the way GOMEMLIMIT is written.
type size int64

var sizeUnits = []struct {
	suffix string
	mult   int64
}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}

func (s *size) Set(v string) error {
	v = strings.TrimSpace(v)
	mult := int64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(v, u.suffix) {
			v, mult = strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("%q is not a size like 64MiB", v)
	}
	if n > (1<<63-1)/mult {
		return fmt.Errorf("%d×%d overflows", n, mult)
	}
	*s = size(n * mult)
	return nil
}

func (s size) String() string {
	for _, u := range sizeUnits {
		if s != 0 && int64(s)%u.mult == 0 {
			return strconv.FormatInt(int64(s)/u.mult, 10) + u.suffix
		}
	}
	return "0"
}
