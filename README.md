# cosmicdust

Log collection for containers on edge nodes.

An **agent** on every node follows Docker and containerd, collects the logs of the containers and the events of their
lifecycle, and keeps them on the node's disk - compressed, in Parquet files partitioned by time - until the control
plane has acknowledged them. A **server** on the control plane receives the files, archives them by application and
makes them searchable.

This repository holds the agent and the protocol it sends with. The server follows; until then
`internal/servertest` is the receiving side, used by the tests.

The agent is built for nodes with little CPU and memory to spare, on links that are slow or go away, with disks that
fill:

- what a container wrote survives a crash of the agent or of the node, and a link that is down for hours;
- a file is sent until the receiver has it, and a receiver can tell a file it already has: every line is delivered
  at least once and can be stored exactly once;
- the agent's cost is bounded: one process, a soft memory limit, a disk quota, a rate limit for the link;
- when the disk does fill, the agent records what it dropped instead of stopping.

## How it works

```
 node                                                             control plane

 dockerd, containerd ──events──▶ lifecycle ──┐
                                             ├─▶ WAL ─▶ Parquet ─▶ sync ══ gRPC ══▶ receiver
 container log files ────────────▶ tailer ───┘         by hour
```

**Collecting.** Lifecycle events come from both runtimes and are merged into one state per container; after every
reconnect the state is reconciled with a listing, and an out-of-memory kill is confirmed from the kernel's counters
([0004](docs/design/0004-container-state.md)). Logs are read from the files the runtime writes (json-file and CRI),
by one scheduler for all files, which follows rotations and backs off on quiet files
([0005](docs/design/0005-tailer.md)).

**Storing.** Lines and events go to a write-ahead log together with the position reading resumes from
([0006](docs/design/0006-wal.md)), and from there into Parquet files by hour, sorted and compressed with zstd, each
line with its container, its application and the container's state at that moment
([0007](docs/design/0007-parquet-store.md), [0009](docs/design/0009-streaming-conversion.md)).

**Sending.** At intervals (10 s by default) the finished files are sent whole, in chunks, after a probe that tells
whether the receiver already has the file or a part of it, so a broken upload is continued and not started over. A
container that died with an error or was killed for memory is also reported at once, on a connection of its own
([0010](docs/design/0010-sync.md)). The protocol is in [`pkg/api`](pkg/api/proto/sync.proto); it travels over gRPC
or, as an option, over NATS.

The decisions and the measurements behind them are in [`docs/design`](docs/design/README.md).

## Guarantees and limits

| | |
|---|---|
| The agent is killed (`kill -9`, power loss) | A line and the position its reading resumes from become durable in the same `fsync`, so reading continues exactly after the last stored line: no gap, no duplicate. |
| The receiver or the link is away | Finished files wait on the node, within the quota. When the connection is back the backlog goes at once, and an upload that broke off continues where it stopped. |
| A file is sent twice | A file is identified by node, table, partition, name, size and CRC-32C, so the receiver can answer `duplicate` to the second copy; the node marks the file synced on either answer. |
| The node's disk fills | Synced files go first. Then, by `-when-full`, either the oldest unsynced logs are dropped and a `logs_dropped` event records how many and from when (`evict`), or reading stops until there is room (`block`). Lifecycle events are never dropped. |

What it does not do:

- Lines the runtime rotated away while the agent was stopped are gone. The agent counts them, and
  `cosmicdust_agent_tail_retention_seconds` says how long the shortest log on the node is kept.
- Only json-file and CRI log files are read. Containers with another log driver are counted and left unread.
- The immediate report of a failed container is a shortcut and is tried for a limited time; the event itself always
  arrives with the events table.
- There is no encryption or authentication on the node's connection yet.

## Quick start

```sh
make build
sudo bin/cosmicdust-agent-amd64 -data-dir /var/lib/cosmicdust -app-labels app
```

or in a container, the way cAdvisor runs:

```sh
docker compose -f deploy/agent/docker-compose.yml up -d --build
```

Without `-sync-url` nothing is sent and the quota decides how much the node keeps. The files are under
`<data-dir>/store/{logs,events}/dt=…/hour=…/` and readable with anything that reads Parquet:

```sh
duckdb -c "SELECT app, state, count(*) FROM read_parquet('/var/lib/cosmicdust/store/logs/*/*/*.parquet', hive_partitioning=true) GROUP BY ALL"
```

containerd's socket is root-only; without root, `-containerd=false` follows Docker alone. In a container the agent
mounts the runtimes' log directories and sockets read-only and runs in the host's cgroup namespace, where the kernel's
out-of-memory counters of the other containers are.

## Configuration

Every flag can also be set through the environment, as `COSMICDUST_` and the flag's name in upper case with
underscores: `-data-dir` is `COSMICDUST_DATA_DIR`. A flag on the command line wins over the environment
([0002](docs/design/0002-configuration.md)).

| Flag | Default | |
|---|---|---|
| `-node` | from `<data-dir>/node-id` | node identity stored with every row; made on first start |
| `-data-dir` | `/var/lib/cosmicdust` | node identity, container state, WAL, local store |
| `-listen` | `:9101` | `/metrics`, `/healthz` |
| `-pprof` | `false` | `/debug/pprof/` on the same address |
| `-memory-limit` | `64MiB` | soft limit for the Go runtime; `GOMEMLIMIT`, if set, wins |
| `-shutdown-timeout` | `30s` | longest a clean stop may take |
| `-log-level`, `-log-format` | `info`, `text` | `debug` logs every lifecycle event |
| `-docker`, `-docker-host` | `true`, from `DOCKER_HOST` | follow dockerd |
| `-docker-reconcile` | `0` | periodic listing through Docker (costly: ~10 ms of CPU per container in each daemon) |
| `-containerd`, `-containerd-address` | `true`, `/run/containerd/containerd.sock` | follow containerd |
| `-containerd-namespaces` | `moby,default` | Docker's and nerdctl's |
| `-containerd-reconcile` | `5m` | periodic listing through containerd |
| `-app-labels` | none | label keys naming the application; else the Compose service, else the image |
| `-cgroup-root` | `/sys/fs/cgroup` | where the kernel's out-of-memory counters are watched and read |
| `-snapshot-every` | `10s` | how often the container state is written when it changed |
| `-poll`, `-poll-max` | `250ms`, `5s` | check a file with recent data / a quiet one this often |
| `-read-buf` | `16KiB` | the read buffer, shared by every followed file |
| `-max-line`, `-max-message` | `1MiB`, `256KiB` | longer physical lines are skipped, longer messages cut |
| `-skip-exited` | `false` | leave what containers not running when first seen left unread |
| `-wal-segment-bytes`, `-wal-segment-age` | `8MiB`, `5s` | seal a WAL segment at this size or age |
| `-convert-every`, `-compact-every` | `10s`, `30s` | turn sealed segments into Parquet; merge finished hours |
| `-convert-bytes` | `32MiB` | log text one conversion takes; a backlog goes in pieces this size |
| `-sort-bytes` | `4MiB` | log text sorted at a time while converting |
| `-parquet-page-bytes` | `1MiB` | Parquet page size; a file being written keeps a buffer this size per column |
| `-disk-reserve` | `64MiB` | keep free on the store's disk, besides room to convert the WAL |
| `-store-quota` | `2GiB` | most the store may hold, WAL included |
| `-when-full` | `evict` | out of room: drop the oldest unsynced logs (each a `logs_dropped` event), or `block` reading |
| `-sync-url` | none | the receiver: `grpc://host:port` or `nats://host:4222`; without it nothing is sent |
| `-sync-every` | `10s` | send the finished files this often |
| `-sync-chunk-bytes`, `-sync-window` | `256KiB`, `32` | chunk size; NATS chunks in flight |
| `-sync-ack-timeout` | `30s` | NATS: the longest wait for the next ack |
| `-sync-rate` | `0` | cap on file bytes per second; urgent events are never held back |
| `-sync-urgent` | `separate` | urgent events on their own connection, `shared` with the files, or `off` |

The agent's own metrics are `cosmicdust_agent_*` on `/metrics`.

## Embedding the agent

A program that already runs on every node can run the node side itself, with package `node`; `cosmicdust-agent` is
that package plus flags ([0008](docs/design/0008-public-node-package.md)).

```go
n, err := node.New(node.Options{
	DataDir:    "/var/lib/cosmicdust",
	Docker:     &node.DockerOptions{},
	Containerd: &node.ContainerdOptions{},
	CgroupRoot: "/sys/fs/cgroup",
	Sync:       &node.SyncOptions{URL: "grpc://control-plane:50051"},
	Registerer: registry, // the cosmicdust_agent_* metrics, served by the program's own endpoint
})
if err != nil {
	return err
}
return n.Run(ctx) // until ctx ends; then every part stops in order
```

`SyncOptions.Transport` takes any `cosmicdust.Transport` instead of a URL, such as one over a connection the program
already holds. Zero option values take the agent's defaults; negative ones turn a feature off. The memory limit is
the process's, so the program sets it (`debug.SetMemoryLimit` or `GOMEMLIMIT`).

## Repository layout

```
cmd/agent/                    the agent: flags into node options, HTTP endpoint, memory limit, signals
cmd/walcat/                   prints what a WAL directory holds, as JSON Lines
pkg/cosmicdust/               public API: data model, interfaces, the component runner
pkg/node/                     public API: the node side assembled from options, for the agent and for embedding
pkg/api/                      the protocol between node and server: proto/sync.proto and generated code
internal/runtime/docker/      event source for dockerd: events, list and inspect
internal/runtime/containerd/  event source for containerd: events, list, describe
internal/lifecycle/           container state: views per runtime, merge, reconciliation, history, snapshot, OOM
internal/tail/                log files: json-file and CRI decoding, rotation, positions, one scheduler for all files
internal/store/wal/           write-ahead log: binary s2 blocks with resume positions, segments, recovery
internal/store/               Parquet by hour, manifests, one commit point, compaction, reserve and quota
internal/syncer/              the sync engine (rounds, claims, backoff) and the urgent channel
internal/transport/           gRPC and NATS transports: probe, chunks, continuing a broken upload, rate limit
internal/servertest/          a minimal receiving side of the protocol, for tests
internal/{metrics,nodeid,fsutil,retry}/   self-metrics endpoint, node identity, atomic writes, reconnect backoff
deploy/agent/                 Dockerfile and compose file
docs/design/                  design records
```

## Development

```sh
make test             # go vet and the unit tests under the race detector
make lint             # gofmt, staticcheck, golangci-lint
make integration      # lifecycle and tailer scenarios against the node's dockerd and containerd (sudo)
make build            # bin/cosmicdust-agent-{amd64,arm64}
make proto            # regenerate pkg/api from pkg/api/proto
```

The crash tests kill the process that reads and stores with `SIGKILL` again and again while the log keeps growing,
and then compare what was written with what was stored, line by line. The NATS tests start a `nats-server` from
`PATH` and are skipped without one.

## License

[MIT](LICENSE)
