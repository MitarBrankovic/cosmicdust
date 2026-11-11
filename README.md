# cosmicdust

Log collection for containers on edge nodes.

An **agent** on every node follows Docker and containerd, collects the logs of the containers and the events of their
lifecycle, and keeps them on the node's disk - compressed, in Parquet files partitioned by time - until the control
plane has acknowledged them. A **server** on the control plane receives the files, archives them by application,
indexes them in Elasticsearch and deletes both copies after each application's retention.

It is built for nodes with little CPU and memory to spare, on links that are slow or go away, with disks that fill:

- what a container wrote survives a crash of the agent, of the node or of the server, and a link that is down for
  hours;
- every line is stored and indexed once, however many times it had to be sent;
- the agent's cost is bounded: one process, a soft memory limit, a disk quota, a rate limit for the link;
- when the disk does fill, the agent records what it dropped instead of stopping.

## How it works

```
 node                                                             control plane

 dockerd, containerd ──events──▶ lifecycle ──┐
                                             ├─▶ WAL ─▶ Parquet ─▶ sync ══ gRPC ══▶ receiver ─▶ inbox + ledger ─▶ ack
 container log files ────────────▶ tailer ───┘         by hour                         │
                                                                                       ▼
                                                                         archive, split by application (Parquet)
                                                                                       │
                                                                                       ▼
                                                                         Elasticsearch, a data stream per application
```

**On the node.** Lifecycle events come from both runtimes and are merged into one state per container; after every
reconnect the state is reconciled with a listing, and an out-of-memory kill is confirmed from the kernel's counters.
Logs are read from the files the runtime writes (json-file and CRI), by one scheduler for all files, which follows
rotations and backs off on quiet files. Lines and events go to a write-ahead log together with the position reading
resumes from, and from there into Parquet files by hour, sorted and compressed with zstd, each line with its
container, its application and the container's state at that moment.

**Between them.** At intervals (10 s by default) the finished files are sent whole, in chunks, after a probe that
tells whether the receiver already has the file or a part of it, so a broken upload is continued and not started over.
A container that died with an error or was killed for memory is also reported at once, on a connection of its own. The
protocol is in [`pkg/api`](pkg/api/proto/sync.proto); it travels over gRPC or, as an option, over NATS.

**On the control plane.** The server acknowledges a file once it is complete, checked and durable, and only then
archives and indexes it, so a slow Elasticsearch never holds files on the nodes. The archive is plain Parquet split by
application; each application has its own retention for the index and for the archive.

## Guarantees and limits

| | |
|---|---|
| The agent is killed (`kill -9`, power loss) | A line and the position its reading resumes from become durable in the same `fsync`, so reading continues exactly after the last stored line: no gap, no duplicate. |
| The server or the link is away | Finished files wait on the node, within the quota. When the connection is back the backlog goes at once, and an upload that broke off continues where it stopped. |
| A file is sent twice | The server knows a file by node, table, partition, name, size and CRC-32C and stores it once; it answers `duplicate` to the second copy, also after its own restart. |
| A line is indexed twice | A document's id is derived from where the line starts in its log file, and documents are created, not overwritten: Elasticsearch keeps one copy. |
| The node's disk fills | Synced files go first. Then, by `-when-full`, either the oldest unsynced logs are dropped and a `logs_dropped` event records how many and from when (`evict`), or reading stops until there is room (`block`). Lifecycle events are never dropped. |
| Elasticsearch is away | Nothing reaches the nodes: files are acknowledged and archived as before, and the indexer catches up from the archive. |

What it does not do:

- Lines the runtime rotated away while the agent was stopped are gone. The agent counts them, and
  `cosmicdust_agent_tail_retention_seconds` says how long the shortest log on the node is kept.
- Only json-file and CRI log files are read. Containers with another log driver are counted and left unread.
- The immediate report of a failed container is a shortcut and is tried for a limited time; the event itself always
  arrives with the events table.
- There is no encryption or authentication on the nodes' connections yet.
- Over NATS, one server instance per NATS server.
- If a data stream rolls over between two attempts to index the same file, that file's documents are indexed twice.
  It takes a crash of the server in that window.

## Quick start

The control plane on one machine - the server, Elasticsearch and Kibana:

```sh
docker compose -f deploy/server/docker-compose.yml up -d --build
```

A node, next to the containers it follows:

```sh
make build
sudo bin/cosmicdust-agent-amd64 -data-dir /var/lib/cosmicdust -sync-url grpc://127.0.0.1:50051 -app-labels app
```

or in a container, the way cAdvisor runs:

```sh
COSMICDUST_SYNC_URL=grpc://<server>:50051 docker compose -f deploy/agent/docker-compose.yml up -d --build
```

In Kibana (<http://localhost:5601>), data views for `cosmicdust-logs-*`, `cosmicdust-events-*` and
`cosmicdust-urgent`, with `@timestamp` as the time field, show what arrives. On the node, the files are under
`<data-dir>/store/{logs,events}/dt=…/hour=…/` and readable with anything that reads Parquet:

```sh
duckdb -c "SELECT app, state, count(*) FROM read_parquet('/var/lib/cosmicdust/store/logs/*/*/*.parquet', hive_partitioning=true) GROUP BY ALL"
```

containerd's socket is root-only; without root, `-containerd=false` follows Docker alone. In a container the agent
mounts the runtimes' log directories and sockets read-only and runs in the host's cgroup namespace, where the kernel's
out-of-memory counters of the other containers are.

To send over NATS instead, start the compose file with `NATS_ADDRESS=nats:4222` and `--profile nats`, and give the
agent `-sync-url nats://<server>:4222`.

## Reading the logs

Other services read what the nodes sent through the server's query API, the gRPC service `cosmicdust.v1.Query`
([`pkg/api/proto/query.proto`](pkg/api/proto/query.proto)), on the address the nodes send to:

| Call | Returns |
|---|---|
| `ListNodes` | the nodes that sent logs or events in a period, with their counts and the first and the last time |
| `ListContainers` | the containers seen on a node in a period, the most recently active first |
| `GetLogs` | log lines of a node, oldest first; filters: application, container (ID or name), stream, words of the line |
| `GetEvents` | lifecycle events of a node, oldest first; filters: container, kind (`start`, `exit`, `oom`, ...) |

`GetLogs` and `GetEvents` answer a page at a time (100 rows by default, at most 1,000). A full page comes with a
`next_page_token`; sent back as `page_token`, it continues after the last row, without skipping or repeating one.

The server answers gRPC reflection, so `grpcurl` needs no proto files:

```sh
grpcurl -plaintext localhost:50051 cosmicdust.v1.Query/ListNodes
grpcurl -plaintext -d '{"node": "<node>"}' localhost:50051 cosmicdust.v1.Query/ListContainers
grpcurl -plaintext -d '{"node": "<node>", "container": "web", "contains": "error", "page_size": 50}' \
    localhost:50051 cosmicdust.v1.Query/GetLogs
grpcurl -plaintext -d '{"node": "<node>", "kind": "exit"}' localhost:50051 cosmicdust.v1.Query/GetEvents
```

The API answers from the search index: it needs Elasticsearch, and it reaches as far back as each application's search
retention. What is older is in the archive, as Parquet.

## Configuration

### Agent

Every flag can also be set through the environment, as `COSMICDUST_` and the flag's name in upper case with
underscores: `-data-dir` is `COSMICDUST_DATA_DIR`. A flag on the command line wins over the environment.

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
| `-sync-url` | none | the server: `grpc://host:port` or `nats://host:4222`; without it nothing is sent |
| `-sync-every` | `10s` | send the finished files this often |
| `-sync-chunk-bytes`, `-sync-window` | `256KiB`, `32` | chunk size; NATS chunks in flight |
| `-sync-ack-timeout` | `30s` | NATS: the longest wait for the next ack |
| `-sync-rate` | `0` | cap on file bytes per second; urgent events are never held back |
| `-sync-urgent` | `separate` | urgent events on their own connection, `shared` with the files, or `off` |

The agent's own metrics are `cosmicdust_agent_*` on `/metrics`.

### Server

The server is configured from the environment.

| Variable | Default | |
|---|---|---|
| `GRPC_ADDRESS` | `:50051` | gRPC listen address; empty: no gRPC |
| `NATS_ADDRESS` | | NATS server, `nats:4222` or a URL; empty: no NATS |
| `SUBJECT_PREFIX` | `cosmicdust` | the nodes' NATS subjects: `<prefix>.files.<node>`, `<prefix>.urgent.<node>` |
| `HTTP_ADDRESS` | `:8080` | `/metrics` and `/healthz` |
| `DATA_DIR` | `data` | `inbox/`, `archive/`, `quarantine/`, `state/` (ledger, urgent spool) |
| `ES_ADDRESS` | | Elasticsearch, e.g. `http://elasticsearch:9200`; empty: archive only |
| `ES_USERNAME`, `ES_PASSWORD` / `ES_API_KEY` | | credentials, if Elasticsearch asks for them |
| `ES_INDEX_PREFIX` | `cosmicdust` | the data streams' prefix |
| `ES_REPLICAS` | cluster default | replicas of the data streams' indices (0 on a single node) |
| `RETENTION_FILE` | built-in | retention per application, see `deploy/server/retention.example.yaml` |
| `PARTIAL_IDLE` | `10m` | drop an upload nobody continued for this long |
| `CLEANUP_EVERY` | `1h` | how often the archive is swept for expired hours |
| `DEDUP_WINDOW` | `7d` | how long a received file is remembered, to recognise it when a node sends it again |
| `COMPACT_AFTER`, `COMPACT_EVERY` | `2h`, `10m` | merge an application's hour of the archive into one file once it ended this long ago and is all indexed; look this often (`0`: never) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

Each application has two periods per table: `search`, how long Elasticsearch keeps its rows, and `archive`, how long
the archive does; without a file, 7 and 30 days for logs, 30 and 90 for events. The archive is Parquet with Hive
partitions, `archive/<table>/app=<app>/dt=<day>/hour=<hour>/`:

```sh
duckdb -c "SELECT app, count(*) FROM read_parquet('data/archive/logs/*/*/*/*.parquet', hive_partitioning=true) GROUP BY app"
```

The server's metrics are `cosmicdust_server_*`: what arrived, what waits for the archive and the index
(`cosmicdust_server_archive_backlog_files`, `cosmicdust_server_index_backlog_files`), documents by outcome, and
`cosmicdust_server_elasticsearch_up`. A received file that is not a Parquet file of its table goes to `quarantine/`
instead of holding up the files behind it.

## Embedding the agent

A program that already runs on every node can run the node side itself, with package `node`; `cosmicdust-agent` is
that package plus flags.

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
cmd/server/                   the server: receives the nodes' files, archives, indexes, deletes by retention
cmd/walcat/                   prints what a WAL directory holds, as JSON Lines
pkg/cosmicdust/               public API: data model, interfaces, the component runner
pkg/node/                     public API: the node side assembled from options, for the agent and for embedding
pkg/api/                      the protocols, proto/ and generated code: sync (node to server) and query (reading)
internal/runtime/docker/      event source for dockerd: events, list and inspect
internal/runtime/containerd/  event source for containerd: events, list, describe
internal/lifecycle/           container state: views per runtime, merge, reconciliation, history, snapshot, OOM
internal/tail/                log files: json-file and CRI decoding, rotation, positions, one scheduler for all files
internal/store/wal/           write-ahead log: binary s2 blocks with resume positions, segments, recovery
internal/store/               Parquet by hour, manifests, one commit point, compaction, reserve and quota
internal/syncer/              the sync engine (rounds, claims, backoff) and the urgent channel
internal/transport/           gRPC and NATS transports: probe, chunks, continuing a broken upload, rate limit
internal/server/              the server's parts: configs, ledger, receiver, urgent spool, archive, Elasticsearch
                              client, indexer, retention, query API, servers, metrics, startup
internal/servertest/          a minimal receiving side of the protocol, for tests
internal/{metrics,nodeid,fsutil,retry}/   self-metrics endpoint, node identity, atomic writes, reconnect backoff
deploy/agent/, deploy/server/ Dockerfiles and compose files
```

## Development

```sh
make test             # go vet and the unit tests under the race detector
make lint             # gofmt, staticcheck, golangci-lint
make integration      # lifecycle and tailer scenarios against the node's dockerd and containerd (sudo)
ES_ADDRESS=http://localhost:9200 make integration-es   # the indexer and the query API against Elasticsearch
make build            # bin/cosmicdust-{agent,server}-{amd64,arm64}
make proto            # regenerate pkg/api from pkg/api/proto
```

The crash tests kill the process that reads and stores with `SIGKILL` again and again while the log keeps growing,
and then compare what was written with what was stored, line by line. The NATS tests start a `nats-server` from
`PATH` and are skipped without one.

## License

[MIT](LICENSE)
