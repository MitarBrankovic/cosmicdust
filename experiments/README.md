# Experiments

One command makes every run of every experiment as many times as asked and ends with the statistics and the charts:

```sh
experiments/run.sh -n 5              # or: make experiments N=5
experiments/run.sh -n 5 -e nodes     # one experiment
experiments/run.sh -q                # a quick pass, to see that everything works
```

One machine plays the whole system. The server runs as a process on the host and indexes into an Elasticsearch in a
container. Every node is a container with a Docker daemon and a containerd of its own ([`node/`](node/Dockerfile)), in
which the agent and the log generators run, so each node has its own runtime, cgroups and network. The nodes send to
the server over gRPC, through the gateway of a Docker network; with `TRANSPORT=nats` they send through a NATS server
instead, which then counts as a part of the server and is away when the server is.

## The experiments

| Experiment | What runs | Axis | Series |
|---|---|---|---|
| `nodes` | the whole system: the server and 1 to 10 nodes, each limited to `NODE_CPUS` and `NODE_MEM` | number of nodes | scenario |
| `adverse` | the same system where things are hard for a node: a narrow link, a burst, a full store | number of nodes | scenario |
| `compare` | one roomy node with a log collector in it as a container, under limits of its own | case | collector: the agent, Fluent Bit, Vector |
| `ceiling` | the same with the agent alone, under growing loads | lines per second asked for | the agent |
| `compress` | one roomy node whose agent sends nothing, and generators that write the lines of a real log | log | what kept the logs: the agent, gzip, zstd |
| `latency` | the server and one node under a steady load, the agent set to be more or less fresh | setting | what was timed: a line to the server, a line to the search, an urgent event |
| `events` | the server and one node on which a script plays the lifecycles of containers | what the agent does meanwhile | the agent |

### nodes

The generators of each node write `CONTAINERS` x `RATE` numbered lines a second for `DURATION` seconds. A scenario is
what happens meanwhile:

| Scenario | While the generators write |
|---|---|
| `steady` | nothing: the cost of the system under a steady load |
| `outage` | the server is away from a quarter of the load until five seconds after its end; meanwhile containers fail on every node |
| `churn` | every `KILL_EVERY` seconds the agent of one node is killed with `kill -9` and started again, node after node |
| `containerd` | every `KILL_EVERY` seconds the containerd of one node is killed; its Docker starts it again and the containers run on, while the agent loses its stream of containerd's events again and again |
| `dockerd` | in the middle of the load the Docker of every node is stopped and started again. Its containers end with it, so the lines that must be there are the ones their log files hold (`lines_written`), not the ones asked for |

A long outage is `outage` with a long load: the server is away from `OUTAGE_AFTER` seconds into the load (a quarter
of it, unless set) until five seconds after its end, so `OUTAGE_AFTER=60 DURATION=3655` keeps it away for an hour.
Containers that fail early in so long an outage are no longer told on the urgent way when the server returns - the
agent keeps an urgent event for ten minutes - but they are in the events it sends with everything else.

### adverse

The same runs, with scenarios of what a node at the edge has to live with:

| Scenario | While the generators write |
|---|---|
| `narrow` | `outage`, with every node behind a link of `LINK_MBIT` Mbit/s with a round trip of `LINK_RTT` ms that loses `LINK_LOSS` % of the packets each way: what waited for the server crosses that link |
| `burst` | for the middle third of the load, `BURST_X` times as many lines |
| `evict` | the server is away for the whole load and the agent's store may hold `STORE_QUOTA`: the agent drops its oldest logs for room and records every drop |
| `block` | the same, with the agent told to stop reading instead of dropping |

The link is shaped inside the node, with netem, and only for what goes to the server and comes from it. In `evict`
a run is right when every line is either there once or among the lines the agents say they dropped; in `block`,
when every line is there once.

### compare and ceiling

The collector under test runs in the node as a container with a CPU and memory limit of its own, and is measured by
the counters of its own cgroup, the same way for all three. The generators and the node's Docker run outside that
limit.

| Case | Containers x lines per second | Seconds | Limit of the collector |
|---|---|---|---|
| `idle0` | none | 90 | 1 CPU, 512 MB |
| `idle200` | 200, quiet | 120 | 1 CPU, 512 MB |
| `load1k` | 10 x 100 | 120 | 1 CPU, 512 MB |
| `load5k` | 10 x 500 | 120 | 1 CPU, 512 MB |
| `load10k` | 20 x 500 | 120 | 1 CPU, 512 MB |
| `edge25` | 10 x 100 | 120 | 0.25 CPU, 128 MB |
| `edge10` | 10 x 100 | 120 | 0.10 CPU, 256 MB |

`ceiling` runs the loads in `LOADS` (containers x lines per second, by default `10x500 20x500 40x500 60x500`), each
for `LOAD_SECONDS` (60), with the agent under `CEILING_CPUS` (a quarter of a core) and 512 MB: a node of this kind
cannot write as many lines as the agent takes with a whole core. Near its ceiling the agent is held back by its
limit more and more (`throttled_s`) and needs longer after the load to hand everything to the server
(`delivered_s`); how soon the lines can be searched after that is the server's indexing, not the agent.

The three collectors do not do the same work. The agent stores what it reads in Parquet and sends it to the server,
and its run ends when every line can be searched there. Fluent Bit tails and writes a plain file; Vector tails,
parses and writes a zstd file. The comparison is what a node pays for its log collector, not a benchmark of equal
pipelines.

### compress

What the agent's store makes of real logs, against compressing the log files the node's Docker wrote. It takes real
logs, given as `DATASETS="<name>=<file> ..."` (or the one in `REPLAY`), and is left out without any: filler compresses
like no log does. Public collections of logs, such as [Loghub](https://github.com/logpai/loghub), have them from
many kinds of systems.

A run takes one log and one length of file. `COMPRESS_CONTAINERS` generators write the log's lines as they are, at
`COMPRESS_RATE` lines a second each. The agent turns what it has read into Parquet every `FILE_SECONDS` seconds, so
each of its files holds that many seconds of logs; it sends nothing and merges nothing. The log files are then cut
into pieces of the same length, and each piece is compressed on its own:

| Series | What is kept |
|---|---|
| `agent` | the agent's Parquet files |
| `gzip-6`, `zstd-3`, `zstd-19` | the log files, compressed with `gzip -6`, `zstd -3` and `zstd -19` |
| `messages-zstd-3` | the text of the lines alone, without the JSON the runtime wraps each of them in, with `zstd -3`: how much of the log files is the wrapping |

The value is `ratio_<seconds>s`, bytes of the log files over bytes kept, one for every length of file; what the run
itself was like is under a series named after that length (`60s`). The bytes do not depend on how busy the machine
is, so this experiment does not need a quiet one.

### latency

How late a line is, by how fresh the agent is set to be, and what the freshness costs. A line waits three times on
its node: in the open segment of the agent's write-ahead log until that is sealed, in the sealed segment until the
next conversion to Parquet, and in the Parquet file until the next sync. A setting is those three periods:

| Setting | Segment sealed after | Conversion every | Sync every |
|---|---|---|---|
| `s1` | 1 s | 2 s | 2 s |
| `s2` | 2 s | 5 s | 5 s |
| `default` | 5 s | 10 s | 10 s |
| `s30` | 10 s | 30 s | 30 s |
| `s60` | 30 s | 60 s | 60 s |

Any other can be written `<segment>-<conversion>-<sync>`, in seconds. A run is one node whose `LATENCY_CONTAINERS`
generators write `LATENCY_RATE` lines a second each for `LATENCY_SECONDS` seconds; every `LATENCY_CRASH_EVERY`
seconds a container fails on it, which the agent reports at once as an urgent event, on a way of its own. What is
timed comes from the server's own record of when each file arrived and when its rows were indexed
([`cmd/latency`](cmd/latency/main.go)), for every line:

| Series | From | To |
|---|---|---|
| `server` | the time the node's runtime took the line | the server has the file that holds it |
| `search` | the same | the server has put the line into the search index |
| `urgent` | the end of the process of a container that failed | the urgent event is at the server |
| `agent` | | what the agent cost and sent meanwhile, and whether every line came back once |

### events

The lifecycle events of containers, against a script that knows what happened. Every run plays the same lifecycles
on one node, with containers of `EVENTS_IMAGE`:

| Container | What happens to it | Exits that must be recorded | Left |
|---|---|---|---|
| `ev-ok`, `ev-fail` | end on their own, with 0 and with 3 | one each, with its code | exited |
| `ev-stop`, `ev-kill` | `docker stop`, `docker kill` | one each, 137 | exited |
| `ev-oom`, `ev-oom-fast` | killed by the kernel for memory, after a moment and at once | one each, 137, for memory | exited |
| `ev-policy` | fails under a restart policy that gives up after three restarts | four, one per incarnation | exited |
| `ev-restart` | `docker restart` | one, 137 | running |
| `ev-pause` | paused and continued | none | running |
| `ev-rm` | removed while it runs | one, 137 | removed |
| `ev-ctr` | run by containerd without Docker, ends with 2 | one | exited |

The events are then read through the query API and compared with that ([`cmd/events`](cmd/events/main.go)). A mode
is what the agent does while the script plays:

| Mode | The agent | What it has to go on |
|---|---|---|
| `live` | runs all along | the event streams of the runtimes |
| `kill9` | is killed with `kill -9` twice and started again | the streams, and a listing for what it missed |
| `away` | is stopped before the lifecycles and started after them | a listing alone |

A listing shows what is, not what was. An exit that a later start of the same container hides, the incarnations a
restart policy went through before the last, and anything of a container that is gone cannot be concluded from it:
in `away`, those are missing by design, and the run says how many.

## What a run measures

| Value | Experiments | |
|---|---|---|
| `exactly_once` | all but `compress`, for the agent | 1 if every line of every generator came back through the query API once: none missing, none twice ([`cmd/verify`](cmd/verify/main.go)); in `evict`, none missing but the ones recorded as dropped |
| `lines_expected`, `lines_stored`, `lines_missing`, `lines_duplicate` | all but `compress` | the lines behind that; for Fluent Bit and Vector, `lines_stored` is the lines in their output file |
| `agent_cpu_pct`, `agent_cpu_pct_max` | `nodes`, `adverse`, `latency` | CPU of the agent while the generators wrote, in percent of one core: the mean of the nodes and the busiest node |
| `agent_rss_mb`, `agent_rss_mb_max` | `nodes`, `adverse`, `latency` | resident memory of the agent, the most seen: the mean of the nodes and the largest |
| `server_cpu_pct`, `server_rss_mb` | `nodes`, `adverse`, `latency` | the same for the server |
| `settle_s` | all but `compress`, for the agent | seconds until every line can be searched, from the end of the load (or, where the server was away, from its return) |
| `scenario_s` | `nodes`, `adverse`, `latency`, `events` | seconds from the first line to the end of the scenario's part: more than planned if a node could not write as fast as asked; in `events`, the seconds the lifecycles took |
| `urgent_expected`, `urgent_delivered` | `outage`, `narrow`, `latency` | containers that failed (in `outage` and `narrow`, while the server was away), and how many of them the server was told about on the urgent way |
| `link_mb`, `link_lost_pct` | `narrow` | what the narrow links carried to the server, and the share of the packets they lost, counted on what comes back from it: on the way out the queue is handed several segments as one packet and counts the loss of all of them once |
| `lines_dropped`, `dropped_pct` | `evict`, `block` | lines the agents dropped for room, as they count them, and their share of the lines written |
| `store_full_times` | `evict`, `block` | how many times an agent stopped reading because its store was full |
| `collector_cpu_pct` | `compare`, `ceiling` | CPU of the collector while the generators wrote (with quiet containers: for the length of the case), in percent of one core |
| `collector_anon_mb`, `collector_peak_mb` | `compare`, `ceiling` | its anonymous memory at the end of the load; its memory at its largest, the page cache it was charged with included |
| `runtime_cpu_pct`, `generators_cpu_pct` | `compare`, `ceiling` | CPU of the node's runtime meanwhile (dockerd, containerd and their shims), and of the generators |
| `cpu_s_per_gb`, `input_mb` | `compare`, `ceiling` | CPU seconds of the collector per GB of log files, over the whole run; the size of the log files |
| `complete_pct` | `compare`, `ceiling` | `lines_stored` as a percentage of the lines written |
| `unread_pct`, `behind_pct` | `compare`, `ceiling`, for the agent | at the moment the last line is written: the lines the agent has not read yet, and the lines not yet searchable |
| `delivered_s` | `compare`, `ceiling`, for the agent | seconds from the end of the load until the agent has stored every line and the server has as many rows in its archive |
| `written_per_s` | `compare`, `ceiling` | lines per second the generators managed to write: less than asked for when the node itself is too busy |
| `throttled_s` | `compare`, `ceiling` | seconds the collector was held back by its CPU limit |
| `ratio_<seconds>s` | `compress` | bytes of the log files over bytes kept, with that many seconds of logs to a file |
| `files`, `file_mb` | `compress` | the Parquet files the agent made of the log lines, and their mean size |
| `lines`, `stored_pct` | `compress` | lines written, and the share of them in the agent's Parquet files when the run ended; the agent's ratio is over those |
| `log_mb`, `message_pct` | `compress` | the size of the log files, and the share of it that is the text of the lines |
| `p50_s`, `p95_s`, `p99_s`, `max_s` | `latency` | seconds within which half, 95 % and 99 % of the lines were there, and the latest of them; for urgent events, half and the latest |
| `files_per_min`, `mb_received` | `latency` | files of log lines the agent sent per minute of load, and megabytes the server received in all |
| `lines_timed`, `log_files`, `events_timed` | `latency` | the lines, the files of log lines and the urgent events behind those values |
| `exits_expected`, `exits_recorded`, `recorded_pct` | `events` | exits the script caused, and how many of them have an exit event of their incarnation that is not older than the incarnation's start |
| `codes_right_pct`, `oom_right_pct` | `events` | of the exits recorded, those with the right exit code, and those that say rightly whether the kernel killed for memory |
| `duplicates` | `events` | exits a runtime reported, or the agent concluded, more than once |
| `exit_events`, `inferred_pct` | `events` | exit events in all (Docker and containerd each report an exit), and the share of them the agent concluded from a listing |
| `containers`, `final_right_pct` | `events` | containers of the script, and the share whose last event leaves them in the state they are in |
| `agent_kills`, `runtime_kills`, `runtime_restarts` | `churn`, `containerd`, `dockerd` | how many times an agent or a containerd was killed, and how many Docker daemons were restarted |
| `lines_written` | `dockerd` | lines in the log files of the generators, which ended when their Docker was stopped: the run is right when exactly those came back once |
| `machine_busy_cores` | all | cores of the whole machine that were busy during the run: a run made while something else was busy is not worth keeping |
| `clock_set_s` | all | seconds the machine's wall clock was set by during the run, against its uptime: the times of lines and events are wall clock times, so across a step of it some are out of order and what was timed is off by as much |

In `nodes`, `adverse` and `latency` the agents and the server are measured by what they report themselves
(`process_cpu_seconds_total`, `process_resident_memory_bytes`), sampled every two seconds.

## Results

`experiments/results/<time>/`, or the directory given with `-o`:

```
runs.csv      one line per run and value: experiment, x (the place on its axis), series, round, metric, value
summary.csv   per experiment, x, series and value: runs, median, mean, deviation, least, most
figures/      a chart per experiment and value (PNG and PDF): the median, the range of the runs as bars and
              every run as a dot; lines where the axis is a number, grouped bars where it is a name
env.txt       the machine, the versions and the settings
runs/         what each run left behind: samples, the verifier's answer, the server's log
```

In every round each run is made once, and each round starts one place further down the list. A session that was
interrupted continues with `-o <its directory>`: runs that are complete are not repeated. `analyze.py <directory>`
makes the summary and the charts again.

Two sessions can run side by side on one machine if the second has its own `NODES_NAME`, `GRPC_PORT`, `HTTP_PORT`,
`NATS_PORT` and `NODE_PORT_BASE`; they share the Elasticsearch and the machine's cores, so only for runs whose
outcome is not a matter of timing.

## Settings

| Variable | Default | |
|---|---|---|
| `CONTAINERS`, `RATE`, `DURATION` | `5`, `100`, `60` | `nodes`, `adverse`: generators on every node, lines per second of each, seconds of load |
| `NODE_CPUS`, `NODE_MEM` | `1`, `512m` | `nodes`, `adverse`: what a node may use |
| `KILL_EVERY`, `CRASHES` | `10`, `5` | `churn`, `containerd`: seconds between two kills; `outage`: containers that fail on every node |
| `OUTAGE_AFTER` | a quarter of `DURATION` | `outage`, `narrow`: seconds of load before the server goes away |
| `TRANSPORT` | `grpc` | how the nodes send: `grpc`, or `nats` through a `nats-server` started with the server, on `NATS_PORT` (`14222`) |
| `NODES_NAME` | `cosmicdust-node` | what the nodes' containers are named after; with the ports, what a second session on the same machine sets apart |
| `ADVERSE` | `narrow,burst,evict,block` | `adverse`: the scenarios |
| `LINK_MBIT`, `LINK_RTT`, `LINK_LOSS` | `1`, `100`, `5` | `narrow`: the link's megabits per second, round trip in milliseconds, and percent of the packets lost |
| `BURST_X` | `5` | `burst`: how many times the load |
| `STORE_QUOTA` | `512KiB`, with `REPLAY` `1MiB` | `evict`, `block`: the most an agent's store may hold; less than what a node's load takes |
| `CASES` | all seven | `compare`: the cases, space separated; a case can also be written `<containers>x<lines per second>` |
| `SUBJECTS` | `agent fluent-bit vector` | `compare`: the collectors |
| `FLUENT_BIT_IMAGE`, `VECTOR_IMAGE` | `fluent/fluent-bit:5.1.2`, `timberio/vector:0.58.0-debian` | their images |
| `LOADS`, `LOAD_SECONDS`, `CEILING_CPUS` | `10x500 20x500 40x500 60x500`, `60`, `0.25` | `ceiling`: the loads, how long each lasts, and the agent's CPU limit |
| `DATASETS` | | `compress`: the logs, as `<name>=<file>`, space separated; without it, the one in `REPLAY` |
| `FILE_SECONDS` | `10 60 300` | `compress`: seconds of logs in one of the agent's files |
| `COMPRESS_CONTAINERS`, `COMPRESS_RATE`, `REPLAY_LINES` | `4`, `500`, `300000` | `compress`: generators, lines per second of each, and the lines of a log one writes before it starts over |
| `SETTINGS` | `s1 s2 default s30 s60` | `latency`: the settings, space separated |
| `LATENCY_CONTAINERS`, `LATENCY_RATE`, `LATENCY_SECONDS` | `10`, `100`, `180` | `latency`: generators, lines per second of each, seconds of load |
| `LATENCY_CRASH_EVERY` | `15` | `latency`: seconds between two containers that fail |
| `MODES` | `live kill9 away` | `events`: the modes, space separated |
| `EVENTS_IMAGE` | `alpine:3` | `events`: the image of the script's containers; anything with a shell, `sleep` and `dd` |
| `REPLAY` | | a log file whose lines the generators write instead of filler, for text that compresses like real logs do |
| `ES_ADDRESS` | | an Elasticsearch to use; without it one is started in a container, on `ES_PORT` (`19200`) |
| `PYTHON` | `python3` | a Python with matplotlib, for the charts |

## What it needs

Docker that can run privileged containers (the nodes are Docker in Docker), Go, curl, and Python 3 with matplotlib;
`zstdcat` to count Vector's output; `gzip` and `zstd` for `compress`; `nats-server` for `TRANSPORT=nats`; for `narrow`, a kernel with netem and ifb. About 2 GB of memory for
Elasticsearch, `NODE_MEM` for every node of `nodes` and `adverse`, and 4 GB for the node of `compare` and `ceiling`. For numbers worth keeping, a machine that does nothing else: the nodes,
the server and Elasticsearch share its cores.
