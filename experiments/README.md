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
the server over gRPC, through the gateway of a Docker network.

## The experiments

| Experiment | What runs | Axis | Series |
|---|---|---|---|
| `nodes` | the whole system: the server and 1 to 10 nodes, each limited to `NODE_CPUS` and `NODE_MEM` | number of nodes | scenario |
| `adverse` | the same system where things are hard for a node: a narrow link, a burst, a full store | number of nodes | scenario |
| `compare` | one roomy node with a log collector in it as a container, under limits of its own | case | collector: the agent, Fluent Bit, Vector |
| `ceiling` | the same with the agent alone, under growing loads | lines per second asked for | the agent |

### nodes

The generators of each node write `CONTAINERS` x `RATE` numbered lines a second for `DURATION` seconds. A scenario is
what happens meanwhile:

| Scenario | While the generators write |
|---|---|
| `steady` | nothing: the cost of the system under a steady load |
| `outage` | the server is away from a quarter of the load until five seconds after its end; meanwhile containers fail on every node |
| `churn` | every `KILL_EVERY` seconds the agent of one node is killed with `kill -9` and started again, node after node |

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

## What a run measures

| Value | Experiments | |
|---|---|---|
| `exactly_once` | all, for the agent | 1 if every line of every generator came back through the query API once: none missing, none twice ([`cmd/verify`](cmd/verify/main.go)); in `evict`, none missing but the ones recorded as dropped |
| `lines_expected`, `lines_stored`, `lines_missing`, `lines_duplicate` | all | the lines behind that; for Fluent Bit and Vector, `lines_stored` is the lines in their output file |
| `agent_cpu_pct`, `agent_cpu_pct_max` | `nodes`, `adverse` | CPU of the agent while the generators wrote, in percent of one core: the mean of the nodes and the busiest node |
| `agent_rss_mb`, `agent_rss_mb_max` | `nodes`, `adverse` | resident memory of the agent, the most seen: the mean of the nodes and the largest |
| `server_cpu_pct`, `server_rss_mb` | `nodes`, `adverse` | the same for the server |
| `settle_s` | all, for the agent | seconds until every line can be searched, from the end of the load (or, where the server was away, from its return) |
| `scenario_s` | `nodes`, `adverse` | seconds from the first line to the end of the scenario's part: more than planned if a node could not write as fast as asked |
| `urgent_expected`, `urgent_delivered` | `outage`, `narrow` | containers that failed while the server was away, and how many of them it was told about at once after its return |
| `link_mb`, `link_lost_pct` | `narrow` | what the narrow links carried to the server, and the share of the packets they lost on the way |
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
| `machine_busy_cores` | all | cores of the whole machine that were busy during the run: a run made while something else was busy is not worth keeping |

In `nodes` the agents and the server are measured by what they report themselves (`process_cpu_seconds_total`,
`process_resident_memory_bytes`), sampled every two seconds.

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

## Settings

| Variable | Default | |
|---|---|---|
| `CONTAINERS`, `RATE`, `DURATION` | `5`, `100`, `60` | `nodes`, `adverse`: generators on every node, lines per second of each, seconds of load |
| `NODE_CPUS`, `NODE_MEM` | `1`, `512m` | `nodes`, `adverse`: what a node may use |
| `KILL_EVERY`, `CRASHES` | `10`, `5` | `churn`: seconds between two kills; `outage`: containers that fail on every node |
| `ADVERSE` | `narrow,burst,evict,block` | `adverse`: the scenarios |
| `LINK_MBIT`, `LINK_RTT`, `LINK_LOSS` | `1`, `100`, `5` | `narrow`: the link's megabits per second, round trip in milliseconds, and percent of the packets lost |
| `BURST_X` | `5` | `burst`: how many times the load |
| `STORE_QUOTA` | `512KiB`, with `REPLAY` `1MiB` | `evict`, `block`: the most an agent's store may hold; less than what a node's load takes |
| `CASES` | all seven | `compare`: the cases, space separated; a case can also be written `<containers>x<lines per second>` |
| `SUBJECTS` | `agent fluent-bit vector` | `compare`: the collectors |
| `FLUENT_BIT_IMAGE`, `VECTOR_IMAGE` | `fluent/fluent-bit:5.1.2`, `timberio/vector:0.58.0-debian` | their images |
| `LOADS`, `LOAD_SECONDS`, `CEILING_CPUS` | `10x500 20x500 40x500 60x500`, `60`, `0.25` | `ceiling`: the loads, how long each lasts, and the agent's CPU limit |
| `REPLAY` | | a log file whose lines the generators write instead of filler, for text that compresses like real logs do |
| `ES_ADDRESS` | | an Elasticsearch to use; without it one is started in a container, on `ES_PORT` (`19200`) |
| `PYTHON` | `python3` | a Python with matplotlib, for the charts |

## What it needs

Docker that can run privileged containers (the nodes are Docker in Docker), Go, curl, and Python 3 with matplotlib;
`zstdcat` to count Vector's output; for `narrow`, a kernel with netem and ifb. About 2 GB of memory for
Elasticsearch, `NODE_MEM` for every node of `nodes` and `adverse`, and 4 GB for the node of `compare` and `ceiling`. For numbers worth keeping, a machine that does nothing else: the nodes,
the server and Elasticsearch share its cores.
