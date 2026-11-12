# Experiments

One command runs every scenario as many times as asked and ends with the statistics and the charts:

```sh
experiments/run.sh -n 5          # or: make experiments N=5
experiments/run.sh -q            # a quick pass, a few minutes, to see that everything works
```

One machine plays the whole system. The server runs as a process on the host and indexes into an Elasticsearch in a
container. Every node is a container with a Docker daemon and a containerd of its own ([`node/`](node/Dockerfile)), in
which the agent and the log generators run, so each node has its own runtime, cgroups and network, limited to
`NODE_CPUS` and `NODE_MEM`. The nodes send to the server over gRPC, through the gateway of a Docker network.

## Scenarios

In every run the generators of each node write `CONTAINERS` x `RATE` numbered lines a second for `DURATION` seconds.
A scenario is what happens meanwhile:

| Scenario | While the generators write |
|---|---|
| `steady` | nothing: the cost of the system under a steady load |
| `outage` | the server is away from a quarter of the load until five seconds after its end; meanwhile containers fail on every node |
| `churn` | every `KILL_EVERY` seconds the agent of one node is killed with `kill -9` and started again, node after node |

Every scenario runs with every number of nodes (`-k`, by default 1, 3, 5 and 10), `-n` times. In each round every
scenario and size runs once, and each round starts one place further down the list.

## What a run measures

| Value | |
|---|---|
| `exactly_once` | 1 if every line of every generator came back through the query API once: none missing, none twice ([`cmd/verify`](cmd/verify/main.go)) |
| `lines_expected`, `lines_stored`, `lines_missing`, `lines_duplicate` | the lines behind that |
| `agent_cpu_pct`, `agent_cpu_pct_max` | CPU of the agent while the generators wrote, in percent of one core: the mean of the nodes and the busiest node |
| `agent_rss_mb`, `agent_rss_mb_max` | resident memory of the agent, the most seen: the mean of the nodes and the largest |
| `server_cpu_pct`, `server_rss_mb` | the same for the server |
| `settle_s` | seconds until every line can be searched, from the end of the load (`steady`, `churn`) or from the server's return (`outage`) |
| `urgent_expected`, `urgent_delivered` | `outage`: containers that failed while the server was away, and how many of them it was told about at once after its return |
| `outage_s`, `agent_kills` | how long the server was away; how many times an agent was killed |
| `files_received`, `mb_received` | what the server received (in `outage`, since its return) |

The agents and the server are measured by what they report themselves (`process_cpu_seconds_total`,
`process_resident_memory_bytes`), sampled every two seconds.

## Results

`experiments/results/<time>/`, or the directory given with `-o`:

```
runs.csv      one line per run and value: scenario, nodes, round, metric, value
summary.csv   per scenario, nodes and value: runs, median, mean, deviation, least, most
figures/      a chart per value (PNG and PDF): the median against the number of nodes, a line per
              scenario, the range of the runs as bars and every run as a dot
env.txt       the machine, the versions and the settings
runs/         what each run left behind: samples, the verifier's answer, the server's log
```

A session that was interrupted continues with `-o <its directory>`: runs that are complete are not repeated.
`analyze.py <directory>` makes the summary and the charts again.

## Settings

| Variable | Default | |
|---|---|---|
| `CONTAINERS` | `5` | generators on every node |
| `RATE` | `100` | lines per second of each generator |
| `DURATION` | `60` | seconds of load |
| `NODE_CPUS`, `NODE_MEM` | `1`, `512m` | what a node may use |
| `KILL_EVERY` | `10` | `churn`: seconds between two kills |
| `CRASHES` | `5` | `outage`: containers that fail on every node |
| `ES_ADDRESS` | | an Elasticsearch to use; without it one is started in a container, on `ES_PORT` (`19200`) |
| `PYTHON` | `python3` | a Python with matplotlib, for the charts |

## What it needs

Docker that can run privileged containers (the nodes are Docker in Docker), Go, curl, and Python 3 with matplotlib.
About 2 GB of memory for Elasticsearch and `NODE_MEM` for every node. For numbers worth keeping, a machine that does
nothing else: the nodes, the server and Elasticsearch share its cores.
