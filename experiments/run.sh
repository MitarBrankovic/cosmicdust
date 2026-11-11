#!/usr/bin/env bash
# Runs the experiments: every scenario, with every number of nodes, as many
# times as asked, and then the statistics and the charts over all the runs.
#
#   experiments/run.sh -n 5                      every scenario, 1, 3, 5 and 10 nodes, five times each
#   experiments/run.sh -n 3 -s steady -k 1,5     one scenario, two sizes
#   experiments/run.sh -q                        a quick pass, to see that everything works
#   experiments/run.sh -n 5 -o <directory>       continue a session that was interrupted
#
#   -n <rounds>     how many times every scenario runs (default 3)
#   -s <scenarios>  steady, outage, churn; comma separated (default all)
#   -k <nodes>      numbers of nodes, comma separated (default 1,3,5,10)
#   -o <directory>  where the results go (default experiments/results/<time>);
#                   runs already there are not repeated
#   -q              quick: 2 generators of 50 lines/s for 20 s on every node
#
# The load and the nodes are set from the environment: CONTAINERS, RATE,
# DURATION, NODE_CPUS, NODE_MEM (see lib.sh). ES_ADDRESS names an
# Elasticsearch to use instead of starting one; PYTHON a Python with
# matplotlib.
#
# In every round each scenario and size runs once, and each round starts one
# place further down the list, so that no scenario always follows the same
# one. The results:
#
#   runs.csv      one line per run and measured value
#   summary.csv   per scenario, size and value: median, mean, deviation, range
#   figures/      the charts, as PNG and PDF
#   runs/         what each run left: samples, the verifier's answer, logs
set -u

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
. "$EXP/scenarios.sh"

ROUNDS=3
SCENARIOS=steady,outage,churn
NODES=1,3,5,10
OUT=
while getopts "n:s:k:o:qh" opt; do
	case $opt in
	n) ROUNDS=$OPTARG ;;
	s) SCENARIOS=$OPTARG ;;
	k) NODES=$OPTARG ;;
	o) OUT=$OPTARG ;;
	q) CONTAINERS=2 RATE=50 DURATION=20 ;;
	*)
		sed -n '2,/^set -u/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'
		exit 2
		;;
	esac
done
[ -n "$OUT" ] || OUT=$EXP/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT/done" "$OUT/runs"
OUT=$(cd "$OUT" && pwd)

pairs=()
for s in ${SCENARIOS//,/ }; do
	declare -F "during_$s" >/dev/null || die "there is no scenario '$s'"
	for k in ${NODES//,/ }; do
		pairs+=("$s $k")
	done
done

need
build
es_up
net_up
trap nodes_stop EXIT
trap 'exit 130' INT TERM

{
	echo "date: $(date -Is)"
	echo "commit: $(git -C "$REPO" describe --always --dirty 2>/dev/null)"
	echo "rounds: $ROUNDS; scenarios: $SCENARIOS; nodes: $NODES"
	echo "load per node: $CONTAINERS generators x $RATE lines/s for $DURATION s"
	echo "node: $NODE_CPUS CPU, $NODE_MEM memory"
	echo "go: $(go version | cut -d' ' -f3-)"
	echo "docker: $(docker version --format '{{.Server.Version}}'); node image: $DIND"
	echo "elasticsearch: $(curl -fs "$ES_ADDRESS" | sed -n 's/.*"number" *: *"\([^"]*\)".*/\1/p')"
	echo "kernel: $(uname -sr)"
	echo "cpu: $(nproc) x $(sed -n 's/^model name[^:]*: //p' /proc/cpuinfo | head -1)"
	echo "memory: $(awk '/MemTotal/ { printf "%.1f GB", $2 / 1e6 }' /proc/meminfo)"
} >>"$OUT/env.txt"

[ -f "$OUT/runs.csv" ] || echo "scenario,nodes,round,metric,value" >"$OUT/runs.csv"
for round in $(seq 1 "$ROUNDS"); do
	for i in $(seq 0 $((${#pairs[@]} - 1))); do
		pair=${pairs[$(((i + round - 1) % ${#pairs[@]}))]}
		s=${pair% *} k=${pair#* }
		[ -e "$OUT/done/$s-k$k-r$round" ] && continue
		# In a shell of its own: a run that cannot go on ends there, and the
		# session continues with the next one.
		(
			trap teardown EXIT
			run_one "$s" "$k" "$round"
		) || log "$s with $k node(s), round $round did not finish"
	done
done

"$PYTHON" "$EXP/analyze.py" "$OUT"
log "results in $OUT"
