#!/usr/bin/env bash
# Runs the experiments: every run of every experiment as many times as asked,
# and then the statistics and the charts over all of them.
#
#   experiments/run.sh -n 5                    every experiment, five times each run
#   experiments/run.sh -n 5 -e nodes           only the system with several nodes
#   experiments/run.sh -n 3 -e compare         only the agent against Fluent Bit and Vector
#   experiments/run.sh -q                      a quick pass, to see that everything works
#   experiments/run.sh -n 5 -o <directory>     continue a session that was interrupted
#
#   -n <rounds>       how many times every run is made (default 3)
#   -e <experiments>  nodes, adverse, compare, ceiling, compress; comma
#                     separated (default all)
#   -s <scenarios>    nodes: steady, outage, churn (default all)
#   -k <nodes>        nodes and adverse: numbers of nodes (default 1,3,5,10)
#   -o <directory>    where the results go (default experiments/results/<time>);
#                     runs already there are not repeated
#   -q                quick: short runs, small loads, two cases
#
# The experiments:
#
#   nodes    the whole system with 1 to 10 nodes: steady load, the server
#            away, agents killed (nodes.sh)
#   adverse  the same system where things are hard for a node: a narrow link
#            that loses packets, a burst of lines, a full store (nodes.sh;
#            ADVERSE, LINK_MBIT, LINK_RTT, LINK_LOSS, BURST_X, STORE_QUOTA)
#   compare  the agent against Fluent Bit and Vector on one node, each under
#            the same limits (collectors.sh; CASES, SUBJECTS)
#   ceiling  the agent under growing loads (collectors.sh; LOADS)
#   compress what the agent's store makes of real logs, against gzip and zstd
#            over the log files (compress.sh; DATASETS, FILE_SECONDS); left
#            out when no real log is given
#
# The rest is set from the environment (lib.sh, collectors.sh): CONTAINERS,
# RATE, DURATION, NODE_CPUS, NODE_MEM, REPLAY, ES_ADDRESS, PYTHON.
#
# In every round each run is made once, and each round starts one place
# further down the list, so that no run always follows the same one. The
# results:
#
#   runs.csv      one line per run and measured value
#   summary.csv   per experiment, place on its axis, series and value: median,
#                 mean, deviation, range
#   figures/      the charts, as PNG and PDF
#   runs/         what each run left: samples, the verifier's answer, logs
set -u

quick_cases=${CASES-} quick_loads=${LOADS-} quick_quota=${STORE_QUOTA-} quick_files=${FILE_SECONDS-}
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
. "$EXP/nodes.sh"
. "$EXP/collectors.sh"
. "$EXP/compress.sh"

ROUNDS=3
EXPERIMENTS=nodes,adverse,compare,ceiling,compress
OUT=
while getopts "n:e:s:k:o:qh" opt; do
	case $opt in
	n) ROUNDS=$OPTARG ;;
	e) EXPERIMENTS=$OPTARG ;;
	s) SCENARIOS=$OPTARG ;;
	k) NODES=$OPTARG ;;
	o) OUT=$OPTARG ;;
	q)
		QUICK=1 CONTAINERS=2 RATE=50 DURATION=20
		[ -n "$quick_cases" ] || CASES="idle0 load1k"
		[ -n "$quick_loads" ] || LOADS="10x100"
		# Small enough for so few lines to fill it.
		[ -n "$quick_quota" ] || STORE_QUOTA=32KiB
		[ -n "$quick_files" ] || FILE_SECONDS=10
		;;
	*)
		sed -n '2,/^set -u/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'
		exit 2
		;;
	esac
done
[ -n "$OUT" ] || OUT=$EXP/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT/done" "$OUT/runs"
OUT=$(cd "$OUT" && pwd)

# The runs of a round: "<experiment> <place on the axis> <series> [what the
# experiment needs to make it]". An experiment may have something of its own
# to check first (<experiment>_need).
units=()
for e in ${EXPERIMENTS//,/ }; do
	declare -F "${e}_units" >/dev/null || die "there is no experiment '$e'"
	if declare -F "${e}_need" >/dev/null; then
		"${e}_need"
	fi
	while read -r line; do
		units+=("$e $line")
	done < <("${e}_units")
done
[ "${#units[@]}" -gt 0 ] || die "nothing to run"

need
build
es_up
net_up
trap nodes_stop EXIT
trap 'exit 130' INT TERM

{
	echo "date: $(date -Is)"
	echo "commit: $(git -C "$REPO" describe --always --dirty 2>/dev/null)"
	echo "rounds: $ROUNDS; experiments: $EXPERIMENTS"
	echo "nodes: scenarios $SCENARIOS; sizes $NODES; per node $CONTAINERS generators x $RATE lines/s for $DURATION s; $NODE_CPUS CPU, $NODE_MEM"
	echo "adverse: scenarios $ADVERSE; link $LINK_MBIT Mbit/s, round trip $LINK_RTT ms, loss $LINK_LOSS %; burst x$BURST_X; store quota $STORE_QUOTA"
	echo "compare: cases $CASES; collectors $SUBJECTS ($FLUENT_BIT_IMAGE, $VECTOR_IMAGE)"
	echo "ceiling: loads $LOADS for $LOAD_SECONDS s; the agent under $CEILING_CPUS CPU"
	echo "compress: logs $(compress_logs | awk '{ printf "%s%s", sep, $0; sep = ", " }'); files of $FILE_SECONDS s; $COMPRESS_CONTAINERS generators x $COMPRESS_RATE lines/s"
	echo "generators write: ${REPLAY:-filler}"
	echo "go: $(go version | cut -d' ' -f3-)"
	echo "docker: $(docker version --format '{{.Server.Version}}'); node image: $DIND"
	echo "elasticsearch: $(curl -fs "$ES_ADDRESS" | sed -n 's/.*"number" *: *"\([^"]*\)".*/\1/p')"
	echo "kernel: $(uname -sr)"
	echo "cpu: $(nproc) x $(sed -n 's/^model name[^:]*: //p' /proc/cpuinfo | head -1)"
	echo "memory: $(awk '/MemTotal/ { printf "%.1f GB", $2 / 1e6 }' /proc/meminfo)"
} >>"$OUT/env.txt"

[ -f "$OUT/runs.csv" ] || echo "experiment,x,series,round,metric,value" >"$OUT/runs.csv"
for round in $(seq 1 "$ROUNDS"); do
	for i in $(seq 0 $((${#units[@]} - 1))); do
		read -r e x series arg <<<"${units[$(((i + round - 1) % ${#units[@]}))]}"
		[ -e "$OUT/done/$e-$x-$series-r$round" ] && continue
		# In a shell of its own: a run that cannot go on ends there, and the
		# session continues with the next one.
		(
			trap teardown EXIT
			# shellcheck disable=SC2086
			"${e}_run" "$x" "$series" "$round" $arg
		) || log "$e $x $series, round $round did not finish"
	done
done

"$PYTHON" "$EXP/analyze.py" "$OUT"
log "results in $OUT"
