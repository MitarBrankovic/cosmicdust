# Shared by run.sh and the experiments: building, Elasticsearch, the server,
# the fictive nodes, sampling and the bookkeeping of results. Sourced, not run.
#
# One machine plays the whole system. The server runs as a process on the
# host and indexes into an Elasticsearch in a container; every node is a
# container with a Docker daemon of its own (experiments/node), in which the
# agent and the log generators run. The nodes reach the server over the
# gateway of a Docker network, as they would over a real one.

EXP=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$EXP/.." && pwd)

# The load on every node, and the node itself. All can be set from outside.
: "${CONTAINERS:=5}"      # generators per node
: "${RATE:=100}"          # lines per second of each generator
: "${DURATION:=60}"       # seconds of load
: "${NODE_CPUS:=1}"       # CPUs of a node
: "${NODE_MEM:=512m}"     # memory of a node
: "${SAMPLE_EVERY:=2}"    # seconds between two looks at the processes
: "${INDEX_TIMEOUT:=300}" # the longest wait for the lines to be searchable
: "${REPLAY:=}"           # a log file whose lines the generators write, instead of filler

# The conditions of the runs that make things hard for a node.
: "${LINK_MBIT:=1}"        # a narrow link: megabits per second, each way
: "${LINK_RTT:=100}"       # its round trip, in milliseconds
: "${LINK_LOSS:=5}"        # the packets it loses, in percent, each way
: "${BURST_X:=5}"          # a burst: how many times the load
: "${STORE_QUOTA:=256KiB}" # a full store: the most the agent's store may hold

: "${ES_PORT:=19200}" "${ES_VERSION:=9.5.5}"
: "${GRPC_PORT:=50071}" "${HTTP_PORT:=18071}" "${NODE_PORT_BASE:=21000}"
: "${DIND:=docker:29-dind}"
: "${PYTHON:=python3}"

NET=cosmicdust-exp
ES_NAME=cosmicdust-exp-es
NODE_IMAGE=cosmicdust-node:exp
LOGGEN_IMAGE=cosmicdust-loggen:exp
AGENT_IMAGE=cosmicdust-agent:exp

log() { echo "$(date +%H:%M:%S) $*" >&2; }
die() {
	log "error: $*"
	exit 1
}

need() {
	local tool
	for tool in go docker curl awk "$PYTHON"; do
		command -v "$tool" >/dev/null || die "$tool is not installed"
	done
	"$PYTHON" -c 'import matplotlib' 2>/dev/null || die "$PYTHON has no matplotlib (set PYTHON to one that has)"
	docker info >/dev/null 2>&1 || die "cannot reach the Docker daemon"
	[ -z "$REPLAY" ] || [ -r "$REPLAY" ] || die "cannot read REPLAY=$REPLAY"
}

# build makes everything a run needs: the server and the verifier for the
# host, the node's image with the agent and the generator inside, and the
# agent as an image of its own, for the runs that put it in a container.
build() {
	BIN=$OUT/bin
	local ctx=$OUT/build
	mkdir -p "$BIN" "$ctx" "$OUT/images/agent"
	log "building"
	(cd "$REPO" &&
		go build -o "$BIN/cosmicdust-server" ./cmd/server &&
		go build -o "$BIN/verify" ./experiments/cmd/verify &&
		CGO_ENABLED=0 go build -o "$ctx/cosmicdust-agent" ./cmd/agent &&
		CGO_ENABLED=0 go build -o "$ctx/loggen" ./experiments/cmd/loggen) || die "build failed"
	cp "$EXP/node/entrypoint.sh" "$ctx/entrypoint.sh"
	chmod +x "$ctx/entrypoint.sh"
	docker build -q -f "$EXP/cmd/loggen/Dockerfile" -t "$LOGGEN_IMAGE" "$ctx" >/dev/null || die "generator image"
	docker save "$LOGGEN_IMAGE" -o "$ctx/loggen.tar"
	docker build -q --build-arg "DIND=$DIND" -f "$EXP/node/Dockerfile" -t "$NODE_IMAGE" "$ctx" >/dev/null || die "node image"
	printf 'FROM scratch\nCOPY cosmicdust-agent /cosmicdust-agent\nENTRYPOINT ["/cosmicdust-agent"]\n' |
		docker build -q -f - -t "$AGENT_IMAGE" "$ctx" >/dev/null || die "agent image"
	docker save "$AGENT_IMAGE" -o "$OUT/images/agent/agent.tar"
}

# es_up makes sure there is an Elasticsearch to index into. Given ES_ADDRESS
# it uses that one; otherwise it runs one in a container of its own and never
# touches an Elasticsearch it did not start.
es_up() {
	if [ -n "${ES_ADDRESS:-}" ]; then
		curl -fs "$ES_ADDRESS/_cluster/health" >/dev/null || die "no Elasticsearch at $ES_ADDRESS"
		return
	fi
	ES_ADDRESS=http://127.0.0.1:$ES_PORT
	if ! docker ps --format '{{.Names}}' | grep -qx "$ES_NAME"; then
		if curl -fs "$ES_ADDRESS/_cluster/health" >/dev/null 2>&1; then
			die "something else answers on port $ES_PORT: set ES_ADDRESS to use it, or ES_PORT to another port"
		fi
		log "starting Elasticsearch $ES_VERSION"
		docker rm -f "$ES_NAME" >/dev/null 2>&1 || true
		docker run -d --name "$ES_NAME" -p "127.0.0.1:$ES_PORT:9200" -m 2g \
			-e discovery.type=single-node -e xpack.security.enabled=false -e ES_JAVA_OPTS="-Xms1g -Xmx1g" \
			"elasticsearch:$ES_VERSION" >/dev/null || die "cannot start Elasticsearch"
	fi
	local i
	for i in $(seq 1 120); do
		curl -fs "$ES_ADDRESS/_cluster/health" >/dev/null 2>&1 && return
		sleep 2
	done
	die "Elasticsearch did not come up"
}

# es_drop deletes what a run indexed.
es_drop() {
	curl -fs -XDELETE "$ES_ADDRESS/_data_stream/$1-*" >/dev/null 2>&1 || true
	curl -fs -XDELETE "$ES_ADDRESS/_index_template/$1-*" >/dev/null 2>&1 || true
}

# es_lines is how many log lines of a run are searchable.
es_lines() {
	curl -fs "$ES_ADDRESS/$1-logs-*/_count?ignore_unavailable=true&allow_no_indices=true" 2>/dev/null |
		sed -n 's/.*"count":\([0-9]*\).*/\1/p'
}

net_up() {
	docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
	GATEWAY=$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET")
	[ -n "$GATEWAY" ] || die "the network $NET has no gateway"
}

# server_start runs the server over a run's directory, indexing under prefix.
# Started again over the same directory it continues where it stopped.
server_start() { # <run directory> <index prefix>
	DATA_DIR="$1/server" GRPC_ADDRESS="0.0.0.0:$GRPC_PORT" HTTP_ADDRESS="127.0.0.1:$HTTP_PORT" \
		ES_ADDRESS="$ES_ADDRESS" ES_INDEX_PREFIX="$2" ES_REPLICAS=0 COMPACT_EVERY=0 \
		"$BIN/cosmicdust-server" >>"$1/server.log" 2>&1 &
	SERVER_PID=$!
	local i
	for i in $(seq 1 50); do
		curl -fs "http://127.0.0.1:$HTTP_PORT/healthz" >/dev/null 2>&1 && return
		sleep 0.2
	done
	die "the server did not start: $(tail -3 "$1/server.log")"
}

server_stop() { # [signal]
	[ -n "${SERVER_PID:-}" ] || return 0
	kill "-${1:-TERM}" "$SERVER_PID" 2>/dev/null || true
	wait "$SERVER_PID" 2>/dev/null || true
	SERVER_PID=
}

node_name() { echo "cosmicdust-node-$1"; }
node_exec() { # <node> <command...>
	local i=$1
	shift
	docker exec "$(node_name "$i")" "$@"
}

# node_run starts node i; whatever follows goes to docker run before the
# image: limits, mounts, settings.
node_run() { # <node> <docker run arguments...>
	local i=$1 replay=()
	shift
	if [ -n "$REPLAY" ]; then
		replay=(-v "$REPLAY:/replay.log:ro")
	fi
	docker run -d --privileged --name "$(node_name "$i")" --hostname "node-$i" --network "$NET" \
		-p "127.0.0.1:$((NODE_PORT_BASE + i)):9101" "${replay[@]}" \
		-e COSMICDUST_NODE="node-$i" -e COSMICDUST_SYNC_URL="grpc://$GATEWAY:$GRPC_PORT" \
		"$@" "$NODE_IMAGE" >/dev/null || die "cannot start node $i"
}

# node_wait waits until node i has its Docker daemon up and its images
# loaded, and - unless told otherwise - its agent answering.
node_wait() { # <node> [docker]
	local i=$1 tries
	for tries in $(seq 1 600); do
		if node_exec "$i" test -e /run/node-ready 2>/dev/null; then
			if [ "${2:-agent}" = docker ] ||
				curl -fs --max-time 1 "http://127.0.0.1:$((NODE_PORT_BASE + i))/healthz" >/dev/null 2>&1; then
				return 0
			fi
		fi
		sleep 0.5
	done
	die "node $i did not come up: $(docker logs --tail 5 "$(node_name "$i")" 2>&1)"
}

# nodes_start runs k nodes with the agent in each, limited to NODE_CPUS and
# NODE_MEM, and waits for all of them. NODE_ARGS are more arguments of docker
# run for every node: the agent's settings, as COSMICDUST_<FLAG>.
NODE_ARGS=()
nodes_start() { # <k>
	local i
	for i in $(seq 1 "$1"); do
		node_run "$i" --cpus "$NODE_CPUS" -m "$NODE_MEM" "${NODE_ARGS[@]}"
	done
	for i in $(seq 1 "$1"); do
		node_wait "$i"
	done
}

# link_shape makes the way between node i and the server a narrow one:
# LINK_MBIT megabits per second, a round trip of LINK_RTT ms, and LINK_LOSS
# percent of the packets lost, each way. Only what goes to the server's port
# and comes from it is shaped, so the node's metrics are read as before. What
# comes in is shaped on an ifb device: a queue holds back only what leaves.
link_shape() { # <node>
	local half=$((LINK_RTT / 2)) limit netem
	# netem's limit counts the packets in its delay line too: about a second's worth.
	limit=$((LINK_MBIT * 1000000 / 8 * (1000 + half) / 1000 / 1514 + 1))
	netem="netem delay ${half}ms loss ${LINK_LOSS}% rate ${LINK_MBIT}mbit limit $limit"
	node_exec "$1" sh -e -c "
		# Whole packets, not the batches the kernel would hand over: a loss is one packet.
		ethtool -K eth0 tso off gso off gro off >/dev/null
		ip link add ifb0 type ifb
		ip link set ifb0 up
		tc qdisc add dev eth0 root handle 1: prio bands 3 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
		tc qdisc add dev eth0 parent 1:3 handle 30: $netem
		tc filter add dev eth0 parent 1: protocol ip u32 \
			match ip protocol 6 0xff match ip dport $GRPC_PORT 0xffff flowid 1:3
		tc qdisc add dev ifb0 root handle 30: $netem
		tc qdisc add dev eth0 handle ffff: ingress
		tc filter add dev eth0 parent ffff: protocol ip u32 \
			match ip protocol 6 0xff match ip sport $GRPC_PORT 0xffff action mirred egress redirect dev ifb0
	" || die "cannot shape the link of node $1"
}

# link_stats prints what the links of k nodes carried to the server and what
# they lost on the way: "<megabytes> <percent of the packets>".
link_stats() { # <k>
	local i
	for i in $(seq 1 "$1"); do
		node_exec "$i" tc -s qdisc show dev eth0
	done | awk '
		/^qdisc/ { netem = ($2 == "netem") }
		netem && $1 == "Sent" { bytes += $2; sent += $4; lost += $7 }
		END { printf "%.2f %.2f\n", bytes / 1e6, (sent + lost > 0 ? 100 * lost / (sent + lost) : 0) }'
}

nodes_stop() {
	docker ps -aq --filter "name=^cosmicdust-node-" | xargs -r docker rm -fv >/dev/null 2>&1 || true
}

# generator_args are loggen's arguments for count lines at rate lines per
# second, of filler or - with REPLAY - of a real log's text.
generator_args() { # <id> <rate> <count>
	echo "-id $1 -rate $2 -count $3${REPLAY:+ -replay /data/replay.log}"
}

# generator_mounts gives a generator the log it replays.
generator_mounts() {
	echo "${REPLAY:+-v /replay.log:/data/replay.log:ro}"
}

# generators_start runs CONTAINERS generators on every node: each writes
# count numbered lines at RATE lines per second (or the rate given) and ends.
# A generator's id is "node-<i>/<letter><j>", with the letter c unless given.
generators_start() { # <k> <count> [letter] [rate]
	local i j letter=${3:-c} rate=${4:-$RATE} pids=()
	for i in $(seq 1 "$1"); do
		for j in $(seq 1 "$CONTAINERS"); do
			# shellcheck disable=SC2046
			node_exec "$i" docker run -d --name "gen-$letter$j" --label "app=app-$j" $(generator_mounts) "$LOGGEN_IMAGE" \
				$(generator_args "node-$i/$letter$j" "$rate" "$2") >/dev/null &
			pids+=($!)
		done
	done
	# Only these: the sampler runs in the background too, and never ends.
	wait "${pids[@]}"
}

# generators_wait returns once no generator of k nodes is still writing.
generators_wait() { # <k>
	local i pids=()
	for i in $(seq 1 "$1"); do
		node_exec "$i" sh -c 'docker wait $(docker ps -q --filter name=gen-) >/dev/null 2>&1 || true' &
		pids+=($!)
	done
	wait "${pids[@]}"
}

# A crash is a container that writes one line and exits with an error: the
# agent reports it at once, as an urgent event.
crash() { # <node> <name>
	node_exec "$1" docker run -d --name "$2" --label app=crash "$LOGGEN_IMAGE" -id "node-$1/x-$2" -count 1 -exit 3 >/dev/null
}

scrape() { curl -s --max-time 2 "$1/metrics" 2>/dev/null; }
server_metric() { scrape "http://127.0.0.1:$HTTP_PORT" | awk -v k="$1" '$1 == k { print $2; exit }'; }

# agents_sum adds up one value of the agents' metrics over k nodes.
agents_sum() { # <k> <metric>
	local i
	for i in $(seq 1 "$1"); do
		scrape "http://127.0.0.1:$((NODE_PORT_BASE + i))"
	done | awk -v k="$2" '$1 == k { s += $2 } END { printf "%d\n", s }'
}

# sampler writes, every SAMPLE_EVERY seconds, the CPU time and the resident
# memory of every agent and of the server, as they report them themselves:
#   time,who,cpu seconds,resident bytes
sampler() { # <k>
	local i now
	while :; do
		now=$(date +%s.%N)
		for i in $(seq 1 "$1"); do
			scrape "http://127.0.0.1:$((NODE_PORT_BASE + i))" | sample_of "$now" "node-$i"
		done
		scrape "http://127.0.0.1:$HTTP_PORT" | sample_of "$now" server
		sleep "$SAMPLE_EVERY"
	done
}

sample_of() { # <time> <who>
	awk -v t="$1" -v w="$2" '
		$1 == "process_cpu_seconds_total" { c = $2 }
		$1 == "process_resident_memory_bytes" { r = $2 }
		END { if (c != "") printf "%s,%s,%s,%s\n", t, w, c, r }'
}

sample_start() { # <k> <file>
	sampler "$1" >>"$2" &
	SAMPLER_PID=$!
}

sample_stop() {
	[ -n "${SAMPLER_PID:-}" ] || return 0
	kill "$SAMPLER_PID" 2>/dev/null || true
	wait "$SAMPLER_PID" 2>/dev/null || true
	SAMPLER_PID=
}

# usage turns the samples between two times into "who cpu% MB" lines: the CPU
# as a percentage of one core over the time sampled, the memory as the most
# seen. A process that was restarted counts from zero again.
usage() { # <samples> <from> <to>
	awk -F, -v a="$2" -v b="$3" '
		$1 >= a && $1 <= b {
			if ($2 in last) { d = $3 - last[$2]; cpu[$2] += (d >= 0 ? d : $3) }
			else first[$2] = $1
			last[$2] = $3; end[$2] = $1
			if ($4 > rss[$2]) rss[$2] = $4
		}
		END {
			for (w in last)
				printf "%s %.3f %.1f\n", w, (end[w] > first[w] ? 100 * cpu[w] / (end[w] - first[w]) : 0), rss[w] / 1e6
		}' "$1"
}

# machine is the whole machine's CPU time so far, busy and in all, in ticks.
# Two of them say how many cores were busy in between: with everything on one
# machine, a run during which something else was busy is not worth keeping.
machine() { awk '/^cpu / { idle = $5 + $6; all = 0; for (i = 2; i <= 9; i++) all += $i; print all - idle, all }' /proc/stat; }
busy_cores() { # <machine before> <machine after>
	echo "$1 $2" | awk -v n="$(nproc)" '{ d = $4 - $2; printf "%.2f\n", (d > 0 ? n * ($3 - $1) / d : 0) }'
}

# wait_indexed waits until a run's lines are all searchable and prints how
# long that took; it fails once INDEX_TIMEOUT has passed. Where the agents may
# drop lines for room (MAY_DROP is the number of nodes), a line an agent says
# it dropped is accounted for as well.
wait_indexed() { # <index prefix> <lines expected>
	local start now have
	start=$(date +%s.%N)
	while :; do
		have=$(es_lines "$1")
		if [ -n "${MAY_DROP:-}" ]; then
			have=$((${have:-0} + $(agents_sum "$MAY_DROP" cosmicdust_agent_store_dropped_rows_total)))
		fi
		now=$(date +%s.%N)
		if [ "${have:-0}" -ge "$2" ]; then
			awk -v a="$start" -v b="$now" 'BEGIN { printf "%.1f\n", b - a }'
			return 0
		fi
		if awk -v a="$start" -v b="$now" -v t="$INDEX_TIMEOUT" 'BEGIN { exit !(b - a > t) }'; then
			log "only ${have:-0} of $2 lines searchable after $INDEX_TIMEOUT s"
			return 1
		fi
		sleep 1
	done
}

since() { awk -v a="$1" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f\n", b - a }'; }

# A run belongs to an experiment and is one point of it: a place on the
# experiment's axis (X: a number of nodes, a load) and a series (SERIES: a
# scenario, a collector). begin sets up its directory and its names.
begin() { # <experiment> <x> <series> <round>
	EXPERIMENT=$1 X=$2 SERIES=$3 ROUND=$4
	RUN=$OUT/runs/$1-$2-$3-r$4
	PREFIX=$(echo "exp-$1-$2-$3-r$4" | tr 'A-Z_' 'a-z-')
	rm -rf "$RUN"
	mkdir -p "$RUN"
	teardown
	es_drop "$PREFIX"
}

# emit adds one measured value of the run in progress.
emit() { # <metric> <value>
	echo "$EXPERIMENT,$X,$SERIES,$ROUND,$1,$2" >>"$RUN/rows.csv"
}

# verified reads every generator's lines back through the query API and
# emits what came of it; it fails unless every line is there exactly once.
# A run may have set:
#   ALSO, ALSO_LINES  more generators on every node, as verify's -also, and
#                     the lines they wrote per node
#   MAY_MISS          lines that are rightly not there: the agents recorded
#                     dropping exactly that many
verified() { # <generators per node> <lines per generator> <node>...
	local generators=$1 count=$2 stored missing duplicate expected
	shift 2
	expected=$(($# * (generators * count + ${ALSO_LINES:-0})))
	# shellcheck disable=SC2086
	"$BIN/verify" -server "127.0.0.1:$GRPC_PORT" -generators "$generators" -count "$count" ${ALSO:-} "$@" \
		>"$RUN/verify.txt" 2>"$RUN/verify.err" || true
	awk -v k="$#" -v expected="$expected" '
		{ n++; for (i = 1; i <= NF; i++) { split($i, kv, "="); sum[kv[1]] += kv[2] } }
		END {
			if (n != k) { sum["lines"] = 0; sum["missing"] = expected }
			print sum["lines"] + 0, sum["missing"] + 0, sum["duplicate"] + 0
		}' "$RUN/verify.txt" >"$RUN/lines.txt"
	read -r stored missing duplicate <"$RUN/lines.txt"
	emit lines_expected "$expected"
	emit lines_stored "$stored"
	emit lines_missing "$missing"
	emit lines_duplicate "$duplicate"
	if [ "$missing" -eq "${MAY_MISS:-0}" ] && [ "$duplicate" -eq 0 ]; then
		emit exactly_once 1
		return 0
	fi
	emit exactly_once 0
	log "$stored lines stored, $missing missing (${MAY_MISS:-0} recorded as dropped), $duplicate twice; see $RUN"
	return 1
}

# finish stops everything and hands the run's values to the session.
finish() {
	teardown
	es_drop "$PREFIX"
	cat "$RUN/rows.csv" >>"$OUT/runs.csv"
	touch "$OUT/done/$EXPERIMENT-$X-$SERIES-r$ROUND"
}

teardown() {
	sample_stop
	nodes_stop
	server_stop
}
