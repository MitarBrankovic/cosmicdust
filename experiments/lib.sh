# Shared by run.sh and scenarios.sh: building, Elasticsearch, the server, the
# fictive nodes, sampling and the bookkeeping of results. Sourced, not run.
#
# One machine plays the whole system. The server runs as a process on the
# host and indexes into an Elasticsearch in a container; every node is a
# container with a Docker daemon of its own (experiments/node), in which the
# agent and the log generators run. The nodes reach the server over the
# gateway of a Docker network, as they would over a real one.

EXP=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$EXP/.." && pwd)

# The load on every node, and the node itself. All can be set from outside.
: "${CONTAINERS:=5}"    # generators per node
: "${RATE:=100}"        # lines per second of each generator
: "${DURATION:=60}"     # seconds of load
: "${NODE_CPUS:=1}"     # CPUs of a node
: "${NODE_MEM:=512m}"   # memory of a node
: "${SAMPLE_EVERY:=2}"  # seconds between two looks at the processes
: "${INDEX_TIMEOUT:=300}" # the longest wait for the lines to be searchable

: "${ES_PORT:=19200}" "${ES_VERSION:=9.5.5}"
: "${GRPC_PORT:=50071}" "${HTTP_PORT:=18071}" "${NODE_PORT_BASE:=21000}"
: "${DIND:=docker:29-dind}"
: "${PYTHON:=python3}"

NET=cosmicdust-exp
ES_NAME=cosmicdust-exp-es
NODE_IMAGE=cosmicdust-node:exp
LOGGEN_IMAGE=cosmicdust-loggen:exp

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
}

# build makes everything a run needs: the server and the verifier for the
# host, and the node's image with the agent and the generator inside.
build() {
	BIN=$OUT/bin
	local ctx=$OUT/build
	mkdir -p "$BIN" "$ctx"
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

# nodes_start runs k nodes and waits until each has its Docker daemon up and
# its agent answering.
nodes_start() { # <k>
	local i tries
	for i in $(seq 1 "$1"); do
		docker run -d --privileged --name "$(node_name "$i")" --hostname "node-$i" --network "$NET" \
			--cpus "$NODE_CPUS" -m "$NODE_MEM" -p "127.0.0.1:$((NODE_PORT_BASE + i)):9101" \
			-e COSMICDUST_NODE="node-$i" -e COSMICDUST_SYNC_URL="grpc://$GATEWAY:$GRPC_PORT" \
			"$NODE_IMAGE" >/dev/null || die "cannot start node $i"
	done
	for i in $(seq 1 "$1"); do
		for tries in $(seq 1 300); do
			curl -fs --max-time 1 "http://127.0.0.1:$((NODE_PORT_BASE + i))/healthz" >/dev/null 2>&1 && break
			sleep 0.5
		done
		[ "$tries" -lt 300 ] || die "node $i did not come up: $(docker logs --tail 5 "$(node_name "$i")" 2>&1)"
	done
}

nodes_stop() {
	docker ps -aq --filter "name=^cosmicdust-node-" | xargs -r docker rm -fv >/dev/null 2>&1 || true
}

# generators_start runs the generators of every node: each writes count
# numbered lines at RATE lines per second and ends.
generators_start() { # <k> <count>
	local i j pids=()
	for i in $(seq 1 "$1"); do
		for j in $(seq 1 "$CONTAINERS"); do
			node_exec "$i" docker run -d --name "gen-$j" --label "app=app-$j" "$LOGGEN_IMAGE" \
				-id "node-$i/c$j" -rate "$RATE" -count "$2" >/dev/null &
			pids+=($!)
		done
	done
	# Only these: the sampler runs in the background too, and never ends.
	wait "${pids[@]}"
}

# A crash is a container that writes one line and exits with an error: the
# agent reports it at once, as an urgent event.
crash() { # <node> <name>
	node_exec "$1" docker run -d --name "$2" --label app=crash "$LOGGEN_IMAGE" -id "node-$1/x-$2" -count 1 -exit 3 >/dev/null
}

scrape() { curl -s --max-time 2 "$1/metrics" 2>/dev/null; }
server_metric() { scrape "http://127.0.0.1:$HTTP_PORT" | awk -v k="$1" '$1 == k { print $2; exit }'; }

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

# wait_indexed waits until a run's lines are all searchable and prints how
# long that took; it fails once INDEX_TIMEOUT has passed.
wait_indexed() { # <index prefix> <lines expected>
	local start now have
	start=$(date +%s.%N)
	while :; do
		have=$(es_lines "$1")
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

# emit adds one measured value of the run in progress.
emit() { # <metric> <value>
	echo "$SCENARIO,$K,$ROUND,$1,$2" >>"$RUN/rows.csv"
}

teardown() {
	sample_stop
	nodes_stop
	server_stop
}
