# The experiments "compare" and "ceiling": one log collector on one node,
# under a CPU and memory limit of its own. Sourced by run.sh after lib.sh.
#
# The node is roomy and runs no agent of its own; the collector under test
# runs in it as a container, limited like any workload would be, so that the
# agent, Fluent Bit and Vector are measured the same way: by the counters of
# their own cgroup. The generators and the node's Docker run outside that
# limit.
#
#   compare  the agent against Fluent Bit and Vector over a set of cases; the
#            axis is the case, a series is a collector
#   ceiling  the agent alone under growing loads; the axis is lines per second.
#            It gets a part of a core (CEILING_CPUS): a node of this kind
#            cannot write as many lines as the agent reads with a whole one
#
# The three do not do the same work. The agent stores what it reads in
# Parquet and sends it to the server, and its run ends when every line can be
# searched there. Fluent Bit tails and writes a plain file; Vector tails,
# parses and writes a zstd file. The comparison is what a node pays for its
# log collector, not a benchmark of equal pipelines.

: "${CASES:=idle0 idle200 load1k load5k load10k edge25 edge10}"
: "${SUBJECTS:=agent fluent-bit vector}"
: "${LOADS:=10x500 20x500 40x500 60x500}"
: "${CEILING_CPUS:=0.25}"
: "${FLUENT_BIT_IMAGE:=fluent/fluent-bit:5.1.2}"
: "${VECTOR_IMAGE:=timberio/vector:0.58.0-debian}"
: "${COLLECTOR_NODE_CPUS:=8}" "${COLLECTOR_NODE_MEM:=4g}"
: "${LOAD_SECONDS:=60}" # how long a load written "NxR" lasts

# load_of sets what a case means: N containers of R lines per second for D
# seconds, and the collector's limits. A case can also be written "NxR".
load_of() { # <case>
	CPUS=1 MEM=512m
	case $1 in
	idle0) N=0 R=0 D=90 ;;     # the collector alone
	idle200) N=200 R=0 D=120 ;; # a full, quiet node
	load1k) N=10 R=100 D=120 ;;
	load5k) N=10 R=500 D=120 ;;
	load10k) N=20 R=500 D=120 ;;
	edge25) N=10 R=100 D=120 CPUS=0.25 MEM=128m ;; # a quarter of a core
	edge10) N=10 R=100 D=120 CPUS=0.10 MEM=256m ;; # a tenth of a core
	*x*) N=${1%x*} R=${1#*x} D=$LOAD_SECONDS CPUS=${LOAD_CPUS:-1} ;;
	*) die "there is no case '$1'" ;;
	esac
	if [ -n "${QUICK:-}" ]; then # checks the chain, measures nothing
		D=20
		[ "$N" -le 20 ] || N=20
	fi
}

compare_need() {
	case " $SUBJECTS " in
	*" vector "*) command -v zstdcat >/dev/null || die "zstdcat is not installed (Vector's output is counted with it)" ;;
	esac
}

compare_units() { # "<case> <collector>"
	local c s
	for c in $CASES; do
		for s in $SUBJECTS; do
			echo "$c $s"
		done
	done
}
compare_run() { collector_run compare "$1" "$2" "$1" "$3"; } # <case> <collector> <round>

ceiling_units() { # "<lines per second> agent <load>"
	local l
	for l in $LOADS; do
		echo "$((${l%x*} * ${l#*x})) agent $l"
	done
}
ceiling_run() { LOAD_CPUS=$CEILING_CPUS collector_run ceiling "$1" "$2" "$4" "$3"; } # <lines per second> agent <round> <load>

# collector_image makes sure the collector's image is there as a tar file,
# for the node to load.
collector_image() { # <collector>
	local image
	case $1 in
	agent) return 0 ;; # built with everything else
	fluent-bit) image=$FLUENT_BIT_IMAGE ;;
	vector) image=$VECTOR_IMAGE ;;
	*) die "there is no collector '$1'" ;;
	esac
	[ -s "$OUT/images/$1/$1.tar" ] && return 0
	mkdir -p "$OUT/images/$1"
	docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null || die "cannot pull $image"
	docker save "$image" -o "$OUT/images/$1/$1.tar"
}

fluent_bit_config() { # <paths, comma separated>
	cat <<YML
service:
  flush: 1
  log_level: warn
parsers:
  - name: docker-json
    format: json
    time_key: time
    time_format: "%Y-%m-%dT%H:%M:%S.%L%z"
pipeline:
  inputs:
    - name: tail
      path: $1
      parser: docker-json
      db: /out/fluent-bit.db
      read_from_head: true
      refresh_interval: 1
      mem_buf_limit: 64MB
  outputs:
    - name: file
      match: "*"
      path: /out/data
      file: out.log
      format: plain
YML
}

vector_config() { # <paths, comma separated>
	cat <<YML
data_dir: /out/vector-data
sources:
  gen:
    type: file
    read_from: beginning
    include:
$(echo "$1" | tr ',' '\n' | sed 's/.*/      - "&"/')
transforms:
  parse:
    type: remap
    inputs: [gen]
    source: |
      . = merge!(., object!(parse_json!(.message)))
      del(.message)
sinks:
  out:
    type: file
    inputs: [parse]
    path: /out/data/vector.log.zst
    compression: zstd
    encoding: { codec: json }
YML
}

# What a cgroup of the node has counted so far, read inside the node. Every
# container has a cgroup of its own; the node's Docker daemon, its containerd
# and their shims are in the cgroup "init".
cg_value() { # <cgroup> <file> <key>
	node_exec 1 awk -v k="$3" '$1 == k { print $2 }' "$1/$2"
}
cg_cpu() { cg_value "$1" cpu.stat usage_usec; }

# collector_run is one run: a collector, a case.
collector_run() { # <experiment> <x> <collector> <case> <round>
	local subject=$3 c=$4 ids id paths="" count lines out cg failed=0 i
	local from to before after u0 u1 u2 u3 n0 n1 r0 r1 th0 th1 anon peak settle have bytes
	local cpu runtime rest throttled anonmb read patience
	load_of "$c"
	begin "$1" "$2" "$subject" "$5"
	log "$1: $c, $subject, round $5: $N x $R lines/s for $D s under $CPUS CPU and $MEM"
	collector_image "$subject"
	if [ "$subject" = agent ]; then
		server_start "$RUN" "$PREFIX"
	fi
	node_run 1 --cpus "$COLLECTOR_NODE_CPUS" -m "$COLLECTOR_NODE_MEM" -e NODE_AGENT=off \
		-v "$OUT/images/$subject:/images:ro"
	node_wait 1 docker
	node_exec 1 mkdir -p /out/data /out/vector-data

	# The generators are created now, so that the names of their log files
	# are known, and started later. A quiet one writes a single line.
	count=$((R > 0 ? R * D : 1))
	ids=$(node_exec 1 sh -c '
		i=1
		while [ "$i" -le "$1" ]; do
			if [ "$2" -gt 0 ]; then
				args="-id node-1/c$i -rate $2 -count $3 $6"
			else
				args="-id node-1/c$i -rate 1 -count 1 -hold"
			fi
			docker create --name "gen-$i" --label "app=app-$((i % 5 + 1))" $5 "$4" $args
			i=$((i + 1))
		done' sh "$N" "$R" "$count" "$LOGGEN_IMAGE" "$(generator_mounts)" "${REPLAY:+-replay /data/replay.log}") ||
		die "cannot create the generators"
	for id in $ids; do
		paths="$paths,/var/lib/docker/containers/$id/$id-json.log"
	done
	paths=${paths#,}
	[ -n "$paths" ] || paths="/var/lib/docker/containers/none-*/none-json.log"

	case $subject in
	agent)
		id=$(node_exec 1 docker run -d --name subject --cpus "$CPUS" -m "$MEM" --network host --cgroupns host \
			-v /var/lib/docker/containers:/var/lib/docker/containers:ro \
			-v /var/run/docker.sock:/var/run/docker.sock:ro \
			-v /run/docker/containerd/containerd.sock:/run/containerd/containerd.sock:ro \
			-e COSMICDUST_NODE=node-1 -e COSMICDUST_SYNC_URL="$(sync_url)" \
			-e COSMICDUST_APP_LABELS=app "$AGENT_IMAGE") || die "cannot start the agent"
		;;
	fluent-bit)
		fluent_bit_config "$paths" | docker exec -i "$(node_name 1)" sh -c 'cat >/out/fluent-bit.yaml'
		id=$(node_exec 1 docker run -d --name subject --cpus "$CPUS" -m "$MEM" \
			-v /var/lib/docker/containers:/var/lib/docker/containers:ro -v /out:/out \
			"$FLUENT_BIT_IMAGE" -c /out/fluent-bit.yaml) || die "cannot start Fluent Bit"
		;;
	vector)
		vector_config "$paths" | docker exec -i "$(node_name 1)" sh -c 'cat >/out/vector.yaml'
		id=$(node_exec 1 docker run -d --name subject --cpus "$CPUS" -m "$MEM" \
			-v /var/lib/docker/containers:/var/lib/docker/containers:ro -v /out:/out \
			"$VECTOR_IMAGE" --config /out/vector.yaml) || die "cannot start Vector"
		;;
	esac
	cg=$(node_exec 1 sh -c "ls -d /sys/fs/cgroup/docker/$id 2>/dev/null || find /sys/fs/cgroup -maxdepth 4 -type d -name $id | head -1")
	[ -n "$cg" ] || die "the collector has no cgroup"

	sleep 5 # the collector alone, before anything is written
	u0=$(cg_cpu "$cg")
	if [ "$R" -eq 0 ] && [ "$N" -gt 0 ]; then
		# Quiet containers: starting them keeps the runtime and the collector
		# busy for a while; what is measured is the quiet after it.
		node_exec 1 sh -c 'docker start $(docker ps -aq --filter name=gen-) >/dev/null'
		sleep 20
	fi
	from=$(date +%s.%N) before=$(machine)
	u1=$(cg_cpu "$cg") n0=$(cg_cpu /sys/fs/cgroup) r0=$(cg_cpu /sys/fs/cgroup/init)
	th0=$(cg_value "$cg" cpu.stat throttled_usec)
	if [ "$R" -gt 0 ]; then
		node_exec 1 sh -c 'docker start $(docker ps -aq --filter name=gen-) >/dev/null'
		# Until the generators have written their lines and ended. Looking
		# costs the node's runtime something, the same for every collector.
		sleep $((D - 2))
		for i in $(seq 1 60); do
			[ -z "$(node_exec 1 docker ps -q --filter name=gen- --filter status=running)" ] && break
			sleep 2
		done
	else
		sleep "$D"
	fi
	to=$(date +%s.%N) after=$(machine)
	u2=$(cg_cpu "$cg") n1=$(cg_cpu /sys/fs/cgroup) r1=$(cg_cpu /sys/fs/cgroup/init)
	th1=$(cg_value "$cg" cpu.stat throttled_usec)
	anon=$(cg_value "$cg" memory.stat anon)
	lines=$((N * count))
	if [ "$subject" = agent ]; then
		# The moment the last line is written: what the agent has read by then,
		# and what of it can already be searched.
		read=$(agents_sum 1 cosmicdust_agent_tail_lines_total)
		have=$(es_lines "$PREFIX")
	fi

	# The collector's CPU while the generators wrote (or, with none writing,
	# for D seconds) and its anonymous memory at the end of that; the CPU of
	# the node's runtime (dockerd, containerd and their shims) and of the
	# rest of the node, which is the generators.
	awk -v a="$from" -v b="$to" -v u="$((u2 - u1))" -v n="$((n1 - n0))" -v r="$((r1 - r0))" \
		-v th="$((th1 - th0))" -v anon="$anon" 'BEGIN {
		t = (b - a) * 1e4
		printf "%.3f %.3f %.3f %.2f %.1f\n", u / t, r / t, (n - u - r) / t, th / 1e6, anon / 1e6 }' >"$RUN/cost.txt"
	read -r cpu runtime rest throttled anonmb <"$RUN/cost.txt"
	emit collector_cpu_pct "$cpu"
	emit runtime_cpu_pct "$runtime"
	emit generators_cpu_pct "$rest"
	emit throttled_s "$throttled"
	emit collector_anon_mb "$anonmb"
	emit lines_per_s $((N * R))
	if [ "$R" -gt 0 ]; then
		# What the generators managed: less than asked when the node is too busy.
		emit written_per_s "$(awk -v a="$from" -v b="$to" -v l="$lines" 'BEGIN { printf "%.0f", l / (b - a) }')"
	fi

	case $subject in
	agent)
		# How far behind it all was when the last line was written: the agent
		# in its reading, and the whole way to the search - on which the
		# server's indexing has a say as well.
		if [ "$lines" -gt 0 ]; then
			emit unread_pct "$(awk -v h="${read:-0}" -v l="$lines" 'BEGIN { printf "%.1f", (h < l ? 100 * (l - h) / l : 0) }')"
			emit behind_pct "$(awk -v h="${have:-0}" -v l="$lines" 'BEGIN { printf "%.1f", (h < l ? 100 * (l - h) / l : 0) }')"
		fi
		# The more lines, the longer the server may take to index them.
		patience=$((INDEX_TIMEOUT + lines / 5000))
		if settle=$(wait_delivered "$lines" "$patience"); then
			emit delivered_s "$settle"
		else
			failed=1
		fi
		if settle=$(INDEX_TIMEOUT=$patience wait_indexed "$PREFIX" "$lines"); then
			# From the end of the load, like delivered_s.
			emit settle_s "$(since "$to")"
		else
			failed=1
		fi
		if [ "$N" -gt 0 ]; then
			verified "$N" "$count" node-1 || failed=1
			out=$(awk '{ print $1 }' "$RUN/lines.txt")
		fi
		u3=$(cg_cpu "$cg") peak=$(node_exec 1 cat "$cg/memory.peak")
		;;
	*)
		sleep 30 # what is still in its buffers
		u3=$(cg_cpu "$cg") peak=$(node_exec 1 cat "$cg/memory.peak") # its cgroup goes when it stops
		node_exec 1 docker stop -t 30 subject >/dev/null
		if [ "$subject" = fluent-bit ]; then
			out=$(node_exec 1 sh -c 'cat /out/data/* 2>/dev/null | wc -l')
		else
			docker cp "$(node_name 1):/out/data" "$RUN/out" >/dev/null 2>&1 || true
			out=$(zstdcat "$RUN"/out/*.zst 2>/dev/null | wc -l)
			rm -rf "$RUN/out"
		fi
		emit lines_expected "$lines"
		emit lines_stored "${out:-0}"
		;;
	esac
	if [ "$lines" -gt 0 ]; then
		emit complete_pct "$(awk -v o="${out:-0}" -v l="$lines" 'BEGIN { printf "%.2f", 100 * o / l }')"
	fi

	# Over the whole run, to the moment everything was delivered: the CPU the
	# collector spent per GB of logs, and its memory at its largest, the page
	# cache it was charged with included.
	if [ "$R" -gt 0 ]; then
		bytes=$(node_exec 1 sh -c "cat $(echo "$paths" | tr ',' ' ') | wc -c")
		emit cpu_s_per_gb "$(awk -v u="$((u3 - u0))" -v b="$bytes" 'BEGIN { printf "%.1f", (b > 0 ? u / 1e6 / (b / 2^30) : 0) }')"
		emit input_mb "$(awk -v b="$bytes" 'BEGIN { printf "%.1f", b / 1e6 }')"
	fi
	emit collector_peak_mb "$(awk -v p="$peak" 'BEGIN { printf "%.1f", p / 1e6 }')"
	emit machine_busy_cores "$(busy_cores "$before" "$after")"
	emit failed "$failed"

	if [ "$failed" -ne 0 ]; then
		node_exec 1 docker logs subject >"$RUN/collector.log" 2>&1 || true
	fi
	finish
}
