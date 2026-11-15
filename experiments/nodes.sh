# The experiments "nodes" and "adverse": the whole system with several nodes.
# Sourced by run.sh after lib.sh.
#
# A run starts the server and k nodes, lets every node's generators write
# CONTAINERS x RATE lines a second for DURATION seconds, waits until every
# line is searchable, reads all of them back through the query API, and
# records what the agents and the server cost meanwhile. The axis is the
# number of nodes; a series is a scenario, which is what happens while the
# generators write.
#
# nodes - the system as it is meant to run:
#
#   steady   nothing: the cost of the system under a steady load
#   outage   the server is away for most of the load, and containers fail
#            meanwhile; it returns once the load is over
#   churn    every few seconds the agent of one node is killed (kill -9) and
#            started again, node after node
#
# adverse - what a node at the edge has to live with:
#
#   narrow   outage, with every node behind a narrow link that loses packets
#            (LINK_MBIT, LINK_RTT, LINK_LOSS): what waited crosses it
#   burst    for the middle third of the load, BURST_X times as many lines
#   evict    the server is away for the whole load and the agent's store may
#            hold STORE_QUOTA: the agent drops its oldest logs for room and
#            records each drop; every line must be either there or recorded
#   block    the same, with the agent told to stop reading instead: once the
#            server is back, every line must be there

: "${SCENARIOS:=steady,outage,churn}"
: "${ADVERSE:=narrow,burst,evict,block}"
: "${NODES:=1,3,5,10}"

# units_of lists the runs of a round over some scenarios: "<nodes> <scenario>".
units_of() { # <scenarios>
	local s k
	for s in ${1//,/ }; do
		declare -F "during_$s" >/dev/null || die "there is no scenario '$s'"
		for k in ${NODES//,/ }; do
			echo "$k $s"
		done
	done
}

nodes_units() { units_of "$SCENARIOS"; }
adverse_units() { units_of "$ADVERSE"; }

# A scenario is up to four functions, of which only the third is a must:
#
#   setup_<scenario>    before the nodes start; may set NODE_ARGS
#   before_<scenario>   the nodes are up, no line is written yet
#   during_<scenario>   while the generators write; returns when the
#                       scenario's part is over
#   after_<scenario>    every line is searchable
#
# They may set EXTRA (lines written by anything but the generators that
# nodes_run starts), ALSO and ALSO_LINES, MAY_DROP and MAY_MISS (lib.sh).

during_steady() {
	sleep "$DURATION"
}

during_outage() {
	local crashes=${CRASHES:-5} away i j gap pids
	sleep $((DURATION / 4))
	server_stop
	away=$(date +%s.%N)
	# Back five seconds after the last line is written, so that what follows
	# is the nodes catching up and nothing else.
	gap=$(((DURATION - DURATION / 4 + 5) / crashes))
	for j in $(seq 1 "$crashes"); do
		pids=()
		for i in $(seq 1 "$K"); do
			crash "$i" "x$j" &
			pids+=($!)
		done
		wait "${pids[@]}"
		sleep "$gap"
	done
	server_start "$RUN" "$PREFIX"
	emit outage_s "$(since "$away")"
	EXTRA=$((K * crashes))
	emit urgent_expected "$EXTRA"
}

# The events of the outage are due on the urgent channel soon after the
# server's return; the server that returned counts only those.
after_outage() {
	local i got=0
	for i in $(seq 1 30); do
		got=$(server_metric cosmicdust_server_urgent_events_total)
		[ "${got:-0}" -ge "$EXTRA" ] && break
		sleep 1
	done
	emit urgent_delivered "${got:-0}"
}

during_churn() {
	local every=${KILL_EVERY:-10} t=0 kills=0
	while [ $((t + every)) -le "$DURATION" ]; do
		sleep "$every"
		t=$((t + every))
		node_exec $((kills % K + 1)) pkill -9 -f cosmicdust-agent || true
		kills=$((kills + 1))
	done
	sleep $((DURATION - t))
	emit agent_kills "$kills"
}

before_narrow() {
	local i
	for i in $(seq 1 "$K"); do
		link_shape "$i"
	done
}

during_narrow() {
	during_outage
}

after_narrow() {
	local mb lost
	after_outage
	read -r mb lost < <(link_stats "$K")
	emit link_mb "$mb"
	emit link_lost_pct "$lost"
}

# The burst comes from generators of its own, CONTAINERS more on every node,
# which together with the first ones write BURST_X times the load.
during_burst() {
	local third=$((DURATION / 3)) rate=$((RATE * (BURST_X - 1))) count
	count=$((rate * third))
	sleep "$third"
	generators_start "$K" "$count" b "$rate"
	sleep $((DURATION - third))
	# A node that could not keep up with the burst is still writing.
	generators_wait "$K"
	EXTRA=$((K * CONTAINERS * count))
	ALSO="-also /b=${CONTAINERS}x$count" ALSO_LINES=$((CONTAINERS * count))
	emit burst_lines_per_s $((K * CONTAINERS * RATE * BURST_X))
}

# A full store: the server is away from before the first line until five
# seconds after the last, and the store has room for a part of the load only.
setup_evict() {
	NODE_ARGS=(-e "COSMICDUST_STORE_QUOTA=$STORE_QUOTA" -e COSMICDUST_WHEN_FULL=evict)
	MAY_DROP=$K
}

setup_block() {
	NODE_ARGS=(-e "COSMICDUST_STORE_QUOTA=$STORE_QUOTA" -e COSMICDUST_WHEN_FULL=block)
}

before_evict() {
	server_stop
	AWAY=$(date +%s.%N)
}

before_block() {
	before_evict
}

during_evict() {
	sleep $((DURATION + 5))
	server_start "$RUN" "$PREFIX"
	emit outage_s "$(since "$AWAY")"
}

during_block() {
	during_evict
}

# What the agents say about their stores: how often reading had to wait for
# room, and how many lines were dropped for it. Those lines are rightly
# missing; any other is not.
after_evict() {
	local dropped
	dropped=$(agents_sum "$K" cosmicdust_agent_store_dropped_rows_total)
	emit store_full_times "$(agents_sum "$K" cosmicdust_agent_store_full_total)"
	emit lines_dropped "$dropped"
	emit dropped_pct "$(awk -v d="$dropped" -v n="$((K * CONTAINERS * RATE * DURATION))" 'BEGIN { printf "%.1f", 100 * d / n }')"
	MAY_MISS=$dropped
}

after_block() {
	after_evict
}

# nodes_run is one run of a scenario with k nodes.
nodes_run() { # <k> <scenario> <round> [experiment]
	K=$1
	local scenario=$2 count=$((RATE * DURATION)) nodes=() i from to before after settle failed=0
	local expected=$((K * CONTAINERS * count))
	EXTRA=0 NODE_ARGS=() ALSO='' ALSO_LINES=0 MAY_DROP='' MAY_MISS=0
	begin "${4:-nodes}" "$K" "$scenario" "$3"
	log "$EXPERIMENT: $scenario, $K node(s), round $3: $((K * CONTAINERS * RATE)) lines/s for $DURATION s"

	if declare -F "setup_$scenario" >/dev/null; then
		"setup_$scenario"
	fi
	server_start "$RUN" "$PREFIX"
	nodes_start "$K"
	if declare -F "before_$scenario" >/dev/null; then
		"before_$scenario"
	fi
	sample_start "$K" "$RUN/samples.csv"
	from=$(date +%s.%N) before=$(machine)
	generators_start "$K" "$count"
	"during_$scenario"
	to=$(date +%s.%N) after=$(machine)

	# From the end of the scenario's part until every line can be searched.
	if settle=$(wait_indexed "$PREFIX" $((expected + EXTRA))); then
		emit settle_s "$settle"
	else
		failed=1
	fi
	if declare -F "after_$scenario" >/dev/null; then
		"after_$scenario"
	fi
	sample_stop

	emit lines_per_s $((K * CONTAINERS * RATE))
	emit scenario_s "$(awk -v a="$from" -v b="$to" 'BEGIN { printf "%.1f", b - a }')"
	for i in $(seq 1 "$K"); do
		nodes+=("node-$i")
	done
	verified "$CONTAINERS" "$count" "${nodes[@]}" || failed=1

	# What the agents and the server cost while the generators wrote: the
	# agents as the mean and the worst of the nodes.
	usage "$RUN/samples.csv" "$from" "$to" >"$RUN/usage.txt"
	awk '$1 ~ /^node-/ { n++; c += $2; r += $3; if ($2 > cmax) cmax = $2; if ($3 > rmax) rmax = $3 }
		END { if (n) printf "%.3f %.3f %.1f %.1f\n", c / n, cmax, r / n, rmax }' "$RUN/usage.txt" >"$RUN/agents.txt"
	if read -r cpu cpumax rss rssmax <"$RUN/agents.txt"; then
		emit agent_cpu_pct "$cpu"
		emit agent_cpu_pct_max "$cpumax"
		emit agent_rss_mb "$rss"
		emit agent_rss_mb_max "$rssmax"
	fi
	if read -r _ cpu rss < <(grep '^server ' "$RUN/usage.txt"); then
		emit server_cpu_pct "$cpu"
		emit server_rss_mb "$rss"
	fi
	emit files_received "$(server_metric cosmicdust_server_files_received_total)"
	emit mb_received "$(server_metric cosmicdust_server_bytes_received_total | awk '{ printf "%.2f", $1 / 1e6 }')"
	emit machine_busy_cores "$(busy_cores "$before" "$after")"
	emit failed "$failed"

	if [ "$failed" -ne 0 ]; then
		for i in $(seq 1 "$K"); do
			node_exec "$i" cat /var/log/agent.log >"$RUN/agent-$i.log" 2>/dev/null || true
		done
	fi
	finish
}

adverse_run() { # <k> <scenario> <round>
	nodes_run "$1" "$2" "$3" adverse
}
