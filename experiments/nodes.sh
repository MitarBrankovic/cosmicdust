# The experiment "nodes": the whole system with several nodes. Sourced by
# run.sh after lib.sh.
#
# A run starts the server and k nodes, lets every node's generators write
# CONTAINERS x RATE lines a second for DURATION seconds, waits until every
# line is searchable, reads all of them back through the query API, and
# records what the agents and the server cost meanwhile. The axis is the
# number of nodes; a series is a scenario, which is what happens while the
# generators write:
#
#   steady   nothing: the cost of the system under a steady load
#   outage   the server is away for most of the load, and containers fail
#            meanwhile; it returns once the load is over
#   churn    every few seconds the agent of one node is killed (kill -9) and
#            started again, node after node

: "${SCENARIOS:=steady,outage,churn}"
: "${NODES:=1,3,5,10}"

# nodes_units lists the runs of a round: "<nodes> <scenario>".
nodes_units() {
	local s k
	for s in ${SCENARIOS//,/ }; do
		declare -F "during_$s" >/dev/null || die "there is no scenario '$s'"
		for k in ${NODES//,/ }; do
			echo "$k $s"
		done
	done
}

# during_<scenario> runs while the generators write and returns when the
# scenario's part is over. It may add to EXTRA the lines written by anything
# but the generators.

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

# nodes_run is one run of a scenario with k nodes.
nodes_run() { # <k> <scenario> <round>
	K=$1
	local scenario=$2 count=$((RATE * DURATION)) nodes=() i from to before after settle failed=0
	local expected=$((K * CONTAINERS * count))
	EXTRA=0
	begin nodes "$K" "$scenario" "$3"
	log "nodes: $scenario, $K node(s), round $3: $((K * CONTAINERS * RATE)) lines/s for $DURATION s"

	server_start "$RUN" "$PREFIX"
	nodes_start "$K"
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
