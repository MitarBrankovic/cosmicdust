# The scenarios, and the run they share. Sourced by run.sh after lib.sh.
#
# A run starts the server and k nodes, lets every node's generators write
# CONTAINERS x RATE lines a second for DURATION seconds, waits until every
# line is searchable, reads all of them back through the query API, and
# records what the agents and the server cost meanwhile. A scenario is what
# happens while the generators write:
#
#   steady   nothing: the cost of the system under a steady load
#   outage   the server is away for most of the load, and containers fail
#            meanwhile; it returns once the load is over
#   churn    every few seconds the agent of one node is killed (kill -9) and
#            started again, node after node

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

# run_one is one run of a scenario with k nodes. Its values go to the run's
# rows.csv and, once the run is complete, to the session's runs.csv.
run_one() { # <scenario> <k> <round>
	SCENARIO=$1 K=$2 ROUND=$3
	RUN=$OUT/runs/$SCENARIO-k$K-r$ROUND
	PREFIX=exp-$SCENARIO-k$K-r$ROUND
	EXTRA=0
	local count=$((RATE * DURATION)) nodes="" i from to settle failed=0
	local expected=$((K * CONTAINERS * count))

	rm -rf "$RUN"
	mkdir -p "$RUN"
	teardown
	es_drop "$PREFIX"
	log "$SCENARIO, $K node(s), round $ROUND: $((K * CONTAINERS * RATE)) lines/s for $DURATION s"

	server_start "$RUN" "$PREFIX"
	nodes_start "$K"
	sample_start "$K" "$RUN/samples.csv"
	from=$(date +%s.%N)
	generators_start "$K" "$count"
	"during_$SCENARIO"
	to=$(date +%s.%N)

	# From the end of the scenario's part until every line can be searched.
	if settle=$(wait_indexed "$PREFIX" $((expected + EXTRA))); then
		emit settle_s "$settle"
	else
		failed=1
	fi
	if declare -F "after_$SCENARIO" >/dev/null; then
		"after_$SCENARIO"
	fi
	sample_stop

	# Every line of every generator, read back through the query API.
	for i in $(seq 1 "$K"); do
		nodes="$nodes node-$i"
	done
	# shellcheck disable=SC2086
	"$BIN/verify" -server "127.0.0.1:$GRPC_PORT" -generators "$CONTAINERS" -count "$count" $nodes \
		>"$RUN/verify.txt" 2>"$RUN/verify.err" || true
	awk -v k="$K" -v expected="$expected" '
		{ n++; for (i = 1; i <= NF; i++) { split($i, kv, "="); sum[kv[1]] += kv[2] } }
		END {
			if (n != k) { sum["lines"] = 0; sum["missing"] = expected }
			print sum["lines"] + 0, sum["missing"] + 0, sum["duplicate"] + 0
		}' "$RUN/verify.txt" >"$RUN/lines.txt"
	read -r stored missing duplicate <"$RUN/lines.txt"
	emit lines_per_s $((K * CONTAINERS * RATE))
	emit lines_expected "$expected"
	emit lines_stored "$stored"
	emit lines_missing "$missing"
	emit lines_duplicate "$duplicate"
	if [ "$missing" -eq 0 ] && [ "$duplicate" -eq 0 ]; then
		emit exactly_once 1
	else
		emit exactly_once 0
		failed=1
	fi

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
	emit failed "$failed"

	if [ "$failed" -ne 0 ]; then
		log "run failed: $stored of $expected lines, $missing missing, $duplicate twice; see $RUN"
		for i in $(seq 1 "$K"); do
			node_exec "$i" cat /var/log/agent.log >"$RUN/agent-$i.log" 2>/dev/null || true
		done
	fi
	teardown
	es_drop "$PREFIX"
	cat "$RUN/rows.csv" >>"$OUT/runs.csv"
	touch "$OUT/done/$SCENARIO-k$K-r$ROUND"
}
