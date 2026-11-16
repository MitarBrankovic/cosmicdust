# The experiment "latency": how late a line is at the server, by how fresh the
# agent is set to be, and what the freshness costs. Sourced by run.sh after
# lib.sh.
#
# A line waits three times on its node: in the open segment of the agent's
# write-ahead log until that is sealed, in the sealed segment until the next
# conversion to Parquet, and in the Parquet file until the next sync. A
# setting is those three periods, in seconds:
#
#   s1         1 /  2 /  2
#   s2         2 /  5 /  5
#   default    5 / 10 / 10    the agent as it comes
#   s30       10 / 30 / 30
#   s60       30 / 60 / 60
#
# and any other can be written "<segment>-<conversion>-<sync>".
#
# A run is one node under a steady load, during which a container fails every
# LATENCY_CRASH_EVERY seconds: the agent reports that at once, as an urgent
# event, on a way of its own. The axis is the setting; the series are what
# was timed (cmd/latency), and the agent:
#
#   server   a line, from the time the node's runtime took it to the moment
#            the server had the file that holds it
#   search   a line, from the same time to the moment the server had it in
#            the search index
#   urgent   an urgent event, from the end of the container's process to its
#            arrival at the server
#   agent    what the agent cost meanwhile and what it sent; whether every
#            line came back once

: "${SETTINGS:=s1 s2 default s30 s60}"
: "${LATENCY_CONTAINERS:=10}"  # generators
: "${LATENCY_RATE:=100}"       # lines per second of each
: "${LATENCY_SECONDS:=180}"    # seconds of load
: "${LATENCY_CRASH_EVERY:=15}" # seconds between two containers that fail

# setting_of sets what a setting means: the seconds a segment of the
# write-ahead log stays open, between two conversions and between two syncs.
setting_of() { # <setting>
	case $1 in
	s1) SEGMENT=1 CONVERT=2 SYNC=2 ;;
	s2) SEGMENT=2 CONVERT=5 SYNC=5 ;;
	default) SEGMENT=5 CONVERT=10 SYNC=10 ;;
	s30) SEGMENT=10 CONVERT=30 SYNC=30 ;;
	s60) SEGMENT=30 CONVERT=60 SYNC=60 ;;
	*[0-9]-*[0-9]-*[0-9]) IFS=- read -r SEGMENT CONVERT SYNC <<<"$1" ;;
	*) die "there is no setting '$1'" ;;
	esac
}

latency_need() {
	local s
	for s in $SETTINGS; do
		setting_of "$s"
	done
}

latency_units() { # "<setting> agent"
	local s
	for s in $SETTINGS; do
		echo "$s agent"
	done
}

# latency_run is one run: a setting.
latency_run() { # <setting> agent <round>
	local count crashes=0 t from to before after settle got=0 failed=0 cpu rss i series metric value
	local CONTAINERS=$LATENCY_CONTAINERS RATE=$LATENCY_RATE
	setting_of "$1"
	count=$((RATE * LATENCY_SECONDS))
	ALSO='' ALSO_LINES=0 MAY_DROP='' MAY_MISS=0
	NODE_ARGS=(-e "COSMICDUST_WAL_SEGMENT_AGE=${SEGMENT}s" -e "COSMICDUST_CONVERT_EVERY=${CONVERT}s"
		-e "COSMICDUST_SYNC_EVERY=${SYNC}s")
	begin latency "$1" agent "$3"
	log "latency: $1 ($SEGMENT / $CONVERT / $SYNC s), round $3: $((CONTAINERS * RATE)) lines/s for $LATENCY_SECONDS s"

	server_start "$RUN" "$PREFIX"
	nodes_start 1
	sample_start 1 "$RUN/samples.csv"
	from=$(date +%s.%N) before=$(machine)
	generators_start 1 "$count"
	# Meanwhile a container fails every so often.
	t=$LATENCY_CRASH_EVERY
	while [ "$t" -lt "$LATENCY_SECONDS" ]; do
		sleep "$LATENCY_CRASH_EVERY"
		crashes=$((crashes + 1))
		crash 1 "x$crashes"
		t=$((t + LATENCY_CRASH_EVERY))
	done
	generators_wait 1
	to=$(date +%s.%N) after=$(machine)

	# Every line searchable - the one line of each container that failed too -
	# and every failure told on the urgent way.
	if settle=$(wait_indexed "$PREFIX" $((CONTAINERS * count + crashes))); then
		emit settle_s "$settle"
	else
		failed=1
	fi
	for i in $(seq 1 30); do
		got=$(server_metric cosmicdust_server_urgent_events_total)
		[ "${got:-0}" -ge "$crashes" ] && break
		sleep 1
	done
	sample_stop
	emit urgent_expected "$crashes"
	emit urgent_delivered "${got:-0}"
	emit lines_per_s $((CONTAINERS * RATE))
	emit scenario_s "$(awk -v a="$from" -v b="$to" 'BEGIN { printf "%.1f", b - a }')"
	verified "$CONTAINERS" "$count" node-1 || failed=1

	# What the agent cost while the generators wrote, and what the server got.
	usage "$RUN/samples.csv" "$from" "$to" >"$RUN/usage.txt"
	if read -r _ cpu rss < <(grep '^node-1 ' "$RUN/usage.txt"); then
		emit agent_cpu_pct "$cpu"
		emit agent_rss_mb "$rss"
	fi
	if read -r _ cpu rss < <(grep '^server ' "$RUN/usage.txt"); then
		emit server_cpu_pct "$cpu"
	fi
	emit mb_received "$(server_metric cosmicdust_server_bytes_received_total | awk '{ printf "%.2f", $1 / 1e6 }')"

	# When the process of each container that failed ended, as its runtime
	# has it; and how late everything was, from the server's own record of
	# when each file arrived and when its rows were indexed. The server is
	# stopped first: its record is then complete.
	node_exec 1 sh -c 'docker inspect -f "{{.Name}} {{.State.FinishedAt}}" $(docker ps -aq --filter label=app=crash)' \
		>"$RUN/exits.txt" 2>/dev/null || true
	server_stop
	"$BIN/latency" -data "$RUN/server" -exits "$RUN/exits.txt" >"$RUN/latency.txt" 2>"$RUN/latency.err" || failed=1
	while read -r series metric value; do
		emit_as "$series" "$metric" "$value"
	done <"$RUN/latency.txt"
	emit files_per_min "$(awk -v a="$from" -v b="$to" '$1 == "agent" && $2 == "log_files" { printf "%.1f", 60 * $3 / (b - a) }' "$RUN/latency.txt")"
	emit machine_busy_cores "$(busy_cores "$before" "$after")"
	emit failed "$failed"

	if [ "$failed" -ne 0 ]; then
		log "latency: $1, round $3 is not right; see $RUN"
		node_exec 1 cat /var/log/agent.log >"$RUN/agent.log" 2>/dev/null || true
	fi
	finish
}
