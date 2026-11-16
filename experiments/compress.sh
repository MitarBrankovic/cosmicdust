# The experiment "compress": what the agent's store makes of real logs,
# against compressing the log files the runtime wrote. Sourced by run.sh
# after lib.sh and collectors.sh.
#
# A run takes one log and one length of file. On a roomy node a few
# generators write the log's lines as they are, without the numbering the
# other experiments need. The agent sends nothing; it turns what it has read
# into Parquet every so many seconds, so that each of its files holds that
# many seconds of logs. The log files the node's Docker wrote meanwhile are
# cut into pieces of the same length, and each piece is compressed on its own
# with gzip and with zstd (compress.py).
#
# The axis is the log. What a run was like is under a series named after the
# length of its files ("60s"); the ratios - bytes of the log files over bytes
# kept, as ratio_<seconds>s - are under a series per way of keeping them:
#
#   agent            the agent's Parquet files
#   gzip-6           the log files, with gzip -6
#   zstd-3, zstd-19  the same, with zstd
#   messages-zstd-3  the text of the lines alone, without the JSON the runtime
#                    wraps each of them in, with zstd -3
#
# The bytes do not depend on how busy the machine is: this is the one
# experiment that does not need it quiet.

: "${DATASETS:=}"              # the logs, as "<name>=<file> ..."; without it, REPLAY
: "${FILE_SECONDS:=10 60 300}" # seconds of logs in one of the agent's files
: "${COMPRESS_CONTAINERS:=4}"  # generators
: "${COMPRESS_RATE:=500}"      # lines per second of each
: "${REPLAY_LINES:=300000}"    # lines of a log a generator writes before it starts over

# compress_logs lists the logs: "<name> <file>".
compress_logs() {
	local d
	if [ -n "$DATASETS" ]; then
		for d in $DATASETS; do
			case $d in
			?*=?*) echo "${d%%=*} $(readlink -f "${d#*=}")" ;;
			esac
		done
	elif [ -n "$REPLAY" ]; then
		d=${REPLAY##*/}
		echo "${d%.*} $(readlink -f "$REPLAY")"
	fi
}

compress_need() {
	local d tool
	for d in $DATASETS; do
		case $d in
		?*=?*) [ -r "${d#*=}" ] || die "cannot read the log of DATASETS '$d'" ;;
		*) die "DATASETS takes <name>=<file>, not '$d'" ;;
		esac
	done
	[ -n "$DATASETS$REPLAY" ] || {
		log "compress: left out, it takes real logs (DATASETS, or REPLAY)"
		return 0
	}
	for tool in gzip zstd; do
		command -v "$tool" >/dev/null || die "$tool is not installed (compress measures against it)"
	done
}

compress_units() { # "<log> <seconds>s <file>"
	local name file s
	while read -r name file; do
		for s in $FILE_SECONDS; do
			echo "$name ${s}s $file"
		done
	done < <(compress_logs)
}

# compress_run is one run: a log, a length of file.
compress_run() { # <log> <seconds>s <round> <file>
	local seconds=${2%s} load count lines rows=0 files size bytes codec failed=0 i before after
	# The generators of this run: the lines of this log, as they are.
	local CONTAINERS=$COMPRESS_CONTAINERS RATE=$COMPRESS_RATE REPLAY=$4 GENERATOR_ARGS="-raw -replay-max $REPLAY_LINES"

	# Long enough for a few files; for one, where a file is that long.
	load=$((seconds * 3))
	[ "$load" -ge 60 ] || load=60
	[ "$load" -le 300 ] || load=$((seconds > 300 ? seconds : 300))
	[ -z "${QUICK:-}" ] || load=$((seconds * 2))
	count=$((RATE * load))

	begin compress "$1" "$2" "$3"
	log "compress: $1, files of $seconds s, round $3: $CONTAINERS x $RATE lines/s for $load s"

	# The agent keeps what it reads and sends nothing. It seals what it has
	# read and converts it every $seconds seconds, whatever the size, and
	# leaves the files as they are.
	node_run 1 --cpus "$COLLECTOR_NODE_CPUS" -m "$COLLECTOR_NODE_MEM" -e COSMICDUST_SYNC_URL= \
		-e "COSMICDUST_WAL_SEGMENT_AGE=${seconds}s" -e COSMICDUST_WAL_SEGMENT_BYTES=1GiB \
		-e "COSMICDUST_CONVERT_EVERY=${seconds}s" -e COSMICDUST_CONVERT_BYTES=1GiB \
		-e COSMICDUST_COMPACT_EVERY=24h
	node_wait 1

	before=$(machine)
	generators_start 1 "$count"
	generators_wait 1
	lines=$(node_exec 1 sh -c 'cat /var/lib/docker/containers/*/*-json.log | wc -l')

	# Until every line is in a Parquet file: what the agent read last is
	# sealed within one of its periods and converted within the next.
	for i in $(seq 1 $((2 * seconds + 30))); do
		rows=$(agents_sum 1 'cosmicdust_agent_store_rows_total{table="logs"}')
		[ "$rows" -ge "$lines" ] && break
		sleep 1
	done
	after=$(machine)
	if [ "$rows" -lt "$lines" ]; then
		log "only $rows of $lines lines are in Parquet files; the ratio is over those"
		failed=1
	fi

	# The agent's files of log lines, as they are on its disk.
	read -r files size < <(node_exec 1 sh -c '
		find /var/lib/cosmicdust -path "*/logs/*" -name "*.parquet" -exec stat -c %s {} \; |
			awk "{ n++; s += \$1 } END { print n + 0, s + 0 }"')
	emit files "${files:-0}"
	emit file_mb "$(awk -v n="${files:-0}" -v s="${size:-0}" 'BEGIN { printf "%.3f", (n > 0 ? s / n / 1e6 : 0) }')"
	emit lines "$lines"
	emit stored_pct "$(awk -v r="$rows" -v l="$lines" 'BEGIN { printf "%.2f", (l > 0 ? 100 * r / l : 0) }')"

	# What gzip and zstd make of the log files, cut into pieces as long as
	# the agent's files.
	node_exec 1 sh -c 'cat /var/lib/docker/containers/*/*-json.log' >"$RUN/json-file.log"
	"$PYTHON" "$EXP/compress.py" "$RUN/json-file.log" "$seconds" >"$RUN/kept.txt" || failed=1
	rm -f "$RUN/json-file.log"
	bytes=$(awk '$1 == "log" { print $2 }' "$RUN/kept.txt")
	emit log_mb "$(awk -v b="${bytes:-0}" 'BEGIN { printf "%.1f", b / 1e6 }')"
	emit message_pct "$(awk '$1 == "log" { l = $2 } $1 == "messages" { m = $2 } END { printf "%.1f", (l > 0 ? 100 * m / l : 0) }' "$RUN/kept.txt")"

	# Bytes of the log files over bytes kept. The agent's files hold the
	# lines it had converted: all of them, unless the run was cut short.
	if [ "${size:-0}" -gt 0 ] && [ "${bytes:-0}" -gt 0 ]; then
		emit_as agent "ratio_${seconds}s" "$(awk -v b="$bytes" -v r="$rows" -v l="$lines" -v s="$size" \
			'BEGIN { printf "%.2f", b * (r < l ? r / l : 1) / s }')"
	else
		failed=1
	fi
	for codec in gzip-6 zstd-3 zstd-19 messages-zstd-3; do
		emit_as "$codec" "ratio_${seconds}s" "$(awk -v k="$codec" -v b="${bytes:-0}" \
			'$1 == k && $2 > 0 { printf "%.2f", b / $2 }' "$RUN/kept.txt")"
	done
	emit machine_busy_cores "$(busy_cores "$before" "$after")"
	emit failed "$failed"

	if [ "$failed" -ne 0 ]; then
		node_exec 1 cat /var/log/agent.log >"$RUN/agent.log" 2>/dev/null || true
	fi
	finish
}
