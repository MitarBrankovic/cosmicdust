# The experiment "events": the lifecycle events of containers, against a
# script that knows what happened. Sourced by run.sh after lib.sh.
#
# Every run plays the same lifecycles on one node: a container that ends
# well and one that ends with an error, one stopped and one killed, one the
# kernel kills for memory after a moment and one at once, a restart policy
# that gives up after four incarnations, a restart, a pause, a container
# removed while it runs, and one that containerd runs without Docker. What
# must come of it - every exit of every container with its code and whether
# it was for memory, and the state each container is left in - is then
# checked against the events the server returns through the query API
# (cmd/events). The axis is what the agent does meanwhile:
#
#   live    it runs all along, and hears of everything from the runtimes
#   kill9   it is killed (kill -9) twice during the lifecycles and started
#           again: what it missed it has to conclude from a listing
#   away    it is stopped before the lifecycles and started after them: a
#           listing is all it has
#
# A listing shows what is, not what was: an exit that a later start of the
# same container hides, or one of a container that is gone, cannot be
# concluded from it.

: "${MODES:=live kill9 away}"
: "${EVENTS_IMAGE:=alpine:3}" # what the containers run: anything with a shell, sleep and dd

events_units() { # "<mode> agent"
	local m
	for m in $MODES; do
		echo "$m agent"
	done
}

events_need() {
	local m
	for m in $MODES; do
		case $m in
		live | kill9 | away) ;;
		*) die "there is no mode '$m'" ;;
		esac
	done
}

# events_image makes sure the image the containers run is there as a tar
# file, for the node to load.
events_image() {
	[ -s "$OUT/images/events/image.tar" ] && return 0
	mkdir -p "$OUT/images/events"
	docker image inspect "$EVENTS_IMAGE" >/dev/null 2>&1 || docker pull -q "$EVENTS_IMAGE" >/dev/null ||
		die "cannot pull $EVENTS_IMAGE"
	docker save "$EVENTS_IMAGE" -o "$OUT/images/events/image.tar"
}

# lifecycles plays the script on node 1 and prints what must come of it, a
# container per line: "<name> <exits> <state>", an exit as <code>:<1 if it
# was for memory, else 0>, the exits of a container's incarnations separated
# by semicolons, "-" for a container that never ended.
lifecycles() {
	docker exec -i "$(node_name 1)" sh -s "$EVENTS_IMAGE" <<'SH'
set -e
image=$1
run() { docker run -d "$@" >/dev/null; }

run --name ev-ok "$image" sh -c 'sleep 1; exit 0'
echo "ev-ok 0:0 exited"
run --name ev-fail "$image" sh -c 'sleep 1; exit 3'
echo "ev-fail 3:0 exited"
for c in ev-stop ev-kill ev-restart ev-pause ev-rm; do
	run --name "$c" "$image" sleep 300
done
# Memory: a file in memory larger than the container may have.
run --name ev-oom -m 64m --memory-swap 64m --shm-size 256m "$image" \
	sh -c 'sleep 1; dd if=/dev/zero of=/dev/shm/f bs=1M count=200'
echo "ev-oom 137:1 exited"
run --name ev-oom-fast -m 6m --memory-swap 6m "$image" dd if=/dev/zero of=/dev/shm/f bs=1M count=50
echo "ev-oom-fast 137:1 exited"
run --name ev-policy --restart on-failure:3 "$image" sh -c 'sleep 1; exit 1'
echo "ev-policy 1:0;1:0;1:0;1:0 exited"
# Without Docker: straight in containerd, in its default namespace. The
# image is imported for this machine's platform alone: the file may name
# others it does not hold.
ctr() { command ctr -a /run/docker/containerd/containerd.sock -n default "$@"; }
ctr images import --local --platform "$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')" \
	/images/image.tar >/dev/null
ctr run -d "$(ctr images ls -q | head -1)" ev-ctr sh -c 'sleep 1; exit 2' >/dev/null
echo "ev-ctr 2:0 exited"
sleep 2

docker stop -t 1 ev-stop >/dev/null # sleep as the first process ignores the request: killed after a second
echo "ev-stop 137:0 exited"
docker kill ev-kill >/dev/null
echo "ev-kill 137:0 exited"
docker restart -t 1 ev-restart >/dev/null
echo "ev-restart 137:0 running"
docker pause ev-pause >/dev/null
sleep 1
docker unpause ev-pause >/dev/null
echo "ev-pause - running"
docker rm -f ev-rm >/dev/null
echo "ev-rm 137:0 removed"

# The restart policy takes its time: three restarts, each after a longer pause.
i=0
while [ "$i" -lt 60 ] && [ "$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' ev-policy)" != "false 3" ]; do
	sleep 0.5
	i=$((i + 1))
done
sleep 2
SH
}

# events_settled waits until no more events arrive at the search index: none
# for longer than the agent takes to seal, convert and send what it has.
events_settled() {
	local have last=-1 same=0 i
	for i in $(seq 1 90); do
		have=$(es_count "$PREFIX" events)
		if [ "${have:-0}" -gt 0 ] && [ "${have:-0}" -eq "$last" ]; then
			same=$((same + 1))
		else
			same=0
		fi
		last=${have:-0}
		[ "$same" -lt 15 ] || return 0
		sleep 2
	done
	return 1
}

# events_run is one run: a mode.
events_run() { # <mode> agent <round>
	local mode=$1 failed=0 killer from before after i metric value
	begin events "$mode" agent "$3"
	log "events: $mode, round $3"
	events_image
	server_start "$RUN" "$PREFIX"
	node_run 1 --cpus "$NODE_CPUS" -m "$NODE_MEM" -v "$OUT/images/events:/images:ro"
	node_wait 1
	sleep 2

	from=$(date +%s.%N) before=$(machine)
	case $mode in
	live)
		lifecycles >"$RUN/truth.txt" || failed=1
		;;
	kill9)
		# Twice, a few seconds apart; the node starts it again a second later.
		(
			sleep $((2 + RANDOM % 4))
			node_exec 1 pkill -9 -f cosmicdust-agent
			sleep $((4 + RANDOM % 4))
			node_exec 1 pkill -9 -f cosmicdust-agent
		) &
		killer=$!
		lifecycles >"$RUN/truth.txt" || failed=1
		wait "$killer" || true
		;;
	away)
		# Stopped the way a service is, and kept from starting again.
		node_exec 1 touch /run/agent-hold
		node_exec 1 pkill -TERM -f cosmicdust-agent || true
		for i in $(seq 1 100); do
			node_exec 1 pgrep -f cosmicdust-agent >/dev/null || break
			sleep 0.2
		done
		lifecycles >"$RUN/truth.txt" || failed=1
		node_exec 1 rm /run/agent-hold
		;;
	esac
	emit scenario_s "$(since "$from")"
	after=$(machine)
	[ "$failed" -eq 0 ] || log "events: the lifecycles did not play to their end"

	events_settled || log "events: events were still arriving after three minutes"
	if "$BIN/events" -server "127.0.0.1:$GRPC_PORT" -truth "$RUN/truth.txt" -detail "$RUN/containers.txt" node-1 \
		>"$RUN/events.txt" 2>"$RUN/events.err"; then
		while read -r metric value; do
			emit "$metric" "$value"
		done <"$RUN/events.txt"
	else
		failed=1
	fi
	emit machine_busy_cores "$(busy_cores "$before" "$after")"
	emit failed "$failed"

	# What the agent itself said meanwhile: when it started, what it concluded.
	node_exec 1 cat /var/log/agent.log >"$RUN/agent.log" 2>/dev/null || true
	[ "$failed" -eq 0 ] || log "events: $mode, round $3 could not be checked; see $RUN"
	finish
}
