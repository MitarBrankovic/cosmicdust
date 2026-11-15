#!/bin/sh
# The entrypoint of a fictive node: starts the node's own dockerd, loads the
# generator's image (and whatever images were mounted under /images) into it,
# and keeps the agent running the way a supervisor would - an agent that is
# killed is started again a second later.
#
# With NODE_AGENT=off the node runs no agent of its own: whoever started it
# puts a log collector in, as a container.
set -eu

dockerd-entrypoint.sh dockerd >/var/log/dockerd.log 2>&1 &
until docker info >/dev/null 2>&1; do sleep 0.2; done
for tar in /loggen.tar /images/*.tar; do
	if [ -e "$tar" ]; then
		docker load -q -i "$tar" >/dev/null
	fi
done
touch /run/node-ready

if [ "${NODE_AGENT:-on}" = off ]; then
	while :; do sleep 3600; done
fi
while :; do
	cosmicdust-agent >>/var/log/agent.log 2>&1 || true
	sleep 1
done
