#!/bin/sh
# The entrypoint of a fictive node: starts the node's own dockerd, loads the
# generator's image into it, and keeps the agent running the way a supervisor
# would - an agent that is killed is started again a second later.
set -eu

dockerd-entrypoint.sh dockerd >/var/log/dockerd.log 2>&1 &
until docker info >/dev/null 2>&1; do sleep 0.2; done
docker load -q -i /loggen.tar >/dev/null
touch /run/node-ready

while :; do
	cosmicdust-agent >>/var/log/agent.log 2>&1 || true
	sleep 1
done
