# A fictive node: a container with a Docker daemon and a containerd of its
# own, the agent, and the image of the log generator. The containers it runs
# are its own, so every node has its own runtime, cgroups and network, and the
# agent in it sees what an agent on a machine would. Needs --privileged, like
# any Docker in Docker.
#
# experiments/lib.sh builds it from a directory that holds the agent's binary
# (static), the generator's image as a tar file, and entrypoint.sh.
ARG DIND=docker:29-dind
FROM ${DIND}

# No TLS for the inner daemon: nothing outside the container talks to it.
ENV DOCKER_TLS_CERTDIR=

COPY cosmicdust-agent /usr/local/bin/cosmicdust-agent
COPY loggen.tar /loggen.tar
COPY entrypoint.sh /usr/local/bin/node-entrypoint.sh

# The agent takes its settings from the environment (COSMICDUST_<FLAG>). The
# inner dockerd runs containerd as its child, on a socket of its own.
ENV COSMICDUST_DATA_DIR=/var/lib/cosmicdust \
    COSMICDUST_LISTEN=:9101 \
    COSMICDUST_APP_LABELS=app \
    COSMICDUST_CONTAINERD_ADDRESS=/run/docker/containerd/containerd.sock

ENTRYPOINT ["node-entrypoint.sh"]
