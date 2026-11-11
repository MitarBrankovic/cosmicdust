# The node daemon. Built from the module's root:
#   docker build -f deploy/agent/Dockerfile --build-arg VERSION=$(git describe --always) -t cosmicdust-agent .
FROM golang:1.27-alpine3.24 AS builder

WORKDIR /src

# Dependencies first, so that they are cached while only the code changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /cosmicdust-agent ./cmd/agent

FROM alpine:3.24

COPY --from=builder /cosmicdust-agent /usr/local/bin/cosmicdust-agent

# It runs as root: Docker's log files and containerd's socket are root's.
VOLUME /var/lib/cosmicdust
EXPOSE 9101
ENTRYPOINT ["cosmicdust-agent"]
