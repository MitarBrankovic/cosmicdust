.PHONY: test lint integration integration-es build proto experiments

# Unit tests under the race detector. The NATS transport tests start a
# nats-server from PATH and are skipped without one:
#   go install github.com/nats-io/nats-server/v2@v2.15.0
test:
	go vet ./...
	go test -race ./...

lint:
	test -z "$$(gofmt -l . | tee /dev/stderr)"
	staticcheck ./...
	golangci-lint run ./...

# Lifecycle and tailer scenarios against the node's dockerd and containerd,
# about 1 min. containerd's socket and Docker's log files need root, so the
# test binaries run under sudo.
integration:
	for p in lifecycle tail; do \
		go test -tags integration -c -o /tmp/cosmicdust-$$p.test ./internal/$$p && \
		sudo /tmp/cosmicdust-$$p.test -test.run Integration -test.v || exit 1; \
	done

# The server against a real Elasticsearch: the indexer, and the query API over
# what it indexed. ES_ADDRESS has no default on purpose: the tests put templates
# and data streams, so they must never meet a cluster by accident.
integration-es:
	@test -n "$$ES_ADDRESS" || { echo "set ES_ADDRESS to an Elasticsearch for tests"; exit 2; }
	ES_REQUIRED=1 go test -tags integration -count=1 -run Integration ./internal/server/...

# bin/cosmicdust-{agent,server}-{amd64,arm64}
build:
	for arch in amd64 arm64; do \
		for cmd in agent server; do \
			CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -trimpath \
				-ldflags "-s -w -X main.version=$$(git describe --always --dirty)" \
				-o bin/cosmicdust-$$cmd-$$arch ./cmd/$$cmd || exit 1; \
		done; \
	done

# Regenerates pkg/api from pkg/api/proto. Needs protoc, protoc-gen-go and
# protoc-gen-go-grpc on PATH.
proto:
	go generate ./pkg/api/...

# Every scenario of experiments/ N times, then the statistics and the charts
# (experiments/README.md). Takes a machine that does nothing else, Docker that
# runs privileged containers, and a Python with matplotlib.
N ?= 3
experiments:
	experiments/run.sh -n $(N)
