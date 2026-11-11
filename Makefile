.PHONY: test lint integration build proto

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

# bin/cosmicdust-agent-{amd64,arm64}
build:
	for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -trimpath \
			-ldflags "-s -w -X main.version=$$(git describe --always --dirty)" \
			-o bin/cosmicdust-agent-$$arch ./cmd/agent || exit 1; \
	done

# Regenerates pkg/api from pkg/api/proto. Needs protoc, protoc-gen-go and
# protoc-gen-go-grpc on PATH.
proto:
	go generate ./pkg/api/...
