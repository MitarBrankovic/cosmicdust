# The control-plane service. Built from the module's root:
#   docker build -f deploy/server/Dockerfile -t cosmicdust-server .
FROM golang:1.27-alpine3.24 AS builder

WORKDIR /src

# Dependencies first, so that they are cached while only the code changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /cosmicdust-server ./cmd/server

FROM alpine:3.24

RUN adduser -D -u 10001 cosmicdust-server && mkdir /data && chown cosmicdust-server /data
USER cosmicdust-server
COPY --from=builder /cosmicdust-server /usr/local/bin/cosmicdust-server

ENV DATA_DIR=/data GRPC_ADDRESS=:50051 HTTP_ADDRESS=:8080
VOLUME /data
EXPOSE 50051 8080
ENTRYPOINT ["cosmicdust-server"]
