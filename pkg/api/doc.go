// Package api is the wire protocol between a node and the control plane:
// the messages of sync.proto and the gRPC service, generated. Over NATS the
// same messages travel on <prefix>.files.<node> (chunks and probes, each
// answered on its reply subject) and <prefix>.urgent.<node> (events).
package api

//go:generate protoc -Iproto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative sync.proto
