// Package api is the server's protocols, generated from proto/.
//
// sync.proto is the wire protocol between a node and the control plane: its
// messages and the gRPC service Sync. Over NATS the same messages travel on
// <prefix>.files.<node> (chunks and probes, each answered on its reply
// subject) and <prefix>.urgent.<node> (events).
//
// query.proto is the gRPC service Query, through which other services read
// what the nodes sent: nodes, containers, log lines and lifecycle events.
package api

//go:generate protoc -Iproto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative sync.proto query.proto
