// Package cosmicdust collects container logs and lifecycle events on an edge node,
// keeps them on local disk until a control plane acknowledges them, and ships
// them there at intervals, with as little load on the node as it can manage.
//
// The package holds the data model, the interfaces every part is written
// against, and the Agent that runs the parts together. Implementations live in
// internal packages; package node wires them into the node side, for
// cmd/agent and for any program that embeds it. Every experiment (a runtime, a
// transport, a codec) is a different implementation behind the same interface,
// not a different code path.
package cosmicdust
