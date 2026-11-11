# 0008 — A public package assembles the node

**Status:** accepted

0001 left the wiring to the caller, so that the agent could be embedded in a daemon that already runs on every
node. Outside this module that was not possible: the parts are in `internal/…`, which only code in the module can
import, so `cmd/agent` was the one program able to wire them.

`pkg/node` does the wiring once, for everyone: `node.New(Options)` builds the lifecycle tracker with its runtime
sources, the tailer, the WAL with its event writer and the store, in data-flow order, and returns a `Node` with
`Run`, `Health`, `ID`, `Store` (what a sync engine needs) and `Close`. A caller's own parts, such as an HTTP endpoint,
join as `Options.Components` and stop after the node's. A runtime cosmicdust has no adapter for joins as a `Source`, any
`cosmicdust.EventSource`, which is also how the whole node side is tested without Docker.

Options are plain structs whose zero values take the defaults, as elsewhere in the library; where zero is a real
setting, a negative value turns the feature off (no disk reserve, no quota, no periodic reconciliation). `cosmicdust-agent` is
now its flags turned into `node.Options`, plus what belongs to a process rather than to a library: the memory limit,
the metrics registry with the Go and process collectors, the HTTP endpoint, signal handling.

The public surface grows by the option structs. Their fields are the settings `cosmicdust-agent` already had, so nothing new is
promised; experiments that need another knob add it to both.
