# 0001 — Public types in one package, parts wired by the caller

**Status:** accepted

The node side is a set of parts - runtime adapters, the lifecycle tracker, the tailer, the write-ahead log, the
store, the sync engine - that run in one process on a node with little memory to spare, either as the standalone
agent or inside a program that already runs there.

Types and interfaces are in `pkg/cosmicdust`, implementations in `internal/…`. `pkg/cosmicdust` imports no implementation: the
caller builds the parts and hands them to `cosmicdust.New`. That keeps the import graph acyclic (the adapters use the
public types), lets the agent be embedded in another daemon that already has its own HTTP server or registry, and
makes every experiment a different set of parts rather than a different code path.

Parts run as `Component`s (`Name`, `Run(ctx)`). The agent stops them in the order they were given, each after the
previous one has returned, under one shutdown deadline: sources stop first, the parts that drain them after, and
the HTTP endpoint last so the final metrics can still be scraped. A component that returns on its own stops the
agent; a failing part is not left running half-wired.
