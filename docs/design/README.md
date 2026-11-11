# Design records

One short record per decision: what was decided, why, what was measured, and what follows from it.

The node:

| | |
|---|---|
| [0001](0001-wiring-by-the-caller.md) | Public types in one package, parts wired by the caller |
| [0002](0002-configuration.md) | Configuration through flags and the environment |
| [0003](0003-node-identity.md) | The node makes and keeps its own identity |
| [0004](0004-container-state.md) | Container state: one view per runtime, merged, reconciled |
| [0005](0005-tailer.md) | The tailer: one scheduler, files only, a batch per pass |
| [0006](0006-wal.md) | The WAL: positions in the block, one fsync per pass |
| [0007](0007-parquet-store.md) | The Parquet store: hourly files, one commit point, room kept |
| [0008](0008-public-node-package.md) | A public package assembles the node |
| [0009](0009-streaming-conversion.md) | Conversion streams; memory follows a chunk, not the rate |

Between the node and the control plane:

| | |
|---|---|
| [0010](0010-sync.md) | Sync: whole files, probed and continued, at least once into an idempotent receiver |
