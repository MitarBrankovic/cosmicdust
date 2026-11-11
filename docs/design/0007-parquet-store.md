# 0007 — The Parquet store: hourly files, one commit point, room kept

**Status:** accepted

Sealed WAL segments are rewritten into Parquet, one directory per table and UTC hour of the line's own time
(`logs/dt=…/hour=…`, Hive style), with a manifest per partition listing its files and whether each has been synced.
On the node the files are a buffer and the unit of sending, not an archive. Every row carries the node, the
application, the container's name and image, and the state and incarnation the container had when the line was
written, decided at conversion when late events are in; stamped at storage time, 39 % of a backlog's lines were
marked exited although their containers ran. Columns that repeat are dictionary-encoded, the text and times are
zstd at its fastest level, pages are 1 MiB, rows are sorted by container and time.

A conversion touches files in several directories, so it has one commit point: the files are written first
(temporary name, fsync, rename), then `store.json` says which segments they replace, then the manifests are updated,
then the WAL releases the segments. A crash before the commit leaves orphans that the next start deletes; after it,
the next start finishes the job; names come from the segment range, so converting twice yields the same files. A
conversion takes at most 32 MiB of lines, so a backlog is converted in bounded pieces.

Once an hour is over, its unsynced files are merged into one by reading them side by side, a few hundred rows of
each at a time; synced files and files the sync engine has claimed are left alone. The manifest is compaction's
commit point.

The store keeps a reserve of free space plus room to convert the WAL that waits, and an optional quota on its size.
Short of either, synced files go first; then, by default, the oldest unsynced log files, never events, each recorded
as a `logs_dropped` event with its time span and row count, so the control plane knows exactly what is missing. The
alternative, `block`, holds the WAL: the tailer stops reading and the loss, if the outage lasts, happens at the
source without a trace.

The sync engine sees the store through `cosmicdust.Store`: unsynced files, a claim on one while it is sent, and the
control plane's answer, which marks it synced. Records and events reach the store only through the WAL, so
`Append` left the interface.
