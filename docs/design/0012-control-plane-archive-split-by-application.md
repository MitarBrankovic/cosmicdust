# 0012 — Control plane: the archive is split by application

**Status:** accepted

Retention is per application, and so is most of what people look for. A node partitions its files by time only - one
file per hour and table, with the rows of every application on it - because a file per application every few seconds
would cost the node small files, more fsyncs and worse compression. The split is done on the control plane instead,
where the CPU is: every received file is read once and written again as one Parquet file per application,

    <table>/app=<app>/dt=<day>/hour=<hour>/<node>-<name>

with the node's schema unchanged. The application is the node's `app` column (`cosmicdust.AppOf`: a container label, else the
Compose service, else the image), escaped for a directory name; rows without one go to `app=_unknown`. Deleting an
application's expired hours is then removing directories, and a query for one application reads only its files;
DuckDB, Spark and pandas read the partitions directly.

The archive is compressed with zstd at its default level, where the node uses the fastest: it is kept for weeks, and
the control plane has the CPU to spare. Each archive file is written next to its final name, fsynced and renamed, so a
crash leaves either the whole file or none.

Nodes send every few seconds, so an application's hour arrives as many small files, each with a footer and dictionaries
of its own (in a test the archive was 30 % larger than the node's store). Once an hour has been over for
`COMPACT_AFTER` (2 hours) and every file of it is indexed, the archiver merges its files into one; it does so between
received files, so nothing else writes the archive meanwhile, and the indexer only ever reads files not yet indexed.
The merged file names its inputs in its metadata and is renamed into place before they are removed, so a crash is
finished at the next pass, and files that arrive late for an hour are merged with it then.

A received file that is not a Parquet file of its table cannot be archived however often it is tried, and would hold
up every file behind it: it is moved to `quarantine/` and recorded as archived with nothing in it. parquet-go panics on
such a file rather than returning an error, so files are opened with `parquet.OpenFile` first and reading is guarded.
