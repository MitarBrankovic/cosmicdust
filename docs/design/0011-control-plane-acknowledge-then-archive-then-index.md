# 0011 — Control plane: acknowledge once received, then archive, then index

**Status:** accepted

`cosmicdust-server` is the control plane's side of the sync (record 0010): it receives the nodes' files, keeps them in an archive
and indexes them in Elasticsearch. A node deletes a file once the service acknowledges it, so the acknowledgement must
mean the file is safe. It is given once the file is complete, checked against the size and CRC-32C the node declared,
fsynced and renamed into the inbox, and recorded in the ledger (an append-only file of JSON lines, fsynced). It is not
given after the archive or the index: Elasticsearch being down, slow or rejecting would otherwise hold the files on the
edge nodes, whose disks are the smallest in the system.

The archive and the index follow at their own pace. The archiver splits each received file by application into the
archive (record 0012), records that, and removes the inbox copy; the indexer reads archive files into Elasticsearch, bulk,
and records them as indexed. While Elasticsearch is away the indexer tries again with a pause that doubles up to a
minute; the archive keeps growing and nothing is lost.

Every step can be done twice. The ledger knows each received file by node, table, partition and name with its size and
CRC, also after the inbox copy is gone, for `DEDUP_WINDOW` (7 days): a node sends a file again only when an
acknowledgement was lost, within hours. A received file archived again writes the same archive files. A document's id
is a hash of what identifies its row - for a log line the node, the container and where the line starts in its log
file (inode, fingerprint of the file's first line, offset); for an event the runtime's report of it - and documents
are *created*, so Elasticsearch keeps one copy and answers 409 to the second. The one gap is a data stream's rollover
between two attempts to index the same file, as the id is unique within a backing index; files are recorded as
indexed as soon as their bulk succeeds, so only a crash in that window repeats them.

`internal/servertest` stays the light receiver for the node's tests and measurements: no archive, no index, no ledger.
