# 0005 — The tailer: one scheduler, files only, a batch per pass

**Status:** accepted

Logs are read from the files the runtime writes (json-file, CRI), never through the Docker API or the runtime's
FIFO: a file is ~2.4x cheaper for the node, resumes to the byte, keeps long lines whole, and a slow reader cannot
block the container's writes. Other log drivers are counted and left unread. A position is an inode, a fingerprint of
the file's first line (the kernel reuses inodes) and an offset; rotation by rename is followed by reading the old
inode to its end before moving on. The files to read on from a position are opened from a listing of the directory
made in one `getdents64` call, which a rename cannot land in the middle of, and kept only if a second listing after
the opens has the same names with the same inodes. A rotation renames the files one at a time, and opened by the
names of a listing taken in the middle of one, a whole file was skipped (1-5 % of the plans under constant
rotation).

One goroutine serves every file. It keeps a queue of deadlines; a file with recent data is checked every 250 ms,
one quiet for 5 s backs off by doubling up to 5 s, and data brings it back at once. Reads never wait. A
goroutine and a timer per file kept a first version awake 89 times a second with no container at all, and polling
every quiet file every 250 ms cost 12.6 % of a core at 200 containers.

Each pass over the files that are due hands their records to a sink (`Add`, which copies) and makes them durable
with one `Flush`; positions count as committed only after it. A pass takes at most 4 096 records, so a long backlog
becomes several bounded writes, not one huge one. A record's text points into the reader's buffer until the next
read: no string per line.

Containers come from the lifecycle tracker's change feed, not from a Docker subscription of the tailer's own. A
container is followed once it has started; a created one has no log yet. When it exits or is removed its files are
read to their end through the descriptors held, then let go. Seen for the first time, every file of it still on disk
is read, oldest first.

dockerd writes one fixed json-file shape, which a hand-written parser reads without allocating, 9x faster than
`encoding/json`; anything else falls back to `encoding/json`.

How long the agent may be stopped without losing lines is set by the runtime's rotation, not by the agent: a
30 s stop lost 18,766 lines of a container whose rotation kept 11 s of its log. Docker's inspect says how much of a
json-file log it keeps (max-size times max-file; with compress only the current file, as gzipped ones are not read),
and every 30 s the tailer divides that by the rate each log is read at. `cosmicdust_agent_tail_retention_seconds` is the
shortest result, and a log that keeps less than 10 minutes is named in a warning, once. The rate is what reading
went through, so it runs high while a backlog is read. A kubelet's rotation is its own setting, not known here.
