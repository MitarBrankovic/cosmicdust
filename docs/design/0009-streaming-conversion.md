# 0009 — Conversion streams; memory follows a chunk, not the rate

**Status:** accepted

A conversion used to gather every row it read (up to 32 MiB of lines), sort them and only then write. At 1,000
lines/s that is ~10,000 rows every 10 s and went unnoticed. At 5,000 lines/s of real text it kept ~25 MB live
during every conversion; with the WAL's and the tailer's buffers grown to the biggest block they had seen, the
process sat at its 64 MiB limit and the collector ran ~950 times a minute: a third of a core, against 5 % now. The
loop fed itself: under the collector the tailer's passes came late, read more at once, and grew the blocks.

Rows now stream into the files as the WAL is read. Each hour being written gathers at most `SortBytes` of lines
(4 MiB), which are sorted by container and time and written; a file whose rows fit is sorted as a whole (every file at
1,000 lines/s), a bigger one is a few sorted runs. Sorting in chunks cost no measurable compression (17.2 either way
at 5,000 lines/s). At most four files are open at once: a backlog read from an old container's log moves through its
hours one after another, so the least recently used file is finished, and its hour gets another file should rows for
it come again. Files are named after the conversion's first segment and a sequence number. `ConvertBytes` still
bounds what one conversion takes.

Buffers no longer keep their high-water mark: the WAL lets go of block buffers four times bigger than the last
block, a tailer pass takes at most 4,096 records, a reader's arena goes when four times bigger than its last read.
The events table gets 64 KiB pages; with the logs' 1 MiB its writer held ~3 MB for a few events per file. The files
a tailer follows share one read buffer instead of 16 KiB each: what is read is copied into the reader's arena before
the next file is read, and a file that gets the buffer back is first moved to its own offset.

Log pages stay at 1 MiB: 256 KiB pages cost 6.5 % of the compression ratio on real text and did not lower resident
memory under the 64 MiB limit. The limit stays at 64 MiB too. At 48 MiB resident memory falls by ~11 MB (to ~60 MB)
and costs 12 % more CPU at 1,000 lines/s, but 80 % more at 5,000 (the live heap peaks at ~33 MB during a
conversion, most of it the writers' column buffers), so it is an option for small nodes with little to read, not
the default.
