# 0003 — The node makes and keeps its own identity

**Status:** accepted

Every row carries the node it came from, and the control plane deduplicates on node and line position. The identity
therefore must survive restarts of the agent, of its container and of the machine. A hostname does not (a container
gets a random one unless told otherwise), and an identity issued by an orchestrator can change on every boot of the
node.

On first start the agent generates 128 random bits, writes them to `<data-dir>/node-id` atomically (temporary file,
fsync, rename, fsync of the directory) and reads them back from then on. An operator can set `-node` instead, or
write the file before the first start. An identity issued by an orchestrator, where there is one, is sent alongside
as metadata rather than used as the key.
