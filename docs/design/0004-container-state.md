# 0004 — Container state: one view per runtime, merged, reconciled

**Status:** accepted

Docker and containerd report the same lifecycle from different heights. containerd reports an exit ~350-420 ms
before Docker and stamps it with the moment the process ended; Docker has been seen to report a start after
containerd had already reported that incarnation's exit (in two separate runs, one of them under a restart
policy). One state machine fed by both would take that late start for a restart. Within one runtime the order is sound,
so each runtime keeps its own view of a container (state, incarnation count, stamp of the last state change), and the
container's state is the view furthest along: a later incarnation, or the same one in a later phase. A runtime that
has not reported a step yet is late, not wrong. In Docker's namespace containerd's container object is created and
deleted with every incarnation, so there those two events are not the container's.

The state follows the event's stamp, not its arrival; a late event is recorded and does not move the state. Events
are not durable, so after every connect a source sends a resync marker and the tracker compares that runtime's views
with its listing. Inferred events are stamped no later than the listing's start, so nothing the open stream delivered
meanwhile is overridden, and carry the runtime's own times where it has them (Docker's inspect), so the history, and
the state stored with each log line, stays right across a restart of the agent. containerd, ~50x cheaper to list, is
reconciled every 5 minutes as well; Docker only after a connect. The state is written to disk a second after it
changed, not only at a clean stop. What changed in that second and was passed on is in the WAL, and the tracker
applies those events again at its start, without passing them on: a kill -9 then no longer makes the listing infer
an event already stored (record 0006).

An OOM event is a hint and the kernel's counter in the cgroup the evidence. The runtimes' events can be missing -
of 400 containers killed within milliseconds of their start, containerd and Docker missed 9, and one such miss failed
CI - and with the systemd cgroup driver the cgroup is gone by the time the exit is reported. So `memory.events` of
every live incarnation is watched with inotify, from containerd's `/tasks/create` on, when the cgroup exists and the
process has not started, and read when the kernel marks it modified; a kill read so is passed on as an event of its
own (`cgroup/oom_kill`). The kernel notifies a file at most every ~10 ms and holds back what changes in between: the
kill of a small container a few milliseconds after it reached its limit waits for the next notification, and the
cgroup is gone before it (29 of 100). For those the watch notes, whenever it reads a container at its limit, the
parent cgroup's `oom_kill`, which counts the kills of its children and outlives them; if the container's cgroup is
gone at its exit and the parent's counter has grown since, the kill was its. The counter is still read at a
runtime's exit or OOM event, for an incarnation the watch missed. All of 200 such kills, 100 of them under full
load, were confirmed by the kernel, the 7 the runtimes missed included.

containerd sends exits of exec processes (every health check) on the same topic as the container's; only the init
process's exit counts.

The source interface follows from this: `List` (with the namespaces where only running containers are listed) and
`Describe` rather than one snapshot call, and `Watch` writes to the caller's channel and marks every connect.
