# 0002 — Configuration through flags and the environment

**Status:** accepted

The daemon is configured with flags, and every flag can also come from an environment variable named `COSMICDUST_` plus
the flag's name in upper case with underscores. Flag beats environment beats default.

The neighbouring agents of an edge stack are configured through the environment only, which suits a container
spec; flags suit measurement scripts. Deriving one from the other keeps
a single list of settings, with its help text, and adds no dependency. A configuration file can come later if the
number of settings outgrows the environment; nothing in the code depends on where a value came from.

The Go runtime's soft memory limit defaults to 64 MiB. Without a limit the collector let the heap double between
cycles and resident memory followed it (about 73 MB); with 64 MiB it stayed at about 50 MB for the same CPU cost.
`GOMEMLIMIT`, when set, is left to the runtime.
