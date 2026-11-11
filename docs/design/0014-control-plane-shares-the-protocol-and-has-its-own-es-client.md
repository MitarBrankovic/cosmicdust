# 0014 — Control plane: the node's protocol and schema, and an Elasticsearch client of its own

**Status:** accepted

The service speaks the node's protocol, `pkg/api`, and reads the node's files with the store's own row types,
`store.LogRow` and `store.EventRow`: one definition of each, so the two sides cannot drift apart while they are
developed together. The NATS subjects come from `api.FilesSubject` and `api.UrgentSubject`, which the node's
transport uses too. The service's packages are under `internal/server` and import nothing else of the node, so the two
sides stay separable. The protocol and the row types must
stay compatible with the nodes in the field - new fields only, defaults that mean the old behaviour.

The service is configured from the environment, as the c12s services are, rather than with flags like `cosmicdust-agent`; it runs
next to them on the control plane.

Elasticsearch is reached through a client of the service's own (`internal/server/es`): bulk indexing, index templates,
data streams and their lifecycle, a handful of REST calls. The official client would bring a large dependency for
those, and every request the service makes can be answered by a fake server in its tests; an integration test runs the
indexer against a real Elasticsearch (9.5).
