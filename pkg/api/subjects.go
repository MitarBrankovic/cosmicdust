package api

// Tables are the two kinds of file a node sends.
const (
	TableLogs   = "logs"
	TableEvents = "events"
)

// FilesSubject is where a node publishes the chunks of its files over NATS,
// each with a reply subject of its own for the ack.
func FilesSubject(prefix, node string) string { return prefix + ".files." + node }

// UrgentSubject is where a node publishes the lifecycle events that cannot
// wait for the next file.
func UrgentSubject(prefix, node string) string { return prefix + ".urgent." + node }
