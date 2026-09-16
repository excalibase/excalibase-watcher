package cdc

// IsPublishable reports whether an event of this type is forwarded to NATS.
// Transaction markers and heartbeats stay internal to the watcher.
func IsPublishable(t EventType) bool {
	switch t {
	case Insert, Update, Delete, DDL, Truncate:
		return true
	default:
		return false
	}
}
