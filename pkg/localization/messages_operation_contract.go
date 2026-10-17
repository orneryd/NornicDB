package localization

import "log/slog"

const (
	// MessageAuthOperationContractRequired identifies a callable without a
	// valid declared privilege contract.
	MessageAuthOperationContractRequired MessageID = "auth.operation_contract_required"

	// EventOperationStorageCleanupFailed identifies a failure to release
	// request-scoped database storage.
	EventOperationStorageCleanupFailed EventID = "cypher.operation.storage_cleanup_failed"
	// MessageCypherLogStorageCleanupFailed is that event's operator message.
	MessageCypherLogStorageCleanupFailed MessageID = "cypher.log.storage_cleanup_failed"
)

// OperationContractRequired identifies a callable without a valid declared
// privilege contract. Such an operation cannot be dispatched.
func OperationContractRequired() Message {
	return Message{
		ID:       MessageAuthOperationContractRequired,
		Fallback: "Operation requires a valid privilege contract",
	}
}

// CypherLogStorageCleanupFailed describes a failed release of
// request-scoped database storage.
func CypherLogStorageCleanupFailed() Message {
	return Message{ID: MessageCypherLogStorageCleanupFailed, Fallback: "Request-scoped database storage cleanup failed"}
}

// OperationStorageCleanupFailedEvent is the operator record of a failed
// release of database's request-scoped storage.
func OperationStorageCleanupFailedEvent(database string, err error) LogEvent {
	return LogEvent{ID: EventOperationStorageCleanupFailed, Message: CypherLogStorageCleanupFailed(), Attrs: []slog.Attr{slog.String("database", database), slog.String("outcome", "cleanup_failed"), slog.Any("error", err)}}
}
