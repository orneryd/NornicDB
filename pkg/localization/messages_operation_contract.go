package localization

import "log/slog"

// OperationContractRequired identifies a callable without a valid declared
// privilege contract. Such an operation cannot be dispatched.
func OperationContractRequired() Message {
	return Message{
		ID:       MessageID("auth.operation_contract_required"),
		Fallback: "Operation requires a valid privilege contract",
	}
}

func OperationStorageCleanupFailedEvent(database string, err error) LogEvent {
	return LogEvent{ID: "cypher.operation.storage_cleanup_failed", Message: Message{ID: "cypher.operation.storage_cleanup_failed", Fallback: "Request-scoped database storage cleanup failed"}, Attrs: []slog.Attr{slog.String("database", database), slog.String("outcome", "cleanup_failed"), slog.Any("error", err)}}
}
