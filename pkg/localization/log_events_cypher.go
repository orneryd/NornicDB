package localization

import "log/slog"

const (
	// EventCypherSlowQuery identifies a query exceeding the configured slow-query threshold.
	EventCypherSlowQuery EventID = "cypher.slow_query"
	// EventCypherCreateDatabaseInvoked identifies entry into CREATE DATABASE execution.
	EventCypherCreateDatabaseInvoked EventID = "cypher.create_database.invoked"
	// EventCypherCreateDatabaseFailed identifies a failed CREATE DATABASE operation.
	EventCypherCreateDatabaseFailed EventID = "cypher.create_database.failed"
	// EventCypherCreateDatabaseSucceeded identifies a successful CREATE DATABASE operation.
	EventCypherCreateDatabaseSucceeded EventID = "cypher.create_database.succeeded"
	// EventCypherVectorSearchDisabled identifies a vector query against a disabled database.
	EventCypherVectorSearchDisabled EventID = "cypher.vector_search.disabled"
	// EventCypherOrphanedEmbeddingDetected identifies an embedding whose node no longer exists.
	EventCypherOrphanedEmbeddingDetected EventID = "cypher.vector_search.orphaned_embedding_detected"
	// EventCypherOrphanedEmbeddingRemovalFailed identifies failure to remove an orphaned embedding.
	EventCypherOrphanedEmbeddingRemovalFailed EventID = "cypher.vector_search.orphaned_embedding_removal_failed"
	// EventCypherDecaySubsystemDisabled identifies decay evaluation while decay is disabled.
	EventCypherDecaySubsystemDisabled EventID = "cypher.knowledgepolicy.decay_subsystem_disabled"

	MessageCypherLogSlowQuery                      MessageID = "cypher.log.slow_query"
	MessageCypherLogCreateDatabaseInvoked          MessageID = "cypher.log.create_database_invoked"
	MessageCypherLogCreateDatabaseFailed           MessageID = "cypher.log.create_database_failed"
	MessageCypherLogCreateDatabaseSucceeded        MessageID = "cypher.log.create_database_succeeded"
	MessageCypherLogVectorSearchDisabled           MessageID = "cypher.log.vector_search_disabled"
	MessageCypherLogOrphanedEmbeddingDetected      MessageID = "cypher.log.orphaned_embedding_detected"
	MessageCypherLogOrphanedEmbeddingRemovalFailed MessageID = "cypher.log.orphaned_embedding_removal_failed"
	MessageCypherLogDecaySubsystemDisabled         MessageID = "cypher.log.decay_subsystem_disabled"
)

// CypherLogSlowQuery describes a slow-query log message.
func CypherLogSlowQuery() Message {
	return Message{ID: MessageCypherLogSlowQuery, Fallback: "slow query"}
}

// CypherLogCreateDatabaseInvoked describes entry into CREATE DATABASE execution.
func CypherLogCreateDatabaseInvoked() Message {
	return Message{ID: MessageCypherLogCreateDatabaseInvoked, Fallback: "executeCreateDatabase invoked"}
}

// CypherLogCreateDatabaseFailed describes a failed CREATE DATABASE operation.
func CypherLogCreateDatabaseFailed() Message {
	return Message{ID: MessageCypherLogCreateDatabaseFailed, Fallback: "CreateDatabase failed"}
}

// CypherLogCreateDatabaseSucceeded describes a successful CREATE DATABASE operation.
func CypherLogCreateDatabaseSucceeded() Message {
	return Message{ID: MessageCypherLogCreateDatabaseSucceeded, Fallback: "CreateDatabase succeeded"}
}

// CypherLogVectorSearchDisabled describes a vector query against a disabled database.
func CypherLogVectorSearchDisabled() Message {
	return Message{ID: MessageCypherLogVectorSearchDisabled, Fallback: "db.index.vector.queryNodes called against vector-disabled database — returning empty result"}
}

// CypherLogOrphanedEmbeddingDetected describes an embedding whose node no longer exists.
func CypherLogOrphanedEmbeddingDetected() Message {
	return Message{ID: MessageCypherLogOrphanedEmbeddingDetected, Fallback: "orphaned embedding detected, removing from indexes"}
}

// CypherLogOrphanedEmbeddingRemovalFailed describes failure to remove an orphaned embedding.
func CypherLogOrphanedEmbeddingRemovalFailed() Message {
	return Message{ID: MessageCypherLogOrphanedEmbeddingRemovalFailed, Fallback: "failed to remove orphaned embedding"}
}

// CypherLogDecaySubsystemDisabled describes decay evaluation while decay is disabled.
func CypherLogDecaySubsystemDisabled() Message {
	return Message{ID: MessageCypherLogDecaySubsystemDisabled, Fallback: "decay function called but decay subsystem is disabled; returning neutral scores"}
}

// CypherSlowQueryEvent describes a query exceeding the configured threshold.
func CypherSlowQueryEvent(planHash string, durationMilliseconds int64, query string) LogEvent {
	return LogEvent{
		ID:      EventCypherSlowQuery,
		Message: CypherLogSlowQuery(),
		Attrs: []slog.Attr{
			slog.String("event", "slow_query"),
			slog.String("plan_hash", planHash),
			slog.Int64("cypher.duration_ms", durationMilliseconds),
			slog.String("query", query),
		},
	}
}

// CypherCreateDatabaseInvokedEvent describes entry into CREATE DATABASE execution.
func CypherCreateDatabaseInvokedEvent(queryLength int) LogEvent {
	return LogEvent{
		ID:      EventCypherCreateDatabaseInvoked,
		Message: CypherLogCreateDatabaseInvoked(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "create_database"),
			slog.Int("query_len", queryLength),
		},
	}
}

// CypherCreateDatabaseFailedEvent describes a failed CREATE DATABASE operation.
func CypherCreateDatabaseFailedEvent() LogEvent {
	return LogEvent{
		ID:      EventCypherCreateDatabaseFailed,
		Message: CypherLogCreateDatabaseFailed(),
		Attrs:   []slog.Attr{slog.String("subsystem", "create_database")},
	}
}

// CypherCreateDatabaseSucceededEvent describes a successful CREATE DATABASE operation.
func CypherCreateDatabaseSucceededEvent() LogEvent {
	return LogEvent{
		ID:      EventCypherCreateDatabaseSucceeded,
		Message: CypherLogCreateDatabaseSucceeded(),
		Attrs:   []slog.Attr{slog.String("subsystem", "create_database")},
	}
}

// CypherVectorSearchDisabledEvent describes a vector query against a disabled database.
func CypherVectorSearchDisabledEvent(indexName string) LogEvent {
	return LogEvent{
		ID:      EventCypherVectorSearchDisabled,
		Message: CypherLogVectorSearchDisabled(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "vector_search"),
			slog.String("index_name", indexName),
		},
	}
}

// CypherOrphanedEmbeddingDetectedEvent describes an embedding whose node no longer exists.
func CypherOrphanedEmbeddingDetectedEvent(nodeID string) LogEvent {
	return LogEvent{
		ID:      EventCypherOrphanedEmbeddingDetected,
		Message: CypherLogOrphanedEmbeddingDetected(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "vector_search"),
			slog.String("node_id", nodeID),
		},
	}
}

// CypherOrphanedEmbeddingRemovalFailedEvent describes failure to remove an orphaned embedding.
func CypherOrphanedEmbeddingRemovalFailedEvent(nodeID string, err error) LogEvent {
	return LogEvent{
		ID:      EventCypherOrphanedEmbeddingRemovalFailed,
		Message: CypherLogOrphanedEmbeddingRemovalFailed(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "vector_search"),
			slog.String("node_id", nodeID),
			slog.Any("error", err),
		},
	}
}

// CypherDecaySubsystemDisabledEvent describes decay evaluation while decay is disabled.
func CypherDecaySubsystemDisabledEvent() LogEvent {
	return LogEvent{
		ID:      EventCypherDecaySubsystemDisabled,
		Message: CypherLogDecaySubsystemDisabled(),
		Attrs:   []slog.Attr{slog.String("component", "knowledgepolicy")},
	}
}
