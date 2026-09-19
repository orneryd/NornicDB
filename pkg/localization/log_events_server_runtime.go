package localization

import (
	"log/slog"
	"time"
)

const (
	EventServerHTTPRequest                         EventID = "server.http.request"
	EventServerSlowQuery                           EventID = "server.slow_query"
	EventServerAuthTrace                           EventID = "server.auth.trace"
	EventServerHTTPPanicRecovered                  EventID = "server.http.panic_recovered"
	EventServerHTTPPanicStackTrace                 EventID = "server.http.panic_stack_trace"
	EventServerOAuthRedirectStateStored            EventID = "server.oauth.redirect_state_stored"
	EventServerOAuthCallbackFailed                 EventID = "server.oauth.callback_failed"
	EventServerOAuthUserAuthenticated              EventID = "server.oauth.user_authenticated"
	EventServerRBACRoleEntitlementsSetFailed       EventID = "server.rbac.role_entitlements_set_failed"
	EventServerRBACAllowlistSaveFailed             EventID = "server.rbac.allowlist_save_failed"
	EventServerRBACPrivilegesPutMatrixFailed       EventID = "server.rbac.privileges_put_matrix_failed"
	EventServerEmbedRegenerationAborted            EventID = "server.embed.regeneration_aborted"
	EventServerEmbedRegenerationStarting           EventID = "server.embed.regeneration_starting"
	EventServerEmbedWorkerResetFailed              EventID = "server.embed.worker_reset_failed"
	EventServerEmbedRegenerationSkippedDBClosing   EventID = "server.embed.regeneration_skipped_db_closing"
	EventServerEmbedClearFailed                    EventID = "server.embed.clear_failed"
	EventServerEmbedCleared                        EventID = "server.embed.cleared"
	EventServerEmbedWorkerTriggerFailed            EventID = "server.embed.worker_trigger_failed"
	EventServerEmbedWorkerTriggered                EventID = "server.embed.worker_triggered"
	EventServerSearchRequest                       EventID = "server.search.request"
	EventServerSearchStorageLookupFailed           EventID = "server.search.storage_lookup_failed"
	EventServerSearchQueryEmbeddingFailed          EventID = "server.search.query_embedding_failed"
	EventServerSearchChunkedQueryEmbeddingFailed   EventID = "server.search.chunked_query_embedding_failed"
	EventServerSearchTiming                        EventID = "server.search.timing"
	EventServerDBConfigReloadFailed                EventID = "server.dbconfig.reload_failed"
	EventServerDBConfigRebuildStorageResolveFailed EventID = "server.dbconfig.rebuild_storage_resolve_failed"
	EventServerDBConfigRebuildStartFailed          EventID = "server.dbconfig.rebuild_start_failed"
	EventServerCreateDatabaseDefensiveFix          EventID = "server.create_database.defensive_fix"
	EventServerRetentionDefaultPolicyAddFailed     EventID = "server.retention.default_policy_add_failed"
	EventServerGraphQLRequest                      EventID = "server.graphql.request"
	EventServerQdrantGRPCEnabled                   EventID = "server.qdrant_grpc.enabled"
)

const (
	MessageServerLogHTTPRequest                         MessageID = "server-runtime.log.http_request"
	MessageServerLogSlowQuery                           MessageID = "server-runtime.log.slow_query"
	MessageServerLogAuthTrace                           MessageID = "server-runtime.log.auth_trace"
	MessageServerLogHTTPPanicRecovered                  MessageID = "server-runtime.log.http_panic_recovered"
	MessageServerLogHTTPPanicStackTrace                 MessageID = "server-runtime.log.http_panic_stack_trace"
	MessageServerLogOAuthRedirectStateStored            MessageID = "server-runtime.log.oauth_redirect_state_stored"
	MessageServerLogOAuthCallbackFailed                 MessageID = "server-runtime.log.oauth_callback_failed"
	MessageServerLogOAuthUserAuthenticated              MessageID = "server-runtime.log.oauth_user_authenticated"
	MessageServerLogRBACRoleEntitlementsSetFailed       MessageID = "server-runtime.log.rbac_role_entitlements_set_failed"
	MessageServerLogRBACAllowlistSaveFailed             MessageID = "server-runtime.log.rbac_allowlist_save_failed"
	MessageServerLogRBACPrivilegesPutMatrixFailed       MessageID = "server-runtime.log.rbac_privileges_put_matrix_failed"
	MessageServerLogEmbedRegenerationAborted            MessageID = "server-runtime.log.embed_regeneration_aborted"
	MessageServerLogEmbedRegenerationStarting           MessageID = "server-runtime.log.embed_regeneration_starting"
	MessageServerLogEmbedWorkerResetFailed              MessageID = "server-runtime.log.embed_worker_reset_failed"
	MessageServerLogEmbedRegenerationSkippedDBClosing   MessageID = "server-runtime.log.embed_regeneration_skipped_db_closing"
	MessageServerLogEmbedClearFailed                    MessageID = "server-runtime.log.embed_clear_failed"
	MessageServerLogEmbedCleared                        MessageID = "server-runtime.log.embed_cleared"
	MessageServerLogEmbedWorkerTriggerFailed            MessageID = "server-runtime.log.embed_worker_trigger_failed"
	MessageServerLogEmbedWorkerTriggered                MessageID = "server-runtime.log.embed_worker_triggered"
	MessageServerLogSearchRequest                       MessageID = "server-runtime.log.search_request"
	MessageServerLogSearchStorageLookupFailed           MessageID = "server-runtime.log.search_storage_lookup_failed"
	MessageServerLogSearchQueryEmbeddingFailed          MessageID = "server-runtime.log.search_query_embedding_failed"
	MessageServerLogSearchChunkedQueryEmbeddingFailed   MessageID = "server-runtime.log.search_chunked_query_embedding_failed"
	MessageServerLogSearchTiming                        MessageID = "server-runtime.log.search_timing"
	MessageServerLogDBConfigReloadFailed                MessageID = "server-runtime.log.dbconfig_reload_failed"
	MessageServerLogDBConfigRebuildStorageResolveFailed MessageID = "server-runtime.log.dbconfig_rebuild_storage_resolve_failed"
	MessageServerLogDBConfigRebuildStartFailed          MessageID = "server-runtime.log.dbconfig_rebuild_start_failed"
	MessageServerLogCreateDatabaseDefensiveFix          MessageID = "server-runtime.log.create_database_defensive_fix"
	MessageServerLogRetentionDefaultPolicyAddFailed     MessageID = "server-runtime.log.retention_default_policy_add_failed"
	MessageServerLogGraphQLRequest                      MessageID = "server-runtime.log.graphql_request"
	MessageServerLogQdrantGRPCEnabled                   MessageID = "server-runtime.log.qdrant_grpc_enabled"
)

// ServerLogHTTPRequest describes a completed HTTP request message.
func ServerLogHTTPRequest() Message {
	return Message{ID: MessageServerLogHTTPRequest, Fallback: "http request"}
}

// ServerLogSlowQuery describes a slow query message.
func ServerLogSlowQuery() Message {
	return Message{ID: MessageServerLogSlowQuery, Fallback: "slow query"}
}

// ServerLogAuthTrace describes an authentication trace message.
func ServerLogAuthTrace() Message {
	return Message{ID: MessageServerLogAuthTrace, Fallback: "auth"}
}

// ServerLogHTTPPanicRecovered describes a recovered HTTP panic message.
func ServerLogHTTPPanicRecovered() Message {
	return Message{ID: MessageServerLogHTTPPanicRecovered, Fallback: "panic recovered in HTTP handler"}
}

// ServerLogHTTPPanicStackTrace describes an HTTP panic stack trace message.
func ServerLogHTTPPanicStackTrace() Message {
	return Message{ID: MessageServerLogHTTPPanicStackTrace, Fallback: "panic stack trace"}
}

// ServerLogOAuthRedirectStateStored describes stored OAuth redirect state.
func ServerLogOAuthRedirectStateStored() Message {
	return Message{ID: MessageServerLogOAuthRedirectStateStored, Fallback: "oauth redirect: stored state in memory (expires in 10 minutes)"}
}

// ServerLogOAuthCallbackFailed describes an OAuth callback failure message.
func ServerLogOAuthCallbackFailed() Message {
	return Message{ID: MessageServerLogOAuthCallbackFailed, Fallback: "oauth callback error"}
}

// ServerLogOAuthUserAuthenticated describes a successful OAuth callback message.
func ServerLogOAuthUserAuthenticated() Message {
	return Message{ID: MessageServerLogOAuthUserAuthenticated, Fallback: "oauth callback: authenticated user"}
}

// ServerLogRBACRoleEntitlementsSetFailed describes a failed role entitlement update.
func ServerLogRBACRoleEntitlementsSetFailed() Message {
	return Message{ID: MessageServerLogRBACRoleEntitlementsSetFailed, Fallback: "role entitlements set failed"}
}

// ServerLogRBACAllowlistSaveFailed describes a failed database allowlist update.
func ServerLogRBACAllowlistSaveFailed() Message {
	return Message{ID: MessageServerLogRBACAllowlistSaveFailed, Fallback: "allowlist save role databases failed"}
}

// ServerLogRBACPrivilegesPutMatrixFailed describes a failed privilege matrix update.
func ServerLogRBACPrivilegesPutMatrixFailed() Message {
	return Message{ID: MessageServerLogRBACPrivilegesPutMatrixFailed, Fallback: "privileges PutMatrix failed"}
}

// ServerLogEmbedRegenerationAborted describes interrupted embedding regeneration.
func ServerLogEmbedRegenerationAborted() Message {
	return Message{ID: MessageServerLogEmbedRegenerationAborted, Fallback: "regeneration aborted during shutdown"}
}

// ServerLogEmbedRegenerationStarting describes the start of embedding regeneration.
func ServerLogEmbedRegenerationStarting() Message {
	return Message{ID: MessageServerLogEmbedRegenerationStarting, Fallback: "starting background regeneration: stopping worker and clearing embeddings"}
}

// ServerLogEmbedWorkerResetFailed describes a failed embedding worker reset.
func ServerLogEmbedWorkerResetFailed() Message {
	return Message{ID: MessageServerLogEmbedWorkerResetFailed, Fallback: "failed to reset embed worker"}
}

// ServerLogEmbedRegenerationSkippedDBClosing describes regeneration skipped during shutdown.
func ServerLogEmbedRegenerationSkippedDBClosing() Message {
	return Message{ID: MessageServerLogEmbedRegenerationSkippedDBClosing, Fallback: "regeneration skipped: database is closing"}
}

// ServerLogEmbedClearFailed describes a failure clearing embeddings.
func ServerLogEmbedClearFailed() Message {
	return Message{ID: MessageServerLogEmbedClearFailed, Fallback: "failed to clear embeddings"}
}

// ServerLogEmbedCleared describes cleared embeddings before regeneration.
func ServerLogEmbedCleared() Message {
	return Message{ID: MessageServerLogEmbedCleared, Fallback: "cleared embeddings; triggering regeneration"}
}

// ServerLogEmbedWorkerTriggerFailed describes a failed embedding worker trigger.
func ServerLogEmbedWorkerTriggerFailed() Message {
	return Message{ID: MessageServerLogEmbedWorkerTriggerFailed, Fallback: "failed to trigger embedding worker"}
}

// ServerLogEmbedWorkerTriggered describes a successful embedding worker trigger.
func ServerLogEmbedWorkerTriggered() Message {
	return Message{ID: MessageServerLogEmbedWorkerTriggered, Fallback: "embedding worker triggered for regeneration"}
}

// ServerLogSearchRequest describes an HTTP search request message.
func ServerLogSearchRequest() Message {
	return Message{ID: MessageServerLogSearchRequest, Fallback: "search request"}
}

// ServerLogSearchStorageLookupFailed describes a search storage lookup failure.
func ServerLogSearchStorageLookupFailed() Message {
	return Message{ID: MessageServerLogSearchStorageLookupFailed, Fallback: "search: storage lookup failed"}
}

// ServerLogSearchQueryEmbeddingFailed describes a query embedding fallback.
func ServerLogSearchQueryEmbeddingFailed() Message {
	return Message{ID: MessageServerLogSearchQueryEmbeddingFailed, Fallback: "query embedding failed"}
}

// ServerLogSearchChunkedQueryEmbeddingFailed describes a chunk embedding fallback.
func ServerLogSearchChunkedQueryEmbeddingFailed() Message {
	return Message{ID: MessageServerLogSearchChunkedQueryEmbeddingFailed, Fallback: "query embedding failed (chunked)"}
}

// ServerLogSearchTiming describes detailed search diagnostics.
func ServerLogSearchTiming() Message {
	return Message{ID: MessageServerLogSearchTiming, Fallback: "search timing"}
}

// ServerLogDBConfigReloadFailed describes a post-update config reload failure.
func ServerLogDBConfigReloadFailed() Message {
	return Message{ID: MessageServerLogDBConfigReloadFailed, Fallback: "failed to reload db config store after PUT"}
}

// ServerLogDBConfigRebuildStorageResolveFailed describes a rebuild storage lookup failure.
func ServerLogDBConfigRebuildStorageResolveFailed() Message {
	return Message{ID: MessageServerLogDBConfigRebuildStorageResolveFailed, Fallback: "failed to resolve storage for db config rebuild"}
}

// ServerLogDBConfigRebuildStartFailed describes a search rebuild start failure.
func ServerLogDBConfigRebuildStartFailed() Message {
	return Message{ID: MessageServerLogDBConfigRebuildStartFailed, Fallback: "failed to start search service rebuild after db config update"}
}

// ServerLogCreateDatabaseDefensiveFix describes repaired empty CREATE DATABASE output.
func ServerLogCreateDatabaseDefensiveFix() Message {
	return Message{ID: MessageServerLogCreateDatabaseDefensiveFix, Fallback: "create_database: server defensive fix applied — executor returned empty result, filled with database name"}
}

// ServerLogRetentionDefaultPolicyAddFailed describes a default policy load failure.
func ServerLogRetentionDefaultPolicyAddFailed() Message {
	return Message{ID: MessageServerLogRetentionDefaultPolicyAddFailed, Fallback: "retention defaults: failed to add policy"}
}

// ServerLogGraphQLRequest describes an enabled GraphQL request trace.
func ServerLogGraphQLRequest() Message {
	return Message{ID: MessageServerLogGraphQLRequest, Fallback: "graphql request"}
}

// ServerLogQdrantGRPCEnabled describes a running Qdrant-compatible gRPC server.
func ServerLogQdrantGRPCEnabled() Message {
	return Message{ID: MessageServerLogQdrantGRPCEnabled, Fallback: "qdrant grpc enabled"}
}

func serverRuntimeEvent(id EventID, message Message, attrs ...slog.Attr) LogEvent {
	return LogEvent{ID: id, Message: message, Attrs: attrs}
}

// ServerHTTPRequestEvent describes a completed HTTP request.
func ServerHTTPRequestEvent(method, path string, status int, duration time.Duration) LogEvent {
	return serverRuntimeEvent(EventServerHTTPRequest, ServerLogHTTPRequest(),
		slog.String("subsystem", "http"), slog.String("method", method), slog.String("path", path),
		slog.Int("status", status), slog.Duration("duration", duration))
}

// ServerSlowQueryEvent describes a query exceeding the configured threshold.
func ServerSlowQueryEvent(message string) LogEvent {
	return serverRuntimeEvent(EventServerSlowQuery, ServerLogSlowQuery(),
		slog.String("event", "slow_query"), slog.String("msg", message))
}

// ServerAuthTraceEvent describes an enabled authentication timing trace.
func ServerAuthTraceEvent(method, path, step string, duration time.Duration, err error) LogEvent {
	return serverRuntimeEvent(EventServerAuthTrace, ServerLogAuthTrace(),
		slog.String("subsystem", "auth"), slog.String("method", method), slog.String("path", path),
		slog.String("step", step), slog.Duration("duration", duration), slog.Any("error", err))
}

// ServerHTTPPanicRecoveredEvent describes a panic recovered at the HTTP boundary.
func ServerHTTPPanicRecoveredEvent(value string) LogEvent {
	return serverRuntimeEvent(EventServerHTTPPanicRecovered, ServerLogHTTPPanicRecovered(), slog.String("panic", value))
}

// ServerHTTPPanicStackTraceEvent describes a debug-only recovered panic stack.
func ServerHTTPPanicStackTraceEvent(stack string) LogEvent {
	return serverRuntimeEvent(EventServerHTTPPanicStackTrace, ServerLogHTTPPanicStackTrace(), slog.String("stack", stack))
}

// ServerOAuthRedirectStateStoredEvent describes temporary OAuth state creation.
func ServerOAuthRedirectStateStoredEvent(statePrefix string) LogEvent {
	return serverRuntimeEvent(EventServerOAuthRedirectStateStored, ServerLogOAuthRedirectStateStored(),
		slog.String("subsystem", "oauth"), slog.String("state_prefix", statePrefix))
}

// ServerOAuthCallbackFailedEvent describes an OAuth callback failure.
func ServerOAuthCallbackFailedEvent() LogEvent {
	return serverRuntimeEvent(EventServerOAuthCallbackFailed, ServerLogOAuthCallbackFailed(), slog.String("subsystem", "oauth"))
}

// ServerOAuthUserAuthenticatedEvent describes a successful OAuth callback.
func ServerOAuthUserAuthenticatedEvent() LogEvent {
	return serverRuntimeEvent(EventServerOAuthUserAuthenticated, ServerLogOAuthUserAuthenticated(), slog.String("subsystem", "oauth"))
}

// ServerRBACRoleEntitlementsSetFailedEvent describes a failed role entitlement update.
func ServerRBACRoleEntitlementsSetFailedEvent(count int) LogEvent {
	return serverRuntimeEvent(EventServerRBACRoleEntitlementsSetFailed, ServerLogRBACRoleEntitlementsSetFailed(),
		slog.String("subsystem", "rbac"), slog.Int("entitlements_count", count))
}

// ServerRBACAllowlistSaveFailedEvent describes a failed database allowlist update.
func ServerRBACAllowlistSaveFailedEvent() LogEvent {
	return serverRuntimeEvent(EventServerRBACAllowlistSaveFailed, ServerLogRBACAllowlistSaveFailed(), slog.String("subsystem", "rbac"))
}

// ServerRBACPrivilegesPutMatrixFailedEvent describes a failed privilege matrix update.
func ServerRBACPrivilegesPutMatrixFailedEvent() LogEvent {
	return serverRuntimeEvent(EventServerRBACPrivilegesPutMatrixFailed, ServerLogRBACPrivilegesPutMatrixFailed(), slog.String("subsystem", "rbac"))
}

func embedRuntimeEvent(id EventID, message Message, attrs ...slog.Attr) LogEvent {
	attrs = append([]slog.Attr{slog.String("subsystem", "embed")}, attrs...)
	return serverRuntimeEvent(id, message, attrs...)
}

// ServerEmbedRegenerationAbortedEvent describes shutdown interrupting regeneration.
func ServerEmbedRegenerationAbortedEvent(value any) LogEvent {
	return embedRuntimeEvent(EventServerEmbedRegenerationAborted, ServerLogEmbedRegenerationAborted(), slog.Any("panic", value))
}

// ServerEmbedRegenerationStartingEvent describes the start of embedding regeneration.
func ServerEmbedRegenerationStartingEvent() LogEvent {
	return embedRuntimeEvent(EventServerEmbedRegenerationStarting, ServerLogEmbedRegenerationStarting())
}

// ServerEmbedWorkerResetFailedEvent describes a failed embedding worker reset.
func ServerEmbedWorkerResetFailedEvent(err error) LogEvent {
	return embedRuntimeEvent(EventServerEmbedWorkerResetFailed, ServerLogEmbedWorkerResetFailed(), slog.Any("error", err))
}

// ServerEmbedRegenerationSkippedDBClosingEvent describes regeneration skipped during shutdown.
func ServerEmbedRegenerationSkippedDBClosingEvent() LogEvent {
	return embedRuntimeEvent(EventServerEmbedRegenerationSkippedDBClosing, ServerLogEmbedRegenerationSkippedDBClosing())
}

// ServerEmbedClearFailedEvent describes a failure clearing embeddings.
func ServerEmbedClearFailedEvent(err error) LogEvent {
	return embedRuntimeEvent(EventServerEmbedClearFailed, ServerLogEmbedClearFailed(), slog.Any("error", err))
}

// ServerEmbedClearedEvent describes cleared embeddings before regeneration.
func ServerEmbedClearedEvent(cleared int) LogEvent {
	return embedRuntimeEvent(EventServerEmbedCleared, ServerLogEmbedCleared(), slog.Int("cleared", cleared))
}

// ServerEmbedWorkerTriggerFailedEvent describes a failed embedding worker trigger.
func ServerEmbedWorkerTriggerFailedEvent(err error) LogEvent {
	return embedRuntimeEvent(EventServerEmbedWorkerTriggerFailed, ServerLogEmbedWorkerTriggerFailed(), slog.Any("error", err))
}

// ServerEmbedWorkerTriggeredEvent describes a successful embedding worker trigger.
func ServerEmbedWorkerTriggeredEvent() LogEvent {
	return embedRuntimeEvent(EventServerEmbedWorkerTriggered, ServerLogEmbedWorkerTriggered())
}

// ServerSearchRequestEvent describes an HTTP search request.
func ServerSearchRequestEvent(database, query string) LogEvent {
	return serverRuntimeEvent(EventServerSearchRequest, ServerLogSearchRequest(),
		slog.String("subsystem", "search"), slog.String("db", database), slog.String("query", query))
}

// ServerSearchStorageLookupFailedEvent describes a search storage lookup failure.
func ServerSearchStorageLookupFailedEvent(database string, err error) LogEvent {
	return serverRuntimeEvent(EventServerSearchStorageLookupFailed, ServerLogSearchStorageLookupFailed(),
		slog.String("subsystem", "search"), slog.String("db", database), slog.Any("error", err))
}

// ServerSearchQueryEmbeddingFailedEvent describes a query embedding fallback.
func ServerSearchQueryEmbeddingFailedEvent(err error) LogEvent {
	return serverRuntimeEvent(EventServerSearchQueryEmbeddingFailed, ServerLogSearchQueryEmbeddingFailed(),
		slog.String("subsystem", "search"), slog.Any("error", err))
}

// ServerSearchChunkedQueryEmbeddingFailedEvent describes a chunk embedding fallback.
func ServerSearchChunkedQueryEmbeddingFailedEvent(err error) LogEvent {
	return serverRuntimeEvent(EventServerSearchChunkedQueryEmbeddingFailed, ServerLogSearchChunkedQueryEmbeddingFailed(),
		slog.String("subsystem", "search"), slog.Any("error", err))
}

// ServerSearchTimingFields contains the stable fields emitted by search diagnostics.
type ServerSearchTimingFields struct {
	Status        string
	Database      string
	Total         time.Duration
	ServiceLookup time.Duration
	EmbedTotal    time.Duration
	EmbedCalls    int
	EmbedOK       int
	SearchTotal   time.Duration
	SearchCalls   int
	Chunks        int
	VectorChunks  int
	ChunkLoop     time.Duration
	FallbackBM25  int
	Error         error
	SearchMethod  string
	Fallback      bool
	Results       int
}

// ServerSearchTimingEvent describes detailed search diagnostics.
func ServerSearchTimingEvent(fields ServerSearchTimingFields) LogEvent {
	attrs := []slog.Attr{
		slog.String("subsystem", "search"),
		slog.String("status", fields.Status),
		slog.String("db", fields.Database),
		slog.Duration("total", fields.Total),
		slog.Duration("svc_lookup", fields.ServiceLookup),
		slog.Duration("embed_total", fields.EmbedTotal),
		slog.Int("embed_calls", fields.EmbedCalls),
		slog.Int("embed_ok", fields.EmbedOK),
		slog.Duration("search_total", fields.SearchTotal),
		slog.Int("search_calls", fields.SearchCalls),
		slog.Int("chunks", fields.Chunks),
		slog.Int("vector_chunks", fields.VectorChunks),
		slog.Duration("chunk_loop", fields.ChunkLoop),
		slog.Int("fallback_bm25", fields.FallbackBM25),
	}
	if fields.Status == "error" {
		attrs = append(attrs, slog.Any("error", fields.Error))
	} else {
		attrs = append(attrs,
			slog.String("search_method", fields.SearchMethod),
			slog.Bool("fallback", fields.Fallback),
			slog.Int("results", fields.Results),
		)
	}
	return serverRuntimeEvent(EventServerSearchTiming, ServerLogSearchTiming(), attrs...)
}

// ServerDBConfigReloadFailedEvent describes a post-update config reload failure.
func ServerDBConfigReloadFailedEvent(err error) LogEvent {
	return serverRuntimeEvent(EventServerDBConfigReloadFailed, ServerLogDBConfigReloadFailed(), slog.Any("error", err))
}

// ServerDBConfigRebuildStorageResolveFailedEvent describes a rebuild storage lookup failure.
func ServerDBConfigRebuildStorageResolveFailedEvent(database string, err error) LogEvent {
	return serverRuntimeEvent(EventServerDBConfigRebuildStorageResolveFailed, ServerLogDBConfigRebuildStorageResolveFailed(),
		slog.String("db", database), slog.Any("error", err))
}

// ServerDBConfigRebuildStartFailedEvent describes a search rebuild start failure.
func ServerDBConfigRebuildStartFailedEvent(database string, err error) LogEvent {
	return serverRuntimeEvent(EventServerDBConfigRebuildStartFailed, ServerLogDBConfigRebuildStartFailed(),
		slog.String("db", database), slog.Any("error", err))
}

// ServerCreateDatabaseDefensiveFixEvent describes repaired empty CREATE DATABASE output.
func ServerCreateDatabaseDefensiveFixEvent(database string) LogEvent {
	return serverRuntimeEvent(EventServerCreateDatabaseDefensiveFix, ServerLogCreateDatabaseDefensiveFix(),
		slog.String("subsystem", "create_database"), slog.String("db", database))
}

// ServerRetentionDefaultPolicyAddFailedEvent describes a default policy load failure.
func ServerRetentionDefaultPolicyAddFailedEvent(policyID string, err error) LogEvent {
	return serverRuntimeEvent(EventServerRetentionDefaultPolicyAddFailed, ServerLogRetentionDefaultPolicyAddFailed(),
		slog.String("policy_id", policyID), slog.Any("error", err))
}

// ServerGraphQLRequestEvent describes an enabled GraphQL request trace.
func ServerGraphQLRequestEvent(method, path string, duration time.Duration) LogEvent {
	return serverRuntimeEvent(EventServerGraphQLRequest, ServerLogGraphQLRequest(),
		slog.String("subsystem", "graphql"), slog.String("method", method), slog.String("path", path), slog.Duration("duration", duration))
}

// ServerQdrantGRPCEnabledEvent describes a running Qdrant-compatible gRPC server.
func ServerQdrantGRPCEnabledEvent(database, address string) LogEvent {
	return serverRuntimeEvent(EventServerQdrantGRPCEnabled, ServerLogQdrantGRPCEnabled(),
		slog.String("db", database), slog.String("addr", address))
}
