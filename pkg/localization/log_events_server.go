package localization

import (
	"log/slog"
	"time"
)

// Additional server event and message identifiers cover asynchronous startup,
// plugin loading, embedding retries, RBAC loading, and HTTP serving failures.
const (
	EventServerHeimdallInitializing                    EventID = "server.heimdall.initializing"
	EventServerHeimdallProviderResolved                EventID = "server.heimdall.provider_resolved"
	EventServerHeimdallInitializationFailed            EventID = "server.heimdall.initialization_failed"
	EventServerAPOCPluginsLoading                      EventID = "server.plugins.apoc.loading"
	EventServerAPOCPluginsLoadFailed                   EventID = "server.plugins.apoc.load_failed"
	EventServerHeimdallPluginsLoading                  EventID = "server.plugins.heimdall.loading"
	EventServerHeimdallPluginsLoadFailed               EventID = "server.plugins.heimdall.load_failed"
	EventServerHeimdallPluginsDirectoryEmpty           EventID = "server.plugins.heimdall.directory_empty"
	EventServerHeimdallPluginsDirectoryDuplicate       EventID = "server.plugins.heimdall.directory_duplicate"
	EventServerHeimdallReady                           EventID = "server.heimdall.ready"
	EventServerHeimdallPluginsMissing                  EventID = "server.plugins.heimdall.missing"
	EventServerHeimdallActionRegistered                EventID = "server.heimdall.action_registered"
	EventServerSearchRerankerModelUnavailable          EventID = "server.search_rerank.model_unavailable"
	EventServerSearchRerankerHealthCheckFailed         EventID = "server.search_rerank.health_check_failed"
	EventServerEmbeddingModelLoading                   EventID = "server.embedding.model_loading"
	EventServerEmbeddingRetryLoopStopped               EventID = "server.embedding.retry_loop_stopped"
	EventServerEmbeddingCacheEnabled                   EventID = "server.embedding.cache_enabled"
	EventServerEmbeddingsReady                         EventID = "server.embedding.ready"
	EventServerEmbeddingInitializationAttemptFailed    EventID = "server.embedding.initialization_attempt_failed"
	EventServerEmbeddingInitializationRetrying         EventID = "server.embedding.initialization_retrying"
	EventServerEmbeddingInitializationRetryInterrupted EventID = "server.embedding.initialization_retry_interrupted"
	EventServerEmbeddingRetryIntervalCapped            EventID = "server.embedding.retry_interval_capped"
	EventServerRBACRolesLoadFailed                     EventID = "server.rbac.roles_load_failed"
	EventServerRBACAllowlistLoadFailed                 EventID = "server.rbac.allowlist_load_failed"
	EventServerRBACAllowlistSeedFailed                 EventID = "server.rbac.allowlist_seed_failed"
	EventServerRBACPrivilegesLoadFailed                EventID = "server.rbac.privileges_load_failed"
	EventServerRBACRoleEntitlementsLoadFailed          EventID = "server.rbac.role_entitlements_load_failed"
	EventServerDatabaseConfigStoreLoadFailed           EventID = "server.dbconfig.store_load_failed"
	EventServerHTTPServeFailed                         EventID = "server.http.serve_failed"

	MessageServerLogHeimdallInitializing                    MessageID = "server.log.heimdall_initializing"
	MessageServerLogHeimdallProviderResolved                MessageID = "server.log.heimdall_provider_resolved"
	MessageServerLogHeimdallInitializationFailed            MessageID = "server.log.heimdall_initialization_failed"
	MessageServerLogAPOCPluginsLoading                      MessageID = "server.log.apoc_plugins_loading"
	MessageServerLogAPOCPluginsLoadFailed                   MessageID = "server.log.apoc_plugins_load_failed"
	MessageServerLogHeimdallPluginsLoading                  MessageID = "server.log.heimdall_plugins_loading"
	MessageServerLogHeimdallPluginsLoadFailed               MessageID = "server.log.heimdall_plugins_load_failed"
	MessageServerLogHeimdallPluginsDirectoryEmpty           MessageID = "server.log.heimdall_plugins_directory_empty"
	MessageServerLogHeimdallPluginsDirectoryDuplicate       MessageID = "server.log.heimdall_plugins_directory_duplicate"
	MessageServerLogHeimdallReady                           MessageID = "server.log.heimdall_ready"
	MessageServerLogHeimdallPluginsMissing                  MessageID = "server.log.heimdall_plugins_missing"
	MessageServerLogHeimdallActionRegistered                MessageID = "server.log.heimdall_action_registered"
	MessageServerLogSearchRerankerModelUnavailable          MessageID = "server.log.search_reranker_model_unavailable"
	MessageServerLogSearchRerankerHealthCheckFailed         MessageID = "server.log.search_reranker_health_check_failed"
	MessageServerLogEmbeddingModelLoading                   MessageID = "server.log.embedding_model_loading"
	MessageServerLogEmbeddingRetryLoopStopped               MessageID = "server.log.embedding_retry_loop_stopped"
	MessageServerLogEmbeddingCacheEnabled                   MessageID = "server.log.embedding_cache_enabled"
	MessageServerLogEmbeddingsReady                         MessageID = "server.log.embeddings_ready"
	MessageServerLogEmbeddingInitializationAttemptFailed    MessageID = "server.log.embedding_initialization_attempt_failed"
	MessageServerLogEmbeddingInitializationRetrying         MessageID = "server.log.embedding_initialization_retrying"
	MessageServerLogEmbeddingInitializationRetryInterrupted MessageID = "server.log.embedding_initialization_retry_interrupted"
	MessageServerLogEmbeddingRetryIntervalCapped            MessageID = "server.log.embedding_retry_interval_capped"
	MessageServerLogRBACRolesLoadFailed                     MessageID = "server.log.rbac_roles_load_failed"
	MessageServerLogRBACAllowlistLoadFailed                 MessageID = "server.log.rbac_allowlist_load_failed"
	MessageServerLogRBACAllowlistSeedFailed                 MessageID = "server.log.rbac_allowlist_seed_failed"
	MessageServerLogRBACPrivilegesLoadFailed                MessageID = "server.log.rbac_privileges_load_failed"
	MessageServerLogRBACRoleEntitlementsLoadFailed          MessageID = "server.log.rbac_role_entitlements_load_failed"
	MessageServerLogDatabaseConfigStoreLoadFailed           MessageID = "server.log.database_config_store_load_failed"
	MessageServerLogHTTPServeFailed                         MessageID = "server.log.http_serve_failed"
)

// ServerLogHeimdallInitializing describes asynchronous Heimdall initialization startup.
func ServerLogHeimdallInitializing() Message {
	return Message{ID: MessageServerLogHeimdallInitializing, Fallback: "heimdall AI assistant initializing asynchronously"}
}

// ServerLogHeimdallProviderResolved describes the resolved Heimdall provider.
func ServerLogHeimdallProviderResolved() Message {
	return Message{ID: MessageServerLogHeimdallProviderResolved, Fallback: "heimdall provider resolved"}
}

// ServerLogHeimdallInitializationFailed describes a Heimdall initialization failure.
func ServerLogHeimdallInitializationFailed() Message {
	return Message{ID: MessageServerLogHeimdallInitializationFailed, Fallback: "heimdall initialization failed"}
}

// ServerLogAPOCPluginsLoading describes an APOC plugin load attempt.
func ServerLogAPOCPluginsLoading() Message {
	return Message{ID: MessageServerLogAPOCPluginsLoading, Fallback: "loading APOC plugins"}
}

// ServerLogAPOCPluginsLoadFailed describes an APOC plugin load failure.
func ServerLogAPOCPluginsLoadFailed() Message {
	return Message{ID: MessageServerLogAPOCPluginsLoadFailed, Fallback: "failed to load APOC plugins"}
}

// ServerLogHeimdallPluginsLoading describes a Heimdall plugin load attempt.
func ServerLogHeimdallPluginsLoading() Message {
	return Message{ID: MessageServerLogHeimdallPluginsLoading, Fallback: "loading Heimdall plugins"}
}

// ServerLogHeimdallPluginsLoadFailed describes a Heimdall plugin load failure.
func ServerLogHeimdallPluginsLoadFailed() Message {
	return Message{ID: MessageServerLogHeimdallPluginsLoadFailed, Fallback: "failed to load Heimdall plugins"}
}

// ServerLogHeimdallPluginsDirectoryEmpty describes an unset Heimdall plugin directory.
func ServerLogHeimdallPluginsDirectoryEmpty() Message {
	return Message{ID: MessageServerLogHeimdallPluginsDirectoryEmpty, Fallback: "heimdall plugins dir is empty"}
}

// ServerLogHeimdallPluginsDirectoryDuplicate describes duplicate plugin directories.
func ServerLogHeimdallPluginsDirectoryDuplicate() Message {
	return Message{ID: MessageServerLogHeimdallPluginsDirectoryDuplicate, Fallback: "heimdall plugins dir same as plugins dir; skipping"}
}

// ServerLogHeimdallReady describes completed Heimdall initialization.
func ServerLogHeimdallReady() Message {
	return Message{ID: MessageServerLogHeimdallReady, Fallback: "heimdall AI assistant ready"}
}

// ServerLogHeimdallPluginsMissing describes startup without loaded Heimdall plugins.
func ServerLogHeimdallPluginsMissing() Message {
	return Message{ID: MessageServerLogHeimdallPluginsMissing, Fallback: "no heimdall plugins loaded — watcher logs will be absent"}
}

// ServerLogHeimdallActionRegistered describes a registered Heimdall action.
func ServerLogHeimdallActionRegistered() Message {
	return Message{ID: MessageServerLogHeimdallActionRegistered, Fallback: "heimdall action registered"}
}

// ServerLogSearchRerankerModelUnavailable describes a local reranker load failure.
func ServerLogSearchRerankerModelUnavailable() Message {
	return Message{ID: MessageServerLogSearchRerankerModelUnavailable, Fallback: "search reranker model unavailable; stage-2 reranking disabled, RRF order only"}
}

// ServerLogSearchRerankerHealthCheckFailed describes a reranker health-check failure.
func ServerLogSearchRerankerHealthCheckFailed() Message {
	return Message{ID: MessageServerLogSearchRerankerHealthCheckFailed, Fallback: "search reranker failed health check"}
}

// ServerLogEmbeddingModelLoading describes asynchronous embedding initialization.
func ServerLogEmbeddingModelLoading() Message {
	return Message{ID: MessageServerLogEmbeddingModelLoading, Fallback: "loading embedding model"}
}

// ServerLogEmbeddingRetryLoopStopped describes retry-loop termination during shutdown.
func ServerLogEmbeddingRetryLoopStopped() Message {
	return Message{ID: MessageServerLogEmbeddingRetryLoopStopped, Fallback: "embedding init retry loop stopped: server shutting down"}
}

// ServerLogEmbeddingCacheEnabled describes enabled embedding caching.
func ServerLogEmbeddingCacheEnabled() Message {
	return Message{ID: MessageServerLogEmbeddingCacheEnabled, Fallback: "embedding cache enabled"}
}

// ServerLogEmbeddingsReady describes a ready embedding provider.
func ServerLogEmbeddingsReady() Message {
	return Message{ID: MessageServerLogEmbeddingsReady, Fallback: "embeddings ready"}
}

// ServerLogEmbeddingInitializationAttemptFailed describes a failed embedding initialization attempt.
func ServerLogEmbeddingInitializationAttemptFailed() Message {
	return Message{ID: MessageServerLogEmbeddingInitializationAttemptFailed, Fallback: "embedding init attempt failed"}
}

// ServerLogEmbeddingInitializationRetrying describes an exponential-backoff retry.
func ServerLogEmbeddingInitializationRetrying() Message {
	return Message{ID: MessageServerLogEmbeddingInitializationRetrying, Fallback: "retrying embedding init (exponential backoff)"}
}

// ServerLogEmbeddingInitializationRetryInterrupted describes a shutdown-interrupted retry.
func ServerLogEmbeddingInitializationRetryInterrupted() Message {
	return Message{ID: MessageServerLogEmbeddingInitializationRetryInterrupted, Fallback: "embedding init retry interrupted by server shutdown"}
}

// ServerLogEmbeddingRetryIntervalCapped describes transition to periodic retries.
func ServerLogEmbeddingRetryIntervalCapped() Message {
	return Message{ID: MessageServerLogEmbeddingRetryIntervalCapped, Fallback: "embedding init retry interval capped; continuing periodic retries"}
}

// ServerLogRBACRolesLoadFailed describes an RBAC role store load failure.
func ServerLogRBACRolesLoadFailed() Message {
	return Message{ID: MessageServerLogRBACRolesLoadFailed, Fallback: "failed to load RBAC roles"}
}

// ServerLogRBACAllowlistLoadFailed describes an RBAC allowlist load failure.
func ServerLogRBACAllowlistLoadFailed() Message {
	return Message{ID: MessageServerLogRBACAllowlistLoadFailed, Fallback: "failed to load RBAC allowlist"}
}

// ServerLogRBACAllowlistSeedFailed describes an RBAC allowlist seed failure.
func ServerLogRBACAllowlistSeedFailed() Message {
	return Message{ID: MessageServerLogRBACAllowlistSeedFailed, Fallback: "failed to seed RBAC allowlist"}
}

// ServerLogRBACPrivilegesLoadFailed describes an RBAC privileges load failure.
func ServerLogRBACPrivilegesLoadFailed() Message {
	return Message{ID: MessageServerLogRBACPrivilegesLoadFailed, Fallback: "failed to load RBAC privileges"}
}

// ServerLogRBACRoleEntitlementsLoadFailed describes an RBAC role entitlements load failure.
func ServerLogRBACRoleEntitlementsLoadFailed() Message {
	return Message{ID: MessageServerLogRBACRoleEntitlementsLoadFailed, Fallback: "failed to load RBAC role entitlements"}
}

// ServerLogDatabaseConfigStoreLoadFailed describes a per-database config store load failure.
func ServerLogDatabaseConfigStoreLoadFailed() Message {
	return Message{ID: MessageServerLogDatabaseConfigStoreLoadFailed, Fallback: "failed to load per-DB config store"}
}

// ServerLogHTTPServeFailed describes an unexpected HTTP serving failure.
func ServerLogHTTPServeFailed() Message {
	return Message{ID: MessageServerLogHTTPServeFailed, Fallback: "http server error"}
}

const (
	// EventServerSearchReconcileStorageUnavailable identifies a storage lookup failure during startup search reconciliation.
	EventServerSearchReconcileStorageUnavailable EventID = "server.search_reconcile.storage_unavailable"
	// EventServerSearchReconcileFailed identifies a search index startup failure during reconciliation.
	EventServerSearchReconcileFailed EventID = "server.search_reconcile.failed"
	// EventServerMCPDisabled identifies startup with MCP disabled by configuration.
	EventServerMCPDisabled EventID = "server.mcp.disabled"
	// EventServerRemoteCredentialKeyFallback identifies credential key reuse.
	EventServerRemoteCredentialKeyFallback EventID = "server.remote_credentials.key_fallback"
	// EventServerUIHeadless identifies UI disablement in headless mode.
	EventServerUIHeadless EventID = "server.ui.headless"
	// EventServerAuthenticationDisabled identifies startup without authentication.
	EventServerAuthenticationDisabled EventID = "server.auth.disabled"
	// EventServerUIInitializationFailed identifies unavailable browser UI assets.
	EventServerUIInitializationFailed EventID = "server.ui.initialization_failed"
	// EventServerUIEnabled identifies successful browser UI registration.
	EventServerUIEnabled EventID = "server.ui.enabled"
	// EventServerRateLimitEnabled identifies enabled request rate limiting.
	EventServerRateLimitEnabled EventID = "server.rate_limit.enabled"
	// EventServerGraphQLEnabled identifies successful GraphQL route registration.
	EventServerGraphQLEnabled EventID = "server.graphql.enabled"
	// EventServerHeimdallDisabled identifies startup with Heimdall disabled.
	EventServerHeimdallDisabled EventID = "server.heimdall.disabled"
	// EventServerSearchRerankDisabled identifies startup without search reranking.
	EventServerSearchRerankDisabled EventID = "server.search_rerank.disabled"
	// EventServerSearchRerankAPIURLMissing identifies an incomplete external reranker configuration.
	EventServerSearchRerankAPIURLMissing EventID = "server.search_rerank.api_url_missing"
	// EventServerSearchRerankerReady identifies a configured stage-2 reranker.
	EventServerSearchRerankerReady EventID = "server.search_rerank.ready"
	// EventServerSearchRerankerLoading identifies asynchronous local reranker loading.
	EventServerSearchRerankerLoading EventID = "server.search_rerank.loading"
	// EventServerSlowQueryLoggingEnabled identifies enabled in-process slow-query logging.
	EventServerSlowQueryLoggingEnabled EventID = "server.slow_query.enabled"
	// EventServerSlowQueryLoggingConfigured identifies file-backed slow-query logging.
	EventServerSlowQueryLoggingConfigured EventID = "server.slow_query.configured"
	// EventServerSlowQueryLogOpenFailed identifies an unavailable slow-query log file.
	EventServerSlowQueryLogOpenFailed EventID = "server.slow_query.open_failed"
	// EventServerHTTP2Enabled identifies the configured HTTP/2 transport mode.
	EventServerHTTP2Enabled                           EventID   = "server.http2.enabled"
	MessageServerLogSearchReconcileStorageUnavailable MessageID = "server.log.search_reconcile_storage_unavailable"
	MessageServerLogSearchReconcileFailed             MessageID = "server.log.search_reconcile_failed"
	MessageServerLogMCPDisabled                       MessageID = "server.log.mcp_disabled"
	MessageServerLogRemoteCredentialKeyFallback       MessageID = "server.log.remote_credentials_key_fallback"
	MessageServerLogUIHeadless                        MessageID = "server.log.ui_headless"
	MessageServerLogAuthenticationDisabled            MessageID = "server.log.authentication_disabled"
	MessageServerLogUIInitializationFailed            MessageID = "server.log.ui_initialization_failed"
	MessageServerLogUIEnabled                         MessageID = "server.log.ui_enabled"
	MessageServerLogRateLimitEnabled                  MessageID = "server.log.rate_limit_enabled"
	MessageServerLogGraphQLEnabled                    MessageID = "server.log.graphql_enabled"
	MessageServerLogHeimdallDisabled                  MessageID = "server.log.heimdall_disabled"
	MessageServerLogSearchRerankDisabled              MessageID = "server.log.search_rerank_disabled"
	MessageServerLogSearchRerankAPIURLMissing         MessageID = "server.log.search_rerank_api_url_missing"
	MessageServerLogSearchRerankerReady               MessageID = "server.log.search_reranker_ready"
	MessageServerLogSearchRerankerLoading             MessageID = "server.log.search_reranker_loading"
	MessageServerLogSlowQueryLoggingEnabled           MessageID = "server.log.slow_query_logging_enabled"
	MessageServerLogSlowQueryLoggingConfigured        MessageID = "server.log.slow_query_logging_configured"
	MessageServerLogSlowQueryLogOpenFailed            MessageID = "server.log.slow_query_log_open_failed"
	MessageServerLogHTTP2Enabled                      MessageID = "server.log.http2_enabled"
)

// ServerLogSearchReconcileStorageUnavailable describes a startup storage lookup failure.
func ServerLogSearchReconcileStorageUnavailable() Message {
	return Message{ID: MessageServerLogSearchReconcileStorageUnavailable, Fallback: "startup search reconcile: storage unavailable"}
}

// ServerLogSearchReconcileFailed describes a startup search reconciliation failure.
func ServerLogSearchReconcileFailed() Message {
	return Message{ID: MessageServerLogSearchReconcileFailed, Fallback: "startup search reconcile failed"}
}

// ServerLogMCPDisabled describes an MCP-disabled startup log message.
func ServerLogMCPDisabled() Message {
	return Message{ID: MessageServerLogMCPDisabled, Fallback: "mcp server disabled via configuration"}
}

// ServerLogRemoteCredentialKeyFallback describes a credential key reuse warning.
func ServerLogRemoteCredentialKeyFallback() Message {
	return Message{ID: MessageServerLogRemoteCredentialKeyFallback, Fallback: "remote credential encryption key fallback in use"}
}

// ServerLogUIHeadless describes UI disablement in headless mode.
func ServerLogUIHeadless() Message {
	return Message{ID: MessageServerLogUIHeadless, Fallback: "headless mode: UI disabled"}
}

// ServerLogAuthenticationDisabled describes startup without authentication.
func ServerLogAuthenticationDisabled() Message {
	return Message{ID: MessageServerLogAuthenticationDisabled, Fallback: "authentication disabled"}
}

// ServerLogUIInitializationFailed describes unavailable browser UI assets.
func ServerLogUIInitializationFailed() Message {
	return Message{ID: MessageServerLogUIInitializationFailed, Fallback: "UI initialization failed"}
}

// ServerLogUIEnabled describes successful browser UI registration.
func ServerLogUIEnabled() Message {
	return Message{ID: MessageServerLogUIEnabled, Fallback: "UI browser enabled"}
}

// ServerLogRateLimitEnabled describes enabled request rate limiting.
func ServerLogRateLimitEnabled() Message {
	return Message{ID: MessageServerLogRateLimitEnabled, Fallback: "rate limiting enabled"}
}

// ServerLogGraphQLEnabled describes successful GraphQL route registration.
func ServerLogGraphQLEnabled() Message {
	return Message{ID: MessageServerLogGraphQLEnabled, Fallback: "graphql API enabled"}
}

// ServerLogHeimdallDisabled describes startup with Heimdall disabled.
func ServerLogHeimdallDisabled() Message {
	return Message{ID: MessageServerLogHeimdallDisabled, Fallback: "heimdall AI assistant disabled"}
}

// ServerLogSearchRerankDisabled describes startup without search reranking.
func ServerLogSearchRerankDisabled() Message {
	return Message{ID: MessageServerLogSearchRerankDisabled, Fallback: "search rerank disabled"}
}

// ServerLogSearchRerankAPIURLMissing describes an incomplete external reranker configuration.
func ServerLogSearchRerankAPIURLMissing() Message {
	return Message{ID: MessageServerLogSearchRerankAPIURLMissing, Fallback: "search rerank enabled but API URL not set; stage-2 reranking disabled"}
}

// ServerLogSearchRerankerReady describes a configured stage-2 reranker.
func ServerLogSearchRerankerReady() Message {
	return Message{ID: MessageServerLogSearchRerankerReady, Fallback: "search reranker ready (stage-2 reranking enabled)"}
}

// ServerLogSearchRerankerLoading describes asynchronous local reranker loading.
func ServerLogSearchRerankerLoading() Message {
	return Message{ID: MessageServerLogSearchRerankerLoading, Fallback: "loading search reranker model"}
}

// ServerLogSlowQueryLoggingEnabled describes enabled in-process slow-query logging.
func ServerLogSlowQueryLoggingEnabled() Message {
	return Message{ID: MessageServerLogSlowQueryLoggingEnabled, Fallback: "slow query logging enabled"}
}

// ServerLogSlowQueryLoggingConfigured describes file-backed slow-query logging.
func ServerLogSlowQueryLoggingConfigured() Message {
	return Message{ID: MessageServerLogSlowQueryLoggingConfigured, Fallback: "slow query logging configured"}
}

// ServerLogSlowQueryLogOpenFailed describes an unavailable slow-query log file.
func ServerLogSlowQueryLogOpenFailed() Message {
	return Message{ID: MessageServerLogSlowQueryLogOpenFailed, Fallback: "failed to open slow query log file"}
}

// ServerLogHTTP2Enabled describes the configured HTTP/2 transport mode.
func ServerLogHTTP2Enabled() Message {
	return Message{ID: MessageServerLogHTTP2Enabled, Fallback: "HTTP/2 enabled"}
}

// ServerSearchReconcileStorageUnavailableEvent describes a storage lookup failure during startup search reconciliation.
func ServerSearchReconcileStorageUnavailableEvent(database string, err error) LogEvent {
	return LogEvent{
		ID:      EventServerSearchReconcileStorageUnavailable,
		Message: ServerLogSearchReconcileStorageUnavailable(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "search"),
			slog.String("db", database),
			slog.Any("error", err),
		},
	}
}

// ServerSearchReconcileFailedEvent describes a search index startup failure during reconciliation.
func ServerSearchReconcileFailedEvent(database string, err error) LogEvent {
	return LogEvent{
		ID:      EventServerSearchReconcileFailed,
		Message: ServerLogSearchReconcileFailed(),
		Attrs: []slog.Attr{
			slog.String("subsystem", "search"),
			slog.String("db", database),
			slog.Any("error", err),
		},
	}
}

// ServerMCPDisabledEvent describes startup with MCP disabled by configuration.
func ServerMCPDisabledEvent() LogEvent {
	return LogEvent{
		ID:      EventServerMCPDisabled,
		Message: ServerLogMCPDisabled(),
	}
}

// ServerRemoteCredentialKeyFallbackEvent describes credential key reuse.
func ServerRemoteCredentialKeyFallbackEvent(fallback, remediation string) LogEvent {
	return LogEvent{
		ID:      EventServerRemoteCredentialKeyFallback,
		Message: ServerLogRemoteCredentialKeyFallback(),
		Attrs: []slog.Attr{
			slog.String("fallback", fallback),
			slog.String("remediation", remediation),
		},
	}
}

// ServerUIHeadlessEvent describes UI disablement in headless mode.
func ServerUIHeadlessEvent() LogEvent {
	return LogEvent{ID: EventServerUIHeadless, Message: ServerLogUIHeadless()}
}

// ServerAuthenticationDisabledEvent describes startup without authentication.
func ServerAuthenticationDisabledEvent() LogEvent {
	return LogEvent{ID: EventServerAuthenticationDisabled, Message: ServerLogAuthenticationDisabled()}
}

// ServerUIInitializationFailedEvent describes unavailable browser UI assets.
func ServerUIInitializationFailedEvent(err error) LogEvent {
	return LogEvent{
		ID:      EventServerUIInitializationFailed,
		Message: ServerLogUIInitializationFailed(),
		Attrs:   []slog.Attr{slog.Any("error", err)},
	}
}

// ServerUIEnabledEvent describes successful browser UI registration.
func ServerUIEnabledEvent(route string) LogEvent {
	return LogEvent{
		ID:      EventServerUIEnabled,
		Message: ServerLogUIEnabled(),
		Attrs:   []slog.Attr{slog.String("route", route)},
	}
}

// ServerRateLimitEnabledEvent describes enabled request rate limiting.
func ServerRateLimitEnabledEvent(perMinute, perHour int, scope string) LogEvent {
	return LogEvent{
		ID:      EventServerRateLimitEnabled,
		Message: ServerLogRateLimitEnabled(),
		Attrs: []slog.Attr{
			slog.Int("per_minute", perMinute),
			slog.Int("per_hour", perHour),
			slog.String("scope", scope),
		},
	}
}

// ServerGraphQLEnabledEvent describes successful GraphQL route registration.
func ServerGraphQLEnabledEvent(route string) LogEvent {
	return LogEvent{
		ID:      EventServerGraphQLEnabled,
		Message: ServerLogGraphQLEnabled(),
		Attrs:   []slog.Attr{slog.String("route", route)},
	}
}

// ServerHeimdallDisabledEvent describes startup with Heimdall disabled.
func ServerHeimdallDisabledEvent(subsystem, overrideEnv string) LogEvent {
	return LogEvent{
		ID:      EventServerHeimdallDisabled,
		Message: ServerLogHeimdallDisabled(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("override_env", overrideEnv),
		},
	}
}

// ServerSearchRerankDisabledEvent describes startup without search reranking.
func ServerSearchRerankDisabledEvent(subsystem, overrideEnv string) LogEvent {
	return LogEvent{
		ID:      EventServerSearchRerankDisabled,
		Message: ServerLogSearchRerankDisabled(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("override_env", overrideEnv),
		},
	}
}

// ServerSearchRerankAPIURLMissingEvent describes an incomplete external reranker configuration.
func ServerSearchRerankAPIURLMissingEvent(subsystem, provider, requiredEnv string) LogEvent {
	return LogEvent{
		ID:      EventServerSearchRerankAPIURLMissing,
		Message: ServerLogSearchRerankAPIURLMissing(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("provider", provider),
			slog.String("required_env", requiredEnv),
		},
	}
}

// ServerSearchRerankerReadyExternalEvent describes a configured external reranker.
func ServerSearchRerankerReadyExternalEvent(subsystem, provider, url string) LogEvent {
	return LogEvent{
		ID:      EventServerSearchRerankerReady,
		Message: ServerLogSearchRerankerReady(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("provider", provider),
			slog.String("url", url),
		},
	}
}

// ServerSearchRerankerReadyLocalEvent describes a configured local reranker.
func ServerSearchRerankerReadyLocalEvent(subsystem, model string) LogEvent {
	return LogEvent{
		ID:      EventServerSearchRerankerReady,
		Message: ServerLogSearchRerankerReady(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("model", model),
		},
	}
}

// ServerSearchRerankerLoadingEvent describes asynchronous local reranker loading.
func ServerSearchRerankerLoadingEvent(subsystem, provider, modelPath, note string) LogEvent {
	return LogEvent{
		ID:      EventServerSearchRerankerLoading,
		Message: ServerLogSearchRerankerLoading(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("provider", provider),
			slog.String("model_path", modelPath),
			slog.String("note", note),
		},
	}
}

// ServerSlowQueryLoggingEnabledEvent describes enabled in-process slow-query logging.
func ServerSlowQueryLoggingEnabledEvent(subsystem string, threshold time.Duration) LogEvent {
	return LogEvent{
		ID:      EventServerSlowQueryLoggingEnabled,
		Message: ServerLogSlowQueryLoggingEnabled(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.Duration("threshold", threshold),
		},
	}
}

// ServerSlowQueryLoggingConfiguredEvent describes file-backed slow-query logging.
func ServerSlowQueryLoggingConfiguredEvent(subsystem, file string, threshold time.Duration) LogEvent {
	return LogEvent{
		ID:      EventServerSlowQueryLoggingConfigured,
		Message: ServerLogSlowQueryLoggingConfigured(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("file", file),
			slog.Duration("threshold", threshold),
		},
	}
}

// ServerSlowQueryLogOpenFailedEvent describes an unavailable slow-query log file.
func ServerSlowQueryLogOpenFailedEvent(subsystem, file string, err error) LogEvent {
	return LogEvent{
		ID:      EventServerSlowQueryLogOpenFailed,
		Message: ServerLogSlowQueryLogOpenFailed(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("file", file),
			slog.Any("error", err),
		},
	}
}

// ServerHTTP2EnabledEvent describes the configured HTTP/2 transport mode.
func ServerHTTP2EnabledEvent(mode, compat string) LogEvent {
	attrs := []slog.Attr{slog.String("mode", mode)}
	if compat != "" {
		attrs = append(attrs, slog.String("compat", compat))
	}
	return LogEvent{
		ID:      EventServerHTTP2Enabled,
		Message: ServerLogHTTP2Enabled(),
		Attrs:   attrs,
	}
}

func serverEvent(id EventID, message Message, attrs ...slog.Attr) LogEvent {
	return LogEvent{ID: id, Message: message, Attrs: attrs}
}

// ServerHeimdallInitializingEvent describes asynchronous Heimdall initialization startup.
func ServerHeimdallInitializingEvent() LogEvent {
	return serverEvent(EventServerHeimdallInitializing, ServerLogHeimdallInitializing(), slog.String("subsystem", "heimdall"))
}

// ServerHeimdallProviderResolvedEvent describes the resolved Heimdall provider.
func ServerHeimdallProviderResolvedEvent(provider, overrideEnv string) LogEvent {
	return serverEvent(EventServerHeimdallProviderResolved, ServerLogHeimdallProviderResolved(), slog.String("subsystem", "heimdall"), slog.String("provider", provider), slog.String("override_env", overrideEnv))
}

// ServerHeimdallInitializationFailedEvent describes a Heimdall initialization failure.
func ServerHeimdallInitializationFailedEvent(err error, remediation string) LogEvent {
	return serverEvent(EventServerHeimdallInitializationFailed, ServerLogHeimdallInitializationFailed(), slog.String("subsystem", "heimdall"), slog.Any("error", err), slog.String("remediation", remediation))
}

// ServerAPOCPluginsLoadingEvent describes an APOC plugin load attempt.
func ServerAPOCPluginsLoadingEvent(dir string) LogEvent {
	return serverEvent(EventServerAPOCPluginsLoading, ServerLogAPOCPluginsLoading(), slog.String("subsystem", "heimdall"), slog.String("dir", dir))
}

// ServerAPOCPluginsLoadFailedEvent describes an APOC plugin load failure.
func ServerAPOCPluginsLoadFailedEvent(dir string, err error) LogEvent {
	return serverEvent(EventServerAPOCPluginsLoadFailed, ServerLogAPOCPluginsLoadFailed(), slog.String("subsystem", "heimdall"), slog.String("dir", dir), slog.Any("error", err))
}

// ServerHeimdallPluginsLoadingEvent describes a Heimdall plugin load attempt.
func ServerHeimdallPluginsLoadingEvent(dir string) LogEvent {
	return serverEvent(EventServerHeimdallPluginsLoading, ServerLogHeimdallPluginsLoading(), slog.String("subsystem", "heimdall"), slog.String("dir", dir))
}

// ServerHeimdallPluginsLoadFailedEvent describes a Heimdall plugin load failure.
func ServerHeimdallPluginsLoadFailedEvent(dir string, err error) LogEvent {
	return serverEvent(EventServerHeimdallPluginsLoadFailed, ServerLogHeimdallPluginsLoadFailed(), slog.String("subsystem", "heimdall"), slog.String("dir", dir), slog.Any("error", err))
}

// ServerHeimdallPluginsDirectoryEmptyEvent describes an unset Heimdall plugin directory.
func ServerHeimdallPluginsDirectoryEmptyEvent() LogEvent {
	return serverEvent(EventServerHeimdallPluginsDirectoryEmpty, ServerLogHeimdallPluginsDirectoryEmpty(), slog.String("subsystem", "heimdall"))
}

// ServerHeimdallPluginsDirectoryDuplicateEvent describes duplicate plugin directories.
func ServerHeimdallPluginsDirectoryDuplicateEvent(heimdallDir, pluginsDir string) LogEvent {
	return serverEvent(EventServerHeimdallPluginsDirectoryDuplicate, ServerLogHeimdallPluginsDirectoryDuplicate(), slog.String("subsystem", "heimdall"), slog.String("heimdall_dir", heimdallDir), slog.String("plugins_dir", pluginsDir))
}

// ServerHeimdallReadyEvent describes completed Heimdall initialization.
func ServerHeimdallReadyEvent(model string, pluginsLoaded, actionsAvailable int, chatRoute, statusRoute string) LogEvent {
	return serverEvent(EventServerHeimdallReady, ServerLogHeimdallReady(), slog.String("subsystem", "heimdall"), slog.String("model", model), slog.Int("plugins_loaded", pluginsLoaded), slog.Int("actions_available", actionsAvailable), slog.String("bifrost_chat_route", chatRoute), slog.String("status_route", statusRoute))
}

// ServerHeimdallPluginsMissingEvent describes startup without loaded Heimdall plugins.
func ServerHeimdallPluginsMissingEvent(remediation string) LogEvent {
	return serverEvent(EventServerHeimdallPluginsMissing, ServerLogHeimdallPluginsMissing(), slog.String("subsystem", "heimdall"), slog.String("remediation", remediation))
}

// ServerHeimdallActionRegisteredEvent describes a registered Heimdall action.
func ServerHeimdallActionRegisteredEvent(action string) LogEvent {
	return serverEvent(EventServerHeimdallActionRegistered, ServerLogHeimdallActionRegistered(), slog.String("subsystem", "heimdall"), slog.String("action", action))
}

// ServerSearchRerankerModelUnavailableEvent describes a local reranker load failure.
func ServerSearchRerankerModelUnavailableEvent(err error) LogEvent {
	return serverEvent(EventServerSearchRerankerModelUnavailable, ServerLogSearchRerankerModelUnavailable(), slog.String("subsystem", "search_rerank"), slog.Any("error", err))
}

// ServerSearchRerankerHealthCheckFailedEvent describes a reranker health-check failure.
func ServerSearchRerankerHealthCheckFailedEvent(err error) LogEvent {
	return serverEvent(EventServerSearchRerankerHealthCheckFailed, ServerLogSearchRerankerHealthCheckFailed(), slog.String("subsystem", "search_rerank"), slog.Any("error", err))
}

// ServerEmbeddingModelLoadingEvent describes asynchronous embedding initialization.
func ServerEmbeddingModelLoadingEvent(model, provider, note string) LogEvent {
	return serverEvent(EventServerEmbeddingModelLoading, ServerLogEmbeddingModelLoading(), slog.String("subsystem", "embed_init"), slog.String("model", model), slog.String("provider", provider), slog.String("note", note))
}

// ServerEmbeddingRetryLoopStoppedEvent describes retry-loop termination during shutdown.
func ServerEmbeddingRetryLoopStoppedEvent() LogEvent {
	return serverEvent(EventServerEmbeddingRetryLoopStopped, ServerLogEmbeddingRetryLoopStopped(), slog.String("subsystem", "embed_init"))
}

// ServerEmbeddingCacheEnabledEvent describes enabled embedding caching.
func ServerEmbeddingCacheEnabledEvent(entries, memoryMB int) LogEvent {
	return serverEvent(EventServerEmbeddingCacheEnabled, ServerLogEmbeddingCacheEnabled(), slog.String("subsystem", "embed_init"), slog.Int("entries", entries), slog.Int("memory_mb", memoryMB))
}

// ServerEmbeddingsReadyLocalEvent describes a ready local embedding provider.
func ServerEmbeddingsReadyLocalEvent(model string, dimensions int) LogEvent {
	return serverEvent(EventServerEmbeddingsReady, ServerLogEmbeddingsReady(), slog.String("subsystem", "embed_init"), slog.String("provider", "local_gguf"), slog.String("model", model), slog.Int("dims", dimensions))
}

// ServerEmbeddingsReadyRemoteEvent describes a ready remote embedding provider.
func ServerEmbeddingsReadyRemoteEvent(provider, url, model string, dimensions int) LogEvent {
	return serverEvent(EventServerEmbeddingsReady, ServerLogEmbeddingsReady(), slog.String("subsystem", "embed_init"), slog.String("provider", provider), slog.String("url", url), slog.String("model", model), slog.Int("dims", dimensions))
}

// ServerEmbeddingInitializationAttemptFailedLocalEvent describes a failed local initialization attempt.
func ServerEmbeddingInitializationAttemptFailedLocalEvent(attempt int, model string, err error) LogEvent {
	return serverEvent(EventServerEmbeddingInitializationAttemptFailed, ServerLogEmbeddingInitializationAttemptFailed(), slog.String("subsystem", "embed_init"), slog.Int("attempt", attempt), slog.String("provider", "local"), slog.String("model", model), slog.Any("error", err))
}

// ServerEmbeddingInitializationAttemptFailedRemoteEvent describes a failed remote initialization attempt.
func ServerEmbeddingInitializationAttemptFailedRemoteEvent(attempt int, provider, model, url string, err error) LogEvent {
	return serverEvent(EventServerEmbeddingInitializationAttemptFailed, ServerLogEmbeddingInitializationAttemptFailed(), slog.String("subsystem", "embed_init"), slog.Int("attempt", attempt), slog.String("provider", provider), slog.String("model", model), slog.String("url", url), slog.Any("error", err))
}

// ServerEmbeddingInitializationRetryingEvent describes an exponential-backoff retry.
func ServerEmbeddingInitializationRetryingEvent(wait time.Duration) LogEvent {
	return serverEvent(EventServerEmbeddingInitializationRetrying, ServerLogEmbeddingInitializationRetrying(), slog.String("subsystem", "embed_init"), slog.Duration("wait", wait))
}

// ServerEmbeddingInitializationRetryInterruptedEvent describes a shutdown-interrupted retry.
func ServerEmbeddingInitializationRetryInterruptedEvent() LogEvent {
	return serverEvent(EventServerEmbeddingInitializationRetryInterrupted, ServerLogEmbeddingInitializationRetryInterrupted(), slog.String("subsystem", "embed_init"))
}

// ServerEmbeddingRetryIntervalCappedEvent describes transition to periodic retries.
func ServerEmbeddingRetryIntervalCappedEvent(interval time.Duration) LogEvent {
	return serverEvent(EventServerEmbeddingRetryIntervalCapped, ServerLogEmbeddingRetryIntervalCapped(), slog.String("subsystem", "embed_init"), slog.Duration("interval", interval))
}

// ServerRBACRolesLoadFailedEvent describes an RBAC role store load failure.
func ServerRBACRolesLoadFailedEvent(err error) LogEvent {
	return serverEvent(EventServerRBACRolesLoadFailed, ServerLogRBACRolesLoadFailed(), slog.String("subsystem", "rbac"), slog.Any("error", err))
}

// ServerRBACAllowlistLoadFailedEvent describes an RBAC allowlist load failure.
func ServerRBACAllowlistLoadFailedEvent(err error) LogEvent {
	return serverEvent(EventServerRBACAllowlistLoadFailed, ServerLogRBACAllowlistLoadFailed(), slog.String("subsystem", "rbac"), slog.Any("error", err))
}

// ServerRBACAllowlistSeedFailedEvent describes an RBAC allowlist seed failure.
func ServerRBACAllowlistSeedFailedEvent() LogEvent {
	return serverEvent(EventServerRBACAllowlistSeedFailed, ServerLogRBACAllowlistSeedFailed(), slog.String("subsystem", "rbac"))
}

// ServerRBACPrivilegesLoadFailedEvent describes an RBAC privileges load failure.
func ServerRBACPrivilegesLoadFailedEvent(err error) LogEvent {
	return serverEvent(EventServerRBACPrivilegesLoadFailed, ServerLogRBACPrivilegesLoadFailed(), slog.String("subsystem", "rbac"), slog.Any("error", err))
}

// ServerRBACRoleEntitlementsLoadFailedEvent describes an RBAC role entitlements load failure.
func ServerRBACRoleEntitlementsLoadFailedEvent(err error) LogEvent {
	return serverEvent(EventServerRBACRoleEntitlementsLoadFailed, ServerLogRBACRoleEntitlementsLoadFailed(), slog.String("subsystem", "rbac"), slog.Any("error", err))
}

// ServerDatabaseConfigStoreLoadFailedEvent describes a per-database config store load failure.
func ServerDatabaseConfigStoreLoadFailedEvent() LogEvent {
	return serverEvent(EventServerDatabaseConfigStoreLoadFailed, ServerLogDatabaseConfigStoreLoadFailed(), slog.String("subsystem", "dbconfig"))
}

// ServerHTTPServeFailedEvent describes an unexpected HTTP serving failure.
func ServerHTTPServeFailedEvent(err error) LogEvent {
	return serverEvent(EventServerHTTPServeFailed, ServerLogHTTPServeFailed(), slog.Any("error", err))
}
