package localization

import "strconv"

// MessageID is a stable, language-independent catalog key.
type MessageID string

// Message identifies localizable text and the named values used to render it.
type Message struct {
	ID          MessageID
	Fallback    string
	Data        map[string]any
	PluralCount any
}

const (
	MessageOSLanguageUndetected  MessageID = "localization.os_language_undetected"
	MessageLanguagePackMissing   MessageID = "localization.language_pack_missing"
	MessageCatalogEntryMissing   MessageID = "localization.catalog_entry_missing"
	MessageInvalidRequestBody    MessageID = "server.invalid_request_body"
	MessageInvalidJSONBody       MessageID = "server.invalid_json_body"
	MessagePostRequired          MessageID = "server.method_post_required"
	MessageGetRequired           MessageID = "server.method_get_required"
	MessageGetOrPostRequired     MessageID = "server.method_get_or_post_required"
	MessageGetOrPutRequired      MessageID = "server.method_get_or_put_required"
	MessageGetPutDeleteRequired  MessageID = "server.method_get_put_or_delete_required"
	MessagePostOrDeleteRequired  MessageID = "server.method_post_or_delete_required"
	MessageMethodNotAllowed      MessageID = "server.method_not_allowed"
	MessageRequestBodyReadFailed MessageID = "request.body_read_failed"
	MessageMCPParseError         MessageID = "mcp.parse_error"
	MessageMCPMethodNotFound     MessageID = "mcp.method_not_found"
	MessageMCPToolFailed         MessageID = "mcp.tool_execution_failed"
	MessageQdrantCollectionName  MessageID = "qdrant.collection_name_required"
	MessageQdrantFieldRequired   MessageID = "qdrant.field_required"
	MessageQdrantFieldsRequired  MessageID = "qdrant.fields_required"
	MessageQdrantDatabaseDenied  MessageID = "qdrant.database_access_denied"
	MessageQdrantDatabaseWrite   MessageID = "qdrant.database_write_denied"
	MessageQdrantPermission      MessageID = "qdrant.permission_denied"
	MessageQdrantAuthRequired    MessageID = "qdrant.authentication_required"
	MessageQdrantInvalidToken    MessageID = "qdrant.invalid_or_expired_token"
	MessageQdrantCollectionMiss  MessageID = "qdrant.collection_not_found"
	MessageQdrantSnapshotMiss    MessageID = "qdrant.snapshot_not_found"
	MessageItemsProcessed        MessageID = "localization.items_processed"
	MessageNotAuthenticated      MessageID = "security.not_authenticated"
	MessageSchemaPermission      MessageID = "security.schema_permission_required"
	MessageAdminPermission       MessageID = "security.admin_permission_required"
	MessageWritePermission       MessageID = "security.write_permission_required"
	MessageReadPermission        MessageID = "security.read_permission_required"
	MessageDatabaseNotFound      MessageID = "database.not_found"
	MessageHTTPDatabaseNotFound  MessageID = "server.database_not_found"
	MessageDatabaseAccessDenied  MessageID = "security.database_access_denied"
	MessageDatabaseWriteDenied   MessageID = "security.database_write_denied"
	MessageAuthNotConfigured     MessageID = "server.authentication_not_configured"
	MessageOAuthNotConfigured    MessageID = "server.oauth_not_configured"
	MessageHTTPNotAuthenticated  MessageID = "server.not_authenticated"
	MessageUserNotFound          MessageID = "server.user_not_found"
	MessageTransactionNotFound   MessageID = "server.transaction_not_found"
	MessageRequestFieldRequired  MessageID = "server.request_field_required"
	MessageNotFound              MessageID = "server.not_found"
	MessageInvalidGPUManager     MessageID = "server.invalid_gpu_manager_type"
	MessageGPUManagerUnavailable MessageID = "server.gpu_manager_not_initialized"
	MessageTemporalReconstruct   MessageID = "server.temporal_graph_reconstruction_unsupported"
	MessageTemporalDiff          MessageID = "server.temporal_graph_diff_unsupported"
	MessageNoAuthentication      MessageID = "security.no_authentication_provided"
	MessageInsufficientPerms     MessageID = "security.insufficient_permissions"
	MessageInternalServerError   MessageID = "server.internal_error"
	MessageBoltAuthRequired      MessageID = "bolt.authentication_required"
	MessageBoltInvalidCreds      MessageID = "bolt.invalid_credentials"
	MessageBoltInvalidToken      MessageID = "bolt.invalid_or_expired_token"
	MessageBoltAuthUnavailable   MessageID = "bolt.authentication_not_configured"
	MessageBoltUnsupportedScheme MessageID = "bolt.unsupported_auth_scheme"
	MessageBoltDatabaseLookup    MessageID = "bolt.database_not_found_with_cause"
	MessageBoltNoTransaction     MessageID = "bolt.no_transaction_to_commit"
	MessageSearcherRequired      MessageID = "search.searcher_required"
	MessageRequestRequired       MessageID = "request.required"
	MessageQueryRequired         MessageID = "search.query_required"
	MessageQueryChunkFailed      MessageID = "search.query_chunk_failed"
	MessageSearchFailed          MessageID = "search.failed"
)

// OSLanguageUndetected reports that OS language detection failed.
func OSLanguageUndetected() Message {
	return Message{ID: MessageOSLanguageUndetected, Fallback: "Unable to determine the operating system language; using English (United States)"}
}

// LanguagePackMissing reports a requested language without an installed pack.
func LanguagePackMissing(requested, resolved, source string) Message {
	return Message{ID: MessageLanguagePackMissing, Fallback: "Requested language pack is unavailable", Data: map[string]any{
		"RequestedLanguage": requested,
		"ResolvedLanguage":  resolved,
		"Source":            source,
	}}
}

// CatalogEntryMissing reports a missing translated catalog entry.
func CatalogEntryMissing(language string, messageID MessageID) Message {
	return Message{ID: MessageCatalogEntryMissing, Fallback: "Catalog entry is unavailable", Data: map[string]any{
		"Language":  language,
		"MessageID": string(messageID),
	}}
}

// InvalidRequestBody identifies the common malformed-request response.
func InvalidRequestBody() Message {
	return Message{ID: MessageInvalidRequestBody, Fallback: "invalid request body"}
}

// InvalidJSONBody identifies malformed JSON in a request body.
func InvalidJSONBody() Message {
	return Message{ID: MessageInvalidJSONBody, Fallback: "invalid JSON body"}
}

// PostRequired identifies an endpoint that only accepts POST requests.
func PostRequired() Message {
	return Message{ID: MessagePostRequired, Fallback: "POST required"}
}

// GetRequired identifies an endpoint that only accepts GET requests.
func GetRequired() Message {
	return Message{ID: MessageGetRequired, Fallback: "GET required"}
}

// GetOrPostRequired identifies an endpoint that accepts GET or POST requests.
func GetOrPostRequired() Message {
	return Message{ID: MessageGetOrPostRequired, Fallback: "GET or POST required"}
}

// GetOrPutRequired identifies an endpoint that accepts GET or PUT requests.
func GetOrPutRequired() Message {
	return Message{ID: MessageGetOrPutRequired, Fallback: "GET or PUT required"}
}

// GetPutOrDeleteRequired identifies an endpoint that accepts GET, PUT, or DELETE requests.
func GetPutOrDeleteRequired() Message {
	return Message{ID: MessageGetPutDeleteRequired, Fallback: "GET, PUT, or DELETE required"}
}

// PostOrDeleteRequired identifies an endpoint that accepts POST or DELETE requests.
func PostOrDeleteRequired() Message {
	return Message{ID: MessagePostOrDeleteRequired, Fallback: "POST or DELETE required"}
}

// MethodNotAllowed identifies a request using an unsupported HTTP method.
func MethodNotAllowed() Message {
	return Message{ID: MessageMethodNotAllowed, Fallback: "method not allowed"}
}

// RequestBodyReadFailed identifies an unreadable HTTP request body.
func RequestBodyReadFailed() Message {
	return Message{ID: MessageRequestBodyReadFailed, Fallback: "failed to read request body"}
}

// MCPParseError identifies an invalid JSON-RPC request document.
func MCPParseError() Message {
	return Message{ID: MessageMCPParseError, Fallback: "Parse error"}
}

// MCPMethodNotFound identifies an unsupported JSON-RPC method.
func MCPMethodNotFound() Message {
	return Message{ID: MessageMCPMethodNotFound, Fallback: "Method not found"}
}

// MCPToolExecutionFailed identifies a failed MCP tool invocation.
func MCPToolExecutionFailed() Message {
	return Message{ID: MessageMCPToolFailed, Fallback: "Tool execution failed"}
}

// QdrantCollectionNameRequired identifies a missing Qdrant collection_name field.
func QdrantCollectionNameRequired() Message {
	return Message{ID: MessageQdrantCollectionName, Fallback: "collection_name is required"}
}

// QdrantFieldRequired identifies a missing Qdrant request field.
func QdrantFieldRequired(field string) Message {
	return Message{
		ID:       MessageQdrantFieldRequired,
		Fallback: field + " is required",
		Data:     map[string]any{"Field": field},
	}
}

// QdrantFieldsRequired identifies missing plural Qdrant request fields or values.
func QdrantFieldsRequired(fields string) Message {
	return Message{
		ID:       MessageQdrantFieldsRequired,
		Fallback: fields + " are required",
		Data:     map[string]any{"Fields": fields},
	}
}

// QdrantDatabaseAccessDenied identifies a Qdrant database authorization failure.
func QdrantDatabaseAccessDenied(name string) Message {
	return Message{
		ID:       MessageQdrantDatabaseDenied,
		Fallback: "access to database " + strconv.Quote(name) + " is not allowed",
		Data:     map[string]any{"Name": name},
	}
}

// QdrantDatabaseWriteDenied identifies a Qdrant database write authorization failure.
func QdrantDatabaseWriteDenied(name string) Message {
	return Message{
		ID:       MessageQdrantDatabaseWrite,
		Fallback: "write on database " + strconv.Quote(name) + " is not allowed",
		Data:     map[string]any{"Name": name},
	}
}

// QdrantPermissionDenied identifies a Qdrant method authorization failure.
func QdrantPermissionDenied() Message {
	return Message{ID: MessageQdrantPermission, Fallback: "permission denied"}
}

// QdrantAuthenticationRequired identifies a missing Qdrant authentication credential.
func QdrantAuthenticationRequired(cause string) Message {
	return Message{
		ID:       MessageQdrantAuthRequired,
		Fallback: "authentication required: " + cause,
		Data:     map[string]any{"Cause": cause},
	}
}

// QdrantInvalidOrExpiredToken identifies a rejected Qdrant bearer token.
func QdrantInvalidOrExpiredToken() Message {
	return Message{ID: MessageQdrantInvalidToken, Fallback: "invalid or expired token"}
}

// QdrantCollectionNotFound identifies a missing Qdrant collection.
func QdrantCollectionNotFound(name string) Message {
	return Message{
		ID:       MessageQdrantCollectionMiss,
		Fallback: "collection " + strconv.Quote(name) + " not found",
		Data:     map[string]any{"Name": name},
	}
}

// QdrantSnapshotNotFound identifies a missing Qdrant snapshot.
func QdrantSnapshotNotFound(name string) Message {
	return Message{
		ID:       MessageQdrantSnapshotMiss,
		Fallback: "snapshot " + strconv.Quote(name) + " not found",
		Data:     map[string]any{"Name": name},
	}
}

// ItemsProcessed demonstrates locale-aware plural selection.
func ItemsProcessed(count int) Message {
	return Message{
		ID:          MessageItemsProcessed,
		Fallback:    "items processed",
		Data:        map[string]any{"Count": count},
		PluralCount: count,
	}
}

// NotAuthenticated identifies an unauthenticated protocol request.
func NotAuthenticated() Message {
	return Message{ID: MessageNotAuthenticated, Fallback: "Not authenticated"}
}

// SchemaPermissionRequired identifies missing schema permission.
func SchemaPermissionRequired() Message {
	return Message{ID: MessageSchemaPermission, Fallback: "Schema operations require schema permission"}
}

// AdminPermissionRequired identifies missing administrator permission.
func AdminPermissionRequired() Message {
	return Message{ID: MessageAdminPermission, Fallback: "Admin operations require admin permission"}
}

// WritePermissionRequired identifies missing write permission.
func WritePermissionRequired() Message {
	return Message{ID: MessageWritePermission, Fallback: "Write operations require write permission"}
}

// ReadPermissionRequired identifies missing read permission.
func ReadPermissionRequired() Message {
	return Message{ID: MessageReadPermission, Fallback: "Read operations require read permission"}
}

// DatabaseNotFound identifies a requested database that does not exist.
func DatabaseNotFound(name string) Message {
	return Message{
		ID:       MessageDatabaseNotFound,
		Fallback: "Database '" + name + "' does not exist",
		Data:     map[string]any{"Name": name},
	}
}

// HTTPDatabaseNotFound identifies the legacy HTTP/Neo4j database lookup response.
func HTTPDatabaseNotFound(name string) Message {
	return Message{
		ID:       MessageHTTPDatabaseNotFound,
		Fallback: "Database '" + name + "' not found",
		Data:     map[string]any{"Name": name},
	}
}

// DatabaseAccessDenied identifies an authorization failure for a database.
func DatabaseAccessDenied(name string) Message {
	return Message{
		ID:       MessageDatabaseAccessDenied,
		Fallback: "Access to database '" + name + "' is not allowed.",
		Data:     map[string]any{"Name": name},
	}
}

// DatabaseWriteDenied identifies an authorization failure for writing to a database.
func DatabaseWriteDenied(name string) Message {
	return Message{
		ID:       MessageDatabaseWriteDenied,
		Fallback: "Write on database '" + name + "' is not allowed.",
		Data:     map[string]any{"Name": name},
	}
}

// AuthenticationNotConfigured identifies an unavailable HTTP authentication service.
func AuthenticationNotConfigured() Message {
	return Message{ID: MessageAuthNotConfigured, Fallback: "authentication not configured"}
}

// OAuthNotConfigured identifies an unavailable OAuth integration.
func OAuthNotConfigured() Message {
	return Message{ID: MessageOAuthNotConfigured, Fallback: "OAuth not configured"}
}

// HTTPNotAuthenticated identifies an unauthenticated HTTP request.
func HTTPNotAuthenticated() Message {
	return Message{ID: MessageHTTPNotAuthenticated, Fallback: "not authenticated"}
}

// UserNotFound identifies an HTTP user lookup failure.
func UserNotFound() Message {
	return Message{ID: MessageUserNotFound, Fallback: "user not found"}
}

// TransactionNotFound identifies an HTTP transaction lookup failure.
func TransactionNotFound() Message {
	return Message{ID: MessageTransactionNotFound, Fallback: "transaction not found"}
}

// RequestFieldRequired identifies a missing HTTP request field.
func RequestFieldRequired(field string) Message {
	return Message{
		ID:       MessageRequestFieldRequired,
		Fallback: field + " is required",
		Data:     map[string]any{"Field": field},
	}
}

// NotFound identifies a generic HTTP resource lookup failure.
func NotFound() Message {
	return Message{ID: MessageNotFound, Fallback: "not found"}
}

// InvalidGPUManagerType identifies an incompatible GPU manager implementation.
func InvalidGPUManagerType() Message {
	return Message{ID: MessageInvalidGPUManager, Fallback: "invalid GPU manager type"}
}

// GPUManagerNotInitialized identifies an unavailable GPU manager.
func GPUManagerNotInitialized() Message {
	return Message{ID: MessageGPUManagerUnavailable, Fallback: "GPU manager not initialized"}
}

// TemporalGraphReconstructionUnsupported identifies unavailable temporal reconstruction support.
func TemporalGraphReconstructionUnsupported() Message {
	return Message{ID: MessageTemporalReconstruct, Fallback: "temporal graph reconstruction is not supported by the configured storage engine"}
}

// TemporalGraphDiffUnsupported identifies unavailable temporal diff support.
func TemporalGraphDiffUnsupported() Message {
	return Message{ID: MessageTemporalDiff, Fallback: "temporal graph diff is not supported by the configured storage engine"}
}

// NoAuthenticationProvided identifies a request without credentials.
func NoAuthenticationProvided() Message {
	return Message{ID: MessageNoAuthentication, Fallback: "No authentication provided"}
}

// InsufficientPermissions identifies a request lacking required permissions.
func InsufficientPermissions() Message {
	return Message{ID: MessageInsufficientPerms, Fallback: "insufficient permissions"}
}

// InternalServerError identifies an unexpected HTTP server failure.
func InternalServerError() Message {
	return Message{ID: MessageInternalServerError, Fallback: "internal server error"}
}

// BoltAuthenticationRequired identifies a Bolt HELLO without required credentials.
func BoltAuthenticationRequired() Message {
	return Message{ID: MessageBoltAuthRequired, Fallback: "Authentication required"}
}

// BoltInvalidCredentials identifies rejected Bolt basic credentials.
func BoltInvalidCredentials() Message {
	return Message{ID: MessageBoltInvalidCreds, Fallback: "Invalid credentials"}
}

// BoltInvalidOrExpiredToken identifies a rejected Bolt bearer token.
func BoltInvalidOrExpiredToken() Message {
	return Message{ID: MessageBoltInvalidToken, Fallback: "Invalid or expired token"}
}

// BoltAuthenticationNotConfigured identifies a server requiring auth without an authenticator.
func BoltAuthenticationNotConfigured() Message {
	return Message{ID: MessageBoltAuthUnavailable, Fallback: "Authentication required but not configured"}
}

// BoltUnsupportedAuthScheme identifies an unsupported Bolt HELLO authentication scheme.
func BoltUnsupportedAuthScheme(scheme string) Message {
	return Message{
		ID:       MessageBoltUnsupportedScheme,
		Fallback: "Unsupported auth scheme: " + scheme,
		Data:     map[string]any{"Scheme": scheme},
	}
}

// BoltDatabaseNotFoundWithCause identifies a Bolt database lookup failure with diagnostic detail.
func BoltDatabaseNotFoundWithCause(name string, cause error) Message {
	causeText := cause.Error()
	return Message{
		ID:       MessageBoltDatabaseLookup,
		Fallback: "Database '" + name + "' not found: " + causeText,
		Data:     map[string]any{"Name": name, "Cause": causeText},
	}
}

// BoltNoTransactionToCommit identifies COMMIT without an active transaction.
func BoltNoTransactionToCommit() Message {
	return Message{ID: MessageBoltNoTransaction, Fallback: "No transaction to commit"}
}

// SearcherRequired identifies missing search-service configuration.
func SearcherRequired() Message {
	return Message{ID: MessageSearcherRequired, Fallback: "searcher is required"}
}

// RequestRequired identifies a missing request payload.
func RequestRequired() Message {
	return Message{ID: MessageRequestRequired, Fallback: "request is required"}
}

// QueryRequired identifies a missing search query.
func QueryRequired() Message {
	return Message{ID: MessageQueryRequired, Fallback: "query is required"}
}

// QueryChunkFailed identifies query chunking failure.
func QueryChunkFailed(cause error) Message {
	return Message{ID: MessageQueryChunkFailed, Fallback: "failed to chunk query: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchFailed identifies search execution failure.
func SearchFailed(cause error) Message {
	return Message{ID: MessageSearchFailed, Fallback: "search: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}
