package localization

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
	MessagePostRequired          MessageID = "server.method_post_required"
	MessageGetOrPostRequired     MessageID = "server.method_get_or_post_required"
	MessageRequestBodyReadFailed MessageID = "request.body_read_failed"
	MessageMCPParseError         MessageID = "mcp.parse_error"
	MessageMCPMethodNotFound     MessageID = "mcp.method_not_found"
	MessageMCPToolFailed         MessageID = "mcp.tool_execution_failed"
	MessageQdrantCollectionName  MessageID = "qdrant.collection_name_required"
	MessageItemsProcessed        MessageID = "localization.items_processed"
	MessageNotAuthenticated      MessageID = "security.not_authenticated"
	MessageSchemaPermission      MessageID = "security.schema_permission_required"
	MessageAdminPermission       MessageID = "security.admin_permission_required"
	MessageWritePermission       MessageID = "security.write_permission_required"
	MessageReadPermission        MessageID = "security.read_permission_required"
	MessageDatabaseNotFound      MessageID = "database.not_found"
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

// PostRequired identifies an endpoint that only accepts POST requests.
func PostRequired() Message {
	return Message{ID: MessagePostRequired, Fallback: "POST required"}
}

// GetOrPostRequired identifies an endpoint that accepts GET or POST requests.
func GetOrPostRequired() Message {
	return Message{ID: MessageGetOrPostRequired, Fallback: "GET or POST required"}
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
