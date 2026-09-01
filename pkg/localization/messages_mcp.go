package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageMCPInvalidRequestBody      MessageID = "mcp.invalid_request_body"
	MessageMCPUnknownTool             MessageID = "mcp.unknown_tool"
	MessageMCPDatabaseExecutor        MessageID = "mcp.database_executor_unavailable"
	MessageMCPCypherExecutor          MessageID = "mcp.cypher_executor_unavailable"
	MessageMCPFieldRequired           MessageID = "mcp.field_required"
	MessageMCPInvalidLabel            MessageID = "mcp.invalid_label"
	MessageMCPStoreNodeFailed         MessageID = "mcp.store_node_failed"
	MessageMCPStoreNoNodeID           MessageID = "mcp.store_no_node_id"
	MessageMCPStoreInvalidNodeID      MessageID = "mcp.store_invalid_node_id"
	MessageMCPStoreNoExecutor         MessageID = "mcp.store_no_executor"
	MessageMCPNodeNotFound            MessageID = "mcp.node_not_found"
	MessageMCPRecallFailed            MessageID = "mcp.recall_failed"
	MessageMCPInvalidRelation         MessageID = "mcp.invalid_relation"
	MessageMCPSourceNotFound          MessageID = "mcp.source_node_not_found"
	MessageMCPTargetNotFound          MessageID = "mcp.target_node_not_found"
	MessageMCPCreateEdgeFailed        MessageID = "mcp.create_edge_failed"
	MessageMCPLinkNoEdgeID            MessageID = "mcp.link_no_edge_id"
	MessageMCPLinkInvalidEdgeID       MessageID = "mcp.link_invalid_edge_id"
	MessageMCPDeleteTaskFailed        MessageID = "mcp.delete_task_failed"
	MessageMCPTaskNotFound            MessageID = "mcp.task_not_found"
	MessageMCPUpdateTaskFailed        MessageID = "mcp.update_task_failed"
	MessageMCPFetchTaskFailed         MessageID = "mcp.fetch_task_failed"
	MessageMCPIDRequiredForDelete     MessageID = "mcp.id_required_for_delete"
	MessageMCPTitleRequiredForNew     MessageID = "mcp.title_required_for_new"
	MessageMCPCreateTaskFailed        MessageID = "mcp.create_task_failed"
	MessageMCPTaskNoID                MessageID = "mcp.task_no_id"
	MessageMCPTaskInvalidID           MessageID = "mcp.task_invalid_id"
	MessageMCPDependencyFailed        MessageID = "mcp.dependency_create_failed"
	MessageMCPListTasksFailed         MessageID = "mcp.list_tasks_failed"
	MessageMCPEmbeddingElementInvalid MessageID = "mcp.embedding_element_invalid"
	MessageMCPEmbeddingTypeInvalid    MessageID = "mcp.embedding_type_invalid"
	MessageMCPEmbeddingEmpty          MessageID = "mcp.embedding_empty"
	MessageMCPEmbeddingDimensions     MessageID = "mcp.embedding_dimensions_invalid"
	MessageMCPAuthenticationRequired  MessageID = "mcp.authentication_required"
	MessageMCPInvalidToken            MessageID = "mcp.invalid_or_expired_token"
	MessageMCPRateLimitExceeded       MessageID = "mcp.rate_limit_exceeded"
	MessageMCPNoAuthContext           MessageID = "mcp.authentication_context_missing"
	MessageMCPToolPermissionDenied    MessageID = "mcp.tool_permission_denied"
	MessageMCPCypherExecutorNil       MessageID = "mcp.cypher_executor_nil"
	MessageMCPServerAlreadyClosed     MessageID = "mcp.server_already_closed"
	MessageMCPUnknownRole             MessageID = "mcp.unknown_role"
	MessageMCPAuthenticatorMissing    MessageID = "mcp.authenticator_not_configured"
)

func MCPInvalidRequestBody() Message {
	return Message{ID: MessageMCPInvalidRequestBody, Fallback: "invalid request body"}
}
func MCPUnknownTool(name string) Message {
	return mcpDataMessage(MessageMCPUnknownTool, "unknown tool: "+name, map[string]any{"Name": name})
}
func MCPDatabaseExecutorUnavailable(database string) Message {
	return mcpDataMessage(MessageMCPDatabaseExecutor, "database scoped executor unavailable for "+strconv.Quote(database), map[string]any{"Database": database})
}
func MCPCypherExecutorUnavailable() Message {
	return Message{ID: MessageMCPCypherExecutor, Fallback: "cypher executor unavailable"}
}
func MCPFieldRequired(field string) Message {
	return mcpDataMessage(MessageMCPFieldRequired, field+" is required", map[string]any{"Field": field})
}
func MCPInvalidLabel(label string) Message {
	return mcpDataMessage(MessageMCPInvalidLabel, "invalid label: "+strconv.Quote(label)+" (must be a valid identifier)", map[string]any{"Label": label})
}
func MCPStoreNodeFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPStoreNodeFailed, "failed to store node: ", cause)
}
func MCPStoreNoNodeID() Message {
	return Message{ID: MessageMCPStoreNoNodeID, Fallback: "store returned no node id"}
}
func MCPStoreInvalidNodeID() Message {
	return Message{ID: MessageMCPStoreInvalidNodeID, Fallback: "store returned invalid node id"}
}
func MCPStoreNoExecutor() Message {
	return Message{ID: MessageMCPStoreNoExecutor, Fallback: "store failed: no database executor available (data would not persist); ensure MCP is wired to the server's default database"}
}
func MCPNodeNotFound(id string) Message {
	return mcpDataMessage(MessageMCPNodeNotFound, "node not found: "+id, map[string]any{"ID": id})
}
func MCPRecallFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPRecallFailed, "failed to recall nodes: ", cause)
}
func MCPInvalidRelation(relation string) Message {
	return mcpDataMessage(MessageMCPInvalidRelation, "invalid relation: "+strconv.Quote(relation)+" (must be a non-empty valid identifier, e.g. relates_to, depends_on)", map[string]any{"Relation": relation})
}
func MCPSourceNodeNotFound(id string) Message {
	return mcpDataMessage(MessageMCPSourceNotFound, "source node not found: "+id, map[string]any{"ID": id})
}
func MCPTargetNodeNotFound(id string) Message {
	return mcpDataMessage(MessageMCPTargetNotFound, "target node not found: "+id, map[string]any{"ID": id})
}
func MCPCreateEdgeFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPCreateEdgeFailed, "failed to create edge: ", cause)
}
func MCPLinkNoEdgeID() Message {
	return Message{ID: MessageMCPLinkNoEdgeID, Fallback: "link returned no edge id"}
}
func MCPLinkInvalidEdgeID() Message {
	return Message{ID: MessageMCPLinkInvalidEdgeID, Fallback: "link returned invalid edge id"}
}
func MCPDeleteTaskFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPDeleteTaskFailed, "failed to delete task: ", cause)
}
func MCPTaskNotFound(id string) Message {
	return mcpDataMessage(MessageMCPTaskNotFound, "task not found: "+id, map[string]any{"ID": id})
}
func MCPUpdateTaskFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPUpdateTaskFailed, "failed to update task: ", cause)
}
func MCPFetchUpdatedTaskFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPFetchTaskFailed, "failed to fetch updated task: ", cause)
}
func MCPIDRequiredForDelete() Message {
	return Message{ID: MessageMCPIDRequiredForDelete, Fallback: "id is required for delete"}
}
func MCPTitleRequiredForNewTasks() Message {
	return Message{ID: MessageMCPTitleRequiredForNew, Fallback: "title is required for new tasks"}
}
func MCPCreateTaskFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPCreateTaskFailed, "failed to create task: ", cause)
}
func MCPTaskCreateNoID() Message {
	return Message{ID: MessageMCPTaskNoID, Fallback: "task create returned no id"}
}
func MCPTaskCreateInvalidID() Message {
	return Message{ID: MessageMCPTaskInvalidID, Fallback: "task create returned invalid id"}
}
func MCPDependencyCreateFailed(id string, cause error) Message {
	return mcpDataMessage(MessageMCPDependencyFailed, "failed to create task dependency "+strconv.Quote(id)+": "+cause.Error(), map[string]any{"ID": id, "Cause": cause.Error()})
}
func MCPListTasksFailed(cause error) Message {
	return mcpCauseMessage(MessageMCPListTasksFailed, "failed to list tasks: ", cause)
}
func MCPEmbeddingElementInvalid(index int, value any) Message {
	kind := fmt.Sprintf("%T", value)
	return mcpDataMessage(MessageMCPEmbeddingElementInvalid, "invalid embedding: element "+strconv.Itoa(index)+" is not a number (got "+kind+")", map[string]any{"Index": index, "Type": kind})
}
func MCPEmbeddingTypeInvalid(value any) Message {
	kind := fmt.Sprintf("%T", value)
	return mcpDataMessage(MessageMCPEmbeddingTypeInvalid, "invalid embedding: must be an array of numbers (got "+kind+")", map[string]any{"Type": kind})
}
func MCPEmbeddingEmpty() Message {
	return Message{ID: MessageMCPEmbeddingEmpty, Fallback: "invalid embedding: cannot be empty array"}
}
func MCPEmbeddingDimensionsInvalid(expected, got int, model string) Message {
	return mcpDataMessage(
		MessageMCPEmbeddingDimensions,
		"invalid embedding dimensions: expected "+strconv.Itoa(expected)+", got "+strconv.Itoa(got)+". The configured embedding model ("+model+") requires "+strconv.Itoa(expected)+"-dimensional vectors",
		map[string]any{"Expected": expected, "Got": got, "Model": model},
	)
}
func MCPAuthenticationRequired() Message {
	return Message{ID: MessageMCPAuthenticationRequired, Fallback: "authentication required"}
}
func MCPInvalidOrExpiredToken() Message {
	return Message{ID: MessageMCPInvalidToken, Fallback: "invalid or expired token"}
}
func MCPRateLimitExceeded(limit int, period string) Message {
	return mcpDataMessage(MessageMCPRateLimitExceeded, "rate limit exceeded: "+strconv.Itoa(limit)+" requests per "+period, map[string]any{"Limit": limit, "Period": period})
}
func MCPAuthenticationContextMissing() Message {
	return Message{ID: MessageMCPNoAuthContext, Fallback: "no authentication context"}
}
func MCPToolPermissionDenied(roles any, tool string) Message {
	roleText := fmt.Sprint(roles)
	return mcpDataMessage(MessageMCPToolPermissionDenied, "permission denied: role(s) "+roleText+" cannot use tool "+tool, map[string]any{"Roles": roleText, "Tool": tool})
}

func MCPCypherExecutorNil() Message {
	return Message{ID: MessageMCPCypherExecutorNil, Fallback: "cypher executor is nil"}
}

func MCPServerAlreadyClosed() Message {
	return Message{ID: MessageMCPServerAlreadyClosed, Fallback: "server already closed"}
}

func MCPUnknownRole(role string) Message {
	return mcpDataMessage(MessageMCPUnknownRole, "unknown MCP role: "+role, map[string]any{"Role": role})
}

func MCPAuthenticatorNotConfigured() Message {
	return Message{ID: MessageMCPAuthenticatorMissing, Fallback: "authenticator not configured"}
}

func mcpCauseMessage(id MessageID, prefix string, cause error) Message {
	return mcpDataMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}
func mcpDataMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}
