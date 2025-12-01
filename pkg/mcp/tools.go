// Package mcp provides tool definitions for the NornicDB MCP server.
package mcp

import (
	"encoding/json"
)

// GetToolDefinitions returns all 6 MCP tool definitions with JSON schemas.
// These tools are designed for LLM-native usage with:
// - Verb-noun naming (clear intent)
// - Minimal required parameters
// - Smart defaults
// - Rich, actionable responses
//
// Note: File indexing (index/unindex) is handled by Mimir (the intelligence layer).
// NornicDB is the storage/embedding layer - it receives already-processed content.
func GetToolDefinitions() []Tool {
	return []Tool{
		getStoreTool(),
		getRecallTool(),
		getDiscoverTool(),
		getLinkTool(),
		getTaskTool(),
		getTasksTool(),
	}
}

// getStoreTool returns the store tool definition
func getStoreTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"content": map[string]interface{}{
				"type":        "string",
				"description": "The main content to store. This is what will be remembered.",
			},
			"type": map[string]interface{}{
				"type":        "string",
				"description": "Node type for categorization.",
				"enum":        []string{"memory", "decision", "concept", "task", "note", "file", "code", "conversation", "project", "person"},
				"default":     "memory",
			},
			"title": map[string]interface{}{
				"type":        "string",
				"description": "Optional title. Auto-generated from content if omitted.",
			},
			"tags": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string"},
				"description": "Tags for organization and filtering.",
			},
			"metadata": map[string]interface{}{
				"type":                 "object",
				"description":          "Additional key-value metadata.",
				"additionalProperties": true,
			},
		},
		"required": []string{"content"},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "store",
		Description: `Store a piece of knowledge, memory, decision, or any information as a node in the graph.
Returns node ID for future reference. Automatically generates embeddings for semantic search.
Use this whenever you want to remember something.

Examples:
- store(content="PostgreSQL is our primary database", type="decision")
- store(content="User prefers dark mode", type="memory", tags=["preferences"])`,
		InputSchema: schemaJSON,
	}
}

// getRecallTool returns the recall tool definition
func getRecallTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type":        "string",
				"description": "Node ID to retrieve directly. If provided, ignores other filters.",
			},
			"type": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string"},
				"description": "Filter by node types.",
			},
			"tags": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string"},
				"description": "Filter by tags (nodes must have ALL specified tags).",
			},
			"since": map[string]interface{}{
				"type":        "string",
				"format":      "date-time",
				"description": "Filter by creation time (RFC3339 format).",
			},
			"limit": map[string]interface{}{
				"type":        "integer",
				"description": "Maximum number of results.",
				"default":     10,
				"minimum":     1,
				"maximum":     100,
			},
		},
		"required": []string{},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "recall",
		Description: `Retrieve specific knowledge by ID, or search by criteria (type, tags, date range).
Use when you know WHAT you're looking for. For semantic "find similar" use discover instead.

Examples:
- recall(id="node-abc123")
- recall(type=["decision"], tags=["database"])
- recall(since="2024-11-01T00:00:00Z", limit=20)`,
		InputSchema: schemaJSON,
	}
}

// getDiscoverTool returns the discover tool definition
func getDiscoverTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{
				"type":        "string",
				"description": "Natural language search query. Searches by MEANING, not exact keywords.",
			},
			"type": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string"},
				"description": "Filter results by node types.",
			},
			"limit": map[string]interface{}{
				"type":        "integer",
				"description": "Maximum number of results.",
				"default":     10,
				"minimum":     1,
				"maximum":     100,
			},
			"min_similarity": map[string]interface{}{
				"type":        "number",
				"description": "Minimum cosine similarity threshold (0.0-1.0). Lower = more results.",
				"default":     0.70,
				"minimum":     0.0,
				"maximum":     1.0,
			},
			"depth": map[string]interface{}{
				"type":        "integer",
				"description": "Graph traversal depth for related context (1-3). Higher = more context but slower.",
				"default":     1,
				"minimum":     1,
				"maximum":     3,
			},
		},
		"required": []string{"query"},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "discover",
		Description: `Find knowledge by MEANING, not exact keywords. Uses vector embeddings to find semantically
similar content. Automatically falls back to keyword search if embeddings disabled.
Use when you're asking "what do we know about X?" or "find similar to Y".

Examples:
- discover(query="database connection pooling")
- discover(query="authentication bugs", type=["code", "decision"], depth=2)
- discover(query="user preferences", min_similarity=0.65)`,
		InputSchema: schemaJSON,
	}
}

// getLinkTool returns the link tool definition
func getLinkTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"from": map[string]interface{}{
				"type":        "string",
				"description": "Source node ID.",
			},
			"to": map[string]interface{}{
				"type":        "string",
				"description": "Target node ID.",
			},
			"relation": map[string]interface{}{
				"type":        "string",
				"description": "Relationship type.",
				"enum":        []string{"depends_on", "relates_to", "implements", "caused_by", "blocks", "contains", "references", "uses", "evolved_from", "contradicts"},
			},
			"strength": map[string]interface{}{
				"type":        "number",
				"description": "Relationship strength (0.0-1.0).",
				"default":     1.0,
				"minimum":     0.0,
				"maximum":     1.0,
			},
			"metadata": map[string]interface{}{
				"type":                 "object",
				"description":          "Additional edge properties.",
				"additionalProperties": true,
			},
		},
		"required": []string{"from", "to", "relation"},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "link",
		Description: `Create a relationship between two nodes. Use this to connect related knowledge,
show dependencies, or build knowledge graphs. Relationships have types and properties.

Examples:
- link(from="task-123", to="task-456", relation="depends_on")
- link(from="decision-a", to="decision-b", relation="contradicts", strength=0.8)
- link(from="file-1", to="module-2", relation="contains")`,
		InputSchema: schemaJSON,
	}
}

// getTaskTool returns the task tool definition
func getTaskTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type":        "string",
				"description": "Task ID for update/complete/delete. Omit for creating new tasks.",
			},
			"title": map[string]interface{}{
				"type":        "string",
				"description": "Task title (required for create).",
			},
			"description": map[string]interface{}{
				"type":        "string",
				"description": "Detailed task description.",
			},
			"status": map[string]interface{}{
				"type":        "string",
				"description": "Task status. Omit for auto-toggle (pending→active→done).",
				"enum":        []string{"pending", "active", "done", "blocked"},
			},
			"priority": map[string]interface{}{
				"type":        "string",
				"description": "Task priority level.",
				"enum":        []string{"low", "medium", "high", "critical"},
				"default":     "medium",
			},
			"depends_on": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string"},
				"description": "IDs of tasks that must complete before this one.",
			},
			"assign": map[string]interface{}{
				"type":        "string",
				"description": "Assign to agent or person.",
			},
			"complete": map[string]interface{}{
				"type":        "boolean",
				"description": "Set to true to mark task as done. Shorthand for status='done'.",
			},
			"delete": map[string]interface{}{
				"type":        "boolean",
				"description": "Set to true to delete the task.",
			},
		},
		"required": []string{},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "task",
		Description: `Create or manage a task. Tasks are special nodes with status tracking (pending/active/done).
Automatically embedded for semantic search. Returns task with context for next steps.

Status auto-toggle: If you provide an ID without status, it advances the status:
- pending → active
- active → done

Examples:
- task(title="Implement auth", priority="high")
- task(id="task-123", complete=true)
- task(id="task-123", status="blocked")
- task(id="task-456")  # Toggle status
- task(id="task-789", delete=true)`,
		InputSchema: schemaJSON,
	}
}

// getTasksTool returns the tasks tool definition
func getTasksTool() Tool {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"status": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string", "enum": []string{"pending", "active", "done", "blocked"}},
				"description": "Filter by status.",
			},
			"priority": map[string]interface{}{
				"type":        "array",
				"items":       map[string]interface{}{"type": "string", "enum": []string{"low", "medium", "high", "critical"}},
				"description": "Filter by priority.",
			},
			"assigned_to": map[string]interface{}{
				"type":        "string",
				"description": "Filter by assignee.",
			},
			"unblocked_only": map[string]interface{}{
				"type":        "boolean",
				"description": "Only return tasks with no blocking dependencies.",
				"default":     false,
			},
			"limit": map[string]interface{}{
				"type":        "integer",
				"description": "Maximum number of results.",
				"default":     20,
				"minimum":     1,
				"maximum":     100,
			},
		},
		"required": []string{},
	}

	schemaJSON, _ := json.Marshal(schema)
	return Tool{
		Name: "tasks",
		Description: `List or query multiple tasks with filtering. Use for dashboards, status checks,
or finding work. Returns tasks sorted by priority and dependency order.

Examples:
- tasks(status=["pending"], unblocked_only=true)
- tasks(priority=["high", "critical"])
- tasks(assigned_to="agent-worker-1", limit=10)
- tasks()  # All tasks with stats`,
		InputSchema: schemaJSON,
	}
}

// ToolName constants for type-safe tool references
const (
	ToolStore    = "store"
	ToolRecall   = "recall"
	ToolDiscover = "discover"
	ToolLink     = "link"
	ToolTask     = "task"
	ToolTasks    = "tasks"
)

// AllTools returns all tool names
func AllTools() []string {
	return []string{
		ToolStore,
		ToolRecall,
		ToolDiscover,
		ToolLink,
		ToolTask,
		ToolTasks,
	}
}

// IsValidTool checks if a tool name is valid
func IsValidTool(name string) bool {
	for _, t := range AllTools() {
		if t == name {
			return true
		}
	}
	return false
}

// InferOperation determines the CRUD operation from tool and arguments
func InferOperation(tool string, args map[string]interface{}) string {
	switch tool {
	case ToolStore:
		return "create"
	case ToolRecall:
		return "read"
	case ToolDiscover:
		return "read"
	case ToolLink:
		return "create"
	case ToolTask:
		if _, hasID := args["id"]; hasID {
			if del, ok := args["delete"].(bool); ok && del {
				return "delete"
			}
			return "update"
		}
		return "create"
	case ToolTasks:
		return "read"
	default:
		return "unknown"
	}
}

// ExtractResourceType determines the resource type from arguments
func ExtractResourceType(tool string, args map[string]interface{}) string {
	switch tool {
	case ToolStore:
		if t, ok := args["type"].(string); ok {
			return t
		}
		return "memory"
	case ToolRecall, ToolDiscover:
		if types, ok := args["type"].([]interface{}); ok && len(types) > 0 {
			if t, ok := types[0].(string); ok {
				return t
			}
		}
		return "*"
	case ToolLink:
		return "edge"
	case ToolTask, ToolTasks:
		return "task"
	default:
		return "*"
	}
}
