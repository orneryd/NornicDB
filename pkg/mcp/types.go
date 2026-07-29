package mcp

import (
	"encoding/json"
	"regexp"
	"time"
)

// ============================================================================
// MCP Protocol Types
// ============================================================================

// Tool represents an MCP tool definition
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// InitRequest is the MCP initialize request
type InitRequest struct {
	ProtocolVersion string                 `json:"protocolVersion"`
	Capabilities    map[string]interface{} `json:"capabilities"`
	ClientInfo      ClientInfo             `json:"clientInfo"`
}

// ClientInfo contains client metadata
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InitResponse is the MCP initialize response
type InitResponse struct {
	ProtocolVersion string                 `json:"protocolVersion"`
	Capabilities    map[string]interface{} `json:"capabilities"`
	ServerInfo      ServerInfo             `json:"serverInfo"`
}

// ServerInfo contains server metadata
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ListToolsRequest requests available tools
type ListToolsRequest struct{}

// ListToolsResponse returns available tools
type ListToolsResponse struct {
	Tools []Tool `json:"tools"`
}

// CallToolRequest executes a tool
type CallToolRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments"`
}

// CallToolResponse returns tool execution result
type CallToolResponse struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}

// Content represents tool response content
type Content struct {
	Type string `json:"type"` // "text" or "resource"
	Text string `json:"text,omitempty"`
}

// ============================================================================
// Tool Input/Output Types
// ============================================================================

// StoreParams - Input for store tool
type StoreParams struct {
	Content  string                 `json:"content"`            // Required
	Type     string                 `json:"type,omitempty"`     // Optional, default: "memory"
	Title    string                 `json:"title,omitempty"`    // Optional, auto-generated if empty
	Tags     []string               `json:"tags,omitempty"`     // Optional
	Metadata map[string]interface{} `json:"metadata,omitempty"` // Optional, will be flattened
}

// StoreResult - Output from store tool
type StoreResult struct {
	ID          string       `json:"id"`
	Title       string       `json:"title"`
	Embedded    bool         `json:"embedded"`
	Suggestions []Suggestion `json:"suggestions,omitempty"`
	Receipt     interface{}  `json:"receipt,omitempty"`
}

// Suggestion represents a suggested related node
type Suggestion struct {
	ID         string  `json:"id"`
	Title      string  `json:"title"`
	Type       string  `json:"type"`
	Similarity float64 `json:"similarity"`
}

// RecallParams - Input for recall tool
type RecallParams struct {
	ID       string    `json:"id,omitempty"`       // Optional, if provided ignores other filters
	Type     []string  `json:"type,omitempty"`     // Optional, filter by types
	Tags     []string  `json:"tags,omitempty"`     // Optional, filter by tags
	Since    time.Time `json:"since,omitempty"`    // Optional, filter by creation time
	Limit    int       `json:"limit,omitempty"`    // Optional, default: 10
	Database string    `json:"database,omitempty"` // Optional, default: configured default database
}

// RecallResult - Output from recall tool
type RecallResult struct {
	Nodes   []Node `json:"nodes"`
	Count   int    `json:"count"`
	Related []Node `json:"related,omitempty"`
}

// DiscoverParams - Input for discover tool
type DiscoverParams struct {
	Query         string   `json:"query"`                    // Required
	Type          []string `json:"type,omitempty"`           // Optional, filter by types
	Limit         int      `json:"limit,omitempty"`          // Optional, default: 10
	MinSimilarity float64  `json:"min_similarity,omitempty"` // Optional, normalized relevance threshold (default: 0)
	Depth         int      `json:"depth,omitempty"`          // Optional, default: 1, range: 1-3
	Database      string   `json:"database,omitempty"`       // Optional, default: configured default database
}

// DiscoverResult - Output from discover tool
type DiscoverResult struct {
	Results     []SearchResult `json:"results"`
	Method      string         `json:"method"` // "vector" or "keyword"
	Total       int            `json:"total"`
	Suggestions []string       `json:"suggestions,omitempty"`
}

// SearchResult represents a search result node
type SearchResult struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Title          string                 `json:"title"`
	ContentPreview string                 `json:"content_preview,omitempty"`
	Similarity     float64                `json:"similarity"`
	Properties     map[string]interface{} `json:"properties,omitempty"`
	// Related nodes discovered via graph traversal (only populated when depth > 1)
	Related []RelatedNode `json:"related,omitempty"`
}

// RelatedNode represents a node connected to a search result via relationships.
// Provides context about how nodes are connected in the knowledge graph.
type RelatedNode struct {
	ID           string   `json:"id"`
	Type         string   `json:"type"`
	Title        string   `json:"title,omitempty"`
	Distance     int      `json:"distance"`            // Hops from the source node (1 = direct, 2 = two hops, etc.)
	Relationship string   `json:"relationship"`        // Relationship type that connects to this node
	Direction    string   `json:"direction,omitempty"` // "outgoing", "incoming", or "both"
	Path         []string `json:"path,omitempty"`      // Node IDs in the path (for depth > 1)
}

// LinkParams - Input for link tool
type LinkParams struct {
	From     string                 `json:"from"`               // Required
	To       string                 `json:"to"`                 // Required
	Relation string                 `json:"relation"`           // Required
	Strength float64                `json:"strength,omitempty"` // Optional, default: 1.0
	Metadata map[string]interface{} `json:"metadata,omitempty"` // Optional
	Database string                 `json:"database,omitempty"` // Optional, default: configured default database
}

// LinkResult - Output from link tool
type LinkResult struct {
	EdgeID    string      `json:"edge_id"`
	From      Node        `json:"from"`
	To        Node        `json:"to"`
	Suggested []Edge      `json:"suggested,omitempty"`
	Receipt   interface{} `json:"receipt,omitempty"`
}

// Note: IndexParams, IndexResult, UnindexParams, UnindexResult removed
// File indexing is handled by the application layer, not NornicDB

// TaskParams - Input for task tool
type TaskParams struct {
	ID          string   `json:"id,omitempty"`          // Optional, for update/complete
	Title       string   `json:"title,omitempty"`       // Required for create
	Description string   `json:"description,omitempty"` // Optional
	Status      string   `json:"status,omitempty"`      // Optional: pending|active|completed|blocked
	Priority    string   `json:"priority,omitempty"`    // Optional: low|medium|high|critical
	DependsOn   []string `json:"depends_on,omitempty"`  // Optional, task IDs
	Assign      string   `json:"assign,omitempty"`      // Optional, agent/person
	Database    string   `json:"database,omitempty"`    // Optional, default: configured default database
}

// TaskResult - Output from task tool
type TaskResult struct {
	Task       Node        `json:"task"`
	Blockers   []Node      `json:"blockers,omitempty"`
	Subtasks   []Node      `json:"subtasks,omitempty"`
	NextAction string      `json:"next_action,omitempty"`
	Receipt    interface{} `json:"receipt,omitempty"`
}

// TasksParams - Input for tasks tool
type TasksParams struct {
	Status        []string `json:"status,omitempty"`         // Optional, filter by status
	Priority      []string `json:"priority,omitempty"`       // Optional, filter by priority
	AssignedTo    string   `json:"assigned_to,omitempty"`    // Optional, filter by assignee
	UnblockedOnly bool     `json:"unblocked_only,omitempty"` // Optional, default: false
	Limit         int      `json:"limit,omitempty"`          // Optional, default: 20
	Database      string   `json:"database,omitempty"`       // Optional, default: configured default database
}

// TasksResult - Output from tasks tool
type TasksResult struct {
	Tasks           []Node       `json:"tasks"`
	Stats           TaskStats    `json:"stats"`
	DependencyGraph []Dependency `json:"dependency_graph,omitempty"`
	Recommended     []Node       `json:"recommended,omitempty"`
}

// TaskStats contains task statistics
type TaskStats struct {
	Total      int            `json:"total"`
	ByStatus   map[string]int `json:"by_status"`
	ByPriority map[string]int `json:"by_priority"`
}

// Dependency represents a task dependency
type Dependency struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

// ============================================================================
// Common Data Types
// ============================================================================

// Node represents a graph node
type Node struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Title      string                 `json:"title,omitempty"`
	Content    string                 `json:"content,omitempty"`
	Tags       []string               `json:"tags,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Created    time.Time              `json:"created,omitempty"`
	Updated    time.Time              `json:"updated,omitempty"`
}

// Edge represents a graph edge/relationship
type Edge struct {
	ID         string                 `json:"id"`
	From       string                 `json:"from"`
	To         string                 `json:"to"`
	Type       string                 `json:"type"`
	Strength   float64                `json:"strength,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Created    time.Time              `json:"created,omitempty"`
}

// ============================================================================
// Validation
// ============================================================================

// validIdentifier matches Cypher-style identifiers: letter or underscore, then alphanumeric/underscore.
// Keeps node types and relation types abstract (any valid identifier allowed).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var (
	// ValidTaskStatuses for task status validation
	ValidTaskStatuses = []string{
		"pending", "active", "completed", "blocked",
	}

	// ValidTaskPriorities for task priority validation
	ValidTaskPriorities = []string{
		"low", "medium", "high", "critical",
	}
)

// ============================================================================
// Helper Functions
// ============================================================================

// IsValidNodeType checks if node type is a valid identifier (abstract: any Cypher-safe label).
func IsValidNodeType(t string) bool {
	return t != "" && validIdentifier.MatchString(t)
}

// IsValidTaskStatus checks if task status is valid
func IsValidTaskStatus(s string) bool {
	for _, valid := range ValidTaskStatuses {
		if s == valid {
			return true
		}
	}
	return false
}

// IsValidTaskPriority checks if task priority is valid
func IsValidTaskPriority(p string) bool {
	for _, valid := range ValidTaskPriorities {
		if p == valid {
			return true
		}
	}
	return false
}

// IsValidRelation checks if relation type is a valid identifier (abstract: any Cypher-safe relationship type).
func IsValidRelation(r string) bool {
	return r != "" && validIdentifier.MatchString(r)
}

// DefaultIfEmpty returns default value if s is empty
func DefaultIfEmpty(s, defaultVal string) string {
	if s == "" {
		return defaultVal
	}
	return s
}

// DefaultIntIfZero returns default value if i is zero
func DefaultIntIfZero(i, defaultVal int) int {
	if i == 0 {
		return defaultVal
	}
	return i
}

// DefaultFloatIfZero returns default value if f is zero
func DefaultFloatIfZero(f, defaultVal float64) float64 {
	if f == 0 {
		return defaultVal
	}
	return f
}
