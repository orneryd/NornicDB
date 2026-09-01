// Package heimdall provides Heimdall - the cognitive guardian for NornicDB.
//
// Heimdall is named after the all-seeing Norse god who guards Bifröst.
// Like its namesake, Heimdall watches over NornicDB's cognitive subsystems,
// providing SLM (Small Language Model) management and plugin architecture.
//
// Heimdall Plugins are a DISTINCT plugin type from regular NornicDB plugins.
// They specifically enable cognitive database features that the SLM manages.
//
// Plugin Type: HeimdallPlugin
//
// Unlike regular plugins (which provide Cypher functions), Heimdall plugins provide
// actions that the SLM can invoke based on user chat requests.
//
// How it works:
//  1. User sends chat message: "Check for graph anomalies"
//  2. SLM interprets intent and maps to registered action: "heimdall_anomaly_detect"
//  3. Action handler is invoked with context
//  4. Results returned to user via chat
//
// Plugin Loading:
//
// Heimdall plugins are loaded from NORNICDB_HEIMDALL_PLUGINS_DIR (separate from regular plugins).
// Each .so plugin must export a "Plugin" variable of type HeimdallPlugin.
//
// Built-in Heimdall Plugins:
//
// Core Heimdall plugins ship with NornicDB:
//   - watcher: SLM management (heimdall_watcher_*) - the core guardian
//   - anomaly: Graph anomaly detection (heimdall_anomaly_*)
//   - health: Runtime health diagnosis (heimdall_health_*)
//   - curator: Memory curation (heimdall_curator_*)
//   - optimizer: Query optimization (heimdall_optimizer_*)
//
// Custom Heimdall Plugins:
//
// Example implementing HeimdallPlugin interface:
//
//	package main
//
//	import "github.com/orneryd/nornicdb/pkg/heimdall"
//
//	// MySubsystem implements heimdall.HeimdallPlugin
//	type MySubsystem struct{}
//
//	func (p *MySubsystem) Name() string    { return "mysubsystem" }
//	func (p *MySubsystem) Version() string { return "1.0.0" }
//	func (p *MySubsystem) Type() string    { return "heimdall" } // MUST return "heimdall"
//
//	func (p *MySubsystem) Actions() map[string]heimdall.ActionFunc {
//	    return map[string]heimdall.ActionFunc{
//	        "analyze": {
//	            Handler:     p.Analyze,
//	            Description: "Analyze custom metrics",
//	            Category:    "analysis",
//	        },
//	    }
//	}
//
//	func (p *MySubsystem) Analyze(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
//	    // Your implementation
//	    return &heimdall.ActionResult{Success: true, Message: "Done"}, nil
//	}
//
//	// Export as HeimdallPlugin type
//	var Plugin heimdall.HeimdallPlugin = &MySubsystem{}
package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"plugin"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// DefaultActionInputSchema is the MCP-compatible JSON Schema when an action
// does not declare parameters (type "object" with no required properties).
var DefaultActionInputSchema = []byte(`{"type":"object","properties":{},"additionalProperties":true}`)

// PluginType identifies the type of plugin.
const PluginTypeHeimdall = "heimdall"

// HeimdallPlugin is the interface that all Heimdall plugins must implement.
// This is a DISTINCT plugin type from regular NornicDB plugins.
//
// Regular plugins provide Cypher functions (apoc.*).
// Heimdall plugins provide SUBSYSTEM MANAGEMENT for cognitive database features.
//
// Heimdall (the guardian) uses this interface to:
//   - Query subsystem state and health
//   - Configure subsystem behavior
//   - Control subsystem lifecycle
//   - Execute subsystem actions
//   - Collect subsystem metrics
type HeimdallPlugin interface {
	// === Identity ===

	// Name returns the plugin/subsystem identifier (e.g., "anomaly", "health", "curator")
	Name() string

	// Version returns the plugin version (semver format)
	Version() string

	// Type must return "heimdall" to identify this as a Heimdall plugin
	Type() string

	// Description returns human-readable description of what this subsystem does
	Description() string

	// === Lifecycle Management ===

	// Initialize is called when the subsystem is loaded
	// Receives context for accessing database, config, etc.
	Initialize(ctx SubsystemContext) error

	// Start begins the subsystem's background operations (if any)
	Start() error

	// Stop halts the subsystem's background operations
	Stop() error

	// Shutdown is called when the subsystem is being unloaded
	Shutdown() error

	// === State & Health ===

	// Status returns current subsystem status
	Status() SubsystemStatus

	// Health returns detailed health information
	Health() SubsystemHealth

	// Metrics returns subsystem-specific metrics for the SLM to analyze
	Metrics() map[string]interface{}

	// === Configuration ===

	// Config returns current configuration
	Config() map[string]interface{}

	// Configure updates subsystem configuration
	// The SLM can use this to tune subsystem behavior
	Configure(settings map[string]interface{}) error

	// Schema returns the configuration schema (for validation)
	ConfigSchema() map[string]interface{}

	// === Actions ===

	// Actions returns all actions this subsystem provides
	// Map key is the action name (e.g., "detect"), will be prefixed as slm.{name}.{action}
	Actions() map[string]ActionFunc

	// === Data Access (for SLM reasoning) ===

	// Summary returns a text summary of current subsystem state
	// Used by SLM to understand what the subsystem is doing
	Summary() string

	// RecentEvents returns recent notable events from this subsystem
	// Used by SLM for contextual awareness
	RecentEvents(limit int) []SubsystemEvent

	// === Optional Hooks ===
	// Plugins can OPTIONALLY implement these interfaces for extended functionality:
	//   - PrePromptHook: Called before each SLM request (modify prompts)
	//   - PreExecuteHook: Called before action execution (validate/modify params)
	//   - PostExecuteHook: Called after action execution (logging/metrics)
	//   - DatabaseEventHook: Called on database operations (audit/monitoring)
	//
	// Plugins only need to implement the hooks they actually use.
	// See types.go for interface definitions.
}

// SubsystemContext is provided to plugins during initialization.
type SubsystemContext struct {
	// Config is the Heimdall configuration
	Config Config

	// Database routes Cypher/search operations across logical databases.
	// This is multi-database aware (Neo4j 4.x style) and should be used instead of
	// relying on a single default database.
	Database DatabaseRouter

	// Metrics provides runtime metrics
	Metrics MetricsReader

	// Logger for subsystem logging
	Logger SubsystemLogger

	// Bifrost provides the communication bridge to connected clients
	// Plugins can use this to send messages, notifications, and request input
	Bifrost BifrostBridge

	// Heimdall provides autonomous action invocation for plugins.
	// Plugins can use this to trigger actions or send prompts to the SLM
	// based on accumulated events or other triggers.
	Heimdall HeimdallInvoker
}

// HeimdallInvoker allows plugins to autonomously trigger SLM actions.
// This enables event-driven automation where plugins can analyze accumulated
// events and trigger appropriate responses.
//
// Example: A security plugin monitors failed auth events and after N failures
// triggers "heimdall_security_analyze" to investigate.
type HeimdallInvoker interface {
	// InvokeAction directly invokes a registered action by name.
	// The action must be registered (e.g., "heimdall_watcher_status").
	// Results are returned synchronously.
	//
	// Example:
	//   result, err := ctx.Heimdall.InvokeAction("heimdall_anomaly_detect", map[string]interface{}{
	//       "threshold": 0.8,
	//   })
	InvokeAction(ctx context.Context, action string, params map[string]interface{}) (*ActionResult, error)

	// SendPrompt sends a natural language prompt to the SLM for processing.
	// The SLM will interpret the prompt and may invoke registered actions.
	// Results are returned after the SLM processes the request.
	//
	// Example:
	//   result, err := ctx.Heimdall.SendPrompt("Analyze recent error patterns")
	SendPrompt(ctx context.Context, prompt string) (*ActionResult, error)

	// InvokeActionAsync invokes an action without waiting for result.
	// Use this for fire-and-forget scenarios where you don't need the result.
	// Results will be broadcast via Bifrost to connected clients.
	InvokeActionAsync(action string, params map[string]interface{})

	// SendPromptAsync sends a prompt without waiting for result.
	// Results will be broadcast via Bifrost to connected clients.
	SendPromptAsync(prompt string)

	// SendRawPrompt sends a prompt directly to the LLM without action routing context.
	// Use this for synthesis or direct question-answering where you don't want
	// the SLM to try parsing actions from the response.
	//
	// Example:
	//   result, err := ctx.Heimdall.SendRawPrompt("Answer this question: How many nodes?")
	SendRawPrompt(ctx context.Context, prompt string) (*ActionResult, error)
}

// NoOpHeimdallInvoker is a no-op implementation when Heimdall is not available.
type NoOpHeimdallInvoker struct{}

func (n *NoOpHeimdallInvoker) InvokeAction(ctx context.Context, action string, params map[string]interface{}) (*ActionResult, error) {
	_ = ctx
	return &ActionResult{Success: false, Message: "Heimdall not available"}, nil
}
func (n *NoOpHeimdallInvoker) SendPrompt(ctx context.Context, prompt string) (*ActionResult, error) {
	_ = ctx
	return &ActionResult{Success: false, Message: "Heimdall not available"}, nil
}
func (n *NoOpHeimdallInvoker) SendRawPrompt(ctx context.Context, prompt string) (*ActionResult, error) {
	_ = ctx
	return &ActionResult{Success: false, Message: "Heimdall not available"}, nil
}
func (n *NoOpHeimdallInvoker) InvokeActionAsync(action string, params map[string]interface{}) {}
func (n *NoOpHeimdallInvoker) SendPromptAsync(prompt string)                                  {}

// LiveHeimdallInvoker is the production implementation of HeimdallInvoker.
// It uses the SubsystemManager to invoke actions and can optionally use
// a Generator for SLM prompt processing.
type LiveHeimdallInvoker struct {
	manager   *SubsystemManager
	generator Generator
	bifrost   BifrostBridge
	database  DatabaseRouter
	metrics   MetricsReader
}

// NewLiveHeimdallInvoker creates a new invoker with the required dependencies.
func NewLiveHeimdallInvoker(manager *SubsystemManager, generator Generator, bifrost BifrostBridge, database DatabaseRouter, metrics MetricsReader) *LiveHeimdallInvoker {
	return &LiveHeimdallInvoker{
		manager:   manager,
		generator: generator,
		bifrost:   bifrost,
		database:  database,
		metrics:   metrics,
	}
}

// InvokeAction directly invokes a registered action.
func (h *LiveHeimdallInvoker) InvokeAction(ctx context.Context, action string, params map[string]interface{}) (*ActionResult, error) {
	actionCtx := ActionContext{
		Context:  ctx,
		Params:   params,
		Bifrost:  h.bifrost,
		Database: h.database,
		Metrics:  h.metrics,
	}
	return ExecuteAction(action, actionCtx)
}

// SendPrompt sends a prompt to the SLM and processes the response.
func (h *LiveHeimdallInvoker) SendPrompt(ctx context.Context, prompt string) (*ActionResult, error) {
	if h.generator == nil {
		return &ActionResult{Success: false, Message: "SLM not available"}, nil
	}

	// Build system prompt with available actions
	actionPrompt := ActionPrompt() // Use the global action prompt generator
	fullPrompt := actionPrompt + "\n\nUser: " + prompt

	// Generate response from SLM
	response, err := h.generator.Generate(ctx, fullPrompt, DefaultGenerateParams())
	if err != nil {
		return &ActionResult{Success: false, Message: fmt.Sprintf("SLM error: %v", err)}, nil
	}

	// Try to parse as action
	if parsedAction := tryParseActionResponse(response); parsedAction != nil {
		return h.InvokeAction(ctx, parsedAction.Action, parsedAction.Params)
	}

	// Return raw response if not an action
	return &ActionResult{
		Success: true,
		Message: response,
	}, nil
}

// SendRawPrompt sends a prompt directly to the SLM without action routing context.
// This is used for synthesis where we just want the LLM to answer a question,
// not try to parse and route actions.
func (h *LiveHeimdallInvoker) SendRawPrompt(ctx context.Context, prompt string) (*ActionResult, error) {
	if h.generator == nil {
		return &ActionResult{Success: false, Message: "SLM not available"}, nil
	}

	// Generate response from SLM directly - no action routing prefix
	response, err := h.generator.Generate(ctx, prompt, DefaultGenerateParams())
	if err != nil {
		return &ActionResult{Success: false, Message: fmt.Sprintf("SLM error: %v", err)}, nil
	}

	return &ActionResult{
		Success: true,
		Message: response,
	}, nil
}

// InvokeActionAsync invokes an action asynchronously, broadcasting results via Bifrost.
func (h *LiveHeimdallInvoker) InvokeActionAsync(action string, params map[string]interface{}) {
	go func() {
		result, err := h.InvokeAction(context.Background(), action, params)
		if h.bifrost != nil && h.bifrost.IsConnected() {
			if err != nil {
				h.bifrost.SendNotification("error", "Action Failed", err.Error())
			} else if result != nil {
				h.bifrost.SendNotification("info", "Action Complete", result.Message)
			}
		}
	}()
}

// SendPromptAsync sends a prompt asynchronously, broadcasting results via Bifrost.
func (h *LiveHeimdallInvoker) SendPromptAsync(prompt string) {
	go func() {
		result, err := h.SendPrompt(context.Background(), prompt)
		if h.bifrost != nil && h.bifrost.IsConnected() {
			if err != nil {
				h.bifrost.SendNotification("error", "Prompt Failed", err.Error())
			} else if result != nil {
				h.bifrost.SendNotification("info", "Heimdall Response", result.Message)
			}
		}
	}()
}

// parsedActionCmd is used internally to parse SLM action responses.
type parsedActionCmd struct {
	Action string                 `json:"action"`
	Params map[string]interface{} `json:"params"`
}

// tryParseActionResponse attempts to parse an action from SLM response.
func tryParseActionResponse(response string) *parsedActionCmd {
	// Simple JSON extraction - look for {"action": ...}
	start := -1
	for i, c := range response {
		if c == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return nil
	}

	// Find matching closing brace
	depth := 0
	end := -1
	for i := start; i < len(response); i++ {
		if response[i] == '{' {
			depth++
		} else if response[i] == '}' {
			depth--
			if depth == 0 {
				end = i + 1
				break
			}
		}
	}
	if end < 0 {
		return nil
	}

	jsonStr := response[start:end]
	var action parsedActionCmd
	if err := json.Unmarshal([]byte(jsonStr), &action); err != nil {
		return nil
	}
	if action.Action == "" {
		return nil
	}
	return &action
}

// BifrostBridge is the interface for plugins to communicate via Bifrost.
// Named after the rainbow bridge connecting Asgard to other realms.
type BifrostBridge interface {
	// SendMessage sends a message to connected Bifrost clients.
	// The message appears as a system message in the chat.
	SendMessage(msg string) error

	// SendNotification sends a notification with a specific type.
	// Types: "info", "warning", "error", "success"
	SendNotification(notifType, title, message string) error

	// Broadcast sends a message to all connected clients.
	// Useful for system-wide announcements.
	Broadcast(msg string) error

	// RequestConfirmation asks the user to confirm an action.
	// Returns true if user confirms, false if they decline or timeout.
	// The action parameter describes what needs confirmation.
	RequestConfirmation(action string) (bool, error)

	// IsConnected returns true if there are active Bifrost connections.
	IsConnected() bool

	// ConnectionCount returns the number of active Bifrost connections.
	ConnectionCount() int
}

// NoOpBifrost is a no-op implementation for when Bifrost is not available.
type NoOpBifrost struct{}

func (n *NoOpBifrost) SendMessage(msg string) error                    { return nil }
func (n *NoOpBifrost) SendNotification(t, title, msg string) error     { return nil }
func (n *NoOpBifrost) Broadcast(msg string) error                      { return nil }
func (n *NoOpBifrost) RequestConfirmation(action string) (bool, error) { return false, nil }
func (n *NoOpBifrost) IsConnected() bool                               { return false }
func (n *NoOpBifrost) ConnectionCount() int                            { return 0 }

// SubsystemLogger is the logging interface for subsystems.
type SubsystemLogger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// SubsystemStatus represents the current state of a subsystem.
type SubsystemStatus string

const (
	StatusUninitialized SubsystemStatus = "uninitialized"
	StatusInitializing  SubsystemStatus = "initializing"
	StatusReady         SubsystemStatus = "ready"
	StatusRunning       SubsystemStatus = "running"
	StatusStopping      SubsystemStatus = "stopping"
	StatusStopped       SubsystemStatus = "stopped"
	StatusError         SubsystemStatus = "error"
)

// SubsystemHealth contains detailed health information.
type SubsystemHealth struct {
	Status    SubsystemStatus        `json:"status"`
	Healthy   bool                   `json:"healthy"`
	Message   string                 `json:"message,omitempty"`
	LastCheck time.Time              `json:"last_check"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// SubsystemEvent represents a notable event from a subsystem.
type SubsystemEvent struct {
	Time    time.Time              `json:"time"`
	Type    string                 `json:"type"` // "info", "warning", "error", "action"
	Message string                 `json:"message"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

// ActionFunc represents an action function provided by an SLM plugin.
// Aligned with MCP (Model Context Protocol) tool format: name, description, inputSchema.
// Invocation uses the same shape as MCP tools/call: action name + params (arguments).
type ActionFunc struct {
	Name        string                                         // Full name: heimdall.{plugin}.{action} (MCP tool "name")
	Handler     func(ctx ActionContext) (*ActionResult, error) // The action handler
	Description string                                         // Human-readable description (MCP "description")
	Category    string                                         // Grouping: monitoring, optimization, curation
	// InputSchema is optional JSON Schema for parameters (MCP "inputSchema").
	// When nil or empty, ActionsAsMCPTools() uses DefaultActionInputSchema.
	InputSchema json.RawMessage
}

// ActionContext provides context for action execution.
// Passed to handlers when actions are invoked.
type ActionContext struct {
	context.Context

	// UserMessage is what the user said to trigger this action
	UserMessage string

	// Params extracted from user message by SLM
	Params map[string]interface{}

	// Database routes Cypher/search operations across logical databases.
	Database DatabaseRouter

	// Metrics provides runtime metrics
	Metrics MetricsReader

	// Bifrost provides communication bridge to the user
	// Use this to send progress updates, request confirmation, etc.
	Bifrost BifrostBridge

	// PrincipalRoles are the authenticated principal's role names (from request context).
	// Plugins can use this with DatabaseAccessMode and ResolvedAccess to enforce per-DB access.
	PrincipalRoles []string

	// DatabaseAccessMode is the principal's per-database see/access mode (from request context).
	// Use CanAccessDatabase(dbName) before running Cypher against a database.
	DatabaseAccessMode auth.DatabaseAccessMode

	// ResolvedAccess returns per-database read/write for the principal (from request context).
	// Use for mutation checks: ResolvedAccess(dbName).Write before CREATE/DELETE/SET/etc.
	ResolvedAccess func(dbName string) auth.ResolvedAccess
}

// ActionResult is the outcome of action execution.
type ActionResult struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

// FormatActionResultForModel formats an action result for the LLM to read (any provider).
// Used in the agentic loop so the model can infer and format the final response.
func FormatActionResultForModel(result *ActionResult) string {
	if result == nil {
		return `{"success":false,"message":"no result"}`
	}
	b, _ := json.Marshal(result)
	return string(b)
}

// MCPTool is the MCP (Model Context Protocol) tool definition shape.
// Use ActionsAsMCPTools() to export Heimdall actions as MCP tools.
// Same fields as MCP Tool: name, description, inputSchema (JSON Schema).
type MCPTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// InMemoryToolRunner provides MCP-style tools (e.g. store, recall, discover) that the
// agentic loop can call in process instead of via HTTP. Used to expose pkg/mcp memory
// tools to the LLM so it can manage memories through the same tool list.
type InMemoryToolRunner interface {
	// ToolDefinitions returns tool definitions for the LLM (name, description, inputSchema).
	ToolDefinitions() []MCPTool
	// ToolNames returns the list of tool names this runner handles (for dispatch).
	ToolNames() []string
	// CallTool executes the named tool with the given arguments in the given database context.
	// dbName is the logical database name (e.g. from DefaultDatabaseName()); use "" for default.
	CallTool(ctx context.Context, name string, args map[string]interface{}, dbName string) (interface{}, error)
}

// FormatInMemoryToolResult formats a raw tool result or error for the LLM (tool message content).
func FormatInMemoryToolResult(raw interface{}, err error) string {
	if err != nil {
		return fmt.Sprintf(`{"success":false,"message":%q}`, err.Error())
	}
	b, _ := json.Marshal(raw)
	return string(b)
}

// DatabaseRouter provides multi-database access for Heimdall actions and plugins.
//
// IMPORTANT:
//   - The `database` parameter is a logical database name (or alias) as used by the
//     Neo4j-compatible multi-database layer.
//   - If `database` is empty, implementations must route to the configured default database.
//
// This interface is intentionally database-name aware so plugins can maintain strict
// tenant isolation by routing each operation to the correct logical database.
type DatabaseRouter interface {
	// DefaultDatabaseName returns the configured default database name.
	DefaultDatabaseName() string

	// ResolveDatabase resolves a database alias or name to the underlying database name.
	ResolveDatabase(nameOrAlias string) (string, error)

	// ListDatabases returns the known logical database names.
	ListDatabases() []string

	// Query executes a Cypher query against the specified logical database.
	//
	// NOTE: Despite the historical "read-only" naming, this method may execute write
	// queries depending on the underlying Cypher engine. Treat it as a general Cypher
	// execution entrypoint unless your implementation enforces read-only semantics.
	Query(ctx context.Context, database string, cypher string, params map[string]interface{}) ([]map[string]interface{}, error)

	// Stats returns database statistics for the specified logical database.
	Stats(database string) (DatabaseStats, error)

	// Discover performs semantic search with graph traversal in the specified database.
	// Returns search results with related nodes up to the specified depth.
	Discover(ctx context.Context, database string, query string, nodeTypes []string, limit int, depth int) (*DiscoverResult, error)
}

// DiscoverResult contains semantic search results with related nodes.
type DiscoverResult struct {
	Results []SearchResult `json:"results"`
	Method  string         `json:"method"` // "vector" or "keyword"
	Total   int            `json:"total"`
}

// SearchResult represents a single search result with similarity and related nodes.
type SearchResult struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Title          string                 `json:"title,omitempty"`
	ContentPreview string                 `json:"content_preview,omitempty"`
	Similarity     float64                `json:"similarity"`
	Properties     map[string]interface{} `json:"properties,omitempty"`
	Related        []RelatedNode          `json:"related,omitempty"`
}

// RelatedNode represents a node connected to a search result.
type RelatedNode struct {
	ID           string `json:"id"`
	Type         string `json:"type,omitempty"`
	Title        string `json:"title,omitempty"`
	Distance     int    `json:"distance"`            // Hops from the source node
	Relationship string `json:"relationship"`        // Relationship type
	Direction    string `json:"direction,omitempty"` // "incoming", "outgoing", or ""
	Path         string `json:"path,omitempty"`      // Path description
}

// DatabaseStats contains database statistics.
type DatabaseStats struct {
	NodeCount         int64            `json:"node_count"`
	RelationshipCount int64            `json:"relationship_count"`
	LabelCounts       map[string]int64 `json:"label_counts"`
	ClusterStats      *ClusterStats    `json:"cluster_stats,omitempty"`
	FeatureFlags      *FeatureFlags    `json:"feature_flags,omitempty"`
}

// ClusterStats contains k-means clustering statistics.
type ClusterStats struct {
	NumClusters    int     `json:"num_clusters"`
	EmbeddingCount int     `json:"embedding_count"`
	IsClustered    bool    `json:"is_clustered"`
	AvgClusterSize float64 `json:"avg_cluster_size,omitempty"`
	Iterations     int     `json:"iterations,omitempty"`
}

// FeatureFlags contains enabled/disabled feature status.
type FeatureFlags struct {
	// Core Heimdall flags
	HeimdallEnabled          bool `json:"heimdall_enabled"`
	HeimdallAnomalyDetection bool `json:"heimdall_anomaly_detection"`
	HeimdallRuntimeDiagnosis bool `json:"heimdall_runtime_diagnosis"`
	HeimdallMemoryCuration   bool `json:"heimdall_memory_curation"`

	// Clustering (derived from search stats)
	ClusteringEnabled bool `json:"clustering_enabled"`

	// Topology/prediction flags
	TopologyEnabled bool `json:"topology_enabled"`
	KalmanEnabled   bool `json:"kalman_enabled"`

	// Runtime flags (derived from DB state)
	AsyncWritesEnabled bool `json:"async_writes_enabled"`
}

// MetricsReader provides runtime metrics access for actions.
type MetricsReader interface {
	// Runtime returns current runtime metrics
	Runtime() RuntimeMetrics
}

// RuntimeMetrics contains runtime statistics.
type RuntimeMetrics struct {
	GoroutineCount int    `json:"goroutine_count"`
	MemoryAllocMB  uint64 `json:"memory_alloc_mb"`
	NumGC          uint32 `json:"num_gc"`
}

// LoadedHeimdallPlugin represents a loaded SLM plugin with full subsystem management.
type LoadedHeimdallPlugin struct {
	Plugin  HeimdallPlugin // The actual plugin implementing full interface
	Path    string         // Path to .so file (empty for built-in)
	Builtin bool           // True if this is a built-in plugin
}

// SubsystemManager manages all SLM plugins/subsystems.
// Provides the SLM with full control over registered subsystems.
type SubsystemManager struct {
	mu             sync.RWMutex
	plugins        map[string]*LoadedHeimdallPlugin // keyed by plugin name
	actions        map[string]ActionFunc            // keyed by full name: slm.plugin.action
	ctx            SubsystemContext                 // shared context for subsystems
	initialized    bool
	orderedPlugins []*LoadedHeimdallPlugin
	orderDirty     bool
}

var (
	globalManager   *SubsystemManager
	globalManagerMu sync.Mutex
)

// ResetSubsystemManagerForTests clears the global subsystem manager singleton.
// It is intended for test isolation when separate test cases need a fresh
// registration state for Heimdall plugins.
func ResetSubsystemManagerForTests() {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	globalManager = nil
}

// GetSubsystemManager returns the global subsystem manager (creates if needed).
func GetSubsystemManager() *SubsystemManager {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	if globalManager == nil {
		globalManager = &SubsystemManager{
			plugins:    make(map[string]*LoadedHeimdallPlugin),
			actions:    make(map[string]ActionFunc),
			orderDirty: true,
		}
	}
	return globalManager
}

// SetContext configures the shared context for all subsystems.
func (m *SubsystemManager) SetContext(ctx SubsystemContext) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
}

// RegisterPlugin registers an SLM plugin and initializes it.
func (m *SubsystemManager) RegisterPlugin(p HeimdallPlugin, path string, builtin bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := p.Name()

	// Verify it's an SLM plugin type
	if p.Type() != PluginTypeHeimdall {
		return fmt.Errorf("plugin %s has type %q, expected %q", name, p.Type(), PluginTypeHeimdall)
	}

	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("plugin already registered: %s", name)
	}

	// Initialize the subsystem
	if err := p.Initialize(m.ctx); err != nil {
		return fmt.Errorf("failed to initialize %s: %w", name, err)
	}

	// Register plugin
	lp := &LoadedHeimdallPlugin{
		Plugin:  p,
		Path:    path,
		Builtin: builtin,
	}
	m.plugins[name] = lp
	m.orderDirty = true

	globalEventDispatcher.mu.RLock()
	running := globalEventDispatcher.running
	globalEventDispatcher.mu.RUnlock()
	if running {
		globalEventDispatcher.ensurePluginQueueForPlugin(lp)
	}

	// Register all actions from this plugin
	for actionName, action := range p.Actions() {
		fullName := fmt.Sprintf("heimdall_%s_%s", name, actionName)
		action.Name = fullName
		m.actions[fullName] = action
	}

	// Mark as initialized once we have at least one plugin
	m.initialized = true

	return nil
}

// GetPlugin returns a plugin by name.
func (m *SubsystemManager) GetPlugin(name string) (HeimdallPlugin, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if lp, ok := m.plugins[name]; ok {
		return lp.Plugin, true
	}
	return nil, false
}

// GetAction returns an action by full name (e.g., "heimdall_plugin_action").
func (m *SubsystemManager) GetAction(name string) (ActionFunc, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	action, ok := m.actions[name]
	return action, ok
}

// StartAll starts all registered subsystems.
func (m *SubsystemManager) StartAll() error {
	m.mu.RLock()
	plugins := m.listOrderedPluginsLocked()
	m.mu.RUnlock()

	for _, lp := range plugins {
		if err := lp.Plugin.Start(); err != nil {
			return fmt.Errorf("failed to start %s: %w", lp.Plugin.Name(), err)
		}
	}
	return nil
}

// StopAll stops all registered subsystems.
func (m *SubsystemManager) StopAll() error {
	m.mu.RLock()
	plugins := m.listOrderedPluginsLocked()
	m.mu.RUnlock()

	var lastErr error
	for _, lp := range plugins {
		if err := lp.Plugin.Stop(); err != nil {
			lastErr = fmt.Errorf("failed to stop %s: %w", lp.Plugin.Name(), err)
		}
	}
	return lastErr
}

// ShutdownAll shuts down all registered subsystems.
func (m *SubsystemManager) ShutdownAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	plugins := m.listOrderedPluginsLocked()
	for _, lp := range plugins {
		if err := lp.Plugin.Shutdown(); err != nil {
			lastErr = fmt.Errorf("failed to shutdown %s: %w", lp.Plugin.Name(), err)
		}
	}
	m.plugins = make(map[string]*LoadedHeimdallPlugin)
	m.actions = make(map[string]ActionFunc)
	m.orderedPlugins = nil
	m.orderDirty = true
	return lastErr
}

// AllHealth returns health status of all subsystems.
func (m *SubsystemManager) AllHealth() map[string]SubsystemHealth {
	m.mu.RLock()
	plugins := m.listOrderedPluginsLocked()
	m.mu.RUnlock()

	result := make(map[string]SubsystemHealth)
	for _, lp := range plugins {
		result[lp.Plugin.Name()] = lp.Plugin.Health()
	}
	return result
}

// AllSummaries returns summaries of all subsystems (for SLM context).
func (m *SubsystemManager) AllSummaries() map[string]string {
	m.mu.RLock()
	plugins := m.listOrderedPluginsLocked()
	m.mu.RUnlock()

	result := make(map[string]string)
	for _, lp := range plugins {
		result[lp.Plugin.Name()] = lp.Plugin.Summary()
	}
	return result
}

// LoadHeimdallPluginsFromDir scans a directory for .so files and loads them.
// Called at startup if NORNICDB_HEIMDALL_PLUGINS_DIR is set.
func LoadHeimdallPluginsFromDir(dir string, ctx SubsystemContext) error {
	if dir == "" {
		return nil
	}

	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil // No plugins directory
	}
	if err != nil {
		return fmt.Errorf("checking Heimdall plugins directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("Heimdall plugins path is not a directory: %s", dir)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.so"))
	if err != nil {
		return fmt.Errorf("scanning Heimdall plugins directory: %w", err)
	}

	if len(matches) == 0 {
		return nil
	}

	manager := GetSubsystemManager()
	manager.SetContext(ctx)

	logHeimdallPrintf(slog.LevelInfo, "╔══════════════════════════════════════════════════════════════╗")
	logHeimdallPrintf(slog.LevelInfo, "║ Loading SLM Plugins                                          ║")
	logHeimdallPrintf(slog.LevelInfo, "╠══════════════════════════════════════════════════════════════╣")

	var totalActions int
	var loadedCount int

	for _, path := range matches {
		p, err := loadHeimdallPluginFromFile(path)
		if err != nil {
			logHeimdallPrintf(slog.LevelWarn, "║ ⚠ %-58s ║", filepath.Base(path)+": "+err.Error())
			continue
		}

		if err := manager.RegisterPlugin(p, path, false); err != nil {
			logHeimdallPrintf(slog.LevelWarn, "║ ⚠ %-58s ║", p.Name()+": "+err.Error())
			continue
		}

		loadedCount++
		totalActions += len(p.Actions())

		logHeimdallPrintf(slog.LevelInfo, "║ ✓ %-15s v%-8s  %d actions %18s ║",
			p.Name(), p.Version(), len(p.Actions()), "")
	}

	logHeimdallPrintf(slog.LevelInfo, "╠══════════════════════════════════════════════════════════════╣")
	logHeimdallPrintf(slog.LevelInfo, "║ Loaded: %d plugins, %d actions %28s ║", loadedCount, totalActions, "")
	logHeimdallPrintf(slog.LevelInfo, "╚══════════════════════════════════════════════════════════════╝")

	manager.mu.Lock()
	manager.initialized = true
	manager.mu.Unlock()

	return nil
}

// loadHeimdallPluginFromFile loads a single .so plugin file.
// The plugin must implement the HeimdallPlugin interface.
func loadHeimdallPluginFromFile(path string) (HeimdallPlugin, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", decoratePluginOpenError(err))
	}

	sym, err := p.Lookup("Plugin")
	if err != nil {
		return nil, fmt.Errorf("no Plugin symbol")
	}

	// Try direct cast to HeimdallPlugin
	if slmPlugin, ok := sym.(HeimdallPlugin); ok {
		// Verify it's the right type
		if slmPlugin.Type() != PluginTypeHeimdall {
			return nil, fmt.Errorf("plugin type is %q, expected %q", slmPlugin.Type(), PluginTypeHeimdall)
		}
		return slmPlugin, nil
	}

	// Try pointer to HeimdallPlugin
	if slmPluginPtr, ok := sym.(*HeimdallPlugin); ok && slmPluginPtr != nil {
		if (*slmPluginPtr).Type() != PluginTypeHeimdall {
			return nil, fmt.Errorf("plugin type is %q, expected %q", (*slmPluginPtr).Type(), PluginTypeHeimdall)
		}
		return *slmPluginPtr, nil
	}

	// Use reflection as fallback (for plugins built separately)
	val := reflect.ValueOf(sym)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	// Check for required methods
	requiredMethods := []string{"Name", "Version", "Type", "Description", "Initialize", "Start", "Stop", "Shutdown", "Status", "Health", "Metrics", "Config", "Configure", "ConfigSchema", "Actions", "Summary", "RecentEvents"}
	for _, method := range requiredMethods {
		if !val.MethodByName(method).IsValid() {
			return nil, fmt.Errorf("missing %s() method - plugin must implement HeimdallPlugin interface", method)
		}
	}

	// Wrap in reflectHeimdallPlugin adapter
	return &reflectHeimdallPlugin{val: val}, nil
}

func decoratePluginOpenError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if !strings.Contains(msg, "different version of package") {
		return err
	}
	return fmt.Errorf("%s; rebuild the plugin with the same Go toolchain (%s), CGO setting, architecture, and build tags as the NornicDB binary (for remote-provider-only Heimdall builds, match whether the host binary was built with -tags nolocalllm)", msg, runtime.Version())
}

// reflectHeimdallPlugin wraps a plugin loaded via reflection.
type reflectHeimdallPlugin struct {
	val reflect.Value
}

func (p *reflectHeimdallPlugin) Name() string {
	return p.val.MethodByName("Name").Call(nil)[0].String()
}
func (p *reflectHeimdallPlugin) Version() string {
	return p.val.MethodByName("Version").Call(nil)[0].String()
}
func (p *reflectHeimdallPlugin) Type() string {
	return p.val.MethodByName("Type").Call(nil)[0].String()
}
func (p *reflectHeimdallPlugin) Description() string {
	return p.val.MethodByName("Description").Call(nil)[0].String()
}
func (p *reflectHeimdallPlugin) Initialize(ctx SubsystemContext) error {
	result := p.val.MethodByName("Initialize").Call([]reflect.Value{reflect.ValueOf(ctx)})
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) Start() error {
	result := p.val.MethodByName("Start").Call(nil)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) Stop() error {
	result := p.val.MethodByName("Stop").Call(nil)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) Shutdown() error {
	result := p.val.MethodByName("Shutdown").Call(nil)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) Status() SubsystemStatus {
	result := p.val.MethodByName("Status").Call(nil)
	if s, ok := result[0].Interface().(SubsystemStatus); ok {
		return s
	}
	return StatusError
}
func (p *reflectHeimdallPlugin) Health() SubsystemHealth {
	result := p.val.MethodByName("Health").Call(nil)
	if h, ok := result[0].Interface().(SubsystemHealth); ok {
		return h
	}
	return SubsystemHealth{Status: StatusError, Healthy: false}
}
func (p *reflectHeimdallPlugin) Metrics() map[string]interface{} {
	result := p.val.MethodByName("Metrics").Call(nil)
	if m, ok := result[0].Interface().(map[string]interface{}); ok {
		return m
	}
	return nil
}
func (p *reflectHeimdallPlugin) Config() map[string]interface{} {
	result := p.val.MethodByName("Config").Call(nil)
	if m, ok := result[0].Interface().(map[string]interface{}); ok {
		return m
	}
	return nil
}
func (p *reflectHeimdallPlugin) Configure(settings map[string]interface{}) error {
	result := p.val.MethodByName("Configure").Call([]reflect.Value{reflect.ValueOf(settings)})
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) ConfigSchema() map[string]interface{} {
	result := p.val.MethodByName("ConfigSchema").Call(nil)
	if m, ok := result[0].Interface().(map[string]interface{}); ok {
		return m
	}
	return nil
}
func (p *reflectHeimdallPlugin) Actions() map[string]ActionFunc {
	result := p.val.MethodByName("Actions").Call(nil)
	if m, ok := result[0].Interface().(map[string]ActionFunc); ok {
		return m
	}
	return nil
}
func (p *reflectHeimdallPlugin) Summary() string {
	return p.val.MethodByName("Summary").Call(nil)[0].String()
}
func (p *reflectHeimdallPlugin) RecentEvents(limit int) []SubsystemEvent {
	result := p.val.MethodByName("RecentEvents").Call([]reflect.Value{reflect.ValueOf(limit)})
	if e, ok := result[0].Interface().([]SubsystemEvent); ok {
		return e
	}
	return nil
}
func (p *reflectHeimdallPlugin) PrePrompt(ctx *PromptContext) error {
	method := p.val.MethodByName("PrePrompt")
	if !method.IsValid() {
		return nil // Optional method
	}
	result := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}
func (p *reflectHeimdallPlugin) PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult)) {
	method := p.val.MethodByName("PreExecute")
	if !method.IsValid() {
		done(PreExecuteResult{Continue: true}) // Default: continue
		return
	}
	method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(done)})
}
func (p *reflectHeimdallPlugin) PostExecute(ctx *PostExecuteContext) {
	method := p.val.MethodByName("PostExecute")
	if !method.IsValid() {
		return // Optional method
	}
	method.Call([]reflect.Value{reflect.ValueOf(ctx)})
}

// GetHeimdallAction returns an action by full name (e.g., "heimdall_anomaly_detect").
func GetHeimdallAction(name string) (ActionFunc, bool) {
	m := GetSubsystemManager()
	m.mu.RLock()
	defer m.mu.RUnlock()
	action, ok := m.actions[name]
	return action, ok
}

// ListHeimdallActions returns all registered SLM action names.
func ListHeimdallActions() []string {
	m := GetSubsystemManager()
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.actions))
	for name := range m.actions {
		names = append(names, name)
	}
	return names
}

// ListHeimdallPlugins returns information about all loaded SLM plugins.
func ListHeimdallPlugins() []*LoadedHeimdallPlugin {
	m := GetSubsystemManager()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.listOrderedPluginsLocked()
}

func (m *SubsystemManager) listOrderedPluginsLocked() []*LoadedHeimdallPlugin {
	if !m.orderDirty && m.orderedPlugins != nil {
		return append([]*LoadedHeimdallPlugin(nil), m.orderedPlugins...)
	}

	ordered := orderPlugins(m.plugins)
	m.orderedPlugins = ordered
	m.orderDirty = false
	return append([]*LoadedHeimdallPlugin(nil), ordered...)
}

func orderPlugins(plugins map[string]*LoadedHeimdallPlugin) []*LoadedHeimdallPlugin {
	if len(plugins) == 0 {
		return nil
	}

	nodes := make(map[string]*LoadedHeimdallPlugin, len(plugins))
	for name, lp := range plugins {
		nodes[name] = lp
	}

	type nodeInfo struct {
		inDegree int
		out      map[string]struct{}
		priority int
	}

	info := make(map[string]*nodeInfo, len(nodes))
	for name, lp := range nodes {
		info[name] = &nodeInfo{
			out:      make(map[string]struct{}),
			priority: pluginPriority(lp.Plugin),
		}
	}

	addEdge := func(from, to string) {
		if from == to {
			return
		}
		if _, ok := nodes[from]; !ok {
			return
		}
		if _, ok := nodes[to]; !ok {
			return
		}
		if _, exists := info[from].out[to]; exists {
			return
		}
		info[from].out[to] = struct{}{}
		info[to].inDegree++
	}

	for name, lp := range nodes {
		before, after := pluginOrdering(lp.Plugin)
		for _, b := range before {
			addEdge(name, b)
		}
		for _, a := range after {
			addEdge(a, name)
		}
	}

	ready := make([]string, 0, len(nodes))
	for name, data := range info {
		if data.inDegree == 0 {
			ready = append(ready, name)
		}
	}

	sortReady := func() {
		sort.Slice(ready, func(i, j int) bool {
			pi := info[ready[i]].priority
			pj := info[ready[j]].priority
			if pi != pj {
				return pi > pj
			}
			return ready[i] < ready[j]
		})
	}

	sortReady()

	result := make([]*LoadedHeimdallPlugin, 0, len(nodes))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		result = append(result, nodes[name])

		for out := range info[name].out {
			info[out].inDegree--
			if info[out].inDegree == 0 {
				ready = append(ready, out)
			}
		}
		sortReady()
	}

	if len(result) != len(nodes) {
		// Cycle detected - fall back to deterministic priority + name ordering.
		fallback := make([]*LoadedHeimdallPlugin, 0, len(nodes))
		for _, lp := range nodes {
			fallback = append(fallback, lp)
		}
		sort.Slice(fallback, func(i, j int) bool {
			pi := pluginPriority(fallback[i].Plugin)
			pj := pluginPriority(fallback[j].Plugin)
			if pi != pj {
				return pi > pj
			}
			return fallback[i].Plugin.Name() < fallback[j].Plugin.Name()
		})
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] Plugin ordering cycle detected; falling back to priority order")
		return fallback
	}

	return result
}

func pluginPriority(p HeimdallPlugin) int {
	if ordering, ok := p.(PluginOrdering); ok {
		return ordering.Priority()
	}
	return 0
}

func pluginOrdering(p HeimdallPlugin) (before []string, after []string) {
	if ordering, ok := p.(PluginOrdering); ok {
		return ordering.Before(), ordering.After()
	}
	return nil, nil
}

// RegisterBuiltinAction registers a built-in action (not from .so plugin).
// Used to register core actions without requiring external plugins.
func RegisterBuiltinAction(action ActionFunc) {
	m := GetSubsystemManager()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.actions[action.Name] = action
}

// ExecuteAction executes an action by name with the given context.
func ExecuteAction(name string, ctx ActionContext) (*ActionResult, error) {
	m := GetSubsystemManager()
	m.mu.RLock()
	action, ok := m.actions[name]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown action: %s", name)
	}

	if action.Handler == nil {
		return nil, fmt.Errorf("action %s has no handler", name)
	}

	return action.Handler(ctx)
}

// ActionCatalog returns all actions grouped by category for display.
func ActionCatalog() map[string][]ActionFunc {
	m := GetSubsystemManager()
	m.mu.RLock()
	defer m.mu.RUnlock()

	catalog := make(map[string][]ActionFunc)
	for _, action := range m.actions {
		cat := action.Category
		if cat == "" {
			cat = "general"
		}
		catalog[cat] = append(catalog[cat], action)
	}
	return catalog
}

// ActionsAsMCPTools returns all registered Heimdall actions in MCP tool format.
// Use this to expose Heimdall actions to MCP clients or to merge with pkg/mcp tool list.
// Each tool has name, description, and inputSchema (JSON Schema); when an action
// has no InputSchema set, DefaultActionInputSchema is used.
func ActionsAsMCPTools() []MCPTool {
	catalog := ActionCatalog()
	var tools []MCPTool
	for _, actions := range catalog {
		for _, a := range actions {
			schema := a.InputSchema
			if len(schema) == 0 {
				schema = DefaultActionInputSchema
			}
			tools = append(tools, MCPTool{
				Name:        a.Name,
				Description: a.Description,
				InputSchema: schema,
			})
		}
	}
	return tools
}

// HeimdallPluginsInitialized returns true if SLM plugins have been loaded.
func HeimdallPluginsInitialized() bool {
	m := GetSubsystemManager()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.initialized
}

// ActionPrompt generates a list of available actions.
func ActionPrompt() string {
	catalog := ActionCatalog()

	var prompt string
	for category, actions := range catalog {
		prompt += fmt.Sprintf("## %s\n", category)
		for _, action := range actions {
			prompt += fmt.Sprintf("- %s: %s\n", action.Name, action.Description)
		}
		prompt += "\n"
	}

	return prompt
}

// ParseActionResponse parses an SLM response to extract action requests.
type ParsedAction struct {
	Action string                 `json:"action"`
	Params map[string]interface{} `json:"params"`
}

// ActionInvoker handles action invocation from SLM responses.
type ActionInvoker struct {
	db      DatabaseRouter
	metrics MetricsReader
}

// NewActionInvoker creates an action invoker with database/metrics access.
func NewActionInvoker(db DatabaseRouter, metrics MetricsReader) *ActionInvoker {
	return &ActionInvoker{db: db, metrics: metrics}
}

// Invoke executes a parsed action.
func (i *ActionInvoker) Invoke(ctx context.Context, parsed ParsedAction, userMessage string) (*ActionResult, error) {
	actCtx := ActionContext{
		Context:     ctx,
		UserMessage: userMessage,
		Params:      parsed.Params,
		Database:    i.db,
		Metrics:     i.metrics,
	}

	start := time.Now()
	result, err := ExecuteAction(parsed.Action, actCtx)
	if result != nil && result.Data == nil {
		result.Data = make(map[string]interface{})
	}
	if result != nil {
		result.Data["duration_ms"] = time.Since(start).Milliseconds()
	}
	return result, err
}

// =============================================================================
// Optional Hook Dispatch Functions
// =============================================================================

// CallPrePromptHooks calls PrePrompt on all plugins that implement PrePromptHook.
// Plugins that don't implement the hook are silently skipped.
// Returns the first cancellation encountered, or nil if no cancellations.
func CallPrePromptHooks(ctx *PromptContext) {
	if !HeimdallPluginsInitialized() {
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PrePrompt hooks skipped: plugins not initialized (request=%s)", ctx.RequestID)
		return
	}

	plugins := ListHeimdallPlugins()
	hookCount := 0
	for _, p := range plugins {
		if _, ok := p.Plugin.(PrePromptHook); ok {
			hookCount++
		}
	}
	logHeimdallPrintf(slog.LevelInfo, "[Heimdall] PrePrompt hooks: request=%s plugins=%d hooks=%d", ctx.RequestID, len(plugins), hookCount)
	for _, p := range plugins {
		// Check if plugin implements PrePromptHook
		if hook, ok := p.Plugin.(PrePromptHook); ok {
			if err := hook.PrePrompt(ctx); err != nil {
				// Log warning but don't abort
				logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PrePrompt warning from %s: %v", p.Plugin.Name(), err)
			}
			// Check for cancellation after each plugin
			if ctx.Cancelled() {
				return
			}
		}
	}
}

// CallPreExecuteHooks calls PreExecute on all plugins that implement PreExecuteHook.
// Plugins that don't implement the hook are silently skipped.
// This is synchronous - waits for each plugin with a timeout.
func CallPreExecuteHooks(ctx *PreExecuteContext) PreExecuteResult {
	if !HeimdallPluginsInitialized() {
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PreExecute hooks skipped: plugins not initialized (request=%s action=%s)", ctx.RequestID, ctx.Action)
		return PreExecuteResult{Continue: true}
	}

	plugins := ListHeimdallPlugins()
	result := PreExecuteResult{Continue: true}
	hookCount := 0
	for _, p := range plugins {
		if _, ok := p.Plugin.(PreExecuteHook); ok {
			hookCount++
		}
	}
	logHeimdallPrintf(slog.LevelInfo, "[Heimdall] PreExecute hooks: request=%s action=%s plugins=%d hooks=%d", ctx.RequestID, ctx.Action, len(plugins), hookCount)

	for _, p := range plugins {
		// Check if plugin implements PreExecuteHook
		if hook, ok := p.Plugin.(PreExecuteHook); ok {
			done := make(chan PreExecuteResult, 1)
			hook.PreExecute(ctx, func(r PreExecuteResult) {
				done <- r
			})

			select {
			case r := <-done:
				if !r.Continue {
					return r // Abort on first Continue=false
				}
				if r.ModifiedParams != nil {
					ctx.Params = r.ModifiedParams
				}
			case <-time.After(5 * time.Second):
				logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PreExecute timeout from %s", p.Plugin.Name())
			}

			// Check for cancellation via context method
			if ctx.Cancelled() {
				return PreExecuteResult{Continue: false, AbortMessage: ctx.CancelReason()}
			}
		}
	}

	return result
}

// CallPostExecuteHooks calls PostExecute on all plugins that implement PostExecuteHook.
// Plugins that don't implement the hook are silently skipped.
// This is fire-and-forget - runs asynchronously using a bounded worker pool.
func CallPostExecuteHooks(ctx *PostExecuteContext) {
	if !HeimdallPluginsInitialized() {
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PostExecute hooks skipped: plugins not initialized (request=%s action=%s)", ctx.RequestID, ctx.Action)
		return
	}

	plugins := ListHeimdallPlugins()
	hookCount := 0
	for _, p := range plugins {
		if _, ok := p.Plugin.(PostExecuteHook); ok {
			hookCount++
		}
	}
	logHeimdallPrintf(slog.LevelInfo, "[Heimdall] PostExecute hooks: request=%s action=%s plugins=%d hooks=%d", ctx.RequestID, ctx.Action, len(plugins), hookCount)
	for _, p := range plugins {
		// Check if plugin implements PostExecuteHook
		if hook, ok := p.Plugin.(PostExecuteHook); ok {
			postExecutePool().enqueue(postExecuteJob{
				pluginName: p.Plugin.Name(),
				hook:       hook,
				ctx:        ctx,
			})
		}
	}
}

type postExecuteJob struct {
	pluginName string
	hook       PostExecuteHook
	ctx        *PostExecuteContext
}

type postExecuteWorkerPool struct {
	once    sync.Once
	jobs    chan postExecuteJob
	workers int
}

const (
	defaultPostExecuteWorkers = 4
	defaultPostExecuteQueue   = 256
)

var globalPostExecutePool = &postExecuteWorkerPool{workers: defaultPostExecuteWorkers}

func postExecutePool() *postExecuteWorkerPool {
	globalPostExecutePool.once.Do(globalPostExecutePool.start)
	return globalPostExecutePool
}

func (p *postExecuteWorkerPool) start() {
	if p.workers <= 0 {
		p.workers = defaultPostExecuteWorkers
	}
	p.jobs = make(chan postExecuteJob, defaultPostExecuteQueue)
	for i := 0; i < p.workers; i++ {
		go func() {
			for job := range p.jobs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							logHeimdallPrintf(slog.LevelError, "[Heimdall] PostExecute panic in %s: %v", job.pluginName, r)
						}
					}()
					job.hook.PostExecute(job.ctx)
				}()
			}
		}()
	}
}

func (p *postExecuteWorkerPool) enqueue(job postExecuteJob) {
	if p.jobs == nil {
		p.start()
	}
	select {
	case p.jobs <- job:
	default:
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] PostExecute queue full, dropping job from %s", job.pluginName)
	}
}

// CallSynthesisHooks calls Synthesize on all plugins that implement SynthesisHook.
// The first plugin to return a non-empty response wins.
// If no plugin provides a response, returns empty string (caller should use default synthesis).
// This is synchronous with a timeout to ensure responsive UX.
func CallSynthesisHooks(ctx *SynthesisContext) string {
	if !HeimdallPluginsInitialized() {
		return ""
	}

	plugins := ListHeimdallPlugins()

	for _, p := range plugins {
		// Check if plugin implements SynthesisHook
		if hook, ok := p.Plugin.(SynthesisHook); ok {
			done := make(chan string, 1)
			hook.Synthesize(ctx, func(response string) {
				done <- response
			})

			select {
			case response := <-done:
				if response != "" {
					logHeimdallPrintf(slog.LevelInfo, "[Heimdall] Plugin %s provided synthesis response", p.Plugin.Name())
					return response // First non-empty response wins
				}
			case <-time.After(5 * time.Second):
				logHeimdallPrintf(slog.LevelWarn, "[Heimdall] Plugin %s synthesis timed out", p.Plugin.Name())
			}
		}
	}

	return "" // No plugin provided synthesis, use default
}

// =============================================================================
// Database Event Dispatcher
// =============================================================================

// dbEventDispatcher manages asynchronous delivery of database events to plugins.
type dbEventDispatcher struct {
	mu           sync.RWMutex
	running      bool
	events       chan *DatabaseEvent
	done         chan struct{}
	pluginQueues map[string]*pluginEventQueue
}

var globalEventDispatcher = &dbEventDispatcher{
	events:       make(chan *DatabaseEvent, 1000), // Buffer up to 1000 events
	done:         make(chan struct{}),
	pluginQueues: make(map[string]*pluginEventQueue),
}

const (
	defaultDatabaseEventQueue = 256
)

type pluginEventQueue struct {
	name string
	hook DatabaseEventHook
	ch   chan *DatabaseEvent
}

// StartEventDispatcher starts the background event dispatcher.
// This should be called when Heimdall is initialized.
func StartEventDispatcher() {
	d := globalEventDispatcher
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return
	}

	d.running = true
	d.events = make(chan *DatabaseEvent, 1000)
	d.done = make(chan struct{})
	d.pluginQueues = make(map[string]*pluginEventQueue)

	go func() {
		for {
			select {
			case event := <-d.events:
				dispatchEventToPlugins(event)
			case <-d.done:
				return
			}
		}
	}()
}

// StopEventDispatcher stops the background event dispatcher.
func StopEventDispatcher() {
	d := globalEventDispatcher
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return
	}

	d.running = false
	close(d.done)
	for _, queue := range d.pluginQueues {
		close(queue.ch)
	}
	d.pluginQueues = make(map[string]*pluginEventQueue)
}

// dispatchEventToPlugins sends an event to all plugins that implement DatabaseEventHook.
func dispatchEventToPlugins(event *DatabaseEvent) {
	if !HeimdallPluginsInitialized() {
		return
	}

	plugins := ListHeimdallPlugins()
	for _, p := range plugins {
		// Check if plugin implements DatabaseEventHook
		if hook, ok := p.Plugin.(DatabaseEventHook); ok {
			globalEventDispatcher.enqueueEvent(p.Plugin.Name(), hook, event)
		}
	}
}

func (d *dbEventDispatcher) ensurePluginQueueForPlugin(lp *LoadedHeimdallPlugin) {
	if lp == nil {
		return
	}
	if hook, ok := lp.Plugin.(DatabaseEventHook); ok {
		d.ensurePluginQueue(lp.Plugin.Name(), hook)
	}
}

func (d *dbEventDispatcher) ensurePluginQueue(name string, hook DatabaseEventHook) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.pluginQueues[name]; ok {
		return
	}

	queue := &pluginEventQueue{
		name: name,
		hook: hook,
		ch:   make(chan *DatabaseEvent, defaultDatabaseEventQueue),
	}
	d.pluginQueues[name] = queue

	go func(q *pluginEventQueue) {
		for event := range q.ch {
			func() {
				defer func() {
					if r := recover(); r != nil {
						logHeimdallPrintf(slog.LevelError, "[Heimdall] DatabaseEventHook panic in %s: %v", q.name, r)
					}
				}()
				q.hook.OnDatabaseEvent(event)
			}()
		}
	}(queue)
}

func (d *dbEventDispatcher) enqueueEvent(name string, hook DatabaseEventHook, event *DatabaseEvent) {
	d.ensurePluginQueue(name, hook)

	d.mu.RLock()
	queue := d.pluginQueues[name]
	d.mu.RUnlock()
	if queue == nil {
		return
	}

	select {
	case queue.ch <- event:
	default:
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] DatabaseEvent queue full for %s, dropping event: %s", name, event.Type)
	}
}

// EmitDatabaseEvent sends a database event to all registered plugins.
// This is non-blocking - events are queued for async delivery.
// If the queue is full, the event is dropped (with a warning).
func EmitDatabaseEvent(event *DatabaseEvent) {
	d := globalEventDispatcher
	d.mu.RLock()
	running := d.running
	d.mu.RUnlock()

	if !running {
		return // Dispatcher not running
	}

	// Set timestamp if not already set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Non-blocking send
	select {
	case d.events <- event:
		// Event queued successfully
	default:
		// Queue full - drop event with warning
		logHeimdallPrintf(slog.LevelWarn, "[Heimdall] Event queue full, dropping event: %s", event.Type)
	}
}

// EmitNodeEvent is a convenience function for emitting node-related events.
func EmitNodeEvent(eventType DatabaseEventType, nodeID string, labels []string, props map[string]interface{}) {
	EmitDatabaseEvent(&DatabaseEvent{
		Type:       eventType,
		NodeID:     nodeID,
		NodeLabels: labels,
		Properties: props,
	})
}

// EmitRelationshipEvent is a convenience function for emitting relationship-related events.
func EmitRelationshipEvent(eventType DatabaseEventType, relID, relType, sourceID, targetID string, props map[string]interface{}) {
	EmitDatabaseEvent(&DatabaseEvent{
		Type:             eventType,
		RelationshipID:   relID,
		RelationshipType: relType,
		SourceNodeID:     sourceID,
		TargetNodeID:     targetID,
		Properties:       props,
	})
}

// EmitQueryEvent is a convenience function for emitting query-related events.
func EmitQueryEvent(eventType DatabaseEventType, query string, params map[string]interface{}, duration time.Duration, rowsAffected int64, err error) {
	event := &DatabaseEvent{
		Type:         eventType,
		Query:        query,
		QueryParams:  params,
		Duration:     duration,
		RowsAffected: rowsAffected,
	}
	if err != nil {
		event.Error = err.Error()
	}
	EmitDatabaseEvent(event)
}
