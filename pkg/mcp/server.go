// Package mcp provides a native Go MCP (Model Context Protocol) server for NornicDB.
//
// This package implements an LLM-native MCP tool surface, designed specifically for
// LLM inference patterns, discovery, and usage. The goal is to provide a dramatically
// improved tool surface for LLM consumption.
//
// Key Design Principles:
//   - Verb-Noun Naming: Clear action verbs + specific nouns (store, recall, discover, link)
//   - Single Responsibility: Each tool does ONE thing well (Unix philosophy)
//   - Minimal Required Parameters: 1-2 required params, rest are smart defaults
//   - Composable & Orthogonal: Tools chain naturally, no overlapping concerns
//   - Rich, Actionable Responses: Return IDs, next-step hints, relationship counts
//   - Progressive Disclosure: Common case is simple, advanced features available
//
// Tool Surface (6 Tools):
//   - store: Store knowledge/memory as a node in the graph
//   - recall: Retrieve knowledge by ID or criteria
//   - discover: Semantic search by meaning (vector embeddings)
//   - link: Create relationships between nodes
//   - task: Create/manage individual tasks
//   - tasks: Query/list multiple tasks
//
// Note: File indexing (index/unindex) is handled by the application layer.
// NornicDB is the storage/embedding layer - it receives already-processed content.
//
// Example Usage (standalone, usually MCP is integrated into main server on port 7474):
//
//	db, _ := nornicdb.Open("./data", nil)
//	server := mcp.NewServer(db, nil)
//
//	// For integration with main NornicDB server, use RegisterRoutes() instead
//	// For standalone testing:
//	if err := server.Start(":7474"); err != nil {
//	    log.Fatal(err)
//	}
//
// MCP Protocol:
//
// The server implements the MCP JSON-RPC protocol:
//   - initialize: Initialize connection and exchange capabilities
//   - tools/list: List available tools
//   - tools/call: Execute a tool
//   - notifications: Handle server notifications
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const cypherMutationConflictRetries = 5

// Embedder interface for generating embeddings (abstracts Ollama/OpenAI).
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
	ChunkText(text string, maxTokens, overlap int) ([]string, error)
	Model() string
	Dimensions() int
}

// Server implements the MCP protocol for NornicDB.
type Server struct {
	db     *nornicdb.DB
	config *ServerConfig
	embed  Embedder

	// HTTP server
	httpServer *http.Server
	mu         sync.RWMutex
	started    time.Time
	closed     bool

	// Tool handlers
	handlers map[string]ToolHandler
}

func runCypherMutationWithRetry(ctx context.Context, exec *cypher.StorageExecutor, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	if exec == nil {
		return nil, fmt.Errorf("cypher executor is nil")
	}

	var lastErr error
	for attempt := 0; attempt < cypherMutationConflictRetries; attempt++ {
		result, err := exec.Execute(ctx, query, params)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, storage.ErrConflict) {
			return nil, err
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, storage.ErrConflict
}

// ServerConfig holds MCP server configuration.
type ServerConfig struct {
	// Address to bind to (default: "localhost")
	Address string `yaml:"address"`
	// Port to listen on (default: 7474, same as NornicDB HTTP API)
	Port int `yaml:"port"`
	// ReadTimeout for requests
	ReadTimeout time.Duration `yaml:"read_timeout"`
	// WriteTimeout for responses
	WriteTimeout time.Duration `yaml:"write_timeout"`
	// MaxRequestSize in bytes (default: 10MB)
	MaxRequestSize int64 `yaml:"max_request_size"`
	// EnableCORS for cross-origin requests
	EnableCORS bool `yaml:"enable_cors"`
	// EmbeddingEnabled controls whether embeddings are generated
	EmbeddingEnabled bool `yaml:"embedding_enabled"`
	// EmbeddingModel is the model name (for error messages)
	EmbeddingModel string `yaml:"embedding_model"`
	// EmbeddingDimensions is the expected vector dimensions (for validation)
	EmbeddingDimensions int `yaml:"embedding_dimensions"`
	// Embedder is the embedding service (set externally if needed)
	Embedder Embedder `yaml:"-"`

	// DatabaseScopedExecutor returns an executor and node getter for the given database name.
	// When set, MCP tool calls that include a database in context (e.g. from the agentic loop)
	// use this to run store/recall/link/task against the request's database instead of the default.
	// If nil or the context has no database, the server uses its single db.
	DatabaseScopedExecutor func(dbName string) (exec *cypher.StorageExecutor, getNode func(context.Context, string) (*nornicdb.Node, error), err error)

	// DatabaseScopedStorage returns a storage engine scoped to dbName.
	// When set, tools that need direct storage access (e.g. discover/search) can operate
	// on the request's database without relying on a single default DB instance.
	DatabaseScopedStorage func(dbName string) (storage.Engine, error)

	// DefaultNodeLabel is the label used when the store tool is called without
	// explicit labels or type. Defaults to "Memory" for backward compatibility.
	// Configured via NORNICDB_DEFAULT_NODE_LABEL env var or config.Memory.DefaultNodeLabel.
	DefaultNodeLabel string
}

// DefaultServerConfig returns sensible defaults for the MCP server.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:          "localhost",
		Port:             7474,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     60 * time.Second,
		MaxRequestSize:   10 * 1024 * 1024, // 10MB
		EnableCORS:       true,
		EmbeddingEnabled: false, // Disabled by default, set Embedder externally
	}
}

// ToolHandler is a function that handles a tool call
type ToolHandler func(ctx context.Context, args map[string]interface{}) (interface{}, error)

// NewServer creates a new MCP server with the given database.
func NewServer(db *nornicdb.DB, config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	s := &Server{
		db:       db,
		config:   config,
		embed:    config.Embedder,
		handlers: make(map[string]ToolHandler),
	}

	// Register all tool handlers
	s.registerHandlers()

	return s
}

// SetEmbedder sets the embedding service.
func (s *Server) SetEmbedder(e Embedder) {
	s.embed = e
	s.config.EmbeddingEnabled = e != nil
}

// SetDatabaseScopedExecutor sets the optional per-database executor and node getter.
// Call this after the server is created when multi-database support is available (e.g. from the HTTP server).
func (s *Server) SetDatabaseScopedExecutor(fn func(dbName string) (exec *cypher.StorageExecutor, getNode func(context.Context, string) (*nornicdb.Node, error), err error)) {
	s.config.DatabaseScopedExecutor = fn
}

// SetDatabaseScopedStorage sets the optional per-database storage resolver.
// Call this after the server is created when multi-database support is available (e.g. from the HTTP server).
func (s *Server) SetDatabaseScopedStorage(fn func(dbName string) (storage.Engine, error)) {
	s.config.DatabaseScopedStorage = fn
}

// registerHandlers registers all MCP tool handlers.
func (s *Server) registerHandlers() {
	// Core memory tools
	s.handlers[ToolStore] = s.handleStore
	s.handlers[ToolRecall] = s.handleRecall
	s.handlers[ToolDiscover] = s.handleDiscover
	s.handlers[ToolLink] = s.handleLink

	// Task management tools
	s.handlers[ToolTask] = s.handleTask
	s.handlers[ToolTasks] = s.handleTasks
}

// RegisterRoutes registers MCP handlers on an existing http.ServeMux.
// Use this to integrate MCP tools into an existing server (e.g., port 7474).
//
// Routes registered:
//   - POST /mcp           - Main JSON-RPC endpoint
//   - POST /mcp/initialize - Initialize MCP connection
//   - GET/POST /mcp/tools/list - List available tools
//   - POST /mcp/tools/call - Execute a tool
//   - GET /mcp/health     - MCP health check
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	s.started = time.Now()

	// MCP endpoints
	mux.HandleFunc("/mcp", s.handleMCP)
	mux.HandleFunc("/mcp/initialize", s.handleInitialize)
	mux.HandleFunc("/mcp/tools/list", s.handleListTools)
	mux.HandleFunc("/mcp/tools/call", s.handleCallTool)
	mux.HandleFunc("/mcp/health", s.handleHealth)
}

// ServeHTTP implements http.Handler for routing MCP requests.
// Use this when integrating with a server that wraps handlers (e.g., for auth middleware).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.started.IsZero() {
		s.started = time.Now()
	}

	switch r.URL.Path {
	case "/mcp":
		s.handleMCP(w, r)
	case "/mcp/initialize":
		s.handleInitialize(w, r)
	case "/mcp/tools/list":
		s.handleListTools(w, r)
	case "/mcp/tools/call":
		s.handleCallTool(w, r)
	case "/mcp/health":
		s.handleHealth(w, r)
	default:
		http.NotFound(w, r)
	}
}

// Start begins listening for HTTP connections on a SEPARATE server.
// For integration with the main NornicDB server on port 7474, use RegisterRoutes() instead.
func (s *Server) Start(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("server already closed")
	}

	if addr == "" {
		addr = fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	// Wrap with CORS middleware if enabled
	var handler http.Handler = mux
	if s.config.EnableCORS {
		handler = s.corsMiddleware(mux)
	}

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("MCP server error: %v\n", err)
		}
	}()

	fmt.Printf("🚀 MCP server started on %s\n", addr)
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// corsMiddleware adds CORS headers.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// =============================================================================
// MCP Protocol Handlers
// =============================================================================

// handleMCP is the main MCP JSON-RPC endpoint.
func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	// Parse JSON-RPC request
	var req struct {
		JSONRPC string                 `json:"jsonrpc"`
		ID      interface{}            `json:"id"`
		Method  string                 `json:"method"`
		Params  map[string]interface{} `json:"params"`
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	if err := json.Unmarshal(body, &req); err != nil {
		s.writeJSONRPCError(w, nil, -32700, "Parse error", err.Error())
		return
	}

	// Route to appropriate handler
	var result interface{}
	var rpcErr error

	switch req.Method {
	case "initialize":
		result, rpcErr = s.doInitialize(req.Params)
	case "tools/list":
		result = s.doListTools()
	case "tools/call":
		toolResult, err := s.doCallTool(r.Context(), req.Params)
		if err != nil {
			// Wrap error in MCP content format
			result = CallToolResponse{
				Content: []Content{{Type: "text", Text: err.Error()}},
				IsError: true,
			}
		} else {
			// Wrap result in MCP content format (required by MCP spec)
			resultJSON, _ := json.Marshal(toolResult)
			result = CallToolResponse{
				Content: []Content{{Type: "text", Text: string(resultJSON)}},
			}
		}
	default:
		s.writeJSONRPCError(w, req.ID, -32601, "Method not found", req.Method)
		return
	}

	if rpcErr != nil {
		s.writeJSONRPCError(w, req.ID, -32000, "Tool execution failed", rpcErr.Error())
		return
	}

	s.writeJSONRPCResult(w, req.ID, result)
}

// handleInitialize handles the initialize request.
func (s *Server) handleInitialize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	var req InitRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	result, _ := s.doInitialize(map[string]interface{}{
		"protocolVersion": req.ProtocolVersion,
		"capabilities":    req.Capabilities,
		"clientInfo":      req.ClientInfo,
	})

	s.writeJSON(w, http.StatusOK, result)
}

// handleListTools returns the list of available tools.
func (s *Server) handleListTools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required")
		return
	}

	result := s.doListTools()
	s.writeJSON(w, http.StatusOK, result)
}

// handleCallTool executes a tool.
func (s *Server) handleCallTool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	var req CallToolRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	result, err := s.doCallTool(r.Context(), map[string]interface{}{
		"name":      req.Name,
		"arguments": req.Arguments,
	})

	if err != nil {
		s.writeJSON(w, http.StatusOK, CallToolResponse{
			Content: []Content{{Type: "text", Text: err.Error()}},
			IsError: true,
		})
		return
	}

	// Convert result to JSON string
	resultJSON, _ := json.Marshal(result)
	s.writeJSON(w, http.StatusOK, CallToolResponse{
		Content: []Content{{Type: "text", Text: string(resultJSON)}},
	})
}

// handleHealth returns server health status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"uptime":  time.Since(s.started).String(),
		"version": "1.0.0",
	})
}

// =============================================================================
// MCP Protocol Implementation
// =============================================================================

func (s *Server) doInitialize(params map[string]interface{}) (interface{}, error) {
	return InitResponse{
		ProtocolVersion: "2024-11-05",
		Capabilities: map[string]interface{}{
			"tools": map[string]interface{}{
				"listChanged": false,
			},
		},
		ServerInfo: ServerInfo{
			Name:    "NornicDB MCP Server",
			Version: "1.0.0",
		},
	}, nil
}

func (s *Server) doListTools() ListToolsResponse {
	return ListToolsResponse{
		Tools: s.ToolDefinitions(),
	}
}

func (s *Server) doCallTool(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	name, _ := params["name"].(string)
	args, _ := params["arguments"].(map[string]interface{})
	if args == nil {
		args = make(map[string]interface{})
	}

	// Allow callers to target a specific database at call time.
	// Precedence:
	// 1) arguments.database / arguments.db
	// 2) database already present in ctx (e.g. agentic loop default)
	// 3) otherwise route to the server's configured default database
	if dbArg := extractDatabaseArg(args); dbArg != "" {
		ctx = ContextWithDatabase(ctx, dbArg)
	}

	handler, ok := s.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}

	return handler(ctx, args)
}

// CallTool runs an MCP tool by name with the given arguments in memory.
// Use this to execute MCP tools (store, recall, discover, link, task, tasks) without HTTP.
// If ctx contains a database name (ContextWithDatabase), tools run against that database when
// DatabaseScopedExecutor is configured.
func (s *Server) CallTool(ctx context.Context, name string, arguments map[string]interface{}) (interface{}, error) {
	return s.doCallTool(ctx, map[string]interface{}{"name": name, "arguments": arguments})
}

// DefaultDatabaseName returns the configured default database name for this server.
// This is derived from the DB's namespaced storage (which is configured during DB open).
func (s *Server) DefaultDatabaseName() string {
	if s == nil || s.db == nil {
		return ""
	}
	if ns, ok := s.db.GetStorage().(*storage.NamespacedEngine); ok && ns != nil {
		return ns.Namespace()
	}
	return ""
}

// ToolDefinitions returns the MCP tool definitions for this server instance.
// The `database` parameter schema will reflect the configured default database name.
func (s *Server) ToolDefinitions() []Tool {
	return GetToolDefinitionsWithDefaultDatabase(s.DefaultDatabaseName())
}

// getExecutorAndGetNode returns the Cypher executor and node getter for this request.
// If context has a database name and DatabaseScopedExecutor is set, uses that database; else uses s.db.
// When dbName is empty but DatabaseScopedExecutor is set, uses DefaultDatabaseName() so store/recall/link
// always use the same database as the rest of the server (e.g. agentic loop).
func (s *Server) getExecutorAndGetNode(ctx context.Context) (exec *cypher.StorageExecutor, getNode func(context.Context, string) (*nornicdb.Node, error), err error) {
	dbName := DatabaseFromContext(ctx)
	if dbName == "" && s.config.DatabaseScopedExecutor != nil {
		dbName = s.DefaultDatabaseName()
	}
	if dbName != "" && s.config.DatabaseScopedExecutor != nil {
		e, gn, resolveErr := s.config.DatabaseScopedExecutor(dbName)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		if e == nil || gn == nil {
			return nil, nil, fmt.Errorf("database scoped executor unavailable for %q", dbName)
		}
		return e, func(ctx context.Context, id string) (*nornicdb.Node, error) {
			return gn(ctx, normalizeNodeElementID(id))
		}, nil
	}
	if s.db != nil {
		exec := s.db.GetCypherExecutor()
		if exec == nil {
			return nil, nil, fmt.Errorf("cypher executor unavailable")
		}
		return exec, func(ctx context.Context, id string) (*nornicdb.Node, error) {
			return s.db.GetNode(ctx, localNodeIDFromAny(id))
		}, nil
	}
	return nil, nil, nil
}

func (s *Server) storageForContext(ctx context.Context) (storage.Engine, error) {
	dbName := DatabaseFromContext(ctx)
	if dbName == "" && s.config.DatabaseScopedStorage != nil {
		dbName = s.DefaultDatabaseName()
	}
	if dbName != "" && s.config.DatabaseScopedStorage != nil {
		return s.config.DatabaseScopedStorage(dbName)
	}
	if s.db == nil {
		return nil, nil
	}
	store := s.db.GetStorage()
	if dbName == "" {
		return store, nil
	}
	// If the DB's storage is namespaced, reuse its inner engine for other databases.
	if ns, ok := store.(*storage.NamespacedEngine); ok {
		if ns.Namespace() == dbName {
			return store, nil
		}
		return storage.NewNamespacedEngine(ns.GetInnerEngine(), dbName), nil
	}
	// Best-effort: fall back to whatever storage we have (single-db mode).
	return store, nil
}

// =============================================================================
// Tool Handlers
// =============================================================================

// handleStore implements the store tool - creates a node in the database.
func (s *Server) handleStore(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	content := getString(args, "content")
	if content == "" {
		return nil, fmt.Errorf("content is required")
	}

	// Resolve labels: prefer explicit "labels" array, fall back to single "type" string.
	labels := getStringSlice(args, "labels")
	if len(labels) == 0 {
		nodeType := getString(args, "type")
		if nodeType == "" {
			nodeType = s.config.DefaultNodeLabel
			if nodeType == "" {
				nodeType = "Memory"
			}
		}
		labels = []string{nodeType}
	}

	title := getString(args, "title")
	if title == "" {
		title = generateTitle(content, 100)
	}

	tags := getStringSlice(args, "tags")
	metadata := getMap(args, "metadata")

	// Build properties
	props := map[string]interface{}{
		"title":      title,
		"content":    content,
		"created_at": time.Now().Format(time.RFC3339),
	}
	if len(tags) > 0 {
		props["tags"] = tags
	}
	for k, v := range metadata {
		props[k] = v
	}

	// Embeddings are internal-only - silently ignore any user-provided embedding
	// The database's embed queue will generate embeddings asynchronously
	delete(props, "embedding")
	delete(props, "embeddings")
	delete(props, "vector")

	// Store in database
	var nodeID string
	var embedded bool
	var receipt interface{}
	exec, _, execErr := s.getExecutorAndGetNode(ctx)
	if execErr != nil {
		return nil, execErr
	}
	if exec != nil {
		// Validate and sanitize all labels
		var safeLabels []string
		for _, l := range labels {
			safe := strings.ReplaceAll(l, "`", "")
			if !IsValidNodeType(safe) {
				return nil, fmt.Errorf("invalid label: %q (must be a valid identifier)", l)
			}
			safeLabels = append(safeLabels, safe)
		}
		labelExpr := strings.Join(safeLabels, ":")
		query := fmt.Sprintf("CREATE (n:%s) SET n += $props RETURN elementId(n) AS id", labelExpr)
		result, err := exec.Execute(ctx, query, map[string]interface{}{"props": props})
		if err != nil {
			return nil, fmt.Errorf("failed to store node: %w", err)
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, fmt.Errorf("store returned no node id")
		}
		id, ok := result.Rows[0][0].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("store returned invalid node id")
		}
		nodeID = id
		embedded = true
		if result.Metadata != nil {
			if rec, ok := result.Metadata["receipt"]; ok && rec != nil {
				receipt = rec
			}
		}
	} else {
		return nil, fmt.Errorf("store failed: no database executor available (data would not persist); ensure MCP is wired to the server's default database")
	}

	return StoreResult{
		ID:       nodeID,
		Title:    title,
		Embedded: embedded,
		Receipt:  receipt,
	}, nil
}

// handleRecall implements the recall tool - retrieves nodes from the database.
func (s *Server) handleRecall(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	id := getString(args, "id")
	nodeTypes := getStringSlice(args, "type")
	tags := getStringSlice(args, "tags")
	since := getString(args, "since")
	limit := getInt(args, "limit", 10)

	exec, getNode, execErr := s.getExecutorAndGetNode(ctx)
	if execErr != nil {
		return nil, execErr
	}

	// If ID provided, fetch specific node
	if id != "" {
		if getNode != nil {
			node, err := getNode(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("node not found: %s", id)
			}
			return RecallResult{
				Nodes: []Node{{
					ID:         normalizeNodeElementID(node.ID),
					Type:       getLabelType(node.Labels),
					Title:      getStringProp(node.Properties, "title"),
					Content:    getStringProp(node.Properties, "content"),
					Properties: sanitizePropertiesForLLM(node.Properties),
				}},
				Count: 1,
			}, nil
		}
		// Fallback without database
		return RecallResult{
			Nodes: []Node{{ID: id, Type: "memory"}},
			Count: 1,
		}, nil
	}

	// Query by filters
	if exec != nil {
		var b strings.Builder
		b.WriteString("MATCH (n)")

		conds := make([]string, 0, 1)
		params := map[string]interface{}{}
		if since != "" {
			// created_at is stored as RFC3339 string by store; lexical compare works for RFC3339 timestamps.
			conds = append(conds, "coalesce(n.created_at, '') >= $since")
			params["since"] = since
		}
		if len(conds) > 0 {
			b.WriteString(" WHERE ")
			b.WriteString(strings.Join(conds, " AND "))
		}
		b.WriteString(" RETURN n LIMIT $limit")

		result, err := exec.Execute(ctx, b.String(), params)
		if err != nil {
			return nil, fmt.Errorf("failed to recall nodes: %w", err)
		}

		nodes := make([]Node, 0, len(result.Rows))
		for _, row := range result.Rows {
			if len(row) == 0 {
				continue
			}
			snode, ok := row[0].(*storage.Node)
			if !ok || snode == nil {
				continue
			}
			props := toInterfaceMap(snode.Properties)
			if len(nodeTypes) > 0 && !containsLabel(snode.Labels, nodeTypes[0]) {
				continue
			}
			if len(tags) > 0 {
				nodeTags := getStringSliceProp(props, "tags")
				if len(nodeTags) == 0 || !hasAllTags(nodeTags, tags) {
					continue
				}
			}
			nodes = append(nodes, Node{
				ID:         normalizeNodeElementID(string(snode.ID)),
				Type:       getLabelType(snode.Labels),
				Title:      getStringProp(props, "title"),
				Content:    getStringProp(props, "content"),
				Properties: sanitizePropertiesForLLM(props),
			})
			if len(nodes) >= limit {
				break
			}
		}

		return RecallResult{Nodes: nodes, Count: len(nodes)}, nil
	}

	return RecallResult{
		Nodes: []Node{},
		Count: 0,
	}, nil
}

func hasAllTags(nodeTags, requested []string) bool {
	if len(requested) == 0 {
		return true
	}
	if len(nodeTags) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(nodeTags))
	for _, tag := range nodeTags {
		set[tag] = struct{}{}
	}
	for _, tag := range requested {
		if _, ok := set[tag]; !ok {
			return false
		}
	}
	return true
}

// handleDiscover implements the discover tool - semantic search with graph traversal.
func (s *Server) handleDiscover(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	query := getString(args, "query")
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	nodeTypes := getStringSlice(args, "type")
	limit := getInt(args, "limit", 10)
	minScore := getFloat64(args, "min_similarity", 0.0)
	// Depth for graph traversal (1-3, default 1 = no related nodes)
	depth := getInt(args, "depth", 1)
	if depth < 1 {
		depth = 1
	}
	if depth > 3 {
		depth = 3
	}

	method := "keyword"

	if s.db != nil {
		dbName := DatabaseFromContext(ctx) // empty = default database
		engine, err := s.storageForContext(ctx)
		if err != nil {
			return nil, err
		}
		svc, err := s.db.GetOrCreateSearchService(dbName, engine)
		if err == nil && svc != nil {
			// Try vector/hybrid search if embeddings enabled
			if s.embed != nil && s.config.EmbeddingEnabled {
				// IMPORTANT: don't rely on embedding failures to detect "too long" queries.
				// Instead, proactively chunk the query into embedding-safe segments.
				const (
					queryChunkSize    = 512
					queryChunkOverlap = 50
					maxQueryChunks    = 32 // safety cap to prevent pathological requests
					outerRRFK         = 60 // RRF constant for cross-chunk fusion
				)

				queryChunks, chunkErr := s.embed.ChunkText(query, queryChunkSize, queryChunkOverlap)
				if chunkErr != nil {
					return nil, chunkErr
				}
				if len(queryChunks) > maxQueryChunks {
					queryChunks = queryChunks[:maxQueryChunks]
				}

				// Embed each chunk and search; then fuse results across chunks using RRF (rank-based).
				queryEmbeddings, err := s.embed.EmbedBatch(ctx, queryChunks)
				if err == nil && len(queryEmbeddings) == len(queryChunks) && len(queryEmbeddings) > 0 {
					method = "vector"

					// Pull more candidates per chunk, then cut down after fusion.
					perChunkLimit := limit
					if perChunkLimit < 10 {
						perChunkLimit = 10
					}
					if len(queryChunks) > 1 && perChunkLimit < limit*3 {
						perChunkLimit = limit * 3
					}
					if perChunkLimit > 100 {
						perChunkLimit = 100
					}

					type fused struct {
						idLocal  string
						labels   []string
						title    string
						preview  string
						props    map[string]any
						scoreRRF float64 // outer RRF, used for ordering across chunks
						bestSim  float64 // max cosine similarity observed across chunks
					}
					fusedByID := make(map[string]*fused)

					for i, emb := range queryEmbeddings {
						if len(emb) == 0 {
							continue
						}
						chunkQuery := queryChunks[i]

						opts := search.GetAdaptiveRRFConfig(chunkQuery)
						opts.Limit = perChunkLimit
						if len(nodeTypes) > 0 {
							opts.Types = nodeTypes
						}

						resp, err := svc.Search(ctx, chunkQuery, emb, opts)
						if err != nil || resp == nil {
							continue
						}
						for rank, r := range resp.Results {
							id := r.ID
							f := fusedByID[id]
							if f == nil {
								f = &fused{
									idLocal:  id,
									labels:   r.Labels,
									title:    r.Title,
									preview:  r.ContentPreview,
									props:    r.Properties,
									scoreRRF: 0,
									bestSim:  0,
								}
								fusedByID[id] = f
							}
							// Outer RRF: 1/(k + rank), rank is 1-based. Used only for ordering.
							f.scoreRRF += 1.0 / (outerRRFK + float64(rank+1))
							// Track the strongest underlying score for this node.
							// search.SearchResult.Similarity carries the cosine score (or
							// reranker bi-score when stage-2 rerank is enabled, or BM25
							// raw score for BM25-only hits); see search.enrichResults.
							if r.Similarity > f.bestSim {
								f.bestSim = r.Similarity
							}
						}
					}

					// Build and sort fused list.
					fusedList := make([]*fused, 0, len(fusedByID))
					for _, f := range fusedByID {
						// Threshold is applied against the actual underlying similarity
						// (cosine for vector hits; raw BM25 for lexical-only hits) so
						// callers can use min_similarity meaningfully — e.g. 0.3 ≈
						// topically related, 0.5 ≈ strong cosine match. Prior behaviour
						// compared against the outer RRF score, which is rank-derived
						// (~0.0164 at rank 1 → 0.0091 at rank 50) and made the threshold
						// useless.
						if minScore > 0 && f.bestSim < minScore {
							continue
						}
						fusedList = append(fusedList, f)
					}

					sort.Slice(fusedList, func(i, j int) bool {
						return fusedList[i].scoreRRF > fusedList[j].scoreRRF
					})

					if limit <= 0 {
						limit = 10
					}
					if len(fusedList) > limit {
						fusedList = fusedList[:limit]
					}

					results := make([]SearchResult, 0, len(fusedList))
					for _, f := range fusedList {
						props := toInterfaceMap(f.props)
						res := SearchResult{
							ID:             normalizeNodeElementID(f.idLocal),
							Type:           getLabelType(f.labels),
							Title:          f.title,
							ContentPreview: f.preview,
							Similarity:     f.bestSim,
							Properties:     sanitizePropertiesForLLM(props),
						}
						if depth > 1 {
							res.Related = s.getRelatedNodes(ctx, res.ID, depth)
						}
						results = append(results, res)
					}

					return DiscoverResult{
						Results: results,
						Method:  method,
						Total:   len(results),
					}, nil
				}
			}

			// Keyword search (BM25-only)
			opts := search.GetAdaptiveRRFConfig(query)
			opts.Limit = limit
			if len(nodeTypes) > 0 {
				opts.Types = nodeTypes
			}
			resp, err := svc.Search(ctx, query, nil, opts)
			if err == nil && resp != nil {
				results := make([]SearchResult, 0, len(resp.Results))
				for _, r := range resp.Results {
					props := toInterfaceMap(r.Properties)
					// BM25-only fallback: r.Score is the outer fused/RRF ranking
					// score, not the raw backend similarity value. Surface
					// r.Similarity instead, which the search service sets to the
					// actual cosine when available and to the BM25 score in the
					// pure-BM25 path. Keeps min_similarity thresholds workable
					// across both code paths.
					res := SearchResult{
						ID:             normalizeNodeElementID(r.ID),
						Type:           getLabelType(r.Labels),
						Title:          r.Title,
						ContentPreview: r.ContentPreview,
						Similarity:     r.Similarity,
						Properties:     sanitizePropertiesForLLM(props),
					}
					if depth > 1 {
						res.Related = s.getRelatedNodes(ctx, res.ID, depth)
					}
					results = append(results, res)
				}
				return DiscoverResult{Results: results, Method: method, Total: len(results)}, nil
			}
		}
	}

	return DiscoverResult{
		Results: []SearchResult{},
		Method:  method,
		Total:   0,
	}, nil
}

// handleLink implements the link tool - creates relationships between nodes.
func (s *Server) handleLink(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	from := getString(args, "from")
	to := getString(args, "to")
	relation := getString(args, "relation")

	if from == "" {
		return nil, fmt.Errorf("from is required")
	}
	if to == "" {
		return nil, fmt.Errorf("to is required")
	}
	if relation == "" {
		return nil, fmt.Errorf("relation is required")
	}

	if !IsValidRelation(relation) {
		return nil, fmt.Errorf("invalid relation: %q (must be a non-empty valid identifier, e.g. relates_to, depends_on)", relation)
	}

	strength := getFloat64(args, "strength", 1.0)
	edgeProps := map[string]interface{}{
		"strength":   strength,
		"created_at": time.Now().Format(time.RFC3339),
	}
	// Merge caller-provided metadata into edge properties
	for k, v := range getMap(args, "metadata") {
		edgeProps[k] = v
	}

	// Create edge in database
	var edgeID string
	var fromNode, toNode Node
	var receipt interface{}

	fromEID := normalizeNodeElementID(from)
	toEID := normalizeNodeElementID(to)

	exec, getNode, execErr := s.getExecutorAndGetNode(ctx)
	if execErr != nil {
		return nil, execErr
	}
	if exec != nil && getNode != nil {
		// Verify source node exists and get its info (support elementId or id property)
		srcNode, srcEID, err := s.resolveNodeForLink(ctx, exec, getNode, from)
		if err != nil {
			return nil, fmt.Errorf("source node not found: %s", from)
		}
		fromNode = Node{
			ID:    srcEID,
			Type:  getLabelType(srcNode.Labels),
			Title: getStringProp(srcNode.Properties, "title"),
		}

		// Verify target node exists and get its info
		tgtNode, tgtEID, err := s.resolveNodeForLink(ctx, exec, getNode, to)
		if err != nil {
			return nil, fmt.Errorf("target node not found: %s", to)
		}
		toNode = Node{
			ID:    tgtEID,
			Type:  getLabelType(tgtNode.Labels),
			Title: getStringProp(tgtNode.Properties, "title"),
		}

		// Create the edge via Cypher to capture receipt metadata
		edgeType := strings.ReplaceAll(strings.ToUpper(relation), "`", "")
		query := fmt.Sprintf("MATCH (a), (b) WHERE elementId(a) = $from AND elementId(b) = $to CREATE (a)-[r:%s]->(b) SET r += $props RETURN elementId(r) AS id", edgeType)
		result, err := runCypherMutationWithRetry(ctx, exec, query, map[string]interface{}{
			"from":  fromNode.ID,
			"to":    toNode.ID,
			"props": edgeProps,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create edge: %w", err)
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, fmt.Errorf("link returned no edge id")
		}
		id, ok := result.Rows[0][0].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("link returned invalid edge id")
		}
		edgeID = id
		if result.Metadata != nil {
			if rec, ok := result.Metadata["receipt"]; ok && rec != nil {
				receipt = rec
			}
		}
	} else {
		// Fallback for testing (no db)
		edgeID = fmt.Sprintf("edge-%d", time.Now().UnixNano())
		fromNode = Node{ID: fromEID}
		toNode = Node{ID: toEID}
	}

	return LinkResult{
		EdgeID:  edgeID,
		From:    fromNode,
		To:      toNode,
		Receipt: receipt,
	}, nil
}

func (s *Server) resolveNodeForLink(ctx context.Context, exec *cypher.StorageExecutor, getNode func(context.Context, string) (*nornicdb.Node, error), id string) (*nornicdb.Node, string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, "", nornicdb.ErrNotFound
	}

	elementID := normalizeNodeElementID(id)
	if getNode != nil {
		if node, err := getNode(ctx, elementID); err == nil && node != nil {
			return node, elementID, nil
		}
	}

	// Fallback: allow linking by node property "id" or "_nodeId" when tool callers pass those IDs.
	if exec != nil {
		localID := localNodeIDFromAny(id)
		// First try internal id(n) match (handles raw storage IDs)
		result, err := exec.Execute(ctx, "MATCH (n) WHERE id(n) = $id RETURN n", map[string]interface{}{"id": localID})
		if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if snode, ok := result.Rows[0][0].(*storage.Node); ok && snode != nil {
				props := make(map[string]interface{}, len(snode.Properties))
				for k, val := range snode.Properties {
					props[k] = val
				}
				node := &nornicdb.Node{
					ID:         string(snode.ID),
					Labels:     snode.Labels,
					Properties: props,
					CreatedAt:  snode.CreatedAt,
				}
				return node, normalizeNodeElementID(node.ID), nil
			}
		}

		// Fallback: allow linking by node property "id" or "_nodeId"
		query := "MATCH (n) WHERE n.id = $id OR n._nodeId = $id RETURN n"
		result, err = exec.Execute(ctx, query, map[string]interface{}{"id": localID})
		if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if snode, ok := result.Rows[0][0].(*storage.Node); ok && snode != nil {
				props := make(map[string]interface{}, len(snode.Properties))
				for k, val := range snode.Properties {
					props[k] = val
				}
				node := &nornicdb.Node{
					ID:         string(snode.ID),
					Labels:     snode.Labels,
					Properties: props,
					CreatedAt:  snode.CreatedAt,
				}
				return node, normalizeNodeElementID(node.ID), nil
			}
		}
	}

	return nil, elementID, nornicdb.ErrNotFound
}

// handleTask implements the task tool - creates/manages tasks.
func (s *Server) handleTask(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	id := getString(args, "id")
	title := getString(args, "title")
	description := getString(args, "description")
	status := strings.TrimSpace(getString(args, "status"))
	priority := getString(args, "priority")
	dependsOn := getStringSlice(args, "depends_on")
	assign := getString(args, "assign")
	complete := getBool(args, "complete", false)
	del := getBool(args, "delete", false)

	// Back-compat: "done" → "completed"
	if strings.EqualFold(status, "done") {
		status = "completed"
	}

	exec, getNode, execErr := s.getExecutorAndGetNode(ctx)
	if execErr != nil {
		return nil, execErr
	}

	// Update existing task
	if id != "" {
		taskEID := normalizeNodeElementID(id)

		// Delete task
		if del {
			if exec == nil {
				return TaskResult{
					Task:       Node{ID: taskEID, Type: "Task"},
					NextAction: "Task deleted.",
				}, nil
			}
			result, err := runCypherMutationWithRetry(ctx, exec,
				"MATCH (t:Task) WHERE elementId(t) = $id DETACH DELETE t RETURN count(t) AS deleted",
				map[string]interface{}{"id": taskEID},
			)
			if err != nil {
				return nil, fmt.Errorf("failed to delete task: %w", err)
			}
			var deleted int64
			if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
				switch v := result.Rows[0][0].(type) {
				case int64:
					deleted = v
				case int:
					deleted = int64(v)
				case float64:
					deleted = int64(v)
				}
			}
			return TaskResult{
				Task:       Node{ID: taskEID, Type: "Task"},
				NextAction: fmt.Sprintf("Deleted %d task(s).", deleted),
			}, nil
		}

		// Fallback without database
		if exec == nil || getNode == nil {
			props := map[string]interface{}{}
			if status != "" {
				props["status"] = status
			}
			if complete {
				props["status"] = "completed"
			}
			return TaskResult{Task: Node{ID: taskEID, Type: "Task", Properties: props}}, nil
		}

		node, err := getNode(ctx, taskEID)
		if err != nil {
			return nil, fmt.Errorf("task not found: %s", id)
		}

		// Update properties
		updates := make(map[string]interface{})
		if complete {
			updates["status"] = "completed"
		} else if status != "" {
			updates["status"] = status
		} else {
			// Toggle status if not provided
			currentStatus := getStringProp(node.Properties, "status")
			if currentStatus == "pending" || currentStatus == "" {
				updates["status"] = "active"
			} else if currentStatus == "active" {
				updates["status"] = "completed"
			}
		}
		if title != "" {
			updates["title"] = title
		}
		if description != "" {
			updates["description"] = description
		}
		if priority != "" {
			updates["priority"] = priority
		}
		if assign != "" {
			updates["assigned_to"] = assign
		}
		updates["updated_at"] = time.Now().Format(time.RFC3339)

		result, err := runCypherMutationWithRetry(ctx, exec,
			"MATCH (t:Task) WHERE elementId(t) = $id SET t += $props RETURN elementId(t) AS id",
			map[string]interface{}{
				"id":    taskEID,
				"props": updates,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update task: %w", err)
		}
		var receipt interface{}
		if result.Metadata != nil {
			if rec, ok := result.Metadata["receipt"]; ok && rec != nil {
				receipt = rec
			}
		}
		updatedNode, err := getNode(ctx, taskEID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch updated task: %w", err)
		}

		return TaskResult{
			Task: Node{
				ID:         taskEID,
				Type:       "Task",
				Title:      getStringProp(updatedNode.Properties, "title"),
				Content:    getStringProp(updatedNode.Properties, "description"),
				Properties: sanitizePropertiesForLLM(updatedNode.Properties),
			},
			Receipt: receipt,
		}, nil
	}

	// Create new task
	if del {
		return nil, fmt.Errorf("id is required for delete")
	}
	if title == "" {
		return nil, fmt.Errorf("title is required for new tasks")
	}

	if complete {
		status = "completed"
	} else if status == "" {
		status = "pending"
	}
	if priority == "" {
		priority = "medium"
	}

	props := map[string]interface{}{
		"title":       title,
		"description": description,
		"status":      status,
		"priority":    priority,
		"created_at":  time.Now().Format(time.RFC3339),
	}
	if assign != "" {
		props["assigned_to"] = assign
	}

	var taskID string
	var receipt interface{}
	if exec != nil {
		query := "CREATE (t:Task) SET t += $props RETURN elementId(t) AS id"
		result, err := runCypherMutationWithRetry(ctx, exec, query, map[string]interface{}{"props": props})
		if err != nil {
			return nil, fmt.Errorf("failed to create task: %w", err)
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, fmt.Errorf("task create returned no id")
		}
		id, ok := result.Rows[0][0].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("task create returned invalid id")
		}
		taskID = id
		if result.Metadata != nil {
			if rec, ok := result.Metadata["receipt"]; ok && rec != nil {
				receipt = rec
			}
		}

		// Create dependency edges
		if len(dependsOn) > 0 {
			for _, depID := range dependsOn {
				if strings.TrimSpace(depID) == "" {
					continue
				}
				depElementID := normalizeNodeElementID(depID)
				if _, err := runCypherMutationWithRetry(ctx, exec,
					`MATCH (t:Task), (d:Task)
					 WHERE elementId(t) = $id AND elementId(d) = $dep
					 CREATE (t)-[:DEPENDS_ON]->(d)`,
					map[string]interface{}{"id": taskID, "dep": depElementID},
				); err != nil {
					return nil, fmt.Errorf("failed to create task dependency %q: %w", depID, err)
				}
			}
		}
	} else {
		taskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	return TaskResult{
		Task: Node{
			ID:         taskID,
			Type:       "Task",
			Title:      title,
			Content:    description,
			Properties: props,
		},
		NextAction: "Task created. Consider adding dependencies or subtasks.",
		Receipt:    receipt,
	}, nil
}

// handleTasks implements the tasks tool - queries multiple tasks.
// TaskRow is a typed struct for task query results.
type TaskRow struct {
	ID          string `cypher:"id" json:"id"`
	Title       string `cypher:"title" json:"title"`
	Description string `cypher:"description" json:"description"`
	Status      string `cypher:"status" json:"status"`
	Priority    string `cypher:"priority" json:"priority"`
	AssignedTo  string `cypher:"assigned_to" json:"assigned_to"`
}

// TaskStatRow is a typed struct for task statistics.
type TaskStatRow struct {
	Status   string `cypher:"status" json:"status"`
	Priority string `cypher:"priority" json:"priority"`
	Count    int64  `cypher:"count" json:"count"`
}

func (s *Server) handleTasks(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	statuses := getStringSlice(args, "status")
	priorities := getStringSlice(args, "priority")
	assignedTo := getString(args, "assigned_to")
	unblockedOnly := getBool(args, "unblocked_only", false)
	limit := getInt(args, "limit", 20)

	// Back-compat: "done" → "completed"
	for i := range statuses {
		if strings.EqualFold(statuses[i], "done") {
			statuses[i] = "completed"
		}
	}

	tasks := make([]Node, 0)
	stats := TaskStats{
		Total:      0,
		ByStatus:   map[string]int{"pending": 0, "active": 0, "completed": 0, "blocked": 0},
		ByPriority: map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0},
	}

	exec, _, execErr := s.getExecutorAndGetNode(ctx)
	if execErr != nil {
		return nil, execErr
	}
	if exec != nil {
		base := "MATCH (t:Task)"
		conditions := make([]string, 0, 4)
		params := map[string]interface{}{}

		if len(statuses) > 0 {
			conditions = append(conditions, "t.status IN $statuses")
			params["statuses"] = statuses
		}
		if len(priorities) > 0 {
			conditions = append(conditions, "t.priority IN $priorities")
			params["priorities"] = priorities
		}
		if assignedTo != "" {
			conditions = append(conditions, "t.assigned_to = $assigned_to")
			params["assigned_to"] = assignedTo
		}
		if len(conditions) > 0 {
			base += " WHERE " + strings.Join(conditions, " AND ")
		}

		// Task list (paged)
		listQuery := base + " RETURN elementId(t) AS id, t.title AS title, t.description AS description, t.status AS status, t.priority AS priority, t.assigned_to AS assigned_to ORDER BY t.created_at DESC LIMIT $limit"
		params["limit"] = limit

		result, err := exec.Execute(ctx, listQuery, params)
		if err != nil {
			return nil, fmt.Errorf("failed to list tasks: %w", err)
		}
		for _, row := range result.Rows {
			if len(row) < 6 {
				continue
			}
			id, _ := row[0].(string)
			titleVal, _ := row[1].(string)
			descVal, _ := row[2].(string)
			statusVal, _ := row[3].(string)
			priorityVal, _ := row[4].(string)
			assignedVal, _ := row[5].(string)
			if unblockedOnly {
				blocked, err := taskHasIncompleteDependencies(ctx, exec, id)
				if err != nil || blocked {
					continue
				}
			}
			tasks = append(tasks, Node{
				ID:      id,
				Type:    "Task",
				Title:   titleVal,
				Content: descVal,
				Properties: map[string]interface{}{
					"status":      statusVal,
					"priority":    priorityVal,
					"assigned_to": assignedVal,
				},
			})
		}

		// Stats for the filtered set (no limit)
		statsQuery := base + " RETURN elementId(t) AS id, t.status AS status, t.priority AS priority"
		statsResult, err := exec.Execute(ctx, statsQuery, params)
		if err == nil {
			for _, row := range statsResult.Rows {
				if len(row) < 3 {
					continue
				}
				id, _ := row[0].(string)
				st, _ := row[1].(string)
				pr, _ := row[2].(string)
				if unblockedOnly {
					blocked, checkErr := taskHasIncompleteDependencies(ctx, exec, id)
					if checkErr != nil || blocked {
						continue
					}
				}
				if st != "" {
					stats.ByStatus[st]++
				}
				if pr != "" {
					stats.ByPriority[pr]++
				}
				stats.Total++
			}
		}
	}

	return TasksResult{
		Tasks: tasks,
		Stats: stats,
	}, nil
}

func taskHasIncompleteDependencies(ctx context.Context, exec *cypher.StorageExecutor, taskID string) (bool, error) {
	if exec == nil || strings.TrimSpace(taskID) == "" {
		return false, nil
	}
	result, err := exec.Execute(ctx,
		`MATCH (t:Task)-[:DEPENDS_ON]->(dep:Task)
		 WHERE elementId(t) = $id AND coalesce(dep.status, '') <> 'completed'
		 RETURN count(dep)`,
		map[string]interface{}{"id": taskID},
	)
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return false, nil
	}
	switch v := result.Rows[0][0].(type) {
	case int64:
		return v > 0, nil
	case int:
		return v > 0, nil
	case float64:
		return v > 0, nil
	default:
		return false, nil
	}
}

// =============================================================================
// Helper Functions for Database Results
// =============================================================================

// getLabelType returns the first label as the type
func getLabelType(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return "Node"
}

func containsLabel(labels []string, label string) bool {
	for _, existing := range labels {
		if existing == label {
			return true
		}
	}
	return false
}

// getStringProp safely gets a string property
func getStringProp(props map[string]interface{}, key string) string {
	if props == nil {
		return ""
	}
	if v, ok := props[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// getStringSliceProp safely gets a string slice property
func getStringSliceProp(props map[string]interface{}, key string) []string {
	if props == nil {
		return nil
	}
	if v, ok := props[key]; ok {
		switch val := v.(type) {
		case []string:
			return val
		case []interface{}:
			result := make([]string, 0, len(val))
			for _, item := range val {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
	}
	return nil
}

// hasAnyTag checks if nodeTags contains any of targetTags
func hasAnyTag(nodeTags, targetTags []string) bool {
	for _, t := range targetTags {
		for _, nt := range nodeTags {
			if nt == t {
				return true
			}
		}
	}
	return false
}

const nodeElementIDPrefix = "4:nornicdb:"

func extractDatabaseArg(args map[string]interface{}) string {
	if args == nil {
		return ""
	}
	var db string
	if v, ok := args["database"].(string); ok {
		db = v
	}
	delete(args, "database")
	if db == "" {
		if v, ok := args["db"].(string); ok {
			db = v
		}
	}
	delete(args, "db")
	return strings.TrimSpace(db)
}

func normalizeNodeElementID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	// Accept any elementId-like string as-is (e.g. "4:db:id").
	if strings.HasPrefix(id, "4:") {
		return id
	}
	return nodeElementIDPrefix + id
}

func localNodeIDFromAny(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if strings.HasPrefix(id, nodeElementIDPrefix) {
		return strings.TrimPrefix(id, nodeElementIDPrefix)
	}
	// Best-effort: for other elementId formats, take the final segment.
	if strings.HasPrefix(id, "4:") {
		if idx := strings.LastIndex(id, ":"); idx != -1 && idx+1 < len(id) {
			return id[idx+1:]
		}
	}
	return id
}

func toInterfaceMap(props map[string]any) map[string]interface{} {
	if props == nil {
		return nil
	}
	out := make(map[string]interface{}, len(props))
	for k, v := range props {
		out[k] = v
	}
	return out
}

// =============================================================================
// Embedding Validation
// =============================================================================

// validateAndConvertEmbedding validates and converts a user-provided embedding.
// Returns a properly typed []float32 or an error with a clear explanation.
//
// Validation checks:
//   - Must be an array of numbers
//   - Dimensions must match configured EmbeddingDimensions (if set)
//   - Values should be in reasonable range (warning only)
func (s *Server) validateAndConvertEmbedding(input interface{}) ([]float32, error) {
	var embedding []float32

	switch v := input.(type) {
	case []float32:
		embedding = v
	case []float64:
		embedding = make([]float32, len(v))
		for i, f := range v {
			embedding[i] = float32(f)
		}
	case []interface{}:
		embedding = make([]float32, len(v))
		for i, val := range v {
			switch f := val.(type) {
			case float64:
				embedding[i] = float32(f)
			case float32:
				embedding[i] = f
			case int:
				embedding[i] = float32(f)
			case int64:
				embedding[i] = float32(f)
			default:
				return nil, fmt.Errorf("invalid embedding: element %d is not a number (got %T)", i, val)
			}
		}
	default:
		return nil, fmt.Errorf("invalid embedding: must be an array of numbers (got %T)", input)
	}

	// Validate not empty
	if len(embedding) == 0 {
		return nil, fmt.Errorf("invalid embedding: cannot be empty array")
	}

	// Validate dimensions if configured
	if s.config.EmbeddingDimensions > 0 && len(embedding) != s.config.EmbeddingDimensions {
		return nil, fmt.Errorf("invalid embedding dimensions: expected %d, got %d. "+
			"The configured embedding model (%s) requires %d-dimensional vectors",
			s.config.EmbeddingDimensions, len(embedding),
			s.config.EmbeddingModel, s.config.EmbeddingDimensions)
	}

	return embedding, nil
}

// =============================================================================
// Response Helpers
// =============================================================================

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) writeJSONRPCResult(w http.ResponseWriter, id interface{}, result interface{}) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func (s *Server) writeJSONRPCError(w http.ResponseWriter, id interface{}, code int, message, data string) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
			"data":    data,
		},
	})
}

// =============================================================================
// Utility Functions
// =============================================================================

// getString safely extracts a string from map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// getStringSlice safely extracts a string slice from map
func getStringSlice(m map[string]interface{}, key string) []string {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case []string:
			return val
		case []interface{}:
			result := make([]string, 0, len(val))
			for _, item := range val {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
	}
	return nil
}

// getInt safely extracts an int from map
func getInt(m map[string]interface{}, key string, defaultVal int) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case int64:
			return int(val)
		case float64:
			return int(val)
		}
	}
	return defaultVal
}

// getFloat64 safely extracts a float64 from map
func getFloat64(m map[string]interface{}, key string, defaultVal float64) float64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return val
		case int:
			return float64(val)
		case int64:
			return float64(val)
		}
	}
	return defaultVal
}

// getBool safely extracts a bool from map
func getBool(m map[string]interface{}, key string, defaultVal bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultVal
}

// getMap safely extracts a map from map
func getMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key]; ok {
		if mp, ok := v.(map[string]interface{}); ok {
			return mp
		}
	}
	return nil
}

// getRelatedNodes fetches related nodes up to the specified depth using graph traversal.
// Returns a slice of RelatedNode with relationship information.
func (s *Server) getRelatedNodes(ctx context.Context, nodeID string, depth int) []RelatedNode {
	if depth <= 1 {
		return nil
	}

	engine, err := s.storageForContext(ctx)
	if err != nil || engine == nil {
		return nil
	}

	startLocal := localNodeIDFromAny(nodeID)
	if startLocal == "" {
		return nil
	}

	maxDepth := depth - 1
	const maxRelated = 50

	type state struct {
		idLocal string
		dist    int
		path    []string // elementIds
	}

	visited := map[string]bool{startLocal: true}
	queue := []state{{
		idLocal: startLocal,
		dist:    0,
		path:    []string{normalizeNodeElementID(startLocal)},
	}}

	related := make([]RelatedNode, 0)
	for len(queue) > 0 && len(related) < maxRelated {
		cur := queue[0]
		queue = queue[1:]

		if cur.dist >= maxDepth {
			continue
		}

		// Outgoing edges
		out, _ := engine.GetOutgoingEdges(storage.NodeID(cur.idLocal))
		for _, e := range out {
			neighbor := string(e.EndNode)
			if neighbor == "" || visited[neighbor] {
				continue
			}
			visited[neighbor] = true
			nextPath := append(append([]string{}, cur.path...), normalizeNodeElementID(neighbor))

			ntype, ntitle := "Node", ""
			if n, err := engine.GetNode(storage.NodeID(neighbor)); err == nil && n != nil {
				props := toInterfaceMap(n.Properties)
				ntype = getLabelType(n.Labels)
				ntitle = getStringProp(props, "title")
				if ntitle == "" {
					ntitle = getStringProp(props, "name")
				}
			}

			related = append(related, RelatedNode{
				ID:           normalizeNodeElementID(neighbor),
				Type:         ntype,
				Title:        ntitle,
				Distance:     cur.dist + 1,
				Relationship: e.Type,
				Direction:    "outgoing",
				Path:         nextPath,
			})
			queue = append(queue, state{idLocal: neighbor, dist: cur.dist + 1, path: nextPath})
			if len(related) >= maxRelated {
				break
			}
		}

		if len(related) >= maxRelated {
			break
		}

		// Incoming edges
		in, _ := engine.GetIncomingEdges(storage.NodeID(cur.idLocal))
		for _, e := range in {
			neighbor := string(e.StartNode)
			if neighbor == "" || visited[neighbor] {
				continue
			}
			visited[neighbor] = true
			nextPath := append(append([]string{}, cur.path...), normalizeNodeElementID(neighbor))

			ntype, ntitle := "Node", ""
			if n, err := engine.GetNode(storage.NodeID(neighbor)); err == nil && n != nil {
				props := toInterfaceMap(n.Properties)
				ntype = getLabelType(n.Labels)
				ntitle = getStringProp(props, "title")
				if ntitle == "" {
					ntitle = getStringProp(props, "name")
				}
			}

			related = append(related, RelatedNode{
				ID:           normalizeNodeElementID(neighbor),
				Type:         ntype,
				Title:        ntitle,
				Distance:     cur.dist + 1,
				Relationship: e.Type,
				Direction:    "incoming",
				Path:         nextPath,
			})
			queue = append(queue, state{idLocal: neighbor, dist: cur.dist + 1, path: nextPath})
			if len(related) >= maxRelated {
				break
			}
		}
	}

	return related
}

// truncateString truncates a string to maxLen with ellipsis
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// sanitizePropertiesForLLM removes large fields like embeddings from properties
// before returning them to LLMs. Embeddings can be 1024+ floats which wastes
// tokens and provides no value to the LLM.
func sanitizePropertiesForLLM(props map[string]interface{}) map[string]interface{} {
	if props == nil {
		return nil
	}
	// Create a filtered copy
	filtered := make(map[string]interface{}, len(props))
	for k, v := range props {
		// Skip embedding-related fields
		switch k {
		case "embedding", "embedding_model", "embedding_dimensions",
			"has_embedding", "embedded_at":
			continue
		}
		// Skip any []float32 or []float64 arrays (likely embeddings)
		switch v.(type) {
		case []float32, []float64, []interface{}:
			// Check if it's a large numeric array (likely embedding)
			if arr, ok := v.([]interface{}); ok && len(arr) > 100 {
				continue
			}
			if arr, ok := v.([]float32); ok && len(arr) > 100 {
				continue
			}
			if arr, ok := v.([]float64); ok && len(arr) > 100 {
				continue
			}
		}
		filtered[k] = v
	}
	return filtered
}

// generateTitle creates a title from content if none provided
func generateTitle(content string, maxLen int) string {
	// Take first line or first N chars
	lines := strings.SplitN(content, "\n", 2)
	title := strings.TrimSpace(lines[0])
	return truncateString(title, maxLen)
}
