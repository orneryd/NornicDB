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
// Tool Surface (8 Tools):
//   - store: Store knowledge/memory as a node in the graph
//   - recall: Retrieve knowledge by ID or criteria
//   - discover: Semantic search by meaning (vector embeddings)
//   - link: Create relationships between nodes
//   - index: Index files/folders for search
//   - unindex: Stop indexing and remove files
//   - task: Create/manage individual tasks
//   - tasks: Query/list multiple tasks
//
// Example Usage:
//
//	db, _ := nornicdb.Open("./data", nil)
//	server := mcp.NewServer(db, nil)
//
//	// Start the server
//	if err := server.Start(":9042"); err != nil {
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
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// Embedder interface for generating embeddings (abstracts Ollama/OpenAI).
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
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

	// File watcher management
	watchers   map[string]*WatchInfo
	watchersMu sync.RWMutex
}

// ServerConfig holds MCP server configuration.
type ServerConfig struct {
	// Address to bind to (default: "localhost")
	Address string `yaml:"address"`
	// Port to listen on (default: 9042)
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
}

// DefaultServerConfig returns sensible defaults for the MCP server.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:          "localhost",
		Port:             9042,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     60 * time.Second,
		MaxRequestSize:   10 * 1024 * 1024, // 10MB
		EnableCORS:       true,
		EmbeddingEnabled: false, // Disabled by default, set Embedder externally
	}
}

// WatchInfo tracks an indexed folder
type WatchInfo struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	Patterns  []string  `json:"patterns"`
	Recursive bool      `json:"recursive"`
	StartedAt time.Time `json:"started_at"`
	FileCount int       `json:"file_count"`
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
		watchers: make(map[string]*WatchInfo),
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

// registerHandlers registers all MCP tool handlers.
func (s *Server) registerHandlers() {
	// Core memory tools
	s.handlers[ToolStore] = s.handleStore
	s.handlers[ToolRecall] = s.handleRecall
	s.handlers[ToolDiscover] = s.handleDiscover
	s.handlers[ToolLink] = s.handleLink

	// File indexing tools
	s.handlers[ToolIndex] = s.handleIndex
	s.handlers[ToolUnindex] = s.handleUnindex

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
		result, rpcErr = s.doCallTool(r.Context(), req.Params)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		Tools: GetToolDefinitions(),
	}
}

func (s *Server) doCallTool(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	name, _ := params["name"].(string)
	args, _ := params["arguments"].(map[string]interface{})

	handler, ok := s.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}

	return handler(ctx, args)
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

	nodeType := getString(args, "type")
	if nodeType == "" {
		nodeType = "Memory"
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
	if metadata != nil {
		for k, v := range metadata {
			props[k] = v
		}
	}

	// Handle embeddings: user-provided OR auto-generated
	var embedding []float32
	embedded := false

	// Check for user-provided embedding
	if userEmb, ok := args["embedding"]; ok && userEmb != nil {
		var err error
		embedding, err = s.validateAndConvertEmbedding(userEmb)
		if err != nil {
			return nil, err // Returns 400-style error with explanation
		}
		embedded = true
		props["embedding"] = embedding
	} else if s.embed != nil && s.config.EmbeddingEnabled {
		// Auto-generate embedding from content
		var err error
		embedding, err = s.embed.Embed(ctx, content)
		if err == nil {
			embedded = true
			props["embedding"] = embedding
		}
	}

	// Store in database
	var nodeID string
	if s.db != nil {
		labels := []string{nodeType}
		node, err := s.db.CreateNode(ctx, labels, props)
		if err != nil {
			return nil, fmt.Errorf("failed to store node: %w", err)
		}
		nodeID = node.ID
	} else {
		// Fallback for testing without database
		nodeID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}

	return StoreResult{
		ID:       nodeID,
		Title:    title,
		Embedded: embedded,
	}, nil
}

// handleRecall implements the recall tool - retrieves nodes from the database.
func (s *Server) handleRecall(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	id := getString(args, "id")
	nodeTypes := getStringSlice(args, "type")
	tags := getStringSlice(args, "tags")
	limit := getInt(args, "limit", 10)

	// If ID provided, fetch specific node
	if id != "" {
		if s.db != nil {
			node, err := s.db.GetNode(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("node not found: %s", id)
			}
			return RecallResult{
				Nodes: []Node{{
					ID:         node.ID,
					Type:       getLabelType(node.Labels),
					Title:      getStringProp(node.Properties, "title"),
					Content:    getStringProp(node.Properties, "content"),
					Properties: node.Properties,
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
	if s.db != nil {
		// Use label filter if provided
		label := ""
		if len(nodeTypes) > 0 {
			label = nodeTypes[0]
		}

		dbNodes, err := s.db.ListNodes(ctx, label, limit, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to list nodes: %w", err)
		}

		// Filter by tags if provided
		var nodes []Node
		for _, n := range dbNodes {
			// Filter by tags if specified
			if len(tags) > 0 {
				nodeTags := getStringSliceProp(n.Properties, "tags")
				if !hasAnyTag(nodeTags, tags) {
					continue
				}
			}

			nodes = append(nodes, Node{
				ID:         n.ID,
				Type:       getLabelType(n.Labels),
				Title:      getStringProp(n.Properties, "title"),
				Content:    getStringProp(n.Properties, "content"),
				Properties: n.Properties,
			})

			if len(nodes) >= limit {
				break
			}
		}

		return RecallResult{
			Nodes: nodes,
			Count: len(nodes),
		}, nil
	}

	return RecallResult{
		Nodes: []Node{},
		Count: 0,
	}, nil
}

// handleDiscover implements the discover tool - semantic search.
func (s *Server) handleDiscover(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	query := getString(args, "query")
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	nodeTypes := getStringSlice(args, "type")
	limit := getInt(args, "limit", 10)
	// Note: RRF hybrid search scores are typically 0.01-0.05 range (not 0-1 cosine similarity)
	// Use a low default threshold to include results, or set to 0 to return all
	minScore := getFloat64(args, "min_similarity", 0.0)

	method := "keyword"

	if s.db != nil {
		// Try vector search if embeddings enabled
		if s.embed != nil && s.config.EmbeddingEnabled {
			queryEmbedding, err := s.embed.Embed(ctx, query)
			if err == nil {
				method = "vector"
				// Use hybrid search with embedding
				dbResults, err := s.db.HybridSearch(ctx, query, queryEmbedding, nodeTypes, limit)
				if err == nil {
					var results []SearchResult
					for _, r := range dbResults {
						if minScore > 0 && r.Score < minScore {
							continue
						}
						content := getStringProp(r.Node.Properties, "content")
						preview := truncateString(content, 200)
						results = append(results, SearchResult{
							ID:             r.Node.ID,
							Type:           getLabelType(r.Node.Labels),
							Title:          getStringProp(r.Node.Properties, "title"),
							ContentPreview: preview,
							Similarity:     r.Score,
							Properties:     r.Node.Properties,
						})
					}
					return DiscoverResult{
						Results: results,
						Method:  method,
						Total:   len(results),
					}, nil
				}
			}
		}

		// Fall back to text search
		dbResults, err := s.db.Search(ctx, query, nodeTypes, limit)
		if err == nil {
			var results []SearchResult
			for _, r := range dbResults {
				content := getStringProp(r.Node.Properties, "content")
				preview := truncateString(content, 200)
				results = append(results, SearchResult{
					ID:             r.Node.ID,
					Type:           getLabelType(r.Node.Labels),
					Title:          getStringProp(r.Node.Properties, "title"),
					ContentPreview: preview,
					Similarity:     r.Score,
					Properties:     r.Node.Properties,
				})
			}
			return DiscoverResult{
				Results: results,
				Method:  method,
				Total:   len(results),
			}, nil
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
		return nil, fmt.Errorf("invalid relation: %s (valid: %v)", relation, ValidRelations)
	}

	strength := getFloat64(args, "strength", 1.0)
	edgeProps := map[string]interface{}{
		"strength":   strength,
		"created_at": time.Now().Format(time.RFC3339),
	}

	// Create edge in database
	var edgeID string
	var fromNode, toNode Node

	if s.db != nil {
		// Verify source node exists and get its info
		srcNode, err := s.db.GetNode(ctx, from)
		if err != nil {
			return nil, fmt.Errorf("source node not found: %s", from)
		}
		fromNode = Node{
			ID:    srcNode.ID,
			Type:  getLabelType(srcNode.Labels),
			Title: getStringProp(srcNode.Properties, "title"),
		}

		// Verify target node exists and get its info
		tgtNode, err := s.db.GetNode(ctx, to)
		if err != nil {
			return nil, fmt.Errorf("target node not found: %s", to)
		}
		toNode = Node{
			ID:    tgtNode.ID,
			Type:  getLabelType(tgtNode.Labels),
			Title: getStringProp(tgtNode.Properties, "title"),
		}

		// Create the edge
		edge, err := s.db.CreateEdge(ctx, from, to, strings.ToUpper(relation), edgeProps)
		if err != nil {
			return nil, fmt.Errorf("failed to create edge: %w", err)
		}
		edgeID = edge.ID
	} else {
		// Fallback for testing
		edgeID = fmt.Sprintf("edge-%d", time.Now().UnixNano())
		fromNode = Node{ID: from}
		toNode = Node{ID: to}
	}

	return LinkResult{
		EdgeID: edgeID,
		From:   fromNode,
		To:     toNode,
	}, nil
}

// handleIndex implements the index tool - indexes files for search.
func (s *Server) handleIndex(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	path := getString(args, "path")
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	patterns := getStringSlice(args, "patterns")
	if len(patterns) == 0 {
		patterns = []string{"*.go", "*.md", "*.txt", "*.py", "*.js", "*.ts"}
	}
	recursive := getBool(args, "recursive", true)
	embeddings := getBool(args, "embeddings", true)

	// Create watch ID
	watchID := fmt.Sprintf("watch-%d", time.Now().UnixNano())

	// Store watch info
	s.watchersMu.Lock()
	s.watchers[watchID] = &WatchInfo{
		ID:        watchID,
		Path:      path,
		Patterns:  patterns,
		Recursive: recursive,
		StartedAt: time.Now(),
		FileCount: 0,
	}
	s.watchersMu.Unlock()

	// Index files in background
	go s.indexFilesAsync(ctx, watchID, path, patterns, recursive, embeddings)

	return IndexResult{
		WatchID:     watchID,
		FilesQueued: 0, // Will be updated async
		Status:      "indexing",
	}, nil
}

// indexFilesAsync indexes files in the background.
func (s *Server) indexFilesAsync(ctx context.Context, watchID, basePath string, patterns []string, recursive, embeddings bool) {
	fileCount := 0

	walkFn := func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if info.IsDir() {
			if !recursive && filePath != basePath {
				return filepath.SkipDir
			}
			return nil
		}

		// Check if file matches patterns
		matched := false
		for _, pattern := range patterns {
			if m, _ := filepath.Match(pattern, info.Name()); m {
				matched = true
				break
			}
		}
		if !matched {
			return nil
		}

		// Read file content
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil
		}

		// Create node for file
		props := map[string]interface{}{
			"title":      info.Name(),
			"content":    string(content),
			"path":       filePath,
			"size":       info.Size(),
			"modified":   info.ModTime().Format(time.RFC3339),
			"indexed_at": time.Now().Format(time.RFC3339),
			"watch_id":   watchID,
		}

		// Generate embedding if enabled
		if embeddings && s.embed != nil && s.config.EmbeddingEnabled {
			if embedding, err := s.embed.Embed(ctx, string(content)); err == nil {
				props["embedding"] = embedding
			}
		}

		// Store in database
		if s.db != nil {
			s.db.CreateNode(ctx, []string{"File"}, props)
		}

		fileCount++
		return nil
	}

	filepath.Walk(basePath, walkFn)

	// Update watcher file count
	s.watchersMu.Lock()
	if w, exists := s.watchers[watchID]; exists {
		w.FileCount = fileCount
	}
	s.watchersMu.Unlock()
}

// handleUnindex implements the unindex tool - removes indexed files.
func (s *Server) handleUnindex(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	path := getString(args, "path")
	watchID := getString(args, "watch_id")

	if path == "" && watchID == "" {
		return nil, fmt.Errorf("path or watch_id is required")
	}

	s.watchersMu.Lock()
	defer s.watchersMu.Unlock()

	// Find and remove watch
	var removed *WatchInfo
	var targetWatchID string
	for id, w := range s.watchers {
		if id == watchID || w.Path == path {
			removed = w
			targetWatchID = id
			delete(s.watchers, id)
			break
		}
	}

	if removed == nil {
		return nil, fmt.Errorf("watch not found")
	}

	// Remove files from database
	removedFiles := 0
	removedChunks := 0
	if s.db != nil {
		// Query for files with this watch_id
		query := `MATCH (f:File) WHERE f.watch_id = $watch_id DELETE f RETURN count(f) as count`
		result, err := s.db.ExecuteCypher(ctx, query, map[string]interface{}{
			"watch_id": targetWatchID,
		})
		if err == nil && len(result.Rows) > 0 {
			if count, ok := result.Rows[0][0].(int64); ok {
				removedFiles = int(count)
			}
		}
	}

	return UnindexResult{
		RemovedFiles:  removedFiles,
		RemovedChunks: removedChunks,
	}, nil
}

// handleTask implements the task tool - creates/manages tasks.
func (s *Server) handleTask(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	id := getString(args, "id")
	title := getString(args, "title")
	description := getString(args, "description")
	status := getString(args, "status")
	priority := getString(args, "priority")
	dependsOn := getStringSlice(args, "depends_on")
	assign := getString(args, "assign")

	// Update existing task
	if id != "" {
		if s.db != nil {
			node, err := s.db.GetNode(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("task not found: %s", id)
			}

			// Update properties
			updates := make(map[string]interface{})
			if status != "" {
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

			updatedNode, err := s.db.UpdateNode(ctx, id, updates)
			if err != nil {
				return nil, fmt.Errorf("failed to update task: %w", err)
			}

			return TaskResult{
				Task: Node{
					ID:         updatedNode.ID,
					Type:       "Task",
					Title:      getStringProp(updatedNode.Properties, "title"),
					Content:    getStringProp(updatedNode.Properties, "description"),
					Properties: updatedNode.Properties,
				},
			}, nil
		}

		// Fallback without database
		return TaskResult{
			Task: Node{
				ID:   id,
				Type: "Task",
				Properties: map[string]interface{}{
					"status": status,
				},
			},
		}, nil
	}

	// Create new task
	if title == "" {
		return nil, fmt.Errorf("title is required for new tasks")
	}

	if status == "" {
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
	if s.db != nil {
		node, err := s.db.CreateNode(ctx, []string{"Task"}, props)
		if err != nil {
			return nil, fmt.Errorf("failed to create task: %w", err)
		}
		taskID = node.ID

		// Create dependency edges
		for _, depID := range dependsOn {
			s.db.CreateEdge(ctx, taskID, depID, "DEPENDS_ON", nil)
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

	tasks := make([]Node, 0)
	stats := TaskStats{
		Total:      0,
		ByStatus:   map[string]int{"pending": 0, "active": 0, "completed": 0, "blocked": 0},
		ByPriority: map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0},
	}

	if s.db != nil {
		// Build Cypher query
		query := "MATCH (t:Task)"
		conditions := []string{}
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
			query += " WHERE " + strings.Join(conditions, " AND ")
		}

		// For unblocked only, exclude tasks with incomplete dependencies
		if unblockedOnly {
			query += ` 
				AND NOT EXISTS {
					MATCH (t)-[:DEPENDS_ON]->(dep:Task) 
					WHERE dep.status <> 'completed'
				}`
		}

		query += " RETURN t.id, t.title, t.description, t.status, t.priority, t.assigned_to ORDER BY t.created_at DESC LIMIT $limit"
		params["limit"] = limit

		// Use typed execute for cleaner decoding
		result, err := nornicdb.ExecuteCypherTyped[TaskRow](s.db, ctx, query, params)
		if err == nil {
			for _, task := range result.Rows {
				taskNode := Node{
					ID:      task.ID,
					Type:    "Task",
					Title:   task.Title,
					Content: task.Description,
					Properties: map[string]interface{}{
						"status":      task.Status,
						"priority":    task.Priority,
						"assigned_to": task.AssignedTo,
					},
				}
				tasks = append(tasks, taskNode)

				// Update stats
				stats.Total++
				if task.Status != "" {
					stats.ByStatus[task.Status]++
				}
				if task.Priority != "" {
					stats.ByPriority[task.Priority]++
				}
			}
		}

		// Get overall stats using typed results
		statsQuery := `MATCH (t:Task) 
			RETURN t.status as status, t.priority as priority, count(t) as count`
		statsResult, _ := nornicdb.ExecuteCypherTyped[TaskStatRow](s.db, ctx, statsQuery, nil)
		if statsResult != nil {
			for _, row := range statsResult.Rows {
				if row.Status != "" {
					stats.ByStatus[row.Status] = int(row.Count)
				}
				if row.Priority != "" {
					stats.ByPriority[row.Priority] = int(row.Count)
				}
			}
		}
	}

	return TasksResult{
		Tasks: tasks,
		Stats: stats,
	}, nil
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

	// Validate dimensions if configured
	if s.config.EmbeddingDimensions > 0 && len(embedding) != s.config.EmbeddingDimensions {
		return nil, fmt.Errorf("invalid embedding dimensions: expected %d, got %d. "+
			"The configured embedding model (%s) requires %d-dimensional vectors",
			s.config.EmbeddingDimensions, len(embedding),
			s.config.EmbeddingModel, s.config.EmbeddingDimensions)
	}

	// Validate not empty
	if len(embedding) == 0 {
		return nil, fmt.Errorf("invalid embedding: cannot be empty array")
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

// generateTitle creates a title from content if none provided
func generateTitle(content string, maxLen int) string {
	// Take first line or first N chars
	lines := strings.SplitN(content, "\n", 2)
	title := strings.TrimSpace(lines[0])
	return truncateString(title, maxLen)
}
