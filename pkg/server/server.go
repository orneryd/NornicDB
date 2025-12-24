// Package server provides a Neo4j-compatible HTTP REST API server for NornicDB.
//
// This package implements the Neo4j HTTP API specification, making NornicDB compatible
// with existing Neo4j tools, drivers, and browsers while adding NornicDB-specific
// extensions for memory decay, vector search, and compliance features.
//
// Neo4j Compatibility:
//   - Discovery endpoint (/) returns Neo4j-compatible service information
//   - Transaction API (/db/{name}/tx) supports implicit and explicit transactions
//   - Cypher query execution with Neo4j response format
//   - Basic Auth and Bearer token authentication
//   - Error codes follow Neo4j conventions (Neo.ClientError.*)
//
// NornicDB Extensions:
//   - JWT authentication with RBAC
//   - Vector search endpoints (/nornicdb/search, /nornicdb/similar)
//   - Memory decay information (/nornicdb/decay)
//   - GDPR compliance endpoints (/gdpr/export, /gdpr/delete)
//   - Admin endpoints (/admin/stats, /admin/config)
//   - GPU acceleration control (/admin/gpu/*)
//
// Example Usage:
//
//	// Create server
//	db, _ := nornicdb.Open("./data", nil)
//	auth, _ := auth.NewAuthenticator(auth.DefaultAuthConfig())
//	config := server.DefaultConfig()
//
//	server, err := server.New(db, auth, config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Start server
//	if err := server.Start(); err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Server listening on %s\n", server.Addr())
//
//	// Use with Neo4j Browser
//	// Open: http://localhost:7474
//	// Connect URI: bolt://localhost:7687 (if Bolt server is running)
//	// Or use HTTP: http://localhost:7474/db/neo4j/tx/commit
//
//	// Use with Neo4j drivers
//	driver := neo4j.NewDriver("http://localhost:7474", neo4j.BasicAuth("admin", "password"))
//	session := driver.NewSession(neo4j.SessionConfig{})
//	result, _ := session.Run("MATCH (n) RETURN count(n)", nil)
//
//	// Graceful shutdown
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	server.Stop(ctx)
//
// Authentication:
//
// The server supports multiple authentication methods:
//
//  1. **Basic Auth** (Neo4j compatible):
//     Authorization: Basic base64(username:password)
//
//  2. **Bearer Token** (JWT):
//     Authorization: Bearer eyJhbGciOiJIUzI1NiIs...
//
//  3. **Cookie** (browser sessions):
//     Cookie: token=eyJhbGciOiJIUzI1NiIs...
//
//  4. **Query Parameter** (for SSE/WebSocket):
//     ?token=eyJhbGciOiJIUzI1NiIs...
//
// Neo4j HTTP API Endpoints:
//
//	GET  /                           - Discovery (service information)
//	GET  /db/{name}                  - Database information
//	POST /db/{name}/tx/commit       - Execute Cypher (implicit transaction)
//	POST /db/{name}/tx              - Begin explicit transaction
//	POST /db/{name}/tx/{id}         - Execute in transaction
//	POST /db/{name}/tx/{id}/commit  - Commit transaction
//	DELETE /db/{name}/tx/{id}       - Rollback transaction
//
// NornicDB Extension Endpoints:
//
//	Authentication:
//	  POST /auth/token                - Get JWT token
//	  POST /auth/logout               - Logout
//	  GET  /auth/me                   - Current user info
//	  POST /auth/api-token            - Generate API token (admin)
//	  GET  /auth/oauth/redirect       - OAuth redirect
//	  GET  /auth/oauth/callback        - OAuth callback
//	  GET  /auth/users                 - List users (admin)
//	  POST /auth/users                 - Create user (admin)
//	  GET  /auth/users/{username}      - Get user (admin)
//	  PUT  /auth/users/{username}      - Update user (admin)
//	  DELETE /auth/users/{username}    - Delete user (admin)
//
//	Search & Embeddings:
//	  POST /nornicdb/search           - Hybrid search (vector + BM25)
//	  POST /nornicdb/similar           - Vector similarity search
//	  GET  /nornicdb/decay             - Memory decay statistics
//	  POST /nornicdb/embed/trigger     - Trigger embedding generation
//	  GET  /nornicdb/embed/stats       - Embedding statistics
//	  POST /nornicdb/embed/clear       - Clear all embeddings (admin)
//	  POST /nornicdb/search/rebuild    - Rebuild search indexes
//
//	Admin & System:
//	  GET  /admin/stats               - System statistics (admin)
//	  GET  /admin/config               - Server configuration (admin)
//	  POST /admin/backup               - Create backup (admin)
//	  GET  /admin/gpu/status           - GPU status (admin)
//	  POST /admin/gpu/enable           - Enable GPU (admin)
//	  POST /admin/gpu/disable          - Disable GPU (admin)
//	  POST /admin/gpu/test              - Test GPU (admin)
//
//	GDPR Compliance:
//	  POST /gdpr/export                - GDPR data export (requires user_id and format in body)
//	  POST /gdpr/delete                - GDPR erasure request
//
//	GraphQL & AI:
//	  POST /graphql                    - GraphQL endpoint
//	  GET  /graphql/playground         - GraphQL Playground
//	  POST /mcp                        - MCP server endpoint
//	  POST /api/bifrost/chat/completions - Heimdall AI chat
//
// For complete API documentation, see: docs/api-reference/openapi.yaml
//
// Security Features:
//
//   - CORS support with configurable origins
//   - Request size limits (default 10MB)
//   - IP-based rate limiting (configurable per-minute/per-hour limits)
//   - Audit logging integration
//   - Panic recovery middleware
//   - TLS/HTTPS support
//
// Compliance:
//   - GDPR Art.15 (right of access) via /gdpr/export
//   - GDPR Art.17 (right to erasure) via /gdpr/delete
//   - HIPAA audit logging for all data access
//   - SOC2 access controls via RBAC
//
// ELI12 (Explain Like I'm 12):
//
// Think of this server like a restaurant:
//
//  1. **Neo4j compatibility**: We speak the same "language" as Neo4j, so existing
//     customers (tools/drivers) can order from our menu without learning new words.
//
//  2. **Authentication**: Like checking IDs at the door - we make sure you're allowed
//     to be here and what you're allowed to do.
//
//  3. **Endpoints**: Different "counters" for different services - one for regular
//     food (Cypher queries), one for special orders (vector search), one for the
//     manager's office (admin functions).
//
//  4. **Middleware**: Like security guards, cashiers, and cleaners who help every
//     customer but do different jobs (logging, auth, error handling).
//
// The server makes sure everyone gets served safely and efficiently!
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/graphql"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
)

// Errors for HTTP operations.
var (
	ErrServerClosed     = fmt.Errorf("server closed")
	ErrUnauthorized     = fmt.Errorf("unauthorized")
	ErrForbidden        = fmt.Errorf("forbidden")
	ErrBadRequest       = fmt.Errorf("bad request")
	ErrNotFound         = fmt.Errorf("not found")
	ErrMethodNotAllowed = fmt.Errorf("method not allowed")
	ErrInternalError    = fmt.Errorf("internal server error")
)

// embeddingCacheMemoryMB calculates approximate memory usage for embedding cache.
// Each cached embedding uses: cacheSize * dimensions * 4 bytes (float32).
func embeddingCacheMemoryMB(cacheSize, dimensions int) int {
	return cacheSize * dimensions * 4 / 1024 / 1024
}

// Config holds HTTP server configuration options.
//
// All settings have sensible defaults via DefaultConfig(). The server follows
// Neo4j conventions where applicable (default port 7474, timeouts, etc.).
//
// Example:
//
//	// Production configuration
//	config := &server.Config{
//		Address:           "0.0.0.0",
//		Port:              7474,
//		ReadTimeout:       30 * time.Second,
//		WriteTimeout:      60 * time.Second,
//		MaxRequestSize:    50 * 1024 * 1024, // 50MB for large imports
//		EnableCORS:        true,
//		CORSOrigins:       []string{"https://myapp.com"},
//		EnableCompression: true,
//		TLSCertFile:       "/etc/ssl/server.crt",
//		TLSKeyFile:        "/etc/ssl/server.key",
//	}
//
//	// Development configuration with CORS for local UI
//	config = server.DefaultConfig()
//	config.Port = 8080
//	config.EnableCORS = true
//	config.CORSOrigins = []string{"http://localhost:3000"} // Local dev UI only
type Config struct {
	// Address to bind to (default: "127.0.0.1" - localhost only for security)
	// Set to "0.0.0.0" to listen on all interfaces (required for Docker/external access)
	Address string
	// Port to listen on (default: 7474)
	Port int
	// ReadTimeout for requests
	ReadTimeout time.Duration
	// WriteTimeout for responses
	WriteTimeout time.Duration
	// IdleTimeout for keep-alive connections
	IdleTimeout time.Duration
	// MaxRequestSize in bytes (default: 10MB)
	MaxRequestSize int64
	// EnableCORS for cross-origin requests (default: false for security)
	EnableCORS bool
	// CORSOrigins allowed origins (default: empty - must be explicitly configured)
	// WARNING: Never use "*" with credentials - this is a CSRF vulnerability
	CORSOrigins []string
	// EnableCompression for responses
	EnableCompression bool

	// Rate Limiting Configuration (DoS protection)
	// RateLimitEnabled enables IP-based rate limiting (default: true)
	RateLimitEnabled bool
	// RateLimitPerMinute max requests per IP per minute (default: 100)
	RateLimitPerMinute int
	// RateLimitPerHour max requests per IP per hour (default: 3000)
	RateLimitPerHour int
	// RateLimitBurst max burst size for short request spikes (default: 20)
	RateLimitBurst int
	// TLSCertFile for HTTPS
	TLSCertFile string
	// TLSKeyFile for HTTPS
	TLSKeyFile string

	// MCP Configuration (Model Context Protocol)
	// MCPEnabled controls whether the MCP server is started (default: true)
	// Set to false to disable MCP tools entirely
	// Env: NORNICDB_MCP_ENABLED=true|false
	MCPEnabled bool

	// Embedding Configuration (for vector search)
	// EmbeddingEnabled turns on automatic embedding generation
	EmbeddingEnabled bool
	// EmbeddingProvider: "ollama" or "openai" or "local"
	EmbeddingProvider string
	// EmbeddingAPIURL is the base URL (e.g., http://localhost:11434)
	EmbeddingAPIURL string
	// EmbeddingModel is the model name (e.g., bge-m3)
	EmbeddingModel string
	// EmbeddingDimensions is expected vector size (e.g., 1024)
	EmbeddingDimensions int
	// EmbeddingCacheSize is max embeddings to cache (0 = disabled, default: 10000)
	// Each cached embedding uses ~4KB (1024 dims × 4 bytes)
	EmbeddingCacheSize int
	// EmbeddingAPIKey is the API key for authenticated embedding providers (OpenAI, Cloudflare Workers AI, etc.)
	// Env: NORNICDB_EMBEDDING_API_KEY
	EmbeddingAPIKey string
	// ModelsDir is the directory containing local GGUF models
	// Env: NORNICDB_MODELS_DIR (default: ./models)
	ModelsDir string

	// Slow Query Logging Configuration
	// SlowQueryEnabled turns on slow query logging (default: true)
	SlowQueryEnabled bool
	// SlowQueryThreshold is minimum duration to log (default: 100ms)
	// Queries taking longer than this will be logged
	SlowQueryThreshold time.Duration
	// SlowQueryLogFile is optional file path for slow query log
	// If empty, logs to stderr with other server logs
	SlowQueryLogFile string

	// Headless Mode Configuration
	// Headless disables the web UI and browser-related endpoints
	// Set to true for API-only deployments (e.g., embedded use, microservices)
	// Env: NORNICDB_HEADLESS=true|false
	Headless bool

	// BasePath for deployment behind a reverse proxy with URL prefix
	// Example: "/nornicdb" when deployed at https://example.com/nornicdb/
	// Leave empty for root deployment (default)
	// Env: NORNICDB_BASE_PATH
	BasePath string

	// Plugins Configuration
	// PluginsDir is the directory for APOC/function plugins
	// Env: NORNICDB_PLUGINS_DIR
	PluginsDir string
	// HeimdallPluginsDir is the directory for Heimdall plugins
	// Env: NORNICDB_HEIMDALL_PLUGINS_DIR
	HeimdallPluginsDir string

	// Features configuration (passed from main config loading)
	// This contains feature flags like HeimdallEnabled loaded from YAML/env
	Features *nornicConfig.FeatureFlagsConfig
}

// DefaultConfig returns Neo4j-compatible default server configuration.
//
// Defaults match Neo4j HTTP server settings:
//   - Port 7474 (Neo4j HTTP default)
//   - 30s read timeout
//   - 60s write timeout
//   - 120s idle timeout
//   - 10MB max request size
//   - CORS enabled for browser compatibility
//   - Compression enabled
//
// Embedding defaults (for MCP vector search):
//   - Enabled by default, connects to localhost:11434 (llama.cpp/Ollama)
//   - Model: bge-m3 (1024 dimensions)
//   - Falls back to text search if embeddings unavailable
//
// Environment Variables to override embedding config:
//
//	NORNICDB_EMBEDDING_ENABLED=true|false  - Enable/disable embeddings
//	NORNICDB_EMBEDDING_PROVIDER=openai     - API format: "openai" or "ollama"
//	NORNICDB_EMBEDDING_URL=http://...      - Embeddings API URL
//	NORNICDB_EMBEDDING_MODEL=bge-m3
//	NORNICDB_EMBEDDING_DIM=1024            - Vector dimensions
//
// Example:
//
//	config := server.DefaultConfig()
//	server, err := server.New(db, auth, config)
//
//	// Or customize
//	config = server.DefaultConfig()
//	config.Port = 8080
//	config.EnableCORS = false
//	server, err = server.New(db, auth, config)
func DefaultConfig() *Config {
	return &Config{
		// SECURITY: Bind to localhost only by default - prevents external access
		// Set Address to "0.0.0.0" for Docker/container deployments or external access
		Address:        "127.0.0.1",
		Port:           7474,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   60 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxRequestSize: 10 * 1024 * 1024, // 10MB
		// CORS enabled by default for ease of use (allows all origins)
		// Override via: NORNICDB_CORS_ENABLED=false or NORNICDB_CORS_ORIGINS=https://myapp.com
		// WARNING: "*" allows any origin - configure specific origins for production
		EnableCORS:        true,
		CORSOrigins:       []string{"*"}, // Allow all origins by default
		EnableCompression: true,

		// Rate limiting enabled by default to prevent DoS attacks
		// High limits for high-performance local/development use
		RateLimitEnabled:   false,
		RateLimitPerMinute: 10000,  // 10,000 requests/minute per IP (166/sec)
		RateLimitPerHour:   100000, // 100,000 requests/hour per IP
		RateLimitBurst:     1000,   // Allow large bursts for batch operations

		// MCP server enabled by default
		// Override: NORNICDB_MCP_ENABLED=false
		MCPEnabled: true,

		// Embedding defaults - connects to local llama.cpp/Ollama server
		// Override via environment variables:
		//   NORNICDB_EMBEDDING_ENABLED=false     - Disable embeddings entirely
		//   NORNICDB_EMBEDDING_PROVIDER=ollama   - Use "ollama" or "openai" format
		//   NORNICDB_EMBEDDING_URL=http://...    - Embeddings API URL
		//   NORNICDB_EMBEDDING_MODEL=...         - Model name
		//   NORNICDB_EMBEDDING_DIM=1024          - Vector dimensions
		EmbeddingEnabled:    true,
		EmbeddingProvider:   "openai", // llama.cpp uses OpenAI-compatible format
		EmbeddingAPIURL:     "http://localhost:11434",
		EmbeddingModel:      "bge-m3",
		EmbeddingDimensions: 1024,
		EmbeddingCacheSize:  10000, // ~40MB cache for 1024-dim vectors

		// Slow query logging enabled by default
		// Override via:
		//   NORNICDB_SLOW_QUERY_ENABLED=false
		//   NORNICDB_SLOW_QUERY_THRESHOLD=200ms
		//   NORNICDB_SLOW_QUERY_LOG=/var/log/nornicdb/slow.log
		SlowQueryEnabled:   false,
		SlowQueryThreshold: 100 * time.Millisecond,
		SlowQueryLogFile:   "", // Empty = log to stderr

		// Headless mode disabled by default (UI enabled)
		// Override via:
		//   NORNICDB_HEADLESS=true
		//   --headless flag
		Headless: false,
	}
}

// Server is the HTTP API server providing Neo4j-compatible endpoints.
//
// The server is thread-safe and handles concurrent requests. It maintains
// metrics, supports graceful shutdown, and integrates with audit logging.
//
// Lifecycle:
//  1. Create with New()
//  2. Optionally set audit logger with SetAuditLogger()
//  3. Start with Start()
//  4. Handle requests automatically
//  5. Stop with Stop() for graceful shutdown
//
// Example:
//
//	server := server.New(db, auth, config)
//
//	// Set up audit logging
//	auditLogger, _ := audit.NewLogger(audit.DefaultConfig())
//	server.SetAuditLogger(auditLogger)
//
//	// Start server
//	if err := server.Start(); err != nil {
//		log.Fatal(err)
//	}
//
//	// Server is now handling requests
//	fmt.Printf("Listening on %s\n", server.Addr())
//
//	// Get metrics
//	stats := server.Stats()
//	fmt.Printf("Requests: %d, Errors: %d\n", stats.RequestCount, stats.ErrorCount)
//
//	// Graceful shutdown
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	server.Stop(ctx)
type Server struct {
	config    *Config
	db        *nornicdb.DB
	dbManager *multidb.DatabaseManager // Manages multiple databases
	auth      *auth.Authenticator
	audit     *audit.Logger

	// MCP server for LLM tool interface
	mcpServer *mcp.Server

	// Heimdall - AI assistant for database management
	heimdallHandler *heimdall.Handler

	// GraphQL handler for GraphQL API
	graphqlHandler *graphql.Handler

	httpServer *http.Server
	listener   net.Listener

	// Rate limiter for DoS protection
	rateLimiter *IPRateLimiter

	// OAuth manager for OAuth 2.0 authentication
	oauthManager *auth.OAuthManager

	mu      sync.RWMutex
	closed  atomic.Bool
	started time.Time

	// Metrics
	requestCount   atomic.Int64
	errorCount     atomic.Int64
	activeRequests atomic.Int64

	// Slow query logging
	slowQueryLogger *log.Logger
	slowQueryCount  atomic.Int64

	// Cached search services per database (namespace-aware indexes)
	searchServicesMu sync.RWMutex
	searchServices   map[string]*search.Service
}

// IPRateLimiter provides IP-based rate limiting to prevent DoS attacks.
type IPRateLimiter struct {
	mu              sync.RWMutex
	counters        map[string]*ipRateLimitCounter
	perMinute       int
	perHour         int
	burst           int
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

type ipRateLimitCounter struct {
	mu          sync.Mutex
	minuteCount int
	hourCount   int
	minuteReset time.Time
	hourReset   time.Time
}

// NewIPRateLimiter creates a new IP-based rate limiter.
func NewIPRateLimiter(perMinute, perHour, burst int) *IPRateLimiter {
	rl := &IPRateLimiter{
		counters:        make(map[string]*ipRateLimitCounter),
		perMinute:       perMinute,
		perHour:         perHour,
		burst:           burst,
		cleanupInterval: 10 * time.Minute,
		stopCleanup:     make(chan struct{}),
	}
	// Start background cleanup of stale entries
	go rl.cleanupLoop()
	return rl
}

// Allow checks if a request from the given IP is allowed.
func (rl *IPRateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	counter, exists := rl.counters[ip]
	if !exists {
		counter = &ipRateLimitCounter{
			minuteReset: time.Now().Add(time.Minute),
			hourReset:   time.Now().Add(time.Hour),
		}
		rl.counters[ip] = counter
	}
	rl.mu.Unlock()

	counter.mu.Lock()
	defer counter.mu.Unlock()

	now := time.Now()

	// Reset minute counter if needed
	if now.After(counter.minuteReset) {
		counter.minuteCount = 0
		counter.minuteReset = now.Add(time.Minute)
	}

	// Reset hour counter if needed
	if now.After(counter.hourReset) {
		counter.hourCount = 0
		counter.hourReset = now.Add(time.Hour)
	}

	// Check limits
	if counter.minuteCount >= rl.perMinute {
		return false
	}
	if counter.hourCount >= rl.perHour {
		return false
	}

	// Increment counters
	counter.minuteCount++
	counter.hourCount++

	return true
}

// cleanupLoop periodically removes stale IP entries to prevent memory leaks.
func (rl *IPRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.cleanup()
		case <-rl.stopCleanup:
			return
		}
	}
}

func (rl *IPRateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for ip, counter := range rl.counters {
		counter.mu.Lock()
		// Remove if both counters have been reset (inactive for >1 hour)
		if now.After(counter.hourReset) {
			delete(rl.counters, ip)
		}
		counter.mu.Unlock()
	}
}

// Stop stops the cleanup goroutine.
func (rl *IPRateLimiter) Stop() {
	close(rl.stopCleanup)
}

// New creates a new HTTP server with the given database, authenticator, and configuration.
//
// The server is created but not started. Call Start() to begin accepting connections.
//
// Parameters:
//   - db: NornicDB database instance (required)
//   - authenticator: Authentication handler (can be nil to disable auth)
//   - config: Server configuration (uses DefaultConfig() if nil)
//
// Returns:
//   - Server instance ready to start
//   - Error if database is nil or configuration is invalid
//
// Example:
//
//	// With authentication
//	db, _ := nornicdb.Open("./data", nil)
//	auth, _ := auth.NewAuthenticator(auth.DefaultAuthConfig())
//	server, err := server.New(db, auth, nil) // Uses default config
//
//	// Without authentication (development)
//	server, err = server.New(db, nil, nil)
//
//	// Custom configuration
//	config := &server.Config{
//		Port: 8080,
//		EnableCORS: false,
//	}
//	server, err = server.New(db, auth, config)
func New(db *nornicdb.DB, authenticator *auth.Authenticator, config *Config) (*Server, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if db == nil {
		return nil, fmt.Errorf("database required")
	}

	// Note: GPU status is logged in main.go during GPU manager initialization
	// This avoids duplicate logs and provides more detailed information

	// Create MCP server for LLM tool interface (if enabled)
	var mcpServer *mcp.Server
	if config.MCPEnabled {
		mcpConfig := mcp.DefaultServerConfig()
		mcpConfig.EmbeddingEnabled = config.EmbeddingEnabled
		mcpConfig.EmbeddingModel = config.EmbeddingModel
		mcpConfig.EmbeddingDimensions = config.EmbeddingDimensions
		mcpServer = mcp.NewServer(db, mcpConfig)
	} else {
		log.Println("ℹ️  MCP server disabled via configuration")
	}

	// ==========================================================================
	// Heimdall - AI Assistant for Database Management
	// ==========================================================================
	var heimdallHandler *heimdall.Handler
	// Use features config passed from main.go (which loads from YAML + env)
	// Fall back to LoadFromEnv() if not provided (for backwards compatibility)
	var featuresConfig *nornicConfig.FeatureFlagsConfig
	if config.Features != nil {
		featuresConfig = config.Features
	} else {
		globalConfig := nornicConfig.LoadFromEnv()
		featuresConfig = &globalConfig.Features
	}
	if featuresConfig.HeimdallEnabled {
		log.Println("🛡️  Heimdall AI Assistant initializing...")
		// Configure token budget from environment variables
		heimdall.SetTokenBudget(featuresConfig)
		heimdallCfg := heimdall.ConfigFromFeatureFlags(featuresConfig)
		manager, err := heimdall.NewManager(heimdallCfg)
		if err != nil {
			log.Printf("⚠️  Heimdall initialization failed: %v", err)
			log.Println("   → AI Assistant will not be available")
			log.Println("   → Check NORNICDB_HEIMDALL_MODEL and NORNICDB_MODELS_DIR")
		} else {
			// Create database reader wrapper for Heimdall
			dbReader := &heimdallDBReader{db: db, features: featuresConfig}
			metricsReader := &heimdallMetricsReader{}
			heimdallHandler = heimdall.NewHandler(manager, heimdallCfg, dbReader, metricsReader)

			// Initialize Heimdall plugin subsystem
			subsystemMgr := heimdall.GetSubsystemManager()

			// Create the Heimdall invoker so plugins can call the LLM
			// This enables plugins to use SendPrompt() for synthesis, autonomous actions, etc.
			heimdallInvoker := heimdall.NewLiveHeimdallInvoker(
				subsystemMgr,
				manager, // Manager implements Generator interface
				heimdallHandler.Bifrost(),
				dbReader,
				metricsReader,
			)

			subsystemCtx := heimdall.SubsystemContext{
				Config:   heimdallCfg,
				Bifrost:  heimdallHandler.Bifrost(),
				Database: dbReader,
				Metrics:  metricsReader,
				Heimdall: heimdallInvoker, // Now plugins can call p.ctx.Heimdall.SendPrompt()
			}
			subsystemMgr.SetContext(subsystemCtx)

			// Register built-in actions (core system actions)
			heimdall.InitBuiltinActions()

			// Load plugins from configured directories
			// Auto-detects plugin types: function plugins (APOC) and Heimdall plugins
			// APOC plugins provide Cypher functions, Heimdall plugins provide AI actions
			if config.PluginsDir != "" {
				if err := nornicdb.LoadPluginsFromDir(config.PluginsDir, &subsystemCtx); err != nil {
					log.Printf("   ⚠️  Failed to load APOC plugins from %s: %v", config.PluginsDir, err)
				}
			}
			if config.HeimdallPluginsDir != "" && config.HeimdallPluginsDir != config.PluginsDir {
				if err := nornicdb.LoadPluginsFromDir(config.HeimdallPluginsDir, &subsystemCtx); err != nil {
					log.Printf("   ⚠️  Failed to load Heimdall plugins from %s: %v", config.HeimdallPluginsDir, err)
				}
			}

			// Log loaded plugins
			plugins := heimdall.ListHeimdallPlugins()
			actions := heimdall.ListHeimdallActions()
			log.Printf("✅ Heimdall AI Assistant ready")
			log.Printf("   → Model: %s", heimdallCfg.Model)
			log.Printf("   → Plugins: %d loaded, %d actions available", len(plugins), len(actions))
			log.Printf("   → Bifrost chat: /api/bifrost/chat/completions")
			log.Printf("   → Status: /api/bifrost/status")

			// Log available actions
			for _, actionName := range actions {
				log.Printf("   → Action: %s", actionName)
			}
		}
	} else {
		log.Println("ℹ️  Heimdall AI Assistant disabled (set NORNICDB_HEIMDALL_ENABLED=true to enable)")
	}

	// Configure embeddings if enabled
	// Local provider doesn't need API URL, others do
	embeddingsReady := config.EmbeddingEnabled && (config.EmbeddingProvider == "local" || config.EmbeddingAPIURL != "")
	if embeddingsReady {
		embedConfig := &embed.Config{
			Provider:   config.EmbeddingProvider,
			APIURL:     config.EmbeddingAPIURL,
			APIKey:     config.EmbeddingAPIKey,
			Model:      config.EmbeddingModel,
			Dimensions: config.EmbeddingDimensions,
			ModelsDir:  config.ModelsDir,
			Timeout:    30 * time.Second,
		}

		// Set API path based on provider (only for remote providers)
		switch config.EmbeddingProvider {
		case "ollama":
			embedConfig.APIPath = "/api/embeddings"
		case "openai":
			embedConfig.APIPath = "/v1/embeddings"
		case "local":
			// Local provider doesn't need API path
		default:
			// Default to Ollama format
			embedConfig.APIPath = "/api/embeddings"
		}

		// Initialize embeddings asynchronously to prevent startup blocking
		// Local GGUF models can take 5-30 seconds to load (graph compilation)
		log.Printf("🔄 Loading embedding model: %s (%s)...", embedConfig.Model, embedConfig.Provider)
		log.Println("   → Server will start immediately, embeddings available after model loads")

		go func() {
			// Use factory function for all providers
			embedder, err := embed.NewEmbedder(embedConfig)
			if err != nil {
				if config.EmbeddingProvider == "local" {
					log.Printf("⚠️  Local embedding model unavailable: %v", err)
				} else {
					log.Printf("⚠️  Embeddings endpoint unavailable (%s): %v", config.EmbeddingAPIURL, err)
				}
				log.Println("   → Falling back to full-text search only")
				// Don't set embedder - MCP server will use text search
				return
			}

			// Health check: test embedding before enabling
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, healthErr := embedder.Embed(ctx, "health check")
			cancel()

			if healthErr != nil {
				if config.EmbeddingProvider == "local" {
					log.Printf("⚠️  Local embedding model failed health check: %v", healthErr)
				} else {
					log.Printf("⚠️  Embeddings endpoint unavailable (%s): %v", config.EmbeddingAPIURL, healthErr)
				}
				log.Println("   → Falling back to full-text search only")
				return
			}

			// Wrap with caching if enabled (default: 10K cache)
			if config.EmbeddingCacheSize > 0 {
				embedder = embed.NewCachedEmbedder(embedder, config.EmbeddingCacheSize)
				log.Printf("✓ Embedding cache enabled: %d entries (~%dMB)",
					config.EmbeddingCacheSize, embeddingCacheMemoryMB(config.EmbeddingCacheSize, config.EmbeddingDimensions))
			}

			if config.EmbeddingProvider == "local" {
				log.Printf("✅ Embeddings ready: local GGUF (%s, %d dims)",
					config.EmbeddingModel, config.EmbeddingDimensions)
			} else {
				log.Printf("✅ Embeddings ready: %s (%s, %d dims)",
					config.EmbeddingAPIURL, config.EmbeddingModel, config.EmbeddingDimensions)
			}

			if mcpServer != nil {
				mcpServer.SetEmbedder(embedder)
			}
			// Share embedder with DB for auto-embed queue
			// The embed worker will wait for this to be set before processing
			db.SetEmbedder(embedder)
		}()
	}

	// Log authentication status
	if authenticator == nil || !authenticator.IsSecurityEnabled() {
		log.Println("⚠️  Authentication disabled")
	}

	// Initialize rate limiter if enabled
	var rateLimiter *IPRateLimiter
	if config.RateLimitEnabled {
		rateLimiter = NewIPRateLimiter(config.RateLimitPerMinute, config.RateLimitPerHour, config.RateLimitBurst)
		log.Printf("✓ Rate limiting enabled: %d/min, %d/hour per IP", config.RateLimitPerMinute, config.RateLimitPerHour)
	}

	// Initialize DatabaseManager for multi-database support
	// Get the base storage engine from the DB (unwraps the namespaced storage)
	// DatabaseManager will create NamespacedEngines for each database
	storageEngine := db.GetBaseStorageForManager()

	// Get multi-database config from global config
	globalConfig := nornicConfig.LoadFromEnv()
	multiDBConfig := &multidb.Config{
		DefaultDatabase:  globalConfig.Database.DefaultDatabase,
		SystemDatabase:   "system",
		MaxDatabases:     0, // Unlimited
		AllowDropDefault: false,
	}

	dbManager, err := multidb.NewDatabaseManager(storageEngine, multiDBConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database manager: %w", err)
	}

	// If authenticator is provided but doesn't have storage, initialize it with system database storage
	// This handles the case where authenticator was created before dbManager
	if authenticator != nil {
		// Authenticator should already have storage from main.go initialization
		// This is just a check to ensure system database is available
	}

	s := &Server{
		config:          config,
		db:              db,
		dbManager:       dbManager,
		auth:            authenticator,
		mcpServer:       mcpServer,
		heimdallHandler: heimdallHandler,
		graphqlHandler:  graphql.NewHandler(db, dbManager),
		rateLimiter:     rateLimiter,
		searchServices:  make(map[string]*search.Service),
	}

	// Initialize OAuth manager if authenticator is available
	if authenticator != nil {
		s.oauthManager = auth.NewOAuthManager(authenticator)
	}

	// Initialize slow query logger if file specified
	if config.SlowQueryEnabled && config.SlowQueryLogFile != "" {
		file, err := os.OpenFile(config.SlowQueryLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("⚠️  Failed to open slow query log file %s: %v", config.SlowQueryLogFile, err)
		} else {
			s.slowQueryLogger = log.New(file, "", log.LstdFlags)
			log.Printf("✓ Slow query logging to: %s (threshold: %v)", config.SlowQueryLogFile, config.SlowQueryThreshold)
		}
	} else if config.SlowQueryEnabled {
		log.Printf("✓ Slow query logging enabled (threshold: %v)", config.SlowQueryThreshold)
	}

	return s, nil
}

// SetAuditLogger sets the audit logger for compliance logging.
func (s *Server) SetAuditLogger(logger *audit.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = logger
}

// Start begins listening for HTTP connections on the configured address and port.
//
// The server starts in a separate goroutine, so this method returns immediately
// after successfully binding to the port. Use Addr() to get the actual listening
// address after starting.
//
// Returns:
//   - nil if server started successfully
//   - Error if failed to bind to port or server is already closed
//
// Example:
//
//	server := server.New(db, auth, config)
//
//	if err := server.Start(); err != nil {
//		log.Fatalf("Failed to start server: %v", err)
//	}
//
//	fmt.Printf("Server started on %s\n", server.Addr())
//
//	// Server is now accepting connections
//	// Keep main goroutine alive
//	select {}
//
// TLS Support:
//
//	If TLSCertFile and TLSKeyFile are configured, the server automatically
//	starts with HTTPS. Otherwise, it uses HTTP.
func (s *Server) Start() error {
	if s.closed.Load() {
		return ErrServerClosed
	}

	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.started = time.Now()

	// Build router
	mux := s.buildRouter()

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}

	// Start serving
	go func() {
		var err error
		if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
			err = s.httpServer.ServeTLS(listener, s.config.TLSCertFile, s.config.TLSKeyFile)
		} else {
			err = s.httpServer.Serve(listener)
		}
		if err != nil && err != http.ErrServerClosed {
			// Log error but don't crash
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Stop rate limiter cleanup goroutine
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}

	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// Addr returns the server's listen address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

// Stats returns current server runtime statistics.
//
// Statistics are updated in real-time by middleware and include:
//   - Uptime since server start
//   - Total request count
//   - Total error count
//   - Currently active requests
//
// Example:
//
//	stats := server.Stats()
//	fmt.Printf("Server uptime: %v\n", stats.Uptime)
//	fmt.Printf("Total requests: %d\n", stats.RequestCount)
//	fmt.Printf("Error rate: %.2f%%\n", float64(stats.ErrorCount)/float64(stats.RequestCount)*100)
//	fmt.Printf("Active requests: %d\n", stats.ActiveRequests)
//
//	// Use for monitoring/alerting
//	if stats.ErrorCount > 1000 {
//		alert("High error count detected")
//	}
//
// Thread-safe: Can be called concurrently from multiple goroutines.
func (s *Server) Stats() ServerStats {
	return ServerStats{
		Uptime:         time.Since(s.started),
		RequestCount:   s.requestCount.Load(),
		ErrorCount:     s.errorCount.Load(),
		ActiveRequests: s.activeRequests.Load(),
	}
}

// ServerStats holds server metrics.
type ServerStats struct {
	Uptime         time.Duration `json:"uptime"`
	RequestCount   int64         `json:"request_count"`
	ErrorCount     int64         `json:"error_count"`
	ActiveRequests int64         `json:"active_requests"`
}
