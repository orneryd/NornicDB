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
//	POST /auth/token                - OAuth 2.0 token endpoint
//	GET  /auth/me                   - Current user info
//	GET  /nornicdb/search           - Hybrid search (vector + BM25)
//	GET  /nornicdb/similar          - Vector similarity search
//	GET  /admin/stats               - System statistics
//	GET  /gdpr/export               - GDPR data export
//	POST /gdpr/delete               - GDPR erasure request
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/graphql"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	// Get the underlying storage engine from the DB
	storageEngine := db.GetStorage()

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

	s := &Server{
		config:          config,
		db:              db,
		dbManager:       dbManager,
		auth:            authenticator,
		mcpServer:       mcpServer,
		heimdallHandler: heimdallHandler,
		graphqlHandler:  graphql.NewHandler(db, dbManager),
		rateLimiter:     rateLimiter,
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

// =============================================================================
// Router Setup
// =============================================================================

func (s *Server) buildRouter() http.Handler {
	mux := http.NewServeMux()

	// ==========================================================================
	// UI Browser (if enabled and not in headless mode)
	// ==========================================================================
	var uiHandler *uiHandler
	if !s.config.Headless {
		var uiErr error
		uiHandler, uiErr = newUIHandler()
		if uiErr != nil {
			log.Printf("⚠️  UI initialization failed: %v", uiErr)
		}
		if uiHandler != nil {
			log.Println("📱 UI Browser enabled at /")
			// Serve UI assets
			mux.Handle("/assets/", uiHandler)
			mux.HandleFunc("/nornicdb.svg", func(w http.ResponseWriter, r *http.Request) {
				uiHandler.ServeHTTP(w, r)
			})
			// UI routes (SPA)
			mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
				uiHandler.ServeHTTP(w, r)
			})
			mux.HandleFunc("/security", func(w http.ResponseWriter, r *http.Request) {
				uiHandler.ServeHTTP(w, r)
			})
			// Auth config endpoint for UI
			mux.HandleFunc("/auth/config", s.handleAuthConfig)
		}
	} else {
		log.Println("🔇 Headless mode: UI disabled")
	}

	// ==========================================================================
	// Neo4j-Compatible Endpoints (for driver/browser compatibility)
	// ==========================================================================

	// Discovery endpoint (no auth required) - Neo4j compatible
	// Also serves UI for browser requests (unless headless)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Serve UI for browser requests at root (unless headless)
		if uiHandler != nil && isUIRequest(r) && r.URL.Path == "/" {
			uiHandler.ServeHTTP(w, r)
			return
		}
		// Otherwise serve Neo4j discovery JSON
		s.handleDiscovery(w, r)
	})

	// Neo4j HTTP API - Transaction endpoints (database-specific)
	// Pattern: /db/{databaseName}/tx/commit for implicit transactions
	// Pattern: /db/{databaseName}/tx for explicit transaction creation
	// Pattern: /db/{databaseName}/tx/{txId} for transaction operations
	// Pattern: /db/{databaseName}/tx/{txId}/commit for transaction commit
	mux.HandleFunc("/db/", s.withAuth(s.handleDatabaseEndpoint, auth.PermRead))

	// ==========================================================================
	// Health/Status/Metrics Endpoints
	// ==========================================================================
	// Health check is public (required for load balancers/k8s probes)
	mux.HandleFunc("/health", s.handleHealth)
	// Status and metrics require authentication to prevent information disclosure
	// These expose node counts, uptime, request stats that aid reconnaissance
	mux.HandleFunc("/status", s.withAuth(s.handleStatus, auth.PermRead))
	mux.HandleFunc("/metrics", s.withAuth(s.handleMetrics, auth.PermRead)) // Prometheus-compatible metrics

	// ==========================================================================
	// Authentication Endpoints (NornicDB additions)
	// ==========================================================================
	mux.HandleFunc("/auth/token", s.handleToken)
	mux.HandleFunc("/auth/logout", s.handleLogout)
	mux.HandleFunc("/auth/me", s.withAuth(s.handleMe, auth.PermRead))
	mux.HandleFunc("/auth/api-token", s.withAuth(s.handleGenerateAPIToken, auth.PermAdmin)) // Admin only - generate API tokens

	// User management (admin only)
	mux.HandleFunc("/auth/users", s.withAuth(s.handleUsers, auth.PermUserManage))
	mux.HandleFunc("/auth/users/", s.withAuth(s.handleUserByID, auth.PermUserManage))

	// ==========================================================================
	// NornicDB Extension Endpoints (additional features)
	// ==========================================================================

	// Vector search (NornicDB-specific)
	mux.HandleFunc("/nornicdb/search", s.withAuth(s.handleSearch, auth.PermRead))
	mux.HandleFunc("/nornicdb/similar", s.withAuth(s.handleSimilar, auth.PermRead))

	// Memory decay (NornicDB-specific)
	mux.HandleFunc("/nornicdb/decay", s.withAuth(s.handleDecay, auth.PermRead))

	// Embedding control (NornicDB-specific)
	mux.HandleFunc("/nornicdb/embed/trigger", s.withAuth(s.handleEmbedTrigger, auth.PermWrite))
	mux.HandleFunc("/nornicdb/embed/stats", s.withAuth(s.handleEmbedStats, auth.PermRead))
	mux.HandleFunc("/nornicdb/embed/clear", s.withAuth(s.handleEmbedClear, auth.PermAdmin))
	mux.HandleFunc("/nornicdb/search/rebuild", s.withAuth(s.handleSearchRebuild, auth.PermWrite))

	// Admin endpoints (NornicDB-specific)
	mux.HandleFunc("/admin/stats", s.withAuth(s.handleAdminStats, auth.PermAdmin))
	mux.HandleFunc("/admin/config", s.withAuth(s.handleAdminConfig, auth.PermAdmin))
	mux.HandleFunc("/admin/backup", s.withAuth(s.handleBackup, auth.PermAdmin))

	// GPU control endpoints (NornicDB-specific)
	mux.HandleFunc("/admin/gpu/status", s.withAuth(s.handleGPUStatus, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/enable", s.withAuth(s.handleGPUEnable, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/disable", s.withAuth(s.handleGPUDisable, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/test", s.withAuth(s.handleGPUTest, auth.PermAdmin))

	// GDPR compliance endpoints (NornicDB-specific)
	mux.HandleFunc("/gdpr/export", s.withAuth(s.handleGDPRExport, auth.PermRead))
	mux.HandleFunc("/gdpr/delete", s.withAuth(s.handleGDPRDelete, auth.PermDelete))

	// ==========================================================================
	// MCP Tool Endpoints (LLM-native interface)
	// ==========================================================================
	// Register MCP routes on the same server (port 7474)
	// Routes: /mcp, /mcp/initialize, /mcp/tools/list, /mcp/tools/call, /mcp/health
	// All MCP endpoints require authentication (PermRead minimum for tool calls)
	if s.mcpServer != nil {
		// Wrap MCP endpoints with auth - MCP is a powerful API that allows full DB access
		mux.HandleFunc("/mcp", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.mcpServer.ServeHTTP(w, r)
		}, auth.PermWrite))
		mux.HandleFunc("/mcp/initialize", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.mcpServer.ServeHTTP(w, r)
		}, auth.PermRead))
		mux.HandleFunc("/mcp/tools/list", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.mcpServer.ServeHTTP(w, r)
		}, auth.PermRead))
		mux.HandleFunc("/mcp/tools/call", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.mcpServer.ServeHTTP(w, r)
		}, auth.PermWrite))
		mux.HandleFunc("/mcp/health", s.handleHealth) // Health check can remain public
	}

	// ==========================================================================
	// Heimdall AI Assistant Endpoints (Bifrost chat interface)
	// ==========================================================================
	// Routes: /api/bifrost/status, /api/bifrost/chat/completions, /api/bifrost/events
	// All Bifrost endpoints require authentication (PermRead minimum)
	if s.heimdallHandler != nil {
		// Status endpoint - read access required
		mux.HandleFunc("/api/bifrost/status", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.heimdallHandler.ServeHTTP(w, r)
		}, auth.PermRead))
		// Chat completions - write access required (modifies state/generates content)
		mux.HandleFunc("/api/bifrost/chat/completions", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.heimdallHandler.ServeHTTP(w, r)
		}, auth.PermWrite))
		// SSE events - read access required
		mux.HandleFunc("/api/bifrost/events", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.heimdallHandler.ServeHTTP(w, r)
		}, auth.PermRead))
	}

	// ==========================================================================
	// GraphQL API Endpoints
	// ==========================================================================
	// Routes: /graphql (query/mutation), /graphql/playground (GraphQL IDE)
	// GraphQL provides a flexible query language for accessing NornicDB
	if s.graphqlHandler != nil {
		// GraphQL endpoint - read access required for queries
		mux.HandleFunc("/graphql", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.graphqlHandler.ServeHTTP(w, r)
		}, auth.PermRead))
		// GraphQL Playground - interactive IDE (read access required)
		mux.HandleFunc("/graphql/playground", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.graphqlHandler.Playground().ServeHTTP(w, r)
		}, auth.PermRead))
		log.Println("📊 GraphQL API enabled at /graphql")
	}

	// Wrap with middleware (order matters: outermost runs first)
	// Security middleware validates all tokens, URLs, and headers FIRST
	securityMiddleware := security.NewSecurityMiddleware()
	handler := securityMiddleware.ValidateRequest(mux)
	handler = s.corsMiddleware(handler)
	handler = s.rateLimitMiddleware(handler) // Rate limit after CORS preflight
	handler = s.loggingMiddleware(handler)
	handler = s.recoveryMiddleware(handler)
	handler = s.metricsMiddleware(handler)
	// Base path middleware runs FIRST (outermost) to strip prefix before routing
	handler = s.basePathMiddleware(handler)

	return handler
}

// =============================================================================
// Middleware
// =============================================================================

// withAuth wraps a handler with authentication and authorization.
// Supports both Neo4j Basic Auth and Bearer JWT tokens.
func (s *Server) withAuth(handler http.HandlerFunc, requiredPerm auth.Permission) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if auth is enabled
		if s.auth == nil || !s.auth.IsSecurityEnabled() {
			// Auth disabled - allow all
			handler(w, r)
			return
		}

		var claims *auth.JWTClaims
		var err error

		// Try Basic Auth first (Neo4j compatibility)
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Basic ") {
			claims, err = s.handleBasicAuth(authHeader, r)
		} else {
			// Try Bearer/JWT token extraction
			// Check both "nornicdb_token" (preferred) and "token" (legacy) cookies
			cookieToken := getCookie(r, "nornicdb_token")
			if cookieToken == "" {
				cookieToken = getCookie(r, "token")
			}
			token := auth.ExtractToken(
				authHeader,
				r.Header.Get("X-API-Key"),
				cookieToken,
				r.URL.Query().Get("token"),
				r.URL.Query().Get("api_key"),
			)

			if token == "" {
				s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", "No authentication provided")
				return
			}

			claims, err = s.auth.ValidateToken(token)
		}

		if err != nil {
			s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", err.Error())
			return
		}

		// Check permission
		if !hasPermission(claims.Roles, requiredPerm) {
			s.logAudit(r, claims.Sub, "access_denied", false,
				fmt.Sprintf("required permission: %s", requiredPerm))
			s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", "insufficient permissions")
			return
		}

		// Add claims to request context
		ctx := context.WithValue(r.Context(), contextKeyClaims, claims)
		handler(w, r.WithContext(ctx))
	}
}

// handleBasicAuth handles Neo4j-compatible Basic authentication.
func (s *Server) handleBasicAuth(authHeader string, r *http.Request) (*auth.JWTClaims, error) {
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid basic auth format")
	}

	username, password := parts[0], parts[1]

	// Authenticate and get token
	_, user, err := s.auth.Authenticate(username, password, getClientIP(r), r.UserAgent())
	if err != nil {
		return nil, err
	}

	// Convert user to claims
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}

	return &auth.JWTClaims{
		Sub:      user.ID,
		Username: user.Username,
		Email:    user.Email,
		Roles:    roles,
	}, nil
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.config.EnableCORS {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			allowed := false
			isWildcard := false
			for _, o := range s.config.CORSOrigins {
				if o == "*" {
					allowed = true
					isWildcard = true
					break
				}
				if o == origin {
					allowed = true
					break
				}
			}

			if allowed {
				// SECURITY: Never use wildcard with credentials - this is a CSRF vector
				// When wildcard is configured, don't send credentials header
				if isWildcard {
					w.Header().Set("Access-Control-Allow-Origin", "*")
					// Deliberately NOT setting Allow-Credentials with wildcard
				} else if origin != "" {
					// Specific origin - safe to allow credentials
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-API-Key")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}

			// Handle preflight
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware applies IP-based rate limiting to prevent DoS attacks.
// Returns 429 Too Many Requests when limits are exceeded.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip rate limiting if disabled
		if s.rateLimiter == nil {
			next.ServeHTTP(w, r)
			return
		}

		// Skip rate limiting for health checks (k8s probes, load balancers)
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Extract client IP (handle proxies via X-Forwarded-For)
		ip := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			// Take first IP in chain (original client)
			if idx := strings.Index(forwarded, ","); idx > 0 {
				ip = strings.TrimSpace(forwarded[:idx])
			} else {
				ip = strings.TrimSpace(forwarded)
			}
		} else if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			ip = realIP
		}

		// Check rate limit
		if !s.rateLimiter.Allow(ip) {
			w.Header().Set("Retry-After", "60")
			s.writeNeo4jError(w, http.StatusTooManyRequests,
				"Neo.ClientError.Request.TooManyRequests",
				"Rate limit exceeded. Please slow down your requests.")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		// Log request (skip health checks for noise reduction)
		if r.URL.Path != "/health" {
			duration := time.Since(start)
			s.logRequest(r, wrapped.status, duration)
		}
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Log panic summary to stderr (without stack trace to prevent info exposure)
				// #nosec CWE-209 -- Panic type only logged to stderr, not exposed to clients
				log.Printf("PANIC: %v", err)
				// Stack trace only in debug mode, written to stderr
				if os.Getenv("NORNICDB_DEBUG") == "true" {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					// #nosec CWE-209 -- Debug-only, stderr output, not exposed to clients
					log.Printf("Stack trace:\n%s", buf[:n])
				}

				s.errorCount.Add(1)
				s.writeError(w, http.StatusInternalServerError, "internal server error", ErrInternalError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requestCount.Add(1)
		s.activeRequests.Add(1)
		defer s.activeRequests.Add(-1)

		next.ServeHTTP(w, r)
	})
}

// =============================================================================
// Neo4j-Compatible Database Endpoint Handler
// =============================================================================

// handleDatabaseEndpoint routes /db/{databaseName}/... requests
// Implements Neo4j HTTP API transaction model:
//
//	POST /db/{dbName}/tx/commit - implicit transaction (query and commit)
//	POST /db/{dbName}/tx - open explicit transaction
//	POST /db/{dbName}/tx/{txId} - execute in open transaction
//	POST /db/{dbName}/tx/{txId}/commit - commit transaction
//	DELETE /db/{dbName}/tx/{txId} - rollback transaction
func (s *Server) handleDatabaseEndpoint(w http.ResponseWriter, r *http.Request) {
	// Parse path: /db/{databaseName}/...
	path := strings.TrimPrefix(r.URL.Path, "/db/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.Invalid", "database name required")
		return
	}

	dbName := parts[0]
	remaining := parts[1:]

	// Route based on remaining path
	switch {
	case len(remaining) == 0:
		// /db/{dbName} - database info
		s.handleDatabaseInfo(w, r, dbName)

	case remaining[0] == "tx":
		// Transaction endpoints
		s.handleTransactionEndpoint(w, r, dbName, remaining[1:])

	case remaining[0] == "cluster":
		// /db/{dbName}/cluster - cluster status
		s.handleClusterStatus(w, r, dbName)

	default:
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown endpoint")
	}
}

// getExecutorForDatabase returns a Cypher executor scoped to the specified database.
//
// This method provides database isolation by creating a Cypher executor that operates
// on a namespaced storage engine. All queries executed through the returned executor
// will only see data in the specified database.
//
// Parameters:
//   - dbName: The name of the database to get an executor for
//
// Returns:
//   - *cypher.StorageExecutor: A Cypher executor scoped to the database
//   - error: Returns an error if the database doesn't exist or cannot be accessed
//
// Example:
//
//	executor, err := server.getExecutorForDatabase("tenant_a")
//	if err != nil {
//		return err // Database doesn't exist
//	}
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
//
// Thread Safety:
//   - Safe for concurrent use
//   - Each call creates a new executor instance (lightweight operation)
//
// Performance:
//   - The executor is created on-demand for each request
//   - Storage engines are cached by DatabaseManager for efficiency
//   - Overhead is minimal (just executor creation, not storage initialization)
func (s *Server) getExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	// Get namespaced storage for this database
	// This returns a NamespacedEngine that automatically prefixes all keys
	// with the database name, ensuring complete data isolation
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return nil, err
	}

	// Create executor scoped to this database
	// The executor will only see data in the namespaced storage
	executor := cypher.NewStorageExecutor(storage)

	// Set DatabaseManager for system commands (CREATE/DROP/SHOW DATABASE)
	// Wrap DatabaseManager to implement the interface expected by executor
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager})

	return executor, nil
}

// databaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface, avoiding import cycles.
type databaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *databaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}

func (a *databaseManagerAdapter) DropDatabase(name string) error {
	return a.manager.DropDatabase(name)
}

func (a *databaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) Exists(name string) bool {
	return a.manager.Exists(name)
}

func (a *databaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}

func (a *databaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}

func (a *databaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}

func (a *databaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}

func (a *databaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	// Convert interface{} to *multidb.Limits
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}

func (a *databaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}

func (a *databaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	// Convert []interface{} to []multidb.ConstituentRef
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			// Try to convert from map
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        getString(m, "alias"),
					DatabaseName: getString(m, "database_name"),
					Type:         getString(m, "type"),
					AccessMode:   getString(m, "access_mode"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}

func (a *databaseManagerAdapter) DropCompositeDatabase(name string) error {
	return a.manager.DropCompositeDatabase(name)
}

func (a *databaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getString(m, "alias"),
			DatabaseName: getString(m, "database_name"),
			Type:         getString(m, "type"),
			AccessMode:   getString(m, "access_mode"),
		}
	} else if r, ok := constituent.(multidb.ConstituentRef); ok {
		ref = r
	} else {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}

func (a *databaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}

func (a *databaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(constituents))
	for i, c := range constituents {
		result[i] = c
	}
	return result, nil
}

func (a *databaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}

// Helper function to get string from map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// databaseInfoAdapter wraps multidb.DatabaseInfo to implement
// cypher.DatabaseInfoInterface.
type databaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *databaseInfoAdapter) Name() string {
	return a.info.Name
}

func (a *databaseInfoAdapter) Type() string {
	return a.info.Type
}

func (a *databaseInfoAdapter) Status() string {
	return a.info.Status
}

func (a *databaseInfoAdapter) IsDefault() bool {
	return a.info.IsDefault
}

func (a *databaseInfoAdapter) CreatedAt() time.Time {
	return a.info.CreatedAt
}

// handleDatabaseInfo returns database information for the specified database.
//
// This endpoint provides metadata about a database including its name, status,
// whether it's the default database, and current statistics (node and edge counts).
//
// Endpoint: GET /db/{dbName}
//
// Parameters:
//   - dbName: The name of the database to get information about
//
// Response (200 OK):
//
//	{
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 1234,
//	  "edgeCount": 5678
//	}
//
// Errors:
//   - 404 Not Found: Database doesn't exist (Neo.ClientError.Database.DatabaseNotFound)
//   - 500 Internal Server Error: Failed to access database (Neo.ClientError.Database.General)
//
// Example:
//
//	GET /db/tenant_a
//	Response: {
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 100,
//	  "edgeCount": 50
//	}
//
// Thread Safety:
//   - Safe for concurrent use
//   - Statistics are read from namespaced storage (thread-safe)
//
// Performance:
//   - Node and edge counts are computed on-demand
//   - For large databases, this may take a few milliseconds
//   - Consider caching if this endpoint is called frequently
func (s *Server) handleDatabaseInfo(w http.ResponseWriter, r *http.Request, dbName string) {
	// Check if database exists
	// This is a fast lookup in the DatabaseManager's metadata
	if !s.dbManager.Exists(dbName) {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound",
			fmt.Sprintf("Database '%s' not found", dbName))
		return
	}

	// Get storage for this database to get stats
	// This returns a NamespacedEngine that provides isolated access
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.Database.General",
			fmt.Sprintf("Failed to access database: %v", err))
		return
	}

	// Get stats from the namespaced storage
	// These counts reflect only data in this database (due to namespacing)
	nodeCount, err := storage.NodeCount()
	if err != nil {
		// Log error but continue with 0 count
		nodeCount = 0
	}
	edgeCount, err := storage.EdgeCount()
	if err != nil {
		// Log error but continue with 0 count
		edgeCount = 0
	}

	// Check if this is the default database
	// The default database is configured at startup and cannot be dropped
	defaultDB := s.dbManager.DefaultDatabaseName()

	response := map[string]interface{}{
		"name":      dbName,
		"status":    "online",
		"default":   dbName == defaultDB,
		"nodeCount": nodeCount,
		"edgeCount": edgeCount,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleClusterStatus returns cluster status (standalone mode)
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request, dbName string) {
	response := map[string]interface{}{
		"mode":     "standalone",
		"database": dbName,
		"status":   "online",
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleTransactionEndpoint routes transaction-related requests
func (s *Server) handleTransactionEndpoint(w http.ResponseWriter, r *http.Request, dbName string, remaining []string) {
	switch {
	case len(remaining) == 0:
		// POST /db/{dbName}/tx - open new transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		s.handleOpenTransaction(w, r, dbName)

	case remaining[0] == "commit" && len(remaining) == 1:
		// POST /db/{dbName}/tx/commit - implicit transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		s.handleImplicitTransaction(w, r, dbName)

	case len(remaining) == 1:
		// POST/DELETE /db/{dbName}/tx/{txId}
		txID := remaining[0]
		switch r.Method {
		case http.MethodPost:
			s.handleExecuteInTransaction(w, r, dbName, txID)
		case http.MethodDelete:
			s.handleRollbackTransaction(w, r, dbName, txID)
		default:
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST or DELETE required")
		}

	case len(remaining) == 2 && remaining[1] == "commit":
		// POST /db/{dbName}/tx/{txId}/commit
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		txID := remaining[0]
		s.handleCommitTransaction(w, r, dbName, txID)

	default:
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown transaction endpoint")
	}
}

// TransactionRequest follows Neo4j HTTP API format exactly.
type TransactionRequest struct {
	Statements []StatementRequest `json:"statements"`
}

// StatementRequest is a single Cypher statement.
type StatementRequest struct {
	Statement          string                 `json:"statement"`
	Parameters         map[string]interface{} `json:"parameters,omitempty"`
	ResultDataContents []string               `json:"resultDataContents,omitempty"` // ["row", "graph"]
	IncludeStats       bool                   `json:"includeStats,omitempty"`
}

// TransactionResponse follows Neo4j HTTP API format exactly.
type TransactionResponse struct {
	Results       []QueryResult        `json:"results"`
	Errors        []QueryError         `json:"errors"`
	Commit        string               `json:"commit,omitempty"`        // URL to commit (for open transactions)
	Transaction   *TransactionInfo     `json:"transaction,omitempty"`   // Transaction state
	LastBookmarks []string             `json:"lastBookmarks,omitempty"` // Bookmark for causal consistency
	Notifications []ServerNotification `json:"notifications,omitempty"` // Server notifications
}

// TransactionInfo holds transaction state.
type TransactionInfo struct {
	Expires string `json:"expires"` // RFC1123 format
}

// QueryResult is a single query result.
type QueryResult struct {
	Columns []string    `json:"columns"`
	Data    []ResultRow `json:"data"`
	Stats   *QueryStats `json:"stats,omitempty"`
}

// ResultRow is a row of results with metadata.
type ResultRow struct {
	Row   []interface{} `json:"row"`
	Meta  []interface{} `json:"meta,omitempty"`
	Graph *GraphResult  `json:"graph,omitempty"`
}

// GraphResult holds graph-format results.
type GraphResult struct {
	Nodes         []GraphNode         `json:"nodes"`
	Relationships []GraphRelationship `json:"relationships"`
}

// GraphNode is a node in graph format.
type GraphNode struct {
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
}

// GraphRelationship is a relationship in graph format.
type GraphRelationship struct {
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Type       string                 `json:"type"`
	StartNode  string                 `json:"startNodeElementId"`
	EndNode    string                 `json:"endNodeElementId"`
	Properties map[string]interface{} `json:"properties"`
}

// QueryStats holds query execution statistics.
type QueryStats struct {
	NodesCreated         int  `json:"nodes_created,omitempty"`
	NodesDeleted         int  `json:"nodes_deleted,omitempty"`
	RelationshipsCreated int  `json:"relationships_created,omitempty"`
	RelationshipsDeleted int  `json:"relationships_deleted,omitempty"`
	PropertiesSet        int  `json:"properties_set,omitempty"`
	LabelsAdded          int  `json:"labels_added,omitempty"`
	LabelsRemoved        int  `json:"labels_removed,omitempty"`
	IndexesAdded         int  `json:"indexes_added,omitempty"`
	IndexesRemoved       int  `json:"indexes_removed,omitempty"`
	ConstraintsAdded     int  `json:"constraints_added,omitempty"`
	ConstraintsRemoved   int  `json:"constraints_removed,omitempty"`
	ContainsUpdates      bool `json:"contains_updates,omitempty"`
}

// QueryError is an error from a query (Neo4j format).
type QueryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ServerNotification is a warning/info from the server.
type ServerNotification struct {
	Code        string           `json:"code"`
	Severity    string           `json:"severity"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Position    *NotificationPos `json:"position,omitempty"`
}

// NotificationPos is the position of a notification in the query.
type NotificationPos struct {
	Offset int `json:"offset"`
	Line   int `json:"line"`
	Column int `json:"column"`
}

// stripCypherComments removes Cypher comments from a query string.
// Supports both single-line comments (//) and multi-line comments (/* */).
// This follows the Cypher specification for comments.
//
// Examples:
//
//	"MATCH (n) RETURN n // comment" -> "MATCH (n) RETURN n "
//	"MATCH (n) /* comment */ RETURN n" -> "MATCH (n)  RETURN n"
//	"MATCH (n)\n// line comment\nRETURN n" -> "MATCH (n)\n\nRETURN n"
func stripCypherComments(query string) string {
	if query == "" {
		return query
	}

	var result strings.Builder
	result.Grow(len(query))

	lines := strings.Split(query, "\n")
	inMultiLineComment := false
	outputLines := []string{}

	for _, line := range lines {
		processed := line

		// Handle multi-line comments that span lines
		if inMultiLineComment {
			// Check if this line closes the multi-line comment
			if idx := strings.Index(processed, "*/"); idx >= 0 {
				// Multi-line comment ends on this line
				processed = processed[idx+2:]
				inMultiLineComment = false
				// Continue processing the rest of this line
			} else {
				// Still inside multi-line comment, skip entire line
				// Don't add anything for skipped comment-only lines
				continue
			}
		}

		// Process remaining line for comments
		var lineResult strings.Builder
		i := 0
		lineStartedMultiLineComment := false
		for i < len(processed) {
			// Check for start of multi-line comment
			if i+1 < len(processed) && processed[i:i+2] == "/*" {
				// Find end of multi-line comment
				endIdx := strings.Index(processed[i+2:], "*/")
				if endIdx >= 0 {
					// Multi-line comment ends on same line
					i = i + 2 + endIdx + 2
					continue
				} else {
					// Multi-line comment spans to next line
					inMultiLineComment = true
					lineStartedMultiLineComment = true
					break
				}
			}

			// Check for single-line comment
			if i+1 < len(processed) && processed[i:i+2] == "//" {
				// Rest of line is comment, stop processing
				break
			}

			// Regular character, keep it
			lineResult.WriteByte(processed[i])
			i++
		}

		// Add processed line to output
		// Don't add empty lines that started a multi-line comment (they're entirely comment)
		lineStr := lineResult.String()
		// Only trim if entire line is whitespace (preserve trailing spaces after comments)
		trimmed := strings.TrimSpace(lineStr)
		if trimmed == "" && lineStr != "" {
			// Entire line is whitespace, use empty string
			lineStr = ""
		}
		if !lineStartedMultiLineComment || lineStr != "" {
			outputLines = append(outputLines, lineStr)
		}
	}

	// Join lines, preserving original line structure
	resultStr := strings.Join(outputLines, "\n")

	// Preserve trailing newline if original had one
	if strings.HasSuffix(query, "\n") {
		resultStr += "\n"
	}

	return resultStr
}

// handleImplicitTransaction executes statements in an implicit transaction.
// This is the main query endpoint: POST /db/{dbName}/tx/commit
func (s *Server) handleImplicitTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	var req TransactionRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0, len(req.Statements)),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	claims := getClaims(r)
	hasError := false

	// Default to database from URL path
	// Each statement can override this with its own :USE command
	defaultDbName := dbName

	for _, stmt := range req.Statements {
		if hasError {
			// Skip remaining statements after error (rollback semantics)
			break
		}

		// Check write permission for mutations
		if isMutationQuery(stmt.Statement) {
			if claims != nil && !hasPermission(claims.Roles, auth.PermWrite) {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: "Write permission required",
				})
				hasError = true
				continue
			}
		}

		// Extract :USE command from this statement if present
		// Each statement can have its own :USE command to switch databases
		effectiveDbName := defaultDbName
		queryStatement := stmt.Statement

		// Check if statement starts with :USE
		trimmedStmt := strings.TrimSpace(stmt.Statement)
		if strings.HasPrefix(trimmedStmt, ":USE") || strings.HasPrefix(trimmedStmt, ":use") {
			lines := strings.Split(stmt.Statement, "\n")
			var remainingLines []string
			foundUse := false
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if !foundUse && (strings.HasPrefix(trimmed, ":USE") || strings.HasPrefix(trimmed, ":use")) {
					// Extract database name from :USE command
					parts := strings.Fields(trimmed)
					if len(parts) >= 2 {
						effectiveDbName = parts[1]
					}
					foundUse = true
					// Check if there's more content on this line after :USE command
					// Format: ":USE dbname rest of query"
					useIndex := strings.Index(trimmed, ":USE")
					if useIndex == -1 {
						useIndex = strings.Index(strings.ToUpper(trimmed), ":USE")
					}
					if useIndex >= 0 {
						// Find where :USE command ends (after database name)
						afterUse := trimmed[useIndex+4:] // Skip ":USE"
						afterUse = strings.TrimSpace(afterUse)
						// Extract database name and remaining query
						fields := strings.Fields(afterUse)
						if len(fields) > 0 {
							// Database name is first field, rest is the query
							if len(fields) > 1 {
								remainingQuery := strings.Join(fields[1:], " ")
								if remainingQuery != "" {
									remainingLines = append(remainingLines, remainingQuery)
								}
							}
						}
					}
					// Skip the :USE line itself (already processed)
					continue
				}
				remainingLines = append(remainingLines, line)
			}
			if foundUse {
				queryStatement = strings.Join(remainingLines, "\n")
				queryStatement = strings.TrimSpace(queryStatement)
			}
		}

		// Check if database exists before attempting to get executor
		if !s.dbManager.Exists(effectiveDbName) {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.DatabaseNotFound",
				Message: fmt.Sprintf("Database '%s' not found", effectiveDbName),
			})
			hasError = true
			continue
		}

		// Strip Cypher comments from query before execution
		// Comments are part of Cypher spec but should be removed before parsing
		queryStatement = stripCypherComments(queryStatement)
		queryStatement = strings.TrimSpace(queryStatement)

		// Skip empty statements (after comment removal)
		if queryStatement == "" {
			// Empty statement after comment removal - return empty result
			response.Results = append(response.Results, QueryResult{
				Columns: []string{},
				Data:    []ResultRow{},
			})
			continue
		}

		// Get executor for the specified database (or the one from :USE command)
		executor, err := s.getExecutorForDatabase(effectiveDbName)
		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.General",
				Message: fmt.Sprintf("Failed to access database '%s': %v", effectiveDbName, err),
			})
			hasError = true
			continue
		}

		// Track query execution time for slow query logging
		queryStart := time.Now()
		result, err := executor.Execute(r.Context(), queryStatement, stmt.Parameters)
		queryDuration := time.Since(queryStart)

		// Log slow queries
		s.logSlowQuery(stmt.Statement, stmt.Parameters, queryDuration, err)

		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Statement.SyntaxError",
				Message: err.Error(),
			})
			hasError = true
			continue
		}

		// Convert result to Neo4j format with metadata
		qr := QueryResult{
			Columns: result.Columns,
			Data:    make([]ResultRow, len(result.Rows)),
		}

		for i, row := range result.Rows {
			convertedRow := s.convertRowToNeo4jFormat(row)
			qr.Data[i] = ResultRow{
				Row:  convertedRow,
				Meta: s.generateRowMeta(convertedRow),
			}
		}

		if stmt.IncludeStats {
			qr.Stats = &QueryStats{ContainsUpdates: isMutationQuery(stmt.Statement)}
		}

		response.Results = append(response.Results, qr)
	}

	// Determine appropriate HTTP status code
	// Neo4j behavior: Query errors return 200 OK with errors in response body
	// Only infrastructure errors (database not found) return 4xx status codes
	status := http.StatusOK

	// Check for infrastructure errors (these return 4xx status codes)
	if len(response.Errors) > 0 {
		for _, err := range response.Errors {
			// Database not found is an infrastructure error - return 404
			if err.Code == "Neo.ClientError.Database.DatabaseNotFound" {
				status = http.StatusNotFound
				break
			}
			// Database access errors are infrastructure errors - return 500
			if err.Code == "Neo.ClientError.Database.General" {
				status = http.StatusInternalServerError
				break
			}
			// Query syntax errors, security errors, etc. return 200 OK
			// with errors in the response body (Neo4j standard behavior)
		}
	} else if s.db.IsAsyncWritesEnabled() {
		// For eventual consistency (async writes), mutations return 202 Accepted
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, status, response)
}

// convertRowToNeo4jFormat converts each value in a row to Neo4j-compatible format.
// This ensures nodes and edges use elementId and have filtered properties.
func (s *Server) convertRowToNeo4jFormat(row []interface{}) []interface{} {
	converted := make([]interface{}, len(row))
	for i, val := range row {
		converted[i] = s.convertValueToNeo4jFormat(val)
	}
	return converted
}

// convertValueToNeo4jFormat converts a single value to Neo4j HTTP format.
// Handles storage.Node, storage.Edge, maps, and slices recursively.
func (s *Server) convertValueToNeo4jFormat(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case *storage.Node:
		return s.nodeToNeo4jHTTPFormat(v)
	case *storage.Edge:
		return s.edgeToNeo4jHTTPFormat(v)
	case map[string]interface{}:
		// Check if this is already a converted node (has elementId)
		if _, hasElementId := v["elementId"]; hasElementId {
			return v
		}
		// Check if this looks like a node map (has _nodeId or id + labels)
		if nodeId, hasNodeId := v["_nodeId"]; hasNodeId {
			return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
		}
		if nodeId, hasId := v["id"]; hasId {
			if _, hasLabels := v["labels"]; hasLabels {
				return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
			}
		}
		// Regular map - convert nested values
		result := make(map[string]interface{}, len(v))
		for k, vv := range v {
			result[k] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, vv := range v {
			result[i] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	default:
		return val
	}
}

// nodeToNeo4jHTTPFormat converts a storage.Node to Neo4j HTTP API format.
// Neo4j format: {"elementId": "4:db:id", "labels": [...], "properties": {...}}
func (s *Server) nodeToNeo4jHTTPFormat(node *storage.Node) map[string]interface{} {
	if node == nil {
		return nil
	}

	elementId := fmt.Sprintf("4:nornicdb:%s", node.ID)

	// Filter properties - remove only embedding arrays, keep metadata
	props := s.filterNodeProperties(node.Properties)

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     node.Labels,
		"properties": props,
	}
}

// mapNodeToNeo4jHTTPFormat converts a map representation to Neo4j HTTP format.
func (s *Server) mapNodeToNeo4jHTTPFormat(nodeId interface{}, m map[string]interface{}) map[string]interface{} {
	elementId := fmt.Sprintf("4:nornicdb:%v", nodeId)

	// Extract labels
	var labels []string
	if l, ok := m["labels"].([]string); ok {
		labels = l
	} else if l, ok := m["labels"].([]interface{}); ok {
		labels = make([]string, len(l))
		for i, v := range l {
			if s, ok := v.(string); ok {
				labels[i] = s
			}
		}
	}

	// Extract and filter properties
	var props map[string]interface{}
	if p, ok := m["properties"].(map[string]interface{}); ok {
		props = s.filterNodeProperties(p)
	} else {
		// Properties might be at top level - collect them
		props = make(map[string]interface{})
		for k, v := range m {
			// Skip metadata fields
			if k == "id" || k == "_nodeId" || k == "labels" || k == "properties" || k == "elementId" || k == "embedding" {
				continue
			}
			if !s.isEmbeddingArrayProperty(k, v) {
				props[k] = v
			}
		}
	}

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     labels,
		"properties": props,
	}
}

// edgeToNeo4jHTTPFormat converts a storage.Edge to Neo4j HTTP API format.
func (s *Server) edgeToNeo4jHTTPFormat(edge *storage.Edge) map[string]interface{} {
	if edge == nil {
		return nil
	}

	elementId := fmt.Sprintf("5:nornicdb:%s", edge.ID)
	startElementId := fmt.Sprintf("4:nornicdb:%s", edge.StartNode)
	endElementId := fmt.Sprintf("4:nornicdb:%s", edge.EndNode)

	return map[string]interface{}{
		"elementId":          elementId,
		"type":               edge.Type,
		"startNodeElementId": startElementId,
		"endNodeElementId":   endElementId,
		"properties":         edge.Properties,
	}
}

// filterNodeProperties filters out embedding arrays but keeps embedding metadata.
// Removes: embedding (array), embeddings, vector, vectors, chunk_embedding, etc.
// Keeps: embedding_model, embedding_dimensions, has_embedding, embedded_at
func (s *Server) filterNodeProperties(props map[string]interface{}) map[string]interface{} {
	if props == nil {
		return make(map[string]interface{})
	}

	filtered := make(map[string]interface{}, len(props))
	for k, v := range props {
		if s.isEmbeddingArrayProperty(k, v) {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

// isEmbeddingArrayProperty returns true if this property is an embedding array
// that should be filtered from HTTP responses (too large to serialize).
// Returns false for embedding metadata like embedding_model, has_embedding, etc.
func (s *Server) isEmbeddingArrayProperty(key string, value interface{}) bool {
	lowerKey := strings.ToLower(key)

	// These are the actual embedding vector properties that contain large arrays
	arrayProps := map[string]bool{
		"embedding":        true,
		"embeddings":       true,
		"vector":           true,
		"vectors":          true,
		"_embedding":       true,
		"_embeddings":      true,
		"chunk_embedding":  true,
		"chunk_embeddings": true,
	}

	if !arrayProps[lowerKey] {
		return false
	}

	// Only filter if the value is actually an array/slice (not metadata)
	if value == nil {
		return false
	}

	// Check if it's a slice/array type
	rv := reflect.ValueOf(value)
	kind := rv.Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// generateRowMeta generates Neo4j-compatible metadata for each value in a row.
// Neo4j meta format: {"id": 123, "type": "node", "deleted": false, "elementId": "4:db:id"}
func (s *Server) generateRowMeta(row []interface{}) []interface{} {
	meta := make([]interface{}, len(row))
	for i, val := range row {
		switch v := val.(type) {
		case map[string]interface{}:
			// Check for elementId (Neo4j format node/edge)
			if elementId, ok := v["elementId"].(string); ok {
				// Determine if it's a node or relationship based on elementId prefix
				entityType := "node"
				if strings.HasPrefix(elementId, "5:") {
					entityType = "relationship"
				}
				// Extract numeric ID from elementId (4:nornicdb:uuid -> hash to int)
				idPart := strings.TrimPrefix(elementId, "4:nornicdb:")
				idPart = strings.TrimPrefix(idPart, "5:nornicdb:")
				numericId := s.hashStringToInt64(idPart)

				meta[i] = map[string]interface{}{
					"id":        numericId,
					"type":      entityType,
					"deleted":   false,
					"elementId": elementId,
				}
			} else {
				meta[i] = nil
			}
		default:
			meta[i] = nil
		}
	}
	return meta
}

// hashStringToInt64 converts a string ID to an int64 for Neo4j compatibility.
// Neo4j drivers expect numeric IDs in metadata.
func (s *Server) hashStringToInt64(id string) int64 {
	var hash int64
	for _, c := range id {
		hash = hash*31 + int64(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// generateBookmark generates a bookmark for causal consistency
func (s *Server) generateBookmark() string {
	return fmt.Sprintf("FB:nornicdb:%d", time.Now().UnixNano())
}

// Transaction management (explicit transactions)
//
// NornicDB implements a simplified transaction model where each request is
// treated as an implicit transaction. Explicit multi-request transactions
// are supported through the transaction ID endpoints, but each statement
// within a transaction is executed immediately (no deferred commit).
//
// This design provides:
//   - Simplicity: No complex transaction state management
//   - Performance: No transaction overhead for single-request operations
//   - Compatibility: Works with Neo4j drivers that expect transaction support
//
// Future enhancements may include:
//   - Deferred commit for explicit transactions
//   - Transaction isolation levels
//   - Long-running transaction support

func (s *Server) handleOpenTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	// Generate transaction ID
	txID := fmt.Sprintf("%d", time.Now().UnixNano())

	host := s.config.Address
	if host == "0.0.0.0" {
		host = "localhost"
	}

	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional body

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  fmt.Sprintf("http://%s:%d/db/%s/tx/%s/commit", host, s.config.Port, dbName, txID),
		Transaction: &TransactionInfo{
			Expires: time.Now().Add(30 * time.Second).Format(time.RFC1123),
		},
	}

	// Get executor for the specified database
	executor, err := s.getExecutorForDatabase(dbName)
	if err != nil {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Database.DatabaseNotFound",
			Message: fmt.Sprintf("Database '%s' not found: %v", dbName, err),
		})
		s.writeJSON(w, http.StatusNotFound, response)
		return
	}

	// Execute any provided statements
	if len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			result, err := executor.Execute(r.Context(), stmt.Statement, stmt.Parameters)
			if err != nil {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Statement.SyntaxError",
					Message: err.Error(),
				})
				continue
			}

			qr := QueryResult{
				Columns: result.Columns,
				Data:    make([]ResultRow, len(result.Rows)),
			}
			for i, row := range result.Rows {
				convertedRow := s.convertRowToNeo4jFormat(row)
				qr.Data[i] = ResultRow{Row: convertedRow, Meta: s.generateRowMeta(convertedRow)}
			}
			response.Results = append(response.Results, qr)
		}
	}

	// For transaction open, 201 Created is correct (creating transaction resource)
	// But if mutations are included with async writes, add consistency header
	if s.db.IsAsyncWritesEnabled() && len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) handleExecuteInTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Execute statements in open transaction
	// For simplified implementation, treat as immediate execution
	s.handleImplicitTransaction(w, r, dbName)
}

func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional final statements

	response := TransactionResponse{
		Results:       make([]QueryResult, 0),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	// Get executor for the specified database
	executor, err := s.getExecutorForDatabase(dbName)
	if err != nil {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Database.DatabaseNotFound",
			Message: fmt.Sprintf("Database '%s' not found: %v", dbName, err),
		})
		s.writeJSON(w, http.StatusNotFound, response)
		return
	}

	// Execute any final statements
	for _, stmt := range req.Statements {
		result, err := executor.Execute(r.Context(), stmt.Statement, stmt.Parameters)
		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Statement.SyntaxError",
				Message: err.Error(),
			})
			continue
		}

		qr := QueryResult{
			Columns: result.Columns,
			Data:    make([]ResultRow, len(result.Rows)),
		}
		for i, row := range result.Rows {
			convertedRow := s.convertRowToNeo4jFormat(row)
			qr.Data[i] = ResultRow{Row: convertedRow, Meta: s.generateRowMeta(convertedRow)}
		}
		response.Results = append(response.Results, qr)
	}

	// For commits with async writes and mutations, use 202 Accepted
	status := http.StatusOK
	if s.db.IsAsyncWritesEnabled() && len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, status, response)
}

func (s *Server) handleRollbackTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Rollback transaction (for simplified implementation, just acknowledge)
	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// writeNeo4jError writes an error in Neo4j format.
func (s *Server) writeNeo4jError(w http.ResponseWriter, status int, code, message string) {
	s.errorCount.Add(1)
	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors: []QueryError{{
			Code:    code,
			Message: message,
		}},
	}
	s.writeJSON(w, status, response)
}

// handleDecay returns memory decay information (NornicDB-specific)
func (s *Server) handleDecay(w http.ResponseWriter, r *http.Request) {
	info := s.db.GetDecayInfo()

	response := map[string]interface{}{
		"enabled":          info.Enabled,
		"archiveThreshold": info.ArchiveThreshold,
		"interval":         info.RecalcInterval.String(),
		"weights": map[string]interface{}{
			"recency":    info.RecencyWeight,
			"frequency":  info.FrequencyWeight,
			"importance": info.ImportanceWeight,
		},
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedTrigger triggers the embedding worker to process nodes without embeddings.
// Query params:
//   - regenerate=true: Clear all existing embeddings first, then regenerate (async)
func (s *Server) handleEmbedTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
		return
	}

	stats := s.db.EmbedQueueStats()
	if stats == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.DatabaseError.General.UnknownError", "Auto-embed not enabled")
		return
	}

	// Check if regenerate=true to clear existing embeddings first
	regenerate := r.URL.Query().Get("regenerate") == "true"

	if regenerate {
		// Return 202 Accepted immediately - clearing happens in background
		response := map[string]interface{}{
			"accepted":   true,
			"regenerate": true,
			"message":    "Regeneration started - clearing embeddings and regenerating in background. Check /nornicdb/embed/stats for progress.",
		}
		s.writeJSON(w, http.StatusAccepted, response)

		// Start background clearing and regeneration
		go func() {
			log.Printf("[EMBED] Starting background regeneration - stopping worker and clearing embeddings...")

			// First, reset the embed worker to stop any in-progress work and clear its state
			if err := s.db.ResetEmbedWorker(); err != nil {
				log.Printf("[EMBED] ⚠️ Failed to reset embed worker: %v", err)
				// Continue anyway - the worker will restart on its own
			}

			// Now clear all embeddings
			cleared, err := s.db.ClearAllEmbeddings()
			if err != nil {
				log.Printf("[EMBED] ❌ Failed to clear embeddings: %v", err)
				return
			}
			log.Printf("[EMBED] ✅ Cleared %d embeddings - triggering regeneration", cleared)

			// Trigger embedding worker to regenerate (worker was already restarted by Reset)
			ctx := context.Background()
			if _, err := s.db.EmbedExisting(ctx); err != nil {
				log.Printf("[EMBED] ❌ Failed to trigger embedding worker: %v", err)
				return
			}
			log.Printf("[EMBED] 🚀 Embedding worker triggered for regeneration")
		}()
		return
	}

	// Non-regenerate case: just trigger the worker (fast, synchronous is fine)
	wasRunning := stats.Running

	// Trigger (safe to call even if already running - just wakes up worker)
	_, err := s.db.EmbedExisting(r.Context())
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	// Get updated stats
	stats = s.db.EmbedQueueStats()

	var message string
	if wasRunning {
		message = "Embedding worker already running - will continue processing"
	} else {
		message = "Embedding worker triggered - processing nodes in background"
	}

	response := map[string]interface{}{
		"triggered":      true,
		"regenerate":     false,
		"already_active": wasRunning,
		"message":        message,
		"stats":          stats,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedStats returns embedding worker statistics.
func (s *Server) handleEmbedStats(w http.ResponseWriter, r *http.Request) {
	stats := s.db.EmbedQueueStats()
	totalEmbeddings := s.db.EmbeddingCount()
	vectorIndexDims := s.db.VectorIndexDimensions()

	if stats == nil {
		response := map[string]interface{}{
			"enabled":                 false,
			"message":                 "Auto-embed not enabled",
			"total_embeddings":        totalEmbeddings,
			"configured_model":        s.config.EmbeddingModel,
			"configured_dimensions":   s.config.EmbeddingDimensions,
			"configured_provider":     s.config.EmbeddingProvider,
			"vector_index_dimensions": vectorIndexDims,
		}
		s.writeJSON(w, http.StatusOK, response)
		return
	}
	response := map[string]interface{}{
		"enabled":                 true,
		"stats":                   stats,
		"total_embeddings":        totalEmbeddings,
		"configured_model":        s.config.EmbeddingModel,
		"configured_dimensions":   s.config.EmbeddingDimensions,
		"configured_provider":     s.config.EmbeddingProvider,
		"vector_index_dimensions": vectorIndexDims,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedClear clears all embeddings from nodes (admin only).
// This allows regeneration with a new model or fixing corrupted embeddings.
func (s *Server) handleEmbedClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST or DELETE required")
		return
	}

	cleared, err := s.db.ClearAllEmbeddings()
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	response := map[string]interface{}{
		"success": true,
		"cleared": cleared,
		"message": fmt.Sprintf("Cleared embeddings from %d nodes - use /nornicdb/embed/trigger to regenerate", cleared),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleSearchRebuild rebuilds search indexes from all nodes in the specified database.
func (s *Server) handleSearchRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
	}

	if err := s.readJSON(r, &req); err != nil {
		// If no JSON body, use default database
		req.Database = ""
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Create a search service for this database's namespaced storage
	searchSvc := search.NewServiceWithDimensions(storageEngine, s.db.VectorIndexDimensions())

	// Build indexes from all nodes in this database
	err = searchSvc.BuildIndexes(r.Context())
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	response := map[string]interface{}{
		"success":  true,
		"database": dbName,
		"message":  fmt.Sprintf("Search indexes rebuilt for database '%s'", dbName),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// =============================================================================
// Discovery & Health Handlers
// =============================================================================

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "not found")
		return
	}

	// Neo4j-compatible discovery response (exact format)
	host := s.config.Address
	if host == "0.0.0.0" {
		host = "localhost"
	}

	// Neo4j-compatible discovery response - minimal info to reduce reconnaissance surface
	// Feature details moved to authenticated /status endpoint
	response := map[string]interface{}{
		"bolt_direct":   fmt.Sprintf("bolt://%s:7687", host),
		"bolt_routing":  fmt.Sprintf("neo4j://%s:7687", host),
		"transaction":   fmt.Sprintf("http://%s:%d/db/{databaseName}/tx", host, s.config.Port),
		"neo4j_version": "5.0.0",
		"neo4j_edition": "community",
	}

	// Add default database name for UI compatibility (NornicDB extension)
	// This allows clients to know which database to use by default
	if s.dbManager != nil {
		response["default_database"] = s.dbManager.DefaultDatabaseName()
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Minimal health response - no operational details to reduce reconnaissance surface
	// Detailed status available at authenticated /status endpoint
	response := map[string]interface{}{
		"status": "healthy",
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()
	dbStats := s.db.Stats()

	// Get database count (all databases across the system)
	databaseCount := 0
	if s.dbManager != nil {
		databases := s.dbManager.ListDatabases()
		databaseCount = len(databases)
	}

	// Build embedding info
	embedInfo := map[string]interface{}{
		"enabled": false,
	}
	if embedStats := s.db.EmbedQueueStats(); embedStats != nil {
		status := "idle"
		if embedStats.Running {
			status = "processing"
		}
		embedInfo = map[string]interface{}{
			"enabled":   true,
			"status":    status,
			"processed": embedStats.Processed,
			"failed":    embedStats.Failed,
		}
	}

	response := map[string]interface{}{
		"status": "running",
		"server": map[string]interface{}{
			"uptime_seconds": stats.Uptime.Seconds(),
			"requests":       stats.RequestCount,
			"errors":         stats.ErrorCount,
			"active":         stats.ActiveRequests,
		},
		"database": map[string]interface{}{
			"nodes":     dbStats.NodeCount, // Global count across all databases
			"edges":     dbStats.EdgeCount, // Global count across all databases
			"databases": databaseCount,     // Number of databases
		},
		"embeddings": embedInfo,
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleMetrics returns Prometheus-compatible metrics.
// This endpoint can be scraped by Prometheus at /metrics.
//
// Metrics exported:
//   - nornicdb_uptime_seconds: Server uptime in seconds
//   - nornicdb_requests_total: Total HTTP requests
//   - nornicdb_errors_total: Total request errors
//   - nornicdb_active_requests: Currently active requests
//   - nornicdb_nodes_total: Total nodes in database
//   - nornicdb_edges_total: Total edges in database
//   - nornicdb_embeddings_processed: Embeddings processed
//   - nornicdb_embeddings_failed: Embedding failures
//   - nornicdb_embedding_worker_running: Whether embed worker is active (0/1)
//
// Example Prometheus config:
//
//	scrape_configs:
//	  - job_name: 'nornicdb'
//	    static_configs:
//	      - targets: ['localhost:7474']
//	    metrics_path: '/metrics'
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()
	dbStats := s.db.Stats()

	// Build Prometheus format output
	var sb strings.Builder

	// Server metrics
	sb.WriteString("# HELP nornicdb_uptime_seconds Server uptime in seconds\n")
	sb.WriteString("# TYPE nornicdb_uptime_seconds gauge\n")
	fmt.Fprintf(&sb, "nornicdb_uptime_seconds %.2f\n", stats.Uptime.Seconds())

	sb.WriteString("# HELP nornicdb_requests_total Total HTTP requests\n")
	sb.WriteString("# TYPE nornicdb_requests_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_requests_total %d\n", stats.RequestCount)

	sb.WriteString("# HELP nornicdb_errors_total Total request errors\n")
	sb.WriteString("# TYPE nornicdb_errors_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_errors_total %d\n", stats.ErrorCount)

	sb.WriteString("# HELP nornicdb_active_requests Currently active requests\n")
	sb.WriteString("# TYPE nornicdb_active_requests gauge\n")
	fmt.Fprintf(&sb, "nornicdb_active_requests %d\n", stats.ActiveRequests)

	// Database metrics
	sb.WriteString("# HELP nornicdb_nodes_total Total nodes in database\n")
	sb.WriteString("# TYPE nornicdb_nodes_total gauge\n")
	fmt.Fprintf(&sb, "nornicdb_nodes_total %d\n", dbStats.NodeCount)

	sb.WriteString("# HELP nornicdb_edges_total Total edges in database\n")
	sb.WriteString("# TYPE nornicdb_edges_total gauge\n")
	fmt.Fprintf(&sb, "nornicdb_edges_total %d\n", dbStats.EdgeCount)

	// Embedding metrics
	if embedStats := s.db.EmbedQueueStats(); embedStats != nil {
		sb.WriteString("# HELP nornicdb_embeddings_processed Total embeddings processed\n")
		sb.WriteString("# TYPE nornicdb_embeddings_processed counter\n")
		fmt.Fprintf(&sb, "nornicdb_embeddings_processed %d\n", embedStats.Processed)

		sb.WriteString("# HELP nornicdb_embeddings_failed Total embedding failures\n")
		sb.WriteString("# TYPE nornicdb_embeddings_failed counter\n")
		fmt.Fprintf(&sb, "nornicdb_embeddings_failed %d\n", embedStats.Failed)

		sb.WriteString("# HELP nornicdb_embedding_worker_running Whether embed worker is active\n")
		sb.WriteString("# TYPE nornicdb_embedding_worker_running gauge\n")
		running := 0
		if embedStats.Running {
			running = 1
		}
		fmt.Fprintf(&sb, "nornicdb_embedding_worker_running %d\n", running)
	}

	// Slow query metrics
	sb.WriteString("# HELP nornicdb_slow_queries_total Total slow queries logged\n")
	sb.WriteString("# TYPE nornicdb_slow_queries_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_slow_queries_total %d\n", s.slowQueryCount.Load())

	sb.WriteString("# HELP nornicdb_slow_query_threshold_ms Slow query threshold in milliseconds\n")
	sb.WriteString("# TYPE nornicdb_slow_query_threshold_ms gauge\n")
	fmt.Fprintf(&sb, "nornicdb_slow_query_threshold_ms %d\n", s.config.SlowQueryThreshold.Milliseconds())

	// Info metric with version
	sb.WriteString("# HELP nornicdb_info Database information\n")
	sb.WriteString("# TYPE nornicdb_info gauge\n")
	sb.WriteString("nornicdb_info{version=\"1.0.0\",backend=\"badger\"} 1\n")

	// Set content type for Prometheus
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(sb.String()))
}

// =============================================================================
// Authentication Handlers
// =============================================================================

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Parse request body
	var req struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		GrantType string `json:"grant_type"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// Support OAuth 2.0 password grant
	if req.GrantType != "" && req.GrantType != "password" {
		s.writeError(w, http.StatusBadRequest, "unsupported grant_type", ErrBadRequest)
		return
	}

	// Authenticate
	tokenResp, _, err := s.auth.Authenticate(
		req.Username,
		req.Password,
		getClientIP(r),
		r.UserAgent(),
	)

	if err != nil {
		status := http.StatusUnauthorized
		if err == auth.ErrAccountLocked {
			status = http.StatusTooManyRequests
		}
		s.writeError(w, status, err.Error(), ErrUnauthorized)
		return
	}

	// Set HTTP-only cookie for browser sessions (secure auth)
	http.SetCookie(w, &http.Cookie{
		Name:     "nornicdb_token",
		Value:    tokenResp.AccessToken,
		Path:     "/",
		HttpOnly: true,                 // Prevent XSS attacks
		Secure:   r.TLS != nil,         // Secure only over HTTPS
		SameSite: http.SameSiteLaxMode, // Lax allows normal navigation, prevents CSRF on POST
		MaxAge:   86400 * 7,            // 7 days
	})

	s.writeJSON(w, http.StatusOK, tokenResp)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Clear the auth cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "nornicdb_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1, // Delete cookie
	})

	// Audit the logout event
	claims := getClaims(r)
	if claims != nil {
		s.logAudit(r, claims.Sub, "logout", true, "")
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
}

// handleGenerateAPIToken generates a stateless API token for MCP servers.
// Only admins can generate these tokens. The tokens inherit the user's roles
// and are not stored - they are validated by signature on each request.
func (s *Server) handleGenerateAPIToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Get the authenticated user's claims
	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "not authenticated", ErrUnauthorized)
		return
	}

	// Check if user has admin role
	isAdmin := false
	for _, role := range claims.Roles {
		if role == "admin" {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		s.writeError(w, http.StatusForbidden, "admin role required to generate API tokens", ErrForbidden)
		return
	}

	// Parse request body
	var req struct {
		Subject   string `json:"subject"`    // Label for the token (e.g., "my-mcp-server")
		ExpiresIn string `json:"expires_in"` // Duration string (e.g., "24h", "7d", "365d", "0" for never)
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Subject == "" {
		req.Subject = "api-token"
	}

	// Parse expiry duration
	var expiry time.Duration
	if req.ExpiresIn != "" && req.ExpiresIn != "0" && req.ExpiresIn != "never" {
		// Handle special "d" suffix for days
		expiresIn := req.ExpiresIn
		if strings.HasSuffix(expiresIn, "d") {
			days, err := strconv.Atoi(strings.TrimSuffix(expiresIn, "d"))
			if err != nil {
				s.writeError(w, http.StatusBadRequest, "invalid expires_in format", ErrBadRequest)
				return
			}
			expiry = time.Duration(days) * 24 * time.Hour
		} else {
			var err error
			expiry, err = time.ParseDuration(expiresIn)
			if err != nil {
				s.writeError(w, http.StatusBadRequest, "invalid expires_in format (use: 1h, 24h, 7d, 365d, 0 for never)", ErrBadRequest)
				return
			}
		}
	}

	// Create a user object from claims for token generation
	roles := make([]auth.Role, len(claims.Roles))
	for i, r := range claims.Roles {
		roles[i] = auth.Role(r)
	}
	user := &auth.User{
		ID:       claims.Sub,
		Username: claims.Username,
		Email:    claims.Email,
		Roles:    roles,
	}

	// Generate the API token
	token, err := s.auth.GenerateAPIToken(user, req.Subject, expiry)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to generate token", err)
		return
	}

	// Calculate expiration time for response
	var expiresAt *time.Time
	if expiry > 0 {
		t := time.Now().Add(expiry)
		expiresAt = &t
	}

	response := struct {
		Token     string     `json:"token"`
		Subject   string     `json:"subject"`
		ExpiresAt *time.Time `json:"expires_at,omitempty"`
		ExpiresIn int64      `json:"expires_in,omitempty"` // seconds
		Roles     []string   `json:"roles"`
	}{
		Token:     token,
		Subject:   req.Subject,
		ExpiresAt: expiresAt,
		Roles:     claims.Roles,
	}
	if expiry > 0 {
		response.ExpiresIn = int64(expiry.Seconds())
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleAuthConfig returns auth configuration for the UI
func (s *Server) handleAuthConfig(w http.ResponseWriter, r *http.Request) {
	config := struct {
		DevLoginEnabled bool `json:"devLoginEnabled"`
		SecurityEnabled bool `json:"securityEnabled"`
		OAuthProviders  []struct {
			Name        string `json:"name"`
			URL         string `json:"url"`
			DisplayName string `json:"displayName"`
		} `json:"oauthProviders"`
	}{
		DevLoginEnabled: true, // Dev login enabled for development convenience
		SecurityEnabled: s.auth != nil && s.auth.IsSecurityEnabled(),
		OAuthProviders: []struct {
			Name        string `json:"name"`
			URL         string `json:"url"`
			DisplayName string `json:"displayName"`
		}{},
	}

	s.writeJSON(w, http.StatusOK, config)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed", ErrMethodNotAllowed)
		return
	}

	// If auth is disabled, return anonymous admin user
	if s.auth == nil || !s.auth.IsSecurityEnabled() {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":       "anonymous",
			"username": "anonymous",
			"roles":    []string{"admin"},
			"enabled":  true,
		})
		return
	}

	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "no user context", ErrUnauthorized)
		return
	}

	user, err := s.auth.GetUserByID(claims.Sub)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
		return
	}

	s.writeJSON(w, http.StatusOK, user)
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// List users
		users := s.auth.ListUsers()
		s.writeJSON(w, http.StatusOK, users)

	case http.MethodPost:
		// Create user
		var req struct {
			Username string   `json:"username"`
			Password string   `json:"password"`
			Roles    []string `json:"roles"`
		}

		if err := s.readJSON(r, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}

		roles := make([]auth.Role, len(req.Roles))
		for i, r := range req.Roles {
			roles[i] = auth.Role(r)
		}

		user, err := s.auth.CreateUser(req.Username, req.Password, roles)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
			return
		}

		s.writeJSON(w, http.StatusCreated, user)

	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleUserByID(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimPrefix(r.URL.Path, "/auth/users/")
	if username == "" {
		// Empty username - delegate to list users handler
		s.handleUsers(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		user, err := s.auth.GetUser(username)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		s.writeJSON(w, http.StatusOK, user)

	case http.MethodPut:
		var req struct {
			Roles    []string `json:"roles,omitempty"`
			Disabled *bool    `json:"disabled,omitempty"`
		}

		if err := s.readJSON(r, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}

		if len(req.Roles) > 0 {
			roles := make([]auth.Role, len(req.Roles))
			for i, r := range req.Roles {
				roles[i] = auth.Role(r)
			}
			if err := s.auth.UpdateRoles(username, roles); err != nil {
				s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
				return
			}
		}

		if req.Disabled != nil {
			if *req.Disabled {
				s.auth.DisableUser(username)
			} else {
				s.auth.EnableUser(username)
			}
		}

		s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})

	case http.MethodDelete:
		if err := s.auth.DeleteUser(username); err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})

	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET, PUT, or DELETE required", ErrMethodNotAllowed)
	}
}

// =============================================================================
// NornicDB-Specific Handlers (Memory OS for LLMs)
// =============================================================================

// Search Handlers
// =============================================================================

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Database string   `json:"database,omitempty"` // Optional: defaults to default database
		Query    string   `json:"query"`
		Labels   []string `json:"labels,omitempty"`
		Limit    int      `json:"limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Try to generate embedding for hybrid search
	queryEmbedding, embedErr := s.db.EmbedQuery(r.Context(), req.Query)
	if embedErr != nil {
		log.Printf("⚠️ Query embedding failed: %v", embedErr)
	}

	// Create a search service for this database's namespaced storage
	// Note: This creates a new service each time. For better performance, consider
	// caching search services per database if this endpoint is called frequently.
	searchSvc := search.NewServiceWithDimensions(storageEngine, s.db.VectorIndexDimensions())

	// Build indexes if needed (this is fast if already indexed)
	// For production, indexes should be built on startup or via /nornicdb/search/rebuild
	ctx := r.Context()
	opts := search.DefaultSearchOptions()
	opts.Limit = req.Limit
	if len(req.Labels) > 0 {
		opts.Types = req.Labels
	}

	var searchResponse *search.SearchResponse
	if queryEmbedding != nil {
		// Use hybrid search (vector + text)
		searchResponse, err = searchSvc.Search(ctx, req.Query, queryEmbedding, opts)
	} else {
		// Fall back to text-only search
		searchResponse, err = searchSvc.Search(ctx, req.Query, nil, opts)
	}

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Convert search results to our format
	// SearchResult.NodeID is already unprefixed (NamespacedEngine.GetNode() unprefixes automatically)
	results := make([]*nornicdb.SearchResult, len(searchResponse.Results))
	for i, r := range searchResponse.Results {
		results[i] = &nornicdb.SearchResult{
			Node: &nornicdb.Node{
				ID:         string(r.NodeID),
				Labels:     r.Labels,
				Properties: r.Properties,
			},
			Score:      r.Score,
			RRFScore:   r.RRFScore,
			VectorRank: r.VectorRank,
			BM25Rank:   r.BM25Rank,
		}
	}

	s.writeJSON(w, http.StatusOK, results)
}

func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
		NodeID   string `json:"node_id"`
		Limit    int    `json:"limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Get the target node from namespaced storage
	targetNode, err := storageEngine.GetNode(storage.NodeID(req.NodeID))
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Node '%s' not found", req.NodeID), ErrNotFound)
		return
	}

	if len(targetNode.Embedding) == 0 {
		s.writeError(w, http.StatusBadRequest, "Node has no embedding", ErrBadRequest)
		return
	}

	// Find similar nodes using vector similarity search
	type scored struct {
		node  *storage.Node
		score float64
	}
	var results []scored

	ctx := r.Context()
	err = storage.StreamNodesWithFallback(ctx, storageEngine, 1000, func(n *storage.Node) error {
		// Skip self and nodes without embeddings
		if string(n.ID) == req.NodeID || len(n.Embedding) == 0 {
			return nil
		}

		sim := vector.CosineSimilarity(targetNode.Embedding, n.Embedding)

		// Maintain top-k results
		if len(results) < req.Limit {
			results = append(results, scored{node: n, score: sim})
			if len(results) == req.Limit {
				sort.Slice(results, func(i, j int) bool {
					return results[i].score > results[j].score
				})
			}
		} else if sim > results[req.Limit-1].score {
			results[req.Limit-1] = scored{node: n, score: sim}
			sort.Slice(results, func(i, j int) bool {
				return results[i].score > results[j].score
			})
		}
		return nil
	})

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Final sort
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	// Convert to response format (node IDs are already unprefixed from NamespacedEngine)
	searchResults := make([]*nornicdb.SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = &nornicdb.SearchResult{
			Node: &nornicdb.Node{
				ID:         string(r.node.ID),
				Labels:     r.node.Labels,
				Properties: r.node.Properties,
				CreatedAt:  r.node.CreatedAt,
			},
			Score: r.score,
		}
	}

	s.writeJSON(w, http.StatusOK, searchResults)
}

// =============================================================================
// Admin Handlers
// =============================================================================

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	serverStats := s.Stats()
	dbStats := s.db.Stats()

	response := map[string]interface{}{
		"server":   serverStats,
		"database": dbStats,
		"memory": map[string]interface{}{
			"alloc_mb":   getMemoryUsageMB(),
			"goroutines": runtime.NumGoroutine(),
		},
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	// Return safe config (no secrets)
	config := map[string]interface{}{
		"address":      s.config.Address,
		"port":         s.config.Port,
		"cors_enabled": s.config.EnableCORS,
		"compression":  s.config.EnableCompression,
		"tls_enabled":  s.config.TLSCertFile != "",
	}

	s.writeJSON(w, http.StatusOK, config)
}

func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Path string `json:"path"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if err := s.db.Backup(r.Context(), req.Path); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "backup complete",
		"path":   req.Path,
	})
}

// =============================================================================
// GPU Control Handlers
// =============================================================================

func (s *Server) handleGPUStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"available": false,
			"enabled":   false,
			"message":   "GPU manager not initialized",
		})
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "invalid GPU manager type", ErrInternalError)
		return
	}

	enabled := gpuManager.IsEnabled()
	device := gpuManager.Device()
	stats := gpuManager.Stats()

	response := map[string]interface{}{
		"available":      device != nil,
		"enabled":        enabled,
		"operations_gpu": stats.OperationsGPU,
		"operations_cpu": stats.OperationsCPU,
		"fallback_count": stats.FallbackCount,
		"allocated_mb":   gpuManager.AllocatedMemoryMB(),
	}

	if device != nil {
		response["device"] = map[string]interface{}{
			"id":            device.ID,
			"name":          device.Name,
			"vendor":        device.Vendor,
			"backend":       device.Backend,
			"memory_mb":     device.MemoryMB,
			"compute_units": device.ComputeUnits,
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGPUEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeError(w, http.StatusServiceUnavailable, "GPU manager not initialized", ErrInternalError)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "invalid GPU manager type", ErrInternalError)
		return
	}

	if err := gpuManager.Enable(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "enabled",
		"message": "GPU acceleration enabled",
	})
}

func (s *Server) handleGPUDisable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeError(w, http.StatusServiceUnavailable, "GPU manager not initialized", ErrInternalError)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "invalid GPU manager type", ErrInternalError)
		return
	}

	gpuManager.Disable()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "disabled",
		"message": "GPU acceleration disabled (CPU fallback active)",
	})
}

func (s *Server) handleGPUTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
		Limit  int    `json:"limit,omitempty"`
		Mode   string `json:"mode,omitempty"` // "auto", "cpu", "gpu"
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.Mode == "" {
		req.Mode = "auto"
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeError(w, http.StatusServiceUnavailable, "GPU manager not initialized", ErrInternalError)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "invalid GPU manager type", ErrInternalError)
		return
	}

	// Store original state
	originallyEnabled := gpuManager.IsEnabled()

	// Configure mode for this test
	switch req.Mode {
	case "cpu":
		gpuManager.Disable()
		defer func() {
			if originallyEnabled {
				gpuManager.Enable()
			}
		}()
	case "gpu":
		if err := gpuManager.Enable(); err != nil {
			s.writeError(w, http.StatusInternalServerError, "GPU unavailable: "+err.Error(), ErrInternalError)
			return
		}
		defer func() {
			if !originallyEnabled {
				gpuManager.Disable()
			}
		}()
	case "auto":
		// Use current state
	}

	// Measure search performance
	startTime := time.Now()
	results, err := s.db.FindSimilar(r.Context(), req.NodeID, req.Limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}
	elapsedMs := time.Since(startTime).Milliseconds()

	// Get stats
	stats := gpuManager.Stats()
	usedMode := "cpu"
	if gpuManager.IsEnabled() {
		usedMode = "gpu"
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
		"performance": map[string]interface{}{
			"elapsed_ms":     elapsedMs,
			"mode":           usedMode,
			"operations_gpu": stats.OperationsGPU,
			"operations_cpu": stats.OperationsCPU,
		},
	})
}

// =============================================================================
// GDPR Compliance Handlers
// =============================================================================

func (s *Server) handleGDPRExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"user_id"`
		Format string `json:"format"` // "json" or "csv"
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// User can only export own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(claims.Roles, auth.PermAdmin) {
		s.writeError(w, http.StatusForbidden, "can only export own data", ErrForbidden)
		return
	}

	data, err := s.db.ExportUserData(r.Context(), req.UserID, req.Format)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.logAudit(r, req.UserID, "gdpr_export", true, fmt.Sprintf("format: %s", req.Format))

	if req.Format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=user_data.csv")
		w.Write(data)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=user_data.json")
		w.Write(data)
	}
}

func (s *Server) handleGDPRDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		UserID    string `json:"user_id"`
		Anonymize bool   `json:"anonymize"` // Anonymize instead of hard delete
		Confirm   bool   `json:"confirm"`   // Confirmation required
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if !req.Confirm {
		s.writeError(w, http.StatusBadRequest, "confirmation required", ErrBadRequest)
		return
	}

	// User can only delete own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(claims.Roles, auth.PermAdmin) {
		s.writeError(w, http.StatusForbidden, "can only delete own data", ErrForbidden)
		return
	}

	var err error
	if req.Anonymize {
		err = s.db.AnonymizeUserData(r.Context(), req.UserID)
	} else {
		err = s.db.DeleteUserData(r.Context(), req.UserID)
	}

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	action := "deleted"
	if req.Anonymize {
		action = "anonymized"
	}

	s.logAudit(r, req.UserID, "gdpr_delete", true, fmt.Sprintf("action: %s", action))

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status":  action,
		"user_id": req.UserID,
	})
}

// =============================================================================
// Helper Functions
// =============================================================================

type contextKey string

const contextKeyClaims = contextKey("claims")

func getClaims(r *http.Request) *auth.JWTClaims {
	claims, _ := r.Context().Value(contextKeyClaims).(*auth.JWTClaims)
	return claims
}

func getCookie(r *http.Request, name string) string {
	cookie, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	// Check X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	// Fall back to RemoteAddr
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func hasPermission(roles []string, required auth.Permission) bool {
	for _, roleStr := range roles {
		role := auth.Role(roleStr)
		perms, ok := auth.RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == required {
				return true
			}
		}
	}
	return false
}

func isMutationQuery(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "MERGE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "SET") ||
		strings.HasPrefix(upper, "REMOVE") ||
		strings.HasPrefix(upper, "DROP")
}

func parseIntQuery(r *http.Request, key string, defaultVal int) int {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return defaultVal
	}
	var val int
	fmt.Sscanf(valStr, "%d", &val)
	if val <= 0 {
		return defaultVal
	}
	return val
}

func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Flush implements http.Flusher interface for SSE streaming.
// This is critical for Bifrost chat streaming to work properly.
func (w *responseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// JSON helpers

func (s *Server) readJSON(r *http.Request, v interface{}) error {
	// Limit body size
	body := io.LimitReader(r.Body, s.config.MaxRequestSize)
	return json.NewDecoder(body).Decode(v)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
	s.errorCount.Add(1)

	response := map[string]interface{}{
		"error":   true,
		"message": message,
		"code":    status,
	}

	s.writeJSON(w, status, response)
}

// Logging helpers

func (s *Server) logRequest(r *http.Request, status int, duration time.Duration) {
	// Could be enhanced with structured logging
	fmt.Printf("[HTTP] %s %s %d %v\n", r.Method, r.URL.Path, status, duration)
}

// logSlowQuery logs queries that exceed the configured threshold.
// Logged info includes: query text (truncated), duration, parameters, error if any.
func (s *Server) logSlowQuery(query string, params map[string]interface{}, duration time.Duration, err error) {
	if !s.config.SlowQueryEnabled {
		return
	}

	if duration < s.config.SlowQueryThreshold {
		return
	}

	s.slowQueryCount.Add(1)

	// Truncate long queries for logging
	queryLog := query
	if len(queryLog) > 500 {
		queryLog = queryLog[:500] + "..."
	}

	// Build log message
	status := "OK"
	if err != nil {
		status = fmt.Sprintf("ERROR: %v", err)
	}

	// Format parameters (limit to avoid huge logs)
	paramStr := ""
	if len(params) > 0 {
		paramBytes, _ := json.Marshal(params)
		if len(paramBytes) > 200 {
			paramStr = string(paramBytes[:200]) + "..."
		} else {
			paramStr = string(paramBytes)
		}
	}

	logMsg := fmt.Sprintf("[SLOW QUERY] duration=%v status=%s query=%q params=%s",
		duration, status, queryLog, paramStr)

	// Log to slow query logger if configured, otherwise to stderr
	if s.slowQueryLogger != nil {
		s.slowQueryLogger.Println(logMsg)
	} else {
		log.Println(logMsg)
	}
}

func (s *Server) logAudit(r *http.Request, userID, eventType string, success bool, details string) {
	if s.audit == nil {
		return
	}

	s.audit.Log(audit.Event{
		Timestamp:   time.Now(),
		Type:        audit.EventType(eventType),
		UserID:      userID,
		IPAddress:   getClientIP(r),
		UserAgent:   r.UserAgent(),
		Success:     success,
		Reason:      details,
		RequestPath: r.URL.Path,
	})
}

// ==========================================================================
// Heimdall Database/Metrics Wrappers
// ==========================================================================

// heimdallDBReader wraps NornicDB for Heimdall's DatabaseReader interface.
type heimdallDBReader struct {
	db       *nornicdb.DB
	features *nornicConfig.FeatureFlagsConfig
}

func (r *heimdallDBReader) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	result, err := r.db.ExecuteCypher(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	// Convert result to []map[string]interface{}
	var rows []map[string]interface{}
	for _, row := range result.Rows {
		rowMap := make(map[string]interface{})
		for i, col := range result.Columns {
			if i < len(row) {
				rowMap[col] = row[i]
			}
		}
		rows = append(rows, rowMap)
	}
	return rows, nil
}

func (r *heimdallDBReader) Stats() heimdall.DatabaseStats {
	stats := r.db.Stats()
	result := heimdall.DatabaseStats{
		NodeCount:         stats.NodeCount,
		RelationshipCount: stats.EdgeCount,
		LabelCounts:       make(map[string]int64), // Label counts not yet implemented (future enhancement)
	}

	// Add search/cluster stats if available
	if searchStats := r.db.GetSearchStats(); searchStats != nil {
		result.ClusterStats = &heimdall.ClusterStats{
			NumClusters:    searchStats.NumClusters,
			EmbeddingCount: searchStats.EmbeddingCount,
			IsClustered:    searchStats.IsClustered,
			AvgClusterSize: searchStats.AvgClusterSize,
			Iterations:     searchStats.ClusterIterations,
		}
	}

	// Add feature flags
	if r.features != nil {
		// Derive clustering enabled from search stats
		clusteringEnabled := false
		if result.ClusterStats != nil {
			clusteringEnabled = result.ClusterStats.NumClusters > 0 || result.ClusterStats.EmbeddingCount > 0
		}

		result.FeatureFlags = &heimdall.FeatureFlags{
			HeimdallEnabled:          r.features.HeimdallEnabled,
			HeimdallAnomalyDetection: r.features.HeimdallAnomalyDetection,
			HeimdallRuntimeDiagnosis: r.features.HeimdallRuntimeDiagnosis,
			HeimdallMemoryCuration:   r.features.HeimdallMemoryCuration,
			ClusteringEnabled:        clusteringEnabled,
			TopologyEnabled:          r.features.TopologyAutoIntegrationEnabled,
			KalmanEnabled:            r.features.KalmanEnabled,
			AsyncWritesEnabled:       r.db.IsAsyncWritesEnabled(),
		}
	}

	return result
}

// Discover implements semantic search with graph traversal for Graph-RAG.
func (r *heimdallDBReader) Discover(ctx context.Context, query string, nodeTypes []string, limit int, depth int) (*heimdall.DiscoverResult, error) {
	// Use text search (BM25) since we don't have embedder access here
	// For vector search, the full MCP server should be used
	dbResults, err := r.db.Search(ctx, query, nodeTypes, limit)
	if err != nil {
		return nil, err
	}

	results := make([]heimdall.SearchResult, 0, len(dbResults))
	for _, dbr := range dbResults {
		result := heimdall.SearchResult{
			ID:         dbr.Node.ID,
			Type:       getFirstLabel(dbr.Node.Labels),
			Title:      getStringProperty(dbr.Node.Properties, "title"),
			Similarity: dbr.Score,
			Properties: dbr.Node.Properties,
		}

		// Content preview
		if content := getStringProperty(dbr.Node.Properties, "content"); content != "" {
			if len(content) > 200 {
				result.ContentPreview = content[:200] + "..."
			} else {
				result.ContentPreview = content
			}
		}

		// Get related nodes if depth > 1
		if depth > 1 {
			result.Related = r.getRelatedNodes(ctx, dbr.Node.ID, depth)
		}

		results = append(results, result)
	}

	return &heimdall.DiscoverResult{
		Results: results,
		Method:  "keyword",
		Total:   len(results),
	}, nil
}

// getRelatedNodes fetches connected nodes up to the specified depth.
func (r *heimdallDBReader) getRelatedNodes(ctx context.Context, nodeID string, depth int) []heimdall.RelatedNode {
	if depth < 1 {
		return nil
	}

	var related []heimdall.RelatedNode
	visited := make(map[string]bool)
	visited[nodeID] = true

	// BFS traversal
	type queueItem struct {
		id       string
		distance int
	}
	queue := []queueItem{{id: nodeID, distance: 0}}

	for len(queue) > 0 && len(related) < 50 {
		current := queue[0]
		queue = queue[1:]

		if current.distance >= depth {
			continue
		}

		// Get neighbors (depth=1 for immediate neighbors, "" for any edge type)
		neighbors, err := r.db.Neighbors(ctx, current.id, 1, "")
		if err != nil {
			continue
		}

		// Get edges for relationship info
		edges, err := r.db.GetEdgesForNode(ctx, current.id)
		if err != nil {
			edges = nil
		}

		// Build edge lookup by neighbor ID
		edgeMap := make(map[string]*nornicdb.GraphEdge)
		for _, edge := range edges {
			if edge.Source == current.id {
				edgeMap[edge.Target] = edge
			} else {
				edgeMap[edge.Source] = edge
			}
		}

		for _, neighbor := range neighbors {
			neighborID := neighbor.ID
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Get node info
			node, err := r.db.GetNode(ctx, neighborID)
			if err != nil {
				continue
			}

			rel := heimdall.RelatedNode{
				ID:       neighborID,
				Type:     getFirstLabel(node.Labels),
				Title:    getStringProperty(node.Properties, "title"),
				Distance: current.distance + 1,
			}

			// Get relationship info from edge
			if edge, ok := edgeMap[neighborID]; ok {
				rel.Relationship = edge.Type
				if edge.Source == current.id {
					rel.Direction = "outgoing"
				} else {
					rel.Direction = "incoming"
				}
			}

			related = append(related, rel)

			// Add to queue for next level
			if current.distance+1 < depth {
				queue = append(queue, queueItem{id: neighborID, distance: current.distance + 1})
			}
		}
	}

	return related
}

// getFirstLabel returns the first label or empty string.
func getFirstLabel(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}

// getStringProperty extracts a string property from a map.
func getStringProperty(props map[string]interface{}, key string) string {
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

// heimdallMetricsReader provides runtime metrics for Heimdall.
type heimdallMetricsReader struct{}

func (r *heimdallMetricsReader) Runtime() heimdall.RuntimeMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return heimdall.RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  m.Alloc / 1024 / 1024,
		NumGC:          m.NumGC,
	}
}
