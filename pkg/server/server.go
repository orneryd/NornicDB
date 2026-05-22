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
//   - HTTP/2 support (always enabled, backwards compatible with HTTP/1.1)
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
//	// Server listening on server.Addr()
//
//	// Use with Neo4j Browser
//	// Open: http://localhost:7474
//	// Connect URI: bolt://localhost:7687 (if Bolt server is running)
//	// Or use HTTP: http://localhost:7474/db/nornic/tx/commit
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
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/graphql"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localllm"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
)

// Errors for HTTP operations.
var (
	ErrServerClosed       = fmt.Errorf("server closed")
	ErrUnauthorized       = fmt.Errorf("unauthorized")
	ErrForbidden          = fmt.Errorf("forbidden")
	ErrBadRequest         = fmt.Errorf("bad request")
	ErrNotFound           = fmt.Errorf("not found")
	ErrMethodNotAllowed   = fmt.Errorf("method not allowed")
	ErrInternalError      = fmt.Errorf("internal server error")
	ErrServiceUnavailable = fmt.Errorf("service unavailable")
)

// embeddingCacheMemoryMB calculates approximate memory usage for embedding cache.
// Each cached embedding uses: cacheSize * dimensions * 4 bytes (float32).
func embeddingCacheMemoryMB(cacheSize, dimensions int) int {
	return cacheSize * dimensions * 4 / 1024 / 1024
}

// waitForDurationOrServerClose sleeps for d and returns true only when the full
// duration elapsed. It returns false when the server is closing so background
// retry loops can exit promptly during shutdown.
func waitForDurationOrServerClose(s *Server, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	if s == nil {
		time.Sleep(d)
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()

	for {
		if s.closed.Load() {
			return false
		}
		select {
		case <-timer.C:
			return true
		case <-poll.C:
			if s.closed.Load() {
				return false
			}
		}
	}
}

// buildEmbedConfigFromResolved builds an embed.Config from per-DB effective map and server config fallbacks.
// Used by the per-DB embedder registry when EmbedConfigForDB is set.
func buildEmbedConfigFromResolved(effective map[string]string, fallback *Config) *embed.Config {
	if fallback == nil {
		return nil
	}
	get := func(key, def string) string {
		if v := effective[key]; v != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	getInt := func(key string, def int) int {
		if v := effective[key]; v != "" {
			if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return i
			}
		}
		return def
	}
	provider := get("NORNICDB_EMBEDDING_PROVIDER", fallback.EmbeddingProvider)
	if provider == "" {
		provider = "openai"
	}
	model := get("NORNICDB_EMBEDDING_MODEL", fallback.EmbeddingModel)
	apiURL := get("NORNICDB_EMBEDDING_API_URL", fallback.EmbeddingAPIURL)
	apiKey := get("NORNICDB_EMBEDDING_API_KEY", fallback.EmbeddingAPIKey)
	dimensions := getInt("NORNICDB_EMBEDDING_DIMENSIONS", fallback.EmbeddingDimensions)
	if dimensions <= 0 {
		dimensions = 1024
	}
	gpuLayers := getInt("NORNICDB_EMBEDDING_GPU_LAYERS", 0)
	cfg := &embed.Config{
		Provider:   provider,
		APIURL:     apiURL,
		APIKey:     apiKey,
		Model:      model,
		Dimensions: dimensions,
		ModelsDir:  fallback.ModelsDir,
		Timeout:    30 * time.Second,
		GPULayers:  gpuLayers,
	}
	switch provider {
	case "ollama":
		cfg.APIPath = "/api/embeddings"
	case "openai":
		cfg.APIPath = "/v1/embeddings"
	case "local":
		// no APIPath
	default:
		cfg.APIPath = "/api/embeddings"
	}
	return cfg
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
	// PerDBYAMLOverrides carries the `databases:` map parsed from
	// nornicdb.yaml. Server.New consumes it during system-DB load to seed
	// dbconfig.Store on first boot via LoadWithYAMLDefaults — admin-API
	// edits remain authoritative across restarts. Must be set BEFORE
	// server.New runs; the post-construction setter is gone because the
	// store load happens inside New and won't see late assignments.
	PerDBYAMLOverrides map[string]map[string]string
	// Port to listen on (default: 7474)
	Port int
	// BoltPort is the port the Bolt protocol server listens on. Surfaced
	// in the discovery response so browser clients constructing
	// neo4j-driver Bolt-over-WS sessions know where to connect.
	// Default: 7687 when zero.
	BoltPort int
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

	// HTTP/2 Configuration
	// HTTP/2 is always enabled (backwards compatible with HTTP/1.1)
	// HTTP/2 provides multiplexing, header compression, and improved performance
	// HTTP/1.1 clients continue to work normally
	// HTTP2MaxConcurrentStreams limits the number of concurrent streams per connection (default: 250)
	// - 250: Go's internal default, matches standard library behavior (default)
	// - 100: Lower memory usage, good for resource-constrained environments
	// - 500-1000: High concurrency scenarios, uses more memory per connection
	// - Very high values (>1000) are not recommended due to DoS attack risk
	HTTP2MaxConcurrentStreams uint32

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
	// D-04d: SlowQueryThreshold and SlowQueryLogFile collapsed into
	// pkg/config.LoggingConfig (the single source of truth). Threaded into
	// the server via the Logging field below; readers go through
	// s.config.Logging.SlowQueryThreshold / .SlowQueryLogFile.
	//
	// Logging carries the runtime LoggingConfig snapshot. Populated by
	// cmd/nornicdb/main.go from cfg.Logging at server construction.
	Logging nornicConfig.LoggingConfig

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

	// Debug/Profiling Configuration
	// EnablePprof enables /debug/pprof endpoints for performance profiling
	// WARNING: Only enable in development/testing environments
	// Env: NORNICDB_ENABLE_PPROF=true|false
	EnablePprof bool

	// Logger is the structured-logging entrypoint per Phase 2 D-01.
	// If nil, a discard-handler fallback is installed at New() — graceful
	// degrade for the transitional period; ctors will be tightened post-M1
	// once all consumers are updated to pass an explicit logger via
	// observability.Provider.Logger().
	Logger *slog.Logger
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
		WriteTimeout:   300 * time.Second, // Bifrost agentic loops (many tool calls) can run 1–2 min; avoid closing stream early
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
		EmbeddingProvider:   "ollama", // default URL targets Ollama (port 11434)
		EmbeddingAPIURL:     "http://localhost:11434",
		EmbeddingModel:      "bge-m3",
		EmbeddingDimensions: 1024,
		EmbeddingCacheSize:  10000, // ~40MB cache for 1024-dim vectors

		// Slow query logging enabled by default
		// Override via:
		//   NORNICDB_SLOW_QUERY_ENABLED=false
		//   NORNICDB_SLOW_QUERY_THRESHOLD=200ms
		//   NORNICDB_SLOW_QUERY_LOG=/var/log/nornicdb/slow.log
		// D-04d: Threshold + LogFile defaults now live in
		// pkg/config.DefaultConfig().Logging (the single source of truth);
		// callers populate Logging from cfg.Logging.
		SlowQueryEnabled: false,
		Logging:          nornicConfig.LoggingConfig{SlowQueryThreshold: 100 * time.Millisecond},

		// Headless mode disabled by default (UI enabled)
		// Override via:
		//   NORNICDB_HEADLESS=true
		//   --headless flag
		Headless: false,

		// Pprof disabled by default (security: profiling endpoints expose internals)
		// Override via:
		//   NORNICDB_ENABLE_PPROF=true
		EnablePprof: envutil.GetBoolStrict("NORNICDB_ENABLE_PPROF", false),

		// HTTP/2 always enabled (backwards compatible with HTTP/1.1)
		// MaxConcurrentStreams: 250 matches Go's internal default
		// - Matches standard library http2.Server default (250)
		// - Good balance between performance and memory usage
		// - Can be reduced to 100 for lower-memory environments
		// - Can be increased to 500+ for high-concurrency scenarios
		HTTP2MaxConcurrentStreams: 250,
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
//	// (Listening on server.Addr())
//
//	// Get metrics
//	stats := server.Stats()
//	// stats.RequestCount, stats.ErrorCount expose request/error counts
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

	// log is the structured logger for operational events (Phase 2 D-01).
	// Tagged .With("component", "server") at construction so every record
	// carries component attribution. NEVER nil after New() returns
	// (discard-fallback handler installed when cfg.Logger == nil).
	log *slog.Logger

	// MCP server for LLM tool interface
	mcpServer *mcp.Server

	// Heimdall - AI assistant for database management
	heimdallHandler *heimdall.Handler

	// GraphQL handler for GraphQL API
	graphqlHandler *graphql.Handler

	// Qdrant-compatible gRPC server (optional; feature-flagged).
	qdrantGRPCServer      *qdrantgrpc.Server
	qdrantCollectionStore qdrantgrpc.CollectionStore

	httpServer *http.Server
	listener   net.Listener
	// requestsCtx is the parent context handed to every inbound HTTP request
	// via http.Server.BaseContext. Cancelled at the start of Stop() so any
	// long-running handler (Cypher traversals, etc.) can unwind promptly
	// instead of holding shutdown open until ReadTimeout/WriteTimeout fires.
	requestsCtx       context.Context
	cancelRequestsCtx context.CancelFunc

	// httpMetrics is the Plan-04-02 HTTP catalog bag (D-02 typed handle DI).
	// Populated by SetHTTPMetrics(...) AFTER observability.New runs in
	// cmd/nornicdb/main.go (Phase 2 D-08 two-phase bootstrap: server is
	// constructed BEFORE obs to keep the existing logger plumbing; metrics
	// bag is injected post-hoc and applied at Start() time when the http
	// handler is wrapped). Nil-safe: instrumentedMux is a pass-through
	// when nil, so test fixtures and pre-Phase-4 callers compile unchanged.
	httpMetrics *observability.HTTPMetrics

	// obsRegistry is the unified pkg/observability *prometheus.Registry,
	// injected post-construction via SetObsRegistry from cmd/nornicdb/main.go.
	// Used by handleMetrics (Phase 5 / Plan 05-04) to call
	// observability.RenderLegacy and produce the legacy :7474/metrics body
	// from the same registry that backs :9090/metrics — eliminating the
	// pre-Phase-5 hand-built second source of truth (ROADMAP SC #1).
	// Nil-safe: handleMetrics tolerates nil (RenderLegacy returns empty bytes).
	obsRegistry *prometheus.Registry

	// Rate limiter for DoS protection
	rateLimiter *IPRateLimiter

	// OAuth manager for OAuth 2.0 authentication
	oauthManager *auth.OAuthManager

	// Cache for Basic auth results to avoid bcrypt+JWT work on every request.
	// This materially improves throughput for Neo4j-compatible clients that
	// send Basic auth on each request.
	basicAuthCache *auth.BasicAuthCache

	mu      sync.RWMutex
	closed  atomic.Bool
	started time.Time

	// Metrics
	requestCount   atomic.Int64
	errorCount     atomic.Int64
	activeRequests atomic.Int64
	activeTxReqs   atomic.Int64

	// Slow query logging
	slowQueryLogger *log.Logger
	slowQueryCount  atomic.Int64

	// Cached search services per database (namespace-aware indexes)
	searchServicesMu sync.RWMutex
	searchServices   map[string]*search.Service

	// Cached Cypher executors per database (thread-safe, reusable)
	executorsMu sync.RWMutex
	executors   map[string]*cypher.StorageExecutor

	// Explicit transaction sessions shared across transports.
	txSessions *txsession.Manager

	// Per-database access control (Neo4j-aligned). When auth disabled, Full is used.
	// When auth enabled, allowlistStore (if set) provides allowlist-based mode per principal.
	databaseAccessMode    auth.DatabaseAccessMode
	allowlistStore        *auth.AllowlistStore        // loaded from system DB when auth enabled
	roleStore             *auth.RoleStore             // user-defined roles when auth enabled
	privilegesStore       *auth.PrivilegesStore       // per-DB read/write (Phase 4) when auth enabled
	roleEntitlementsStore *auth.RoleEntitlementsStore // per-role global entitlements when auth enabled
	dbConfigStore         *dbconfig.Store             // per-DB config overrides (embedding, search, etc.)
	// perDBYAMLOverrides carries the `databases:` map from nornicdb.yaml,
	// passed through by the binary at construction time. Seeded into
	// dbConfigStore on first boot via LoadWithYAMLDefaults; subsequent
	// admin API edits are authoritative across restarts.
	perDBYAMLOverrides map[string]map[string]string
}

// ensureSearchBuildStartedForKnownDatabases reconciles search-service startup for
// databases known to DatabaseManager, including metadata-only empty databases.
// It is safe to call repeatedly; per-db build start is idempotent.
func (s *Server) ensureSearchBuildStartedForKnownDatabases() {
	if s == nil || s.db == nil || s.dbManager == nil {
		return
	}
	for _, info := range s.dbManager.ListDatabases() {
		if info == nil || info.Name == "" || info.Name == "system" {
			continue
		}
		if s.dbManager.IsCompositeDatabase(info.Name) {
			continue
		}
		status := s.db.GetDatabaseSearchStatus(info.Name)
		if status.Initialized {
			continue
		}
		storageEngine, err := s.dbManager.GetStorage(info.Name)
		if err != nil {
			s.log.Warn("startup search reconcile: storage unavailable", "subsystem", "search", "db", info.Name, "error", err)
			continue
		}
		if _, err := s.db.EnsureSearchIndexesBuildStarted(info.Name, storageEngine); err != nil {
			s.log.Warn("startup search reconcile failed", "subsystem", "search", "db", info.Name, "error", err)
		}
	}
}

// mcpToolRunnerAdapter adapts pkg/mcp.Server to heimdall.InMemoryToolRunner so the agentic loop
// can expose store, recall, discover, link, task, tasks to the LLM and execute them in process.
// allowlist: nil = all tools, empty = no tools, non-empty = only those names.
type mcpToolRunnerAdapter struct {
	s         *mcp.Server
	allowlist []string
}

func (a *mcpToolRunnerAdapter) allowedNames() []string {
	if a.allowlist == nil {
		return mcp.AllTools()
	}
	return a.allowlist
}

func (a *mcpToolRunnerAdapter) allow(name string) bool {
	allowed := a.allowedNames()
	for _, n := range allowed {
		if n == name {
			return true
		}
	}
	return false
}

func (a *mcpToolRunnerAdapter) ToolDefinitions() []heimdall.MCPTool {
	defs := a.s.ToolDefinitions()
	allowed := a.allowedNames()
	if len(allowed) == 0 {
		return nil
	}
	allowSet := make(map[string]struct{}, len(allowed))
	for _, n := range allowed {
		allowSet[n] = struct{}{}
	}
	var out []heimdall.MCPTool
	for _, t := range defs {
		if _, ok := allowSet[t.Name]; ok {
			out = append(out, heimdall.MCPTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
		}
	}
	return out
}

func (a *mcpToolRunnerAdapter) ToolNames() []string {
	return a.allowedNames()
}

func (a *mcpToolRunnerAdapter) CallTool(ctx context.Context, name string, args map[string]interface{}, dbName string) (interface{}, error) {
	if !a.allow(name) {
		return nil, fmt.Errorf("tool %q is not in the MCP allowlist", name)
	}
	// Ensure we always pass a concrete database so MCP uses DatabaseScopedExecutor when set.
	// Empty dbName would cause MCP to fall back to s.db, which can diverge from the default DB in multi-db setups.
	if dbName == "" {
		dbName = a.s.DefaultDatabaseName()
	}
	ctx = mcp.ContextWithDatabase(ctx, dbName)
	return a.s.CallTool(ctx, name, args)
}

// mcpDatabaseScopedExecutor returns a callback that provides a Cypher executor and node getter for the given database.
// Used so MCP tools (store, recall, link, etc.) run against the request's database when invoked from the agentic loop.
func (s *Server) mcpDatabaseScopedExecutor() func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
	return func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
		exec, err := s.getExecutorForDatabase(dbName)
		if err != nil {
			return nil, nil, err
		}
		getNode := func(ctx context.Context, id string) (*nornicdb.Node, error) {
			result, err := exec.Execute(ctx, "MATCH (n) WHERE elementId(n) = $id RETURN n", map[string]interface{}{"id": id})
			if err != nil {
				return nil, err
			}
			if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
				return nil, nornicdb.ErrNotFound
			}
			v := result.Rows[0][0]
			if snode, ok := v.(*storage.Node); ok {
				props := make(map[string]interface{}, len(snode.Properties))
				for k, val := range snode.Properties {
					props[k] = val
				}
				return &nornicdb.Node{
					ID:         string(snode.ID),
					Labels:     snode.Labels,
					Properties: props,
					CreatedAt:  snode.CreatedAt,
				}, nil
			}
			return nil, nornicdb.ErrNotFound
		}
		return exec, getNode, nil
	}
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
	// Phase 2 D-01a: graceful-degrade discard fallback when caller did not
	// thread observability.Provider.Logger() through Config.Logger. Keeps
	// existing tests/callers compileable; tightens post-M1 once all
	// consumers wire the logger explicitly.
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	// Note: GPU status is logged in main.go during GPU manager initialization
	// This avoids duplicate logs and provides more detailed information

	// Load environment-backed global config once (used for multi-db + feature defaults).
	globalConfig := nornicConfig.LoadFromEnv()

	// Create MCP server for LLM tool interface (if enabled)
	var mcpServer *mcp.Server
	if config.MCPEnabled {
		mcpConfig := mcp.DefaultServerConfig()
		mcpConfig.EmbeddingEnabled = config.EmbeddingEnabled
		mcpConfig.EmbeddingModel = config.EmbeddingModel
		mcpConfig.EmbeddingDimensions = config.EmbeddingDimensions
		mcpConfig.DefaultNodeLabel = globalConfig.Memory.DefaultNodeLabel
		mcpServer = mcp.NewServer(db, mcpConfig)
	} else {
		config.Logger.With("component", "server").Info("mcp server disabled via configuration")
	}

	// Initialize DatabaseManager for multi-database support.
	// IMPORTANT: This must happen before Heimdall/GraphQL so they can route per database.
	//
	// Get the base storage engine from the DB (unwraps the namespaced storage).
	// DatabaseManager will create NamespacedEngines for each logical database.
	storageEngine := db.GetBaseStorageForManager()
	remoteCredentialEncryptionKey := ""
	switch {
	case strings.TrimSpace(os.Getenv("NORNICDB_REMOTE_CREDENTIALS_KEY")) != "":
		remoteCredentialEncryptionKey = strings.TrimSpace(os.Getenv("NORNICDB_REMOTE_CREDENTIALS_KEY"))
	case strings.TrimSpace(globalConfig.Database.EncryptionPassword) != "":
		remoteCredentialEncryptionKey = strings.TrimSpace(globalConfig.Database.EncryptionPassword)
		config.Logger.With("component", "server").Warn("remote credential encryption key fallback in use",
			"fallback", "database_encryption_password",
			"remediation", "set NORNICDB_REMOTE_CREDENTIALS_KEY for key separation")
	case strings.TrimSpace(globalConfig.Auth.JWTSecret) != "":
		remoteCredentialEncryptionKey = strings.TrimSpace(globalConfig.Auth.JWTSecret)
		config.Logger.With("component", "server").Warn("remote credential encryption key fallback in use",
			"fallback", "jwt_signing_secret",
			"remediation", "set NORNICDB_REMOTE_CREDENTIALS_KEY for stronger key separation")
	}
	multiDBConfig := &multidb.Config{
		DefaultDatabase:               globalConfig.Database.DefaultDatabase,
		SystemDatabase:                "system",
		MaxDatabases:                  0, // Unlimited
		AllowDropDefault:              false,
		RemoteCredentialEncryptionKey: remoteCredentialEncryptionKey,
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			useUserPassword := strings.EqualFold(strings.TrimSpace(ref.AuthMode), "user_password")
			cfg := storage.RemoteEngineConfig{
				URI:       ref.URI,
				Database:  ref.DatabaseName,
				AuthToken: authToken,
			}
			if useUserPassword {
				cfg.User = ref.User
				cfg.Password = ref.Password
			}
			return storage.NewRemoteEngine(cfg)
		},
	}
	dbManager, err := multidb.NewDatabaseManager(storageEngine, multiDBConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database manager: %w", err)
	}

	s := &Server{
		config:             config,
		db:                 db,
		dbManager:          dbManager,
		auth:               authenticator,
		log:                config.Logger.With("component", "server"),
		mcpServer:          mcpServer,
		graphqlHandler:     graphql.NewHandler(db, dbManager),
		basicAuthCache:     auth.NewBasicAuthCache(auth.DefaultAuthCacheEntries, auth.DefaultAuthCacheTTL),
		searchServices:     make(map[string]*search.Service),
		executors:          make(map[string]*cypher.StorageExecutor),
		perDBYAMLOverrides: config.PerDBYAMLOverrides,
	}
	// Foreground-first policy: while tx requests are active, background embed work yields.
	s.db.SetEmbedQueueShouldYield(func() bool {
		return s.activeTxReqs.Load() > 0
	})
	s.txSessions = txsession.NewManager(30*time.Second, s.newExecutorForDatabase)
	s.txSessions.SetTerminalErrorObserver(func(session *txsession.Session, err error) {
		s.logMVCCSnapshotExpiration(session, err)
	})

	// ==========================================================================
	// Heimdall - AI Assistant for Database Management
	// ==========================================================================
	// Use features config passed from main.go (which loads from YAML + env)
	// Fall back to LoadFromEnv() if not provided (for backwards compatibility)
	var featuresConfig *nornicConfig.FeatureFlagsConfig
	if config.Features != nil {
		featuresConfig = config.Features
	} else {
		featuresConfig = &globalConfig.Features
		config.Features = featuresConfig
	}
	var rerankerResolverMu sync.RWMutex
	var globalRerankerResolver func(string) search.Reranker
	perDBRerankerCache := make(map[string]search.Reranker)
	setGlobalRerankerResolver := func(fn func(string) search.Reranker) {
		rerankerResolverMu.Lock()
		globalRerankerResolver = fn
		rerankerResolverMu.Unlock()
	}
	getGlobalRerankerResolver := func() func(string) search.Reranker {
		rerankerResolverMu.RLock()
		defer rerankerResolverMu.RUnlock()
		return globalRerankerResolver
	}
	resolveDBRerankConfig := func(dbName string) (enabled bool, provider, model, apiURL, apiKey string) {
		enabled = featuresConfig.SearchRerankEnabled
		provider = strings.TrimSpace(strings.ToLower(featuresConfig.SearchRerankProvider))
		model = featuresConfig.SearchRerankModel
		apiURL = strings.TrimSpace(featuresConfig.SearchRerankAPIURL)
		apiKey = featuresConfig.SearchRerankAPIKey
		if provider == "" {
			provider = "local"
		}
		if provider == "ollama" && apiURL == "" {
			apiURL = "http://localhost:11434/rerank"
		}
		if s.dbConfigStore == nil {
			return enabled, provider, model, apiURL, apiKey
		}
		overrides := s.dbConfigStore.GetOverrides(dbName)
		resolved := dbconfig.Resolve(globalConfig, overrides)
		if resolved == nil || resolved.Effective == nil {
			return enabled, provider, model, apiURL, apiKey
		}
		eff := resolved.Effective
		if raw := strings.TrimSpace(strings.ToLower(eff["NORNICDB_SEARCH_RERANK_ENABLED"])); raw != "" {
			switch raw {
			case "1", "true", "yes", "on":
				enabled = true
			case "0", "false", "no", "off":
				enabled = false
			}
		}
		if v := strings.TrimSpace(strings.ToLower(eff["NORNICDB_SEARCH_RERANK_PROVIDER"])); v != "" {
			provider = v
		}
		if v := eff["NORNICDB_SEARCH_RERANK_MODEL"]; strings.TrimSpace(v) != "" {
			model = strings.TrimSpace(v)
		}
		if v := eff["NORNICDB_SEARCH_RERANK_API_URL"]; strings.TrimSpace(v) != "" {
			apiURL = strings.TrimSpace(v)
		}
		if v := eff["NORNICDB_SEARCH_RERANK_API_KEY"]; strings.TrimSpace(v) != "" {
			apiKey = v
		}
		if provider == "ollama" && apiURL == "" {
			apiURL = "http://localhost:11434/rerank"
		}
		return enabled, provider, model, apiURL, apiKey
	}
	getOrCreateExternalReranker := func(provider, model, apiURL, apiKey string) search.Reranker {
		key := strings.Join([]string{provider, model, apiURL, apiKey}, "|")
		rerankerResolverMu.RLock()
		if cached, ok := perDBRerankerCache[key]; ok {
			rerankerResolverMu.RUnlock()
			return cached
		}
		rerankerResolverMu.RUnlock()
		if apiURL == "" {
			return nil
		}
		ceConfig := &search.CrossEncoderConfig{
			Enabled:  true,
			APIURL:   apiURL,
			APIKey:   apiKey,
			Model:    model,
			TopK:     100,
			Timeout:  30 * time.Second,
			MinScore: 0.0,
		}
		if ceConfig.Model == "" && provider == "ollama" {
			ceConfig.Model = "reranker"
		}
		ce := search.NewCrossEncoder(ceConfig)
		rerankerResolverMu.Lock()
		perDBRerankerCache[key] = ce
		rerankerResolverMu.Unlock()
		return ce
	}
	// Install per-DB reranker resolver. It respects DB overrides and falls back to global resolver.
	db.SetRerankerResolver(func(dbName string) search.Reranker {
		enabled, provider, model, apiURL, apiKey := resolveDBRerankConfig(dbName)
		if !enabled {
			return nil
		}
		if provider == "local" {
			if resolver := getGlobalRerankerResolver(); resolver != nil {
				return resolver(dbName)
			}
			return nil
		}
		if r := getOrCreateExternalReranker(provider, model, apiURL, apiKey); r != nil {
			return r
		}
		if resolver := getGlobalRerankerResolver(); resolver != nil {
			return resolver(dbName)
		}
		return nil
	})
	if featuresConfig.HeimdallEnabled {
		// Derive a child logger once at goroutine entry so every record
		// carries subsystem=heimdall (Phase 2 D-10a).
		heimdallLog := s.log.With("subsystem", "heimdall")
		heimdallLog.Info("heimdall AI assistant initializing asynchronously")
		go func() {
			// Configure token budget from environment variables
			heimdall.SetTokenBudget(featuresConfig)
			heimdallCfg := heimdall.ConfigFromFeatureFlags(featuresConfig)
			// Log resolved provider so users can verify env overrides (openai/ollama/local)
			provider := strings.TrimSpace(strings.ToLower(heimdallCfg.Provider))
			if provider == "" {
				provider = "local"
			}
			heimdallLog.Info("heimdall provider resolved",
				"provider", provider,
				"override_env", "NORNICDB_HEIMDALL_PROVIDER")
			manager, err := heimdall.NewManager(heimdallCfg)
			if err != nil {
				heimdallLog.Warn("heimdall initialization failed",
					"error", err,
					"remediation", "check NORNICDB_HEIMDALL_MODEL and NORNICDB_MODELS_DIR")
				return
			}
			if baseExec := db.GetCypherExecutor(); baseExec != nil {
				baseExec.SetInferenceManager(manager)
			}

			// Create database router wrapper for Heimdall (multi-db aware)
			dbRouter := newHeimdallDBRouter(db, dbManager, featuresConfig)
			metricsReader := &heimdallMetricsReader{}
			handler := heimdall.NewHandler(manager, heimdallCfg, dbRouter, metricsReader)
			// Expose MCP tools to the agentic loop only when enabled (default off to avoid context bloat)
			if mcpServer != nil && featuresConfig.HeimdallMCPEnable {
				handler.SetInMemoryToolRunner(&mcpToolRunnerAdapter{
					s:         mcpServer,
					allowlist: featuresConfig.HeimdallMCPTools,
				})
			}

			// Initialize Heimdall plugin subsystem
			subsystemMgr := heimdall.GetSubsystemManager()

			// Create the Heimdall invoker so plugins can call the LLM
			heimdallInvoker := heimdall.NewLiveHeimdallInvoker(
				subsystemMgr,
				manager, // Manager implements Generator interface
				handler.Bifrost(),
				dbRouter,
				metricsReader,
			)

			subsystemCtx := heimdall.SubsystemContext{
				Config:   heimdallCfg,
				Bifrost:  handler.Bifrost(),
				Database: dbRouter,
				Metrics:  metricsReader,
				Heimdall: heimdallInvoker, // Now plugins can call p.ctx.Heimdall.SendPrompt()
			}
			subsystemMgr.SetContext(subsystemCtx)

			// Load plugins from configured directories.
			if config.PluginsDir != "" {
				heimdallLog.Debug("loading APOC plugins", "dir", config.PluginsDir)
				if err := nornicdb.LoadPluginsFromDir(config.PluginsDir, &subsystemCtx); err != nil {
					heimdallLog.Warn("failed to load APOC plugins", "dir", config.PluginsDir, "error", err)
				}
			}
			if config.HeimdallPluginsDir != "" && config.HeimdallPluginsDir != config.PluginsDir {
				heimdallLog.Debug("loading Heimdall plugins", "dir", config.HeimdallPluginsDir)
				if err := nornicdb.LoadPluginsFromDir(config.HeimdallPluginsDir, &subsystemCtx); err != nil {
					heimdallLog.Warn("failed to load Heimdall plugins", "dir", config.HeimdallPluginsDir, "error", err)
				}
			} else if config.HeimdallPluginsDir == "" {
				heimdallLog.Debug("heimdall plugins dir is empty")
			} else {
				heimdallLog.Debug("heimdall plugins dir same as plugins dir; skipping",
					"heimdall_dir", config.HeimdallPluginsDir,
					"plugins_dir", config.PluginsDir)
			}

			s.setHeimdallHandler(handler)

			plugins := heimdall.ListHeimdallPlugins()
			actions := heimdall.ListHeimdallActions()
			heimdallLog.Info("heimdall AI assistant ready",
				"model", heimdallCfg.Model,
				"plugins_loaded", len(plugins),
				"actions_available", len(actions),
				"bifrost_chat_route", "/api/bifrost/chat/completions",
				"status_route", "/api/bifrost/status",
			)
			if len(plugins) == 0 {
				heimdallLog.Warn("no heimdall plugins loaded — watcher logs will be absent",
					"remediation", "ensure a .so exists in HeimdallPluginsDir")
			}
			for _, actionName := range actions {
				heimdallLog.Debug("heimdall action registered", "action", actionName)
			}
		}()
	} else {
		s.log.Info("heimdall AI assistant disabled",
			"subsystem", "heimdall",
			"override_env", "NORNICDB_HEIMDALL_ENABLED")
	}

	// Independent search rerank (Stage-2 reranking, not tied to Heimdall).
	// Supports local (GGUF, like embeddings) or external provider (ollama/openai/http),
	// similar to Heimdall and embeddings.
	if featuresConfig.SearchRerankEnabled {
		provider := strings.TrimSpace(strings.ToLower(featuresConfig.SearchRerankProvider))
		if provider == "" {
			provider = "local"
		}

		if provider == "local" {
			// Load GGUF into memory (same pattern as embedding model).
			modelsDir := config.ModelsDir
			if modelsDir == "" {
				modelsDir = "./models"
			}
			modelName := featuresConfig.SearchRerankModel
			if modelName == "" {
				modelName = "bge-reranker-v2-m3-Q4_K_M.gguf"
			}
			if !strings.HasSuffix(modelName, ".gguf") {
				modelName = modelName + ".gguf"
			}
			modelPath := filepath.Join(modelsDir, modelName)

			rerankLog := s.log.With("subsystem", "search_rerank")
			rerankLog.Info("loading search reranker model",
				"provider", "local",
				"model_path", modelPath,
				"note", "server starts immediately; reranking available after model loads")

			go func() {
				opts := localllm.DefaultOptions(modelPath)
				opts.GPULayers = -1
				rerankerModel, err := localllm.LoadRerankerModel(opts)
				if err != nil {
					rerankLog.Warn("search reranker model unavailable; stage-2 reranking disabled, RRF order only",
						"error", err)
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, healthErr := rerankerModel.Score(ctx, "health", "check")
				cancel()
				if healthErr != nil {
					rerankerModel.Close()
					rerankLog.Warn("search reranker failed health check", "error", healthErr)
					return
				}

				cfg := search.DefaultLocalRerankerConfig()
				cfg.Enabled = true
				r := search.NewLocalReranker(rerankerModel, cfg)
				db.SetSearchReranker(r)
				setGlobalRerankerResolver(func(string) search.Reranker { return r })
				rerankLog.Info("search reranker ready (stage-2 reranking enabled)",
					"model", modelName)
			}()
		} else {
			// External provider: use HTTP rerank API (Cohere, HuggingFace TEI, Ollama adapter, etc.).
			apiURL := strings.TrimSpace(featuresConfig.SearchRerankAPIURL)
			if apiURL == "" {
				if provider == "ollama" {
					apiURL = "http://localhost:11434/rerank"
				}
			}
			if apiURL == "" {
				s.log.Warn("search rerank enabled but API URL not set; stage-2 reranking disabled",
					"subsystem", "search_rerank",
					"provider", provider,
					"required_env", "NORNICDB_SEARCH_RERANK_API_URL")
			} else {
				ceConfig := &search.CrossEncoderConfig{
					Enabled:  true,
					APIURL:   apiURL,
					APIKey:   featuresConfig.SearchRerankAPIKey,
					Model:    featuresConfig.SearchRerankModel,
					TopK:     100,
					Timeout:  30 * time.Second,
					MinScore: 0.0,
				}
				if ceConfig.Model == "" && provider == "ollama" {
					ceConfig.Model = "reranker"
				}
				ce := search.NewCrossEncoder(ceConfig)
				db.SetSearchReranker(ce)
				setGlobalRerankerResolver(func(string) search.Reranker { return ce })
				s.log.Info("search reranker ready (stage-2 reranking enabled)",
					"subsystem", "search_rerank",
					"provider", provider,
					"url", apiURL)
			}
		}
	} else {
		s.log.Info("search rerank disabled",
			"subsystem", "search_rerank",
			"override_env", "NORNICDB_SEARCH_RERANK_ENABLED")
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
		embedInitLog := s.log.With("subsystem", "embed_init")
		embedInitLog.Info("loading embedding model",
			"model", embedConfig.Model,
			"provider", embedConfig.Provider,
			"note", "server starts immediately; embeddings available after model loads")

		go func() {
			// Retry forever: exponential backoff to 5m, then fixed 5m interval.
			const (
				initialBackoff = 2 * time.Second
				maxBackoff     = 5 * time.Minute
			)

			backoff := initialBackoff
			attempt := 0

			for {
				if s.closed.Load() {
					embedInitLog.Info("embedding init retry loop stopped: server shutting down")
					return
				}

				attempt++

				// Use factory function for all providers.
				embedder, err := embed.NewEmbedder(embedConfig)
				if err == nil {
					// Health check: test embedding before enabling.
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_, healthErr := embedder.Embed(ctx, "health check")
					cancel()
					if healthErr != nil {
						err = fmt.Errorf("health check failed: %w", healthErr)
					}
				}

				if err == nil {
					// Wrap with caching if enabled (default: 10K cache).
					if config.EmbeddingCacheSize > 0 {
						embedder = embed.NewCachedEmbedder(embedder, config.EmbeddingCacheSize)
						embedInitLog.Info("embedding cache enabled",
							"entries", config.EmbeddingCacheSize,
							"memory_mb", embeddingCacheMemoryMB(config.EmbeddingCacheSize, config.EmbeddingDimensions))
					}

					if config.EmbeddingProvider == "local" {
						embedInitLog.Info("embeddings ready",
							"provider", "local_gguf",
							"model", config.EmbeddingModel,
							"dims", config.EmbeddingDimensions)
					} else {
						embedInitLog.Info("embeddings ready",
							"provider", config.EmbeddingProvider,
							"url", config.EmbeddingAPIURL,
							"model", config.EmbeddingModel,
							"dims", config.EmbeddingDimensions)
					}

					if mcpServer != nil {
						mcpServer.SetEmbedder(embedder)
					}
					// Share embedder with DB for auto-embed queue.
					// The embed worker will wait for this to be set before processing.
					db.SetEmbedder(embedder)
					// Register as default for per-DB embedder registry (no-op if SetEmbedConfigForDB was not set).
					db.SetDefaultEmbedConfig(embedConfig)
					return
				}

				if config.EmbeddingProvider == "local" {
					embedInitLog.Warn("embedding init attempt failed",
						"attempt", attempt,
						"provider", "local",
						"model", config.EmbeddingModel,
						"error", err)
				} else {
					embedInitLog.Warn("embedding init attempt failed",
						"attempt", attempt,
						"provider", config.EmbeddingProvider,
						"model", config.EmbeddingModel,
						"url", config.EmbeddingAPIURL,
						"error", err)
				}

				if backoff < maxBackoff {
					embedInitLog.Info("retrying embedding init (exponential backoff)", "wait", backoff)
					if !waitForDurationOrServerClose(s, backoff) {
						embedInitLog.Info("embedding init retry interrupted by server shutdown")
						return
					}
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				} else {
					embedInitLog.Info("embedding init retry interval capped; continuing periodic retries",
						"interval", maxBackoff)
					if !waitForDurationOrServerClose(s, maxBackoff) {
						embedInitLog.Info("embedding init retry interrupted by server shutdown")
						return
					}
				}
			}
		}()
	}

	// Log authentication status
	if authenticator == nil || !authenticator.IsSecurityEnabled() {
		s.log.Warn("authentication disabled")
	}

	// Initialize rate limiter if enabled
	var rateLimiter *IPRateLimiter
	if config.RateLimitEnabled {
		rateLimiter = NewIPRateLimiter(config.RateLimitPerMinute, config.RateLimitPerHour, config.RateLimitBurst)
		s.log.Info("rate limiting enabled",
			"per_minute", config.RateLimitPerMinute,
			"per_hour", config.RateLimitPerHour,
			"scope", "per_ip")
	}
	s.rateLimiter = rateLimiter

	// So EmbeddingCount() aggregates across all databases (not just default)
	s.db.SetAllDatabasesProvider(func() []nornicdb.DatabaseAndStorage {
		var out []nornicdb.DatabaseAndStorage
		for _, info := range s.dbManager.ListDatabases() {
			if info.Name == "system" {
				continue
			}
			isComposite := s.dbManager.IsCompositeDatabase(info.Name)
			if isComposite {
				continue
			}
			storageEngine, err := s.dbManager.GetStorage(info.Name)
			if err != nil {
				continue
			}
			out = append(out, nornicdb.DatabaseAndStorage{
				Name:        info.Name,
				Storage:     storageEngine,
				IsComposite: isComposite,
			})
		}
		return out
	})

	// Reconcile search-service startup for metadata-only or late-created databases.
	// DB.Open() warms namespaces present in storage; this loop ensures known DB metadata
	// also gets initialized, and keeps doing so without requiring first-search triggers.
	s.ensureSearchBuildStartedForKnownDatabases()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			if s.closed.Load() {
				return
			}
			s.ensureSearchBuildStartedForKnownDatabases()
			<-ticker.C
		}
	}()

	// Wire MCP to use per-database executors when invoked from the agentic loop (so link/store/recall use the request's database)
	if mcpServer != nil && dbManager != nil {
		mcpServer.SetDatabaseScopedExecutor(s.mcpDatabaseScopedExecutor())
		mcpServer.SetDatabaseScopedStorage(func(dbName string) (storage.Engine, error) {
			return s.dbManager.GetStorage(dbName)
		})
	}

	// Initialize OAuth manager if authenticator is available
	if authenticator != nil {
		s.oauthManager = auth.NewOAuthManager(authenticator)
	}

	// Per-database access: Full when auth disabled; when auth enabled, DenyAll until allowlist resolves.
	if authenticator == nil || !authenticator.IsSecurityEnabled() {
		s.databaseAccessMode = auth.FullDatabaseAccessMode
	} else {
		s.databaseAccessMode = auth.DenyAllDatabaseAccessMode
	}

	// Load RBAC stores from system DB when available so roles/allowlist/privileges/entitlements APIs
	// work even with --no-auth (e.g. fetch roles, configure RBAC before enabling auth).
	if systemStorage, err := dbManager.GetStorage("system"); err == nil {
		ctx := context.Background()
		roleStore := auth.NewRoleStore(systemStorage)
		if loadErr := roleStore.Load(ctx); loadErr != nil {
			s.log.Warn("failed to load RBAC roles", "subsystem", "rbac", "error", loadErr)
		} else {
			s.roleStore = roleStore
		}
		allowlistStore := auth.NewAllowlistStore(systemStorage)
		if loadErr := allowlistStore.Load(ctx); loadErr != nil {
			s.log.Warn("failed to load RBAC allowlist", "subsystem", "rbac", "error", loadErr)
		} else {
			dbList := make([]string, 0, len(dbManager.ListDatabases()))
			for _, info := range dbManager.ListDatabases() {
				dbList = append(dbList, info.Name)
			}
			if seedErr := allowlistStore.SeedIfEmpty(ctx, dbList); seedErr != nil {
				s.log.Warn("failed to seed RBAC allowlist", "subsystem", "rbac", "error", seedErr)
			}
			s.allowlistStore = allowlistStore
		}
		privilegesStore := auth.NewPrivilegesStore(systemStorage)
		if loadErr := privilegesStore.Load(ctx); loadErr != nil {
			s.log.Warn("failed to load RBAC privileges", "subsystem", "rbac", "error", loadErr)
		} else {
			s.privilegesStore = privilegesStore
		}
		roleEntitlementsStore := auth.NewRoleEntitlementsStore(systemStorage)
		if loadErr := roleEntitlementsStore.Load(ctx); loadErr != nil {
			s.log.Warn("failed to load RBAC role entitlements", "subsystem", "rbac", "error", loadErr)
		} else {
			s.roleEntitlementsStore = roleEntitlementsStore
		}
		dbConfigStore := dbconfig.NewStore(systemStorage)
		// Seed yaml-declared per-DB overrides on first boot. After the
		// first successful seed, admin-API edits are authoritative across
		// restarts (LoadWithYAMLDefaults skips keys already in the store).
		// Falls back to plain Load when no yaml overrides are present.
		yamlOverrides := s.perDBYAMLOverrides
		if loadErr := dbConfigStore.LoadWithYAMLDefaults(ctx, yamlOverrides); loadErr != nil {
			s.log.Warn("failed to load per-DB config store", "subsystem", "dbconfig", "error", loadErr)
		} else {
			s.dbConfigStore = dbConfigStore
			globalConfig := nornicConfig.LoadFromEnv()
			db.SetDbConfigResolver(func(dbName string) (int, float64, string) {
				overrides := dbConfigStore.GetOverrides(dbName)
				r := dbconfig.Resolve(globalConfig, overrides)
				if r == nil {
					return 0, 0, ""
				}
				return r.EmbeddingDimensions, r.SearchMinSimilarity, r.BM25Engine
			})
			db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
				overrides := dbConfigStore.GetOverrides(dbName)
				r := dbconfig.Resolve(globalConfig, overrides)
				if r == nil {
					return true, true, "startup", "startup"
				}
				return r.BM25Enabled, r.VectorEnabled, r.BM25Warming, r.VectorWarming
			})
			// Per-DB embedder registry: resolve embed config per database for EmbedQueryForDB.
			db.SetEmbedConfigForDB(func(dbName string) (*embed.Config, error) {
				overrides := dbConfigStore.GetOverrides(dbName)
				r := dbconfig.Resolve(globalConfig, overrides)
				if r == nil || r.Effective == nil {
					return nil, nil
				}
				return buildEmbedConfigFromResolved(r.Effective, config), nil
			})
		}
	}
	// Release the search-index warmup gate. nornicdb.Open holds the
	// warmup goroutine at a sync point until this is called so it can
	// resolve per-DB flags through the resolver wired above instead of
	// racing with it. Default DB and named DBs go through the same
	// resolution path; per-DB overrides (YAML databases: block, admin
	// API store) apply uniformly. If wiring above failed (e.g. system
	// storage unavailable) the gate is still released — warmup falls
	// through to db.config.Memory.Search* + the (true,true,startup)
	// final fallback, which matches today's behaviour for misconfigured
	// systems.
	db.MarkSearchWarmupReady()

	// Initialize slow query logger if file specified.
	// D-04d collapse: threshold + log file path read from the canonical
	// pkg/config.LoggingConfig snapshot threaded via Config.Logging.
	if config.SlowQueryEnabled && config.Logging.SlowQueryLogFile != "" {
		file, err := os.OpenFile(config.Logging.SlowQueryLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			s.log.Warn("failed to open slow query log file",
				"subsystem", "slow_query",
				"file", config.Logging.SlowQueryLogFile,
				"error", err)
		} else {
			s.slowQueryLogger = log.New(file, "", log.LstdFlags)
			s.log.Info("slow query logging configured",
				"subsystem", "slow_query",
				"file", config.Logging.SlowQueryLogFile,
				"threshold", config.Logging.SlowQueryThreshold)
		}
	} else if config.SlowQueryEnabled {
		s.log.Info("slow query logging enabled",
			"subsystem", "slow_query",
			"threshold", config.Logging.SlowQueryThreshold)
	}

	return s, nil
}

// SetHTTPMetrics injects the Plan-04-02 HTTP catalog bag (D-02 typed
// handle DI). MUST be called BEFORE Start() — once the http.Server's
// Handler is wired in Start(), the wrapper is fixed for the server
// lifetime. Callers (cmd/nornicdb/main.go) inject after observability.New
// returns the registry, then call Start().
//
// Nil-safe: passing nil is equivalent to never calling — instrumentedMux
// is a pass-through. Test fixtures and pre-Phase-4 callers compile and
// run unchanged.
func (s *Server) SetHTTPMetrics(m *observability.HTTPMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpMetrics = m
}

// SetObsRegistry plumbs the unified prometheus registry from
// observability.New into the server so handleMetrics can call
// observability.RenderLegacy. Phase 5 / Plan 05-04. Mirrors the
// SetHTTPMetrics pattern (mu.Lock + assign + unlock).
//
// Nil-safe: passing nil is equivalent to never calling — handleMetrics
// tolerates a nil registry by emitting empty body bytes (RenderLegacy
// contract). Test fixtures and pre-Phase-5 callers compile and run
// unchanged.
func (s *Server) SetObsRegistry(reg *prometheus.Registry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.obsRegistry = reg
}

// SetAuditLogger sets the audit logger for compliance logging.
func (s *Server) SetAuditLogger(logger *audit.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = logger
	if s.db != nil {
		s.db.SetRetentionAuditCallback(func(action, recordID, category string) {
			if s.audit == nil {
				return
			}
			_ = s.audit.LogDataAccess("system", "retention-manager", "node", recordID, action, true, category)
		})
	}
}

func (s *Server) setHeimdallHandler(handler *heimdall.Handler) {
	s.mu.Lock()
	s.heimdallHandler = handler
	s.mu.Unlock()
}

func (s *Server) getHeimdallHandler() *heimdall.Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.heimdallHandler
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
//	// Server started on server.Addr()
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

	// Plan 04-02 D-03: instrumentedMux is the SOLE HTTP observation
	// chokepoint per AGENTS.md §7 DRY. It wraps mux.ServeHTTP, reading
	// `r.Pattern` post-dispatch (Go 1.22+ stdlib field) so path_template
	// values come from the closed route table — never from r.URL.Path
	// (cardinality bomb). Nil-safe: when s.httpMetrics is nil (test
	// fixtures, pre-Phase-4 callers), the wrapper is a pass-through.
	// Panic-safe: handler panics still emit a 5xx observation before
	// re-propagating (T-04-08).
	instrumented := instrumentedMux(mux, s.httpMetrics)

	s.requestsCtx, s.cancelRequestsCtx = context.WithCancel(context.Background())
	s.httpServer = &http.Server{
		Handler:      instrumented,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
		// BaseContext links every request's r.Context() to the server's
		// shutdown signal. When Stop() cancels requestsCtx, all in-flight
		// handlers see ctx.Err() != nil on their next cancellation probe and
		// unwind, so http.Server.Shutdown returns instead of waiting on a
		// long-running BFS.
		BaseContext: func(net.Listener) context.Context { return s.requestsCtx },
	}

	// Configure HTTP/2 (always enabled, backwards compatible with HTTP/1.1)
	http2Config := &http2.Server{
		MaxConcurrentStreams: s.config.HTTP2MaxConcurrentStreams,
	}

	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		// HTTPS mode: HTTP/2 is automatically enabled via ALPN
		// Configure HTTP/2 settings for TLS connections
		if err := http2.ConfigureServer(s.httpServer, http2Config); err != nil {
			return fmt.Errorf("failed to configure HTTP/2 for TLS: %w", err)
		}
		s.log.Info("HTTP/2 enabled", "mode", "https")
	} else {
		// HTTP mode: Use h2c (HTTP/2 cleartext) for backwards compatibility
		// h2c allows HTTP/2 over plain TCP, falling back to HTTP/1.1 for older clients
		// Wrap the INSTRUMENTED mux (not bare mux) so observation runs
		// inside the h2c transport adapter (Plan 04-02 D-03).
		s.httpServer.Handler = h2c.NewHandler(instrumented, http2Config)
		s.log.Info("HTTP/2 enabled", "mode", "h2c_cleartext", "compat", "http/1.1")
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
			s.log.Error("http server error", "error", err)
		}
	}()

	// Optional gRPC endpoints (feature-flagged).
	if err := s.startQdrantGRPC(); err != nil {
		_ = s.httpServer.Shutdown(context.Background())
		return err
	}

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	s.stopQdrantGRPC()

	// Stop rate limiter cleanup goroutine
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}

	if s.httpServer == nil {
		return nil
	}

	// Cancel the BaseContext handed to all in-flight requests so handlers that
	// honour ctx (Cypher BFS / shortestPath traversals, etc.) abandon work
	// and let http.Server.Shutdown drain promptly. Without this, an unbounded
	// shortestPath traversal could hold shutdown open for the duration of the
	// configured WriteTimeout.
	if s.cancelRequestsCtx != nil {
		s.cancelRequestsCtx()
	}

	// Hard-bound shutdown: even if net/http Shutdown fails to return at ctx deadline
	// (e.g., a stuck handler or an internal deadlock), Stop must return so callers
	// can exit deterministically.
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- s.httpServer.Shutdown(ctx)
	}()

	select {
	case err := <-shutdownDone:
		if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			_ = s.httpServer.Close()
		}
		return err
	case <-ctx.Done():
		_ = s.httpServer.Close()
		return ctx.Err()
	}
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
//	// stats.Uptime: server uptime
//	// stats.RequestCount: total requests
//	// stats.ErrorCount / stats.RequestCount: error rate
//	// stats.ActiveRequests: in-flight requests
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
		Version:        buildinfo.Version(),
		Commit:         buildinfo.ShortCommit(),
		BuildTime:      buildinfo.BuildTime,
	}
}

// ServerStats holds server metrics.
type ServerStats struct {
	Uptime         time.Duration `json:"uptime"`
	RequestCount   int64         `json:"request_count"`
	ErrorCount     int64         `json:"error_count"`
	ActiveRequests int64         `json:"active_requests"`
	Version        string        `json:"version"`
	Commit         string        `json:"commit"`
	BuildTime      string        `json:"build_time"`
}
