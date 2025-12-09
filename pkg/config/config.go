// Package config handles NornicDB configuration via YAML files and environment variables.
//
// Configuration Precedence (highest to lowest):
//  1. Command-line flags (--bolt-port, --admin-password, etc.)
//  2. Environment variables (NORNICDB_*)
//  3. Config file (config.yaml)
//  4. Built-in defaults
//
// Example Usage:
//
//	config, err := config.LoadFromFile(config.FindConfigFile())
//	if err != nil {
//		log.Fatalf("Invalid config: %v", err)
//	}
//
//	fmt.Printf("Bolt server: %s:%d\n",
//		config.Server.BoltAddress, config.Server.BoltPort)
//
// Environment Variables (all use NORNICDB_ prefix):
//
// Authentication:
//   - NORNICDB_AUTH="admin:admin" or "none"
//   - NORNICDB_MIN_PASSWORD_LENGTH=8
//
// Server:
//   - NORNICDB_BOLT_PORT=7687
//   - NORNICDB_HTTP_PORT=7474
//   - NORNICDB_BOLT_ADDRESS="0.0.0.0"
//   - NORNICDB_DATA_DIR="./data"
//
// Features:
//   - NORNICDB_EMBEDDING_PROVIDER="ollama" or "openai"
//   - NORNICDB_EMBEDDING_MODEL="bge-m3"
//   - NORNICDB_HEIMDALL_ENABLED=true
//
// Logging:
//   - NORNICDB_LOG_LEVEL="INFO"
//   - NORNICDB_LOG_FORMAT="json"
//
// For a complete list, see the Config struct field documentation.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all NornicDB configuration loaded from environment variables.
//
// Configuration is organized into logical sections:
//   - Auth: Authentication and authorization
//   - Database: Storage and transaction settings
//   - Server: Bolt and HTTP server settings
//   - Memory: NornicDB-specific memory decay and embeddings
//   - Compliance: GDPR/HIPAA/FISMA/SOC2 compliance controls
//   - Logging: Logging configuration
//   - Features: Experimental and optional features (feature flags)
//
// Use LoadFromEnv() to create a Config from environment variables.
//
// Example:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Config: %s\n", config)
type Config struct {
	// Authentication (NORNICDB_AUTH format: "username/password" or "none")
	Auth AuthConfig

	// Database settings
	Database DatabaseConfig

	// Server settings
	Server ServerConfig

	// Memory/Decay settings (NornicDB-specific)
	Memory MemoryConfig

	// Embedding worker settings (NornicDB-specific)
	EmbeddingWorker EmbeddingWorkerConfig

	// Compliance settings for GDPR/HIPAA/FISMA/SOC2 (NornicDB-specific)
	Compliance ComplianceConfig

	// Logging
	Logging LoggingConfig

	// Feature flags for experimental/optional features
	Features FeatureFlagsConfig
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	// Enabled controls whether authentication is required
	Enabled bool
	// InitialUsername is the default admin username
	InitialUsername string
	// InitialPassword is the default admin password
	InitialPassword string
	// MinPasswordLength for password policy
	MinPasswordLength int
	// TokenExpiry for JWT tokens
	TokenExpiry time.Duration
	// JWTSecret for signing tokens
	JWTSecret string
}

// DatabaseConfig holds database settings.
type DatabaseConfig struct {
	// DataDir is the directory for data storage
	DataDir string
	// DefaultDatabase name
	DefaultDatabase string
	// ReadOnly mode
	ReadOnly bool
	// TransactionTimeout for long-running queries
	TransactionTimeout time.Duration
	// MaxConcurrentTransactions limit
	MaxConcurrentTransactions int

	// === Durability Settings ===
	// These control the trade-off between performance and data safety.
	// Default settings provide good balance; opt-in to stricter settings
	// for financial or critical data.

	// WALSyncMode controls when WAL writes are synced to disk.
	// - "batch" (default): fsync every WALSyncInterval - good balance
	// - "immediate": fsync after each write - safest but 2-5x slower
	// - "none": no fsync - fastest but data loss on crash
	// Environment: NORNICDB_WAL_SYNC_MODE
	WALSyncMode string

	// WALSyncInterval for batch sync mode (default: 100ms).
	// Smaller = safer but slower, larger = faster but more data at risk.
	// Environment: NORNICDB_WAL_SYNC_INTERVAL
	WALSyncInterval time.Duration

	// StrictDurability enables maximum safety settings (opt-in):
	// - WAL: immediate sync (fsync every write)
	// - Badger: SyncWrites=true
	// - AsyncEngine: smaller flush interval (10ms)
	// WARNING: 2-5x slower writes. Use for financial/critical data only.
	// Environment: NORNICDB_STRICT_DURABILITY
	StrictDurability bool

	// EncryptionEnabled controls whether database encryption is active
	// Env: NORNICDB_ENCRYPTION_ENABLED
	EncryptionEnabled bool

	// EncryptionPassword for database encryption at rest
	// Required when EncryptionEnabled is true. Use a strong password in production.
	// Env: NORNICDB_ENCRYPTION_PASSWORD
	EncryptionPassword string
}

// ServerConfig holds server settings.
type ServerConfig struct {
	// BoltEnabled controls Bolt protocol server
	BoltEnabled bool
	// BoltPort for Bolt connections (default 7687)
	BoltPort int
	// BoltAddress to bind to
	BoltAddress string
	// BoltTLSEnabled for encrypted connections
	BoltTLSEnabled bool
	// BoltTLSCert path to certificate
	BoltTLSCert string
	// BoltTLSKey path to private key
	BoltTLSKey string

	// HTTPEnabled controls HTTP API server
	HTTPEnabled bool
	// HTTPPort for HTTP connections (default 7474)
	HTTPPort int
	// HTTPAddress to bind to
	HTTPAddress string
	// HTTPSEnabled for encrypted connections
	HTTPSEnabled bool
	// HTTPSPort for HTTPS connections (default 7473)
	HTTPSPort int
	// HTTPTLSCert path to certificate
	HTTPTLSCert string
	// HTTPTLSKey path to private key
	HTTPTLSKey string

	// Environment is the runtime environment (development, production)
	// Env: NORNICDB_ENV (default: development)
	Environment string
	// AllowHTTP permits non-TLS connections (development only)
	// Env: NORNICDB_ALLOW_HTTP (default: true in development)
	AllowHTTP bool
	// PluginsDir is the directory for APOC plugins
	// Env: NORNICDB_PLUGINS_DIR (default: ./plugins)
	PluginsDir string
	// HeimdallPluginsDir is the directory for Heimdall plugins
	// Env: NORNICDB_HEIMDALL_PLUGINS_DIR (default: ./plugins/heimdall)
	HeimdallPluginsDir string
}

// EmbeddingWorkerConfig holds settings for the background embedding worker.
// Environment variables:
//   - NORNICDB_EMBED_SCAN_INTERVAL: How often to scan for unembedded nodes (default: 15m)
//   - NORNICDB_EMBED_BATCH_DELAY: Delay between processing nodes (default: 500ms)
//   - NORNICDB_EMBED_MAX_RETRIES: Max retry attempts per node (default: 3)
//   - NORNICDB_EMBED_CHUNK_SIZE: Max characters per chunk (default: 512)
//   - NORNICDB_EMBED_CHUNK_OVERLAP: Characters to overlap between chunks (default: 50)
type EmbeddingWorkerConfig struct {
	// ScanInterval is how often to scan for nodes without embeddings
	ScanInterval time.Duration
	// BatchDelay is the delay between processing individual nodes
	BatchDelay time.Duration
	// MaxRetries is the max retry attempts per node
	MaxRetries int
	// ChunkSize is max characters per chunk (matches MIMIR_EMBEDDINGS_CHUNK_SIZE)
	ChunkSize int
	// ChunkOverlap is characters to overlap between chunks (matches MIMIR_EMBEDDINGS_CHUNK_OVERLAP)
	ChunkOverlap int
}

// MemoryConfig holds NornicDB memory decay settings and runtime memory management.
type MemoryConfig struct {
	// DecayEnabled controls memory decay
	DecayEnabled bool
	// DecayInterval for recalculation
	DecayInterval time.Duration
	// ArchiveThreshold below which memories are archived
	ArchiveThreshold float64
	// EmbeddingEnabled controls whether embedding generation is active
	// Env: NORNICDB_EMBEDDING_ENABLED
	EmbeddingEnabled bool
	// EmbeddingProvider (local, ollama, openai)
	EmbeddingProvider string
	// EmbeddingModel name
	EmbeddingModel string
	// EmbeddingAPIURL endpoint
	EmbeddingAPIURL string
	// EmbeddingDimensions size
	EmbeddingDimensions int
	// EmbeddingCacheSize is max embeddings to cache (0 = disabled, default: 10000)
	// Each cached embedding uses ~4KB (1024 dims × 4 bytes)
	// 10000 cache = ~40MB memory, provides significant speedup for repeated queries
	EmbeddingCacheSize int
	// ModelsDir is the directory containing local GGUF models
	// Env: NORNICDB_MODELS_DIR (default: ./models)
	ModelsDir string
	// EmbeddingGPULayers controls GPU offloading for local embeddings
	// -1 = auto, 0 = CPU only, >0 = specific layers
	// Env: NORNICDB_EMBEDDING_GPU_LAYERS
	EmbeddingGPULayers int
	// EmbeddingWarmupInterval for periodic model warmup
	// Env: NORNICDB_EMBEDDING_WARMUP_INTERVAL
	EmbeddingWarmupInterval time.Duration
	// AutoLinksEnabled for automatic relationship detection
	AutoLinksEnabled bool
	// AutoLinksSimilarityThreshold for similarity-based links
	AutoLinksSimilarityThreshold float64
	// KmeansMinEmbeddings is minimum embeddings required for k-means clustering
	// Env: NORNICDB_KMEANS_MIN_EMBEDDINGS (default: 1000)
	KmeansMinEmbeddings int

	// === Runtime Memory Management (Go runtime tuning) ===

	// RuntimeLimit is the soft memory limit (GOMEMLIMIT) in bytes
	// 0 = unlimited (Go manages automatically)
	// Set to 80% of container memory for optimal performance
	RuntimeLimit int64
	// RuntimeLimitStr is the human-readable form (e.g., "2GB", "512MB")
	RuntimeLimitStr string
	// GCPercent controls GC aggressiveness (GOGC)
	// 100 = default, lower = more aggressive (less memory, more CPU)
	GCPercent int
	// PoolEnabled controls object pooling for query results
	PoolEnabled bool
	// PoolMaxSize limits pool memory usage per pool
	PoolMaxSize int
	// QueryCacheEnabled controls query plan caching
	QueryCacheEnabled bool
	// QueryCacheSize is the maximum number of cached query plans
	QueryCacheSize int
	// QueryCacheTTL is how long cached plans remain valid
	QueryCacheTTL time.Duration
}

// ComplianceConfig holds settings for GDPR/HIPAA/FISMA/SOC2 compliance.
// These are framework-agnostic controls that satisfy multiple regulations.
type ComplianceConfig struct {
	// AuditLogging - Required by: GDPR Art.30, HIPAA §164.312(b), FISMA, SOC2
	AuditEnabled       bool
	AuditLogPath       string
	AuditRetentionDays int // How long to keep audit logs (HIPAA: 6 years, SOC2: 7 years)

	// Data Retention - Required by: GDPR Art.5(1)(e), HIPAA §164.530(j)
	RetentionEnabled     bool
	RetentionPolicyDays  int      // Default retention period (0 = indefinite)
	RetentionAutoDelete  bool     // Auto-delete vs archive after retention
	RetentionExemptRoles []string // Roles exempt from retention (e.g., "admin")

	// Access Control - Required by: GDPR Art.32, HIPAA §164.312(a), FISMA
	AccessControlEnabled bool
	SessionTimeout       time.Duration
	MaxFailedLogins      int
	LockoutDuration      time.Duration

	// Encryption - Required by: GDPR Art.32, HIPAA §164.312(a)(2)(iv)
	EncryptionAtRest    bool
	EncryptionInTransit bool
	EncryptionKeyPath   string

	// Data Subject Rights - Required by: GDPR Art.15-20
	DataExportEnabled  bool // Right to data portability
	DataErasureEnabled bool // Right to erasure/be forgotten
	DataAccessEnabled  bool // Right of access

	// Anonymization - Required by: GDPR Recital 26
	AnonymizationEnabled bool
	AnonymizationMethod  string // "pseudonymization", "generalization", "suppression"

	// Consent - Required by: GDPR Art.7
	ConsentRequired   bool
	ConsentVersioning bool
	ConsentAuditTrail bool

	// Breach Notification - Required by: GDPR Art.33-34, HIPAA §164.408
	BreachDetectionEnabled bool
	BreachNotifyEmail      string
	BreachNotifyWebhook    string
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	// Level (DEBUG, INFO, WARN, ERROR)
	Level string
	// Format (json, text)
	Format string
	// Output path (stdout, stderr, or file path)
	Output string
	// QueryLogEnabled for query logging
	QueryLogEnabled bool
	// SlowQueryThreshold for logging slow queries
	SlowQueryThreshold time.Duration
}

// FeatureFlagsConfig holds all feature flags for experimental/optional features.
// Centralized location for all feature toggles in NornicDB.
type FeatureFlagsConfig struct {
	// Kalman filtering for predictive smoothing
	KalmanEnabled bool

	// Topological link prediction AUTOMATIC integration
	// NOTE: Neo4j GDS procedures (CALL gds.linkPrediction.*) are ALWAYS available
	// This flag only controls automatic integration with inference.Engine.OnStore()
	TopologyAutoIntegrationEnabled bool    // Enable automatic topology in OnStore()
	TopologyAlgorithm              string  // adamic_adar, jaccard, etc.
	TopologyWeight                 float64 // 0.0-1.0, weight vs semantic
	TopologyTopK                   int
	TopologyMinScore               float64
	TopologyGraphRefreshInterval   int

	// A/B testing for automatic topology integration
	TopologyABTestEnabled    bool
	TopologyABTestPercentage int // 0-100

	// Heimdall - the cognitive guardian of NornicDB
	// When enabled, NornicDB loads a local SLM for anomaly detection,
	// runtime diagnosis, and memory curation.
	// Environment: NORNICDB_HEIMDALL_ENABLED (default: false)
	HeimdallEnabled bool

	// Heimdall model name (without .gguf extension)
	// Environment: NORNICDB_HEIMDALL_MODEL (default: qwen2.5-1.5b-instruct-q4_k_m)
	HeimdallModel string

	// GPU layers for Heimdall SLM (-1=auto, 0=CPU only)
	// Falls back to CPU if GPU memory insufficient
	// Environment: NORNICDB_HEIMDALL_GPU_LAYERS (default: -1)
	HeimdallGPULayers int

	// Context size for Heimdall model (max tokens in context window)
	// This controls GPU memory usage for KV cache. Lower = less memory.
	// Default: 8192 (8K) - memory efficient, saves ~2GB GPU RAM vs 32K
	// For longer conversations, increase to 16384 or 32768
	// Environment: NORNICDB_HEIMDALL_CONTEXT_SIZE (default: 8192)
	HeimdallContextSize int

	// Batch size for Heimdall model (tokens processed at once)
	// Should be <= ContextSize. Higher values may improve throughput.
	// Default: 2048 (2K) - balanced for typical prompt sizes
	// Environment: NORNICDB_HEIMDALL_BATCH_SIZE (default: 2048)
	HeimdallBatchSize int

	// Max tokens for Heimdall generation
	// Environment: NORNICDB_HEIMDALL_MAX_TOKENS (default: 512)
	HeimdallMaxTokens int

	// Temperature for Heimdall (lower = more deterministic)
	// Environment: NORNICDB_HEIMDALL_TEMPERATURE (default: 0.1)
	HeimdallTemperature float32

	// Enable Heimdall anomaly detection on graph
	// Environment: NORNICDB_HEIMDALL_ANOMALY_DETECTION (default: true when Heimdall enabled)
	HeimdallAnomalyDetection bool

	// Enable Heimdall runtime diagnosis
	// Environment: NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS (default: true when Heimdall enabled)
	HeimdallRuntimeDiagnosis bool

	// Enable Heimdall memory curation (experimental)
	// Environment: NORNICDB_HEIMDALL_MEMORY_CURATION (default: false)
	HeimdallMemoryCuration bool

	// === Token Budget Settings for Heimdall Prompt Construction ===
	// These control how the context window is partitioned between system prompt,
	// user message, and generation output. Tune these if you see truncation or
	// want to allow longer prompts/responses.
	//
	// Memory impact: Larger context = more GPU memory for KV cache
	// - 8K context ≈ 200-400MB GPU RAM
	// - 16K context ≈ 400-800MB GPU RAM
	// - 32K context ≈ 800-1600MB GPU RAM

	// Max context tokens for prompt validation (should match HeimdallContextSize)
	// This is the total token budget for system + user + output combined.
	// Environment: NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS (default: 8192)
	HeimdallMaxContextTokens int

	// Max tokens reserved for system prompt (actions + instructions + Cypher primer)
	// The system prompt includes: action definitions, Cypher reference, and plugin context.
	// Remaining context is split between user message and model output.
	// Environment: NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS (default: 6000)
	HeimdallMaxSystemTokens int

	// Max tokens reserved for user message input
	// Longer user messages (complex queries, multi-line inputs) need more budget.
	// Environment: NORNICDB_HEIMDALL_MAX_USER_TOKENS (default: 2000)
	HeimdallMaxUserTokens int
}

// Heimdall config getter methods for heimdall.FeatureFlagsSource interface
func (f *FeatureFlagsConfig) GetHeimdallEnabled() bool          { return f.HeimdallEnabled }
func (f *FeatureFlagsConfig) GetHeimdallModel() string          { return f.HeimdallModel }
func (f *FeatureFlagsConfig) GetHeimdallGPULayers() int         { return f.HeimdallGPULayers }
func (f *FeatureFlagsConfig) GetHeimdallContextSize() int       { return f.HeimdallContextSize }
func (f *FeatureFlagsConfig) GetHeimdallBatchSize() int         { return f.HeimdallBatchSize }
func (f *FeatureFlagsConfig) GetHeimdallMaxTokens() int         { return f.HeimdallMaxTokens }
func (f *FeatureFlagsConfig) GetHeimdallTemperature() float32   { return f.HeimdallTemperature }
func (f *FeatureFlagsConfig) GetHeimdallAnomalyDetection() bool { return f.HeimdallAnomalyDetection }
func (f *FeatureFlagsConfig) GetHeimdallRuntimeDiagnosis() bool { return f.HeimdallRuntimeDiagnosis }
func (f *FeatureFlagsConfig) GetHeimdallMemoryCuration() bool   { return f.HeimdallMemoryCuration }
func (f *FeatureFlagsConfig) GetHeimdallMaxContextTokens() int  { return f.HeimdallMaxContextTokens }
func (f *FeatureFlagsConfig) GetHeimdallMaxSystemTokens() int   { return f.HeimdallMaxSystemTokens }
func (f *FeatureFlagsConfig) GetHeimdallMaxUserTokens() int     { return f.HeimdallMaxUserTokens }

// LoadFromEnv loads configuration from environment variables.
//
// This function reads all configuration from the environment, using Neo4j-compatible
// variable names where applicable (e.g., NORNICDB_AUTH, NEO4J_dbms_*) and NornicDB-specific
// variables prefixed with NORNICDB_.
//
// All values have sensible defaults, so LoadFromEnv() can be called without any
// environment variables set.
//
// Example:
//
//	// Minimal setup - uses all defaults
//	config := config.LoadFromEnv()
//
//	// With custom environment
//	os.Setenv("NORNICDB_AUTH", "myuser/mypass")
//	os.Setenv("NORNICDB_BOLT_PORT", "7688")
//	os.Setenv("NORNICDB_EMBEDDING_PROVIDER", "openai")
//	os.Setenv("NORNICDB_EMBEDDING_API_KEY", "sk-...")
//	config = config.LoadFromEnv()
//
//	if err := config.Validate(); err != nil {
//		log.Fatal(err)
//	}
//
// Returns a fully populated Config with defaults applied where environment
// variables are not set.
//
// Example 1 - Basic Development Setup:
//
//	// No environment variables set - use defaults
//	config := config.LoadFromEnv()
//
//	// Auth disabled by default (NORNICDB_AUTH=none)
//	fmt.Printf("Auth enabled: %v\n", config.Auth.Enabled) // false
//
//	// Bolt server on default port
//	fmt.Printf("Bolt: %s:%d\n",
//		config.Server.BoltAddress, config.Server.BoltPort) // 0.0.0.0:7687
//
//	// Memory decay enabled by default
//	fmt.Printf("Decay enabled: %v\n", config.Memory.DecayEnabled) // true
//
// Example 2 - Production with Authentication:
//
//	// Set environment variables
//	os.Setenv("NORNICDB_AUTH", "admin/SecurePassword123!")
//	os.Setenv("NORNICDB_BOLT_PORT", "7687")
//	os.Setenv("NORNICDB_AUTH_JWT_SECRET", "your-32-char-secret-key-here!!")
//	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")
//
//	config := config.LoadFromEnv()
//
//	// Validate before use
//	if err := config.Validate(); err != nil {
//		log.Fatal("Invalid config:", err)
//	}
//
//	// Auth now enabled
//	fmt.Printf("Admin: %s\n", config.Auth.InitialUsername) // admin
//	fmt.Printf("Audit: %s\n", config.Compliance.AuditLogPath)
//
// Example 3 - Docker Compose Setup:
//
//	# docker-compose.yml
//	services:
//	  nornicdb:
//	    image: nornicdb:latest
//	    environment:
//	      - NORNICDB_AUTH=neo4j/password
//	      - NORNICDB_DATA_DIR=/data
//	      - NORNICDB_MEMORY_DECAY_ENABLED=true
//	      - NORNICDB_EMBEDDING_PROVIDER=ollama
//	      - NORNICDB_EMBEDDING_API_URL=http://ollama:11434
//	      - NORNICDB_AUDIT_ENABLED=true
//	      - NORNICDB_AUDIT_LOG_PATH=/logs/audit.log
//	    volumes:
//	      - nornicdb-data:/data
//	      - nornicdb-logs:/logs
//	    ports:
//	      - "7687:7687"
//	      - "7474:7474"
//
//	// In application code
//	config := config.LoadFromEnv()
//	// All environment variables automatically loaded
//
// Example 4 - HIPAA Compliance Configuration:
//
//	// Set HIPAA-required environment variables
//	os.Setenv("NORNICDB_AUTH", "admin/ComplexPassword123!")
//	os.Setenv("NORNICDB_AUTH_TOKEN_EXPIRY", "4h")
//	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")
//	os.Setenv("NORNICDB_AUDIT_RETENTION_DAYS", "2555") // 7 years
//	os.Setenv("NORNICDB_ENCRYPTION_AT_REST", "true")
//	os.Setenv("NORNICDB_ENCRYPTION_IN_TRANSIT", "true")
//	os.Setenv("NORNICDB_MAX_FAILED_LOGINS", "3")
//	os.Setenv("NORNICDB_LOCKOUT_DURATION", "30m")
//	os.Setenv("NORNICDB_SESSION_TIMEOUT", "15m")
//
//	config := config.LoadFromEnv()
//
//	// Verify HIPAA requirements met
//	if !config.Compliance.AuditEnabled {
//		log.Fatal("HIPAA requires audit logging")
//	}
//	if config.Compliance.AuditRetentionDays < 2555 {
//		log.Fatal("HIPAA requires 7-year audit retention")
//	}
//
// Example 5 - Multi-Environment Setup:
//
//	// Load from .env file first
//	err := godotenv.Load(".env." + os.Getenv("ENV"))
//	if err != nil {
//		log.Printf("No .env file: %v", err)
//	}
//
//	// Then load from environment
//	config := config.LoadFromEnv()
//
//	// Override for specific environment
//	switch os.Getenv("ENV") {
//	case "production":
//		if !config.Auth.Enabled {
//			log.Fatal("Production requires authentication!")
//		}
//	case "development":
//		config.Logging.Level = "DEBUG"
//	case "test":
//		config.Database.DataDir = os.TempDir()
//	}
//
// ELI12:
//
// Think of LoadFromEnv like reading a recipe from sticky notes on your fridge:
//
//   - Each sticky note is an environment variable (e.g., "PORT=7687")
//   - If there's no sticky note, use the default ("PORT not found? Use 7687")
//   - The function reads ALL the sticky notes and builds a complete recipe
//
// Why use environment variables?
//  1. Security: Keep secrets out of code (passwords, API keys)
//  2. Flexibility: Change settings without recompiling
//  3. Docker-friendly: Easy to configure containers
//  4. 12-Factor App: Industry best practice
//
// Neo4j Compatibility:
//   - NORNICDB_AUTH format: "username/password" or "none"
//   - NEO4J_dbms_* settings match Neo4j exactly
//   - Tools like Neo4j Desktop work out of the box
//
// Common Environment Variables:
//
//	Authentication:
//	- NORNICDB_AUTH="neo4j/password" (enable auth)
//	- NORNICDB_AUTH="none" (disable auth, dev only)
//	- NORNICDB_AUTH_JWT_SECRET="..." (32+ chars)
//
//	Network:
//	- NORNICDB_BOLT_PORT=7687
//	- NORNICDB_HTTP_PORT=7474
//
//	Storage:
//	- NORNICDB_DATA_DIR="./data"
//	- NORNICDB_DEFAULT_DATABASE="nornicdb"
//
//	Memory (NornicDB-specific):
//	- NORNICDB_MEMORY_DECAY_ENABLED=true
//	- NORNICDB_EMBEDDING_PROVIDER=ollama
//	- NORNICDB_EMBEDDING_MODEL=bge-m3
//
//	Compliance:
//	- NORNICDB_AUDIT_ENABLED=true
//	- NORNICDB_AUDIT_RETENTION_DAYS=2555
//	- NORNICDB_ENCRYPTION_AT_REST=true
//
// Configuration Priority:
//  1. Environment variables (highest)
//  2. Default values (if env var not set)
//  3. No config files (environment-only by design)
//
// Validation:
//
//	Always call config.Validate() after LoadFromEnv() to catch errors:
//	- Missing required fields
//	- Invalid values (negative numbers, bad formats)
//	- Conflicting settings
//
// Performance:
//   - O(n) where n = number of environment variables
//   - Typically <1ms to load full configuration
//   - Config is loaded once at startup
//
// Thread Safety:
//
//	LoadFromEnv reads environment variables which are process-global and
//	should not be modified after startup. The returned Config is immutable.
//
// This function is kept for backward compatibility but LoadFromFile is preferred
// as it properly implements the precedence: defaults -> config file -> env vars.
func LoadFromEnv() *Config {
	// Start with defaults, then apply env vars
	config := LoadDefaults()
	applyEnvVars(config)
	return config
}

// legacyLoadFromEnv is the original implementation kept for reference.
// Use LoadFromEnv() or LoadFromFile() instead.
func legacyLoadFromEnv() *Config {
	config := &Config{}

	// Authentication - supports both "username:password" and "username/password" formats
	// Default: disabled for easy development
	authStr := getEnv("NORNICDB_AUTH", "none")
	if authStr == "none" {
		config.Auth.Enabled = false
		config.Auth.InitialUsername = "admin"
		config.Auth.InitialPassword = "password"
	} else {
		config.Auth.Enabled = true
		// Try colon delimiter first (preferred format: admin:admin)
		// Fall back to slash for Neo4j compatibility (admin/password)
		parts := strings.SplitN(authStr, ":", 2)
		if len(parts) != 2 {
			// Try slash as fallback for Neo4j compatibility
			parts = strings.SplitN(authStr, "/", 2)
		}
		if len(parts) == 2 {
			config.Auth.InitialUsername = parts[0]
			config.Auth.InitialPassword = parts[1]
		} else {
			config.Auth.InitialUsername = "admin"
			config.Auth.InitialPassword = authStr
		}
	}
	config.Auth.MinPasswordLength = getEnvInt("NORNICDB_MIN_PASSWORD_LENGTH", 8)
	config.Auth.TokenExpiry = getEnvDuration("NORNICDB_AUTH_TOKEN_EXPIRY", 24*time.Hour)
	config.Auth.JWTSecret = getEnv("NORNICDB_AUTH_JWT_SECRET", generateDefaultSecret())

	// Database settings
	config.Database.DataDir = getEnv("NORNICDB_DATA_DIR", "./data")
	config.Database.DefaultDatabase = getEnv("NORNICDB_DEFAULT_DATABASE", "nornicdb")
	config.Database.ReadOnly = getEnvBool("NORNICDB_READ_ONLY", false)
	config.Database.TransactionTimeout = getEnvDuration("NORNICDB_TRANSACTION_TIMEOUT", 30*time.Second)
	config.Database.MaxConcurrentTransactions = getEnvInt("NORNICDB_MAX_TRANSACTIONS", 1000)

	// Durability settings - defaults optimized for performance
	// Set NORNICDB_STRICT_DURABILITY=true for maximum safety (financial/critical data)
	config.Database.StrictDurability = getEnvBool("NORNICDB_STRICT_DURABILITY", false)
	if config.Database.StrictDurability {
		// Strict mode: override to maximum safety
		config.Database.WALSyncMode = "immediate"
		config.Database.WALSyncInterval = 0 // Not used in immediate mode
	} else {
		// Default mode: good balance of performance and safety
		config.Database.WALSyncMode = getEnv("NORNICDB_WAL_SYNC_MODE", "batch")
		config.Database.WALSyncInterval = getEnvDuration("NORNICDB_WAL_SYNC_INTERVAL", 100*time.Millisecond)
	}

	// Server settings - Bolt
	config.Server.BoltEnabled = getEnvBool("NORNICDB_BOLT_ENABLED", true)
	config.Server.BoltPort = getEnvInt("NORNICDB_BOLT_PORT", 7687)
	config.Server.BoltAddress = getEnv("NORNICDB_BOLT_ADDRESS", "0.0.0.0")
	config.Server.BoltTLSEnabled = getEnvBool("NORNICDB_BOLT_TLS_ENABLED", false)
	config.Server.BoltTLSCert = getEnv("NORNICDB_TLS_DIR", "") + "/public.crt"
	config.Server.BoltTLSKey = getEnv("NORNICDB_TLS_DIR", "") + "/private.key"

	// Server settings - HTTP
	config.Server.HTTPEnabled = getEnvBool("NORNICDB_HTTP_ENABLED", true)
	config.Server.HTTPPort = getEnvInt("NORNICDB_HTTP_PORT", 7474)
	config.Server.HTTPAddress = getEnv("NORNICDB_HTTP_ADDRESS", "0.0.0.0")
	config.Server.HTTPSEnabled = getEnvBool("NORNICDB_HTTPS_ENABLED", false)
	config.Server.HTTPSPort = getEnvInt("NORNICDB_HTTPS_PORT", 7473)
	config.Server.HTTPTLSCert = getEnv("NORNICDB_TLS_DIR", "") + "/public.crt"
	config.Server.HTTPTLSKey = getEnv("NORNICDB_TLS_DIR", "") + "/private.key"

	// Memory settings (NornicDB-specific, prefixed with NORNICDB_)
	config.Memory.DecayEnabled = getEnvBool("NORNICDB_MEMORY_DECAY_ENABLED", true)
	config.Memory.DecayInterval = getEnvDuration("NORNICDB_MEMORY_DECAY_INTERVAL", time.Hour)
	config.Memory.ArchiveThreshold = getEnvFloat("NORNICDB_MEMORY_ARCHIVE_THRESHOLD", 0.05)
	// Only override EmbeddingEnabled if env var is explicitly set
	if v := os.Getenv("NORNICDB_EMBEDDING_ENABLED"); v != "" {
		config.Memory.EmbeddingEnabled = v == "true" || v == "1"
	}
	if v := getEnv("NORNICDB_EMBEDDING_PROVIDER", ""); v != "" {
		config.Memory.EmbeddingProvider = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_MODEL", ""); v != "" {
		config.Memory.EmbeddingModel = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_API_URL", ""); v != "" {
		config.Memory.EmbeddingAPIURL = v
	}
	if v := os.Getenv("NORNICDB_EMBEDDING_DIMENSIONS"); v != "" {
		config.Memory.EmbeddingDimensions = getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024)
	}
	if v := os.Getenv("NORNICDB_EMBEDDING_CACHE_SIZE"); v != "" {
		config.Memory.EmbeddingCacheSize = getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 10000)
	}
	config.Memory.AutoLinksEnabled = getEnvBool("NORNICDB_AUTO_LINKS_ENABLED", true)
	config.Memory.AutoLinksSimilarityThreshold = getEnvFloat("NORNICDB_AUTO_LINKS_THRESHOLD", 0.82)

	// Runtime memory management settings
	config.Memory.RuntimeLimitStr = getEnv("NORNICDB_MEMORY_LIMIT", "0")
	config.Memory.RuntimeLimit = parseMemorySize(config.Memory.RuntimeLimitStr)
	config.Memory.GCPercent = getEnvInt("NORNICDB_GC_PERCENT", 100)
	config.Memory.PoolEnabled = getEnvBool("NORNICDB_POOL_ENABLED", true)
	config.Memory.PoolMaxSize = getEnvInt("NORNICDB_POOL_MAX_SIZE", 1000)
	config.Memory.QueryCacheEnabled = getEnvBool("NORNICDB_QUERY_CACHE_ENABLED", true)
	config.Memory.QueryCacheSize = getEnvInt("NORNICDB_QUERY_CACHE_SIZE", 1000)
	config.Memory.QueryCacheTTL = getEnvDuration("NORNICDB_QUERY_CACHE_TTL", 5*time.Minute)

	// Embedding worker settings (NornicDB-specific)
	config.EmbeddingWorker.ScanInterval = getEnvDuration("NORNICDB_EMBED_SCAN_INTERVAL", 15*time.Minute)
	config.EmbeddingWorker.BatchDelay = getEnvDuration("NORNICDB_EMBED_BATCH_DELAY", 500*time.Millisecond)
	config.EmbeddingWorker.MaxRetries = getEnvInt("NORNICDB_EMBED_MAX_RETRIES", 3)
	config.EmbeddingWorker.ChunkSize = getEnvInt("NORNICDB_EMBED_CHUNK_SIZE", 512)
	config.EmbeddingWorker.ChunkOverlap = getEnvInt("NORNICDB_EMBED_CHUNK_OVERLAP", 50)

	// Compliance settings (NornicDB-specific, framework-agnostic)
	// Audit Logging
	config.Compliance.AuditEnabled = getEnvBool("NORNICDB_AUDIT_ENABLED", true)
	config.Compliance.AuditLogPath = getEnv("NORNICDB_AUDIT_LOG_PATH", "./logs/audit.log")
	config.Compliance.AuditRetentionDays = getEnvInt("NORNICDB_AUDIT_RETENTION_DAYS", 2555) // ~7 years for SOC2

	// Data Retention
	config.Compliance.RetentionEnabled = getEnvBool("NORNICDB_RETENTION_ENABLED", false)
	config.Compliance.RetentionPolicyDays = getEnvInt("NORNICDB_RETENTION_POLICY_DAYS", 0)
	config.Compliance.RetentionAutoDelete = getEnvBool("NORNICDB_RETENTION_AUTO_DELETE", false)
	config.Compliance.RetentionExemptRoles = getEnvStringSlice("NORNICDB_RETENTION_EXEMPT_ROLES", []string{"admin"})

	// Access Control
	config.Compliance.AccessControlEnabled = getEnvBool("NORNICDB_ACCESS_CONTROL_ENABLED", true)
	config.Compliance.SessionTimeout = getEnvDuration("NORNICDB_SESSION_TIMEOUT", 30*time.Minute)
	config.Compliance.MaxFailedLogins = getEnvInt("NORNICDB_MAX_FAILED_LOGINS", 5)
	config.Compliance.LockoutDuration = getEnvDuration("NORNICDB_LOCKOUT_DURATION", 15*time.Minute)

	// Encryption
	config.Compliance.EncryptionAtRest = getEnvBool("NORNICDB_ENCRYPTION_AT_REST", false)
	config.Compliance.EncryptionInTransit = getEnvBool("NORNICDB_ENCRYPTION_IN_TRANSIT", true)
	config.Compliance.EncryptionKeyPath = getEnv("NORNICDB_ENCRYPTION_KEY_PATH", "")

	// Data Subject Rights
	config.Compliance.DataExportEnabled = getEnvBool("NORNICDB_DATA_EXPORT_ENABLED", true)
	config.Compliance.DataErasureEnabled = getEnvBool("NORNICDB_DATA_ERASURE_ENABLED", true)
	config.Compliance.DataAccessEnabled = getEnvBool("NORNICDB_DATA_ACCESS_ENABLED", true)

	// Anonymization
	config.Compliance.AnonymizationEnabled = getEnvBool("NORNICDB_ANONYMIZATION_ENABLED", true)
	config.Compliance.AnonymizationMethod = getEnv("NORNICDB_ANONYMIZATION_METHOD", "pseudonymization")

	// Consent
	config.Compliance.ConsentRequired = getEnvBool("NORNICDB_CONSENT_REQUIRED", false)
	config.Compliance.ConsentVersioning = getEnvBool("NORNICDB_CONSENT_VERSIONING", true)
	config.Compliance.ConsentAuditTrail = getEnvBool("NORNICDB_CONSENT_AUDIT_TRAIL", true)

	// Breach Notification
	config.Compliance.BreachDetectionEnabled = getEnvBool("NORNICDB_BREACH_DETECTION_ENABLED", false)
	config.Compliance.BreachNotifyEmail = getEnv("NORNICDB_BREACH_NOTIFY_EMAIL", "")
	config.Compliance.BreachNotifyWebhook = getEnv("NORNICDB_BREACH_NOTIFY_WEBHOOK", "")

	// Logging settings
	config.Logging.Level = getEnv("NORNICDB_LOG_LEVEL", "INFO")
	config.Logging.Format = getEnv("NORNICDB_LOG_FORMAT", "json")
	config.Logging.Output = getEnv("NORNICDB_LOG_OUTPUT", "stdout")
	config.Logging.QueryLogEnabled = getEnvBool("NORNICDB_QUERY_LOG_ENABLED", false)
	config.Logging.SlowQueryThreshold = getEnvDuration("NORNICDB_SLOW_QUERY_THRESHOLD", 5*time.Second)

	// Feature flags
	config.Features.KalmanEnabled = getEnvBool("NORNICDB_KALMAN_ENABLED", false)
	// Topology procedures are always available; this controls automatic integration only
	config.Features.TopologyAutoIntegrationEnabled = getEnvBool("NORNICDB_TOPOLOGY_AUTO_INTEGRATION_ENABLED", false)
	config.Features.TopologyAlgorithm = getEnv("NORNICDB_TOPOLOGY_ALGORITHM", "adamic_adar")
	config.Features.TopologyWeight = getEnvFloat("NORNICDB_TOPOLOGY_WEIGHT", 0.4)
	config.Features.TopologyTopK = getEnvInt("NORNICDB_TOPOLOGY_TOPK", 10)
	config.Features.TopologyMinScore = getEnvFloat("NORNICDB_TOPOLOGY_MIN_SCORE", 0.3)
	config.Features.TopologyGraphRefreshInterval = getEnvInt("NORNICDB_TOPOLOGY_GRAPH_REFRESH_INTERVAL", 100)
	config.Features.TopologyABTestEnabled = getEnvBool("NORNICDB_TOPOLOGY_AB_TEST_ENABLED", false)
	config.Features.TopologyABTestPercentage = getEnvInt("NORNICDB_TOPOLOGY_AB_TEST_PERCENTAGE", 50)

	// Heimdall - the cognitive guardian feature flags
	// Opt-in cognitive database features - only override if env var is explicitly set
	if v := os.Getenv("NORNICDB_HEIMDALL_ENABLED"); v != "" {
		config.Features.HeimdallEnabled = v == "true" || v == "1"
	}
	if v := getEnv("NORNICDB_HEIMDALL_MODEL", ""); v != "" {
		config.Features.HeimdallModel = v
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_GPU_LAYERS"); v != "" {
		config.Features.HeimdallGPULayers = getEnvInt("NORNICDB_HEIMDALL_GPU_LAYERS", -1)
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_CONTEXT_SIZE"); v != "" {
		config.Features.HeimdallContextSize = getEnvInt("NORNICDB_HEIMDALL_CONTEXT_SIZE", 8192)
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_BATCH_SIZE"); v != "" {
		config.Features.HeimdallBatchSize = getEnvInt("NORNICDB_HEIMDALL_BATCH_SIZE", 2048)
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_MAX_TOKENS"); v != "" {
		config.Features.HeimdallMaxTokens = getEnvInt("NORNICDB_HEIMDALL_MAX_TOKENS", 1024)
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_TEMPERATURE"); v != "" {
		config.Features.HeimdallTemperature = float32(getEnvFloat("NORNICDB_HEIMDALL_TEMPERATURE", 0.1))
	}
	// Sub-features default to true when Heimdall is enabled
	if v := os.Getenv("NORNICDB_HEIMDALL_ANOMALY_DETECTION"); v != "" {
		config.Features.HeimdallAnomalyDetection = v == "true" || v == "1"
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS"); v != "" {
		config.Features.HeimdallRuntimeDiagnosis = v == "true" || v == "1"
	}
	if v := os.Getenv("NORNICDB_HEIMDALL_MEMORY_CURATION"); v != "" {
		config.Features.HeimdallMemoryCuration = v == "true" || v == "1"
	}

	// Token budget settings for prompt construction
	config.Features.HeimdallMaxContextTokens = getEnvInt("NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS", 8192) // Match context size
	config.Features.HeimdallMaxSystemTokens = getEnvInt("NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS", 6000)   // System prompt budget
	config.Features.HeimdallMaxUserTokens = getEnvInt("NORNICDB_HEIMDALL_MAX_USER_TOKENS", 2000)       // User message budget

	return config
}

// Validate checks the configuration for logical errors and invalid values.
//
// This method checks:
//   - Authentication is properly configured if enabled
//   - Password meets minimum length requirements
//   - Port numbers are valid (> 0)
//   - Embedding dimensions are positive
//
// Call Validate() after LoadFromEnv() and before using the Config.
//
// Example:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatalf("Configuration error: %v", err)
//	}
//	// Config is valid, proceed with startup
//
// Returns nil if configuration is valid, or an error describing the problem.
func (c *Config) Validate() error {
	if c.Auth.Enabled {
		if c.Auth.InitialUsername == "" {
			return fmt.Errorf("authentication enabled but no username provided")
		}
		if len(c.Auth.InitialPassword) < c.Auth.MinPasswordLength {
			return fmt.Errorf("password must be at least %d characters", c.Auth.MinPasswordLength)
		}
	}

	if c.Server.BoltEnabled && c.Server.BoltPort <= 0 {
		return fmt.Errorf("invalid bolt port: %d", c.Server.BoltPort)
	}

	if c.Server.HTTPEnabled && c.Server.HTTPPort <= 0 {
		return fmt.Errorf("invalid http port: %d", c.Server.HTTPPort)
	}

	if c.Memory.EmbeddingDimensions <= 0 {
		return fmt.Errorf("invalid embedding dimensions: %d", c.Memory.EmbeddingDimensions)
	}

	return nil
}

// String returns a safe string representation of the Config.
//
// Sensitive values like passwords and API keys are NOT included in the output,
// making this safe for logging.
//
// Example:
//
//	config := config.LoadFromEnv()
//	log.Printf("Starting with config: %s", config)
//	// Output: Config{Auth: true, Bolt: 0.0.0.0:7687, HTTP: 0.0.0.0:7474, DataDir: ./data}
//
// Returns a string suitable for logging and debugging.
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{Auth: %v, Bolt: %s:%d, HTTP: %s:%d, DataDir: %s}",
		c.Auth.Enabled,
		c.Server.BoltAddress, c.Server.BoltPort,
		c.Server.HTTPAddress, c.Server.HTTPPort,
		c.Database.DataDir,
	)
}

// YAMLConfig represents the YAML configuration file structure.
// All fields mirror the environment variable configuration options.
type YAMLConfig struct {
	// Server configuration
	Server struct {
		BoltPort    int    `yaml:"bolt_port"`
		HTTPPort    int    `yaml:"http_port"`
		Port        int    `yaml:"port"`         // Alias for bolt_port
		Host        string `yaml:"host"`         // Bind address
		Address     string `yaml:"address"`      // Alias for host
		DataDir     string `yaml:"data_dir"`     // Data directory
		Auth        string `yaml:"auth"`         // Format: "username:password" or "none"
		BoltEnabled bool   `yaml:"bolt_enabled"` // Enable Bolt protocol
		HTTPEnabled bool   `yaml:"http_enabled"` // Enable HTTP API
		TLS         struct {
			Enabled  bool   `yaml:"enabled"`
			CertFile string `yaml:"cert_file"`
			KeyFile  string `yaml:"key_file"`
		} `yaml:"tls"`
	} `yaml:"server"`

	// Database/Storage configuration
	Database struct {
		DataDir                   string `yaml:"data_dir"`
		DefaultDatabase           string `yaml:"default_database"`
		ReadOnly                  bool   `yaml:"read_only"`
		TransactionTimeout        string `yaml:"transaction_timeout"`
		MaxConcurrentTransactions int    `yaml:"max_concurrent_transactions"`
		StrictDurability          bool   `yaml:"strict_durability"`
		WALSyncMode               string `yaml:"wal_sync_mode"`
		WALSyncInterval           string `yaml:"wal_sync_interval"`
		EncryptionEnabled         bool   `yaml:"encryption_enabled"`
		EncryptionPassword        string `yaml:"encryption_password"`
	} `yaml:"database"`

	// Storage alias for database
	Storage struct {
		Path string `yaml:"path"`
	} `yaml:"storage"`

	// Authentication configuration
	Auth struct {
		Enabled           bool   `yaml:"enabled"`
		Username          string `yaml:"username"`
		Password          string `yaml:"password"`
		MinPasswordLength int    `yaml:"min_password_length"`
		TokenExpiry       string `yaml:"token_expiry"`
		JWTSecret         string `yaml:"jwt_secret"`
	} `yaml:"auth"`

	// Embedding configuration
	Embedding struct {
		Enabled    bool   `yaml:"enabled"`
		Provider   string `yaml:"provider"`
		Model      string `yaml:"model"`
		URL        string `yaml:"url"`
		APIKey     string `yaml:"api_key"`
		Dimensions int    `yaml:"dimensions"`
		CacheSize  int    `yaml:"cache_size"`
	} `yaml:"embedding"`

	// Memory/Decay configuration
	Memory struct {
		DecayEnabled                 bool    `yaml:"decay_enabled"`
		DecayInterval                string  `yaml:"decay_interval"`
		ArchiveThreshold             float64 `yaml:"archive_threshold"`
		AutoLinksEnabled             bool    `yaml:"auto_links_enabled"`
		AutoLinksSimilarityThreshold float64 `yaml:"auto_links_similarity_threshold"`
		RuntimeLimit                 string  `yaml:"runtime_limit"`
		GCPercent                    int     `yaml:"gc_percent"`
		PoolEnabled                  bool    `yaml:"pool_enabled"`
		PoolMaxSize                  int     `yaml:"pool_max_size"`
		QueryCacheEnabled            bool    `yaml:"query_cache_enabled"`
		QueryCacheSize               int     `yaml:"query_cache_size"`
		QueryCacheTTL                string  `yaml:"query_cache_ttl"`
	} `yaml:"memory"`

	// Embedding worker configuration
	EmbeddingWorker struct {
		ScanInterval string `yaml:"scan_interval"`
		BatchDelay   string `yaml:"batch_delay"`
		MaxRetries   int    `yaml:"max_retries"`
		ChunkSize    int    `yaml:"chunk_size"`
		ChunkOverlap int    `yaml:"chunk_overlap"`
	} `yaml:"embedding_worker"`

	// K-means clustering
	Kmeans struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"kmeans"`

	// Auto-TLP (Topology Link Prediction)
	AutoTLP struct {
		Enabled              bool    `yaml:"enabled"`
		Algorithm            string  `yaml:"algorithm"`
		Weight               float64 `yaml:"weight"`
		TopK                 int     `yaml:"top_k"`
		MinScore             float64 `yaml:"min_score"`
		GraphRefreshInterval int     `yaml:"graph_refresh_interval"`
		ABTestEnabled        bool    `yaml:"ab_test_enabled"`
		ABTestPercentage     int     `yaml:"ab_test_percentage"`
	} `yaml:"auto_tlp"`

	// Heimdall AI guardian
	Heimdall struct {
		Enabled          bool    `yaml:"enabled"`
		Model            string  `yaml:"model"`
		GPULayers        int     `yaml:"gpu_layers"`
		ContextSize      int     `yaml:"context_size"`
		BatchSize        int     `yaml:"batch_size"`
		MaxTokens        int     `yaml:"max_tokens"`
		Temperature      float64 `yaml:"temperature"`
		AnomalyDetection bool    `yaml:"anomaly_detection"`
		RuntimeDiagnosis bool    `yaml:"runtime_diagnosis"`
		MemoryCuration   bool    `yaml:"memory_curation"`
		MaxContextTokens int     `yaml:"max_context_tokens"`
		MaxSystemTokens  int     `yaml:"max_system_tokens"`
		MaxUserTokens    int     `yaml:"max_user_tokens"`
	} `yaml:"heimdall"`

	// Compliance settings
	Compliance struct {
		// Audit
		AuditEnabled       bool   `yaml:"audit_enabled"`
		AuditLogPath       string `yaml:"audit_log_path"`
		AuditRetentionDays int    `yaml:"audit_retention_days"`
		// Retention
		RetentionEnabled     bool     `yaml:"retention_enabled"`
		RetentionPolicyDays  int      `yaml:"retention_policy_days"`
		RetentionAutoDelete  bool     `yaml:"retention_auto_delete"`
		RetentionExemptRoles []string `yaml:"retention_exempt_roles"`
		// Access Control
		AccessControlEnabled bool   `yaml:"access_control_enabled"`
		SessionTimeout       string `yaml:"session_timeout"`
		MaxFailedLogins      int    `yaml:"max_failed_logins"`
		LockoutDuration      string `yaml:"lockout_duration"`
		// Encryption
		EncryptionAtRest    bool   `yaml:"encryption_at_rest"`
		EncryptionInTransit bool   `yaml:"encryption_in_transit"`
		EncryptionKeyPath   string `yaml:"encryption_key_path"`
		// Data Subject Rights
		DataExportEnabled  bool `yaml:"data_export_enabled"`
		DataErasureEnabled bool `yaml:"data_erasure_enabled"`
		DataAccessEnabled  bool `yaml:"data_access_enabled"`
		// Anonymization
		AnonymizationEnabled bool   `yaml:"anonymization_enabled"`
		AnonymizationMethod  string `yaml:"anonymization_method"`
		// Consent
		ConsentRequired   bool `yaml:"consent_required"`
		ConsentVersioning bool `yaml:"consent_versioning"`
		ConsentAuditTrail bool `yaml:"consent_audit_trail"`
		// Breach
		BreachDetectionEnabled bool   `yaml:"breach_detection_enabled"`
		BreachNotifyEmail      string `yaml:"breach_notify_email"`
		BreachNotifyWebhook    string `yaml:"breach_notify_webhook"`
	} `yaml:"compliance"`

	// Logging configuration
	Logging struct {
		Level              string `yaml:"level"`
		Format             string `yaml:"format"`
		Output             string `yaml:"output"`
		QueryLogEnabled    bool   `yaml:"query_log_enabled"`
		SlowQueryThreshold string `yaml:"slow_query_threshold"`
	} `yaml:"logging"`

	// Plugins configuration
	Plugins struct {
		Dir         string `yaml:"dir"`          // APOC plugins directory
		HeimdallDir string `yaml:"heimdall_dir"` // Heimdall plugins directory
	} `yaml:"plugins"`
}

// LoadDefaults returns a Config with all built-in safe defaults.
// This is the base configuration before any overrides are applied.
//
// Precedence (lowest to highest):
//  1. Built-in defaults (this function)
//  2. Config file (YAML)
//  3. Environment variables
//  4. Command-line arguments (applied in main.go)
func LoadDefaults() *Config {
	config := &Config{}

	// Auth defaults
	config.Auth.Enabled = false
	config.Auth.InitialUsername = "admin"
	config.Auth.InitialPassword = "password"
	config.Auth.MinPasswordLength = 8
	config.Auth.TokenExpiry = 24 * time.Hour
	config.Auth.JWTSecret = generateDefaultSecret()

	// Database defaults
	config.Database.DataDir = "./data"
	config.Database.DefaultDatabase = "nornicdb"
	config.Database.ReadOnly = false
	config.Database.TransactionTimeout = 30 * time.Second
	config.Database.MaxConcurrentTransactions = 1000
	config.Database.StrictDurability = false
	config.Database.WALSyncMode = "batch"
	config.Database.WALSyncInterval = 100 * time.Millisecond
	config.Database.EncryptionPassword = "" // disabled by default

	// Server defaults - Bolt
	config.Server.BoltEnabled = true
	config.Server.BoltPort = 7687
	config.Server.BoltAddress = "0.0.0.0"
	config.Server.BoltTLSEnabled = false

	// Server defaults - HTTP
	config.Server.HTTPEnabled = true
	config.Server.HTTPPort = 7474
	config.Server.HTTPAddress = "0.0.0.0"
	config.Server.HTTPSEnabled = false
	config.Server.HTTPSPort = 7473

	// Server defaults - Environment
	config.Server.Environment = "development"
	config.Server.AllowHTTP = true
	config.Server.PluginsDir = "./plugins"
	config.Server.HeimdallPluginsDir = "./plugins/heimdall"

	// Memory defaults
	config.Memory.DecayEnabled = true
	config.Memory.DecayInterval = time.Hour
	config.Memory.ArchiveThreshold = 0.05
	config.Memory.EmbeddingEnabled = false    // Disabled by default - opt-in feature
	config.Memory.EmbeddingProvider = "local" // Use local GGUF models by default
	config.Memory.EmbeddingModel = "bge-m3"
	config.Memory.EmbeddingAPIURL = "http://localhost:11434"
	config.Memory.EmbeddingDimensions = 1024
	config.Memory.EmbeddingCacheSize = 10000
	config.Memory.ModelsDir = "./models"
	config.Memory.EmbeddingGPULayers = -1     // auto
	config.Memory.EmbeddingWarmupInterval = 0 // disabled
	config.Memory.AutoLinksEnabled = true
	config.Memory.AutoLinksSimilarityThreshold = 0.82
	config.Memory.KmeansMinEmbeddings = 100
	config.Memory.RuntimeLimitStr = "0"
	config.Memory.RuntimeLimit = 0
	config.Memory.GCPercent = 100
	config.Memory.PoolEnabled = true
	config.Memory.PoolMaxSize = 1000
	config.Memory.QueryCacheEnabled = true
	config.Memory.QueryCacheSize = 1000
	config.Memory.QueryCacheTTL = 5 * time.Minute

	// Embedding worker defaults
	config.EmbeddingWorker.ScanInterval = 15 * time.Minute
	config.EmbeddingWorker.BatchDelay = 500 * time.Millisecond
	config.EmbeddingWorker.MaxRetries = 3
	config.EmbeddingWorker.ChunkSize = 512
	config.EmbeddingWorker.ChunkOverlap = 50

	// Compliance defaults
	config.Compliance.AuditEnabled = true
	config.Compliance.AuditLogPath = "./logs/audit.log"
	config.Compliance.AuditRetentionDays = 2555
	config.Compliance.RetentionEnabled = false
	config.Compliance.RetentionPolicyDays = 0
	config.Compliance.RetentionAutoDelete = false
	config.Compliance.RetentionExemptRoles = []string{"admin"}
	config.Compliance.AccessControlEnabled = true
	config.Compliance.SessionTimeout = 30 * time.Minute
	config.Compliance.MaxFailedLogins = 5
	config.Compliance.LockoutDuration = 15 * time.Minute
	config.Compliance.EncryptionAtRest = false
	config.Compliance.EncryptionInTransit = true
	config.Compliance.DataExportEnabled = true
	config.Compliance.DataErasureEnabled = true
	config.Compliance.DataAccessEnabled = true
	config.Compliance.AnonymizationEnabled = true
	config.Compliance.AnonymizationMethod = "pseudonymization"
	config.Compliance.ConsentRequired = false
	config.Compliance.ConsentVersioning = true
	config.Compliance.ConsentAuditTrail = true
	config.Compliance.BreachDetectionEnabled = false

	// Logging defaults
	config.Logging.Level = "INFO"
	config.Logging.Format = "json"
	config.Logging.Output = "stdout"
	config.Logging.QueryLogEnabled = false
	config.Logging.SlowQueryThreshold = 5 * time.Second

	// Feature flags defaults
	config.Features.KalmanEnabled = false
	config.Features.TopologyAutoIntegrationEnabled = false
	config.Features.TopologyAlgorithm = "adamic_adar"
	config.Features.TopologyWeight = 0.4
	config.Features.TopologyTopK = 10
	config.Features.TopologyMinScore = 0.3
	config.Features.TopologyGraphRefreshInterval = 100
	config.Features.TopologyABTestEnabled = false
	config.Features.TopologyABTestPercentage = 50
	config.Features.HeimdallEnabled = false
	config.Features.HeimdallModel = "qwen2.5-0.5b-instruct"
	config.Features.HeimdallGPULayers = -1
	config.Features.HeimdallContextSize = 8192
	config.Features.HeimdallBatchSize = 2048
	config.Features.HeimdallMaxTokens = 1024
	config.Features.HeimdallTemperature = 0.1
	config.Features.HeimdallAnomalyDetection = false
	config.Features.HeimdallRuntimeDiagnosis = false
	config.Features.HeimdallMemoryCuration = false
	config.Features.HeimdallMaxContextTokens = 8192
	config.Features.HeimdallMaxSystemTokens = 6000
	config.Features.HeimdallMaxUserTokens = 2000

	return config
}

// applyEnvVars applies environment variable overrides to an existing config.
// Environment variables take precedence over config file values.
func applyEnvVars(config *Config) {
	// Authentication - supports both "username:password" and "username/password" formats
	authStr := getEnv("NORNICDB_AUTH", "")
	// Backward compatibility: allow Neo4j env var
	if authStr == "" {
		authStr = getEnv("NEO4J_AUTH", "")
	}
	if authStr != "" {
		if authStr == "none" {
			config.Auth.Enabled = false
		} else {
			config.Auth.Enabled = true
			// Try colon delimiter first (preferred format: admin:admin)
			parts := strings.SplitN(authStr, ":", 2)
			if len(parts) != 2 {
				// Fall back to slash for Neo4j compatibility
				parts = strings.SplitN(authStr, "/", 2)
			}
			if len(parts) == 2 {
				config.Auth.InitialUsername = parts[0]
				config.Auth.InitialPassword = parts[1]
			} else {
				config.Auth.InitialUsername = "admin"
				config.Auth.InitialPassword = authStr
			}
		}
	}
	if v := getEnvInt("NORNICDB_MIN_PASSWORD_LENGTH", 0); v > 0 {
		config.Auth.MinPasswordLength = v
	}
	if v := getEnvDuration("NORNICDB_AUTH_TOKEN_EXPIRY", 0); v > 0 {
		config.Auth.TokenExpiry = v
	}
	if v := getEnv("NORNICDB_AUTH_JWT_SECRET", ""); v != "" {
		config.Auth.JWTSecret = v
	}

	// Database settings
	if v := getEnv("NORNICDB_DATA_DIR", ""); v != "" {
		config.Database.DataDir = v
	} else if v := getEnv("NEO4J_dbms_directories_data", ""); v != "" {
		config.Database.DataDir = v
	}
	if v := getEnv("NORNICDB_DEFAULT_DATABASE", ""); v != "" {
		config.Database.DefaultDatabase = v
	} else if v := getEnv("NEO4J_dbms_default__database", ""); v != "" {
		config.Database.DefaultDatabase = v
	}
	// Flexible boolean parsing for read-only (supports legacy Neo4j env)
	if getEnvBool("NORNICDB_READ_ONLY", false) || getEnvBool("NEO4J_dbms_read__only", false) {
		config.Database.ReadOnly = true
	}
	if v := getEnvDuration("NORNICDB_TRANSACTION_TIMEOUT", 0); v > 0 {
		config.Database.TransactionTimeout = v
	} else {
		// Backward compatibility: Neo4j env name
		if v := getEnvDuration("NEO4J_dbms_transaction_timeout", 0); v > 0 {
			config.Database.TransactionTimeout = v
		}
	}
	if v := getEnvInt("NORNICDB_MAX_TRANSACTIONS", 0); v > 0 {
		config.Database.MaxConcurrentTransactions = v
	}
	if getEnv("NORNICDB_STRICT_DURABILITY", "") == "true" {
		config.Database.StrictDurability = true
		config.Database.WALSyncMode = "immediate"
	}
	if v := getEnv("NORNICDB_WAL_SYNC_MODE", ""); v != "" {
		config.Database.WALSyncMode = v
	}
	if v := getEnvDuration("NORNICDB_WAL_SYNC_INTERVAL", 0); v > 0 {
		config.Database.WALSyncInterval = v
	}

	// If strict durability, enforce immediate WAL sync and interval 0 regardless of overrides
	if config.Database.StrictDurability {
		config.Database.WALSyncMode = "immediate"
		config.Database.WALSyncInterval = 0
	}

	// Server settings - Bolt
	if getEnv("NORNICDB_BOLT_ENABLED", "") == "false" {
		config.Server.BoltEnabled = false
	}
	if v := getEnvInt("NORNICDB_BOLT_PORT", 0); v > 0 {
		config.Server.BoltPort = v
	} else if v := getEnvInt("NEO4J_dbms_connector_bolt_listen__address_port", 0); v > 0 {
		config.Server.BoltPort = v
	}
	if v := getEnv("NORNICDB_BOLT_ADDRESS", ""); v != "" {
		config.Server.BoltAddress = v
	}
	if getEnv("NORNICDB_BOLT_TLS_ENABLED", "") == "true" {
		config.Server.BoltTLSEnabled = true
	}
	if v := getEnv("NORNICDB_TLS_DIR", ""); v != "" {
		config.Server.BoltTLSCert = v + "/public.crt"
		config.Server.BoltTLSKey = v + "/private.key"
	}

	// Server settings - HTTP
	if getEnv("NORNICDB_HTTP_ENABLED", "") == "false" {
		config.Server.HTTPEnabled = false
	}
	if v := getEnvInt("NORNICDB_HTTP_PORT", 0); v > 0 {
		config.Server.HTTPPort = v
	} else if v := getEnvInt("NEO4J_dbms_connector_http_listen__address_port", 0); v > 0 {
		config.Server.HTTPPort = v
	}
	if v := getEnv("NORNICDB_HTTP_ADDRESS", ""); v != "" {
		config.Server.HTTPAddress = v
	}
	if getEnv("NORNICDB_HTTPS_ENABLED", "") == "true" {
		config.Server.HTTPSEnabled = true
	}
	if v := getEnvInt("NORNICDB_HTTPS_PORT", 0); v > 0 {
		config.Server.HTTPSPort = v
	}

	// Server environment settings
	if v := getEnv("NORNICDB_ENV", ""); v != "" {
		config.Server.Environment = v
	}
	if getEnv("NORNICDB_ALLOW_HTTP", "") == "true" {
		config.Server.AllowHTTP = true
	} else if getEnv("NORNICDB_ALLOW_HTTP", "") == "false" {
		config.Server.AllowHTTP = false
	}
	if v := getEnv("NORNICDB_PLUGINS_DIR", ""); v != "" {
		config.Server.PluginsDir = v
	}
	if v := getEnv("NORNICDB_HEIMDALL_PLUGINS_DIR", ""); v != "" {
		config.Server.HeimdallPluginsDir = v
	}

	// Database encryption
	if getEnvBool("NORNICDB_ENCRYPTION_ENABLED", false) {
		config.Database.EncryptionEnabled = true
	}
	if v := getEnv("NORNICDB_ENCRYPTION_PASSWORD", ""); v != "" {
		config.Database.EncryptionPassword = v
		// Auto-enable encryption if password is provided via env var
		if config.Database.EncryptionPassword != "" {
			config.Database.EncryptionEnabled = true
		}
	}

	// Memory settings
	if getEnv("NORNICDB_MEMORY_DECAY_ENABLED", "") == "false" {
		config.Memory.DecayEnabled = false
	}
	if v := getEnvDuration("NORNICDB_MEMORY_DECAY_INTERVAL", 0); v > 0 {
		config.Memory.DecayInterval = v
	}
	if v := getEnvFloat("NORNICDB_MEMORY_ARCHIVE_THRESHOLD", 0); v > 0 {
		config.Memory.ArchiveThreshold = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_PROVIDER", ""); v != "" {
		config.Memory.EmbeddingProvider = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_MODEL", ""); v != "" {
		config.Memory.EmbeddingModel = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_API_URL", ""); v != "" {
		config.Memory.EmbeddingAPIURL = v
	}
	if v := getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 0); v > 0 {
		config.Memory.EmbeddingDimensions = v
	}
	if v := getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 0); v > 0 {
		config.Memory.EmbeddingCacheSize = v
	}
	if v := getEnv("NORNICDB_MODELS_DIR", ""); v != "" {
		config.Memory.ModelsDir = v
	}
	if v := getEnvInt("NORNICDB_EMBEDDING_GPU_LAYERS", -999); v != -999 {
		config.Memory.EmbeddingGPULayers = v
	}
	if v := getEnvDuration("NORNICDB_EMBEDDING_WARMUP_INTERVAL", 0); v > 0 {
		config.Memory.EmbeddingWarmupInterval = v
	}
	if v := getEnvInt("NORNICDB_KMEANS_MIN_EMBEDDINGS", 0); v > 0 {
		config.Memory.KmeansMinEmbeddings = v
	}
	if getEnv("NORNICDB_AUTO_LINKS_ENABLED", "") == "false" {
		config.Memory.AutoLinksEnabled = false
	}
	if v := getEnvFloat("NORNICDB_AUTO_LINKS_THRESHOLD", 0); v > 0 {
		config.Memory.AutoLinksSimilarityThreshold = v
	}
	if v := getEnv("NORNICDB_MEMORY_LIMIT", ""); v != "" {
		config.Memory.RuntimeLimitStr = v
		config.Memory.RuntimeLimit = parseMemorySize(v)
	}
	if v := getEnvInt("NORNICDB_GC_PERCENT", 0); v != 0 {
		config.Memory.GCPercent = v
	}
	if getEnv("NORNICDB_POOL_ENABLED", "") == "false" {
		config.Memory.PoolEnabled = false
	}
	if v := getEnvInt("NORNICDB_POOL_MAX_SIZE", 0); v > 0 {
		config.Memory.PoolMaxSize = v
	}
	if getEnv("NORNICDB_QUERY_CACHE_ENABLED", "") == "false" {
		config.Memory.QueryCacheEnabled = false
	}
	if v := getEnvInt("NORNICDB_QUERY_CACHE_SIZE", 0); v > 0 {
		config.Memory.QueryCacheSize = v
	}
	if v := getEnvDuration("NORNICDB_QUERY_CACHE_TTL", 0); v > 0 {
		config.Memory.QueryCacheTTL = v
	}

	// Embedding worker settings
	if v := getEnvDuration("NORNICDB_EMBED_SCAN_INTERVAL", 0); v > 0 {
		config.EmbeddingWorker.ScanInterval = v
	}
	if v := getEnvDuration("NORNICDB_EMBED_BATCH_DELAY", 0); v > 0 {
		config.EmbeddingWorker.BatchDelay = v
	}
	if v := getEnvInt("NORNICDB_EMBED_MAX_RETRIES", 0); v > 0 {
		config.EmbeddingWorker.MaxRetries = v
	}
	if v := getEnvInt("NORNICDB_EMBED_CHUNK_SIZE", 0); v > 0 {
		config.EmbeddingWorker.ChunkSize = v
	}
	if v := getEnvInt("NORNICDB_EMBED_CHUNK_OVERLAP", 0); v > 0 {
		config.EmbeddingWorker.ChunkOverlap = v
	}

	// Compliance settings
	if getEnv("NORNICDB_AUDIT_ENABLED", "") == "false" {
		config.Compliance.AuditEnabled = false
	}
	if v := getEnv("NORNICDB_AUDIT_LOG_PATH", ""); v != "" {
		config.Compliance.AuditLogPath = v
	}
	if v := getEnvInt("NORNICDB_AUDIT_RETENTION_DAYS", 0); v > 0 {
		config.Compliance.AuditRetentionDays = v
	}
	if getEnv("NORNICDB_RETENTION_ENABLED", "") == "true" {
		config.Compliance.RetentionEnabled = true
	}
	if v := getEnvInt("NORNICDB_RETENTION_POLICY_DAYS", 0); v > 0 {
		config.Compliance.RetentionPolicyDays = v
	}
	if getEnv("NORNICDB_RETENTION_AUTO_DELETE", "") == "true" {
		config.Compliance.RetentionAutoDelete = true
	}
	if v := getEnvStringSlice("NORNICDB_RETENTION_EXEMPT_ROLES", nil); len(v) > 0 {
		config.Compliance.RetentionExemptRoles = v
	}
	if getEnv("NORNICDB_ACCESS_CONTROL_ENABLED", "") == "false" {
		config.Compliance.AccessControlEnabled = false
	}
	if v := getEnvDuration("NORNICDB_SESSION_TIMEOUT", 0); v > 0 {
		config.Compliance.SessionTimeout = v
	}
	if v := getEnvInt("NORNICDB_MAX_FAILED_LOGINS", 0); v > 0 {
		config.Compliance.MaxFailedLogins = v
	}
	if v := getEnvDuration("NORNICDB_LOCKOUT_DURATION", 0); v > 0 {
		config.Compliance.LockoutDuration = v
	}
	if getEnv("NORNICDB_ENCRYPTION_AT_REST", "") == "true" {
		config.Compliance.EncryptionAtRest = true
	}
	if getEnv("NORNICDB_ENCRYPTION_IN_TRANSIT", "") == "false" {
		config.Compliance.EncryptionInTransit = false
	}
	if v := getEnv("NORNICDB_ENCRYPTION_KEY_PATH", ""); v != "" {
		config.Compliance.EncryptionKeyPath = v
	}
	if getEnv("NORNICDB_DATA_EXPORT_ENABLED", "") == "false" {
		config.Compliance.DataExportEnabled = false
	}
	if getEnv("NORNICDB_DATA_ERASURE_ENABLED", "") == "false" {
		config.Compliance.DataErasureEnabled = false
	}
	if getEnv("NORNICDB_DATA_ACCESS_ENABLED", "") == "false" {
		config.Compliance.DataAccessEnabled = false
	}
	if getEnv("NORNICDB_ANONYMIZATION_ENABLED", "") == "false" {
		config.Compliance.AnonymizationEnabled = false
	}
	if v := getEnv("NORNICDB_ANONYMIZATION_METHOD", ""); v != "" {
		config.Compliance.AnonymizationMethod = v
	}
	if getEnv("NORNICDB_CONSENT_REQUIRED", "") == "true" {
		config.Compliance.ConsentRequired = true
	}
	if getEnv("NORNICDB_CONSENT_VERSIONING", "") == "false" {
		config.Compliance.ConsentVersioning = false
	}
	if getEnv("NORNICDB_CONSENT_AUDIT_TRAIL", "") == "false" {
		config.Compliance.ConsentAuditTrail = false
	}
	if getEnv("NORNICDB_BREACH_DETECTION_ENABLED", "") == "true" {
		config.Compliance.BreachDetectionEnabled = true
	}
	if v := getEnv("NORNICDB_BREACH_NOTIFY_EMAIL", ""); v != "" {
		config.Compliance.BreachNotifyEmail = v
	}
	if v := getEnv("NORNICDB_BREACH_NOTIFY_WEBHOOK", ""); v != "" {
		config.Compliance.BreachNotifyWebhook = v
	}

	// Logging settings
	if v := getEnv("NORNICDB_LOG_LEVEL", ""); v != "" {
		config.Logging.Level = v
	}
	if v := getEnv("NORNICDB_LOG_FORMAT", ""); v != "" {
		config.Logging.Format = v
	}
	if v := getEnv("NORNICDB_LOG_OUTPUT", ""); v != "" {
		config.Logging.Output = v
	}
	if getEnv("NORNICDB_QUERY_LOG_ENABLED", "") == "true" {
		config.Logging.QueryLogEnabled = true
	}
	if v := getEnvDuration("NORNICDB_SLOW_QUERY_THRESHOLD", 0); v > 0 {
		config.Logging.SlowQueryThreshold = v
	}

	// Feature flags
	if getEnv("NORNICDB_KALMAN_ENABLED", "") == "true" {
		config.Features.KalmanEnabled = true
	}
	if getEnv("NORNICDB_TOPOLOGY_AUTO_INTEGRATION_ENABLED", "") == "true" {
		config.Features.TopologyAutoIntegrationEnabled = true
	}
	if v := getEnv("NORNICDB_TOPOLOGY_ALGORITHM", ""); v != "" {
		config.Features.TopologyAlgorithm = v
	}
	if v := getEnvFloat("NORNICDB_TOPOLOGY_WEIGHT", 0); v > 0 {
		config.Features.TopologyWeight = v
	}
	if v := getEnvInt("NORNICDB_TOPOLOGY_TOPK", 0); v > 0 {
		config.Features.TopologyTopK = v
	}
	if v := getEnvFloat("NORNICDB_TOPOLOGY_MIN_SCORE", 0); v > 0 {
		config.Features.TopologyMinScore = v
	}
	if v := getEnvInt("NORNICDB_TOPOLOGY_GRAPH_REFRESH_INTERVAL", 0); v > 0 {
		config.Features.TopologyGraphRefreshInterval = v
	}
	if getEnv("NORNICDB_TOPOLOGY_AB_TEST_ENABLED", "") == "true" {
		config.Features.TopologyABTestEnabled = true
	}
	if v := getEnvInt("NORNICDB_TOPOLOGY_AB_TEST_PERCENTAGE", 0); v > 0 {
		config.Features.TopologyABTestPercentage = v
	}
	if getEnv("NORNICDB_HEIMDALL_ENABLED", "") == "true" {
		config.Features.HeimdallEnabled = true
	}
	if v := getEnv("NORNICDB_HEIMDALL_MODEL", ""); v != "" {
		config.Features.HeimdallModel = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_GPU_LAYERS", 0); v != 0 {
		config.Features.HeimdallGPULayers = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_CONTEXT_SIZE", 0); v > 0 {
		config.Features.HeimdallContextSize = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_BATCH_SIZE", 0); v > 0 {
		config.Features.HeimdallBatchSize = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_MAX_TOKENS", 0); v > 0 {
		config.Features.HeimdallMaxTokens = v
	}
	if v := getEnvFloat("NORNICDB_HEIMDALL_TEMPERATURE", 0); v > 0 {
		config.Features.HeimdallTemperature = float32(v)
	}
	if getEnv("NORNICDB_HEIMDALL_ANOMALY_DETECTION", "") == "true" {
		config.Features.HeimdallAnomalyDetection = true
	}
	if getEnv("NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS", "") == "true" {
		config.Features.HeimdallRuntimeDiagnosis = true
	}
	if getEnv("NORNICDB_HEIMDALL_MEMORY_CURATION", "") == "true" {
		config.Features.HeimdallMemoryCuration = true
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS", 0); v > 0 {
		config.Features.HeimdallMaxContextTokens = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS", 0); v > 0 {
		config.Features.HeimdallMaxSystemTokens = v
	}
	if v := getEnvInt("NORNICDB_HEIMDALL_MAX_USER_TOKENS", 0); v > 0 {
		config.Features.HeimdallMaxUserTokens = v
	}
}

// ApplyEnvVars applies environment variable overrides to an existing config.
// This is the exported version for use in main.go.
func ApplyEnvVars(config *Config) {
	applyEnvVars(config)
}

// LoadFromFile loads configuration with proper precedence:
//  1. Built-in defaults (lowest priority)
//  2. YAML config file
//  3. Environment variables (highest priority before CLI args)
//
// Command-line arguments are applied by the caller (main.go) after this.
//
// Example YAML:
//
//	server:
//	  port: 7687
//	  host: "localhost"
//	  auth: "admin:admin"  # Format: username:password or "none"
//	embedding:
//	  enabled: true
//	  provider: "local"
//
// The auth field supports both colon format (admin:admin) for consistency
// and slash format (admin/password) for Neo4j compatibility.
func LoadFromFile(configPath string) (*Config, error) {
	// Step 1: Start with built-in defaults
	config := LoadDefaults()

	// Try to load YAML config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		// If file doesn't exist, just return env config
		if os.IsNotExist(err) {
			return config, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var yamlCfg YAMLConfig
	if err := yaml.Unmarshal(data, &yamlCfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// === Server Settings ===
	if yamlCfg.Server.BoltPort > 0 {
		config.Server.BoltPort = yamlCfg.Server.BoltPort
	}
	if yamlCfg.Server.Port > 0 {
		config.Server.BoltPort = yamlCfg.Server.Port
	}
	if yamlCfg.Server.HTTPPort > 0 {
		config.Server.HTTPPort = yamlCfg.Server.HTTPPort
	}
	if yamlCfg.Server.Host != "" {
		config.Server.BoltAddress = yamlCfg.Server.Host
		config.Server.HTTPAddress = yamlCfg.Server.Host
	}
	if yamlCfg.Server.Address != "" {
		config.Server.BoltAddress = yamlCfg.Server.Address
		config.Server.HTTPAddress = yamlCfg.Server.Address
	}
	if yamlCfg.Server.DataDir != "" {
		config.Database.DataDir = yamlCfg.Server.DataDir
	}
	if yamlCfg.Server.BoltEnabled {
		config.Server.BoltEnabled = true
	}
	if yamlCfg.Server.HTTPEnabled {
		config.Server.HTTPEnabled = true
	}
	if yamlCfg.Server.TLS.Enabled {
		config.Server.BoltTLSEnabled = true
		config.Server.HTTPSEnabled = true
	}
	if yamlCfg.Server.TLS.CertFile != "" {
		config.Server.BoltTLSCert = yamlCfg.Server.TLS.CertFile
		config.Server.HTTPTLSCert = yamlCfg.Server.TLS.CertFile
	}
	if yamlCfg.Server.TLS.KeyFile != "" {
		config.Server.BoltTLSKey = yamlCfg.Server.TLS.KeyFile
		config.Server.HTTPTLSKey = yamlCfg.Server.TLS.KeyFile
	}

	// === Database Settings ===
	if yamlCfg.Storage.Path != "" {
		config.Database.DataDir = yamlCfg.Storage.Path
	}
	if yamlCfg.Database.DataDir != "" {
		config.Database.DataDir = yamlCfg.Database.DataDir
	}
	if yamlCfg.Database.DefaultDatabase != "" {
		config.Database.DefaultDatabase = yamlCfg.Database.DefaultDatabase
	}
	if yamlCfg.Database.ReadOnly {
		config.Database.ReadOnly = true
	}
	if yamlCfg.Database.TransactionTimeout != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.TransactionTimeout); err == nil {
			config.Database.TransactionTimeout = d
		}
	}
	if yamlCfg.Database.MaxConcurrentTransactions > 0 {
		config.Database.MaxConcurrentTransactions = yamlCfg.Database.MaxConcurrentTransactions
	}
	if yamlCfg.Database.StrictDurability {
		config.Database.StrictDurability = true
		config.Database.WALSyncMode = "immediate"
	}
	if yamlCfg.Database.WALSyncMode != "" {
		config.Database.WALSyncMode = yamlCfg.Database.WALSyncMode
	}
	if yamlCfg.Database.WALSyncInterval != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.WALSyncInterval); err == nil {
			config.Database.WALSyncInterval = d
		}
	}
	// Encryption settings
	if yamlCfg.Database.EncryptionEnabled {
		config.Database.EncryptionEnabled = true
	}
	if yamlCfg.Database.EncryptionPassword != "" {
		config.Database.EncryptionPassword = yamlCfg.Database.EncryptionPassword
	}

	// === Authentication ===
	// Parse auth from server.auth (format: "username:password" or "none")
	if yamlCfg.Server.Auth != "" {
		authStr := yamlCfg.Server.Auth
		if authStr == "none" {
			config.Auth.Enabled = false
			config.Auth.InitialUsername = "admin"
			config.Auth.InitialPassword = "password"
		} else {
			config.Auth.Enabled = true
			// Try colon delimiter first (preferred: admin:admin)
			parts := strings.SplitN(authStr, ":", 2)
			if len(parts) != 2 {
				// Fall back to slash for Neo4j compatibility
				parts = strings.SplitN(authStr, "/", 2)
			}
			if len(parts) == 2 {
				config.Auth.InitialUsername = parts[0]
				config.Auth.InitialPassword = parts[1]
			} else {
				config.Auth.InitialUsername = "admin"
				config.Auth.InitialPassword = authStr
			}
		}
	}
	// Also support dedicated auth section
	if yamlCfg.Auth.Enabled {
		config.Auth.Enabled = true
	}
	if yamlCfg.Auth.Username != "" {
		config.Auth.InitialUsername = yamlCfg.Auth.Username
	}
	if yamlCfg.Auth.Password != "" {
		config.Auth.InitialPassword = yamlCfg.Auth.Password
	}
	if yamlCfg.Auth.MinPasswordLength > 0 {
		config.Auth.MinPasswordLength = yamlCfg.Auth.MinPasswordLength
	}
	if yamlCfg.Auth.TokenExpiry != "" {
		if d, err := time.ParseDuration(yamlCfg.Auth.TokenExpiry); err == nil {
			config.Auth.TokenExpiry = d
		}
	}
	if yamlCfg.Auth.JWTSecret != "" {
		config.Auth.JWTSecret = yamlCfg.Auth.JWTSecret
	}

	// === Embedding Settings ===
	if yamlCfg.Embedding.Enabled {
		config.Memory.EmbeddingEnabled = true
	}
	if yamlCfg.Embedding.Provider != "" {
		config.Memory.EmbeddingProvider = yamlCfg.Embedding.Provider
	}
	if yamlCfg.Embedding.Model != "" {
		config.Memory.EmbeddingModel = yamlCfg.Embedding.Model
	}
	if yamlCfg.Embedding.URL != "" {
		config.Memory.EmbeddingAPIURL = yamlCfg.Embedding.URL
	}
	if yamlCfg.Embedding.Dimensions > 0 {
		config.Memory.EmbeddingDimensions = yamlCfg.Embedding.Dimensions
	}
	if yamlCfg.Embedding.CacheSize > 0 {
		config.Memory.EmbeddingCacheSize = yamlCfg.Embedding.CacheSize
	}

	// === Memory Settings ===
	if yamlCfg.Memory.DecayEnabled {
		config.Memory.DecayEnabled = true
	}
	if yamlCfg.Memory.DecayInterval != "" {
		if d, err := time.ParseDuration(yamlCfg.Memory.DecayInterval); err == nil {
			config.Memory.DecayInterval = d
		}
	}
	if yamlCfg.Memory.ArchiveThreshold > 0 {
		config.Memory.ArchiveThreshold = yamlCfg.Memory.ArchiveThreshold
	}
	if yamlCfg.Memory.AutoLinksEnabled {
		config.Memory.AutoLinksEnabled = true
	}
	if yamlCfg.Memory.AutoLinksSimilarityThreshold > 0 {
		config.Memory.AutoLinksSimilarityThreshold = yamlCfg.Memory.AutoLinksSimilarityThreshold
	}
	if yamlCfg.Memory.RuntimeLimit != "" {
		config.Memory.RuntimeLimitStr = yamlCfg.Memory.RuntimeLimit
		config.Memory.RuntimeLimit = parseMemorySize(yamlCfg.Memory.RuntimeLimit)
	}
	if yamlCfg.Memory.GCPercent > 0 {
		config.Memory.GCPercent = yamlCfg.Memory.GCPercent
	}
	if yamlCfg.Memory.PoolEnabled {
		config.Memory.PoolEnabled = true
	}
	if yamlCfg.Memory.PoolMaxSize > 0 {
		config.Memory.PoolMaxSize = yamlCfg.Memory.PoolMaxSize
	}
	if yamlCfg.Memory.QueryCacheEnabled {
		config.Memory.QueryCacheEnabled = true
	}
	if yamlCfg.Memory.QueryCacheSize > 0 {
		config.Memory.QueryCacheSize = yamlCfg.Memory.QueryCacheSize
	}
	if yamlCfg.Memory.QueryCacheTTL != "" {
		if d, err := time.ParseDuration(yamlCfg.Memory.QueryCacheTTL); err == nil {
			config.Memory.QueryCacheTTL = d
		}
	}

	// === Embedding Worker ===
	if yamlCfg.EmbeddingWorker.ScanInterval != "" {
		if d, err := time.ParseDuration(yamlCfg.EmbeddingWorker.ScanInterval); err == nil {
			config.EmbeddingWorker.ScanInterval = d
		}
	}
	if yamlCfg.EmbeddingWorker.BatchDelay != "" {
		if d, err := time.ParseDuration(yamlCfg.EmbeddingWorker.BatchDelay); err == nil {
			config.EmbeddingWorker.BatchDelay = d
		}
	}
	if yamlCfg.EmbeddingWorker.MaxRetries > 0 {
		config.EmbeddingWorker.MaxRetries = yamlCfg.EmbeddingWorker.MaxRetries
	}
	if yamlCfg.EmbeddingWorker.ChunkSize > 0 {
		config.EmbeddingWorker.ChunkSize = yamlCfg.EmbeddingWorker.ChunkSize
	}
	if yamlCfg.EmbeddingWorker.ChunkOverlap > 0 {
		config.EmbeddingWorker.ChunkOverlap = yamlCfg.EmbeddingWorker.ChunkOverlap
	}

	// === Feature Flags ===
	if yamlCfg.Kmeans.Enabled {
		config.Features.KalmanEnabled = true
	}

	// Auto-TLP settings
	if yamlCfg.AutoTLP.Enabled {
		config.Features.TopologyAutoIntegrationEnabled = true
	}
	if yamlCfg.AutoTLP.Algorithm != "" {
		config.Features.TopologyAlgorithm = yamlCfg.AutoTLP.Algorithm
	}
	if yamlCfg.AutoTLP.Weight > 0 {
		config.Features.TopologyWeight = yamlCfg.AutoTLP.Weight
	}
	if yamlCfg.AutoTLP.TopK > 0 {
		config.Features.TopologyTopK = yamlCfg.AutoTLP.TopK
	}
	if yamlCfg.AutoTLP.MinScore > 0 {
		config.Features.TopologyMinScore = yamlCfg.AutoTLP.MinScore
	}
	if yamlCfg.AutoTLP.GraphRefreshInterval > 0 {
		config.Features.TopologyGraphRefreshInterval = yamlCfg.AutoTLP.GraphRefreshInterval
	}
	if yamlCfg.AutoTLP.ABTestEnabled {
		config.Features.TopologyABTestEnabled = true
	}
	if yamlCfg.AutoTLP.ABTestPercentage > 0 {
		config.Features.TopologyABTestPercentage = yamlCfg.AutoTLP.ABTestPercentage
	}

	// Heimdall settings - explicitly set enabled/disabled from YAML
	config.Features.HeimdallEnabled = yamlCfg.Heimdall.Enabled
	if yamlCfg.Heimdall.Model != "" {
		config.Features.HeimdallModel = yamlCfg.Heimdall.Model
	}
	if yamlCfg.Heimdall.GPULayers != 0 {
		config.Features.HeimdallGPULayers = yamlCfg.Heimdall.GPULayers
	}
	if yamlCfg.Heimdall.ContextSize > 0 {
		config.Features.HeimdallContextSize = yamlCfg.Heimdall.ContextSize
	}
	if yamlCfg.Heimdall.BatchSize > 0 {
		config.Features.HeimdallBatchSize = yamlCfg.Heimdall.BatchSize
	}
	if yamlCfg.Heimdall.MaxTokens > 0 {
		config.Features.HeimdallMaxTokens = yamlCfg.Heimdall.MaxTokens
	}
	if yamlCfg.Heimdall.Temperature > 0 {
		config.Features.HeimdallTemperature = float32(yamlCfg.Heimdall.Temperature)
	}
	if yamlCfg.Heimdall.AnomalyDetection {
		config.Features.HeimdallAnomalyDetection = true
	}
	if yamlCfg.Heimdall.RuntimeDiagnosis {
		config.Features.HeimdallRuntimeDiagnosis = true
	}
	if yamlCfg.Heimdall.MemoryCuration {
		config.Features.HeimdallMemoryCuration = true
	}
	if yamlCfg.Heimdall.MaxContextTokens > 0 {
		config.Features.HeimdallMaxContextTokens = yamlCfg.Heimdall.MaxContextTokens
	}
	if yamlCfg.Heimdall.MaxSystemTokens > 0 {
		config.Features.HeimdallMaxSystemTokens = yamlCfg.Heimdall.MaxSystemTokens
	}
	if yamlCfg.Heimdall.MaxUserTokens > 0 {
		config.Features.HeimdallMaxUserTokens = yamlCfg.Heimdall.MaxUserTokens
	}

	// === Compliance Settings ===
	if yamlCfg.Compliance.AuditEnabled {
		config.Compliance.AuditEnabled = true
	}
	if yamlCfg.Compliance.AuditLogPath != "" {
		config.Compliance.AuditLogPath = yamlCfg.Compliance.AuditLogPath
	}
	if yamlCfg.Compliance.AuditRetentionDays > 0 {
		config.Compliance.AuditRetentionDays = yamlCfg.Compliance.AuditRetentionDays
	}
	if yamlCfg.Compliance.RetentionEnabled {
		config.Compliance.RetentionEnabled = true
	}
	if yamlCfg.Compliance.RetentionPolicyDays > 0 {
		config.Compliance.RetentionPolicyDays = yamlCfg.Compliance.RetentionPolicyDays
	}
	if yamlCfg.Compliance.RetentionAutoDelete {
		config.Compliance.RetentionAutoDelete = true
	}
	if len(yamlCfg.Compliance.RetentionExemptRoles) > 0 {
		config.Compliance.RetentionExemptRoles = yamlCfg.Compliance.RetentionExemptRoles
	}
	if yamlCfg.Compliance.AccessControlEnabled {
		config.Compliance.AccessControlEnabled = true
	}
	if yamlCfg.Compliance.SessionTimeout != "" {
		if d, err := time.ParseDuration(yamlCfg.Compliance.SessionTimeout); err == nil {
			config.Compliance.SessionTimeout = d
		}
	}
	if yamlCfg.Compliance.MaxFailedLogins > 0 {
		config.Compliance.MaxFailedLogins = yamlCfg.Compliance.MaxFailedLogins
	}
	if yamlCfg.Compliance.LockoutDuration != "" {
		if d, err := time.ParseDuration(yamlCfg.Compliance.LockoutDuration); err == nil {
			config.Compliance.LockoutDuration = d
		}
	}
	if yamlCfg.Compliance.EncryptionAtRest {
		config.Compliance.EncryptionAtRest = true
	}
	if yamlCfg.Compliance.EncryptionInTransit {
		config.Compliance.EncryptionInTransit = true
	}
	if yamlCfg.Compliance.EncryptionKeyPath != "" {
		config.Compliance.EncryptionKeyPath = yamlCfg.Compliance.EncryptionKeyPath
	}
	if yamlCfg.Compliance.DataExportEnabled {
		config.Compliance.DataExportEnabled = true
	}
	if yamlCfg.Compliance.DataErasureEnabled {
		config.Compliance.DataErasureEnabled = true
	}
	if yamlCfg.Compliance.DataAccessEnabled {
		config.Compliance.DataAccessEnabled = true
	}
	if yamlCfg.Compliance.AnonymizationEnabled {
		config.Compliance.AnonymizationEnabled = true
	}
	if yamlCfg.Compliance.AnonymizationMethod != "" {
		config.Compliance.AnonymizationMethod = yamlCfg.Compliance.AnonymizationMethod
	}
	if yamlCfg.Compliance.ConsentRequired {
		config.Compliance.ConsentRequired = true
	}
	if yamlCfg.Compliance.ConsentVersioning {
		config.Compliance.ConsentVersioning = true
	}
	if yamlCfg.Compliance.ConsentAuditTrail {
		config.Compliance.ConsentAuditTrail = true
	}
	if yamlCfg.Compliance.BreachDetectionEnabled {
		config.Compliance.BreachDetectionEnabled = true
	}
	if yamlCfg.Compliance.BreachNotifyEmail != "" {
		config.Compliance.BreachNotifyEmail = yamlCfg.Compliance.BreachNotifyEmail
	}
	if yamlCfg.Compliance.BreachNotifyWebhook != "" {
		config.Compliance.BreachNotifyWebhook = yamlCfg.Compliance.BreachNotifyWebhook
	}

	// === Logging Settings ===
	if yamlCfg.Logging.Level != "" {
		config.Logging.Level = yamlCfg.Logging.Level
	}
	if yamlCfg.Logging.Format != "" {
		config.Logging.Format = yamlCfg.Logging.Format
	}
	if yamlCfg.Logging.Output != "" {
		config.Logging.Output = yamlCfg.Logging.Output
	}
	if yamlCfg.Logging.QueryLogEnabled {
		config.Logging.QueryLogEnabled = true
	}
	if yamlCfg.Logging.SlowQueryThreshold != "" {
		if d, err := time.ParseDuration(yamlCfg.Logging.SlowQueryThreshold); err == nil {
			config.Logging.SlowQueryThreshold = d
		}
	}

	// === Plugins Settings ===
	if yamlCfg.Plugins.Dir != "" {
		config.Server.PluginsDir = yamlCfg.Plugins.Dir
	}
	if yamlCfg.Plugins.HeimdallDir != "" {
		config.Server.HeimdallPluginsDir = yamlCfg.Plugins.HeimdallDir
	}

	// Step 3: Apply environment variable overrides (higher priority than config file)
	applyEnvVars(config)

	return config, nil
}

// FindConfigFile searches for config file in standard locations.
// Returns the path to the first config file found, or empty string if none found.
// Search order:
//  1. ~/.nornicdb/config.yaml (user home directory - highest priority)
//  2. Same directory as the binary (config.yaml, nornicdb.yaml)
//  3. Current working directory (config.yaml, nornicdb.yaml)
//  4. ~/Library/Application Support/NornicDB/config.yaml (macOS)
//  5. ~/.config/nornicdb/config.yaml (Linux/Unix XDG standard)
func FindConfigFile() string {
	var candidates []string

	// Priority 1: User home directory ~/.nornicdb/config.yaml (highest priority)
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".nornicdb", "config.yaml"))
	}

	// Priority 2: Same directory as the binary
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "config.yaml"),
			filepath.Join(exeDir, "nornicdb.yaml"),
		)
	}

	// Priority 3: Current working directory
	candidates = append(candidates,
		"config.yaml",
		"nornicdb.yaml",
	)

	// Priority 4: OS-specific user config paths
	if home, err := os.UserHomeDir(); err == nil {
		// macOS
		candidates = append(candidates, filepath.Join(home, "Library", "Application Support", "NornicDB", "config.yaml"))
		// Linux/Unix XDG standard
		candidates = append(candidates, filepath.Join(home, ".config", "nornicdb", "config.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// Helper functions for environment variable parsing

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		val = strings.ToLower(val)
		return val == "true" || val == "1" || val == "yes" || val == "on"
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
		// Try parsing as seconds
		if secs, err := strconv.Atoi(val); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultVal
}

func getEnvStringSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		// Split by comma, trim whitespace
		parts := strings.Split(val, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultVal
}

func generateDefaultSecret() string {
	// In production, this should be explicitly set
	return "CHANGE_ME_IN_PRODUCTION_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// parseMemorySize parses a human-readable memory size string.
// Supports: "1024", "1KB", "1MB", "1GB", "1TB", "0", "unlimited"
func parseMemorySize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" || s == "UNLIMITED" {
		return 0
	}

	s = strings.TrimSuffix(s, "B")

	var multiplier int64 = 1
	switch {
	case strings.HasSuffix(s, "K"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "K")
	case strings.HasSuffix(s, "M"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "G"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	case strings.HasSuffix(s, "T"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "T")
	}

	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return val * multiplier
}

// FormatMemorySize formats bytes as human-readable string.
func FormatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// ApplyRuntimeMemory applies the runtime memory settings to the Go runtime.
// Should be called early in main() before heavy allocations.
func (c *MemoryConfig) ApplyRuntimeMemory() {
	if c.RuntimeLimit > 0 {
		debug.SetMemoryLimit(c.RuntimeLimit)
	}
	if c.GCPercent != 100 {
		debug.SetGCPercent(c.GCPercent)
	}
}
