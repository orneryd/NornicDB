// Package config handles Neo4j-compatible configuration via environment variables.
//
// NornicDB uses environment variables for configuration to maintain compatibility with
// Neo4j tooling and deployment workflows. All Neo4j environment variables are supported,
// plus NornicDB-specific extensions prefixed with NORNICDB_.
//
// Configuration is loaded from environment variables using LoadFromEnv() and can be
// validated with Validate() before use.
//
// Example Usage:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatalf("Invalid config: %v", err)
//	}
//
//	fmt.Printf("Bolt server: %s:%d\n",
//		config.Server.BoltAddress, config.Server.BoltPort)
//
// Environment Variables:
//
// Neo4j-Compatible:
//   - NEO4J_AUTH="username/password" or "none"
//   - NEO4J_dbms_connector_bolt_listen__address_port=7687
//   - NEO4J_dbms_connector_http_listen__address_port=7474
//   - NEO4J_dbms_directories_data="./data"
//
// NornicDB-Specific:
//   - NORNICDB_MEMORY_DECAY_ENABLED=true
//   - NORNICDB_MEMORY_DECAY_INTERVAL=1h
//   - NORNICDB_EMBEDDING_PROVIDER="ollama" or "openai"
//   - NORNICDB_EMBEDDING_MODEL=bge-m3
//   - NORNICDB_AUDIT_ENABLED=true
//
// For a complete list, see the Config struct field documentation.
package config

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
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
	// Authentication (NEO4J_AUTH format: "username/password" or "none")
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
	// EmbeddingProvider (ollama, openai)
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
	// AutoLinksEnabled for automatic relationship detection
	AutoLinksEnabled bool
	// AutoLinksSimilarityThreshold for similarity-based links
	AutoLinksSimilarityThreshold float64

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
// variable names where applicable (e.g., NEO4J_AUTH, NEO4J_dbms_*) and NornicDB-specific
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
//	os.Setenv("NEO4J_AUTH", "myuser/mypass")
//	os.Setenv("NEO4J_dbms_connector_bolt_listen__address_port", "7688")
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
//	// Auth disabled by default (NEO4J_AUTH=none)
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
//	os.Setenv("NEO4J_AUTH", "admin/SecurePassword123!")
//	os.Setenv("NEO4J_dbms_connector_bolt_listen__address_port", "7687")
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
//	      - NEO4J_AUTH=neo4j/password
//	      - NEO4J_dbms_directories_data=/data
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
//	os.Setenv("NEO4J_AUTH", "admin/ComplexPassword123!")
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
//   - NEO4J_AUTH format: "username/password" or "none"
//   - NEO4J_dbms_* settings match Neo4j exactly
//   - Tools like Neo4j Desktop work out of the box
//
// Common Environment Variables:
//
//	Authentication:
//	- NEO4J_AUTH="neo4j/password" (enable auth)
//	- NEO4J_AUTH="none" (disable auth, dev only)
//	- NORNICDB_AUTH_JWT_SECRET="..." (32+ chars)
//
//	Network:
//	- NEO4J_dbms_connector_bolt_listen__address_port=7687
//	- NEO4J_dbms_connector_http_listen__address_port=7474
//
//	Storage:
//	- NEO4J_dbms_directories_data="./data"
//	- NEO4J_dbms_default__database="nornicdb"
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
func LoadFromEnv() *Config {
	config := &Config{}

	// Authentication - NEO4J_AUTH format: "username/password" or "none"
	// Default: disabled for easy development
	authStr := getEnv("NEO4J_AUTH", "none")
	if authStr == "none" {
		config.Auth.Enabled = false
		config.Auth.InitialUsername = "admin"
		config.Auth.InitialPassword = "admin"
	} else {
		config.Auth.Enabled = true
		parts := strings.SplitN(authStr, "/", 2)
		if len(parts) == 2 {
			config.Auth.InitialUsername = parts[0]
			config.Auth.InitialPassword = parts[1]
		} else {
			config.Auth.InitialUsername = "admin"
			config.Auth.InitialPassword = authStr
		}
	}
	config.Auth.MinPasswordLength = getEnvInt("NEO4J_dbms_security_auth_minimum__password__length", 8)
	config.Auth.TokenExpiry = getEnvDuration("NORNICDB_AUTH_TOKEN_EXPIRY", 24*time.Hour)
	config.Auth.JWTSecret = getEnv("NORNICDB_AUTH_JWT_SECRET", generateDefaultSecret())

	// Database settings
	config.Database.DataDir = getEnv("NEO4J_dbms_directories_data", "./data")
	config.Database.DefaultDatabase = getEnv("NEO4J_dbms_default__database", "nornicdb")
	config.Database.ReadOnly = getEnvBool("NEO4J_dbms_read__only", false)
	config.Database.TransactionTimeout = getEnvDuration("NEO4J_dbms_transaction_timeout", 30*time.Second)
	config.Database.MaxConcurrentTransactions = getEnvInt("NEO4J_dbms_transaction_concurrent_maximum", 1000)

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
	config.Server.BoltEnabled = getEnvBool("NEO4J_dbms_connector_bolt_enabled", true)
	config.Server.BoltPort = getEnvInt("NEO4J_dbms_connector_bolt_listen__address_port", 7687)
	config.Server.BoltAddress = getEnv("NEO4J_dbms_connector_bolt_listen__address", "0.0.0.0")
	config.Server.BoltTLSEnabled = getEnvBool("NEO4J_dbms_connector_bolt_tls__level", false)
	config.Server.BoltTLSCert = getEnv("NEO4J_dbms_ssl_policy_bolt_base__directory", "") + "/public.crt"
	config.Server.BoltTLSKey = getEnv("NEO4J_dbms_ssl_policy_bolt_base__directory", "") + "/private.key"

	// Server settings - HTTP
	config.Server.HTTPEnabled = getEnvBool("NEO4J_dbms_connector_http_enabled", true)
	config.Server.HTTPPort = getEnvInt("NEO4J_dbms_connector_http_listen__address_port", 7474)
	config.Server.HTTPAddress = getEnv("NEO4J_dbms_connector_http_listen__address", "0.0.0.0")
	config.Server.HTTPSEnabled = getEnvBool("NEO4J_dbms_connector_https_enabled", false)
	config.Server.HTTPSPort = getEnvInt("NEO4J_dbms_connector_https_listen__address_port", 7473)
	config.Server.HTTPTLSCert = getEnv("NEO4J_dbms_ssl_policy_https_base__directory", "") + "/public.crt"
	config.Server.HTTPTLSKey = getEnv("NEO4J_dbms_ssl_policy_https_base__directory", "") + "/private.key"

	// Memory settings (NornicDB-specific, prefixed with NORNICDB_)
	config.Memory.DecayEnabled = getEnvBool("NORNICDB_MEMORY_DECAY_ENABLED", true)
	config.Memory.DecayInterval = getEnvDuration("NORNICDB_MEMORY_DECAY_INTERVAL", time.Hour)
	config.Memory.ArchiveThreshold = getEnvFloat("NORNICDB_MEMORY_ARCHIVE_THRESHOLD", 0.05)
	config.Memory.EmbeddingProvider = getEnv("NORNICDB_EMBEDDING_PROVIDER", "ollama")
	config.Memory.EmbeddingModel = getEnv("NORNICDB_EMBEDDING_MODEL", "bge-m3")
	config.Memory.EmbeddingAPIURL = getEnv("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434")
	config.Memory.EmbeddingDimensions = getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024)
	config.Memory.EmbeddingCacheSize = getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 10000) // Default: 10K (~40MB)
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
	config.Logging.Level = getEnv("NEO4J_dbms_logs_debug_level", "INFO")
	config.Logging.Format = getEnv("NORNICDB_LOG_FORMAT", "json")
	config.Logging.Output = getEnv("NORNICDB_LOG_OUTPUT", "stdout")
	config.Logging.QueryLogEnabled = getEnvBool("NEO4J_dbms_logs_query_enabled", false)
	config.Logging.SlowQueryThreshold = getEnvDuration("NEO4J_dbms_logs_query_threshold", 5*time.Second)

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
	// Opt-in cognitive database features - disabled by default
	config.Features.HeimdallEnabled = getEnvBool("NORNICDB_HEIMDALL_ENABLED", false)
	config.Features.HeimdallModel = getEnv("NORNICDB_HEIMDALL_MODEL", "qwen2.5-0.5b-instruct")
	config.Features.HeimdallGPULayers = getEnvInt("NORNICDB_HEIMDALL_GPU_LAYERS", -1)       // -1 = auto
	config.Features.HeimdallContextSize = getEnvInt("NORNICDB_HEIMDALL_CONTEXT_SIZE", 8192) // 8K (memory efficient)
	config.Features.HeimdallBatchSize = getEnvInt("NORNICDB_HEIMDALL_BATCH_SIZE", 2048)     // 2K batch size
	config.Features.HeimdallMaxTokens = getEnvInt("NORNICDB_HEIMDALL_MAX_TOKENS", 1024)     // 1K output (faster)
	config.Features.HeimdallTemperature = float32(getEnvFloat("NORNICDB_HEIMDALL_TEMPERATURE", 0.1))
	// Sub-features default to true when Heimdall is enabled
	config.Features.HeimdallAnomalyDetection = getEnvBool("NORNICDB_HEIMDALL_ANOMALY_DETECTION", config.Features.HeimdallEnabled)
	config.Features.HeimdallRuntimeDiagnosis = getEnvBool("NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS", config.Features.HeimdallEnabled)
	config.Features.HeimdallMemoryCuration = getEnvBool("NORNICDB_HEIMDALL_MEMORY_CURATION", false) // Experimental

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
