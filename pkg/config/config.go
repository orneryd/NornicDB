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

// Environment Variables (all use NORNICDB_ prefix):
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
// Vector Search (HNSW):
//   - NORNICDB_VECTOR_ANN_QUALITY="fast"|"balanced"|"accurate" (default: balanced)
//   - NORNICDB_VECTOR_HNSW_M: Max connections per node (default: based on quality preset)
//   - NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION: Construction candidate list size (default: based on quality preset)
//   - NORNICDB_VECTOR_HNSW_EF_SEARCH: Search candidate list size (default: based on quality preset)
//
// Logging:
//   - NORNICDB_LOG_LEVEL="INFO"
//   - NORNICDB_LOG_FORMAT="json"
//
// For a complete list, see the Config struct field documentation.
package config

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/observability"
	"gopkg.in/yaml.v3"
)

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
//
// StorageRuntimeConfig holds runtime-only storage settings that do not
// belong on DatabaseConfig (which mirrors persisted ENV/YAML knobs).
//
// Plan 04-04 introduces BytesMetricInterval here so cmd/nornicdb can
// override the D-07 30s default sweep cadence at startup without
// polluting the on-disk config schema.
type StorageRuntimeConfig struct {
	// BytesMetricInterval overrides the bytes_metrics_sweeper cadence.
	// Zero/unset uses storage.DefaultBytesMetricsInterval (30s).
	BytesMetricInterval time.Duration `yaml:"bytes_metric_interval"`
}

type Config struct {
	// Authentication (NORNICDB_AUTH format: "username/password" or "none")
	Auth AuthConfig

	// Database settings
	Database DatabaseConfig

	// Storage holds runtime-only storage settings (Plan 04-04). Distinct
	// from Database which mirrors ENV/YAML on-disk schema.
	Storage StorageRuntimeConfig

	// Server settings
	Server ServerConfig

	// Memory/Decay settings (NornicDB-specific)
	Memory MemoryConfig

	// Embedding worker settings (NornicDB-specific)
	EmbeddingWorker EmbeddingWorkerConfig

	// Compliance settings for GDPR/HIPAA/FISMA/SOC2 (NornicDB-specific)
	Compliance ComplianceConfig

	// Retention settings (extended, label-aware)
	Retention RetentionConfig

	// Logging
	Logging LoggingConfig

	// Observability holds telemetry config (metrics, tracing, pprof).
	// Phase 1 introduces this; Phase 2 wires slog from Logging.
	Observability observability.ObservabilityConfig

	// Feature flags for experimental/optional features
	Features FeatureFlagsConfig

	// Logger is the structured *slog.Logger threaded into storage and other
	// runtime subsystems by Phase 2 D-01 logger DI. Optional; nil falls
	// back to discard handlers at storage ctor entry per D-01a. Marked
	// yaml:"-" / json:"-" because it is a runtime-only handle that must
	// not appear in YAML/JSON config payloads.
	Logger *slog.Logger `yaml:"-" json:"-"`
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

	// WALAutoCompactionEnabled controls automatic snapshots + WAL truncation.
	// Default: true (preserves existing behavior).
	// Environment: NORNICDB_WAL_AUTO_COMPACTION_ENABLED
	WALAutoCompactionEnabled bool

	// StrictDurability enables maximum safety settings (opt-in):
	// - WAL: immediate sync (fsync every write)
	// - Badger: SyncWrites=true
	// - AsyncEngine: smaller flush interval (10ms)
	// WARNING: 2-5x slower writes. Use for financial/critical data only.
	// Environment: NORNICDB_STRICT_DURABILITY
	StrictDurability bool

	// === WAL Retention Settings ===
	// These control how long WAL segments are retained for audit/ledger purposes.
	// By default, auto-compaction truncates old WAL entries after snapshots.

	// WALRetentionMaxSegments keeps at most N sealed segments (0 = unlimited, default: 0).
	// Segments are sealed when they reach MaxFileSize or MaxEntries.
	// Set to > 0 to enable immutable segment retention for audit trails.
	// Environment: NORNICDB_WAL_RETENTION_MAX_SEGMENTS
	WALRetentionMaxSegments int

	// WALRetentionMaxAge keeps segments newer than this duration (0 = unlimited, default: 0).
	// Older segments are eligible for deletion after snapshots.
	// Example: 7 * 24 * time.Hour to keep 7 days of WAL history.
	// Environment: NORNICDB_WAL_RETENTION_MAX_AGE
	WALRetentionMaxAge time.Duration

	// WALRetentionLedgerDefaults enables ledger-grade retention defaults when no explicit
	// retention settings are supplied. This is opt-in and defaults to false.
	// When true, defaults to 24 segments and 7 days if no explicit retention is set.
	// Environment: NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS
	WALRetentionLedgerDefaults bool

	// WALSnapshotRetentionMaxCount is the max number of snapshot files to keep (0 = use storage default, typically 3).
	// Environment: NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT
	WALSnapshotRetentionMaxCount int

	// WALSnapshotRetentionMaxAge is the max age of snapshot files to keep (0 = unlimited).
	// Environment: NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_AGE
	WALSnapshotRetentionMaxAge time.Duration

	// EncryptionEnabled controls whether database encryption is active
	// Env: NORNICDB_ENCRYPTION_ENABLED
	EncryptionEnabled bool

	// EncryptionPassword for database encryption at rest
	// Required when EncryptionEnabled is true. Use a strong password in production.
	// Env: NORNICDB_ENCRYPTION_PASSWORD
	EncryptionPassword string

	// EncryptionProvider selects key management mode for at-rest DB key material.
	// Supported: "password" (default), "local".
	// Env: NORNICDB_ENCRYPTION_PROVIDER
	EncryptionProvider string

	// EncryptionKeyURI identifies the KEK in provider-backed modes.
	// Env: NORNICDB_ENCRYPTION_KEY_URI
	EncryptionKeyURI string

	// EncryptionMasterKey provides the provider master key material (dev/local).
	// Expected encoding: raw 32-byte string OR hex/base64 depending on provider.
	// Env: NORNICDB_ENCRYPTION_MASTER_KEY
	EncryptionMasterKey string

	// AWS KMS provider settings.
	EncryptionAWSRegion               string
	EncryptionAWSKMSKeyID             string
	EncryptionAWSEndpoint             string
	EncryptionAWSRoleARN              string
	EncryptionAWSRoleSessionName      string
	EncryptionAWSAccessKey            string
	EncryptionAWSSecretKey            string
	EncryptionAWSSessionToken         string
	EncryptionAWSSharedCredsFilename  string
	EncryptionAWSSharedCredsProfile   string
	EncryptionAWSWebIdentityTokenFile string

	// Azure Key Vault provider settings.
	EncryptionAzureVaultName    string
	EncryptionAzureKeyName      string
	EncryptionAzureTenantID     string
	EncryptionAzureClientID     string
	EncryptionAzureClientSecret string
	EncryptionAzureEnvironment  string
	EncryptionAzureResource     string

	// GCP Cloud KMS provider settings.
	EncryptionGCPProject         string
	EncryptionGCPLocation        string
	EncryptionGCPKeyRing         string
	EncryptionGCPKeyName         string
	EncryptionGCPCredentialsFile string

	// Encryption audit settings for provider-backed modes.
	EncryptionAuditLogPath    string
	EncryptionAuditSignEvents bool
	EncryptionAuditSignKey    string

	// Encryption rotation settings for persisted wrapped DEKs.
	EncryptionRotationEnabled  bool
	EncryptionRotationInterval time.Duration

	// === Async Write Settings ===
	// These control the async write-behind cache for better throughput.

	// AsyncWritesEnabled enables async writes for faster performance.
	// Writes return immediately after caching; flushed to disk in background.
	// Env: NORNICDB_ASYNC_WRITES_ENABLED (default: true)
	AsyncWritesEnabled bool

	// AsyncFlushInterval controls how often pending writes are flushed.
	// Smaller = more consistent, larger = better throughput.
	// Env: NORNICDB_ASYNC_FLUSH_INTERVAL (default: 50ms)
	AsyncFlushInterval time.Duration

	// AsyncMaxNodeCacheSize is the max nodes to buffer before forcing a flush.
	// Prevents unbounded memory growth during bulk inserts.
	// Set to 0 for unlimited (not recommended for bulk operations).
	// Env: NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE (default: 50000)
	AsyncMaxNodeCacheSize int

	// AsyncMaxEdgeCacheSize is the max edges to buffer before forcing a flush.
	// Prevents unbounded memory growth during bulk inserts.
	// Set to 0 for unlimited (not recommended for bulk operations).
	// Env: NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE (default: 100000)
	AsyncMaxEdgeCacheSize int

	// === Badger In-Process Cache Settings ===
	// These control the in-memory caches inside BadgerEngine for hot read paths.

	// BadgerNodeCacheMaxEntries is the max nodes to keep in the hot node cache.
	// When exceeded, the cache is cleared (simple eviction).
	// Env: NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES (default: 10000)
	BadgerNodeCacheMaxEntries int

	// BadgerEdgeTypeCacheMaxTypes is the max distinct edge types to cache for GetEdgesByType.
	// When exceeded, the cache is cleared (simple eviction).
	// Env: NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES (default: 50)
	BadgerEdgeTypeCacheMaxTypes int

	// MVCCRetentionMaxVersions keeps at most this many closed historical MVCC versions per key by default.
	// The current head is always preserved separately.
	// Env: NORNICDB_MVCC_RETENTION_MAX_VERSIONS (default: 1)
	MVCCRetentionMaxVersions int

	// MVCCRetentionTTL protects MVCC versions newer than now-TTL from pruning.
	// Zero disables age-based protection.
	// Env: NORNICDB_MVCC_RETENTION_TTL
	MVCCRetentionTTL time.Duration

	// IDFreelistTTL is the debounce window before a deleted node/edge's
	// numID can be recycled. Long enough that any in-flight snapshot
	// reader that started before the delete has finished. Default 30s
	// — query-scoped readers finish in seconds, so 30s is comfortably
	// beyond the normal read window.
	// Env: NORNICDB_ID_FREELIST_TTL (e.g. "30s", "5m", "1h")
	IDFreelistTTL time.Duration

	// MVCCLifecycleEnabled enables the MVCC lifecycle manager.
	MVCCLifecycleEnabled bool

	// MVCCLifecycleCycleInterval controls background lifecycle cadence.
	MVCCLifecycleCycleInterval time.Duration

	// MVCCLifecycleMaxSnapshotAge bounds snapshot lifetime under pressure.
	MVCCLifecycleMaxSnapshotAge time.Duration

	// MVCCLifecycleMaxChainCap bounds pathological MVCC chain growth.
	MVCCLifecycleMaxChainCap int

	// AllowStorageUpgrade authorizes the engine to advance the on-disk
	// storage version through migration arms this binary understands.
	// Without it, opening an out-of-date data directory fails with a
	// clear error message. The upgrade is one-way; operators should
	// back up before enabling this. Set via --upgrade-storage on the CLI.
	AllowStorageUpgrade bool

	// PersistSearchIndexes (EXPERIMENTAL) when true saves BM25, vector, and HNSW indexes under DataDir and loads
	// them on startup so BuildIndexes can skip the full storage iteration. Default: false.
	// Note: if indexes are incompatible/missing and must be rebuilt, startup can be long for large datasets.
	// For example, rebuilding IVF-HNSW for ~1M embeddings can take ~30 minutes on startup (hardware dependent).
	// Env: NORNICDB_PERSIST_SEARCH_INDEXES
	PersistSearchIndexes bool
}

// ServerConfig holds server settings.
type ServerConfig struct {
	// BoltEnabled controls Bolt protocol server
	BoltEnabled bool
	// BoltPort for Bolt connections (default 7687)
	BoltPort int
	// BoltAddress to bind to
	BoltAddress string
	// BoltServerAnnouncement overrides the Bolt HELLO SUCCESS "server" metadata.
	// Use this only as a client compatibility workaround for strict Neo4j-only tools.
	// Env: NORNICDB_BOLT_SERVER_ANNOUNCEMENT
	BoltServerAnnouncement string
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

	// EnableCORS enables CORS headers for cross-origin requests
	// Env: NORNICDB_CORS_ENABLED (default: false for security)
	EnableCORS bool
	// CORSOrigins is a comma-separated list of allowed origins
	// Use "*" to allow all origins (not recommended for production with credentials)
	// Env: NORNICDB_CORS_ORIGINS (default: empty - must be explicitly configured)
	// Example: "https://myapp.com,https://admin.myapp.com"
	CORSOrigins []string
}

// EmbeddingWorkerConfig holds settings for the background embedding worker.
// Environment variables:
//   - NORNICDB_EMBED_SCAN_INTERVAL: How often to scan for unembedded nodes (default: 15m)
//   - NORNICDB_EMBED_BATCH_DELAY: Delay between processing nodes (default: 500ms)
//   - NORNICDB_EMBED_TRIGGER_DEBOUNCE: Delay before write-triggered scans fire (default: 2s)
//   - NORNICDB_EMBED_MAX_RETRIES: Max retry attempts per node (default: 3)
//   - NORNICDB_EMBED_CHUNK_SIZE: Max tokens per chunk (default: 8192)
//   - NORNICDB_EMBED_CHUNK_OVERLAP: Tokens to overlap between chunks (default: 50)
//   - NORNICDB_EMBED_WORKER_NUM_WORKERS: Number of concurrent embedding workers (default: 1)
//   - NORNICDB_EMBEDDING_PROPERTIES_INCLUDE: Comma-separated property keys to use for embedding text (empty = all)
//   - NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE: Comma-separated property keys to exclude from embedding text
//   - NORNICDB_EMBEDDING_INCLUDE_LABELS: Whether to prepend node labels to embedding text (default: true)
type EmbeddingWorkerConfig struct {
	// NumWorkers is the number of concurrent workers processing embeddings
	// Use more workers for network-based embedders (OpenAI, etc.) or multiple GPUs
	NumWorkers int
	// ScanInterval is how often to scan for nodes without embeddings
	ScanInterval time.Duration
	// BatchDelay is the delay between processing individual nodes
	BatchDelay time.Duration
	// TriggerDebounceDelay delays write-triggered scans until mutation bursts settle.
	TriggerDebounceDelay time.Duration
	// MaxRetries is the max retry attempts per node
	MaxRetries int
	// ChunkSize is max tokens per chunk.
	ChunkSize int
	// ChunkOverlap is tokens to overlap between chunks.
	ChunkOverlap int
	// PropertiesInclude: if non-empty, only these property keys are used when building embedding text.
	// Enables "embed only content" or "embed only title,description". Empty = use all (subject to PropertiesExclude).
	PropertiesInclude []string
	// PropertiesExclude: these property keys are never used when building embedding text (in addition to built-in skips).
	PropertiesExclude []string
	// IncludeLabels: if true (default), node labels are prepended to the embedding text.
	IncludeLabels bool
}

// MemoryConfig holds NornicDB memory decay settings and runtime memory management.
type MemoryConfig struct {
	// DecayEnabled controls memory decay
	DecayEnabled bool
	// DecayInterval controls how often access metadata is flushed from
	// in-memory accumulators to Badger. Shorter intervals mean access
	// counts reach the scorer faster; longer intervals reduce write I/O.
	DecayInterval time.Duration
	// AccessFlushBufferSize is the maximum number of distinct entities buffered
	// in the access accumulator before an automatic flush is triggered. 0 means
	// unlimited (flush only on the DecayInterval timer).
	AccessFlushBufferSize int
	// VisibilityThreshold is the score below which nodes are suppressed from
	// visibility in query results.
	VisibilityThreshold float64
	// EmbeddingEnabled controls whether embedding generation is active
	// Env: NORNICDB_EMBEDDING_ENABLED
	EmbeddingEnabled bool
	// EmbeddingProvider (local, ollama, openai)
	EmbeddingProvider string
	// EmbeddingModel name
	EmbeddingModel string
	// EmbeddingAPIURL endpoint
	EmbeddingAPIURL string
	// EmbeddingAPIKey for authenticated providers (OpenAI, etc.). Env: NORNICDB_EMBEDDING_API_KEY
	EmbeddingAPIKey string
	// EmbeddingDimensions size
	EmbeddingDimensions int
	// EmbeddingCacheSize is max embeddings to cache (0 = disabled, default: 10000)
	// Each cached embedding uses ~4KB (1024 dims × 4 bytes)
	// 10000 cache = ~40MB memory, provides significant speedup for repeated queries
	EmbeddingCacheSize int
	// SearchMinSimilarity is the minimum cosine similarity threshold for vector search results.
	// Apple Intelligence embeddings produce scores in 0.2-0.8 range, bge-m3/mxbai produce 0.7-0.99.
	// Default: 0.0 (let RRF ranking handle relevance filtering)
	// Env: NORNICDB_SEARCH_MIN_SIMILARITY
	SearchMinSimilarity float64
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
	// DefaultNodeLabel is the label applied to nodes when no label is specified.
	// Env: NORNICDB_DEFAULT_NODE_LABEL (default: "Memory")
	DefaultNodeLabel string
	// AutoLinksEnabled for automatic relationship detection
	AutoLinksEnabled bool
	// AutoLinksSimilarityThreshold for similarity-based links
	AutoLinksSimilarityThreshold float64
	// KmeansMinEmbeddings is minimum embeddings required for k-means clustering
	// Env: NORNICDB_KMEANS_MIN_EMBEDDINGS (default: 1000)
	KmeansMinEmbeddings int
	// KmeansClusterInterval is how often to run k-means clustering (0 = disabled)
	// Env: NORNICDB_KMEANS_CLUSTER_INTERVAL (default: 5m)
	KmeansClusterInterval time.Duration
	// KmeansNumClusters is the number of k-means clusters (0 = auto from dataset size).
	// Env: NORNICDB_KMEANS_NUM_CLUSTERS (0 or unset = auto)
	KmeansNumClusters int

	// === Runtime Memory Management (Go runtime tuning) ===

	// RuntimeLimit is the soft memory limit (GOMEMLIMIT) in bytes
	// 0 = unlimited (Go manages automatically)
	// Set to 80% of container memory for optimal performance
	RuntimeLimit int64
	// RuntimeLimitStr is the configured value in megabytes as a string (e.g., "500")
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
	// Retention is opt-in and disabled by default.
	RetentionEnabled     bool
	RetentionPolicyDays  int      // Default retention period (0 = indefinite)
	RetentionAutoDelete  bool     // Auto-delete vs archive after retention
	RetentionExemptRoles []string // Roles exempt from retention (default "admin" matches auth.RoleAdmin; config cannot import auth due to cycle)

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
	// SubjectIdentifierProperties lists properties used to associate a node with a data subject.
	// Nodes are considered owned by a subject when any configured property matches the subject ID.
	SubjectIdentifierProperties []string
	// SubjectPseudonymizeProperties lists properties whose values should be replaced with an anonymized token.
	// When empty, SubjectIdentifierProperties are used.
	SubjectPseudonymizeProperties []string
	// SubjectRedactProperties lists properties to remove from matching nodes during anonymization.
	SubjectRedactProperties []string

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

// RetentionPolicyConfig defines a single retention policy in YAML config.
type RetentionPolicyConfig struct {
	ID                   string   `yaml:"id"`
	Name                 string   `yaml:"name"`
	Category             string   `yaml:"category"`
	RetentionDays        int      `yaml:"retention_days"`
	Indefinite           bool     `yaml:"indefinite"`
	ArchiveBeforeDelete  bool     `yaml:"archive_before_delete"`
	ArchivePath          string   `yaml:"archive_path"`
	ComplianceFrameworks []string `yaml:"compliance_frameworks"`
	Description          string   `yaml:"description"`
	Active               *bool    `yaml:"active"`
}

// RetentionConfig holds extended retention configuration.
type RetentionConfig struct {
	// SweepIntervalSeconds controls how often the retention sweep runs in whole seconds.
	SweepIntervalSeconds int
	// ExcludedLabels lists labels that are globally exempt from retention deletion.
	ExcludedLabels []string
	// PoliciesFile is the optional JSON persistence path for retention policies.
	PoliciesFile string
	// DefaultPolicies loads the built-in compliance policies on startup.
	DefaultPolicies bool
	// MaxSweepRecords limits how many records a single sweep iteration will process.
	MaxSweepRecords int
	// Policies defines per-category policies inline in config.
	Policies []RetentionPolicyConfig
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
	// SlowQueryThreshold for logging slow queries (single source of truth
	// per D-04d; replaces the prior pkg/server.Config.SlowQueryThreshold).
	SlowQueryThreshold time.Duration
	// SlowQueryLogFile is the optional file path for the slow-query log
	// stream. Empty => log to the configured server logger. Single source of
	// truth per D-04d.
	SlowQueryLogFile string
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

	// Heimdall model name (without .gguf extension for local; model name for ollama/openai)
	// Environment: NORNICDB_HEIMDALL_MODEL (default: qwen2.5-1.5b-instruct-q4_k_m)
	HeimdallModel string

	// Heimdall provider: "local" (GGUF), "ollama", or "openai"
	// Environment: NORNICDB_HEIMDALL_PROVIDER (default: local)
	HeimdallProvider string

	// Heimdall API URL for remote providers (ollama/openai)
	// Environment: NORNICDB_HEIMDALL_API_URL (e.g. http://localhost:11434 for ollama)
	HeimdallAPIURL string

	// Heimdall API key for openai (and other authenticated providers)
	// Environment: NORNICDB_HEIMDALL_API_KEY
	HeimdallAPIKey string

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

	// Expose MCP tools (store, recall, discover, link, task, tasks) to the Heimdall agentic loop.
	// When false, the LLM does not see or call MCP tools (reduces context size). Default: false.
	// Environment: NORNICDB_HEIMDALL_MCP_ENABLE (default: false)
	HeimdallMCPEnable bool
	// Allowlist of MCP tool names to expose when HeimdallMCPEnable is true. Nil = expose all tools;
	// empty slice = expose none (disable); non-empty = only those names (e.g. ["store","link"]).
	// Environment: NORNICDB_HEIMDALL_MCP_TOOLS (comma-separated; unset = all, empty string = none)
	HeimdallMCPTools []string

	// === Search Rerank (Stage-2 reranking, independent of Heimdall) ===
	// Reranking can use a local GGUF model (like embeddings) or an external API
	// (ollama/openai/cohere/HTTP), similar to Heimdall and embeddings.

	// SearchRerankEnabled enables Stage-2 reranking for vector/hybrid search.
	// Environment: NORNICDB_SEARCH_RERANK_ENABLED (default: false)
	SearchRerankEnabled bool
	// SearchRerankProvider: "local" (GGUF), "ollama", "openai", or "http" (Cohere/HuggingFace TEI/custom).
	// Environment: NORNICDB_SEARCH_RERANK_PROVIDER (default: local)
	SearchRerankProvider string
	// SearchRerankModel: for local = GGUF filename (e.g. bge-reranker-v2-m3-Q4_K_M.gguf); for API = model name/id.
	// Environment: NORNICDB_SEARCH_RERANK_MODEL
	SearchRerankModel string
	// SearchRerankAPIURL is the rerank API endpoint for ollama/openai/http (e.g. http://localhost:11434/rerank, https://api.cohere.ai/v1/rerank).
	// Environment: NORNICDB_SEARCH_RERANK_API_URL
	SearchRerankAPIURL string
	// SearchRerankAPIKey for authenticated providers (e.g. OpenAI, Cohere).
	// Environment: NORNICDB_SEARCH_RERANK_API_KEY
	SearchRerankAPIKey string

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

	// === Qdrant gRPC Compatibility Layer ===
	// When enabled, NornicDB exposes a Qdrant-compatible gRPC endpoint
	// allowing existing Qdrant SDKs (Python, Go, Rust, etc.) to connect.
	// This integrates with the existing search.Service for unified indexes.

	// QdrantGRPCEnabled enables the Qdrant-compatible gRPC server
	// Environment: NORNICDB_QDRANT_GRPC_ENABLED (default: false)
	QdrantGRPCEnabled bool

	// QdrantGRPCListenAddr is the address for the Qdrant gRPC server
	// Environment: NORNICDB_QDRANT_GRPC_LISTEN_ADDR (default: ":6334")
	QdrantGRPCListenAddr string

	// QdrantGRPCMaxVectorDim is the maximum allowed vector dimension
	// Environment: NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM (default: 4096)
	QdrantGRPCMaxVectorDim int

	// QdrantGRPCMaxBatchPoints is the max points per upsert batch
	// Environment: NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS (default: 1000)
	QdrantGRPCMaxBatchPoints int

	// QdrantGRPCMaxTopK is the maximum search results
	// Environment: NORNICDB_QDRANT_GRPC_MAX_TOP_K (default: 1000)
	QdrantGRPCMaxTopK int

	// QdrantGRPCMethodPermissions optionally overrides required permissions for
	// specific Qdrant gRPC RPCs.
	//
	// This is configured via YAML config only (not env vars) under:
	//   qdrant_grpc:
	//     rbac:
	//       methods:
	//         "Points/Upsert": "write"
	//
	// Values are one of: read, write, create, delete, admin, schema, user_manage.
	QdrantGRPCMethodPermissions map[string]string
}

// Heimdall config getter methods for heimdall.FeatureFlagsSource interface
func (f *FeatureFlagsConfig) GetHeimdallEnabled() bool          { return f.HeimdallEnabled }
func (f *FeatureFlagsConfig) GetHeimdallModel() string          { return f.HeimdallModel }
func (f *FeatureFlagsConfig) GetHeimdallProvider() string       { return f.HeimdallProvider }
func (f *FeatureFlagsConfig) GetHeimdallAPIURL() string         { return f.HeimdallAPIURL }
func (f *FeatureFlagsConfig) GetHeimdallAPIKey() string         { return f.HeimdallAPIKey }
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
func (f *FeatureFlagsConfig) GetHeimdallMCPEnable() bool        { return f.HeimdallMCPEnable }
func (f *FeatureFlagsConfig) GetHeimdallMCPTools() []string     { return f.HeimdallMCPTools }

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
//	// Memory decay disabled by default (opt-in)
//	fmt.Printf("Decay enabled: %v\n", config.Memory.DecayEnabled) // false
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
//	- NORNICDB_DEFAULT_DATABASE="nornic"
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
	if err := applyEnvVars(config); err != nil {
		panic(fmt.Sprintf("invalid environment configuration: %v", err))
	}
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

	providerMode := strings.ToLower(strings.TrimSpace(c.Database.EncryptionProvider))
	if providerMode == "" {
		providerMode = "password"
	}
	switch providerMode {
	case "password", "local", "aws-kms", "azure-keyvault", "gcp-cloudkms":
	default:
		return fmt.Errorf("invalid encryption provider: %s", c.Database.EncryptionProvider)
	}
	if c.Database.EncryptionRotationInterval < 0 {
		return fmt.Errorf("invalid encryption rotation interval: %s", c.Database.EncryptionRotationInterval)
	}
	if c.Database.EncryptionAuditSignEvents && strings.TrimSpace(c.Database.EncryptionAuditSignKey) == "" {
		return fmt.Errorf("encryption audit signing is enabled but no signing key was provided")
	}
	if c.Database.EncryptionEnabled {
		switch providerMode {
		case "password":
			if strings.TrimSpace(c.Database.EncryptionPassword) == "" {
				return fmt.Errorf("encryption is enabled but no encryption password was provided")
			}
		case "local":
			if strings.TrimSpace(c.Database.EncryptionMasterKey) == "" {
				return fmt.Errorf("local encryption provider requires encryption_master_key")
			}
		case "aws-kms":
			if strings.TrimSpace(c.Database.EncryptionAWSRegion) == "" || strings.TrimSpace(c.Database.EncryptionAWSKMSKeyID) == "" {
				return fmt.Errorf("aws-kms encryption provider requires encryption_aws_region and encryption_aws_kms_key_id")
			}
		case "azure-keyvault":
			if strings.TrimSpace(c.Database.EncryptionAzureVaultName) == "" || strings.TrimSpace(c.Database.EncryptionAzureKeyName) == "" {
				return fmt.Errorf("azure-keyvault encryption provider requires encryption_azure_vault_name and encryption_azure_key_name")
			}
		case "gcp-cloudkms":
			if strings.TrimSpace(c.Database.EncryptionGCPProject) == "" || strings.TrimSpace(c.Database.EncryptionGCPLocation) == "" || strings.TrimSpace(c.Database.EncryptionGCPKeyRing) == "" || strings.TrimSpace(c.Database.EncryptionGCPKeyName) == "" {
				return fmt.Errorf("gcp-cloudkms encryption provider requires encryption_gcp_project, encryption_gcp_location, encryption_gcp_key_ring, and encryption_gcp_key_name")
			}
		}
	}

	if c.Database.MVCCRetentionMaxVersions < 0 {
		return fmt.Errorf("invalid mvcc retention max versions: %d", c.Database.MVCCRetentionMaxVersions)
	}
	if c.Database.IDFreelistTTL < 0 {
		return fmt.Errorf("invalid id freelist ttl: %s", c.Database.IDFreelistTTL)
	}
	if c.Database.MVCCRetentionTTL < 0 {
		return fmt.Errorf("invalid mvcc retention ttl: %s", c.Database.MVCCRetentionTTL)
	}
	if c.Database.MVCCLifecycleCycleInterval < 0 {
		return fmt.Errorf("invalid mvcc lifecycle interval: %s", c.Database.MVCCLifecycleCycleInterval)
	}
	if c.Database.MVCCLifecycleMaxSnapshotAge < 0 {
		return fmt.Errorf("invalid mvcc lifecycle max snapshot age: %s", c.Database.MVCCLifecycleMaxSnapshotAge)
	}
	if c.Database.MVCCLifecycleMaxChainCap < 0 {
		return fmt.Errorf("invalid mvcc lifecycle max chain cap: %d", c.Database.MVCCLifecycleMaxChainCap)
	}
	if c.EmbeddingWorker.TriggerDebounceDelay < 0 {
		return fmt.Errorf("invalid embed trigger debounce: %s", c.EmbeddingWorker.TriggerDebounceDelay)
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
		BoltPort               int    `yaml:"bolt_port"`
		HTTPPort               int    `yaml:"http_port"`
		Port                   int    `yaml:"port"`                     // Alias for bolt_port
		Host                   string `yaml:"host"`                     // Bind address
		Address                string `yaml:"address"`                  // Alias for host
		DataDir                string `yaml:"data_dir"`                 // Data directory
		Auth                   string `yaml:"auth"`                     // Format: "username:password" or "none"
		BoltEnabled            bool   `yaml:"bolt_enabled"`             // Enable Bolt protocol
		HTTPEnabled            bool   `yaml:"http_enabled"`             // Enable HTTP API
		BoltServerAnnouncement string `yaml:"bolt_server_announcement"` // Override Bolt HELLO server metadata
		TLS                    struct {
			Enabled  bool   `yaml:"enabled"`
			CertFile string `yaml:"cert_file"`
			KeyFile  string `yaml:"key_file"`
		} `yaml:"tls"`
	} `yaml:"server"`

	// Database/Storage configuration
	Database struct {
		DataDir                           string `yaml:"data_dir"`
		DefaultDatabase                   string `yaml:"default_database"`
		ReadOnly                          bool   `yaml:"read_only"`
		TransactionTimeout                string `yaml:"transaction_timeout"`
		MaxConcurrentTransactions         int    `yaml:"max_concurrent_transactions"`
		StrictDurability                  bool   `yaml:"strict_durability"`
		WALSyncMode                       string `yaml:"wal_sync_mode"`
		WALSyncInterval                   string `yaml:"wal_sync_interval"`
		WALAutoCompactionEnabled          *bool  `yaml:"wal_auto_compaction_enabled"`
		WALRetentionMaxSegments           int    `yaml:"wal_retention_max_segments"`
		WALRetentionMaxAge                string `yaml:"wal_retention_max_age"`
		WALRetentionLedgerDefaults        bool   `yaml:"wal_ledger_retention_defaults"`
		WALSnapshotRetentionMaxCount      int    `yaml:"wal_snapshot_retention_max_count"`
		WALSnapshotRetentionMaxAge        string `yaml:"wal_snapshot_retention_max_age"`
		EncryptionEnabled                 bool   `yaml:"encryption_enabled"`
		EncryptionPassword                string `yaml:"encryption_password"`
		EncryptionProvider                string `yaml:"encryption_provider"`
		EncryptionKeyURI                  string `yaml:"encryption_key_uri"`
		EncryptionMasterKey               string `yaml:"encryption_master_key"`
		EncryptionAWSRegion               string `yaml:"encryption_aws_region"`
		EncryptionAWSKMSKeyID             string `yaml:"encryption_aws_kms_key_id"`
		EncryptionAWSEndpoint             string `yaml:"encryption_aws_endpoint"`
		EncryptionAWSRoleARN              string `yaml:"encryption_aws_role_arn"`
		EncryptionAWSRoleSessionName      string `yaml:"encryption_aws_role_session_name"`
		EncryptionAWSAccessKey            string `yaml:"encryption_aws_access_key"`
		EncryptionAWSSecretKey            string `yaml:"encryption_aws_secret_key"`
		EncryptionAWSSessionToken         string `yaml:"encryption_aws_session_token"`
		EncryptionAWSSharedCredsFilename  string `yaml:"encryption_aws_shared_creds_filename"`
		EncryptionAWSSharedCredsProfile   string `yaml:"encryption_aws_shared_creds_profile"`
		EncryptionAWSWebIdentityTokenFile string `yaml:"encryption_aws_web_identity_token_file"`
		EncryptionAzureVaultName          string `yaml:"encryption_azure_vault_name"`
		EncryptionAzureKeyName            string `yaml:"encryption_azure_key_name"`
		EncryptionAzureTenantID           string `yaml:"encryption_azure_tenant_id"`
		EncryptionAzureClientID           string `yaml:"encryption_azure_client_id"`
		EncryptionAzureClientSecret       string `yaml:"encryption_azure_client_secret"`
		EncryptionAzureEnvironment        string `yaml:"encryption_azure_environment"`
		EncryptionAzureResource           string `yaml:"encryption_azure_resource"`
		EncryptionGCPProject              string `yaml:"encryption_gcp_project"`
		EncryptionGCPLocation             string `yaml:"encryption_gcp_location"`
		EncryptionGCPKeyRing              string `yaml:"encryption_gcp_key_ring"`
		EncryptionGCPKeyName              string `yaml:"encryption_gcp_key_name"`
		EncryptionGCPCredentialsFile      string `yaml:"encryption_gcp_credentials_file"`
		EncryptionAuditLogPath            string `yaml:"encryption_audit_log_path"`
		EncryptionAuditSignEvents         *bool  `yaml:"encryption_audit_sign_events"`
		EncryptionAuditSignKey            string `yaml:"encryption_audit_sign_key"`
		EncryptionRotationEnabled         *bool  `yaml:"encryption_rotation_enabled"`
		EncryptionRotationInterval        string `yaml:"encryption_rotation_interval"`
		BadgerNodeCacheMaxEntries         int    `yaml:"badger_node_cache_max_entries"`
		BadgerEdgeTypeCacheMaxTypes       int    `yaml:"badger_edge_type_cache_max_types"`
		MVCCRetentionMaxVersions          int    `yaml:"mvcc_retention_max_versions"`
		MVCCRetentionTTL                  string `yaml:"mvcc_retention_ttl"`
		IDFreelistTTL                     string `yaml:"id_freelist_ttl"`
		MVCCLifecycleEnabled              *bool  `yaml:"mvcc_lifecycle_enabled"`
		MVCCLifecycleCycleInterval        string `yaml:"mvcc_lifecycle_interval"`
		MVCCLifecycleMaxSnapshotAge       string `yaml:"mvcc_lifecycle_max_snapshot_age"`
		MVCCLifecycleMaxChainCap          int    `yaml:"mvcc_lifecycle_max_chain_cap"`
		PersistSearchIndexes              bool   `yaml:"persist_search_indexes"`
	} `yaml:"database"`

	// Storage alias for database
	Storage struct {
		Path string `yaml:"path"`
		// BytesMetricInterval is the cadence for the Plan 04-04 D-07
		// bytes_metrics_sweeper lifecycle.Component. Default 30s when
		// zero/unset. Configurable for tests + ops who want denser or
		// sparser cadence.
		BytesMetricInterval time.Duration `yaml:"bytes_metric_interval"`
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
		Enabled       bool    `yaml:"enabled"`
		Provider      string  `yaml:"provider"`
		Model         string  `yaml:"model"`
		URL           string  `yaml:"url"`
		APIKey        string  `yaml:"api_key"`
		Dimensions    int     `yaml:"dimensions"`
		CacheSize     int     `yaml:"cache_size"`
		MinSimilarity float64 `yaml:"min_similarity"`
	} `yaml:"embedding"`

	// Memory/Decay configuration
	Memory struct {
		DecayEnabled                 bool    `yaml:"decay_enabled"`
		DecayIntervalSeconds         int     `yaml:"decay_interval"`
		AccessFlushBufferSize        int     `yaml:"access_flush_buffer_size"`
		VisibilityThreshold          float64 `yaml:"visibility_threshold"`
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
		ScanInterval      string   `yaml:"scan_interval"`
		BatchDelay        string   `yaml:"batch_delay"`
		TriggerDebounce   string   `yaml:"trigger_debounce"`
		MaxRetries        int      `yaml:"max_retries"`
		ChunkSize         int      `yaml:"chunk_size"`
		ChunkOverlap      int      `yaml:"chunk_overlap"`
		PropertiesInclude []string `yaml:"properties_include"`
		PropertiesExclude []string `yaml:"properties_exclude"`
		IncludeLabels     *bool    `yaml:"include_labels"`
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
		Enabled          bool     `yaml:"enabled"`
		Model            string   `yaml:"model"`
		Provider         string   `yaml:"provider"` // local, ollama, openai
		APIURL           string   `yaml:"api_url"`  // for ollama/openai
		APIKey           string   `yaml:"api_key"`  // for openai
		GPULayers        *int     `yaml:"gpu_layers"`
		ContextSize      int      `yaml:"context_size"`
		BatchSize        int      `yaml:"batch_size"`
		MaxTokens        int      `yaml:"max_tokens"`
		Temperature      float64  `yaml:"temperature"`
		AnomalyDetection bool     `yaml:"anomaly_detection"`
		RuntimeDiagnosis bool     `yaml:"runtime_diagnosis"`
		MemoryCuration   bool     `yaml:"memory_curation"`
		MaxContextTokens int      `yaml:"max_context_tokens"`
		MaxSystemTokens  int      `yaml:"max_system_tokens"`
		MaxUserTokens    int      `yaml:"max_user_tokens"`
		MCPEnable        bool     `yaml:"mcp_enable"` // expose MCP tools to agentic loop (default: false)
		MCPTools         []string `yaml:"mcp_tools"`  // allowlist: nil/omit = all, [] = none, [store,link] = only those
	} `yaml:"heimdall"`

	// Search rerank (Stage-2 reranking: local GGUF or external API like embeddings/Heimdall).
	SearchRerank struct {
		Enabled  bool   `yaml:"enabled"`
		Provider string `yaml:"provider"` // local, ollama, openai, http
		Model    string `yaml:"model"`
		APIURL   string `yaml:"api_url"`
		APIKey   string `yaml:"api_key"`
	} `yaml:"search_rerank"`

	// Feature flags (subset supported in YAML).
	Features struct {
		// Qdrant gRPC compatibility endpoint.
		QdrantGRPCEnabled        bool   `yaml:"qdrant_grpc_enabled"`
		QdrantGRPCListenAddr     string `yaml:"qdrant_grpc_listen_addr"`
		QdrantGRPCMaxVectorDim   int    `yaml:"qdrant_grpc_max_vector_dim"`
		QdrantGRPCMaxBatchPoints int    `yaml:"qdrant_grpc_max_batch_points"`
		QdrantGRPCMaxTopK        int    `yaml:"qdrant_grpc_max_top_k"`

		QdrantGRPCRBAC struct {
			Methods map[string]string `yaml:"methods"`
		} `yaml:"qdrant_grpc_rbac"`
	} `yaml:"features"`

	// Qdrant gRPC compatibility endpoint (legacy YAML shape; supported for compatibility).
	QdrantGRPC struct {
		Enabled        bool   `yaml:"enabled"`
		ListenAddr     string `yaml:"listen_addr"`
		MaxVectorDim   int    `yaml:"max_vector_dim"`
		MaxBatchPoints int    `yaml:"max_batch_points"`
		MaxTopK        int    `yaml:"max_top_k"`

		RBAC struct {
			Methods map[string]string `yaml:"methods"`
		} `yaml:"rbac"`
	} `yaml:"qdrant_grpc"`

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
		DataExportEnabled             bool     `yaml:"data_export_enabled"`
		DataErasureEnabled            bool     `yaml:"data_erasure_enabled"`
		DataAccessEnabled             bool     `yaml:"data_access_enabled"`
		SubjectIdentifierProperties   []string `yaml:"subject_identifier_properties"`
		SubjectPseudonymizeProperties []string `yaml:"subject_pseudonymize_properties"`
		SubjectRedactProperties       []string `yaml:"subject_redact_properties"`
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

	Retention struct {
		SweepInterval   int                     `yaml:"sweep_interval"`
		ExcludedLabels  []string                `yaml:"excluded_labels"`
		PoliciesFile    string                  `yaml:"policies_file"`
		DefaultPolicies bool                    `yaml:"default_policies"`
		MaxSweepRecords int                     `yaml:"max_sweep_records"`
		Policies        []RetentionPolicyConfig `yaml:"policies"`
	} `yaml:"retention"`

	// Logging configuration
	Logging struct {
		Level              string `yaml:"level"`
		Format             string `yaml:"format"`
		Output             string `yaml:"output"`
		QueryLogEnabled    bool   `yaml:"query_log_enabled"`
		SlowQueryThreshold string `yaml:"slow_query_threshold"`
	} `yaml:"logging"`

	// Observability configuration (Phase 1 — OBS-02)
	Observability struct {
		Metrics struct {
			Enabled             *bool  `yaml:"enabled"`
			Listen              string `yaml:"listen"`
			TenantLabelsEnabled *bool  `yaml:"tenant_labels_enabled"`
		} `yaml:"metrics"`
		Tracing struct {
			Enabled  *bool  `yaml:"enabled"`
			Endpoint string `yaml:"endpoint"`
			Protocol string `yaml:"protocol"`
			Insecure *bool  `yaml:"insecure"`
			Timeout  string `yaml:"timeout"`
		} `yaml:"tracing"`
		Pprof struct {
			Enabled *bool  `yaml:"enabled"`
			Listen  string `yaml:"listen"`
		} `yaml:"pprof"`
	} `yaml:"observability"`

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
	config.Database.DefaultDatabase = "nornic" // Default database name (like Neo4j's "neo4j")
	config.Database.ReadOnly = false
	config.Database.TransactionTimeout = 30 * time.Second
	config.Database.MaxConcurrentTransactions = 1000
	config.Database.StrictDurability = false
	config.Database.WALSyncMode = "batch"
	config.Database.WALSyncInterval = 100 * time.Millisecond
	config.Database.WALAutoCompactionEnabled = true
	config.Database.WALRetentionMaxSegments = 0 // Unlimited by default
	config.Database.WALRetentionMaxAge = 0      // Unlimited by default
	config.Database.WALRetentionLedgerDefaults = false
	config.Database.WALSnapshotRetentionMaxCount = 0 // 0 = use storage default (3)
	config.Database.WALSnapshotRetentionMaxAge = 0   // Unlimited by default
	config.Database.EncryptionPassword = ""          // disabled by default
	config.Database.EncryptionProvider = "password"
	config.Database.EncryptionKeyURI = ""
	config.Database.EncryptionMasterKey = ""
	config.Database.EncryptionAWSRegion = ""
	config.Database.EncryptionAWSKMSKeyID = ""
	config.Database.EncryptionAWSEndpoint = ""
	config.Database.EncryptionAWSRoleARN = ""
	config.Database.EncryptionAWSRoleSessionName = ""
	config.Database.EncryptionAWSAccessKey = ""
	config.Database.EncryptionAWSSecretKey = ""
	config.Database.EncryptionAWSSessionToken = ""
	config.Database.EncryptionAWSSharedCredsFilename = ""
	config.Database.EncryptionAWSSharedCredsProfile = ""
	config.Database.EncryptionAWSWebIdentityTokenFile = ""
	config.Database.EncryptionAzureVaultName = ""
	config.Database.EncryptionAzureKeyName = ""
	config.Database.EncryptionAzureTenantID = ""
	config.Database.EncryptionAzureClientID = ""
	config.Database.EncryptionAzureClientSecret = ""
	config.Database.EncryptionAzureEnvironment = ""
	config.Database.EncryptionAzureResource = ""
	config.Database.EncryptionGCPProject = ""
	config.Database.EncryptionGCPLocation = ""
	config.Database.EncryptionGCPKeyRing = ""
	config.Database.EncryptionGCPKeyName = ""
	config.Database.EncryptionGCPCredentialsFile = ""
	config.Database.EncryptionAuditLogPath = ""
	config.Database.EncryptionAuditSignEvents = false
	config.Database.EncryptionAuditSignKey = ""
	config.Database.EncryptionRotationEnabled = true
	config.Database.EncryptionRotationInterval = 90 * 24 * time.Hour
	config.Database.AsyncWritesEnabled = true
	config.Database.AsyncFlushInterval = 50 * time.Millisecond
	config.Database.AsyncMaxNodeCacheSize = 50000  // ~35MB assuming 700 bytes/node
	config.Database.AsyncMaxEdgeCacheSize = 100000 // ~50MB assuming 500 bytes/edge
	config.Database.BadgerNodeCacheMaxEntries = 10000
	config.Database.BadgerEdgeTypeCacheMaxTypes = 50
	// Head-only MVCC by default: no historical body duplication into the
	// MVCC version keyspace. Operators who need audit/rollback raise
	// this via NORNICDB_MVCC_RETENTION_MAX_VERSIONS or YAML.
	config.Database.MVCCRetentionMaxVersions = 0
	config.Database.MVCCRetentionTTL = 0
	// 30s debounce is larger than any normal query-scoped snapshot
	// reader, so freed numIDs recycle quickly. Operators with long
	// analytics queries raise this via NORNICDB_ID_FREELIST_TTL.
	config.Database.IDFreelistTTL = 30 * time.Second
	config.Database.MVCCLifecycleEnabled = true
	config.Database.MVCCLifecycleCycleInterval = 30 * time.Second
	config.Database.MVCCLifecycleMaxSnapshotAge = time.Hour
	config.Database.MVCCLifecycleMaxChainCap = 1000

	// Server defaults - Bolt
	config.Server.BoltEnabled = true
	config.Server.BoltPort = 7687
	config.Server.BoltAddress = "0.0.0.0"
	config.Server.BoltServerAnnouncement = ""
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

	// Server defaults - CORS
	config.Server.EnableCORS = true
	config.Server.CORSOrigins = []string{"*"} // Allow all origins by default

	// Memory defaults
	config.Memory.DecayEnabled = false
	config.Memory.DecayInterval = 2 * time.Second
	config.Memory.AccessFlushBufferSize = 10000
	config.Memory.VisibilityThreshold = 0.05
	config.Memory.EmbeddingEnabled = false    // Disabled by default - opt-in feature
	config.Memory.EmbeddingProvider = "local" // Use local GGUF models by default
	config.Memory.EmbeddingModel = "bge-m3"
	config.Memory.EmbeddingAPIURL = "http://localhost:11434"
	config.Memory.EmbeddingDimensions = 1024
	config.Memory.EmbeddingCacheSize = 10000
	config.Memory.ModelsDir = "./models"
	config.Memory.EmbeddingGPULayers = -1     // auto
	config.Memory.EmbeddingWarmupInterval = 0 // disabled
	config.Memory.DefaultNodeLabel = "Memory"
	config.Memory.AutoLinksEnabled = true
	config.Memory.AutoLinksSimilarityThreshold = 0.82
	config.Memory.KmeansMinEmbeddings = 100
	config.Memory.KmeansClusterInterval = 15 * time.Minute // timer-based clustering every 15 min (skips if no changes)
	config.Memory.RuntimeLimitStr = "0"
	config.Memory.RuntimeLimit = 0
	config.Memory.GCPercent = 100
	config.Memory.PoolEnabled = true
	config.Memory.PoolMaxSize = 1000
	config.Memory.QueryCacheEnabled = true
	config.Memory.QueryCacheSize = 1000
	config.Memory.QueryCacheTTL = 5 * time.Minute

	// Embedding worker defaults
	config.EmbeddingWorker.NumWorkers = 1
	config.EmbeddingWorker.ScanInterval = 15 * time.Minute
	config.EmbeddingWorker.BatchDelay = 500 * time.Millisecond
	config.EmbeddingWorker.TriggerDebounceDelay = 2 * time.Second
	config.EmbeddingWorker.MaxRetries = 3
	config.EmbeddingWorker.ChunkSize = 8192
	config.EmbeddingWorker.ChunkOverlap = 50
	config.EmbeddingWorker.PropertiesInclude = nil
	config.EmbeddingWorker.PropertiesExclude = nil
	config.EmbeddingWorker.IncludeLabels = true

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
	config.Compliance.SubjectIdentifierProperties = []string{"owner_id"}
	config.Compliance.SubjectPseudonymizeProperties = []string{"owner_id"}
	config.Compliance.SubjectRedactProperties = []string{"email", "name", "username", "ip_address"}
	config.Compliance.AnonymizationEnabled = true
	config.Compliance.AnonymizationMethod = "pseudonymization"
	config.Compliance.ConsentRequired = false
	config.Compliance.ConsentVersioning = true
	config.Compliance.ConsentAuditTrail = true
	config.Compliance.BreachDetectionEnabled = false
	config.Retention.SweepIntervalSeconds = 3600
	config.Retention.DefaultPolicies = false
	config.Retention.MaxSweepRecords = 50000

	// Logging defaults
	config.Logging.Level = "INFO"
	config.Logging.Format = "json"
	config.Logging.Output = "stdout"
	config.Logging.QueryLogEnabled = false
	config.Logging.SlowQueryThreshold = 5 * time.Second

	// Observability defaults (Phase 1 — OBS-02)
	config.Observability = observability.DefaultConfig()

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
	config.Features.HeimdallModel = "qwen3-0.6b-instruct"
	config.Features.HeimdallProvider = "local"
	config.Features.HeimdallAPIURL = ""
	config.Features.HeimdallAPIKey = ""
	config.Features.HeimdallGPULayers = -1
	config.Features.HeimdallContextSize = 8192
	config.Features.HeimdallBatchSize = 2048
	config.Features.HeimdallMaxTokens = 1024
	config.Features.HeimdallTemperature = 0.5 // Qwen3 0.6B instruct: reduces repetition vs 0.1
	config.Features.HeimdallAnomalyDetection = false
	config.Features.HeimdallRuntimeDiagnosis = false
	config.Features.HeimdallMemoryCuration = false
	config.Features.HeimdallMCPEnable = false
	config.Features.HeimdallMCPTools = nil
	config.Features.SearchRerankEnabled = false
	config.Features.SearchRerankProvider = "local"
	config.Features.SearchRerankModel = "bge-reranker-v2-m3"
	config.Features.SearchRerankAPIURL = ""
	config.Features.SearchRerankAPIKey = ""
	config.Features.HeimdallMaxContextTokens = 8192
	config.Features.HeimdallMaxSystemTokens = 6000
	config.Features.HeimdallMaxUserTokens = 2000

	// Qdrant gRPC defaults
	config.Features.QdrantGRPCEnabled = false
	config.Features.QdrantGRPCListenAddr = ":6334"
	config.Features.QdrantGRPCMaxVectorDim = 4096
	config.Features.QdrantGRPCMaxBatchPoints = 1000
	config.Features.QdrantGRPCMaxTopK = 1000
	config.Features.QdrantGRPCMethodPermissions = nil

	return config
}

// applyEnvVars applies environment variable overrides to an existing config.
// Environment variables take precedence over config file values.
func applyEnvVars(config *Config) error {
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

	// WAL auto-compaction settings
	if v, ok := envutil.LookupBoolLoose("NORNICDB_WAL_AUTO_COMPACTION_ENABLED"); ok {
		config.Database.WALAutoCompactionEnabled = v
	}

	// WAL retention settings
	if v := getEnvInt("NORNICDB_WAL_RETENTION_MAX_SEGMENTS", 0); v > 0 {
		config.Database.WALRetentionMaxSegments = v
	}
	if v := getEnvDuration("NORNICDB_WAL_RETENTION_MAX_AGE", 0); v > 0 {
		config.Database.WALRetentionMaxAge = v
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS"); ok {
		config.Database.WALRetentionLedgerDefaults = v
	}
	if config.Database.WALRetentionLedgerDefaults &&
		config.Database.WALRetentionMaxSegments == 0 &&
		config.Database.WALRetentionMaxAge == 0 {
		config.Database.WALRetentionMaxSegments = 24
		config.Database.WALRetentionMaxAge = 7 * 24 * time.Hour
	}
	if v := getEnvInt("NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT", 0); v > 0 {
		config.Database.WALSnapshotRetentionMaxCount = v
	}
	if v := getEnvDuration("NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_AGE", 0); v > 0 {
		config.Database.WALSnapshotRetentionMaxAge = v
	}

	// Async write settings
	if getEnv("NORNICDB_ASYNC_WRITES_ENABLED", "true") == "false" {
		config.Database.AsyncWritesEnabled = false
	} else {
		config.Database.AsyncWritesEnabled = true
	}
	if v := getEnvDuration("NORNICDB_ASYNC_FLUSH_INTERVAL", 0); v > 0 {
		config.Database.AsyncFlushInterval = v
	}
	if v := getEnvInt("NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE", -1); v >= 0 {
		config.Database.AsyncMaxNodeCacheSize = v
	}
	if v := getEnvInt("NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE", -1); v >= 0 {
		config.Database.AsyncMaxEdgeCacheSize = v
	}
	if v := getEnvInt("NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES", -1); v >= 0 {
		config.Database.BadgerNodeCacheMaxEntries = v
	}
	if v := getEnvInt("NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES", -1); v >= 0 {
		config.Database.BadgerEdgeTypeCacheMaxTypes = v
	}
	if v := getEnvInt("NORNICDB_MVCC_RETENTION_MAX_VERSIONS", -1); v >= 0 {
		config.Database.MVCCRetentionMaxVersions = v
	}
	if v := getEnvDuration("NORNICDB_MVCC_RETENTION_TTL", 0); v > 0 {
		config.Database.MVCCRetentionTTL = v
	}
	if v := getEnvDuration("NORNICDB_ID_FREELIST_TTL", 0); v > 0 {
		config.Database.IDFreelistTTL = v
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_MVCC_LIFECYCLE_ENABLED"); ok {
		config.Database.MVCCLifecycleEnabled = v
	}
	if v := getEnvDuration("NORNICDB_MVCC_LIFECYCLE_INTERVAL", 0); v > 0 {
		config.Database.MVCCLifecycleCycleInterval = v
	}
	if v := getEnvDuration("NORNICDB_MVCC_LIFECYCLE_MAX_SNAPSHOT_AGE", 0); v > 0 {
		config.Database.MVCCLifecycleMaxSnapshotAge = v
	}
	if v := getEnvInt("NORNICDB_MVCC_LIFECYCLE_MAX_CHAIN_CAP", -1); v >= 0 {
		config.Database.MVCCLifecycleMaxChainCap = v
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_PERSIST_SEARCH_INDEXES"); ok {
		config.Database.PersistSearchIndexes = v
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
	if v := strings.TrimSpace(getEnv("NORNICDB_BOLT_SERVER_ANNOUNCEMENT", "")); v != "" {
		config.Server.BoltServerAnnouncement = v
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

	// CORS settings
	if getEnv("NORNICDB_CORS_ENABLED", "") == "true" {
		config.Server.EnableCORS = true
	} else if getEnv("NORNICDB_CORS_ENABLED", "") == "false" {
		config.Server.EnableCORS = false
	}
	if v := getEnv("NORNICDB_CORS_ORIGINS", ""); v != "" {
		// Parse comma-separated origins
		origins := strings.Split(v, ",")
		for i, o := range origins {
			origins[i] = strings.TrimSpace(o)
		}
		config.Server.CORSOrigins = origins
		// Auto-enable CORS if origins are specified
		if len(origins) > 0 && origins[0] != "" {
			config.Server.EnableCORS = true
		}
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
	if v := getEnv("NORNICDB_ENCRYPTION_PROVIDER", ""); v != "" {
		config.Database.EncryptionProvider = strings.ToLower(strings.TrimSpace(v))
	}
	if v := getEnv("NORNICDB_ENCRYPTION_KEY_URI", ""); v != "" {
		config.Database.EncryptionKeyURI = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_MASTER_KEY", ""); v != "" {
		config.Database.EncryptionMasterKey = strings.TrimSpace(v)
		if config.Database.EncryptionMasterKey != "" {
			config.Database.EncryptionEnabled = true
		}
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_REGION", ""); v != "" {
		config.Database.EncryptionAWSRegion = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_KMS_KEY_ID", ""); v != "" {
		config.Database.EncryptionAWSKMSKeyID = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_ENDPOINT", ""); v != "" {
		config.Database.EncryptionAWSEndpoint = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_ROLE_ARN", ""); v != "" {
		config.Database.EncryptionAWSRoleARN = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_ROLE_SESSION_NAME", ""); v != "" {
		config.Database.EncryptionAWSRoleSessionName = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_ACCESS_KEY", ""); v != "" {
		config.Database.EncryptionAWSAccessKey = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_SECRET_KEY", ""); v != "" {
		config.Database.EncryptionAWSSecretKey = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_SESSION_TOKEN", ""); v != "" {
		config.Database.EncryptionAWSSessionToken = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_SHARED_CREDS_FILENAME", ""); v != "" {
		config.Database.EncryptionAWSSharedCredsFilename = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_SHARED_CREDS_PROFILE", ""); v != "" {
		config.Database.EncryptionAWSSharedCredsProfile = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AWS_WEB_IDENTITY_TOKEN_FILE", ""); v != "" {
		config.Database.EncryptionAWSWebIdentityTokenFile = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_VAULT_NAME", ""); v != "" {
		config.Database.EncryptionAzureVaultName = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_KEY_NAME", ""); v != "" {
		config.Database.EncryptionAzureKeyName = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_TENANT_ID", ""); v != "" {
		config.Database.EncryptionAzureTenantID = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_CLIENT_ID", ""); v != "" {
		config.Database.EncryptionAzureClientID = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_CLIENT_SECRET", ""); v != "" {
		config.Database.EncryptionAzureClientSecret = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_ENVIRONMENT", ""); v != "" {
		config.Database.EncryptionAzureEnvironment = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AZURE_RESOURCE", ""); v != "" {
		config.Database.EncryptionAzureResource = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_GCP_PROJECT", ""); v != "" {
		config.Database.EncryptionGCPProject = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_GCP_LOCATION", ""); v != "" {
		config.Database.EncryptionGCPLocation = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_GCP_KEY_RING", ""); v != "" {
		config.Database.EncryptionGCPKeyRing = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_GCP_KEY_NAME", ""); v != "" {
		config.Database.EncryptionGCPKeyName = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_GCP_CREDENTIALS_FILE", ""); v != "" {
		config.Database.EncryptionGCPCredentialsFile = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AUDIT_LOG_PATH", ""); v != "" {
		config.Database.EncryptionAuditLogPath = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AUDIT_SIGN_EVENTS", ""); v != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err == nil {
			config.Database.EncryptionAuditSignEvents = parsed
		}
	}
	if v := getEnv("NORNICDB_ENCRYPTION_AUDIT_SIGN_KEY", ""); v != "" {
		config.Database.EncryptionAuditSignKey = strings.TrimSpace(v)
	}
	if v := getEnv("NORNICDB_ENCRYPTION_ROTATION_ENABLED", ""); v != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err == nil {
			config.Database.EncryptionRotationEnabled = parsed
		}
	}
	if v := getEnv("NORNICDB_ENCRYPTION_ROTATION_INTERVAL", ""); v != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			config.Database.EncryptionRotationInterval = d
		}
	}

	// Memory settings
	if v := getEnv("NORNICDB_MEMORY_DECAY_ENABLED", ""); v == "true" || v == "1" {
		config.Memory.DecayEnabled = true
	} else if v == "false" || v == "0" {
		config.Memory.DecayEnabled = false
	}
	if v := getEnvDuration("NORNICDB_MEMORY_DECAY_INTERVAL", 0); v > 0 {
		config.Memory.DecayInterval = v
	}
	if v := getEnvInt("NORNICDB_MEMORY_ACCESS_FLUSH_BUFFER_SIZE", 0); v > 0 {
		config.Memory.AccessFlushBufferSize = v
	}
	if v := getEnvFloat("NORNICDB_MEMORY_VISIBILITY_THRESHOLD", 0); v > 0 {
		config.Memory.VisibilityThreshold = v
	}
	// Embedding enabled - check for explicit true/false
	if v := getEnv("NORNICDB_EMBEDDING_ENABLED", ""); v != "" {
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
	if v := getEnv("NORNICDB_EMBEDDING_API_KEY", ""); v != "" {
		config.Memory.EmbeddingAPIKey = v
	}
	if v := getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 0); v > 0 {
		config.Memory.EmbeddingDimensions = v
	}
	if v := getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 0); v > 0 {
		config.Memory.EmbeddingCacheSize = v
	}
	if v := getEnv("NORNICDB_SEARCH_MIN_SIMILARITY", ""); v != "" {
		config.Memory.SearchMinSimilarity = getEnvFloat("NORNICDB_SEARCH_MIN_SIMILARITY", 0.5)
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
	if v := getEnvDuration("NORNICDB_KMEANS_CLUSTER_INTERVAL", 0); v > 0 {
		config.Memory.KmeansClusterInterval = v
	}
	if v := getEnvInt("NORNICDB_KMEANS_NUM_CLUSTERS", 0); v >= 0 {
		config.Memory.KmeansNumClusters = v
	}
	if v := getEnv("NORNICDB_DEFAULT_NODE_LABEL", ""); v != "" {
		config.Memory.DefaultNodeLabel = v
	}
	if getEnv("NORNICDB_AUTO_LINKS_ENABLED", "") == "false" {
		config.Memory.AutoLinksEnabled = false
	}
	if v := getEnvFloat("NORNICDB_AUTO_LINKS_THRESHOLD", 0); v > 0 {
		config.Memory.AutoLinksSimilarityThreshold = v
	}
	if v := getEnv("NORNICDB_MEMORY_LIMIT", ""); v != "" {
		runtimeLimit, err := ParseMemoryLimitMB(v)
		if err != nil {
			return fmt.Errorf("invalid NORNICDB_MEMORY_LIMIT value %q: %w", v, err)
		}
		config.Memory.RuntimeLimitStr = v
		config.Memory.RuntimeLimit = runtimeLimit
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
	if v := getEnvInt("NORNICDB_EMBED_WORKER_NUM_WORKERS", 0); v > 0 {
		config.EmbeddingWorker.NumWorkers = v
	}
	if v := getEnvDuration("NORNICDB_EMBED_SCAN_INTERVAL", 0); v > 0 {
		config.EmbeddingWorker.ScanInterval = v
	}
	if v := getEnvDuration("NORNICDB_EMBED_BATCH_DELAY", 0); v > 0 {
		config.EmbeddingWorker.BatchDelay = v
	}
	if v := getEnvDuration("NORNICDB_EMBED_TRIGGER_DEBOUNCE", -1); v >= 0 {
		config.EmbeddingWorker.TriggerDebounceDelay = v
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
	if v := getEnvStringSlice("NORNICDB_EMBEDDING_PROPERTIES_INCLUDE", nil); len(v) > 0 {
		config.EmbeddingWorker.PropertiesInclude = v
	}
	if v := getEnvStringSlice("NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE", nil); len(v) > 0 {
		config.EmbeddingWorker.PropertiesExclude = v
	}
	if v := getEnv("NORNICDB_EMBEDDING_INCLUDE_LABELS", ""); v != "" {
		config.EmbeddingWorker.IncludeLabels = getEnvBool("NORNICDB_EMBEDDING_INCLUDE_LABELS", config.EmbeddingWorker.IncludeLabels)
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
	if v := getEnvStringSlice("NORNICDB_SUBJECT_IDENTIFIER_PROPERTIES", nil); len(v) > 0 {
		config.Compliance.SubjectIdentifierProperties = v
	}
	if v := getEnvStringSlice("NORNICDB_SUBJECT_PSEUDONYMIZE_PROPERTIES", nil); len(v) > 0 {
		config.Compliance.SubjectPseudonymizeProperties = v
	}
	if v := getEnvStringSlice("NORNICDB_SUBJECT_REDACT_PROPERTIES", nil); len(v) > 0 {
		config.Compliance.SubjectRedactProperties = v
	}
	if v := getEnvInt("NORNICDB_RETENTION_SWEEP_INTERVAL", 0); v > 0 {
		config.Retention.SweepIntervalSeconds = v
	}
	if v := getEnvStringSlice("NORNICDB_RETENTION_EXCLUDED_LABELS", nil); len(v) > 0 {
		config.Retention.ExcludedLabels = v
	}
	if v := getEnv("NORNICDB_RETENTION_POLICIES_FILE", ""); v != "" {
		config.Retention.PoliciesFile = v
	}
	if getEnv("NORNICDB_RETENTION_DEFAULT_POLICIES", "") == "true" {
		config.Retention.DefaultPolicies = true
	}
	if v := getEnvInt("NORNICDB_RETENTION_MAX_SWEEP_RECORDS", 0); v > 0 {
		config.Retention.MaxSweepRecords = v
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

	// Observability settings (Phase 1 — OBS-02).
	// OTEL_EXPORTER_OTLP_* env vars are intentionally NOT consumed here —
	// they are read on demand by observability.TracingConfig.OTLPEndpoint()
	// at exporter-init time so OBS-12 precedence (env > YAML) is honored.
	if v := getEnv("NORNICDB_TELEMETRY_LISTEN", ""); v != "" {
		config.Observability.Metrics.Listen = normalizeTelemetryListen(v)
	}
	if v := getEnv("NORNICDB_TELEMETRY_PORT", ""); v != "" && config.Observability.Metrics.Listen == "" {
		config.Observability.Metrics.Listen = ":" + v
	}
	if v := getEnv("NORNICDB_TRACING_ENABLED", ""); v != "" {
		config.Observability.Tracing.Enabled = v == "true"
	}
	if v := getEnv("NORNICDB_PPROF_ENABLED", ""); v != "" {
		config.Observability.Pprof.Enabled = v == "true"
	}
	if v := getEnv("NORNICDB_PPROF_LISTEN", ""); v != "" {
		config.Observability.Pprof.Listen = v
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
	// K-Means clustering (maps to Kalman filtering) - used by macOS menu bar app
	if getEnv("NORNICDB_KMEANS_CLUSTERING_ENABLED", "") == "true" {
		config.Features.KalmanEnabled = true
	}
	// Auto-TLP (Topological Link prediction) - used by macOS menu bar app
	if getEnv("NORNICDB_AUTO_TLP_ENABLED", "") == "true" {
		config.Features.TopologyAutoIntegrationEnabled = true
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_HEIMDALL_ENABLED"); ok {
		config.Features.HeimdallEnabled = v
	}
	if v := getEnv("NORNICDB_HEIMDALL_MODEL", ""); v != "" {
		config.Features.HeimdallModel = v
	}
	if v := getEnv("NORNICDB_HEIMDALL_PROVIDER", ""); v != "" {
		config.Features.HeimdallProvider = v
	}
	if v := getEnv("NORNICDB_HEIMDALL_API_URL", ""); v != "" {
		config.Features.HeimdallAPIURL = v
	}
	if v := getEnv("NORNICDB_HEIMDALL_API_KEY", ""); v != "" {
		config.Features.HeimdallAPIKey = v
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
	if v, ok := envutil.LookupBoolLoose("NORNICDB_HEIMDALL_ANOMALY_DETECTION"); ok {
		config.Features.HeimdallAnomalyDetection = v
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS"); ok {
		config.Features.HeimdallRuntimeDiagnosis = v
	}
	if v, ok := envutil.LookupBoolLoose("NORNICDB_HEIMDALL_MEMORY_CURATION"); ok {
		config.Features.HeimdallMemoryCuration = v
	}
	// MCP tools in agentic loop (NORNICDB_HEIMDALL_MCP_ENABLE, NORNICDB_HEIMDALL_MCP_TOOLS)
	if v := os.Getenv("NORNICDB_HEIMDALL_MCP_ENABLE"); v != "" {
		config.Features.HeimdallMCPEnable = v == "true" || v == "1"
	}
	if v, ok := os.LookupEnv("NORNICDB_HEIMDALL_MCP_TOOLS"); ok {
		if strings.TrimSpace(v) == "" {
			config.Features.HeimdallMCPTools = []string{}
		} else {
			parts := strings.Split(v, ",")
			config.Features.HeimdallMCPTools = make([]string, 0, len(parts))
			for _, p := range parts {
				if name := strings.TrimSpace(p); name != "" {
					config.Features.HeimdallMCPTools = append(config.Features.HeimdallMCPTools, name)
				}
			}
		}
	}
	if getEnv("NORNICDB_SEARCH_RERANK_ENABLED", "") == "true" {
		config.Features.SearchRerankEnabled = true
	}
	if v := getEnv("NORNICDB_SEARCH_RERANK_PROVIDER", ""); v != "" {
		config.Features.SearchRerankProvider = strings.TrimSpace(strings.ToLower(v))
	}
	if v := getEnv("NORNICDB_SEARCH_RERANK_MODEL", ""); v != "" {
		config.Features.SearchRerankModel = v
	}
	if v := getEnv("NORNICDB_SEARCH_RERANK_API_URL", ""); v != "" {
		config.Features.SearchRerankAPIURL = v
	}
	if v := getEnv("NORNICDB_SEARCH_RERANK_API_KEY", ""); v != "" {
		config.Features.SearchRerankAPIKey = v
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

	// Qdrant gRPC compatibility layer
	if v := getEnv("NORNICDB_QDRANT_GRPC_ENABLED", ""); v != "" {
		config.Features.QdrantGRPCEnabled = v == "true" || v == "1"
	}
	if v := getEnv("NORNICDB_QDRANT_GRPC_LISTEN_ADDR", ""); v != "" {
		config.Features.QdrantGRPCListenAddr = v
	}
	if v := getEnvInt("NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM", 0); v > 0 {
		config.Features.QdrantGRPCMaxVectorDim = v
	}
	if v := getEnvInt("NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS", 0); v > 0 {
		config.Features.QdrantGRPCMaxBatchPoints = v
	}
	if v := getEnvInt("NORNICDB_QDRANT_GRPC_MAX_TOP_K", 0); v > 0 {
		config.Features.QdrantGRPCMaxTopK = v
	}

	return nil
}

// ApplyEnvVars applies environment variable overrides to an existing config.
// This is the exported version for use in main.go.
func ApplyEnvVars(config *Config) error {
	return applyEnvVars(config)
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
	if yamlCfg.Server.BoltServerAnnouncement != "" {
		config.Server.BoltServerAnnouncement = yamlCfg.Server.BoltServerAnnouncement
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
	if yamlCfg.Database.WALAutoCompactionEnabled != nil {
		config.Database.WALAutoCompactionEnabled = *yamlCfg.Database.WALAutoCompactionEnabled
	}
	if yamlCfg.Database.WALRetentionMaxSegments > 0 {
		config.Database.WALRetentionMaxSegments = yamlCfg.Database.WALRetentionMaxSegments
	}
	if yamlCfg.Database.WALRetentionMaxAge != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.WALRetentionMaxAge); err == nil {
			config.Database.WALRetentionMaxAge = d
		}
	}
	if yamlCfg.Database.WALRetentionLedgerDefaults {
		config.Database.WALRetentionLedgerDefaults = true
	}
	if config.Database.WALRetentionLedgerDefaults &&
		config.Database.WALRetentionMaxSegments == 0 &&
		config.Database.WALRetentionMaxAge == 0 {
		config.Database.WALRetentionMaxSegments = 24
		config.Database.WALRetentionMaxAge = 7 * 24 * time.Hour
	}
	if yamlCfg.Database.WALSnapshotRetentionMaxCount > 0 {
		config.Database.WALSnapshotRetentionMaxCount = yamlCfg.Database.WALSnapshotRetentionMaxCount
	}
	if yamlCfg.Database.WALSnapshotRetentionMaxAge != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.WALSnapshotRetentionMaxAge); err == nil {
			config.Database.WALSnapshotRetentionMaxAge = d
		}
	}
	// Encryption settings
	if yamlCfg.Database.EncryptionEnabled {
		config.Database.EncryptionEnabled = true
	}
	// Only use YAML encryption password if it's a real value (not a placeholder)
	if yamlCfg.Database.EncryptionPassword != "" &&
		yamlCfg.Database.EncryptionPassword != "[stored-in-keychain]" &&
		!strings.Contains(yamlCfg.Database.EncryptionPassword, "stored-in-keychain") {
		config.Database.EncryptionPassword = yamlCfg.Database.EncryptionPassword
	}
	if yamlCfg.Database.EncryptionProvider != "" {
		config.Database.EncryptionProvider = strings.ToLower(strings.TrimSpace(yamlCfg.Database.EncryptionProvider))
	}
	if yamlCfg.Database.EncryptionKeyURI != "" {
		config.Database.EncryptionKeyURI = strings.TrimSpace(yamlCfg.Database.EncryptionKeyURI)
	}
	if yamlCfg.Database.EncryptionMasterKey != "" {
		config.Database.EncryptionMasterKey = strings.TrimSpace(yamlCfg.Database.EncryptionMasterKey)
	}
	if yamlCfg.Database.EncryptionAWSRegion != "" {
		config.Database.EncryptionAWSRegion = strings.TrimSpace(yamlCfg.Database.EncryptionAWSRegion)
	}
	if yamlCfg.Database.EncryptionAWSKMSKeyID != "" {
		config.Database.EncryptionAWSKMSKeyID = strings.TrimSpace(yamlCfg.Database.EncryptionAWSKMSKeyID)
	}
	if yamlCfg.Database.EncryptionAWSEndpoint != "" {
		config.Database.EncryptionAWSEndpoint = strings.TrimSpace(yamlCfg.Database.EncryptionAWSEndpoint)
	}
	if yamlCfg.Database.EncryptionAWSRoleARN != "" {
		config.Database.EncryptionAWSRoleARN = strings.TrimSpace(yamlCfg.Database.EncryptionAWSRoleARN)
	}
	if yamlCfg.Database.EncryptionAWSRoleSessionName != "" {
		config.Database.EncryptionAWSRoleSessionName = strings.TrimSpace(yamlCfg.Database.EncryptionAWSRoleSessionName)
	}
	if yamlCfg.Database.EncryptionAWSAccessKey != "" {
		config.Database.EncryptionAWSAccessKey = strings.TrimSpace(yamlCfg.Database.EncryptionAWSAccessKey)
	}
	if yamlCfg.Database.EncryptionAWSSecretKey != "" {
		config.Database.EncryptionAWSSecretKey = strings.TrimSpace(yamlCfg.Database.EncryptionAWSSecretKey)
	}
	if yamlCfg.Database.EncryptionAWSSessionToken != "" {
		config.Database.EncryptionAWSSessionToken = strings.TrimSpace(yamlCfg.Database.EncryptionAWSSessionToken)
	}
	if yamlCfg.Database.EncryptionAWSSharedCredsFilename != "" {
		config.Database.EncryptionAWSSharedCredsFilename = strings.TrimSpace(yamlCfg.Database.EncryptionAWSSharedCredsFilename)
	}
	if yamlCfg.Database.EncryptionAWSSharedCredsProfile != "" {
		config.Database.EncryptionAWSSharedCredsProfile = strings.TrimSpace(yamlCfg.Database.EncryptionAWSSharedCredsProfile)
	}
	if yamlCfg.Database.EncryptionAWSWebIdentityTokenFile != "" {
		config.Database.EncryptionAWSWebIdentityTokenFile = strings.TrimSpace(yamlCfg.Database.EncryptionAWSWebIdentityTokenFile)
	}
	if yamlCfg.Database.EncryptionAzureVaultName != "" {
		config.Database.EncryptionAzureVaultName = strings.TrimSpace(yamlCfg.Database.EncryptionAzureVaultName)
	}
	if yamlCfg.Database.EncryptionAzureKeyName != "" {
		config.Database.EncryptionAzureKeyName = strings.TrimSpace(yamlCfg.Database.EncryptionAzureKeyName)
	}
	if yamlCfg.Database.EncryptionAzureTenantID != "" {
		config.Database.EncryptionAzureTenantID = strings.TrimSpace(yamlCfg.Database.EncryptionAzureTenantID)
	}
	if yamlCfg.Database.EncryptionAzureClientID != "" {
		config.Database.EncryptionAzureClientID = strings.TrimSpace(yamlCfg.Database.EncryptionAzureClientID)
	}
	if yamlCfg.Database.EncryptionAzureClientSecret != "" {
		config.Database.EncryptionAzureClientSecret = strings.TrimSpace(yamlCfg.Database.EncryptionAzureClientSecret)
	}
	if yamlCfg.Database.EncryptionAzureEnvironment != "" {
		config.Database.EncryptionAzureEnvironment = strings.TrimSpace(yamlCfg.Database.EncryptionAzureEnvironment)
	}
	if yamlCfg.Database.EncryptionAzureResource != "" {
		config.Database.EncryptionAzureResource = strings.TrimSpace(yamlCfg.Database.EncryptionAzureResource)
	}
	if yamlCfg.Database.EncryptionGCPProject != "" {
		config.Database.EncryptionGCPProject = strings.TrimSpace(yamlCfg.Database.EncryptionGCPProject)
	}
	if yamlCfg.Database.EncryptionGCPLocation != "" {
		config.Database.EncryptionGCPLocation = strings.TrimSpace(yamlCfg.Database.EncryptionGCPLocation)
	}
	if yamlCfg.Database.EncryptionGCPKeyRing != "" {
		config.Database.EncryptionGCPKeyRing = strings.TrimSpace(yamlCfg.Database.EncryptionGCPKeyRing)
	}
	if yamlCfg.Database.EncryptionGCPKeyName != "" {
		config.Database.EncryptionGCPKeyName = strings.TrimSpace(yamlCfg.Database.EncryptionGCPKeyName)
	}
	if yamlCfg.Database.EncryptionGCPCredentialsFile != "" {
		config.Database.EncryptionGCPCredentialsFile = strings.TrimSpace(yamlCfg.Database.EncryptionGCPCredentialsFile)
	}
	if yamlCfg.Database.EncryptionAuditLogPath != "" {
		config.Database.EncryptionAuditLogPath = strings.TrimSpace(yamlCfg.Database.EncryptionAuditLogPath)
	}
	if yamlCfg.Database.EncryptionAuditSignEvents != nil {
		config.Database.EncryptionAuditSignEvents = *yamlCfg.Database.EncryptionAuditSignEvents
	}
	if yamlCfg.Database.EncryptionAuditSignKey != "" {
		config.Database.EncryptionAuditSignKey = strings.TrimSpace(yamlCfg.Database.EncryptionAuditSignKey)
	}
	if yamlCfg.Database.EncryptionRotationEnabled != nil {
		config.Database.EncryptionRotationEnabled = *yamlCfg.Database.EncryptionRotationEnabled
	}
	if yamlCfg.Database.EncryptionRotationInterval != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(yamlCfg.Database.EncryptionRotationInterval)); err == nil {
			config.Database.EncryptionRotationInterval = d
		}
	}
	if yamlCfg.Database.BadgerNodeCacheMaxEntries > 0 {
		config.Database.BadgerNodeCacheMaxEntries = yamlCfg.Database.BadgerNodeCacheMaxEntries
	}
	if yamlCfg.Database.BadgerEdgeTypeCacheMaxTypes > 0 {
		config.Database.BadgerEdgeTypeCacheMaxTypes = yamlCfg.Database.BadgerEdgeTypeCacheMaxTypes
	}
	if yamlCfg.Database.MVCCRetentionMaxVersions > 0 {
		config.Database.MVCCRetentionMaxVersions = yamlCfg.Database.MVCCRetentionMaxVersions
	}
	if yamlCfg.Database.MVCCRetentionTTL != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.MVCCRetentionTTL); err == nil {
			config.Database.MVCCRetentionTTL = d
		}
	}
	if yamlCfg.Database.IDFreelistTTL != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.IDFreelistTTL); err == nil {
			config.Database.IDFreelistTTL = d
		}
	}
	if yamlCfg.Database.MVCCLifecycleEnabled != nil {
		config.Database.MVCCLifecycleEnabled = *yamlCfg.Database.MVCCLifecycleEnabled
	}
	if yamlCfg.Database.MVCCLifecycleCycleInterval != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.MVCCLifecycleCycleInterval); err == nil {
			config.Database.MVCCLifecycleCycleInterval = d
		}
	}
	if yamlCfg.Database.MVCCLifecycleMaxSnapshotAge != "" {
		if d, err := time.ParseDuration(yamlCfg.Database.MVCCLifecycleMaxSnapshotAge); err == nil {
			config.Database.MVCCLifecycleMaxSnapshotAge = d
		}
	}
	if yamlCfg.Database.MVCCLifecycleMaxChainCap > 0 {
		config.Database.MVCCLifecycleMaxChainCap = yamlCfg.Database.MVCCLifecycleMaxChainCap
	}
	if yamlCfg.Database.PersistSearchIndexes {
		config.Database.PersistSearchIndexes = true
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
	// Only use YAML JWT secret if it's a real value (not a placeholder)
	// The placeholder "[stored-in-keychain]" indicates the secret is passed via env var
	if yamlCfg.Auth.JWTSecret != "" &&
		yamlCfg.Auth.JWTSecret != "[stored-in-keychain]" &&
		!strings.Contains(yamlCfg.Auth.JWTSecret, "stored-in-keychain") {
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
	if yamlCfg.Embedding.APIKey != "" {
		config.Memory.EmbeddingAPIKey = yamlCfg.Embedding.APIKey
	}
	if yamlCfg.Embedding.Dimensions > 0 {
		config.Memory.EmbeddingDimensions = yamlCfg.Embedding.Dimensions
	}
	if yamlCfg.Embedding.CacheSize > 0 {
		config.Memory.EmbeddingCacheSize = yamlCfg.Embedding.CacheSize
	}
	if yamlCfg.Embedding.MinSimilarity > 0 {
		config.Memory.SearchMinSimilarity = yamlCfg.Embedding.MinSimilarity
	}

	// === Memory Settings ===
	if yamlCfg.Memory.DecayEnabled {
		config.Memory.DecayEnabled = true
	}
	if yamlCfg.Memory.DecayIntervalSeconds > 0 {
		config.Memory.DecayInterval = time.Duration(yamlCfg.Memory.DecayIntervalSeconds) * time.Second
	}
	if yamlCfg.Memory.VisibilityThreshold > 0 {
		config.Memory.VisibilityThreshold = yamlCfg.Memory.VisibilityThreshold
	}
	if yamlCfg.Memory.AccessFlushBufferSize > 0 {
		config.Memory.AccessFlushBufferSize = yamlCfg.Memory.AccessFlushBufferSize
	}
	if yamlCfg.Memory.AutoLinksEnabled {
		config.Memory.AutoLinksEnabled = true
	}
	if yamlCfg.Memory.AutoLinksSimilarityThreshold > 0 {
		config.Memory.AutoLinksSimilarityThreshold = yamlCfg.Memory.AutoLinksSimilarityThreshold
	}
	if yamlCfg.Memory.RuntimeLimit != "" {
		runtimeLimit, err := ParseMemoryLimitMB(yamlCfg.Memory.RuntimeLimit)
		if err != nil {
			return nil, fmt.Errorf("invalid memory.runtime_limit value %q: %w", yamlCfg.Memory.RuntimeLimit, err)
		}
		config.Memory.RuntimeLimitStr = yamlCfg.Memory.RuntimeLimit
		config.Memory.RuntimeLimit = runtimeLimit
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
	if yamlCfg.EmbeddingWorker.TriggerDebounce != "" {
		if d, err := time.ParseDuration(yamlCfg.EmbeddingWorker.TriggerDebounce); err == nil {
			config.EmbeddingWorker.TriggerDebounceDelay = d
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
	if len(yamlCfg.EmbeddingWorker.PropertiesInclude) > 0 {
		config.EmbeddingWorker.PropertiesInclude = yamlCfg.EmbeddingWorker.PropertiesInclude
	}
	if len(yamlCfg.EmbeddingWorker.PropertiesExclude) > 0 {
		config.EmbeddingWorker.PropertiesExclude = yamlCfg.EmbeddingWorker.PropertiesExclude
	}
	if yamlCfg.EmbeddingWorker.IncludeLabels != nil {
		config.EmbeddingWorker.IncludeLabels = *yamlCfg.EmbeddingWorker.IncludeLabels
	}

	// === Feature Flags ===
	if yamlCfg.Kmeans.Enabled {
		config.Features.KalmanEnabled = true
	}

	// Qdrant gRPC feature flags (YAML).
	if yamlCfg.Features.QdrantGRPCEnabled {
		config.Features.QdrantGRPCEnabled = true
	}
	if yamlCfg.Features.QdrantGRPCListenAddr != "" {
		config.Features.QdrantGRPCListenAddr = yamlCfg.Features.QdrantGRPCListenAddr
	}
	if yamlCfg.Features.QdrantGRPCMaxVectorDim > 0 {
		config.Features.QdrantGRPCMaxVectorDim = yamlCfg.Features.QdrantGRPCMaxVectorDim
	}
	if yamlCfg.Features.QdrantGRPCMaxBatchPoints > 0 {
		config.Features.QdrantGRPCMaxBatchPoints = yamlCfg.Features.QdrantGRPCMaxBatchPoints
	}
	if yamlCfg.Features.QdrantGRPCMaxTopK > 0 {
		config.Features.QdrantGRPCMaxTopK = yamlCfg.Features.QdrantGRPCMaxTopK
	}
	if yamlCfg.Features.QdrantGRPCRBAC.Methods != nil {
		config.Features.QdrantGRPCMethodPermissions = yamlCfg.Features.QdrantGRPCRBAC.Methods
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
	if yamlCfg.Heimdall.Provider != "" {
		config.Features.HeimdallProvider = yamlCfg.Heimdall.Provider
	}
	if yamlCfg.Heimdall.APIURL != "" {
		config.Features.HeimdallAPIURL = yamlCfg.Heimdall.APIURL
	}
	if yamlCfg.Heimdall.APIKey != "" {
		config.Features.HeimdallAPIKey = yamlCfg.Heimdall.APIKey
	}
	if yamlCfg.Heimdall.GPULayers != nil {
		config.Features.HeimdallGPULayers = *yamlCfg.Heimdall.GPULayers
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
	config.Features.HeimdallMCPEnable = yamlCfg.Heimdall.MCPEnable
	if yamlCfg.Heimdall.MCPTools != nil {
		config.Features.HeimdallMCPTools = yamlCfg.Heimdall.MCPTools
	}

	// Search rerank settings
	if yamlCfg.SearchRerank.Enabled {
		config.Features.SearchRerankEnabled = true
	}
	if yamlCfg.SearchRerank.Provider != "" {
		config.Features.SearchRerankProvider = strings.TrimSpace(strings.ToLower(yamlCfg.SearchRerank.Provider))
	}
	if yamlCfg.SearchRerank.Model != "" {
		config.Features.SearchRerankModel = yamlCfg.SearchRerank.Model
	}
	if yamlCfg.SearchRerank.APIURL != "" {
		config.Features.SearchRerankAPIURL = yamlCfg.SearchRerank.APIURL
	}
	if yamlCfg.SearchRerank.APIKey != "" {
		config.Features.SearchRerankAPIKey = yamlCfg.SearchRerank.APIKey
	}

	// Qdrant gRPC settings
	if yamlCfg.QdrantGRPC.Enabled {
		config.Features.QdrantGRPCEnabled = true
	}
	if yamlCfg.QdrantGRPC.ListenAddr != "" {
		config.Features.QdrantGRPCListenAddr = yamlCfg.QdrantGRPC.ListenAddr
	}
	if yamlCfg.QdrantGRPC.MaxVectorDim > 0 {
		config.Features.QdrantGRPCMaxVectorDim = yamlCfg.QdrantGRPC.MaxVectorDim
	}
	if yamlCfg.QdrantGRPC.MaxBatchPoints > 0 {
		config.Features.QdrantGRPCMaxBatchPoints = yamlCfg.QdrantGRPC.MaxBatchPoints
	}
	if yamlCfg.QdrantGRPC.MaxTopK > 0 {
		config.Features.QdrantGRPCMaxTopK = yamlCfg.QdrantGRPC.MaxTopK
	}
	if yamlCfg.QdrantGRPC.RBAC.Methods != nil {
		config.Features.QdrantGRPCMethodPermissions = yamlCfg.QdrantGRPC.RBAC.Methods
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
	if len(yamlCfg.Compliance.SubjectIdentifierProperties) > 0 {
		config.Compliance.SubjectIdentifierProperties = yamlCfg.Compliance.SubjectIdentifierProperties
	}
	if len(yamlCfg.Compliance.SubjectPseudonymizeProperties) > 0 {
		config.Compliance.SubjectPseudonymizeProperties = yamlCfg.Compliance.SubjectPseudonymizeProperties
	}
	if len(yamlCfg.Compliance.SubjectRedactProperties) > 0 {
		config.Compliance.SubjectRedactProperties = yamlCfg.Compliance.SubjectRedactProperties
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

	// === Retention Settings ===
	if yamlCfg.Retention.SweepInterval > 0 {
		config.Retention.SweepIntervalSeconds = yamlCfg.Retention.SweepInterval
	}
	if len(yamlCfg.Retention.ExcludedLabels) > 0 {
		config.Retention.ExcludedLabels = yamlCfg.Retention.ExcludedLabels
	}
	if yamlCfg.Retention.PoliciesFile != "" {
		config.Retention.PoliciesFile = yamlCfg.Retention.PoliciesFile
	}
	if yamlCfg.Retention.DefaultPolicies {
		config.Retention.DefaultPolicies = true
	}
	if yamlCfg.Retention.MaxSweepRecords > 0 {
		config.Retention.MaxSweepRecords = yamlCfg.Retention.MaxSweepRecords
	}
	if len(yamlCfg.Retention.Policies) > 0 {
		config.Retention.Policies = append([]RetentionPolicyConfig(nil), yamlCfg.Retention.Policies...)
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

	// === Observability Settings (Phase 1 — OBS-02) ===
	if yamlCfg.Observability.Metrics.Enabled != nil {
		config.Observability.Metrics.Enabled = *yamlCfg.Observability.Metrics.Enabled
	}
	if yamlCfg.Observability.Metrics.Listen != "" {
		config.Observability.Metrics.Listen = normalizeTelemetryListen(yamlCfg.Observability.Metrics.Listen)
	}
	// Phase 5 R-02: defer dereference of *bool. The Phase 5 startup hook in
	// cmd/nornicdb/main.go calls ResolveTenantLabels(TenantLabelsExplicit, ...)
	// which enforces the precedence chain (explicit > autodetect > default).
	// Storing the *bool sentinel — instead of dereferencing into the resolved
	// bool here — allows YAML "explicit false" to win over autodetect on K8s.
	config.Observability.Metrics.TenantLabelsExplicit = yamlCfg.Observability.Metrics.TenantLabelsEnabled
	if yamlCfg.Observability.Tracing.Enabled != nil {
		config.Observability.Tracing.Enabled = *yamlCfg.Observability.Tracing.Enabled
	}
	if yamlCfg.Observability.Tracing.Endpoint != "" {
		config.Observability.Tracing.Endpoint = yamlCfg.Observability.Tracing.Endpoint
	}
	if yamlCfg.Observability.Tracing.Protocol != "" {
		config.Observability.Tracing.Protocol = yamlCfg.Observability.Tracing.Protocol
	}
	if yamlCfg.Observability.Tracing.Insecure != nil {
		config.Observability.Tracing.Insecure = *yamlCfg.Observability.Tracing.Insecure
	}
	if yamlCfg.Observability.Tracing.Timeout != "" {
		if d, err := time.ParseDuration(yamlCfg.Observability.Tracing.Timeout); err == nil {
			config.Observability.Tracing.Timeout = d
		}
	}
	if yamlCfg.Observability.Pprof.Enabled != nil {
		config.Observability.Pprof.Enabled = *yamlCfg.Observability.Pprof.Enabled
	}
	if yamlCfg.Observability.Pprof.Listen != "" {
		config.Observability.Pprof.Listen = yamlCfg.Observability.Pprof.Listen
	}

	// === Plugins Settings ===
	if yamlCfg.Plugins.Dir != "" {
		config.Server.PluginsDir = yamlCfg.Plugins.Dir
	}
	if yamlCfg.Plugins.HeimdallDir != "" {
		config.Server.HeimdallPluginsDir = yamlCfg.Plugins.HeimdallDir
	}

	// Step 3: Apply environment variable overrides (higher priority than config file)
	if err := applyEnvVars(config); err != nil {
		return nil, err
	}

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

	// Priority 0: explicit env override (common in Docker/K8s)
	// Env: NORNICDB_CONFIG=/config/nornicdb.yaml
	if v := strings.TrimSpace(os.Getenv("NORNICDB_CONFIG")); v != "" {
		candidates = append(candidates, v)
	}

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

	// Priority 3.5: Container-friendly mount point (used by docs/images)
	candidates = append(candidates,
		"/config/nornicdb.yaml",
		"/config/config.yaml",
	)

	// Priority 4: OS-specific user config paths
	if home, err := os.UserHomeDir(); err == nil {
		// macOS
		candidates = append(candidates, filepath.Join(home, "Library", "Application Support", "NornicDB", "config.yaml"))
		// Linux/Unix XDG standard
		candidates = append(candidates, filepath.Join(home, ".config", "nornicdb", "config.yaml"))
	}

	for _, path := range candidates {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// Helper functions for environment variable parsing

func getEnv(key, defaultVal string) string {
	return envutil.Get(key, defaultVal)
}

func getEnvInt(key string, defaultVal int) int {
	return envutil.GetInt(key, defaultVal)
}

func getEnvFloat(key string, defaultVal float64) float64 {
	return envutil.GetFloat(key, defaultVal)
}

func getEnvBool(key string, defaultVal bool) bool {
	return envutil.GetBoolLoose(key, defaultVal)
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	return envutil.GetDurationOrSeconds(key, defaultVal)
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

// normalizeTelemetryListen accepts either a full bind address (":9090",
// "127.0.0.1:9090") or a numeric port ("9090"). Numeric-only values are
// normalized to ":<port>" for net.Listen compatibility.
func normalizeTelemetryListen(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return v
	}
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return v
		}
	}
	return ":" + v
}

func generateDefaultSecret() string {
	// In production, this should be explicitly set
	return "CHANGE_ME_IN_PRODUCTION_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// ParseMemoryLimitMB parses a memory limit value expressed in megabytes.
// Valid examples: "0" (unlimited), "500" (500 MB).
// Invalid examples: "500MB", "1GB", "unlimited".
func ParseMemoryLimitMB(s string) (int64, error) {
	const bytesPerMB int64 = 1024 * 1024

	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, fmt.Errorf("must be a whole number of megabytes (e.g., 500) or 0 for unlimited")
	}

	mb, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be a whole number of megabytes (e.g., 500) or 0 for unlimited")
	}
	if mb < 0 {
		return 0, fmt.Errorf("must be non-negative")
	}
	if mb > math.MaxInt64/bytesPerMB {
		return 0, fmt.Errorf("value is too large")
	}

	return mb * bytesPerMB, nil
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
