// Package nornicdb provides the main API for embedded NornicDB usage.
//
// This package implements the core NornicDB graph database API, providing a
// high-level interface for nodes, edges, search, automatic relationship
// inference, knowledge-layer decay policies, and Cypher query execution.
//
// All data is represented as generic labeled property graph elements:
//   - Nodes have labels ([]string) and properties (map[string]any)
//   - Edges connect nodes with a type and optional properties
//   - Knowledge-layer policies control visibility and decay per label/type
//
// Example Usage:
//
//	db, err := nornicdb.Open("./data", nornicdb.DefaultConfig())
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Create a node
//	node, err := db.CreateNode(ctx, []string{"Concept"}, map[string]any{
//		"content": "Machine learning is a subset of artificial intelligence",
//		"title":   "ML Definition",
//		"tags":    []string{"AI", "ML"},
//	})
//
//	// Execute Cypher queries
//	result, err := db.Cypher(ctx, "MATCH (n:Concept) RETURN n.title", nil)
package nornicdb

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/encryption"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/storage/lifecycle"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Errors returned by DB operations.
var (
	ErrNotFound     = errors.New("not found")
	ErrInvalidID    = errors.New("invalid ID")
	ErrClosed       = errors.New("database is closed")
	ErrInvalidInput = errors.New("invalid input")

	// ErrQueryEmbeddingDimensionMismatch is returned when the query is embedded
	// with global embedder dimensions that do not match the database's resolved
	// embedding dimensions (e.g. per-DB override), which would cause vector
	// search to return no results. Use EmbedQueryForDB and align config or use
	// the same dimensions for the index and query.
	ErrQueryEmbeddingDimensionMismatch = errors.New("query embedding dimension mismatch")
)

// generateID creates a unique UUID.
func generateID() string {
	return uuid.New().String()
}

// Config holds NornicDB database configuration options.
//
// The configuration controls all aspects of the database including storage,
// embeddings, knowledge-layer decay policies, automatic relationship inference, and server ports.
//
// Example:
//
//	// Production configuration
//	config := &nornicdb.Config{
//		DataDir:                      "/var/lib/nornicdb",
//		EmbeddingProvider:            "openai",
//		EmbeddingAPIURL:              "https://api.openai.com/v1",
//		EmbeddingModel:               "text-embedding-3-large",
//		EmbeddingDimensions:          3072,
//		DecayEnabled:                 true,
//		DecayInterval:     30 * time.Minute,
//		VisibilityThreshold:        0.01, // Archive at 1%
//		AutoLinksEnabled:             true,
//		AutoLinksSimilarityThreshold: 0.85, // Higher precision
//		AutoLinksCoAccessWindow:      60 * time.Second,
//		BoltPort:                     7687,
//		HTTPPort:                     7474,
//	}
//
//	// Development configuration
//	config = nornicdb.DefaultConfig()
//	config.DecayEnabled = false // Disable for testing
type Config = nornicConfig.Config

// DefaultConfig returns sensible default configuration for NornicDB.
//
// The defaults are optimized for development and small-scale deployments:
//   - Local Ollama for embeddings (bge-m3 model)
//   - Knowledge-layer decay policies enabled
//   - Auto-linking enabled with 0.82 similarity threshold
//   - Standard Neo4j ports (7687 Bolt, 7474 HTTP)
//
// Example:
//
//	config := nornicdb.DefaultConfig()
//	// Customize as needed
//	config.EmbeddingModel = "nomic-embed-text"
//	config.VisibilityThreshold = 0.1 // Archive at 10%
//
//	db, err := nornicdb.Open("./data", config)
func DefaultConfig() *Config {
	return nornicConfig.LoadDefaults()
}

// DB represents a NornicDB database instance with all core functionality.
//
// The DB provides a high-level API for creating nodes and edges, executing
// Cypher queries, and performing hybrid search. It coordinates between storage,
// knowledge-layer decay policies, relationship inference, and search services.
//
// Example:
//
//	db, err := nornicdb.Open("./data", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	node, _ := db.CreateNode(ctx, []string{"Concept"}, map[string]any{"content": "Important information"})
//
// Thread Safety:
//
//	All methods are thread-safe and can be called concurrently.
type DB struct {
	config *Config
	mu     sync.RWMutex
	closed bool

	// Internal components
	storage           storage.Engine // Namespaced storage for default database (all DB operations use this)
	baseStorage       storage.Engine // Underlying storage chain (must be closed to release Badger/WAL locks)
	wal               *storage.WAL   // Write-ahead log for durability
	accessAccumulator *knowledgepolicy.AccessAccumulator
	accessFlusher     *knowledgepolicy.AccessFlusher
	cypherExecutor    *cypher.StorageExecutor
	gpuManagerMu      sync.RWMutex
	gpuManager        interface{} // *gpu.Manager - interface to avoid circular import

	// Search services (per database) using pre-computed embeddings.
	embeddingDims       int     // Effective vector index dimensions in use
	searchMinSimilarity float64 // Default MinSimilarity threshold for search
	searchServicesMu    sync.RWMutex
	searchServices      map[string]*dbSearchService
	// searchServiceCreationMu serializes the "create and insert" path in getOrCreateSearchService
	// so only one goroutine creates a service per dbName at a time, avoiding RWMutex contention deadlock.
	searchServiceCreationMu sync.Mutex
	searchReranker          search.Reranker // Optional Stage-2 reranker for hybrid search

	// Per-database inference engines (topology, Kalman, etc.).
	inferenceServicesMu sync.RWMutex
	inferenceServices   map[string]*inference.Engine

	// Async embedding queue for auto-generating embeddings
	embedQueue        *EmbedQueue
	embedWorkerConfig *EmbedWorkerConfig // Configurable via ENV vars
	embedQueueYieldFn func() bool

	// K-means clustering timer (runs on schedule instead of trigger)
	clusterTicker           *time.Ticker
	clusterTickerStop       chan struct{}
	lastClusteredEmbedCount int // Track embedding count at last clustering

	// Encryption flag - when true, all data is encrypted at BadgerDB level
	encryptionEnabled bool

	// Background goroutine tracking
	bgWg sync.WaitGroup

	// buildCtx is cancelled when the DB closes so startup index build stops promptly.
	buildCtx    context.Context
	buildCancel context.CancelFunc

	// Replication / HA (optional; enabled when NORNICDB_CLUSTER_MODE != standalone)
	replicator         replication.Replicator
	replicationAdapter *replication.StorageAdapter
	replicationTrans   replication.Transport

	// Optional: when set, EmbeddingCount() aggregates across all DBs returned by this provider (used by server with multi-db).
	allDatabasesProvider func() []DatabaseAndStorage

	// Optional: when set, per-database config overrides are applied for search/embed (dims, minSimilarity).
	// Server wires this when system DB and DbConfigStore are available.
	// Use dbConfigResolverMu (not db.mu) so getOrCreateSearchService does not contend with other db.mu users and avoid deadlock.
	dbConfigResolverMu sync.RWMutex
	dbConfigResolver   DbConfigResolver

	// Per-DB embedder registry: keyed by embedConfigKey(cfg). Used by EmbedQueryForDB when embedConfigForDB is set.
	embedderRegistryMu sync.RWMutex
	embedderRegistry   map[string]embed.Embedder
	defaultEmbedKey    string // key for the embedder set via SetEmbedder (global default)
	// embedderFactory allows tests to inject deterministic embedder creation behavior.
	// Production uses embed.NewEmbedder.
	embedderFactory func(cfg *embed.Config) (embed.Embedder, error)
	// in-flight embedder creations keyed by embedConfigKey(cfg), used to avoid duplicate
	// expensive model loads under concurrent query traffic.
	embedderCreateMu sync.Mutex
	embedderCreate   map[string]chan struct{}
	// embedConfigForDB returns resolved embed config for a database; when set, EmbedQueryForDB uses the registry.
	embedConfigForDB func(dbName string) (*embed.Config, error)

	// Optional: when set, getOrCreateSearchService uses this to get the reranker for a database (enables per-DB reranker).
	rerankerResolver func(dbName string) search.Reranker

	lifecycleManager  *lifecycle.MVCCLifecycleManager
	retentionManager  *retention.Manager
	onRetentionAction func(action, recordID, category string)
}

// DbConfigResolver returns effective embedding dimensions, search min similarity, and BM25 engine for a database.
// When set, getOrCreateSearchService uses these instead of the global db.config values.
// Return ("", 0, 0) values to use global defaults for that DB.
type DbConfigResolver func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string)

// embedConfigKey returns a stable key for the embedder registry from an embed config.
func embedConfigKey(cfg *embed.Config) string {
	if cfg == nil {
		return ""
	}
	gpuLayers := cfg.GPULayers
	// For local GGUF, GPULayers=0 means "use library default" which is currently
	// equivalent to auto/all offload (-1). Normalize to keep registry keys stable.
	if strings.EqualFold(strings.TrimSpace(cfg.Provider), "local") && gpuLayers == 0 {
		gpuLayers = -1
	}
	return cfg.Provider + "|" + cfg.Model + "|" + strconv.Itoa(cfg.Dimensions) + "|" +
		cfg.APIURL + "|" + cfg.APIKey + "|" + cfg.ModelsDir + "|" + strconv.Itoa(gpuLayers)
}

// DatabaseAndStorage pairs a database name with its storage engine.
// Used by SetAllDatabasesProvider so EmbeddingCount() can aggregate across all databases.
type DatabaseAndStorage struct {
	Name        string
	Storage     storage.Engine
	IsComposite bool
}

// Open opens or creates a NornicDB database at the specified directory.
//
// This initializes all database components including storage, decay management,
// relationship inference, and search services based on the configuration. The
// database is ready for use immediately after opening.
//
// # Initialization Steps
//
// The function performs the following initialization in order:
//  1. Applies DefaultConfig() if config is nil
//  2. Opens or creates persistent storage (BadgerDB) if dataDir provided
//  3. Initializes Cypher query executor
//  4. Sets up knowledge-layer decay policies (if enabled in config)
//  5. Configures relationship inference engine (if enabled)
//  6. Prepares hybrid search services
//
// # Storage Modes
//
// Persistent Storage (dataDir != ""):
//   - Uses BadgerDB for durable storage
//   - Data survives process restarts
//   - Suitable for production use
//   - Directory created if doesn't exist
//
// In-Memory Storage (dataDir == ""):
//   - Uses memory-only storage
//   - Data lost on process exit
//   - Faster for testing/development
//   - No disk I/O overhead
//
// # Parameters
//
// dataDir: Database directory path
//   - Non-empty: Persistent storage at this location
//   - Empty string: In-memory storage (not persistent)
//   - Created if doesn't exist
//   - Must be writable by current user
//
// config: Database configuration
//   - nil: Uses DefaultConfig() with sensible defaults
//   - See Config type for all options
//   - See DefaultConfig() for default values
//
// # Returns
//
//   - DB: Ready-to-use database instance
//   - error: nil on success, error if initialization fails
//
// # Thread Safety
//
// The returned DB instance is thread-safe and can be used
// concurrently from multiple goroutines.
//
// # Performance Characteristics
//
// Startup Time:
//   - In-memory: <10ms (instant)
//   - Persistent (empty): ~50-100ms (directory creation)
//   - Persistent (existing): ~100-500ms (BadgerDB recovery)
//   - With large database: ~1-5s (index rebuilding)
//
// Memory Usage:
//   - Minimum: ~50MB (base overhead)
//   - Per node: ~1KB (without embedding)
//   - Per embedding: dimensions × 4 bytes (1024 dims = 4KB)
//   - 100K nodes with embeddings: ~500MB
//
// Disk Usage (Persistent):
//   - Metadata: ~10MB base
//   - Per node: ~0.5-2KB (compressed)
//   - Badger value log: Grows with data
//   - Recommend 10x data size for value log
//
// Example (Basic Usage):
//
//	// Open persistent database
//	db, err := nornicdb.Open("./mydata", nil)
//	if err != nil {
//		log.Fatalf("Failed to open database: %v", err)
//	}
//	defer db.Close()
//
//	// Database is ready
//	fmt.Println("Database opened successfully")
//
//	// Create a node
//	node, _ := db.CreateNode(context.Background(), []string{"Concept"}, map[string]any{"content": "Important fact"})
//	fmt.Printf("Created node: %s\n", node.ID)
//
// Example (Production Setup):
//
//	// Production configuration
//	config := nornicdb.DefaultConfig()
//	config.DataDir = "/var/lib/nornicdb"
//	config.DecayEnabled = true
//	config.DecayInterval = 30 * time.Minute
//	config.VisibilityThreshold = 0.01 // Archive at 1%
//	config.AutoLinksEnabled = true
//	config.AutoLinksSimilarityThreshold = 0.85
//
//	db, err := nornicdb.Open("/var/lib/nornicdb", config)
//	if err != nil {
//		log.Fatalf("Failed to open database: %v", err)
//	}
//	defer db.Close()
//
//	// Set up periodic maintenance
//	go func() {
//		ticker := time.NewTicker(1 * time.Hour)
//		for range ticker.C {
//			stats := db.Stats()
//			log.Printf("Nodes: %d, Edges: %d, Memory: %d MB",
//				stats.NodeCount, stats.EdgeCount, stats.MemoryUsageMB)
//		}
//	}()
//
// Example (Development/Testing):
//
//	// In-memory database for tests
//	db, err := nornicdb.Open("", nil) // Empty string = in-memory
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer db.Close()
//
//	// Fast, no disk I/O
//	// Data lost when db.Close() or process exits
//
//	// Disable decay for predictable tests
//	config := nornicdb.DefaultConfig()
//	config.DecayEnabled = false
//	db, err = nornicdb.Open("", config)
//
// Example (Multiple Databases):
//
//	// Open multiple databases for different purposes
//	userDB, err := nornicdb.Open("/data/users", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer userDB.Close()
//
//	docsDB, err := nornicdb.Open("/data/documents", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer docsDB.Close()
//
//	// Each database is independent
//	// No data sharing between them
//
// Example (Custom Embeddings):
//
//	// Configure for OpenAI embeddings
//	config := nornicdb.DefaultConfig()
//	config.EmbeddingProvider = "openai"
//	config.EmbeddingAPIURL = "https://api.openai.com/v1"
//	config.EmbeddingModel = "text-embedding-3-large"
//	config.EmbeddingDimensions = 3072
//
//	db, err := nornicdb.Open("./data", config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Note: NornicDB expects pre-computed embeddings
//	// The config documents what embeddings you're using
//	// Actual embedding computation done by the application
//
// Example (Disaster Recovery):
//
//	// Open database with recovery
//	db, err := nornicdb.Open("/data/backup", nil)
//	if err != nil {
//		log.Printf("Failed to open primary: %v", err)
//		// Try backup location
//		db, err = nornicdb.Open("/data/backup-secondary", nil)
//		if err != nil {
//			log.Fatal("All database locations failed")
//		}
//	}
//	defer db.Close()
//
//	// Database recovered
//	fmt.Println("Database opened successfully")
//
// Example (Migration):
//
//	// Open old database
//	oldDB, err := nornicdb.Open("/data/old", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer oldDB.Close()
//
//	// Create new database with updated config
//	newConfig := nornicdb.DefaultConfig()
//	newConfig.EmbeddingDimensions = 3072 // Upgraded embeddings
//	newDB, err := nornicdb.Open("/data/new", newConfig)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer newDB.Close()
//
//	// Migrate data
//	nodes, _ := oldDB.GetAllNodes(context.Background())
//	for _, node := range nodes {
//		// Re-embed with new model (done by the application)
//		// Store in new database
//		newDB.Store(context.Background(), node)
//	}
//
// # Error Handling
//
// Common errors and solutions:
//
// Permission Denied:
//   - Ensure directory is writable
//   - Check SELinux/AppArmor policies
//   - Run with appropriate user permissions
//
// Directory Not Found:
//   - Parent directory must exist
//   - Function creates final directory only
//   - Create parent: os.MkdirAll(filepath.Dir(dataDir), 0755)
//
// Database Locked:
//   - Another process has the database open
//   - BadgerDB uses file locks
//   - Close other instances or use different directory
//
// Corruption:
//   - BadgerDB detected corruption
//   - Restore from backup
//   - Or use badger.DB.Verify() to check integrity
//
// Out of Disk Space:
//   - Free up disk space
//   - Or use in-memory mode
//   - Check value log size (can grow large)
//
// # ELI12 Explanation
//
// Think of Open() like opening a library:
//
// When you open a library:
//  1. **Check if it exists**: If the building (dataDir) doesn't exist, we build it
//  2. **Unlock the doors**: Open the storage system (BadgerDB)
//  3. **Set up the catalog**: Initialize the search system
//  4. **Hire the librarian**: Start the decay manager to organize old books
//  5. **Connect related books**: Set up the inference engine to find relationships
//
// Two types of libraries:
//   - **Real building** (dataDir provided): Books stored on shelves, survive overnight
//   - **Pop-up library** (no dataDir): Books on temporary tables, packed away at night
//
// After opening, the library is ready:
//   - You can add books (Create nodes)
//   - Search for books (Search queries)
//   - Find related books (Relationship inference)
//   - Old books fade from view (Knowledge-layer decay policies)
//
// Important:
//   - Only one person can have the keys (file lock)
//   - Must close the library when done (db.Close())
//   - Multiple libraries can exist in different locations
//
// The library staff (background goroutines) work automatically:
//   - Decay manager reorganizes books periodically
//   - Inference engine finds connections between books
//   - Search system keeps the catalog updated
//
// You just add books and search - the rest happens automatically!
func Open(dataDir string, config *Config) (*DB, error) {
	if config == nil {
		config = DefaultConfig()
	}
	config.Database.DataDir = dataDir

	db := &DB{
		config: config,
	}

	// Initialize storage - use BadgerEngine for persistence, MemoryEngine for testing
	if dataDir != "" {
		// Configure BadgerDB based on memory mode
		// HighPerformance uses ~1GB RAM, LowMemory uses ~50MB
		badgerOpts := storage.BadgerOptions{
			DataDir:               dataDir,
			HighPerformance:       true,
			LowMemory:             false,
			NodeCacheMaxEntries:   config.Database.BadgerNodeCacheMaxEntries,
			EdgeTypeCacheMaxTypes: config.Database.BadgerEdgeTypeCacheMaxTypes,
			EngineOptions: storage.EngineOptions{
				RetentionPolicy: storage.RetentionPolicy{
					MaxVersionsPerKey: config.Database.MVCCRetentionMaxVersions,
					TTL:               config.Database.MVCCRetentionTTL,
				},
				IDFreelistTTL: config.Database.IDFreelistTTL,
			},
			// Phase 2 D-01: thread the structured *slog.Logger from
			// nornicConfig.Config (set by cmd/nornicdb/main.go before Open)
			// into BadgerEngine so all storage emissions flow through the
			// production handler stack. Discard fallback in NewBadgerEngine-
			// WithOptions handles nil per D-01a.
			Logger: config.Logger,
		}
		providerMode := strings.TrimSpace(strings.ToLower(config.Database.EncryptionProvider))
		if providerMode == "" {
			providerMode = "password"
		}

		// Enable BadgerDB-level encryption at rest if configured
		if config.Database.EncryptionEnabled {
			switch providerMode {
			case "password":
				// Require password if password-based encryption is enabled
				if config.Database.EncryptionPassword == "" {
					return nil, fmt.Errorf("encryption is enabled but no password was provided")
				}
				// Load or generate salt for this database
				saltFile := dataDir + "/db.salt"
				var salt []byte
				if existingSalt, err := os.ReadFile(saltFile); err == nil && len(existingSalt) == 32 {
					salt = existingSalt
					fmt.Println("🔐 Loading existing encryption salt")
				} else {
					// Generate new salt for new encrypted database
					salt = make([]byte, 32)
					if _, err := rand.Read(salt); err != nil {
						return nil, fmt.Errorf("failed to generate encryption salt: %w", err)
					}
					// Persist salt (required for decryption after restart)
					if err := os.MkdirAll(dataDir, 0700); err != nil {
						return nil, fmt.Errorf("failed to create data directory: %w", err)
					}
					if err := os.WriteFile(saltFile, salt, 0600); err != nil {
						return nil, fmt.Errorf("failed to save encryption salt: %w", err)
					}
					fmt.Println("🔐 Generated new encryption salt")
				}

				// Derive 32-byte AES-256 key from password using PBKDF2
				encryptionKey := encryption.DeriveKey([]byte(config.Database.EncryptionPassword), salt, 600000)
				badgerOpts.EncryptionKey = encryptionKey
				db.encryptionEnabled = true
				fmt.Println("🔒 Database encryption enabled (AES-256/password)")
			default:
				// Provider-backed key management with persisted encrypted DEK
				encryptionKey, err := resolveProviderManagedDBKey(dataDir, config, providerMode)
				if err != nil {
					return nil, err
				}
				badgerOpts.EncryptionKey = encryptionKey
				db.encryptionEnabled = true
				fmt.Printf("🔒 Database encryption enabled (AES-256/%s)\n", providerMode)
			}
		}

		badgerEngine, err := storage.NewBadgerEngineWithOptions(badgerOpts)
		if err != nil {
			// Check for encryption-related errors and provide helpful messages
			// IMPORTANT: BadgerDB fails safely - data files are NOT touched or corrupted
			errStr := err.Error()
			if strings.Contains(errStr, "encryption") || strings.Contains(errStr, "decrypt") || strings.Contains(errStr, "cipher") ||
				strings.Contains(errStr, "Invalid checksum") || strings.Contains(errStr, "MANIFEST") {
				if config.Database.EncryptionEnabled {
					// Log clear warning and fail safely
					log.Printf("🔒 ENCRYPTION ERROR: Database decryption failed")
					log.Printf("   ⚠️  Data files are safe and unchanged")
					log.Printf("   ⚠️  Server will NOT start to protect your data")
					return nil, fmt.Errorf("ENCRYPTION ERROR: Failed to open database. "+
						"The encryption password appears to be incorrect. "+
						"Your data files have NOT been modified - they remain safely encrypted. "+
						"If you forgot your password, your data cannot be recovered. "+
						"Original error: %w", err)
				} else {
					// Database is encrypted but no password was provided
					log.Printf("🔒 ENCRYPTION ERROR: Database is encrypted but no password was provided")
					log.Printf("   ⚠️  Data files are safe and unchanged")
					log.Printf("   ⚠️  Server will NOT start to protect your data")
					return nil, fmt.Errorf("ENCRYPTION ERROR: Database appears to be encrypted but no password was provided. "+
						"Your data files have NOT been modified. "+
						"Set encryption_password in config.yaml or NORNICDB_ENCRYPTION_PASSWORD environment variable. "+
						"Original error: %w", err)
				}
			}

			// Auto-recover (Neo4j-style): if the primary store can't open due to corruption,
			// rebuild a fresh Badger store from snapshot + WAL replay, preserving the old
			// directory for forensics.
			autoRecoverEnabled := autoRecoverOnCorruptionEnabled()
			autoRecoverExplicit := strings.TrimSpace(os.Getenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION")) != ""
			corruptionSuspected := looksLikeCorruption(err)
			recoverableArtifacts := hasRecoverableArtifacts(dataDir)
			if autoRecoverEnabled && !config.Database.EncryptionEnabled && recoverableArtifacts && (corruptionSuspected || autoRecoverExplicit) {
				log.Printf("🔧 Auto-recover setting: enabled=%t env(NORNICDB_AUTO_RECOVER_ON_CORRUPTION)=%q", autoRecoverEnabled, os.Getenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION"))
				log.Printf("⚠️  Persistent store open failed; attempting auto-recovery from snapshots + WAL (dataDir=%s)", dataDir)
				recovered, backupDir, recErr := recoverBadgerFromSnapshotAndWAL(dataDir, badgerOpts)
				if recErr != nil {
					return nil, fmt.Errorf("failed to open persistent storage: %w (auto-recovery failed: %v)", err, recErr)
				}
				log.Printf("✅ Auto-recovery succeeded; preserved old data dir at %s", backupDir)
				badgerEngine = recovered
			} else {
				log.Printf("🔧 Auto-recover skipped: enabled=%t env(NORNICDB_AUTO_RECOVER_ON_CORRUPTION)=%q corruption_suspected=%t encryption_enabled=%t",
					autoRecoverEnabled, os.Getenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION"), corruptionSuspected, config.Database.EncryptionEnabled)
				return nil, fmt.Errorf("failed to open persistent storage: %w", err)
			}
		}
		if config.Database.MVCCLifecycleEnabled {
			lifecycleConfig := lifecycle.DefaultLifecycleConfig()
			lifecycleConfig.Enabled = config.Database.MVCCRetentionMaxVersions > 1 || config.Database.MVCCRetentionTTL > 0
			lifecycleConfig.CycleInterval = config.Database.MVCCLifecycleCycleInterval
			lifecycleConfig.MaxVersionsPerKey = config.Database.MVCCRetentionMaxVersions
			lifecycleConfig.TTL = config.Database.MVCCRetentionTTL
			lifecycleConfig.MaxSnapshotLifetime = config.Database.MVCCLifecycleMaxSnapshotAge
			lifecycleConfig.MaxChainHardCap = config.Database.MVCCLifecycleMaxChainCap
			db.lifecycleManager = lifecycle.NewMVCCLifecycleManager(lifecycleConfig, badgerEngine)
			badgerEngine.SetLifecycleController(db.lifecycleManager)
		}

		if config.Compliance.RetentionEnabled {
			rm := retention.NewManager()
			policiesPath := db.retentionPoliciesPath()
			if _, err := os.Stat(policiesPath); err == nil {
				if loadErr := rm.LoadPolicies(policiesPath); loadErr != nil {
					log.Printf("⚠️  Failed to load retention policies: %v", loadErr)
				}
			}

			if config.Retention.DefaultPolicies {
				for _, policy := range retention.DefaultPolicies() {
					if err := rm.AddPolicy(policy); err != nil && !errors.Is(err, retention.ErrAlreadyExists) {
						log.Printf("⚠️  Failed to add default retention policy %s: %v", policy.ID, err)
					}
				}
			}

			for _, policyConfig := range config.Retention.Policies {
				active := true
				if policyConfig.Active != nil {
					active = *policyConfig.Active
				}

				policy := &retention.Policy{
					ID:                   policyConfig.ID,
					Name:                 policyConfig.Name,
					Category:             retention.DataCategory(policyConfig.Category),
					ArchiveBeforeDelete:  policyConfig.ArchiveBeforeDelete,
					ArchivePath:          policyConfig.ArchivePath,
					ComplianceFrameworks: append([]string(nil), policyConfig.ComplianceFrameworks...),
					Description:          policyConfig.Description,
					Active:               active,
				}
				if policyConfig.Indefinite {
					policy.RetentionPeriod = retention.RetentionPeriod{Indefinite: true}
				} else {
					policy.RetentionPeriod = retention.RetentionPeriod{
						Duration: time.Duration(policyConfig.RetentionDays) * 24 * time.Hour,
					}
				}
				if err := rm.AddPolicy(policy); err != nil && !errors.Is(err, retention.ErrAlreadyExists) {
					log.Printf("⚠️  Failed to add retention policy %s: %v", policyConfig.ID, err)
				}
			}

			if len(rm.ListPolicies()) == 0 && config.Compliance.RetentionPolicyDays > 0 {
				defaultPolicy := &retention.Policy{
					ID:       "config-default",
					Name:     "Default Retention Policy (from config)",
					Category: retention.CategoryUser,
					RetentionPeriod: retention.RetentionPeriod{
						Duration: time.Duration(config.Compliance.RetentionPolicyDays) * 24 * time.Hour,
					},
					ArchiveBeforeDelete: !config.Compliance.RetentionAutoDelete,
					ArchivePath:         filepath.Join(dataDir, "archive"),
					Active:              true,
				}
				if err := rm.AddPolicy(defaultPolicy); err != nil {
					log.Printf("⚠️  Failed to add default retention policy: %v", err)
				}
			}

			rm.SetDeleteCallback(func(record *retention.DataRecord) error {
				if err := db.storage.DeleteNode(storage.NodeID(record.ID)); err != nil {
					return err
				}
				if db.onRetentionAction != nil {
					db.onRetentionAction("RETENTION_DELETE", record.ID, string(record.Category))
				}
				return nil
			})
			rm.SetArchiveCallback(func(record *retention.DataRecord, archivePath string) error {
				node, err := db.storage.GetNode(storage.NodeID(record.ID))
				if err != nil {
					return err
				}
				data, err := json.Marshal(node)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(archivePath, 0755); err != nil {
					return err
				}
				archiveFile := filepath.Join(archivePath, record.ID+".json")
				if err := os.WriteFile(archiveFile, data, 0644); err != nil {
					return err
				}
				if db.onRetentionAction != nil {
					db.onRetentionAction("RETENTION_ARCHIVE", record.ID, string(record.Category))
				}
				return nil
			})

			db.retentionManager = rm
			log.Printf("📋 Retention manager enabled (%d policies loaded)", len(rm.ListPolicies()))
		}

		// Initialize WAL for durability (uses batch sync mode by default for better performance)
		walConfig := storage.DefaultWALConfig()
		walConfig.Dir = dataDir + "/wal"
		walConfig.SnapshotInterval = 5 * time.Minute // Compact WAL every 5 minutes (not 1 hour!)
		if config.Database.WALRetentionLedgerDefaults &&
			config.Database.WALRetentionMaxSegments == 0 &&
			config.Database.WALRetentionMaxAge == 0 {
			config.Database.WALRetentionMaxSegments = 24
			config.Database.WALRetentionMaxAge = 7 * 24 * time.Hour
		}
		// Apply WAL retention settings from config
		if config.Database.WALRetentionMaxSegments > 0 {
			walConfig.RetentionMaxSegments = config.Database.WALRetentionMaxSegments
		}
		if config.Database.WALRetentionMaxAge > 0 {
			walConfig.RetentionMaxAge = config.Database.WALRetentionMaxAge
		}
		if config.Database.WALSnapshotRetentionMaxCount > 0 {
			walConfig.SnapshotRetentionMaxCount = config.Database.WALSnapshotRetentionMaxCount
		}
		if config.Database.WALSnapshotRetentionMaxAge > 0 {
			walConfig.SnapshotRetentionMaxAge = config.Database.WALSnapshotRetentionMaxAge
		}
		// D-07: thread the structured *slog.Logger into the WAL config so
		// the wal/wal_compaction/wal_recovery subsystems emit through the
		// production handler stack. Discard fallback inside NewWAL.
		walConfig.SlogLogger = config.Logger
		wal, err := storage.NewWAL(walConfig.Dir, walConfig)
		if err != nil {
			badgerEngine.Close()
			return nil, fmt.Errorf("failed to initialize WAL: %w", err)
		}
		db.wal = wal

		// Wrap storage with WAL for durability
		walEngine := storage.NewWALEngine(badgerEngine, wal)

		// Enable auto-compaction to prevent unbounded WAL growth
		// This creates periodic snapshots and truncates the WAL
		if config.Database.WALAutoCompactionEnabled {
			snapshotDir := dataDir + "/snapshots"
			if err := walEngine.EnableAutoCompaction(snapshotDir); err != nil {
				fmt.Printf("⚠️  WAL auto-compaction failed to enable: %v\n", err)
			} else {
				fmt.Printf("🗜️  WAL auto-compaction enabled (snapshot interval: %v)\n", walConfig.SnapshotInterval)
			}
		} else {
			fmt.Printf("🛑 WAL auto-compaction disabled (manual snapshots required)\n")
		}

		// Optionally wrap with AsyncEngine for faster writes (eventual consistency)
		var baseStorage storage.Engine
		if config.Database.AsyncWritesEnabled {
			asyncConfig := &storage.AsyncEngineConfig{
				FlushInterval:    config.Database.AsyncFlushInterval,
				MaxNodeCacheSize: config.Database.AsyncMaxNodeCacheSize,
				MaxEdgeCacheSize: config.Database.AsyncMaxEdgeCacheSize,
				// Phase 2 D-01 + D-06: thread the structured *slog.Logger
				// so the AsyncEngine flush goroutine derives the
				// single-allocation flushLog (subsystem=async_flush) from
				// the same handler stack used everywhere else.
				Logger: config.Logger,
			}
			baseStorage = storage.NewAsyncEngine(walEngine, asyncConfig)
			if config.Database.AsyncMaxNodeCacheSize > 0 || config.Database.AsyncMaxEdgeCacheSize > 0 {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v, node cache: %d, edge cache: %d)\n",
					dataDir, config.Database.AsyncFlushInterval, config.Database.AsyncMaxNodeCacheSize, config.Database.AsyncMaxEdgeCacheSize)
			} else {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v)\n", dataDir, config.Database.AsyncFlushInterval)
			}
		} else {
			baseStorage = walEngine
			fmt.Printf("📂 Using persistent storage at %s (WAL enabled, batch sync)\n", dataDir)
		}

		// Track the underlying storage chain so Close() can release Badger directory locks.
		db.baseStorage = baseStorage

		// Optionally wrap base storage with replication.
		//
		// This uses the existing pkg/replication cluster protocol transport (port 7688)
		// and routes ALL writes through a Replicator, while reads remain local.
		baseStorage, err = db.maybeEnableReplication(baseStorage)
		if err != nil {
			_ = db.baseStorage.Close()
			return nil, err
		}

		// Wrap base storage with NamespacedEngine for the default database ("nornic")
		// This ensures everything uses namespaced storage - no direct base storage access
		// Get default database name from global config (same as server does)
		globalConfig := nornicConfig.LoadFromEnv()
		defaultDBName := globalConfig.Database.DefaultDatabase
		if defaultDBName == "" {
			defaultDBName = "nornic" // Fallback to "nornic" if not configured
		}
		db.storage = storage.NewNamespacedEngine(baseStorage, defaultDBName)
		if config.Memory.DecayEnabled {
			if err := maybeBootstrapDefaultKnowledgePolicy(baseStorage, defaultDBName); err != nil {
				_ = db.baseStorage.Close()
				return nil, err
			}
		}
		fmt.Printf("📦 Wrapped storage with namespace '%s' (all operations are namespaced)\n", defaultDBName)
	} else {
		var baseStorage storage.Engine = storage.NewMemoryEngine()
		fmt.Println("⚠️  Using in-memory storage (data will not persist)")

		// Track the underlying storage chain so Close() can cleanly shut down goroutines/locks.
		db.baseStorage = baseStorage

		// Optionally wrap base storage with replication.
		replicated, err := db.maybeEnableReplication(baseStorage)
		if err != nil {
			_ = db.baseStorage.Close()
			return nil, err
		}
		baseStorage = replicated

		// Wrap in-memory storage with NamespacedEngine for consistency
		// Get default database name from global config (same as server does)
		globalConfig := nornicConfig.LoadFromEnv()
		defaultDBName := globalConfig.Database.DefaultDatabase
		if defaultDBName == "" {
			defaultDBName = "nornic" // Fallback to "nornic" if not configured
		}
		db.storage = storage.NewNamespacedEngine(baseStorage, defaultDBName)
		if config.Memory.DecayEnabled {
			if err := maybeBootstrapDefaultKnowledgePolicy(baseStorage, defaultDBName); err != nil {
				_ = db.baseStorage.Close()
				return nil, err
			}
		}
		fmt.Printf("📦 Wrapped in-memory storage with namespace '%s' (all operations are namespaced)\n", defaultDBName)
	}

	// Initialize Cypher executor
	db.cypherExecutor = cypher.NewStorageExecutor(db.storage)

	// Configure executor with embedding dimensions for vector index creation
	if config.Memory.EmbeddingDimensions > 0 {
		db.cypherExecutor.SetDefaultEmbeddingDimensions(config.Memory.EmbeddingDimensions)
	}

	// Load function plugins from configured directory
	// Heimdall plugins will be loaded later by the server after Heimdall is initialized
	if db.config.Server.PluginsDir != "" {
		if err := LoadPluginsFromDir(db.config.Server.PluginsDir, nil); err != nil {
			fmt.Printf("⚠️  Plugin loading warning: %v\n", err)
		}
	}

	// Wire up plugin function lookup for Cypher executor
	cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
		fn, found := GetPluginFunction(name)
		if !found {
			return nil, false
		}
		return fn.Handler, true
	}

	// Configure parallel execution
	parallelCfg := cypher.ParallelConfig{
		Enabled:      true,
		MaxWorkers:   0,
		MinBatchSize: 1000,
	}
	// If MaxWorkers is 0, the parallel package will use runtime.NumCPU()
	cypher.SetParallelConfig(parallelCfg)

	// Gate the pending-embed index at the storage layer. When embeddings are
	// globally disabled nobody consumes the marker, so writing it is pure
	// write amplification on every CreateNode/UpsertNode. Thread the flag
	// down to the engine once at startup.
	if be := unwrapToBadgerEngine(db.baseStorage); be != nil {
		be.SetEmbeddingsEnabled(config.Memory.EmbeddingEnabled)
	}

	// Initialize knowledge-layer decay: wire scorer into BadgerEngine read paths.
	// The flusher is created here but started later (after buildCtx is initialized).
	if config.Memory.DecayEnabled {
		if be := unwrapToBadgerEngine(db.baseStorage); be != nil {
			be.SetDecayEnabled(true)
			defaultDBName := db.defaultDatabaseName()

			// Record startup reconcile pass so operators see the init-time
			// schema sweep in the counter. We read the metrics handle via
			// the global ref so late-initialised observability (common when
			// Provider.New runs AFTER nornicdb.Open in main.go) still
			// captures the startup fire on the next scrape.
			if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
				kp.IncReconcile("startup", defaultDBName)
			}

			be.GetSchemaForNamespace(defaultDBName).SetKnowledgePolicyChangedHook(func() {
				// Wrap schema-change reconcile in a span + counter so the
				// (usually rare) churn from DDL is visible.
				hookCtx, span := otel.Tracer("nornicdb/knowledge_policy").Start(
					context.Background(), "nornicdb.knowledge_policy.reconcile",
					trace.WithSpanKind(trace.SpanKindInternal),
				)
				defer span.End()
				if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
					kp.IncReconcile("schema_change", defaultDBName)
				}
				changes, err := be.ReconcileDecaySuppressionWithChanges(defaultDBName)
				if err != nil || db.cypherExecutor == nil {
					return
				}
				span.SetAttributes(
					attribute.Int("changes_count", len(changes)),
					attribute.String("trigger", "schema_change"),
					attribute.String("database", defaultDBName),
				)
				for _, change := range changes {
					db.cypherExecutor.InvalidateEntityCaches(change.EntityID, change.Tokens)
				}
				_ = hookCtx
			})
			db.accessAccumulator = knowledgepolicy.NewAccessAccumulator(true, config.Memory.AccessFlushBufferSize)
			be.SetAccessAccumulator(db.accessAccumulator)
			db.accessFlusher = knowledgepolicy.NewAccessFlusher(
				db.accessAccumulator, be, config.Memory.DecayInterval,
			)
			// Metrics are resolved lazily via the global ref inside the
			// flusher; no explicit SetMetrics call needed. Same for Scorer.
			db.accessFlusher.SetPropertySuppression(
				func(namespace string) *knowledgepolicy.Scorer {
					s := be.ScorerForNamespace(namespace)
					if s != nil {
						// Database label is the namespace; metrics resolve
						// via the global ref at fire time.
						s.SetMetrics(nil, namespace)
					}
					return s
				},
				be,
				func(entityID string) { be.AddToPendingEmbeddings(storage.NodeID(entityID)) },
			)
			db.accessFlusher.SetSuppressionRecheck(func(entityID string, meta knowledgepolicy.EntityMeta) {
				becameSuppressed, err := be.EnqueueDeindexIfSuppressed(entityID, meta.Scope == knowledgepolicy.ScopeEdge)
				if err == nil && becameSuppressed {
					// Emit deindex counter + on-access suppression record.
					if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
						kind := "node"
						if meta.Scope == knowledgepolicy.ScopeEdge {
							kind = "edge"
						}
						kp.IncDeindexEnqueued(kind, defaultDBName)
						kp.IncSuppression(kind, "on_access", defaultDBName)
					}
					if db.cypherExecutor != nil {
						tokens := append([]string(nil), meta.Labels...)
						if meta.EdgeType != "" {
							tokens = append(tokens, meta.EdgeType)
						}
						db.cypherExecutor.InvalidateEntityCaches(entityID, tokens)
					}
				}
			})
		}
	}

	// Initialize inference engine
	if config.Memory.AutoLinksEnabled {
		db.inferenceServices = make(map[string]*inference.Engine)
		// Eagerly create default DB inference for parity with prior behavior.
		if _, err := db.getOrCreateInferenceService(db.defaultDatabaseName(), db.storage); err != nil {
			db.mu.Lock()
			db.closed = true
			db.mu.Unlock()
			_ = db.closeInternal()
			return nil, fmt.Errorf("init inference: %w", err)
		}
	}

	// Initialize embedding worker config from main config
	db.embedWorkerConfig = &EmbedWorkerConfig{
		NumWorkers:           config.EmbeddingWorker.NumWorkers,
		ScanInterval:         config.EmbeddingWorker.ScanInterval,
		BatchDelay:           config.EmbeddingWorker.BatchDelay,
		TriggerDebounceDelay: config.EmbeddingWorker.TriggerDebounceDelay,
		MaxRetries:           config.EmbeddingWorker.MaxRetries,
		ChunkSize:            config.EmbeddingWorker.ChunkSize,
		ChunkOverlap:         config.EmbeddingWorker.ChunkOverlap,
		ClusterDebounceDelay: 30 * time.Second, // Wait 30s after last embedding before k-means
		ClusterMinBatchSize:  10,               // Need at least 10 embeddings to trigger k-means
		PropertiesInclude:    config.EmbeddingWorker.PropertiesInclude,
		PropertiesExclude:    config.EmbeddingWorker.PropertiesExclude,
		IncludeLabels:        config.EmbeddingWorker.IncludeLabels,
	}

	// Initialize search service config (per-database services are created lazily).
	embeddingDims := config.Memory.EmbeddingDimensions
	if embeddingDims <= 0 {
		embeddingDims = 1024 // Default for bge-m3 / mxbai-embed-large
	}
	db.embeddingDims = embeddingDims
	db.searchMinSimilarity = config.Memory.SearchMinSimilarity
	db.searchServices = make(map[string]*dbSearchService)
	log.Printf("🔍 Search services enabled (per-database init, %d-dimension vector index)", embeddingDims)

	// Start the embed queue (with nil embedder) when auto-embed is enabled. Worker goroutines
	// are deferred until after search index build + k-means warmup to avoid slowing startup.
	// SetEmbedder(embedder) is called by the server when the model loads.
	if config.Memory.EmbeddingEnabled && db.baseStorage != nil && db.embedWorkerConfig != nil {
		workerCfg := *db.embedWorkerConfig
		workerCfg.DeferWorkerStart = true
		db.embedQueue = NewEmbedQueue(nil, db.baseStorage, &workerCfg)
		if db.embedQueueYieldFn != nil {
			db.embedQueue.SetShouldYield(db.embedQueueYieldFn)
		}
		db.embedQueue.SetOnEmbedded(func(node *storage.Node) {
			db.onNodeEmbedded(node)
		})
		if db.cypherExecutor != nil {
			db.cypherExecutor.SetNodeMutatedCallback(func(nodeID string) {
				if db.embedQueue != nil {
					db.embedQueue.Enqueue(nodeID)
				}
			})
		}
		log.Printf("🧠 Embed queue created (workers will start after DB warmup)")
	}

	// Wire Cypher vector procedures through the unified search service.
	// This preserves Neo4j/Cypher interface compatibility while centralizing the
	// implementation in the core search layer.
	if db.cypherExecutor != nil {
		if svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage); err == nil {
			db.cypherExecutor.SetSearchService(svc)
		} else {
			fmt.Printf("⚠️  Search service unavailable for Cypher executor: %v\n", err)
		}
	}

	// Wire up storage event callbacks to keep search indexes synchronized
	// Storage is the single source of truth - it notifies when changes happen
	// The storage chain can be: AsyncEngine -> WALEngine -> BadgerEngine
	var underlyingEngine storage.Engine = db.storage
	var asyncNotifier storage.StorageEventNotifier

	// Unwrap NamespacedEngine first so we can:
	//  1) Reach the underlying engine that emits events (BadgerEngine)
	//  2) Receive events with fully-qualified node IDs (<db>:<id>)
	if namespacedEngine, ok := underlyingEngine.(*storage.NamespacedEngine); ok {
		underlyingEngine = namespacedEngine.GetInnerEngine()
	}

	// Unwrap AsyncEngine if present
	if asyncEngine, ok := underlyingEngine.(*storage.AsyncEngine); ok {
		// Keep a reference to also receive cache-only delete notifications (pending creates).
		// These deletes never hit the inner engine, so only the async layer can emit them.
		asyncNotifier = asyncEngine
		underlyingEngine = asyncEngine.GetEngine()
	}

	// Unwrap WALEngine if present
	if walEngine, ok := underlyingEngine.(*storage.WALEngine); ok {
		underlyingEngine = walEngine.GetEngine()
	}

	// Set callbacks on the actual storage engine (BadgerEngine which implements StorageEventNotifier)
	if notifier, ok := underlyingEngine.(storage.StorageEventNotifier); ok {
		// When a node is created/updated/deleted, route the event to the correct
		// per-database search service based on the namespace prefix (<db>:<id>).
		// The downstream mutation path is debounced, so we can enqueue inline here
		// without spawning one goroutine per write event.
		notifier.OnNodeCreated(func(node *storage.Node) {
			if node == nil {
				return
			}
			nodeCopy := storage.CopyNode(node)
			db.indexNodeFromEvent(nodeCopy)
		})
		notifier.OnNodeUpdated(func(node *storage.Node) {
			if node == nil {
				return
			}
			nodeCopy := storage.CopyNode(node)
			db.indexNodeFromEvent(nodeCopy)
		})
		notifier.OnNodeDeleted(func(nodeID storage.NodeID) {
			db.removeNodeFromEvent(nodeID)
		})
	}

	// Also register for async-cache delete notifications if the async layer exists.
	// This handles the case where a node is created and then deleted while still
	// buffered in AsyncEngine (so the inner engine never emits a delete event).
	if asyncNotifier != nil {
		asyncNotifier.OnNodeDeleted(func(nodeID storage.NodeID) {
			db.removeNodeFromEvent(nodeID)
		})
	}

	// Enable k-means clustering if feature flag is set (applied lazily per database).
	if nornicConfig.IsGPUClusteringEnabled() {
		fmt.Println("🔬 K-means clustering enabled (per-database init)")
	}

	// Note: Database encryption is now handled at the BadgerDB storage level (see above)
	// When encryption is enabled, ALL data is encrypted at rest using AES-256.

	// Build search indexes (BM25 + vector) for all databases on startup, then run k-means.
	// This ensures search is ready before first query and k-means runs after indexes exist.
	// Use a context cancelled on DB close so the build stops when the process is killed.
	defaultDBName := db.defaultDatabaseName()
	db.buildCtx, db.buildCancel = context.WithCancel(context.Background())
	if db.accessFlusher != nil {
		db.accessFlusher.Start(db.buildCtx)
	}
	_ = db.startBackgroundTask(func() {
		ctx, cancel := context.WithTimeout(db.buildCtx, 4*time.Hour)
		defer cancel()

		if maint, ok := db.baseStorage.(storage.TemporalMaintenanceEngine); ok {
			log.Printf("🕰️ Rebuilding temporal indexes before search warmup...")
			if err := maint.RebuildTemporalIndexes(ctx); err != nil {
				log.Printf("⚠️  Temporal index rebuild failed: %v", err)
			}
		}
		if maint, ok := db.baseStorage.(storage.MVCCMaintenanceEngine); ok {
			log.Printf("🧾 Rebuilding MVCC heads before search warmup...")
			if err := maint.RebuildMVCCHeads(ctx); err != nil {
				log.Printf("⚠️  MVCC head rebuild failed: %v", err)
			}
		}
		if db.lifecycleManager != nil {
			db.lifecycleManager.StartLifecycle(db.buildCtx)
			if db.config.Database.MVCCLifecycleCycleInterval > 0 {
				log.Printf("🔄 MVCC lifecycle manager enabled (cycle=%s)", db.config.Database.MVCCLifecycleCycleInterval)
			} else {
				log.Printf("🔄 MVCC lifecycle manager enabled in manual-only mode")
			}
		}
		if db.config != nil && db.config.Compliance.RetentionEnabled && db.retentionManager != nil {
			db.startRetentionSweep(db.buildCtx)
		}

		// Collect all database names: default plus any from storage namespace listing.
		dbNames := make(map[string]struct{})
		dbNames[defaultDBName] = struct{}{}
		if lister, ok := db.baseStorage.(storage.NamespaceLister); ok {
			for _, ns := range lister.ListNamespaces() {
				if ns != "" && ns != "system" {
					dbNames[ns] = struct{}{}
				}
			}
		}

		// Build search indexes (BM25 + vector) for each database in parallel so a large
		// database warmup doesn't block smaller databases from becoming ready.
		type buildResult struct {
			dbName string
			err    error
		}
		results := make(chan buildResult, len(dbNames))
		var buildWg sync.WaitGroup
		for dbName := range dbNames {
			buildWg.Add(1)
			go func(dbName string) {
				defer buildWg.Done()
				if ctx.Err() != nil {
					results <- buildResult{dbName: dbName, err: ctx.Err()}
					return
				}
				var storageEngine storage.Engine
				if dbName == defaultDBName {
					storageEngine = db.storage
				}
				log.Printf("🔍 Building BM25 + vector indexes for database %s...", dbName)
				_, err := db.EnsureSearchIndexesBuilt(ctx, dbName, storageEngine)
				results <- buildResult{dbName: dbName, err: err}
			}(dbName)
		}
		go func() {
			buildWg.Wait()
			close(results)
		}()
		var built, failed int
		for res := range results {
			if res.err != nil {
				if ctx.Err() != nil {
					log.Printf("🔍 Search index build cancelled for db %s (shutdown)", res.dbName)
					continue
				}
				log.Printf("⚠️  Failed to build search indexes for db %s: %v", res.dbName, res.err)
				failed++
				continue
			}
			built++
			if len(dbNames) > 1 {
				log.Printf("✅ BM25 + vector indexes built for db %s", res.dbName)
			}
		}
		if built > 0 && len(dbNames) == 1 {
			log.Printf("✅ BM25 + vector search indexes ready (default database)")
		} else if built > 0 {
			log.Printf("✅ BM25 + vector search indexes ready for %d database(s)", built)
		}
		if failed > 0 {
			log.Printf("⚠️  Search index build failed for %d database(s)", failed)
		}

		// Run k-means only after all search indexes are built (skip if build was cancelled).
		if ctx.Err() == nil {
			db.mu.RLock()
			timerActive := db.clusterTicker != nil
			db.mu.RUnlock()
			if !timerActive {
				db.runClusteringOnceAllDatabases(ctx)
			}
		}

		// Start embed queue workers after warmup so they don't compete with index build on startup.
		if db.embedQueue != nil {
			db.embedQueue.StartWorkers()
		}
	})

	// Note: Auto-embed queue is initialized via SetEmbedder() after the server creates
	// the embedder. This avoids duplicate embedder creation and ensures consistency
	// between search embeddings and auto-embed.

	return db, nil
}

func (db *DB) maybeEnableReplication(base storage.Engine) (storage.Engine, error) {
	mode := os.Getenv("NORNICDB_CLUSTER_MODE")
	if mode == "" || mode == string(replication.ModeStandalone) {
		return base, nil
	}

	replCfg := replication.LoadFromEnv()

	// Keep replication state under the DB data dir unless explicitly overridden.
	if os.Getenv("NORNICDB_CLUSTER_DATA_DIR") == "" {
		if db.config != nil && db.config.Database.DataDir != "" {
			replCfg.DataDir = db.config.Database.DataDir + "/replication"
		} else {
			replCfg.DataDir = "./data/replication"
		}
	}

	adapter, err := replication.NewStorageAdapterWithWAL(base, replCfg.DataDir+"/wal")
	if err != nil {
		return nil, fmt.Errorf("replication: create storage adapter: %w", err)
	}

	replicator, err := replication.NewReplicator(replCfg, adapter)
	if err != nil {
		_ = adapter.Close()
		return nil, fmt.Errorf("replication: create replicator: %w", err)
	}

	transport := replication.NewDefaultTransport(&replication.ClusterTransportConfig{
		NodeID:   replCfg.NodeID,
		BindAddr: replCfg.BindAddr,
	})

	// Wire transport into modes that support it.
	if r, ok := replicator.(interface{ SetTransport(replication.Transport) }); ok {
		r.SetTransport(transport)
	}
	// If we're using the default ClusterTransport, register message handlers for the replicator.
	if ct, ok := transport.(*replication.ClusterTransport); ok {
		replication.RegisterClusterHandlers(ct, replicator)
	}

	if err := replicator.Start(context.Background()); err != nil {
		_ = transport.Close()
		_ = replicator.Shutdown()
		_ = adapter.Close()
		return nil, fmt.Errorf("replication: start: %w", err)
	}

	db.replicator = replicator
	db.replicationAdapter = adapter
	db.replicationTrans = transport

	return replication.NewReplicatedEngine(base, replicator, 30*time.Second), nil
}

// SetEmbedder configures the auto-embed queue with the given embedder.
// This should be called by the server after creating a working embedder.
// The embedder is shared with the MCP server and Cypher executor for consistency.
func (db *DB) SetEmbedder(embedder embed.Embedder) {
	if embedder == nil {
		return
	}

	db.mu.Lock()

	if db.baseStorage == nil {
		// baseStorage is required for correctness (it’s the only engine that can see all namespaces).
		// If this ever happens, initialization order is broken.
		db.mu.Unlock()
		panic("nornicdb: baseStorage is nil in SetEmbedder")
	}

	// Share embedder with Cypher executor for server-side query embedding
	// This enables: CALL db.index.vector.queryNodes('idx', 10, 'search text')
	if db.cypherExecutor != nil {
		db.cypherExecutor.SetEmbedder(embedder)
	}

	if db.embedQueue != nil {
		// Queue was created in Open(); activate with loaded embedder and start cluster timer
		if db.embedQueueYieldFn != nil {
			db.embedQueue.SetShouldYield(db.embedQueueYieldFn)
		}
		db.embedQueue.SetEmbedder(embedder)
		log.Printf("🧠 Auto-embed queue started using %s (%d dims)",
			embedder.Model(), embedder.Dimensions())
		db.mu.Unlock()
		if nornicConfig.IsGPUClusteringEnabled() {
			interval := db.config.Memory.KmeansClusterInterval
			if interval > 0 {
				db.startClusteringTimer(interval)
			}
		}
		return
	}

	// Create embed queue against the un-namespaced base storage so it can pull work
	// from ALL databases (node IDs are fully-qualified, e.g. "nornic:<id>").
	db.embedQueue = NewEmbedQueue(embedder, db.baseStorage, db.embedWorkerConfig)
	if db.embedQueueYieldFn != nil {
		db.embedQueue.SetShouldYield(db.embedQueueYieldFn)
	}

	// Set callback to update search index after embedding and run inference.
	// Note: IndexNode is idempotent (uses map keyed by node ID), so if the storage
	// OnNodeUpdated callback also calls IndexNode, no double-counting occurs.
	db.embedQueue.SetOnEmbedded(func(node *storage.Node) {
		db.onNodeEmbedded(node)
	})

	// Start timer-based k-means clustering instead of trigger-based.
	// Compute decision under lock, but start the timer outside the lock.
	var startClusterTimer bool
	var clusterInterval time.Duration
	if nornicConfig.IsGPUClusteringEnabled() {
		clusterInterval = db.config.Memory.KmeansClusterInterval
		if clusterInterval > 0 {
			startClusterTimer = true
		} else {
			log.Printf("🔬 K-means clustering enabled (manual trigger only, no timer)")
		}
	}

	// Wire up Cypher executor to trigger embedding queue when nodes are created or mutated
	// This ensures nodes created or updated via Cypher get embeddings (re)generated
	if db.cypherExecutor != nil {
		db.cypherExecutor.SetNodeMutatedCallback(func(nodeID string) {
			db.embedQueue.Enqueue(nodeID)
		})
	}

	log.Printf("🧠 Auto-embed queue started using %s (%d dims)",
		embedder.Model(), embedder.Dimensions())

	db.mu.Unlock()

	if startClusterTimer {
		db.startClusteringTimer(clusterInterval)
	}
}

// SetEmbedQueueShouldYield sets an optional foreground-pressure callback used to
// pause background embedding while high-priority request traffic is active.
func (db *DB) SetEmbedQueueShouldYield(fn func() bool) {
	db.mu.Lock()
	db.embedQueueYieldFn = fn
	q := db.embedQueue
	db.mu.Unlock()
	if q != nil {
		q.SetShouldYield(fn)
	}
}

// SetDefaultEmbedConfig registers the current global embedder (must have been set via SetEmbedder first)
// under the given config key so the per-DB embedder registry can use it as the default when a database
// has no overrides. Call this after SetEmbedder when per-DB config is available (e.g. server with dbConfigStore).
func (db *DB) SetDefaultEmbedConfig(defaultConfig *embed.Config) {
	if defaultConfig == nil {
		return
	}
	db.mu.Lock()
	eq := db.embedQueue
	db.mu.Unlock()
	if eq == nil || eq.embedder == nil {
		return
	}
	key := embedConfigKey(defaultConfig)
	db.embedderRegistryMu.Lock()
	if db.embedderRegistry == nil {
		db.embedderRegistry = make(map[string]embed.Embedder)
	}
	db.embedderRegistry[key] = eq.embedder
	db.defaultEmbedKey = key
	db.embedderRegistryMu.Unlock()
}

// SetEmbedConfigForDB sets the optional resolver that returns resolved embed config for a database.
// When set, EmbedQueryForDB uses the embedder registry (get or create by config key) instead of the global embedder.
func (db *DB) SetEmbedConfigForDB(fn func(dbName string) (*embed.Config, error)) {
	db.mu.Lock()
	db.embedConfigForDB = fn
	db.mu.Unlock()
}

// getOrCreateEmbedderForDB returns the embedder for the given database, using the registry when embedConfigForDB is set.
func (db *DB) getOrCreateEmbedderForDB(dbName string) (embed.Embedder, error) {
	db.mu.RLock()
	fn := db.embedConfigForDB
	embedQueue := db.embedQueue
	db.mu.RUnlock()

	if fn == nil || embedQueue == nil || embedQueue.embedder == nil {
		if embedQueue != nil {
			return embedQueue.embedder, nil
		}
		return nil, nil
	}
	cfg, err := fn(dbName)
	if err != nil || cfg == nil {
		return embedQueue.embedder, nil
	}
	key := embedConfigKey(cfg)
	db.embedderRegistryMu.RLock()
	if db.embedderRegistry != nil {
		if e, ok := db.embedderRegistry[key]; ok {
			db.embedderRegistryMu.RUnlock()
			return e, nil
		}
	}
	db.embedderRegistryMu.RUnlock()

	db.embedderRegistryMu.Lock()
	if db.embedderRegistry == nil {
		db.embedderRegistry = make(map[string]embed.Embedder)
	}
	if e, ok := db.embedderRegistry[key]; ok {
		db.embedderRegistryMu.Unlock()
		return e, nil
	}
	// Reuse the active default local embedder when resolved config is equivalent.
	// This avoids expensive re-initialization on the query path due key drift
	// from equivalent local defaults (e.g. GPULayers 0 vs -1).
	if strings.EqualFold(strings.TrimSpace(cfg.Provider), "local") &&
		embedQueue.embedder.Dimensions() == cfg.Dimensions &&
		strings.EqualFold(strings.TrimSpace(embedQueue.embedder.Model()), strings.TrimSpace(cfg.Model)) {
		db.embedderRegistry[key] = embedQueue.embedder
		db.embedderRegistryMu.Unlock()
		return embedQueue.embedder, nil
	}
	db.embedderRegistryMu.Unlock()

	create := db.embedderFactory
	if create == nil {
		create = embed.NewEmbedder
	}

	// Single-flight per embedder key: do not hold embedderRegistryMu while creating.
	db.embedderCreateMu.Lock()
	if db.embedderCreate == nil {
		db.embedderCreate = make(map[string]chan struct{})
	}
	// Re-check the registry after entering the single-flight section. A concurrent
	// creator may have completed after our optimistic registry lookup above but
	// before we acquired embedderCreateMu, in which case starting a new creation
	// would duplicate work.
	db.embedderRegistryMu.RLock()
	if e, ok := db.embedderRegistry[key]; ok {
		db.embedderRegistryMu.RUnlock()
		db.embedderCreateMu.Unlock()
		return e, nil
	}
	db.embedderRegistryMu.RUnlock()
	if ch, ok := db.embedderCreate[key]; ok {
		db.embedderCreateMu.Unlock()
		<-ch
		db.embedderRegistryMu.RLock()
		if e, ok := db.embedderRegistry[key]; ok {
			db.embedderRegistryMu.RUnlock()
			return e, nil
		}
		db.embedderRegistryMu.RUnlock()
		// Creation completed but no registry entry (likely create failed). Fall back.
		return embedQueue.embedder, nil
	}
	ch := make(chan struct{})
	db.embedderCreate[key] = ch
	db.embedderCreateMu.Unlock()

	newEmbedder, createErr := create(cfg)

	db.embedderRegistryMu.Lock()
	if createErr == nil && newEmbedder != nil {
		if existing, ok := db.embedderRegistry[key]; ok {
			newEmbedder = existing
		} else {
			db.embedderRegistry[key] = newEmbedder
		}
	}
	db.embedderRegistryMu.Unlock()

	db.embedderCreateMu.Lock()
	close(ch)
	delete(db.embedderCreate, key)
	db.embedderCreateMu.Unlock()

	if createErr != nil || newEmbedder == nil {
		return embedQueue.embedder, nil
	}
	return newEmbedder, nil
}

// BuildSearchIndexes builds the search indexes from loaded data.
// Call this after loading data to enable search functionality.
func (db *DB) BuildSearchIndexes(ctx context.Context) error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil {
		return err
	}
	return svc.BuildIndexes(ctx)
}

// GetStorage returns the namespaced storage engine for the default database.
// This is used by the DB itself for all operations (embedding queue, search, etc.).
//
// Note: This returns the namespaced storage, not the base storage.
// All DB operations use namespaced storage - there is no direct base storage access.
// Use GetBaseStorageForManager() if you need the base storage for DatabaseManager.
func (db *DB) GetStorage() storage.Engine {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.storage
}

// GetBaseStorageForManager returns the underlying base storage engine (unwraps namespace).
// This is used by DatabaseManager to create NamespacedEngines for other databases.
//
// This method unwraps the NamespacedEngine to get the base storage (BadgerEngine/MemoryEngine/etc.)
// so that DatabaseManager can create new NamespacedEngines for different databases.
func (db *DB) GetBaseStorageForManager() storage.Engine {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Unwrap the NamespacedEngine to get the base storage
	if namespaced, ok := db.storage.(*storage.NamespacedEngine); ok {
		return namespaced.GetInnerEngine()
	}

	// DB storage must always be namespaced; anything else is a programmer error.
	panic("nornicdb: GetBaseStorageForManager called but DB storage is not namespaced")
}

// StopEmbedQueue stops the embed queue and waits for workers to exit.
// Call this as soon as shutdown is requested (e.g. on SIGINT) so embeddings stop
// before the server is torn down. Safe to call if already stopped or never started.
func (db *DB) StopEmbedQueue() {
	db.mu.Lock()
	q := db.embedQueue
	db.embedQueue = nil
	db.mu.Unlock()
	if q != nil {
		q.Close()
	}
}

// startBackgroundTask tracks DB-owned goroutines and prevents new tracked tasks
// from being launched once shutdown has started.
func (db *DB) startBackgroundTask(fn func()) bool {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return false
	}
	db.bgWg.Add(1)
	db.mu.RUnlock()

	go func() {
		defer db.bgWg.Done()
		fn()
	}()
	return true
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	return db.closeInternal()
}

// HealthCheck reports whether the underlying storage engine is responsive.
//
// Phase 1 (M1) implementation: probes the namespaced storage engine via
// NodeCount(), which is the cheapest synchronous Engine accessor that
// returns storage.ErrStorageClosed when the engine has been Closed
// (contract verified in pkg/storage/badger_test.go — "NodeCount returns
// ErrStorageClosed").
//
// Why NodeCount and not a Begin/Discard round-trip:
//   - NodeCount is a stat read; no Badger txn allocation.
//   - The Engine interface guarantees it; both BadgerEngine and
//     MemoryEngine implement it. AsyncEngine forwards to the underlying
//     engine.
//   - The closed-engine sentinel path is already covered by an existing
//     test, so we inherit that contract for free.
//
// Future phases may extend with deeper liveness probes (replication peer
// reachability, MVCC scheduler health, etc.) — the contract stays
// "nil iff process can serve queries right now."
//
// Used by pkg/observability.Health via cmd/nornicdb/main.go:
//
//	obs.Health().Register("storage", db.HealthCheck)
//
// The signature matches observability.CheckFunc exactly so registration
// is a direct method-value pass.
func (db *DB) HealthCheck(ctx context.Context) error {
	if db == nil {
		return errors.New("nornicdb: nil DB")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.storage == nil {
		return errors.New("nornicdb: storage engine not initialized")
	}
	// Real probe: NodeCount returns storage.ErrStorageClosed on a closed
	// engine; treat any error as "not responsive right now."
	if _, err := db.storage.NodeCount(); err != nil {
		return fmt.Errorf("nornicdb: storage probe: %w", err)
	}
	return nil
}

// GetRetentionManager returns the retention manager or nil when retention is disabled.
func (db *DB) GetRetentionManager() *retention.Manager {
	return db.retentionManager
}

// SetRetentionAuditCallback installs a callback invoked after retention archive/delete actions.
func (db *DB) SetRetentionAuditCallback(fn func(action, recordID, category string)) {
	db.onRetentionAction = fn
}

func (db *DB) retentionPoliciesPath() string {
	if db == nil || db.config == nil {
		return "retention-policies.json"
	}
	if path := strings.TrimSpace(db.config.Retention.PoliciesFile); path != "" {
		return path
	}
	dataDir := strings.TrimSpace(db.config.Database.DataDir)
	if dataDir == "" {
		return "retention-policies.json"
	}
	return filepath.Join(dataDir, "retention-policies.json")
}

// closeInternal performs cleanup without requiring the lock.
// Used during initialization failures and normal close.
func (db *DB) closeInternal() error {
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()

	// Stop clustering timer first (before waiting for goroutines)
	db.stopClusteringTimer()

	// Cancel build context so startup index-build goroutine exits promptly (e.g. on SIGINT/SIGTERM).
	if db.buildCancel != nil {
		db.buildCancel()
	}

	// Close embed queue first so workers stop before we persist indexes.
	// Otherwise embeddings keep running during the (slow) persist and after "Shutting down".
	if db.embedQueue != nil {
		db.embedQueue.Close()
	}

	// Wait for background goroutines to complete
	db.bgWg.Wait()

	var errs []error

	if db.retentionManager != nil && db.config != nil {
		if err := db.retentionManager.SavePolicies(db.retentionPoliciesPath()); err != nil {
			log.Printf("⚠️  Failed to save retention policies: %v", err)
		}
	}

	// Persist search indexes on shutdown only when persistence is enabled.
	if db.config != nil && db.config.Database.PersistSearchIndexes && db.config.Database.DataDir != "" {
		db.searchServicesMu.RLock()
		for _, entry := range db.searchServices {
			if entry != nil && entry.svc != nil {
				entry.svc.PersistIndexesToDisk()
			}
		}
		db.searchServicesMu.RUnlock()
	}

	if db.accessFlusher != nil {
		db.accessFlusher.Stop()
	}

	// Stop replication before closing storage.
	if db.replicator != nil {
		if err := db.replicator.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.replicationTrans != nil {
		if err := db.replicationTrans.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.replicationAdapter != nil {
		if err := db.replicationAdapter.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close the underlying storage chain to release Badger directory locks and stop
	// background goroutines (AsyncEngine flush loop, WAL auto-compaction, etc).
	// NamespacedEngine.Close() intentionally does NOT close its inner engine.
	if db.baseStorage != nil {
		if err := db.baseStorage.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// EmbedQueueStats returns statistics about the async embedding queue.
// Returns nil if auto-embed is not enabled.
func (db *DB) EmbedQueueStats() *QueueStats {
	if db.embedQueue == nil {
		return nil
	}
	stats := db.embedQueue.Stats()
	return &stats
}

// PendingEmbeddingsCount returns the current size of the pending-embeddings index.
// This is a fast, storage-backed counter path used by embed/stats.
func (db *DB) PendingEmbeddingsCount() int {
	if db.baseStorage != nil {
		if counter, ok := db.baseStorage.(interface{ PendingEmbeddingsCount() int }); ok {
			return counter.PendingEmbeddingsCount()
		}
	}
	return 0
}

// EmbeddingCountCached returns total embeddings from already-initialized search services only.
// Unlike EmbeddingCount(), this does not create services and therefore avoids startup/path contention.
func (db *DB) EmbeddingCountCached() int {
	db.searchServicesMu.RLock()
	defer db.searchServicesMu.RUnlock()
	total := 0
	for _, entry := range db.searchServices {
		if entry != nil && entry.svc != nil {
			total += entry.svc.EmbeddingCount()
		}
	}
	return total
}

// VectorIndexDimensionsCached returns dimensions from an existing default-db search service,
// or configured embedding dimensions when the service is not initialized yet.
func (db *DB) VectorIndexDimensionsCached() int {
	defaultDB := db.defaultDatabaseName()
	db.searchServicesMu.RLock()
	entry := db.searchServices[defaultDB]
	db.searchServicesMu.RUnlock()
	if entry != nil && entry.svc != nil {
		return entry.svc.VectorIndexDimensions()
	}
	if db.embeddingDims > 0 {
		return db.embeddingDims
	}
	return 0
}

// EmbeddingCount returns the total number of nodes with embeddings.
// When allDatabasesProvider is set (e.g. by the server in multi-db mode), it get-or-creates
// the search service for each database and sums their tracked counts (O(1) per DB).
// Otherwise returns the count for the default database only.
func (db *DB) EmbeddingCount() int {
	if db.allDatabasesProvider != nil {
		var total int
		for _, d := range db.allDatabasesProvider() {
			if d.Name == "system" || d.IsComposite {
				continue
			}
			svc, err := db.GetOrCreateSearchService(d.Name, d.Storage)
			if err != nil || svc == nil {
				continue
			}
			total += svc.EmbeddingCount()
		}
		return total
	}
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return 0
	}
	return svc.EmbeddingCount()
}

// SetAllDatabasesProvider sets an optional provider so EmbeddingCount() aggregates across all databases.
// The server sets this when multi-database is enabled so "Total embeddings" reflects every DB.
func (db *DB) SetAllDatabasesProvider(provider func() []DatabaseAndStorage) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.allDatabasesProvider = provider
}

// SetDbConfigResolver sets an optional resolver for per-database config
// (embedding dims, search min similarity, BM25 engine).
// When set, getOrCreateSearchService uses the resolver for the given dbName instead of global db.config.
// Call with nil to use global config only.
func (db *DB) SetDbConfigResolver(resolver DbConfigResolver) {
	db.dbConfigResolverMu.Lock()
	defer db.dbConfigResolverMu.Unlock()
	db.dbConfigResolver = resolver
}

// VectorIndexDimensions returns the actual dimensions of the search service's vector index.
// This is useful for debugging dimension mismatches between config and runtime.
func (db *DB) VectorIndexDimensions() int {
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return 0
	}
	return svc.VectorIndexDimensions()
}

// EmbedExisting triggers the worker to scan for nodes without embeddings.
// The worker runs automatically, but this can be used to trigger immediate processing.
func (db *DB) EmbedExisting(ctx context.Context) (int, error) {
	if db.embedQueue == nil {
		return 0, fmt.Errorf("auto-embed not enabled")
	}
	db.embedQueue.TriggerImmediate()
	return 0, nil // Worker will process in background
}

// ResetEmbedWorker stops the current embedding worker and restarts it fresh.
// This clears all worker state (processed counts, recently-processed cache),
// which is necessary when regenerating all embeddings after ClearAllEmbeddings.
func (db *DB) ResetEmbedWorker() error {
	if db.embedQueue == nil {
		return fmt.Errorf("auto-embed not enabled")
	}
	db.embedQueue.Reset()
	return nil
}

// ClearAllEmbeddings removes embeddings from all nodes, allowing them to be regenerated.
// This is useful for re-embedding with a new model or fixing corrupted embeddings.
// It clears both the node embeddings in storage AND the search index.
func (db *DB) ClearAllEmbeddings() (int, error) {
	// First, clear the search service's vector index
	// This ensures EmbeddingCount() returns 0 immediately
	if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
		svc.ClearVectorIndex()
	}

	// Unwrap storage layers to find the BadgerEngine
	// The storage chain can be: AsyncEngine -> WALEngine -> BadgerEngine
	engine := db.storage
	idPrefix := ""

	// Unwrap NamespacedEngine first (DB instances are scoped to a single database namespace)
	if namespacedEngine, ok := engine.(*storage.NamespacedEngine); ok {
		idPrefix = namespacedEngine.Namespace() + ":"
		engine = namespacedEngine.GetInnerEngine()
	}

	// Unwrap AsyncEngine if present
	if asyncEngine, ok := engine.(*storage.AsyncEngine); ok {
		engine = asyncEngine.GetEngine()
	}

	// Unwrap WALEngine if present
	if walEngine, ok := engine.(*storage.WALEngine); ok {
		engine = walEngine.GetEngine()
	}

	// Now check if we have a BadgerEngine
	if badgerStorage, ok := engine.(*storage.BadgerEngine); ok {
		if idPrefix != "" {
			return badgerStorage.ClearAllEmbeddingsForPrefix(idPrefix)
		}
		return badgerStorage.ClearAllEmbeddings()
	}

	return 0, fmt.Errorf("storage engine does not support ClearAllEmbeddings")
}

// embedQueryWithEmbedder runs the shared query-embedding logic (chunking + average) with the given embedder.
func (db *DB) embedQueryWithEmbedder(ctx context.Context, emb embed.Embedder, query string) ([]float32, error) {
	if emb == nil {
		return nil, nil
	}
	chunks, err := db.chunkQueryWithEmbedder(ctx, emb, query)
	if err != nil {
		return nil, err
	}
	if len(chunks) <= 1 {
		return emb.Embed(ctx, query)
	}
	const (
		maxQueryChunks = 32
	)
	if len(chunks) > maxQueryChunks {
		chunks = chunks[:maxQueryChunks]
	}
	embs, err := emb.EmbedBatch(ctx, chunks)
	if len(embs) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	var sum []float32
	var count int
	for _, v := range embs {
		if len(v) == 0 {
			continue
		}
		if sum == nil {
			sum = make([]float32, len(v))
		}
		if len(v) != len(sum) {
			continue
		}
		for i := range v {
			sum[i] += v[i]
		}
		count++
	}
	if count == 0 {
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	inv := float32(1.0 / float32(count))
	for i := range sum {
		sum[i] *= inv
	}
	vector.NormalizeInPlace(sum)
	return sum, nil
}

func (db *DB) chunkQueryWithEmbedder(ctx context.Context, emb embed.Embedder, query string) ([]string, error) {
	if emb == nil {
		return nil, nil
	}
	const (
		queryChunkSize    = 512
		queryChunkOverlap = 50
		maxQueryChunks    = 32
	)
	chunks, err := emb.ChunkText(query, queryChunkSize, queryChunkOverlap)
	if err != nil {
		return nil, err
	}
	if len(chunks) > maxQueryChunks {
		chunks = chunks[:maxQueryChunks]
	}
	return chunks, nil
}

// EmbedQuery generates an embedding for a search query.
// Returns nil if embeddings are not enabled or embedder is not yet set (e.g. still loading).
func (db *DB) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	if db.embedQueue == nil || db.embedQueue.embedder == nil {
		return nil, nil // Not an error - just no embedding available
	}
	return db.embedQueryWithEmbedder(ctx, db.embedQueue.embedder, query)
}

// ChunkQuery splits a search query using the configured global embedder.
func (db *DB) ChunkQuery(ctx context.Context, query string) ([]string, error) {
	if db.embedQueue == nil || db.embedQueue.embedder == nil {
		return []string{query}, nil
	}
	return db.chunkQueryWithEmbedder(ctx, db.embedQueue.embedder, query)
}

// EmbedQueryForDB generates an embedding for a search query using the embedder for the given database.
// When SetEmbedConfigForDB is set, the per-DB embedder registry is used so the query vector matches
// the index for that database. Otherwise it uses the global embedder and validates dimensions match
// (returns ErrQueryEmbeddingDimensionMismatch if not).
func (db *DB) EmbedQueryForDB(ctx context.Context, dbName string, query string) ([]float32, error) {
	db.mu.RLock()
	useRegistry := db.embedConfigForDB != nil
	db.mu.RUnlock()
	if useRegistry {
		emb, err := db.getOrCreateEmbedderForDB(dbName)
		if err != nil {
			return nil, err
		}
		if emb != nil {
			return db.embedQueryWithEmbedder(ctx, emb, query)
		}
	}
	// No registry or fallback: use global embedder and dimension check (legacy behavior).
	vec, err := db.EmbedQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	db.dbConfigResolverMu.RLock()
	resolver := db.dbConfigResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver == nil || len(vec) == 0 {
		return vec, nil
	}
	resolvedDims, _, _ := resolver(dbName)
	if resolvedDims <= 0 {
		return vec, nil
	}
	if len(vec) != resolvedDims {
		return nil, fmt.Errorf("database %q: %w (index dims %d, query dims %d)",
			dbName, ErrQueryEmbeddingDimensionMismatch, resolvedDims, len(vec))
	}
	return vec, nil
}

// ChunkQueryForDB splits a search query using the embedder configured for the given database.
func (db *DB) ChunkQueryForDB(ctx context.Context, dbName string, query string) ([]string, error) {
	db.mu.RLock()
	useRegistry := db.embedConfigForDB != nil
	db.mu.RUnlock()
	if useRegistry {
		emb, err := db.getOrCreateEmbedderForDB(dbName)
		if err != nil {
			return nil, err
		}
		if emb != nil {
			return db.chunkQueryWithEmbedder(ctx, emb, query)
		}
	}
	return db.ChunkQuery(ctx, query)
}

// DecayInfo contains decay system information for monitoring.
type DecayInfo struct {
	Enabled             bool          `json:"enabled"`
	VisibilityThreshold float64       `json:"visibilityThreshold"`
	FlushInterval       time.Duration `json:"flushInterval"`
}

// GetDecayInfo returns information about the decay system configuration.
func (db *DB) GetDecayInfo() *DecayInfo {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.config.Memory.DecayEnabled {
		return &DecayInfo{Enabled: false}
	}
	return &DecayInfo{
		Enabled:             true,
		VisibilityThreshold: db.config.Memory.VisibilityThreshold,
		FlushInterval:       db.config.Memory.DecayInterval,
	}
}

// GetCypherExecutor returns the Cypher executor for this database.
// This is used by GraphQL and other integrations that need direct access to the executor.
func (db *DB) GetCypherExecutor() *cypher.StorageExecutor {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.cypherExecutor
}

// GetEmbedQueue returns the embedding queue for this database.
// This is used by GraphQL and other integrations that need to wire up
// embedding callbacks for namespaced executors.
func (db *DB) GetEmbedQueue() *EmbedQueue {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.embedQueue
}

// GetAccessFlusher returns the active knowledge-policy AccessFlusher, or
// nil when decay is disabled / not yet constructed. Exposed so the
// observability layer can install a passive-scrape callback reading
// flusher.BufferFullness().
func (db *DB) GetAccessFlusher() *knowledgepolicy.AccessFlusher {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.accessFlusher
}

// GetReplicator returns the active replicator (nil when running in
// standalone mode or before maybeEnableReplication has run). Used by
// cmd/nornicdb to inject Plan-04-06 ReplicationMetrics via the
// replication.MetricsAware optional interface.
func (db *DB) GetReplicator() replication.Replicator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.replicator
}

// Cypher executes a Cypher query.
func (db *DB) Cypher(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Execute query through Cypher executor
	result, err := db.cypherExecutor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}

	// Convert to []map[string]any format
	results := make([]map[string]any, len(result.Rows))
	for i, row := range result.Rows {
		results[i] = make(map[string]any)
		for j, col := range result.Columns {
			if j < len(row) {
				results[i][col] = row[j]
			}
		}
	}

	return results, nil
}

// unwrapToBadgerEngine walks the engine wrapper chain to find the underlying BadgerEngine.
func unwrapToBadgerEngine(eng storage.Engine) *storage.BadgerEngine {
	for {
		switch e := eng.(type) {
		case *storage.BadgerEngine:
			return e
		case interface{ GetInnerEngine() storage.Engine }:
			eng = e.GetInnerEngine()
		case interface{ UnwrapEngine() storage.Engine }:
			eng = e.UnwrapEngine()
		default:
			return nil
		}
	}
}
