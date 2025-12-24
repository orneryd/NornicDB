// Package nornicdb provides the main API for embedded NornicDB usage.
//
// This package implements the core NornicDB database API, providing a high-level
// interface for storing, retrieving, and searching memories (nodes) with automatic
// relationship inference, memory decay, and hybrid search capabilities.
//
// Key Features:
//   - Memory storage with automatic decay simulation
//   - Vector similarity search using pre-computed embeddings
//   - Full-text search with BM25 scoring
//   - Automatic relationship inference
//   - Cypher query execution
//   - Neo4j compatibility
//
// Architecture:
//   - Storage: In-memory graph storage (Badger planned)
//   - Decay: Simulates human memory decay patterns
//   - Inference: Automatic relationship detection
//   - Search: Hybrid vector + full-text search
//   - Cypher: Neo4j-compatible query language
//
// Example Usage:
//
//	// Open database
//	config := nornicdb.DefaultConfig()
//	config.DecayEnabled = true
//	config.AutoLinksEnabled = true
//
//	db, err := nornicdb.Open("./data", config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Store a memory
//	memory := &nornicdb.Memory{
//		Content:   "Machine learning is a subset of artificial intelligence",
//		Title:     "ML Definition",
//		Tier:      nornicdb.TierSemantic,
//		Tags:      []string{"AI", "ML", "definition"},
//		Embedding: embedding, // Pre-computed from Mimir
//	}
//
//	stored, err := db.Store(ctx, memory)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Search memories
//	results, err := db.Search(ctx, "artificial intelligence", 10)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for _, result := range results {
//		fmt.Printf("Found: %s (score: %.3f)\n", result.Title, result.Score)
//	}
//
//	// Execute Cypher queries
//	cypherResult, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n)", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Total nodes: %v\n", cypherResult.Rows[0][0])
//
// Memory Tiers:
//
// NornicDB simulates human memory with three tiers based on cognitive science:
//
// 1. **Episodic** (7-day half-life):
//   - Personal experiences and events
//   - "I went to the store yesterday"
//   - Decays quickly unless reinforced
//
// 2. **Semantic** (69-day half-life):
//   - Facts, concepts, and general knowledge
//   - "Paris is the capital of France"
//   - More stable, slower decay
//
// 3. **Procedural** (693-day half-life):
//   - Skills, procedures, and patterns
//   - "How to ride a bicycle"
//   - Very stable, minimal decay
//
// Integration with Mimir:
//
// NornicDB is designed to work with Mimir (the file indexing system):
//   - Mimir: File discovery, reading, embedding generation
//   - NornicDB: Storage, search, relationships, decay
//   - Clean separation of concerns
//   - Embeddings are pre-computed by Mimir and passed to NornicDB
//
// Data Flow:
//  1. Mimir discovers and reads files
//  2. Mimir generates embeddings via Ollama/OpenAI
//  3. Mimir sends nodes with embeddings to NornicDB
//  4. NornicDB stores, indexes, and infers relationships
//  5. Applications query NornicDB for search and retrieval
//
// ELI12 (Explain Like I'm 12):
//
// Think of NornicDB like your brain's memory system:
//
//  1. **Different types of memories**: Just like you remember your birthday party
//     differently than how to tie your shoes, NornicDB has different "tiers"
//     for different kinds of information.
//
//  2. **Memories fade over time**: Just like you might forget what you had for
//     lunch last Tuesday, old memories in NornicDB get "weaker" over time
//     unless you access them again.
//
//  3. **Finding related memories**: When you think of "summer", you might
//     remember "beach", "swimming", and "ice cream". NornicDB automatically
//     finds these connections between related information.
//
//  4. **Smart search**: You can ask NornicDB "find me something about dogs"
//     and it will find information about "puppies", "canines", and "pets"
//     even if those exact words aren't in your search.
//
// It's like having a super-smart assistant that remembers everything you tell
// it and can find connections you might not have noticed!
package nornicdb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/decay"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/encryption"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Errors returned by DB operations.
var (
	ErrNotFound     = errors.New("memory not found")
	ErrInvalidID    = errors.New("invalid memory ID")
	ErrClosed       = errors.New("database is closed")
	ErrInvalidInput = errors.New("invalid input")
)

// MemoryTier represents the decay tier of a memory.
type MemoryTier string

const (
	// TierEpisodic is for short-term memories (7-day half-life)
	TierEpisodic MemoryTier = "EPISODIC"
	// TierSemantic is for facts and concepts (69-day half-life)
	TierSemantic MemoryTier = "SEMANTIC"
	// TierProcedural is for skills and patterns (693-day half-life)
	TierProcedural MemoryTier = "PROCEDURAL"
)

// Memory represents a node in the NornicDB knowledge graph.
//
// Memories are the fundamental unit of storage in NornicDB, representing
// pieces of information with associated metadata, embeddings, and decay scores.
//
// Key fields:
//   - Content: The main textual content
//   - Tier: Memory type (Episodic, Semantic, Procedural)
//   - DecayScore: Current strength (1.0 = new, 0.0 = fully decayed)
//   - Embedding: Vector representation for similarity search
//   - Tags: Categorical labels for organization
//
// Example:
//
//	// Create a semantic memory
//	memory := &nornicdb.Memory{
//		Content:   "The mitochondria is the powerhouse of the cell",
//		Title:     "Cell Biology Fact",
//		Tier:      nornicdb.TierSemantic,
//		Tags:      []string{"biology", "cells", "education"},
//		Source:    "textbook-chapter-3",
//		Embedding: embedding, // From Mimir
//		Properties: map[string]any{
//			"subject":    "biology",
//			"difficulty": "beginner",
//		},
//	}
//
//	stored, err := db.Store(ctx, memory)
//
// Memory Lifecycle:
//  1. Created with DecayScore = 1.0
//  2. DecayScore decreases over time based on tier
//  3. AccessCount increases when retrieved
//  4. LastAccessed updated on each access
//  5. Archived when DecayScore < threshold
type Memory struct {
	ID              string         `json:"id"`
	Content         string         `json:"content"`
	Title           string         `json:"title,omitempty"`
	Tier            MemoryTier     `json:"tier"`
	DecayScore      float64        `json:"decay_score"`
	CreatedAt       time.Time      `json:"created_at"`
	LastAccessed    time.Time      `json:"last_accessed"`
	AccessCount     int64          `json:"access_count"`
	ChunkEmbeddings [][]float32    `json:"chunk_embeddings,omitempty"` // Always stored as array of arrays (even single chunk = array of 1)
	Tags            []string       `json:"tags,omitempty"`
	Source          string         `json:"source,omitempty"`
	Properties      map[string]any `json:"properties,omitempty"`
}

// Edge represents a relationship between two memories in the knowledge graph.
//
// Edges can be manually created or automatically inferred by the relationship
// inference engine. They include confidence scores and reasoning information.
//
// Types of relationships:
//   - Manual: Explicitly created by users
//   - Similarity: Based on vector embedding similarity
//   - CoAccess: Based on memories accessed together
//   - Temporal: Based on creation/access time proximity
//   - Transitive: Inferred through other relationships
//
// Example:
//
//	// Manual relationship
//	edge := &nornicdb.Edge{
//		SourceID:      "memory-1",
//		TargetID:      "memory-2",
//		Type:          "RELATES_TO",
//		Confidence:    1.0,
//		AutoGenerated: false,
//		Reason:        "User-defined relationship",
//		Properties: map[string]any{
//			"strength": "strong",
//			"category": "conceptual",
//		},
//	}
//
//	// Auto-generated relationship (from inference engine)
//	edge = &nornicdb.Edge{
//		SourceID:      "memory-3",
//		TargetID:      "memory-4",
//		Type:          "SIMILAR_TO",
//		Confidence:    0.87,
//		AutoGenerated: true,
//		Reason:        "Vector similarity: 0.87",
//	}
type Edge struct {
	ID            string         `json:"id"`
	SourceID      string         `json:"source_id"`
	TargetID      string         `json:"target_id"`
	Type          string         `json:"type"`
	Confidence    float64        `json:"confidence"`
	AutoGenerated bool           `json:"auto_generated"`
	Reason        string         `json:"reason,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	Properties    map[string]any `json:"properties,omitempty"`
}

// Config holds NornicDB database configuration options.
//
// The configuration controls all aspects of the database including storage,
// embeddings, memory decay, automatic relationship inference, and server ports.
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
//		DecayRecalculateInterval:     30 * time.Minute,
//		DecayArchiveThreshold:        0.01, // Archive at 1%
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
type Config struct {
	// Storage
	DataDir string `yaml:"data_dir"`

	// Embeddings
	EmbeddingProvider     string  `yaml:"embedding_provider"` // ollama, openai
	EmbeddingAPIURL       string  `yaml:"embedding_api_url"`
	EmbeddingAPIKey       string  `yaml:"embedding_api_key"` // API key (use dummy for llama.cpp)
	EmbeddingModel        string  `yaml:"embedding_model"`
	EmbeddingDimensions   int     `yaml:"embedding_dimensions"`
	AutoEmbedEnabled      bool    `yaml:"auto_embed_enabled"`       // Auto-generate embeddings on node create/update
	EmbedWorkerNumWorkers int     `yaml:"embed_worker_num_workers"` // Number of concurrent embedding workers (default: 1, use more for network/parallel processing)
	SearchMinSimilarity   float64 `yaml:"search_min_similarity"`    // Min cosine similarity for vector search (0.0 = no filter)

	// Decay
	DecayEnabled             bool          `yaml:"decay_enabled"`
	DecayRecalculateInterval time.Duration `yaml:"decay_recalculate_interval"`
	DecayArchiveThreshold    float64       `yaml:"decay_archive_threshold"`

	// Auto-linking
	AutoLinksEnabled             bool          `yaml:"auto_links_enabled"`
	AutoLinksSimilarityThreshold float64       `yaml:"auto_links_similarity_threshold"`
	AutoLinksCoAccessWindow      time.Duration `yaml:"auto_links_co_access_window"`

	// Parallel execution
	ParallelEnabled      bool `yaml:"parallel_enabled"`        // Enable parallel query execution
	ParallelMaxWorkers   int  `yaml:"parallel_max_workers"`    // Max worker goroutines (0 = auto, uses runtime.NumCPU())
	ParallelMinBatchSize int  `yaml:"parallel_min_batch_size"` // Min items before parallelizing (default: 1000)

	// Async writes (eventual consistency)
	AsyncWritesEnabled    bool          `yaml:"async_writes_enabled"`      // Enable async writes for faster performance
	AsyncFlushInterval    time.Duration `yaml:"async_flush_interval"`      // How often to flush pending writes (default: 50ms)
	AsyncMaxNodeCacheSize int           `yaml:"async_max_node_cache_size"` // Max nodes to buffer before forcing flush (default: 50000, 0=unlimited)
	AsyncMaxEdgeCacheSize int           `yaml:"async_max_edge_cache_size"` // Max edges to buffer before forcing flush (default: 100000, 0=unlimited)

	// BadgerEngine in-process caches (hot read paths)
	BadgerNodeCacheMaxEntries   int `yaml:"badger_node_cache_max_entries"`    // Max hot nodes cached in-process (default: 10000)
	BadgerEdgeTypeCacheMaxTypes int `yaml:"badger_edge_type_cache_max_types"` // Max edge types cached for GetEdgesByType (default: 50)

	// Encryption (data-at-rest) - AES-256 full database encryption
	// Disabled by default for performance. Enable for HIPAA/GDPR/SOC2 compliance.
	// When enabled, ALL data is encrypted at the storage level - all or nothing.
	EncryptionEnabled  bool   `yaml:"encryption_enabled"`  // Enable AES-256 encryption for entire database
	EncryptionPassword string `yaml:"encryption_password"` // Master password for key derivation (env: NORNICDB_ENCRYPTION_PASSWORD)

	// Server
	BoltPort int `yaml:"bolt_port"`
	HTTPPort int `yaml:"http_port"`

	// Plugins
	PluginsDir string `yaml:"plugins_dir"` // Directory for APOC plugins

	// Memory management
	LowMemoryMode bool `yaml:"low_memory_mode"` // Use minimal RAM (for containers with limited memory)

	// K-means clustering
	KmeansClusterInterval time.Duration `yaml:"kmeans_cluster_interval"` // How often to run k-means (0 = disabled, default 15m)
}

// DefaultConfig returns sensible default configuration for NornicDB.
//
// The defaults are optimized for development and small-scale deployments:
//   - Local Ollama for embeddings (bge-m3 model)
//   - Memory decay enabled with 1-hour recalculation
//   - Auto-linking enabled with 0.82 similarity threshold
//   - Standard Neo4j ports (7687 Bolt, 7474 HTTP)
//
// Example:
//
//	config := nornicdb.DefaultConfig()
//	// Customize as needed
//	config.EmbeddingModel = "nomic-embed-text"
//	config.DecayArchiveThreshold = 0.1 // Archive at 10%
//
//	db, err := nornicdb.Open("./data", config)
func DefaultConfig() *Config {
	return &Config{
		DataDir:                      "./data",
		EmbeddingProvider:            "openai", // Use OpenAI-compatible endpoint (llama.cpp, vLLM, etc.)
		EmbeddingAPIURL:              "http://localhost:11434",
		EmbeddingAPIKey:              "not-needed", // Dummy key for llama.cpp (doesn't validate)
		EmbeddingModel:               "bge-m3",
		EmbeddingDimensions:          1024,
		AutoEmbedEnabled:             true, // Auto-generate embeddings on node creation
		EmbedWorkerNumWorkers:        1,    // Single worker by default, increase for network-based embedders or multiple GPUs
		DecayEnabled:                 true,
		DecayRecalculateInterval:     time.Hour,
		DecayArchiveThreshold:        0.05,
		AutoLinksEnabled:             true,
		AutoLinksSimilarityThreshold: 0.82,
		AutoLinksCoAccessWindow:      30 * time.Second,
		ParallelEnabled:              true,                  // Enable parallel query execution by default
		ParallelMaxWorkers:           0,                     // 0 = auto (runtime.NumCPU())
		ParallelMinBatchSize:         1000,                  // Parallelize for 1000+ items
		AsyncWritesEnabled:           true,                  // Enable async writes for eventual consistency (faster writes)
		AsyncFlushInterval:           50 * time.Millisecond, // Flush pending writes every 50ms
		AsyncMaxNodeCacheSize:        50000,                 // Buffer up to 50K nodes before forcing flush (~35MB)
		AsyncMaxEdgeCacheSize:        100000,                // Buffer up to 100K edges before forcing flush (~50MB)
		BadgerNodeCacheMaxEntries:    10000,                 // Cache up to 10K hot nodes
		BadgerEdgeTypeCacheMaxTypes:  50,                    // Cache up to 50 distinct edge types
		EncryptionEnabled:            false,                 // Encryption disabled by default (opt-in)
		EncryptionPassword:           "",                    // Must be set if encryption enabled
		BoltPort:                     7687,
		HTTPPort:                     7474,
		KmeansClusterInterval:        15 * time.Minute, // Run k-means every 15 min (skips if no changes)
	}
}

// DB represents a NornicDB database instance with all core functionality.
//
// The DB provides a high-level API for storing memories, executing queries,
// and performing hybrid search. It coordinates between storage, decay management,
// relationship inference, and search services.
//
// Key components:
//   - Storage: Graph storage engine (currently in-memory)
//   - Decay: Memory decay simulation
//   - Inference: Automatic relationship detection
//   - Search: Hybrid vector + full-text search
//   - Cypher: Query execution engine
//
// Example:
//
//	db, err := nornicdb.Open("./data", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Database is ready for operations
//	memory := &nornicdb.Memory{
//		Content: "Important information",
//		Tier:    nornicdb.TierSemantic,
//	}
//	stored, _ := db.Store(ctx, memory)
//
// Thread Safety:
//
//	All methods are thread-safe and can be called concurrently.
type DB struct {
	config *Config
	mu     sync.RWMutex
	closed bool

	// Internal components
	storage        storage.Engine // Namespaced storage for default database (all DB operations use this)
	baseStorage    storage.Engine // Underlying storage chain (must be closed to release Badger/WAL locks)
	wal            *storage.WAL   // Write-ahead log for durability
	decay          *decay.Manager
	cypherExecutor *cypher.StorageExecutor
	gpuManager     interface{} // *gpu.Manager - interface to avoid circular import

	// Search services (per database) using pre-computed embeddings.
	embeddingDims       int     // Effective vector index dimensions in use
	searchMinSimilarity float64 // Default MinSimilarity threshold for search
	searchServicesMu    sync.RWMutex
	searchServices      map[string]*dbSearchService

	// Per-database inference engines (topology, Kalman, etc.).
	inferenceServicesMu sync.RWMutex
	inferenceServices   map[string]*inference.Engine

	// Async embedding queue for auto-generating embeddings
	embedQueue        *EmbedQueue
	embedWorkerConfig *EmbedWorkerConfig // Configurable via ENV vars

	// K-means clustering timer (runs on schedule instead of trigger)
	clusterTicker           *time.Ticker
	clusterTickerStop       chan struct{}
	lastClusteredEmbedCount int // Track embedding count at last clustering

	// Encryption flag - when true, all data is encrypted at BadgerDB level
	encryptionEnabled bool

	// Background goroutine tracking
	bgWg sync.WaitGroup
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
//  4. Sets up memory decay manager (if enabled in config)
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
//	// Store a memory
//	memory := &nornicdb.Memory{
//		Content: "Important fact",
//		Tier:    nornicdb.TierSemantic,
//	}
//	stored, _ := db.Store(context.Background(), memory)
//	fmt.Printf("Stored memory: %s\n", stored.ID)
//
// Example (Production Setup):
//
//	// Production configuration
//	config := nornicdb.DefaultConfig()
//	config.DataDir = "/var/lib/nornicdb"
//	config.DecayEnabled = true
//	config.DecayRecalculateInterval = 30 * time.Minute
//	config.DecayArchiveThreshold = 0.01 // Archive at 1%
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
//	// Actual embedding computation done by Mimir
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
//		// Re-embed with new model (done by Mimir)
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
//   - You can add books (Store memories)
//   - Search for books (Search queries)
//   - Find related books (Relationship inference)
//   - Old books get moved to archive (Decay simulation)
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
	config.DataDir = dataDir

	db := &DB{
		config: config,
	}

	// Initialize storage - use BadgerEngine for persistence, MemoryEngine for testing
	if dataDir != "" {
		// Configure BadgerDB based on memory mode
		// HighPerformance uses ~1GB RAM, LowMemory uses ~50MB
		badgerOpts := storage.BadgerOptions{
			DataDir:               dataDir,
			HighPerformance:       !config.LowMemoryMode,
			LowMemory:             config.LowMemoryMode,
			NodeCacheMaxEntries:   config.BadgerNodeCacheMaxEntries,
			EdgeTypeCacheMaxTypes: config.BadgerEdgeTypeCacheMaxTypes,
		}
		if config.LowMemoryMode {
			fmt.Println("⚡ Using low-memory storage mode (reduced RAM usage)")
		}

		// Require password if encryption is enabled
		if config.EncryptionEnabled && config.EncryptionPassword == "" {
			return nil, fmt.Errorf("encryption is enabled but no password was provided")
		}

		// Enable BadgerDB-level encryption at rest if configured
		if config.EncryptionEnabled {
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
			encryptionKey := encryption.DeriveKey([]byte(config.EncryptionPassword), salt, 600000)
			badgerOpts.EncryptionKey = encryptionKey
			db.encryptionEnabled = true
			fmt.Println("🔒 Database encryption enabled (AES-256)")
		}

		badgerEngine, err := storage.NewBadgerEngineWithOptions(badgerOpts)
		if err != nil {
			// Check for encryption-related errors and provide helpful messages
			// IMPORTANT: BadgerDB fails safely - data files are NOT touched or corrupted
			errStr := err.Error()
			if strings.Contains(errStr, "encryption") || strings.Contains(errStr, "decrypt") || strings.Contains(errStr, "cipher") ||
				strings.Contains(errStr, "Invalid checksum") || strings.Contains(errStr, "MANIFEST") {
				if config.EncryptionEnabled {
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
			return nil, fmt.Errorf("failed to open persistent storage: %w", err)
		}

		// Initialize WAL for durability (uses batch sync mode by default for better performance)
		walConfig := storage.DefaultWALConfig()
		walConfig.Dir = dataDir + "/wal"
		walConfig.SnapshotInterval = 5 * time.Minute // Compact WAL every 5 minutes (not 1 hour!)
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
		snapshotDir := dataDir + "/snapshots"
		if err := walEngine.EnableAutoCompaction(snapshotDir); err != nil {
			fmt.Printf("⚠️  WAL auto-compaction failed to enable: %v\n", err)
		} else {
			fmt.Printf("🗜️  WAL auto-compaction enabled (snapshot interval: %v)\n", walConfig.SnapshotInterval)
		}

		// Optionally wrap with AsyncEngine for faster writes (eventual consistency)
		var baseStorage storage.Engine
		if config.AsyncWritesEnabled {
			asyncConfig := &storage.AsyncEngineConfig{
				FlushInterval:    config.AsyncFlushInterval,
				MaxNodeCacheSize: config.AsyncMaxNodeCacheSize,
				MaxEdgeCacheSize: config.AsyncMaxEdgeCacheSize,
			}
			baseStorage = storage.NewAsyncEngine(walEngine, asyncConfig)
			if config.AsyncMaxNodeCacheSize > 0 || config.AsyncMaxEdgeCacheSize > 0 {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v, node cache: %d, edge cache: %d)\n",
					dataDir, config.AsyncFlushInterval, config.AsyncMaxNodeCacheSize, config.AsyncMaxEdgeCacheSize)
			} else {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v)\n", dataDir, config.AsyncFlushInterval)
			}
		} else {
			baseStorage = walEngine
			fmt.Printf("📂 Using persistent storage at %s (WAL enabled, batch sync)\n", dataDir)
		}

		// Track the underlying storage chain so Close() can release Badger directory locks.
		db.baseStorage = baseStorage

		// Wrap base storage with NamespacedEngine for the default database ("nornic")
		// This ensures everything uses namespaced storage - no direct base storage access
		// Get default database name from global config (same as server does)
		globalConfig := nornicConfig.LoadFromEnv()
		defaultDBName := globalConfig.Database.DefaultDatabase
		if defaultDBName == "" {
			defaultDBName = "nornic" // Fallback to "nornic" if not configured
		}
		db.storage = storage.NewNamespacedEngine(baseStorage, defaultDBName)
		fmt.Printf("📦 Wrapped storage with namespace '%s' (all operations are namespaced)\n", defaultDBName)
	} else {
		baseStorage := storage.NewMemoryEngine()
		fmt.Println("⚠️  Using in-memory storage (data will not persist)")

		// Track the underlying storage chain so Close() can cleanly shut down goroutines/locks.
		db.baseStorage = baseStorage

		// Wrap in-memory storage with NamespacedEngine for consistency
		// Get default database name from global config (same as server does)
		globalConfig := nornicConfig.LoadFromEnv()
		defaultDBName := globalConfig.Database.DefaultDatabase
		if defaultDBName == "" {
			defaultDBName = "nornic" // Fallback to "nornic" if not configured
		}
		db.storage = storage.NewNamespacedEngine(baseStorage, defaultDBName)
		fmt.Printf("📦 Wrapped in-memory storage with namespace '%s' (all operations are namespaced)\n", defaultDBName)
	}

	// Initialize Cypher executor
	db.cypherExecutor = cypher.NewStorageExecutor(db.storage)

	// Configure executor with embedding dimensions for vector index creation
	if config.EmbeddingDimensions > 0 {
		db.cypherExecutor.SetDefaultEmbeddingDimensions(config.EmbeddingDimensions)
	}

	// Load function plugins from configured directory
	// Heimdall plugins will be loaded later by the server after Heimdall is initialized
	if db.config.PluginsDir != "" {
		if err := LoadPluginsFromDir(db.config.PluginsDir, nil); err != nil {
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
		Enabled:      config.ParallelEnabled,
		MaxWorkers:   config.ParallelMaxWorkers,
		MinBatchSize: config.ParallelMinBatchSize,
	}
	// If MaxWorkers is 0, the parallel package will use runtime.NumCPU()
	cypher.SetParallelConfig(parallelCfg)

	// Initialize decay manager
	if config.DecayEnabled {
		decayConfig := &decay.Config{
			RecalculateInterval: config.DecayRecalculateInterval,
			ArchiveThreshold:    config.DecayArchiveThreshold,
			RecencyWeight:       0.4,
			FrequencyWeight:     0.3,
			ImportanceWeight:    0.3,
		}
		db.decay = decay.New(decayConfig)
	}

	// Initialize inference engine
	if config.AutoLinksEnabled {
		db.inferenceServices = make(map[string]*inference.Engine)
		// Eagerly create default DB inference for parity with prior behavior.
		if _, err := db.getOrCreateInferenceService(db.defaultDatabaseName(), db.storage); err != nil {
			return nil, fmt.Errorf("init inference: %w", err)
		}
	}

	// Initialize embedding worker config from main config
	db.embedWorkerConfig = &EmbedWorkerConfig{
		NumWorkers:           config.EmbedWorkerNumWorkers,
		ScanInterval:         15 * time.Minute,       // Scan for missed nodes every 15 minutes
		BatchDelay:           500 * time.Millisecond, // Delay between processing nodes
		MaxRetries:           3,
		ChunkSize:            512,
		ChunkOverlap:         50,
		ClusterDebounceDelay: 30 * time.Second, // Wait 30s after last embedding before k-means
		ClusterMinBatchSize:  10,               // Need at least 10 embeddings to trigger k-means
	}

	// Initialize search service config (per-database services are created lazily).
	embeddingDims := config.EmbeddingDimensions
	if embeddingDims <= 0 {
		embeddingDims = 1024 // Default for bge-m3 / mxbai-embed-large
	}
	db.embeddingDims = embeddingDims
	db.searchMinSimilarity = config.SearchMinSimilarity
	db.searchServices = make(map[string]*dbSearchService)
	log.Printf("🔍 Search services enabled (lazy per-database init, %d-dimension vector index)", embeddingDims)

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
		notifier.OnNodeCreated(func(node *storage.Node) { db.indexNodeFromEvent(node) })
		notifier.OnNodeUpdated(func(node *storage.Node) { db.indexNodeFromEvent(node) })
		notifier.OnNodeDeleted(func(nodeID storage.NodeID) { db.removeNodeFromEvent(nodeID) })
	}

	// Also register for async-cache delete notifications if the async layer exists.
	// This handles the case where a node is created and then deleted while still
	// buffered in AsyncEngine (so the inner engine never emits a delete event).
	if asyncNotifier != nil {
		asyncNotifier.OnNodeDeleted(func(nodeID storage.NodeID) { db.removeNodeFromEvent(nodeID) })
	}

	// Enable k-means clustering if feature flag is set (applied lazily per database).
	if nornicConfig.IsGPUClusteringEnabled() {
		fmt.Println("🔬 K-means clustering enabled (per-database, lazy init)")
	}

	// Note: Database encryption is now handled at the BadgerDB storage level (see above)
	// When encryption is enabled, ALL data is encrypted at rest using AES-256.

	// Build search indexes for the default DB in background (other DBs are lazy).
	defaultDBName := db.defaultDatabaseName()
	db.bgWg.Add(1)
	go func() {
		defer db.bgWg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		// EnsureSearchIndexesBuilt also lazily creates the per-DB service entry.
		// Without this, the background build can run before the service is created
		// (e.g., before any request hits EmbeddingCount/Search), leaving the vector
		// index empty until a manual rebuild/regenerate happens.
		if _, err := db.EnsureSearchIndexesBuilt(ctx, defaultDBName, db.storage); err != nil {
			fmt.Printf("⚠️  Failed to build search indexes: %v\n", err)
		} else {
			fmt.Println("✅ Search indexes built from existing data")

			// Trigger k-means clustering if enabled
			db.runClusteringOnceAllDatabases()
		}
	}()

	// Note: Auto-embed queue is initialized via SetEmbedder() after the server creates
	// the embedder. This avoids duplicate embedder creation and ensures consistency
	// between search embeddings and auto-embed.

	return db, nil
}

// SetEmbedder configures the auto-embed queue with the given embedder.
// This should be called by the server after creating a working embedder.
// The embedder is shared with the MCP server and Cypher executor for consistency.
func (db *DB) SetEmbedder(embedder embed.Embedder) {
	if embedder == nil {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.baseStorage == nil {
		// baseStorage is required for correctness (it’s the only engine that can see all namespaces).
		// If this ever happens, initialization order is broken.
		panic("nornicdb: baseStorage is nil in SetEmbedder")
	}

	// Share embedder with Cypher executor for server-side query embedding
	// This enables: CALL db.index.vector.queryNodes('idx', 10, 'search text')
	if db.cypherExecutor != nil {
		db.cypherExecutor.SetEmbedder(embedder)
	}

	if db.embedQueue != nil {
		// Already created, just update embedder
		db.embedQueue.SetEmbedder(embedder)
		log.Printf("🧠 Embed worker activated with %s (%d dims)",
			embedder.Model(), embedder.Dimensions())
		return
	}

	// Create embed queue against the un-namespaced base storage so it can pull work
	// from ALL databases (node IDs are fully-qualified, e.g. "nornic:<id>").
	db.embedQueue = NewEmbedQueue(embedder, db.baseStorage, db.embedWorkerConfig)

	// Set callback to update search index after embedding.
	// Note: IndexNode is idempotent (uses map keyed by node ID), so if the storage
	// OnNodeUpdated callback also calls IndexNode, no double-counting occurs.
	db.embedQueue.SetOnEmbedded(func(node *storage.Node) {
		db.indexNodeFromEvent(node)
	})

	// Start timer-based k-means clustering instead of trigger-based
	// This runs clustering on a schedule (default every 15 minutes) rather than after each batch
	if nornicConfig.IsGPUClusteringEnabled() {
		interval := db.config.KmeansClusterInterval
		if interval > 0 {
			db.startClusteringTimer(interval)
		} else {
			log.Printf("🔬 K-means clustering enabled (manual trigger only, no timer)")
		}
	}

	// Wire up Cypher executor to trigger embedding queue when nodes are created/updated
	// This ensures nodes created via Cypher queries get embeddings generated
	if db.cypherExecutor != nil {
		db.cypherExecutor.SetNodeCreatedCallback(func(nodeID string) {
			db.embedQueue.Enqueue(nodeID)
		})
	}

	log.Printf("🧠 Auto-embed queue started using %s (%d dims)",
		embedder.Model(), embedder.Dimensions())
}

// LoadFromExport loads data from a Mimir JSON export directory.
// This loads nodes, relationships, and embeddings from the exported files.
func (db *DB) LoadFromExport(ctx context.Context, exportDir string) (*LoadResult, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Use the storage loader
	result, err := storage.LoadFromMimirExport(db.storage, exportDir)
	if err != nil {
		return nil, fmt.Errorf("loading export: %w", err)
	}

	return &LoadResult{
		NodesLoaded:      result.NodesImported,
		EdgesLoaded:      result.EdgesImported,
		EmbeddingsLoaded: result.EmbeddingsLoaded,
	}, nil
}

// LoadResult holds the result of a data load operation.
type LoadResult struct {
	NodesLoaded      int `json:"nodes_loaded"`
	EdgesLoaded      int `json:"edges_loaded"`
	EmbeddingsLoaded int `json:"embeddings_loaded"`
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

// Close closes the database.

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	return db.closeInternal()
}

// closeInternal performs cleanup without requiring the lock.
// Used during initialization failures and normal close.
func (db *DB) closeInternal() error {
	// Stop clustering timer first (before waiting for goroutines)
	db.stopClusteringTimer()

	// Wait for background goroutines to complete
	db.bgWg.Wait()

	var errs []error

	if db.decay != nil {
		db.decay.Stop()
	}

	// Close embed queue gracefully (processes remaining batch)
	if db.embedQueue != nil {
		db.embedQueue.Close()
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

// EmbeddingCount returns the total number of nodes with embeddings.
// This is O(1) - the count is tracked by the vector index.
func (db *DB) EmbeddingCount() int {
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return 0
	}
	return svc.EmbeddingCount()
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
	db.embedQueue.Trigger()
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

// EmbedQuery generates an embedding for a search query.
// Returns nil if embeddings are not enabled.
func (db *DB) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	if db.embedQueue == nil {
		return nil, nil // Not an error - just no embedding available
	}
	return db.embedQueue.embedder.Embed(ctx, query)
}

// Store creates a new memory with automatic relationship inference.
func (db *DB) Store(ctx context.Context, mem *Memory) (*Memory, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	if mem == nil {
		return nil, ErrInvalidInput
	}

	// Set defaults
	if mem.ID == "" {
		mem.ID = generateID("mem")
	}
	if mem.Tier == "" {
		mem.Tier = TierSemantic
	}
	mem.DecayScore = 1.0
	now := time.Now()
	mem.CreatedAt = now
	mem.LastAccessed = now
	mem.AccessCount = 0

	// Convert to storage node
	node := memoryToNode(mem)

	// Encrypt sensitive fields in properties before storage
	node.Properties = db.encryptProperties(node.Properties)

	// Store in storage engine
	_, err := db.storage.CreateNode(node)
	if err != nil {
		return nil, fmt.Errorf("storing memory: %w", err)
	}

	// Run auto-relationship inference if enabled (per-database, default DB here).
	if len(mem.ChunkEmbeddings) > 0 && len(mem.ChunkEmbeddings[0]) > 0 {
		inferEngine, _ := db.getOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
		if inferEngine != nil {
			suggestions, err := inferEngine.OnStore(ctx, mem.ID, mem.ChunkEmbeddings[0]) // Use first chunk for inference
			if err == nil {
				for _, suggestion := range suggestions {
					edge := &storage.Edge{
						ID:            storage.EdgeID(generateID("edge")),
						StartNode:     storage.NodeID(suggestion.SourceID),
						EndNode:       storage.NodeID(suggestion.TargetID),
						Type:          suggestion.Type,
						Confidence:    suggestion.Confidence,
						AutoGenerated: true,
						CreatedAt:     now,
						Properties: map[string]any{
							"reason": suggestion.Reason,
							"method": suggestion.Method,
						},
					}
					_ = db.storage.CreateEdge(edge) // Best effort
				}
			}
		}
	}

	return mem, nil
}

// Remember performs semantic search for memories using embedding.
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) Remember(ctx context.Context, embedding []float32, limit int) ([]*Memory, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if len(embedding) == 0 {
		return nil, ErrInvalidInput
	}

	if limit <= 0 {
		limit = 10
	}

	type scored struct {
		mem   *Memory
		score float64
	}

	// Use streaming iteration to avoid loading all nodes at once
	// We maintain a sorted slice of top-k results
	var results []scored

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(node *storage.Node) error {
		// Skip nodes without embeddings (always stored in ChunkEmbeddings, even single chunk = array of 1)
		if len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0 {
			return nil
		}

		// Decrypt properties before converting to memory
		node.Properties = db.decryptProperties(node.Properties)

		mem := nodeToMemory(node)
		// Use first chunk embedding for similarity (always stored in ChunkEmbeddings, even single chunk = array of 1)
		var memEmb []float32
		if len(mem.ChunkEmbeddings) > 0 && len(mem.ChunkEmbeddings[0]) > 0 {
			memEmb = mem.ChunkEmbeddings[0]
		}
		sim := vector.CosineSimilarity(embedding, memEmb)

		// If we don't have enough results yet, just add
		if len(results) < limit {
			results = append(results, scored{mem: mem, score: sim})
			// Sort when we reach limit
			if len(results) == limit {
				sort.Slice(results, func(i, j int) bool {
					return results[i].score > results[j].score
				})
			}
		} else if sim > results[limit-1].score {
			// Only add if better than worst in results
			results[limit-1] = scored{mem: mem, score: sim}
			// Re-sort (could optimize with heap)
			sort.Slice(results, func(i, j int) bool {
				return results[i].score > results[j].score
			})
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("streaming nodes: %w", err)
	}

	// Final sort (in case we have fewer than limit results)
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	memories := make([]*Memory, len(results))
	for i, r := range results {
		memories[i] = r.mem
	}

	return memories, nil
}

// Recall retrieves a specific memory by ID and reinforces it.
func (db *DB) Recall(ctx context.Context, id string) (*Memory, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	if id == "" {
		return nil, ErrInvalidID
	}

	// Get from storage
	node, err := db.storage.GetNode(storage.NodeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	// Decrypt properties before converting to memory
	node.Properties = db.decryptProperties(node.Properties)

	mem := nodeToMemory(node)

	// Reinforce memory and update access patterns
	now := time.Now()

	if db.decay != nil {
		// Use decay manager to reinforce the memory
		info := &decay.MemoryInfo{
			ID:           mem.ID,
			Tier:         decay.Tier(mem.Tier),
			CreatedAt:    mem.CreatedAt,
			LastAccessed: mem.LastAccessed,
			AccessCount:  mem.AccessCount,
		}
		info = db.decay.Reinforce(info)
		mem.LastAccessed = info.LastAccessed
		mem.AccessCount = info.AccessCount
		mem.DecayScore = db.decay.CalculateScore(info)
	} else {
		mem.LastAccessed = now
		mem.AccessCount++
	}

	// Update storage
	node = memoryToNode(mem)
	if err := db.storage.UpdateNode(node); err != nil {
		return nil, fmt.Errorf("updating memory: %w", err)
	}

	// Track access for co-access inference
	if inferEngine, _ := db.getOrCreateInferenceService(db.defaultDatabaseName(), db.storage); inferEngine != nil {
		inferEngine.OnAccess(ctx, mem.ID)
	}

	return mem, nil
}

// DecayInfo contains decay system information for monitoring.
type DecayInfo struct {
	Enabled          bool          `json:"enabled"`
	ArchiveThreshold float64       `json:"archiveThreshold"`
	RecalcInterval   time.Duration `json:"recalculateInterval"`
	RecencyWeight    float64       `json:"recencyWeight"`
	FrequencyWeight  float64       `json:"frequencyWeight"`
	ImportanceWeight float64       `json:"importanceWeight"`
}

// GetDecayInfo returns information about the decay system configuration.
func (db *DB) GetDecayInfo() *DecayInfo {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.decay == nil {
		return &DecayInfo{Enabled: false}
	}

	cfg := db.decay.GetConfig()
	return &DecayInfo{
		Enabled:          true,
		ArchiveThreshold: cfg.ArchiveThreshold,
		RecalcInterval:   cfg.RecalculateInterval,
		RecencyWeight:    cfg.RecencyWeight,
		FrequencyWeight:  cfg.FrequencyWeight,
		ImportanceWeight: cfg.ImportanceWeight,
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

// Link creates a relationship between two memories.
func (db *DB) Link(ctx context.Context, sourceID, targetID, edgeType string, confidence float64) (*Edge, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	if sourceID == "" || targetID == "" {
		return nil, ErrInvalidID
	}

	if edgeType == "" {
		edgeType = "RELATES_TO"
	}

	if confidence <= 0 || confidence > 1 {
		confidence = 1.0
	}

	// Verify both nodes exist
	if _, err := db.storage.GetNode(storage.NodeID(sourceID)); err != nil {
		return nil, fmt.Errorf("source not found: %w", ErrNotFound)
	}
	if _, err := db.storage.GetNode(storage.NodeID(targetID)); err != nil {
		return nil, fmt.Errorf("target not found: %w", ErrNotFound)
	}

	now := time.Now()
	storageEdge := &storage.Edge{
		ID:            storage.EdgeID(generateID("edge")),
		StartNode:     storage.NodeID(sourceID),
		EndNode:       storage.NodeID(targetID),
		Type:          edgeType,
		Confidence:    confidence,
		AutoGenerated: false,
		CreatedAt:     now,
		Properties:    map[string]any{},
	}

	if err := db.storage.CreateEdge(storageEdge); err != nil {
		return nil, fmt.Errorf("creating edge: %w", err)
	}

	return storageEdgeToEdge(storageEdge), nil
}

// Neighbors returns memories connected to the given memory.
func (db *DB) Neighbors(ctx context.Context, id string, depth int, edgeType string) ([]*Memory, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if id == "" {
		return nil, ErrInvalidID
	}

	if depth <= 0 {
		depth = 1
	}
	if depth > 5 {
		depth = 5 // Cap depth to prevent excessive traversal
	}

	// Helper to get all edges for a node
	getAllEdges := func(nodeID storage.NodeID) []*storage.Edge {
		var allEdges []*storage.Edge
		if out, err := db.storage.GetOutgoingEdges(nodeID); err == nil {
			allEdges = append(allEdges, out...)
		}
		if in, err := db.storage.GetIncomingEdges(nodeID); err == nil {
			allEdges = append(allEdges, in...)
		}
		return allEdges
	}

	// Collect neighbor IDs (BFS for depth > 1)
	visited := map[string]bool{id: true}
	currentLevel := []string{id}
	var neighborIDs []string

	for d := 0; d < depth; d++ {
		var nextLevel []string
		for _, nodeID := range currentLevel {
			nodeEdges := getAllEdges(storage.NodeID(nodeID))
			for _, edge := range nodeEdges {
				// Filter by edge type if specified
				if edgeType != "" && edge.Type != edgeType {
					continue
				}

				// Determine the "other" node
				var targetID string
				if string(edge.StartNode) == nodeID {
					targetID = string(edge.EndNode)
				} else {
					targetID = string(edge.StartNode)
				}

				if !visited[targetID] {
					visited[targetID] = true
					neighborIDs = append(neighborIDs, targetID)
					nextLevel = append(nextLevel, targetID)
				}
			}
		}
		currentLevel = nextLevel
	}

	// Fetch memory nodes
	var memories []*Memory
	for _, nid := range neighborIDs {
		node, err := db.storage.GetNode(storage.NodeID(nid))
		if err == nil {
			// Decrypt properties before converting to memory
			node.Properties = db.decryptProperties(node.Properties)
			memories = append(memories, nodeToMemory(node))
		}
	}

	return memories, nil
}

// Forget removes a memory and its edges.
func (db *DB) Forget(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	if id == "" {
		return ErrInvalidID
	}

	// Check if memory exists
	if _, err := db.storage.GetNode(storage.NodeID(id)); err != nil {
		return ErrNotFound
	}

	// Remove from search indexes first (before storage deletion)
	if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
		_ = svc.RemoveNode(storage.NodeID(id))
	}

	// Delete the node (storage should handle edge cleanup)
	if err := db.storage.DeleteNode(storage.NodeID(id)); err != nil {
		return fmt.Errorf("deleting memory: %w", err)
	}

	return nil
}

// generateID creates a unique UUID.
// The prefix parameter is ignored for backward compatibility but UUIDs are used for all IDs.
func generateID(prefix string) string {
	return uuid.New().String()
}

// memoryToNode converts a Memory to a storage.Node.
func memoryToNode(mem *Memory) *storage.Node {
	props := make(map[string]any)
	props["content"] = mem.Content
	props["title"] = mem.Title
	props["tier"] = string(mem.Tier)
	props["decay_score"] = mem.DecayScore
	props["last_accessed"] = mem.LastAccessed.Format(time.RFC3339)
	props["access_count"] = mem.AccessCount
	props["source"] = mem.Source
	props["tags"] = mem.Tags

	// Merge custom properties
	for k, v := range mem.Properties {
		props[k] = v
	}

	return &storage.Node{
		ID:              storage.NodeID(mem.ID),
		Labels:          []string{"Memory"},
		Properties:      props,
		ChunkEmbeddings: mem.ChunkEmbeddings,
		CreatedAt:       mem.CreatedAt,
	}
}

// nodeToMemory converts a storage.Node to a Memory.
func nodeToMemory(node *storage.Node) *Memory {
	mem := &Memory{
		ID:         string(node.ID),
		CreatedAt:  node.CreatedAt,
		Properties: make(map[string]any),
	}

	// Extract known properties
	if v, ok := node.Properties["content"].(string); ok {
		mem.Content = v
	}
	if v, ok := node.Properties["title"].(string); ok {
		mem.Title = v
	}
	if v, ok := node.Properties["tier"].(string); ok {
		mem.Tier = MemoryTier(v)
	}
	if v, ok := node.Properties["decay_score"].(float64); ok {
		mem.DecayScore = v
	}
	if v, ok := node.Properties["last_accessed"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			mem.LastAccessed = t
		}
	}
	if v, ok := node.Properties["access_count"].(int64); ok {
		mem.AccessCount = v
	} else if v, ok := node.Properties["access_count"].(int); ok {
		mem.AccessCount = int64(v)
	} else if v, ok := node.Properties["access_count"].(float64); ok {
		mem.AccessCount = int64(v)
	}
	if v, ok := node.Properties["source"].(string); ok {
		mem.Source = v
	}
	if v, ok := node.Properties["tags"].([]string); ok {
		mem.Tags = v
	} else if v, ok := node.Properties["tags"].([]interface{}); ok {
		mem.Tags = make([]string, len(v))
		for i, tag := range v {
			mem.Tags[i], _ = tag.(string)
		}
	}

	// Copy chunk embeddings (always stored as ChunkEmbeddings, even single chunk = array of 1)
	if len(node.ChunkEmbeddings) > 0 {
		mem.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
		for i, emb := range node.ChunkEmbeddings {
			mem.ChunkEmbeddings[i] = make([]float32, len(emb))
			copy(mem.ChunkEmbeddings[i], emb)
		}
	}

	// Store remaining properties
	knownKeys := map[string]bool{
		"content": true, "title": true, "tier": true,
		"decay_score": true, "last_accessed": true,
		"access_count": true, "source": true, "tags": true,
	}
	for k, v := range node.Properties {
		if !knownKeys[k] {
			mem.Properties[k] = v
		}
	}

	return mem
}

// edgeToEdge converts storage.Edge to nornicdb.Edge.
func storageEdgeToEdge(se *storage.Edge) *Edge {
	e := &Edge{
		ID:            string(se.ID),
		SourceID:      string(se.StartNode),
		TargetID:      string(se.EndNode),
		Type:          se.Type,
		Confidence:    se.Confidence,
		AutoGenerated: se.AutoGenerated,
		CreatedAt:     se.CreatedAt,
		Properties:    se.Properties,
	}
	if v, ok := se.Properties["reason"].(string); ok {
		e.Reason = v
	}
	return e
}
