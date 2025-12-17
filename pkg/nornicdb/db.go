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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/decay"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/encryption"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/temporal"
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
	ID           string         `json:"id"`
	Content      string         `json:"content"`
	Title        string         `json:"title,omitempty"`
	Tier         MemoryTier     `json:"tier"`
	DecayScore   float64        `json:"decay_score"`
	CreatedAt    time.Time      `json:"created_at"`
	LastAccessed time.Time      `json:"last_accessed"`
	AccessCount  int64          `json:"access_count"`
	Embedding    []float32      `json:"embedding,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Source       string         `json:"source,omitempty"`
	Properties   map[string]any `json:"properties,omitempty"`
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
	EmbeddingProvider   string  `yaml:"embedding_provider"` // ollama, openai
	EmbeddingAPIURL     string  `yaml:"embedding_api_url"`
	EmbeddingAPIKey     string  `yaml:"embedding_api_key"` // API key (use dummy for llama.cpp)
	EmbeddingModel      string  `yaml:"embedding_model"`
	EmbeddingDimensions int     `yaml:"embedding_dimensions"`
	AutoEmbedEnabled    bool    `yaml:"auto_embed_enabled"`    // Auto-generate embeddings on node create/update
	SearchMinSimilarity float64 `yaml:"search_min_similarity"` // Min cosine similarity for vector search (0.0 = no filter)

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
	AsyncWritesEnabled   bool          `yaml:"async_writes_enabled"`     // Enable async writes for faster performance
	AsyncFlushInterval   time.Duration `yaml:"async_flush_interval"`     // How often to flush pending writes (default: 50ms)
	AsyncMaxNodeCacheSize int          `yaml:"async_max_node_cache_size"` // Max nodes to buffer before forcing flush (default: 50000, 0=unlimited)
	AsyncMaxEdgeCacheSize int          `yaml:"async_max_edge_cache_size"` // Max edges to buffer before forcing flush (default: 100000, 0=unlimited)

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
	storage        storage.Engine
	wal            *storage.WAL // Write-ahead log for durability
	decay          *decay.Manager
	inference      *inference.Engine
	cypherExecutor *cypher.StorageExecutor
	gpuManager     interface{} // *gpu.Manager - interface to avoid circular import

	// Search service (uses pre-computed embeddings from Mimir)
	searchService *search.Service

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
			DataDir:         dataDir,
			HighPerformance: !config.LowMemoryMode,
			LowMemory:       config.LowMemoryMode,
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
		if config.AsyncWritesEnabled {
			asyncConfig := &storage.AsyncEngineConfig{
				FlushInterval:    config.AsyncFlushInterval,
				MaxNodeCacheSize: config.AsyncMaxNodeCacheSize,
				MaxEdgeCacheSize: config.AsyncMaxEdgeCacheSize,
			}
			db.storage = storage.NewAsyncEngine(walEngine, asyncConfig)
			if config.AsyncMaxNodeCacheSize > 0 || config.AsyncMaxEdgeCacheSize > 0 {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v, node cache: %d, edge cache: %d)\n",
					dataDir, config.AsyncFlushInterval, config.AsyncMaxNodeCacheSize, config.AsyncMaxEdgeCacheSize)
			} else {
				fmt.Printf("📂 Using persistent storage at %s (WAL + async writes, flush: %v)\n", dataDir, config.AsyncFlushInterval)
			}
		} else {
			db.storage = walEngine
			fmt.Printf("📂 Using persistent storage at %s (WAL enabled, batch sync)\n", dataDir)
		}
	} else {
		db.storage = storage.NewMemoryEngine()
		fmt.Println("⚠️  Using in-memory storage (data will not persist)")
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
		inferConfig := &inference.Config{
			SimilarityThreshold: config.AutoLinksSimilarityThreshold,
			SimilarityTopK:      10,
			CoAccessEnabled:     true,
			CoAccessWindow:      config.AutoLinksCoAccessWindow,
			CoAccessMinCount:    3,
			TransitiveEnabled:   true,
			TransitiveMinConf:   0.5,
		}
		db.inference = inference.New(inferConfig)

		// Wire up TopologyIntegration if Auto-TLP (Temporal Link Prediction) feature flag is enabled
		// This enables automatic relationship creation based on similarity, co-access, etc.
		// Note: Manual TLP via Cypher (CALL gds.linkPrediction.*) is always available
		if featureflags.IsAutoTLPEnabled() {
			topoConfig := inference.DefaultTopologyConfig()
			topoConfig.Enabled = true
			topoConfig.Algorithm = "adamic_adar" // Best for social/knowledge graphs
			topoConfig.Weight = 0.4              // 40% topology, 60% semantic
			topoConfig.MinScore = 0.3
			topoConfig.GraphRefreshInterval = 100 // Rebuild every 100 predictions

			topo := inference.NewTopologyIntegration(db.storage, topoConfig)
			db.inference.SetTopologyIntegration(topo)
			fmt.Println("✅ Auto-TLP enabled (NORNICDB_AUTO_TLP_ENABLED=true)")
		}

		// Wire up KalmanAdapter if feature flag is enabled
		// This enables Kalman-smoothed confidence and temporal pattern tracking
		// Note: Base inference works without this - it's an enhancement
		if featureflags.IsKalmanEnabled() {
			kalmanConfig := inference.DefaultKalmanAdapterConfig()
			kalmanAdapter := inference.NewKalmanAdapter(db.inference, kalmanConfig)

			// Create temporal tracker for access pattern analysis
			trackerConfig := temporal.DefaultConfig()
			tracker := temporal.NewTracker(trackerConfig)
			kalmanAdapter.SetTracker(tracker)

			db.inference.SetKalmanAdapter(kalmanAdapter)
			fmt.Println("✅ Kalman filtering enabled (NORNICDB_KALMAN_ENABLED=true)")
		}
	}

	// Initialize search service with configured embedding dimensions
	// This must match the embedding model's output dimensions (e.g., 512 for Apple Intelligence, 1024 for bge-m3)
	embeddingDims := config.EmbeddingDimensions
	if embeddingDims <= 0 {
		embeddingDims = 1024 // Default for bge-m3 / mxbai-embed-large
	}
	db.searchService = search.NewServiceWithDimensions(db.storage, embeddingDims)
	log.Printf("🔍 Search service initialized with %d-dimension vector index", embeddingDims)

	// Configure search MinSimilarity threshold from config
	// Apple Intelligence embeddings produce scores in 0.2-0.8 range, bge-m3/mxbai produce 0.7-0.99
	// Always set this - 0.0 is a valid value meaning "no filtering"
	db.searchService.SetDefaultMinSimilarity(config.SearchMinSimilarity)
	log.Printf("🔍 Search MinSimilarity threshold set to %.2f", config.SearchMinSimilarity)

	// Wire up storage event callbacks to keep search indexes synchronized
	// Storage is the single source of truth - it notifies when changes happen
	// The storage chain can be: AsyncEngine -> WALEngine -> BadgerEngine
	var underlyingEngine storage.Engine = db.storage

	// Unwrap AsyncEngine if present
	if asyncEngine, ok := underlyingEngine.(*storage.AsyncEngine); ok {
		underlyingEngine = asyncEngine.GetEngine()
	}

	// Unwrap WALEngine if present
	if walEngine, ok := underlyingEngine.(*storage.WALEngine); ok {
		underlyingEngine = walEngine.GetEngine()
	}

	// Set callbacks on the actual storage engine (BadgerEngine which implements StorageEventNotifier)
	if notifier, ok := underlyingEngine.(storage.StorageEventNotifier); ok {
		// When a node is created, automatically index it for search
		notifier.OnNodeCreated(func(node *storage.Node) {
			if err := db.searchService.IndexNode(node); err != nil {
				// Log but don't fail - search indexing is best-effort
				fmt.Printf("⚠️  Failed to index node %s: %v\n", node.ID, err)
			}
		})

		// When a node is updated, re-index it for search
		notifier.OnNodeUpdated(func(node *storage.Node) {
			if err := db.searchService.IndexNode(node); err != nil {
				fmt.Printf("⚠️  Failed to re-index node %s: %v\n", node.ID, err)
			}
		})

		// When a node is deleted, remove it from search indexes
		notifier.OnNodeDeleted(func(nodeID storage.NodeID) {
			if err := db.searchService.RemoveNode(nodeID); err != nil {
				fmt.Printf("⚠️  Failed to remove node %s from search indexes: %v\n", nodeID, err)
			}
		})
	}

	// Enable k-means clustering if feature flag is set
	// This provides 10-50x speedup on large datasets (10K+ embeddings)
	// Works with or without GPU (CPU fallback available)
	if featureflags.IsGPUClusteringEnabled() {
		db.searchService.EnableClustering(nil, 100) // nil GPU manager = CPU-only
		fmt.Println("🔬 K-means clustering enabled for accelerated semantic search")
	}

	// Note: Database encryption is now handled at the BadgerDB storage level (see above)
	// When encryption is enabled, ALL data is encrypted at rest using AES-256.

	// Build search indexes from existing data (including embeddings)
	// This runs in background to not block startup
	db.bgWg.Add(1)
	go func() {
		defer db.bgWg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := db.searchService.BuildIndexes(ctx); err != nil {
			fmt.Printf("⚠️  Failed to build search indexes: %v\n", err)
		} else {
			fmt.Println("✅ Search indexes built from existing data")

			// Trigger k-means clustering if enabled
			if db.searchService.IsClusteringEnabled() {
				if err := db.searchService.TriggerClustering(); err != nil {
					// Don't fail - clustering is optional optimization
					fmt.Printf("⚠️  K-means clustering skipped: %v\n", err)
				}
			}
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

	// Create embed queue - worker will wait if embedder not ready yet
	db.embedQueue = NewEmbedQueue(embedder, db.storage, db.embedWorkerConfig)
	// Set callback to update search index after embedding
	db.embedQueue.SetOnEmbedded(func(node *storage.Node) {
		if db.searchService != nil {
			if err := db.searchService.IndexNode(node); err != nil {
				log.Printf("⚠️ Failed to index embedded node %s: %v", node.ID, err)
			}
		}
	})

	// Start timer-based k-means clustering instead of trigger-based
	// This runs clustering on a schedule (default every 15 minutes) rather than after each batch
	if featureflags.IsGPUClusteringEnabled() {
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
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	if db.searchService == nil {
		return fmt.Errorf("search service not initialized")
	}

	return db.searchService.BuildIndexes(ctx)
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

	// Close WAL first to ensure all writes are flushed
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("WAL close: %w", err))
		}
	}

	if db.storage != nil {
		if err := db.storage.Close(); err != nil {
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
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.searchService == nil {
		return 0
	}
	return db.searchService.EmbeddingCount()
}

// VectorIndexDimensions returns the actual dimensions of the search service's vector index.
// This is useful for debugging dimension mismatches between config and runtime.
func (db *DB) VectorIndexDimensions() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.searchService == nil {
		return 0
	}
	return db.searchService.VectorIndexDimensions()
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
	if db.searchService != nil {
		db.searchService.ClearVectorIndex()
	}

	// Unwrap storage layers to find the BadgerEngine
	// The storage chain can be: AsyncEngine -> WALEngine -> BadgerEngine
	engine := db.storage

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
	if err := db.storage.CreateNode(node); err != nil {
		return nil, fmt.Errorf("storing memory: %w", err)
	}

	// Run auto-relationship inference if enabled
	if db.inference != nil && len(mem.Embedding) > 0 {
		suggestions, err := db.inference.OnStore(ctx, mem.ID, mem.Embedding)
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
		// Skip nodes without embeddings
		if len(node.Embedding) == 0 {
			return nil
		}

		// Decrypt properties before converting to memory
		node.Properties = db.decryptProperties(node.Properties)

		mem := nodeToMemory(node)
		sim := vector.CosineSimilarity(embedding, mem.Embedding)

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
	if db.inference != nil {
		db.inference.OnAccess(ctx, mem.ID)
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
	if db.searchService != nil {
		_ = db.searchService.RemoveNode(storage.NodeID(id))
	}

	// Delete the node (storage should handle edge cleanup)
	if err := db.storage.DeleteNode(storage.NodeID(id)); err != nil {
		return fmt.Errorf("deleting memory: %w", err)
	}

	return nil
}

// generateID creates a unique ID with prefix.
func generateID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
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
		ID:         storage.NodeID(mem.ID),
		Labels:     []string{"Memory"},
		Properties: props,
		Embedding:  mem.Embedding,
		CreatedAt:  mem.CreatedAt,
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

	// Copy embedding directly (both are []float32)
	if len(node.Embedding) > 0 {
		mem.Embedding = make([]float32, len(node.Embedding))
		copy(mem.Embedding, node.Embedding)
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

// =============================================================================
// HTTP Server Interface Methods
// =============================================================================

// Stats returns database statistics.
type DBStats struct {
	NodeCount int64 `json:"node_count"`
	EdgeCount int64 `json:"edge_count"`
	// Removed TransactionCount - was never incremented (always 0)
	// NornicDB uses thread-safe maps with RWMutex, not ACID transactions
}

// Stats returns current database statistics.
func (db *DB) Stats() DBStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := DBStats{}
	if db.storage != nil {
		nodeCount, _ := db.storage.NodeCount()
		edgeCount, _ := db.storage.EdgeCount()
		stats.NodeCount = nodeCount
		stats.EdgeCount = edgeCount
	}
	return stats
}

// SetGPUManager sets the GPU manager for vector search acceleration.
// Uses interface{} to avoid circular import with gpu package.
// If clustering is already enabled (via feature flag), this upgrades it to use GPU.
func (db *DB) SetGPUManager(manager interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.gpuManager = manager

	// If clustering is enabled and GPU manager is provided, upgrade to GPU-accelerated
	if manager != nil && db.searchService != nil && featureflags.IsGPUClusteringEnabled() {
		if gpuMgr, ok := manager.(*gpu.Manager); ok {
			db.searchService.EnableClustering(gpuMgr, 100)
			fmt.Println("🚀 K-means clustering upgraded to GPU-accelerated mode")
		}
	}
}

// GetGPUManager returns the GPU manager if set.
// Returns interface{} - caller must type assert to *gpu.Manager.
func (db *DB) GetGPUManager() interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.gpuManager
}

// TriggerSearchClustering runs k-means clustering on search embeddings.
// Call this after bulk data loading to enable cluster-accelerated search.
// Returns nil if clustering is not enabled or there are too few embeddings.
func (db *DB) TriggerSearchClustering() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.searchService == nil {
		return nil
	}

	if !db.searchService.IsClusteringEnabled() {
		return nil
	}

	return db.searchService.TriggerClustering()
}

// startClusteringTimer starts a background timer that runs k-means clustering
// at a regular interval. This is preferred over trigger-based clustering which
// can cause performance issues when embeddings are created frequently.
// Runs immediately on startup, then every interval thereafter (skipping if no changes).
func (db *DB) startClusteringTimer(interval time.Duration) {
	db.clusterTicker = time.NewTicker(interval)
	db.clusterTickerStop = make(chan struct{})

	db.bgWg.Add(1)
	go func() {
		defer db.bgWg.Done()
		log.Printf("🔬 K-means clustering timer started (interval: %v)", interval)

		// Helper to run clustering
		runClustering := func() {
			if db.searchService != nil && db.searchService.IsClusteringEnabled() {
				currentCount := db.searchService.EmbeddingCount()
				// Skip if no new embeddings since last clustering
				if currentCount == db.lastClusteredEmbedCount && db.lastClusteredEmbedCount > 0 {
					return // No changes, skip
				}

				if err := db.searchService.TriggerClustering(); err != nil {
					log.Printf("⚠️  K-means clustering skipped: %v", err)
				} else {
					db.lastClusteredEmbedCount = currentCount
					log.Printf("🔬 K-means clustering completed (%d embeddings)", currentCount)
				}
			}
		}

		// Run immediately on startup
		runClustering()

		// Then run on timer
		for {
			select {
			case <-db.clusterTickerStop:
				log.Printf("🔬 K-means clustering timer stopped")
				return
			case <-db.clusterTicker.C:
				runClustering()
			}
		}
	}()
}

// stopClusteringTimer stops the k-means clustering timer if running.
func (db *DB) stopClusteringTimer() {
	if db.clusterTicker != nil {
		db.clusterTicker.Stop()
		close(db.clusterTickerStop)
		db.clusterTicker = nil
	}
}

// SearchStats contains search service statistics.
type SearchStats struct {
	EmbeddingCount    int     `json:"embedding_count"`
	ClusteringEnabled bool    `json:"clustering_enabled"`
	IsClustered       bool    `json:"is_clustered"`
	NumClusters       int     `json:"num_clusters,omitempty"`
	AvgClusterSize    float64 `json:"avg_cluster_size,omitempty"`
	ClusterIterations int     `json:"cluster_iterations,omitempty"`
}

// GetSearchStats returns search service statistics including clustering info.
func (db *DB) GetSearchStats() *SearchStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.searchService == nil {
		return nil
	}

	stats := &SearchStats{
		EmbeddingCount:    db.searchService.EmbeddingCount(),
		ClusteringEnabled: db.searchService.IsClusteringEnabled(),
	}

	if clusterStats := db.searchService.ClusterStats(); clusterStats != nil {
		stats.IsClustered = clusterStats.Clustered
		stats.NumClusters = clusterStats.NumClusters
		stats.AvgClusterSize = clusterStats.AvgClusterSize
		stats.ClusterIterations = clusterStats.Iterations
	}

	return stats
}

// IsAsyncWritesEnabled returns true if async writes (eventual consistency) is enabled.
// When enabled, write operations return immediately and are flushed in the background.
// HTTP handlers should return 202 Accepted instead of 201 Created for writes.
func (db *DB) IsAsyncWritesEnabled() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.config.AsyncWritesEnabled
}

// CypherResult holds results from a Cypher query.
type CypherResult struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// ExecuteCypher runs a Cypher query and returns structured results.
func (db *DB) ExecuteCypher(ctx context.Context, query string, params map[string]interface{}) (*CypherResult, error) {
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

	return &CypherResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

// TypedCypherResult holds typed query results.
type TypedCypherResult[T any] struct {
	Columns []string `json:"columns"`
	Rows    []T      `json:"rows"`
}

// ExecuteCypherTyped runs a Cypher query and decodes results into typed structs.
// Usage:
//
//	type Task struct {
//	    ID     string `cypher:"id"`
//	    Title  string `cypher:"title"`
//	    Status string `cypher:"status"`
//	}
//	result, err := db.ExecuteCypherTyped[Task](ctx, "MATCH (t:Task) RETURN t.id, t.title, t.status", nil)
func ExecuteCypherTyped[T any](db *DB, ctx context.Context, query string, params map[string]interface{}) (*TypedCypherResult[T], error) {
	raw, err := db.ExecuteCypher(ctx, query, params)
	if err != nil {
		return nil, err
	}

	rows := make([]T, 0, len(raw.Rows))
	for _, row := range raw.Rows {
		var decoded T
		if err := decodeRow(raw.Columns, row, &decoded); err != nil {
			return nil, fmt.Errorf("failed to decode row: %w", err)
		}
		rows = append(rows, decoded)
	}

	return &TypedCypherResult[T]{
		Columns: raw.Columns,
		Rows:    rows,
	}, nil
}

// First returns the first row or zero value if empty.
func (r *TypedCypherResult[T]) First() (T, bool) {
	if len(r.Rows) == 0 {
		var zero T
		return zero, false
	}
	return r.Rows[0], true
}

// decodeRow decodes a row into a typed struct using reflection.
func decodeRow(columns []string, values []interface{}, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	destElem := destVal.Elem()
	destType := destElem.Type()

	// Handle map return (node as map)
	if len(values) == 1 {
		if m, ok := values[0].(map[string]interface{}); ok {
			// Check for nested properties
			if props, ok := m["properties"].(map[string]interface{}); ok {
				return decodeMapToStruct(props, destElem, destType)
			}
			return decodeMapToStruct(m, destElem, destType)
		}
	}

	// Build field mapping from struct tags
	fieldMap := make(map[string]int)
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if idx := strings.Index(name, ","); idx != -1 {
				name = name[:idx]
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}
		fieldMap[name] = i
	}

	// Map columns to fields
	for i, col := range columns {
		if i >= len(values) {
			break
		}

		// Normalize column name (handle n.property notation)
		colName := col
		if idx := strings.LastIndex(col, "."); idx != -1 {
			colName = col[idx+1:]
		}
		colName = strings.ToLower(colName)

		fieldIdx, ok := fieldMap[colName]
		if !ok {
			continue
		}

		field := destElem.Field(fieldIdx)
		if !field.CanSet() {
			continue
		}

		if err := assignValue(field, values[i]); err != nil {
			return fmt.Errorf("field %s: %w", col, err)
		}
	}

	return nil
}

// decodeMapToStruct decodes a map into a struct
func decodeMapToStruct(m map[string]interface{}, destElem reflect.Value, destType reflect.Type) error {
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		fieldVal := destElem.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if idx := strings.Index(name, ","); idx != -1 {
				name = name[:idx]
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}

		val, ok := m[name]
		if !ok {
			val, ok = m[strings.ToLower(name)]
		}
		if !ok {
			val, ok = m[field.Name]
		}
		if !ok {
			continue
		}

		if err := assignValue(fieldVal, val); err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
	}
	return nil
}

// assignValue assigns a value to a reflect.Value with type conversion
func assignValue(field reflect.Value, val interface{}) error {
	if val == nil {
		return nil
	}

	valReflect := reflect.ValueOf(val)

	// Direct assignment if types match
	if valReflect.Type().AssignableTo(field.Type()) {
		field.Set(valReflect)
		return nil
	}

	// Type conversion
	if valReflect.Type().ConvertibleTo(field.Type()) {
		field.Set(valReflect.Convert(field.Type()))
		return nil
	}

	// Handle numeric conversions
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := val.(type) {
		case float64:
			field.SetInt(int64(v))
			return nil
		case int:
			field.SetInt(int64(v))
			return nil
		case int64:
			field.SetInt(v)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch v := val.(type) {
		case int:
			field.SetFloat(float64(v))
			return nil
		case int64:
			field.SetFloat(float64(v))
			return nil
		case float64:
			field.SetFloat(v)
			return nil
		}
	case reflect.String:
		field.SetString(fmt.Sprintf("%v", val))
		return nil
	case reflect.Bool:
		if b, ok := val.(bool); ok {
			field.SetBool(b)
			return nil
		}
	}

	return fmt.Errorf("cannot assign %T to %v", val, field.Type())
}

// Node represents a graph node for HTTP API.
type Node struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
	CreatedAt  time.Time              `json:"created_at"`
}

// ListNodes returns nodes with optional label filter.
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) ListNodes(ctx context.Context, label string, limit, offset int) ([]*Node, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	var nodes []*Node
	count := 0

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		// Filter by label if specified
		if label != "" {
			hasLabel := false
			for _, l := range n.Labels {
				if l == label {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				return nil // Skip, continue iteration
			}
		}

		// Handle offset
		if count < offset {
			count++
			return nil
		}

		// Handle limit - stop early when we have enough
		if len(nodes) >= limit {
			return storage.ErrIterationStopped
		}

		// Decrypt sensitive fields before returning
		decryptedProps := db.decryptProperties(n.Properties)

		nodes = append(nodes, &Node{
			ID:         string(n.ID),
			Labels:     n.Labels,
			Properties: decryptedProps,
			CreatedAt:  n.CreatedAt,
		})
		count++
		return nil
	})

	if err != nil && err != storage.ErrIterationStopped {
		return nil, err
	}

	return nodes, nil
}

// GetNode retrieves a node by ID.
func (db *DB) GetNode(ctx context.Context, id string) (*Node, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	n, err := db.storage.GetNode(storage.NodeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	// Decrypt sensitive fields after retrieval
	decryptedProps := db.decryptProperties(n.Properties)

	return &Node{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: decryptedProps,
		CreatedAt:  n.CreatedAt,
	}, nil
}

// CreateNode creates a new node.
func (db *DB) CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*Node, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	id := generateID("node")
	now := time.Now()

	// Embeddings are internal-only - silently strip any user-provided embedding property
	// Users cannot supply embeddings; they are generated asynchronously by the embed queue
	delete(properties, "embedding")
	delete(properties, "embeddings")
	delete(properties, "vector")

	// Encrypt sensitive fields before storage (PHI/PII protection)
	encryptedProps := db.encryptProperties(properties)

	node := &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     labels,
		Properties: encryptedProps,
		CreatedAt:  now,
	}

	if err := db.storage.CreateNode(node); err != nil {
		return nil, err
	}

	// Always queue for async embedding generation (non-blocking)
	if db.embedQueue != nil {
		db.embedQueue.Enqueue(id)
	}

	// Update search indexes (live indexing for seamless Mimir compatibility)
	if db.searchService != nil {
		_ = db.searchService.IndexNode(node) // Best effort - search may lag behind writes
	}

	return &Node{
		ID:         id,
		Labels:     labels,
		Properties: properties,
		CreatedAt:  now,
	}, nil
}

// UpdateNode updates a node's properties.
func (db *DB) UpdateNode(ctx context.Context, id string, properties map[string]interface{}) (*Node, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	n, err := db.storage.GetNode(storage.NodeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	// Embeddings are internal-only - silently strip any user-provided embedding property
	delete(properties, "embedding")
	delete(properties, "embeddings")
	delete(properties, "vector")

	// Encrypt sensitive fields before merging
	encryptedProps := db.encryptProperties(properties)

	// Merge properties (encrypted values replace existing)
	for k, v := range encryptedProps {
		n.Properties[k] = v
	}

	if err := db.storage.UpdateNode(n); err != nil {
		return nil, err
	}

	// Decrypt for return
	decryptedProps := db.decryptProperties(n.Properties)

	return &Node{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: decryptedProps,
		CreatedAt:  n.CreatedAt,
	}, nil
}

// DeleteNode deletes a node.
func (db *DB) DeleteNode(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Remove from search indexes first (before storage deletion)
	if db.searchService != nil {
		_ = db.searchService.RemoveNode(storage.NodeID(id))
	}

	return db.storage.DeleteNode(storage.NodeID(id))
}

// GraphEdge represents an edge for HTTP API.
type GraphEdge struct {
	ID         string                 `json:"id"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

// ListEdges returns edges with optional type filter.
func (db *DB) ListEdges(ctx context.Context, relType string, limit, offset int) ([]*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	allEdges, err := db.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	var edges []*GraphEdge
	count := 0
	for _, e := range allEdges {
		// Filter by type if specified
		if relType != "" && e.Type != relType {
			continue
		}

		// Handle offset
		if count < offset {
			count++
			continue
		}

		// Handle limit
		if len(edges) >= limit {
			break
		}

		edges = append(edges, &GraphEdge{
			ID:         string(e.ID),
			Source:     string(e.StartNode),
			Target:     string(e.EndNode),
			Type:       e.Type,
			Properties: e.Properties,
			CreatedAt:  e.CreatedAt,
		})
		count++
	}

	return edges, nil
}

// GetEdge retrieves an edge by ID.
func (db *DB) GetEdge(ctx context.Context, id string) (*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	e, err := db.storage.GetEdge(storage.EdgeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	return &GraphEdge{
		ID:         string(e.ID),
		Source:     string(e.StartNode),
		Target:     string(e.EndNode),
		Type:       e.Type,
		Properties: e.Properties,
		CreatedAt:  e.CreatedAt,
	}, nil
}

// GetEdgesForNode returns all edges (both incoming and outgoing) for a given node.
// This is useful for graph traversal and relationship inspection.
func (db *DB) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if nodeID == "" {
		return nil, ErrInvalidID
	}

	var edges []*GraphEdge
	storageNodeID := storage.NodeID(nodeID)

	// Get outgoing edges
	outgoing, err := db.storage.GetOutgoingEdges(storageNodeID)
	if err == nil {
		for _, e := range outgoing {
			edges = append(edges, &GraphEdge{
				ID:         string(e.ID),
				Source:     string(e.StartNode),
				Target:     string(e.EndNode),
				Type:       e.Type,
				Properties: e.Properties,
				CreatedAt:  e.CreatedAt,
			})
		}
	}

	// Get incoming edges
	incoming, err := db.storage.GetIncomingEdges(storageNodeID)
	if err == nil {
		for _, e := range incoming {
			edges = append(edges, &GraphEdge{
				ID:         string(e.ID),
				Source:     string(e.StartNode),
				Target:     string(e.EndNode),
				Type:       e.Type,
				Properties: e.Properties,
				CreatedAt:  e.CreatedAt,
			})
		}
	}

	return edges, nil
}

// CreateEdge creates a new edge.
func (db *DB) CreateEdge(ctx context.Context, source, target, edgeType string, properties map[string]interface{}) (*GraphEdge, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Verify nodes exist
	if _, err := db.storage.GetNode(storage.NodeID(source)); err != nil {
		return nil, fmt.Errorf("source node not found")
	}
	if _, err := db.storage.GetNode(storage.NodeID(target)); err != nil {
		return nil, fmt.Errorf("target node not found")
	}

	id := generateID("edge")
	now := time.Now()

	edge := &storage.Edge{
		ID:         storage.EdgeID(id),
		StartNode:  storage.NodeID(source),
		EndNode:    storage.NodeID(target),
		Type:       edgeType,
		Properties: properties,
		CreatedAt:  now,
	}

	if err := db.storage.CreateEdge(edge); err != nil {
		return nil, err
	}

	return &GraphEdge{
		ID:         id,
		Source:     source,
		Target:     target,
		Type:       edgeType,
		Properties: properties,
		CreatedAt:  now,
	}, nil
}

// DeleteEdge deletes an edge.
func (db *DB) DeleteEdge(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	return db.storage.DeleteEdge(storage.EdgeID(id))
}

// SearchResult holds a search result with score.
type SearchResult struct {
	Node  *Node   `json:"node"`
	Score float64 `json:"score"`

	// RRF metadata
	RRFScore   float64 `json:"rrf_score,omitempty"`
	VectorRank int     `json:"vector_rank,omitempty"`
	BM25Rank   int     `json:"bm25_rank,omitempty"`
}

// Search performs full-text BM25 search.
// For hybrid vector+text search, use HybridSearch with pre-computed query embedding.
func (db *DB) Search(ctx context.Context, query string, labels []string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if db.searchService == nil {
		return nil, fmt.Errorf("search service not initialized")
	}

	// Get adaptive search options based on query
	opts := search.GetAdaptiveRRFConfig(query)
	opts.Limit = limit
	if len(labels) > 0 {
		opts.Types = labels
	}

	// Full-text search only (no embedding generation)
	// For hybrid search, Mimir should call VectorSearch with pre-computed embedding
	response, err := db.searchService.Search(ctx, query, nil, opts)
	if err != nil {
		return nil, err
	}

	// Convert search results to our format
	results := make([]*SearchResult, len(response.Results))
	for i, r := range response.Results {
		results[i] = &SearchResult{
			Node: &Node{
				ID:         r.ID,
				Labels:     r.Labels,
				Properties: r.Properties,
			},
			Score:      r.Score,
			RRFScore:   r.RRFScore,
			VectorRank: r.VectorRank,
			BM25Rank:   r.BM25Rank,
		}
	}

	return results, nil
}

// HybridSearch performs RRF hybrid search combining vector similarity and BM25 full-text.
// The queryEmbedding should be pre-computed by Mimir using its embedding service.
// This is the primary search method for semantic search with ranking fusion.
func (db *DB) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if db.searchService == nil {
		return nil, fmt.Errorf("search service not initialized")
	}

	// Get adaptive search options based on query
	opts := search.GetAdaptiveRRFConfig(query)
	opts.Limit = limit
	if len(labels) > 0 {
		opts.Types = labels
	}

	// Execute RRF hybrid search with Mimir's pre-computed embedding
	response, err := db.searchService.Search(ctx, query, queryEmbedding, opts)
	if err != nil {
		return nil, err
	}

	// Convert search results to our format
	results := make([]*SearchResult, len(response.Results))
	for i, r := range response.Results {
		results[i] = &SearchResult{
			Node: &Node{
				ID:         r.ID,
				Labels:     r.Labels,
				Properties: r.Properties,
			},
			Score:      r.Score,
			RRFScore:   r.RRFScore,
			VectorRank: r.VectorRank,
			BM25Rank:   r.BM25Rank,
		}
	}

	return results, nil
}

// FindSimilar finds nodes similar to a given node by embedding.
func (db *DB) FindSimilar(ctx context.Context, nodeID string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Get target node
	target, err := db.storage.GetNode(storage.NodeID(nodeID))
	if err != nil {
		return nil, ErrNotFound
	}

	if len(target.Embedding) == 0 {
		return nil, fmt.Errorf("node has no embedding")
	}

	// Find similar by embedding using streaming iteration
	type scored struct {
		node  *storage.Node
		score float64
	}
	var results []scored

	err = storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		// Skip self and nodes without embeddings
		if string(n.ID) == nodeID || len(n.Embedding) == 0 {
			return nil
		}

		sim := vector.CosineSimilarity(target.Embedding, n.Embedding)

		// Maintain top-k results
		if len(results) < limit {
			results = append(results, scored{node: n, score: sim})
			if len(results) == limit {
				sort.Slice(results, func(i, j int) bool {
					return results[i].score > results[j].score
				})
			}
		} else if sim > results[limit-1].score {
			results[limit-1] = scored{node: n, score: sim}
			sort.Slice(results, func(i, j int) bool {
				return results[i].score > results[j].score
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Final sort
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	searchResults := make([]*SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = &SearchResult{
			Node: &Node{
				ID:         string(r.node.ID),
				Labels:     r.node.Labels,
				Properties: r.node.Properties,
				CreatedAt:  r.node.CreatedAt,
			},
			Score: r.score,
		}
	}

	return searchResults, nil
}

// GetLabels returns all distinct node labels.
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) GetLabels(ctx context.Context) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Use streaming helper for memory efficiency
	labels, err := storage.CollectLabels(ctx, db.storage)
	if err != nil {
		return nil, err
	}

	sort.Strings(labels)
	return labels, nil
}

// GetRelationshipTypes returns all distinct edge types.
// Uses streaming iteration to avoid loading all edges into memory.
func (db *DB) GetRelationshipTypes(ctx context.Context) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Use streaming helper for memory efficiency
	types, err := storage.CollectEdgeTypes(ctx, db.storage)
	if err != nil {
		return nil, err
	}

	sort.Strings(types)

	return types, nil
}

// IndexInfo holds index metadata.
type IndexInfo struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Property string `json:"property"`
	Type     string `json:"type"` // btree, fulltext, vector
}

// toStringValue safely converts interface{} to string
func toStringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// GetIndexes returns all indexes from the storage schema manager.
func (db *DB) GetIndexes(ctx context.Context) ([]*IndexInfo, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	schema := db.storage.GetSchema()
	if schema == nil {
		return []*IndexInfo{}, nil
	}

	// Get all indexes from schema manager
	rawIndexes := schema.GetIndexes()
	result := make([]*IndexInfo, 0, len(rawIndexes))

	for _, idx := range rawIndexes {
		if m, ok := idx.(map[string]interface{}); ok {
			info := &IndexInfo{
				Name:  toStringValue(m["name"]),
				Label: toStringValue(m["label"]),
				Type:  strings.ToLower(toStringValue(m["type"])),
			}
			// Handle single property or first property from array
			if prop, ok := m["property"].(string); ok {
				info.Property = prop
			} else if props, ok := m["properties"].([]string); ok && len(props) > 0 {
				info.Property = props[0]
			}
			result = append(result, info)
		}
	}

	return result, nil
}

// CreateIndex creates a new index on a label/property combination.
// Supported types: "property", "fulltext", "vector", "range"
func (db *DB) CreateIndex(ctx context.Context, label, property, indexType string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	schema := db.storage.GetSchema()
	if schema == nil {
		return fmt.Errorf("schema manager not initialized")
	}

	indexName := fmt.Sprintf("idx_%s_%s", strings.ToLower(label), strings.ToLower(property))

	switch strings.ToLower(indexType) {
	case "property", "btree":
		return schema.AddPropertyIndex(indexName, label, []string{property})
	case "fulltext":
		return schema.AddFulltextIndex(indexName, []string{label}, []string{property})
	case "vector":
		// Use configured embedding dimensions instead of hardcoded value
		dims := db.config.EmbeddingDimensions
		return schema.AddVectorIndex(indexName, label, property, dims, "cosine")
	case "range":
		return schema.AddRangeIndex(indexName, label, property)
	default:
		return fmt.Errorf("unsupported index type: %s (use: property, fulltext, vector, range)", indexType)
	}
}

// BackupableEngine is an interface for engines that support backup.
type BackupableEngine interface {
	Backup(path string) error
}

// Backup creates a database backup to the specified path.
// For BadgerDB, this creates a streaming backup that is consistent and portable.
// For in-memory databases, it exports all data as JSON.
func (db *DB) Backup(ctx context.Context, path string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrClosed
	}

	// Check if storage engine supports backup
	if backupable, ok := db.storage.(BackupableEngine); ok {
		return backupable.Backup(path)
	}

	// Fallback: Export as JSON for non-backupable engines (memory)
	nodes, err := db.storage.AllNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}

	edges, err := db.storage.AllEdges()
	if err != nil {
		return fmt.Errorf("failed to get edges: %w", err)
	}

	backup := map[string]interface{}{
		"version":    "1.0",
		"created_at": time.Now().Format(time.RFC3339),
		"nodes":      nodes,
		"edges":      edges,
	}

	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write backup: %w", err)
	}

	return nil
}

// Restore restores the database from a JSON backup file.
// This is primarily for in-memory databases or cross-engine migration.
// For BadgerDB production use, use the storage-level backup/restore.
//
// Example:
//
//	err := db.Restore(ctx, "backup-20241201.json")
func (db *DB) Restore(ctx context.Context, path string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Read backup file
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read backup: %w", err)
	}

	// Parse backup
	var backup struct {
		Version   string          `json:"version"`
		CreatedAt string          `json:"created_at"`
		Nodes     []*storage.Node `json:"nodes"`
		Edges     []*storage.Edge `json:"edges"`
	}

	if err := json.Unmarshal(data, &backup); err != nil {
		return fmt.Errorf("failed to parse backup: %w", err)
	}

	// Restore nodes
	for _, node := range backup.Nodes {
		if err := db.storage.CreateNode(node); err != nil {
			// Try update if node exists
			if updateErr := db.storage.UpdateNode(node); updateErr != nil {
				return fmt.Errorf("failed to restore node %s: %w", node.ID, err)
			}
		}
	}

	// Restore edges
	for _, edge := range backup.Edges {
		if err := db.storage.CreateEdge(edge); err != nil {
			// Try update if edge exists
			if updateErr := db.storage.UpdateEdge(edge); updateErr != nil {
				return fmt.Errorf("failed to restore edge %s: %w", edge.ID, err)
			}
		}
	}

	// Rebuild search indexes
	if db.searchService != nil {
		if err := db.searchService.BuildIndexes(ctx); err != nil {
			log.Printf("⚠️  Warning: failed to rebuild search indexes: %v", err)
		}
	}

	return nil
}

// ExportUserData exports all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
// Supports "json" (default) and "csv" formats.
func (db *DB) ExportUserData(ctx context.Context, userID, format string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Collect user data using streaming
	var userData []map[string]interface{}
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			userData = append(userData, map[string]interface{}{
				"id":         string(n.ID),
				"labels":     n.Labels,
				"properties": n.Properties,
				"created_at": n.CreatedAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Format output
	if format == "csv" {
		return db.exportUserDataCSV(userData)
	}

	// Default to JSON
	return json.Marshal(map[string]interface{}{
		"user_id":     userID,
		"data":        userData,
		"exported_at": time.Now(),
	})
}

// exportUserDataCSV converts user data to CSV format.
func (db *DB) exportUserDataCSV(userData []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer

	// Collect all unique property keys across all nodes
	propertyKeys := make(map[string]bool)
	for _, data := range userData {
		if props, ok := data["properties"].(map[string]interface{}); ok {
			for k := range props {
				propertyKeys[k] = true
			}
		}
	}

	// Sort property keys for consistent output
	sortedKeys := make([]string, 0, len(propertyKeys))
	for k := range propertyKeys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Write CSV header
	headers := []string{"id", "labels", "created_at"}
	headers = append(headers, sortedKeys...)
	buf.WriteString(strings.Join(headers, ",") + "\n")

	// Write data rows
	for _, data := range userData {
		row := make([]string, len(headers))

		// ID
		row[0] = escapeCSV(toStringValue(data["id"]))

		// Labels
		if labels, ok := data["labels"].([]string); ok {
			row[1] = escapeCSV(strings.Join(labels, ";"))
		} else {
			row[1] = ""
		}

		// Created at
		if createdAt, ok := data["created_at"].(time.Time); ok {
			row[2] = escapeCSV(createdAt.Format(time.RFC3339))
		} else {
			row[2] = ""
		}

		// Properties
		if props, ok := data["properties"].(map[string]interface{}); ok {
			for i, key := range sortedKeys {
				if val, exists := props[key]; exists {
					row[3+i] = escapeCSV(toStringValue(val))
				} else {
					row[3+i] = ""
				}
			}
		}

		buf.WriteString(strings.Join(row, ",") + "\n")
	}

	return buf.Bytes(), nil
}

// escapeCSV escapes a string for CSV output
func escapeCSV(s string) string {
	needsQuote := strings.ContainsAny(s, ",\"\n\r")
	if !needsQuote {
		return s
	}
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(s, "\"", "\"\"")
	return "\"" + escaped + "\""
}

// DeleteUserData deletes all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) DeleteUserData(ctx context.Context, userID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Collect IDs to delete first (can't delete while iterating)
	var toDelete []storage.NodeID
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			toDelete = append(toDelete, n.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Now delete the collected nodes
	for _, id := range toDelete {
		// Remove from search indexes first (before storage deletion)
		if db.searchService != nil {
			_ = db.searchService.RemoveNode(id)
		}
		if err := db.storage.DeleteNode(id); err != nil {
			return err
		}
	}

	return nil
}

// AnonymizeUserData anonymizes all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) AnonymizeUserData(ctx context.Context, userID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	anonymousID := "anon-" + generateID("")

	// Collect nodes to update (can't update while streaming in some engines)
	// We must make copies since other goroutines might be iterating over these nodes
	var toUpdate []*storage.Node
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			// Make a deep copy of the node to avoid concurrent modification
			nodeCopy := &storage.Node{
				ID:           n.ID,
				Labels:       append([]string(nil), n.Labels...),
				Properties:   make(map[string]any, len(n.Properties)),
				CreatedAt:    n.CreatedAt,
				UpdatedAt:    n.UpdatedAt,
				DecayScore:   n.DecayScore,
				LastAccessed: n.LastAccessed,
				AccessCount:  n.AccessCount,
				Embedding:    append([]float32(nil), n.Embedding...),
			}
			for k, v := range n.Properties {
				nodeCopy.Properties[k] = v
			}

			// Replace identifying info in the copy
			nodeCopy.Properties["owner_id"] = anonymousID
			delete(nodeCopy.Properties, "email")
			delete(nodeCopy.Properties, "name")
			delete(nodeCopy.Properties, "username")
			delete(nodeCopy.Properties, "ip_address")
			toUpdate = append(toUpdate, nodeCopy)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Now update the collected nodes
	for _, n := range toUpdate {
		if err := db.storage.UpdateNode(n); err != nil {
			return err
		}
	}

	return nil
}

// =============================================================================
// GDPR Consent Management
// =============================================================================

// Consent represents a user's consent for a specific purpose.
// Implements GDPR Article 7 requirements for consent management.
type Consent struct {
	UserID    string    `json:"user_id"`
	Purpose   string    `json:"purpose"`
	Given     bool      `json:"given"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"` // e.g., "web_form", "api", "email"
}

// RecordConsent records a user's consent for a specific purpose (GDPR compliance).
// Creates or updates a consent record in the database.
//
// Example:
//
//	err := db.RecordConsent(ctx, &Consent{
//	    UserID:  "user-123",
//	    Purpose: "marketing",
//	    Given:   true,
//	    Source:  "web_form",
//	})
func (db *DB) RecordConsent(ctx context.Context, consent *Consent) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	if consent.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if consent.Purpose == "" {
		return fmt.Errorf("purpose is required")
	}

	// Set timestamp if not provided
	if consent.Timestamp.IsZero() {
		consent.Timestamp = time.Now()
	}

	// Create consent node ID
	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", consent.UserID, consent.Purpose))

	// Check if consent already exists
	existingNode, err := db.storage.GetNode(consentID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("checking existing consent: %w", err)
	}

	props := map[string]interface{}{
		"user_id":   consent.UserID,
		"purpose":   consent.Purpose,
		"given":     consent.Given,
		"timestamp": consent.Timestamp.Format(time.RFC3339),
		"source":    consent.Source,
	}

	if existingNode != nil {
		// Update existing consent
		existingNode.Properties = props
		return db.storage.UpdateNode(existingNode)
	}

	// Create new consent node
	node := &storage.Node{
		ID:         consentID,
		Labels:     []string{"Consent"},
		Properties: props,
	}

	return db.storage.CreateNode(node)
}

// HasConsent checks if a user has given consent for a specific purpose.
// Returns true if consent was given, false otherwise.
//
// Example:
//
//	hasConsent, err := db.HasConsent(ctx, "user-123", "marketing")
//	if err != nil {
//	    return err
//	}
//	if !hasConsent {
//	    return ErrNoConsent
//	}
func (db *DB) HasConsent(ctx context.Context, userID, purpose string) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return false, ErrClosed
	}

	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", userID, purpose))
	node, err := db.storage.GetNode(consentID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil // No consent record = no consent given
		}
		return false, fmt.Errorf("checking consent: %w", err)
	}

	given, ok := node.Properties["given"].(bool)
	if !ok {
		return false, nil
	}

	return given, nil
}

// RevokeConsent revokes a user's consent for a specific purpose.
// Sets the consent to false and updates the timestamp.
//
// Example:
//
//	err := db.RevokeConsent(ctx, "user-123", "marketing")
func (db *DB) RevokeConsent(ctx context.Context, userID, purpose string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", userID, purpose))
	node, err := db.storage.GetNode(consentID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Create a revoked consent record for audit trail
			node = &storage.Node{
				ID:     consentID,
				Labels: []string{"Consent"},
				Properties: map[string]interface{}{
					"user_id":   userID,
					"purpose":   purpose,
					"given":     false,
					"timestamp": time.Now().Format(time.RFC3339),
					"source":    "revocation",
				},
			}
			return db.storage.CreateNode(node)
		}
		return fmt.Errorf("getting consent: %w", err)
	}

	// Update existing consent to revoked
	node.Properties["given"] = false
	node.Properties["timestamp"] = time.Now().Format(time.RFC3339)
	node.Properties["source"] = "revocation"

	return db.storage.UpdateNode(node)
}

// GetUserConsents returns all consent records for a user.
// Useful for GDPR data access requests.
func (db *DB) GetUserConsents(ctx context.Context, userID string) ([]Consent, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	var consents []Consent
	prefix := fmt.Sprintf("consent:%s:", userID)

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if strings.HasPrefix(string(n.ID), prefix) {
			consent := Consent{
				UserID:  userID,
				Purpose: n.Properties["purpose"].(string),
			}
			if given, ok := n.Properties["given"].(bool); ok {
				consent.Given = given
			}
			if ts, ok := n.Properties["timestamp"].(string); ok {
				consent.Timestamp, _ = time.Parse(time.RFC3339, ts)
			}
			if source, ok := n.Properties["source"].(string); ok {
				consent.Source = source
			}
			consents = append(consents, consent)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("streaming consent nodes: %w", err)
	}

	return consents, nil
}

// encryptProperties is a no-op - encryption is handled at the BadgerDB storage level.
// All data is encrypted transparently when encryption is enabled.
// This method is kept for API compatibility but simply returns the input unchanged.
func (db *DB) encryptProperties(props map[string]interface{}) map[string]interface{} {
	return props
}

// decryptProperties is a no-op - decryption is handled at the BadgerDB storage level.
// All data is decrypted transparently when read from an encrypted database.
// This method is kept for API compatibility but simply returns the input unchanged.
func (db *DB) decryptProperties(props map[string]interface{}) map[string]interface{} {
	return props
}

// IsEncryptionEnabled returns whether data-at-rest encryption is enabled.
// When enabled, ALL data is encrypted at the BadgerDB storage level using AES-256.
func (db *DB) IsEncryptionEnabled() bool {
	return db.encryptionEnabled
}

// EncryptionStats returns encryption statistics for monitoring.
func (db *DB) EncryptionStats() map[string]interface{} {
	return map[string]interface{}{
		"enabled":        db.encryptionEnabled,
		"algorithm":      "AES-256 (BadgerDB)",
		"key_derivation": "PBKDF2-SHA256 (600k iterations)",
		"scope":          "full-database",
	}
}
