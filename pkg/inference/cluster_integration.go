// Integration between GPU k-means clustering and semantic inference.
//
// This file bridges:
//   - pkg/gpu (ClusterIndex with k-means clustering)
//   - pkg/inference (semantic inference engine)
//
// Provides cluster-accelerated similarity search for large embedding indices.
//
// Feature Flags:
//   - NORNICDB_GPU_CLUSTERING_ENABLED: Enable GPU clustering (default: false)
//   - NORNICDB_GPU_CLUSTERING_AUTO_INTEGRATION_ENABLED: Auto-integrate with inference engine
//
// See pkg/config/feature_flags.go for details.
package inference

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
)

// ClusterConfig controls k-means clustering integration with inference.
//
// This allows the inference engine to use cluster-accelerated search
// for faster semantic similarity lookup on large embedding sets.
//
// Example:
//
//	config := &inference.ClusterConfig{
//		Enabled:           true,
//		NumClustersSearch: 3,         // Search 3 nearest clusters
//		AutoRecluster:     true,
//		ReclusterThreshold: 0.1,      // 10% drift triggers recluster
//	}
type ClusterConfig struct {
	// Enable cluster-accelerated search
	Enabled bool

	// Number of clusters to search during similarity lookup
	// Higher = better recall, slower; Lower = faster, may miss results
	// Default: 3
	NumClustersSearch int

	// Automatically recluster when drift threshold is exceeded
	AutoRecluster bool

	// ReclusterThreshold: trigger re-clustering when this fraction
	// of embeddings have been updated since last cluster (0.0-1.0)
	// Default: 0.1 (10%)
	ReclusterThreshold float64

	// MinEmbeddingsForClustering: minimum embeddings before clustering is used
	// Below this threshold, brute-force search is used
	// Default: 1000
	MinEmbeddingsForClustering int
}

// DefaultClusterConfig returns sensible defaults for cluster integration.
//
// The Enabled field is set based on the NORNICDB_GPU_CLUSTERING_ENABLED
// environment variable (default: false).
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		Enabled:                    config.IsGPUClusteringEnabled(), // Respects feature flag
		NumClustersSearch:          3,
		AutoRecluster:              true,
		ReclusterThreshold:         0.1,
		MinEmbeddingsForClustering: 1000,
	}
}

// ClusterIntegration adds GPU k-means clustering to the inference engine.
//
// This is an optional extension that can be enabled to accelerate semantic
// similarity search on large embedding indices. When enabled:
//   - Embeddings are organized into clusters using k-means
//   - Search queries first find nearest clusters, then search within them
//   - Provides 10-50x speedup on indices with 10K+ embeddings
//
// Thread Safety: All methods are thread-safe.
//
// Architecture:
//
//	┌────────────────────────────────────────────────────────┐
//	│                 ClusterIntegration                      │
//	├────────────────────────────────────────────────────────┤
//	│  config *ClusterConfig     <- search/recluster config   │
//	│  clusterIndex *gpu.ClusterIndex  <- GPU-accelerated     │
//	│  mu sync.RWMutex           <- thread safety             │
//	├────────────────────────────────────────────────────────┤
//	│  Methods:                                               │
//	│    OnIndexComplete()  <- trigger clustering             │
//	│    Search()           <- cluster-accelerated search     │
//	│    OnNodeUpdate()     <- real-time embedding updates    │
//	│    Stats()            <- clustering statistics          │
//	└────────────────────────────────────────────────────────┘
//
// Example:
//
//	// Create integration with GPU manager
//	gpuManager, _ := gpu.NewManager(&gpu.Config{Enabled: true})
//
//	clusterConfig := inference.DefaultClusterConfig()
//	clusterConfig.Enabled = true
//
//	ci := inference.NewClusterIntegration(gpuManager, clusterConfig, nil)
//	engine.SetClusterIntegration(ci)
//
//	// After indexing complete, trigger clustering
//	ci.OnIndexComplete()
//
//	// Searches now use cluster acceleration
//	results, _ := engine.SimilaritySearch(ctx, embedding, 10)
type ClusterIntegration struct {
	config       *ClusterConfig
	clusterIndex *gpu.ClusterIndex
	mu           sync.RWMutex

	// Stats tracking. Searches run under the read lock, concurrently, so the
	// search counters are atomic rather than guarded by mu.
	searchesTotal     atomic.Int64
	searchesClustered atomic.Int64
	lastReclusterAt   int64 // embedding count at last recluster
}

// NewClusterIntegration creates a new cluster integration.
//
// Parameters:
//   - manager: GPU manager for acceleration (can be nil for CPU-only)
//   - config: Cluster configuration (nil uses defaults)
//   - kmeansConfig: K-means configuration (nil uses defaults)
//   - embConfig: Embedding index config (nil uses defaults with 1024 dims)
//
// Returns ready-to-use integration that can be attached to inference engine.
//
// Example:
//
//	// Basic setup
//	ci := inference.NewClusterIntegration(nil, nil, nil, nil)
//
//	// With GPU acceleration
//	gpuMgr, _ := gpu.NewManager(&gpu.Config{Enabled: true})
//	ci = inference.NewClusterIntegration(gpuMgr, nil, nil, nil)
//
//	// With custom config
//	config := &inference.ClusterConfig{
//		Enabled:           true,
//		NumClustersSearch: 5,
//	}
//	kmeansConfig := &gpu.KMeansConfig{
//		NumClusters:   100,
//		MaxIterations: 50,
//	}
//	embConfig := gpu.DefaultEmbeddingIndexConfig(768)
//	ci = inference.NewClusterIntegration(gpuMgr, config, kmeansConfig, embConfig)
func NewClusterIntegration(manager *gpu.Manager, config *ClusterConfig, kmeansConfig *gpu.KMeansConfig, embConfig *gpu.EmbeddingIndexConfig) *ClusterIntegration {
	if config == nil {
		config = DefaultClusterConfig()
	}

	return &ClusterIntegration{
		config:       config,
		clusterIndex: gpu.NewClusterIndex(manager, embConfig, kmeansConfig),
	}
}

// AddEmbedding adds an embedding to the cluster index.
//
// Call this during index building phase, before OnIndexComplete().
// After clustering, use OnNodeUpdate() for incremental updates.
//
// Parameters:
//   - nodeID: Unique identifier for the node
//   - embedding: Vector embedding
//
// Returns error if dimensions mismatch.
//
// Example:
//
//	for _, node := range nodes {
//		err := ci.AddEmbedding(node.ID, node.Embedding)
//		if err != nil {
//			log.Printf("Failed to add %s: %v", node.ID, err)
//		}
//	}
func (ci *ClusterIntegration) AddEmbedding(nodeID string, embedding []float32) error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	return ci.clusterIndex.Add(nodeID, embedding)
}

// OnIndexComplete triggers k-means clustering after initial indexing.
//
// Call this method after all embeddings have been added via AddEmbedding().
// Clustering organizes embeddings into groups for fast approximate search.
//
// This is typically called:
//  1. After initial bulk loading
//  2. After periodic re-indexing
//  3. When ShouldRecluster() returns true
//
// Returns error if clustering fails.
//
// Example:
//
//	// After loading all embeddings
//	for _, emb := range embeddings {
//		ci.AddEmbedding(emb.ID, emb.Vector)
//	}
//
//	// Trigger clustering
//	if err := ci.OnIndexComplete(); err != nil {
//		log.Printf("Clustering failed: %v", err)
//	}
//
//	stats := ci.Stats()
//	fmt.Printf("Created %d clusters in %v\n",
//		stats.NumClusters, stats.ClusteringTime)
func (ci *ClusterIntegration) OnIndexComplete() error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	if !ci.config.Enabled {
		return nil
	}

	stats := ci.clusterIndex.ClusterStats()
	if stats.EmbeddingCount < ci.config.MinEmbeddingsForClustering {
		// Not enough embeddings to benefit from clustering
		return nil
	}

	err := ci.clusterIndex.Cluster()
	if err != nil {
		return err
	}

	ci.lastReclusterAt = int64(stats.EmbeddingCount)
	return nil
}

// Search performs cluster-accelerated similarity search.
//
// If clustering is enabled and has been performed:
//  1. Finds the k nearest clusters to the query
//  2. Searches only within those clusters
//  3. Returns top results from the candidate set
//
// Falls back to brute-force search if:
//   - Clustering is disabled
//   - Not yet clustered
//   - Too few embeddings
//
// Parameters:
//   - ctx: Context for cancellation
//   - query: Query embedding vector
//   - topK: Number of results to return
//
// Returns: SearchResult slice sorted by similarity (descending)
//
// Example:
//
//	results, err := ci.Search(ctx, queryEmbedding, 10)
//	for _, r := range results {
//		fmt.Printf("%s: %.3f\n", r.ID, r.Score)
//	}
func (ci *ClusterIntegration) Search(ctx context.Context, query []float32, topK int) ([]gpu.SearchResult, error) {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	ci.searchesTotal.Add(1)

	if !ci.config.Enabled || !ci.clusterIndex.IsClustered() {
		// Fall back to brute-force search
		return ci.clusterIndex.Search(query, topK)
	}

	ci.searchesClustered.Add(1)
	return ci.clusterIndex.SearchWithClusters(query, topK, ci.config.NumClustersSearch)
}

// OnNodeUpdate handles real-time embedding updates.
//
// Call this when a node's embedding changes after initial indexing.
// The embedding is reassigned to its nearest cluster without full re-clustering.
//
// For batch updates, consider calling Recluster() after the batch completes.
//
// Parameters:
//   - nodeID: Node identifier
//   - embedding: New embedding vector
//
// Returns error if update fails.
//
// Example:
//
//	// Node embedding changed
//	if err := ci.OnNodeUpdate("node-123", newEmbedding); err != nil {
//		log.Printf("Update failed: %v", err)
//	}
//
//	// Check if reclustering is recommended
//	if ci.ShouldRecluster() {
//		ci.Recluster()
//	}
func (ci *ClusterIntegration) OnNodeUpdate(nodeID string, embedding []float32) error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	err := ci.clusterIndex.OnNodeUpdate(nodeID, embedding)
	if err != nil {
		return err
	}

	// Check for auto-recluster
	if ci.config.AutoRecluster && ci.ShouldReclusterLocked() {
		// Trigger async recluster in background
		go func() {
			ci.Recluster()
		}()
	}

	return nil
}

// ShouldRecluster checks if re-clustering is recommended.
//
// Returns true if:
//   - Too many updates since last cluster (>threshold)
//   - Centroid drift exceeds threshold
//   - Too much time has passed
//
// Use this to decide when to call Recluster() for batch operations.
func (ci *ClusterIntegration) ShouldRecluster() bool {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return ci.ShouldReclusterLocked()
}

// ShouldReclusterLocked checks without acquiring lock.
// Caller must hold ci.mu.
func (ci *ClusterIntegration) ShouldReclusterLocked() bool {
	if !ci.config.Enabled {
		return false
	}
	return ci.clusterIndex.ShouldRecluster()
}

// Recluster performs a full re-clustering.
//
// This recomputes all clusters from scratch. Use when:
//   - ShouldRecluster() returns true
//   - After significant batch updates
//   - Cluster quality has degraded
//
// Returns error if clustering fails.
//
// Example:
//
//	if ci.ShouldRecluster() {
//		if err := ci.Recluster(); err != nil {
//			log.Printf("Recluster failed: %v", err)
//		}
//	}
func (ci *ClusterIntegration) Recluster() error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	if !ci.config.Enabled {
		return nil
	}

	err := ci.clusterIndex.Cluster()
	if err != nil {
		return err
	}

	stats := ci.clusterIndex.ClusterStats()
	ci.lastReclusterAt = int64(stats.EmbeddingCount)
	return nil
}

// Stats returns clustering statistics.
//
// Example:
//
//	stats := ci.Stats()
//	fmt.Printf("Clusters: %d, Avg Size: %.1f\n",
//		stats.NumClusters, stats.AvgClusterSize)
//	fmt.Printf("Search hit rate: %.1f%%\n",
//		float64(stats.SearchesClustered)/float64(stats.SearchesTotal)*100)
func (ci *ClusterIntegration) Stats() ClusterIntegrationStats {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	clusterStats := ci.clusterIndex.ClusterStats()

	return ClusterIntegrationStats{
		ClusterStats:      clusterStats,
		SearchesTotal:     ci.searchesTotal.Load(),
		SearchesClustered: ci.searchesClustered.Load(),
		Enabled:           ci.config.Enabled,
	}
}

// ClusterIntegrationStats holds statistics about cluster integration.
type ClusterIntegrationStats struct {
	gpu.ClusterStats

	// Inference-specific stats
	SearchesTotal     int64
	SearchesClustered int64
	Enabled           bool
}

// IsEnabled returns whether clustering is enabled.
func (ci *ClusterIntegration) IsEnabled() bool {
	return ci.config.Enabled
}

// IsClustered returns whether clustering has been performed.
func (ci *ClusterIntegration) IsClustered() bool {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return ci.clusterIndex.IsClustered()
}

// GetClusterIndex returns the underlying ClusterIndex for advanced usage.
//
// Use with caution - direct manipulation may cause inconsistencies.
func (ci *ClusterIntegration) GetClusterIndex() *gpu.ClusterIndex {
	return ci.clusterIndex
}

// SetConfig updates the cluster configuration.
//
// Changes take effect immediately for future operations.
// Does not trigger reclustering - call Recluster() if needed.
func (ci *ClusterIntegration) SetConfig(config *ClusterConfig) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.config = config
}

// GetConfig returns the current configuration.
func (ci *ClusterIntegration) GetConfig() *ClusterConfig {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return ci.config
}
