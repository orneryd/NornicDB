// Package heimdall provides comprehensive metrics collection for the cognitive guardian.
package heimdall

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"
)

// ============================================================================
// Comprehensive Metrics Collection
// ============================================================================

// NornicDBMetrics aggregates ALL database metrics for the SLM.
// This is the single source of truth for database observability.
type NornicDBMetrics struct {
	// Server metrics
	Server ServerMetrics `json:"server"`

	// Database metrics
	Database DatabaseMetrics `json:"database"`

	// Storage engine metrics
	Storage StorageMetrics `json:"storage"`

	// Cache metrics
	Cache CacheMetrics `json:"cache"`

	// Embedding metrics
	Embedding EmbeddingMetrics `json:"embedding"`

	// GPU metrics
	GPU GPUMetrics `json:"gpu"`

	// Query metrics
	Query QueryMetrics `json:"query"`

	// Runtime metrics
	Runtime RuntimeMetrics `json:"runtime"`

	// Timestamp
	CollectedAt time.Time `json:"collected_at"`
}

// ServerMetrics contains HTTP server statistics.
type ServerMetrics struct {
	Uptime         time.Duration `json:"uptime"`
	RequestsTotal  int64         `json:"requests_total"`
	ErrorsTotal    int64         `json:"errors_total"`
	ActiveRequests int64         `json:"active_requests"`
	SlowQueryCount int64         `json:"slow_query_count"`
	RequestsPerSec float64       `json:"requests_per_sec"`
}

// DatabaseMetrics contains core database statistics.
type DatabaseMetrics struct {
	NodeCount        int64                  `json:"node_count"`
	EdgeCount        int64                  `json:"edge_count"`
	LabelCounts      map[string]int64       `json:"label_counts,omitempty"`
	IndexCount       int                    `json:"index_count"`
	PropertyIndexes  int                    `json:"property_indexes"`
	CompositeIndexes int                    `json:"composite_indexes"`
	MVCCLifecycle    map[string]interface{} `json:"mvcc_lifecycle,omitempty"`
}

// StorageMetrics contains storage engine statistics.
type StorageMetrics struct {
	// Async engine stats
	PendingWrites int64 `json:"pending_writes"`
	TotalFlushes  int64 `json:"total_flushes"`

	// WAL stats
	WALSequence    uint64    `json:"wal_sequence"`
	WALEntries     uint64    `json:"wal_entries"`
	WALBytes       uint64    `json:"wal_bytes"`
	WALTotalWrites uint64    `json:"wal_total_writes"`
	WALTotalSyncs  uint64    `json:"wal_total_syncs"`
	WALLastSync    time.Time `json:"wal_last_sync"`

	// Node config stats
	NodeConfigs    int64   `json:"node_configs"`
	ConfigChecks   int64   `json:"config_checks"`
	ConfigsBlocked int64   `json:"configs_blocked"`
	BlockRate      float64 `json:"block_rate"`

	// Edge meta stats
	EdgeMetaRecords      int64            `json:"edge_meta_records"`
	EdgeMetaMaterialized int64            `json:"edge_meta_materialized"`
	EdgeMetaBySignal     map[string]int64 `json:"edge_meta_by_signal,omitempty"`
}

// CacheMetrics contains query cache statistics.
type CacheMetrics struct {
	Size      int     `json:"size"`
	MaxSize   int     `json:"max_size"`
	Hits      uint64  `json:"hits"`
	Misses    uint64  `json:"misses"`
	HitRate   float64 `json:"hit_rate"`
	Evictions uint64  `json:"evictions"`
	TTL       string  `json:"ttl"`
}

// EmbeddingMetrics contains embedding worker statistics.
type EmbeddingMetrics struct {
	WorkerRunning     bool    `json:"worker_running"`
	Processed         int     `json:"processed"`
	Failed            int     `json:"failed"`
	QueueLength       int     `json:"queue_length"`
	NodesWithEmbed    int64   `json:"nodes_with_embeddings"`
	NodesWithoutEmbed int64   `json:"nodes_without_embeddings"`
	EmbedRate         float64 `json:"embed_rate"`
	Provider          string  `json:"provider"`
	Model             string  `json:"model"`
	Dimensions        int     `json:"dimensions"`
}

// GPUMetrics contains GPU acceleration statistics.
type GPUMetrics struct {
	Available     bool   `json:"available"`
	Enabled       bool   `json:"enabled"`
	DeviceName    string `json:"device_name,omitempty"`
	Backend       string `json:"backend,omitempty"`
	MemoryMB      int    `json:"memory_mb,omitempty"`
	AllocatedMB   int    `json:"allocated_mb"`
	OperationsGPU int64  `json:"operations_gpu"`
	OperationsCPU int64  `json:"operations_cpu"`
	FallbackCount int64  `json:"fallback_count"`
}

// QueryMetrics contains Cypher query statistics.
type QueryMetrics struct {
	TotalQueries     int64         `json:"total_queries"`
	SlowQueries      int64         `json:"slow_queries"`
	AvgExecutionTime time.Duration `json:"avg_execution_time"`
	CacheHitRate     float64       `json:"cache_hit_rate"`
	ThresholdMs      int64         `json:"threshold_ms"`
}

// ============================================================================
// Metrics Collector Implementation
// ============================================================================

// MetricsCollector collects metrics from all NornicDB subsystems.
type MetricsCollector struct {
	mu sync.RWMutex

	// Database reference for metrics collection
	db DatabaseMetricsSource

	// Server reference for server metrics
	server ServerMetricsSource

	// Cache for expensive metrics
	cache     *NornicDBMetrics
	cacheTTL  time.Duration
	lastCache time.Time
}

// DatabaseMetricsSource is the interface for collecting database metrics.
type DatabaseMetricsSource interface {
	// Core stats
	Stats() interface{} // Returns DBStats or similar

	// Node/Edge counts
	NodeCount() (int64, error)
	EdgeCount() (int64, error)

	// Embed queue
	EmbedQueueStats() interface{}

	// Storage engine
	GetAsyncEngine() AsyncEngineStats
	GetWAL() WALStats
	GetSchemaManager() SchemaManagerStats

	// Query cache
	GetQueryCache() QueryCacheStats

	// GPU
	GetGPUManager() GPUManagerStats

	// Encryption
	EncryptionStats() map[string]interface{}
}

// AsyncEngineStats is the interface for async storage metrics.
type AsyncEngineStats interface {
	Stats() (pendingWrites, totalFlushes int64)
}

// WALStats is the interface for WAL metrics.
type WALStats interface {
	Stats() interface{}
}

// SchemaManagerStats is the interface for schema/index metrics.
type SchemaManagerStats interface {
	GetIndexStats() interface{}
}

// QueryCacheStats is the interface for cache metrics.
type QueryCacheStats interface {
	Stats() interface{}
}

// GPUManagerStats is the interface for GPU metrics.
type GPUManagerStats interface {
	IsEnabled() bool
	Device() interface{}
	Stats() interface{}
	AllocatedMemoryMB() int
}

// ServerMetricsSource is the interface for collecting server metrics.
type ServerMetricsSource interface {
	Stats() interface{}
	SlowQueryCount() int64
}

type mvccLifecycleMetricsSource interface {
	LifecycleStatus() (map[string]interface{}, error)
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(db DatabaseMetricsSource, server ServerMetricsSource) *MetricsCollector {
	return &MetricsCollector{
		db:       db,
		server:   server,
		cacheTTL: 5 * time.Second, // Cache expensive metrics for 5 seconds
	}
}

// Collect gathers all metrics from the database.
func (c *MetricsCollector) Collect() *NornicDBMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return cached metrics if still fresh
	if c.cache != nil && time.Since(c.lastCache) < c.cacheTTL {
		return c.cache
	}

	metrics := &NornicDBMetrics{
		CollectedAt: time.Now(),
	}

	// Collect runtime metrics (always fresh - very cheap)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	metrics.Runtime = RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  memStats.Alloc / 1024 / 1024,
		NumGC:          memStats.NumGC,
	}
	if source, ok := c.db.(mvccLifecycleMetricsSource); ok {
		if status, err := source.LifecycleStatus(); err == nil {
			metrics.Database.MVCCLifecycle = status
		}
	}

	// Note: Actual metric collection requires type assertions on the real types
	// The interfaces above define the contract - actual wiring happens in server.go

	c.cache = metrics
	c.lastCache = time.Now()

	return metrics
}

// Runtime returns current runtime metrics (always cheap to collect).
func (c *MetricsCollector) Runtime() RuntimeMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  memStats.Alloc / 1024 / 1024,
		NumGC:          memStats.NumGC,
	}
}

// ============================================================================
// Query Executor Implementation
// ============================================================================

// QueryExecutor provides read-only database access for Heimdall actions.
type QueryExecutor struct {
	db       QueryDatabase
	searcher SemanticSearcher // Optional: may be nil
	embedder Embedder         // Optional: may be nil
	timeout  time.Duration
}

// QueryDatabase is the interface for executing Cypher queries.
type QueryDatabase interface {
	// Query executes a read-only Cypher query
	Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error)

	// Stats returns basic database stats
	Stats() interface{}

	// NodeCount returns total nodes
	NodeCount() (int64, error)

	// EdgeCount returns total edges
	EdgeCount() (int64, error)
}

// SemanticSearcher is an optional interface for databases that support semantic search.
// QueryDatabase implementations may optionally implement this for vector search.
type SemanticSearcher interface {
	// HybridSearch performs vector + text search with pre-computed embedding
	HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SemanticSearchResult, error)
	// Search performs full-text BM25 search
	Search(ctx context.Context, query string, labels []string, limit int) ([]*SemanticSearchResult, error)
	// Neighbors returns connected node IDs
	Neighbors(ctx context.Context, nodeID string) ([]string, error)
	// GetEdgesForNode returns edges for a node
	GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error)
	// GetNode retrieves a node by ID
	GetNode(ctx context.Context, nodeID string) (*NodeData, error)
}

// SemanticSearchResult is the result of a semantic search operation.
type SemanticSearchResult struct {
	ID         string
	Labels     []string
	Properties map[string]interface{}
	Score      float64
}

// GraphEdge represents an edge in the graph.
type GraphEdge struct {
	ID         string
	Type       string
	SourceID   string
	TargetID   string
	Properties map[string]interface{}
}

// NodeData represents a node from the database.
type NodeData struct {
	ID         string
	Labels     []string
	Properties map[string]interface{}
}

// Embedder generates embeddings for text.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	ChunkText(text string, maxTokens, overlap int) ([]string, error)
}

// NewQueryExecutor creates a query executor with the given database.
func NewQueryExecutor(db QueryDatabase, timeout time.Duration) *QueryExecutor {
	return &QueryExecutor{
		db:      db,
		timeout: timeout,
	}
}

// NewQueryExecutorWithSearch creates a query executor with semantic search support.
func NewQueryExecutorWithSearch(db QueryDatabase, searcher SemanticSearcher, embedder Embedder, timeout time.Duration) *QueryExecutor {
	return &QueryExecutor{
		db:       db,
		searcher: searcher,
		embedder: embedder,
		timeout:  timeout,
	}
}

// Query implements DatabaseReader.Query
func (e *QueryExecutor) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	return e.db.Query(ctx, cypher, params)
}

// Stats implements DatabaseReader.Stats
func (e *QueryExecutor) Stats() DatabaseStats {
	nodeCount, _ := e.db.NodeCount()
	edgeCount, _ := e.db.EdgeCount()

	return DatabaseStats{
		NodeCount:         nodeCount,
		RelationshipCount: edgeCount,
		LabelCounts:       make(map[string]int64), // Can be expanded
	}
}

// Discover implements DatabaseReader.Discover for semantic search.
func (e *QueryExecutor) Discover(ctx context.Context, query string, nodeTypes []string, limit int, depth int) (*DiscoverResult, error) {
	if e.searcher == nil {
		return nil, fmt.Errorf("semantic search not available")
	}

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	method := "keyword"
	var searchResults []*SemanticSearchResult
	var err error

	// Try vector search if embedder is available.
	// For long queries, chunk using the embedder's own chunking method and fuse results across chunks.
	if e.embedder != nil {
		const (
			queryChunkSize    = 512
			queryChunkOverlap = 50
			maxQueryChunks    = 32
			outerRRFK         = 60
		)

		queryChunks, chunkErr := e.embedder.ChunkText(query, queryChunkSize, queryChunkOverlap)
		if chunkErr != nil {
			return nil, chunkErr
		}
		if len(queryChunks) > maxQueryChunks {
			queryChunks = queryChunks[:maxQueryChunks]
		}

		if len(queryChunks) <= 1 {
			queryEmbedding, embedErr := e.embedder.Embed(ctx, query)
			if embedErr == nil && len(queryEmbedding) > 0 {
				method = "vector"
				searchResults, err = e.searcher.HybridSearch(ctx, query, queryEmbedding, nodeTypes, limit)
			}
		} else {
			perChunkLimit := limit
			if perChunkLimit < 10 {
				perChunkLimit = 10
			}
			if perChunkLimit < limit*3 {
				perChunkLimit = limit * 3
			}
			if perChunkLimit > 100 {
				perChunkLimit = 100
			}

			type fused struct {
				best     *SemanticSearchResult
				scoreRRF float64 // outer RRF used for ordering across chunks
				bestSim  float64 // strongest underlying similarity across chunks
			}
			fusedByID := make(map[string]*fused)

			var usedVectorChunks int
			for _, chunkQuery := range queryChunks {
				emb, embedErr := e.embedder.Embed(ctx, chunkQuery)
				if embedErr != nil || len(emb) == 0 {
					continue
				}
				usedVectorChunks++

				chunkResults, searchErr := e.searcher.HybridSearch(ctx, chunkQuery, emb, nodeTypes, perChunkLimit)
				if searchErr != nil {
					continue
				}

				for rank, r := range chunkResults {
					if r == nil {
						continue
					}
					f := fusedByID[r.ID]
					if f == nil {
						f = &fused{best: r, bestSim: r.Score}
						fusedByID[r.ID] = f
					}
					// Outer RRF: 1/(k + rank), rank is 1-based. Used for ordering only.
					f.scoreRRF += 1.0 / (outerRRFK + float64(rank+1))
					if r.Score > f.bestSim {
						f.bestSim = r.Score
						f.best = r
					}
				}
			}

			if usedVectorChunks > 0 && len(fusedByID) > 0 {
				method = "vector"
				fusedList := make([]*fused, 0, len(fusedByID))
				for _, f := range fusedByID {
					fusedList = append(fusedList, f)
				}
				sort.Slice(fusedList, func(i, j int) bool {
					return fusedList[i].scoreRRF > fusedList[j].scoreRRF
				})
				if limit <= 0 {
					limit = 10
				}
				if len(fusedList) > limit {
					fusedList = fusedList[:limit]
				}

				searchResults = make([]*SemanticSearchResult, 0, len(fusedList))
				for _, f := range fusedList {
					if f.best == nil {
						continue
					}
					// Surface the strongest underlying similarity (cosine when
					// available), NOT the outer RRF rank-derived score. This
					// restores meaningful min_similarity threshold semantics
					// downstream.
					searchResults = append(searchResults, &SemanticSearchResult{
						ID:         f.best.ID,
						Labels:     f.best.Labels,
						Properties: f.best.Properties,
						Score:      f.bestSim,
					})
				}
			}
		}
	}

	// Fall back to text search
	if searchResults == nil {
		searchResults, err = e.searcher.Search(ctx, query, nodeTypes, limit)
	}

	if err != nil {
		return nil, err
	}

	// Convert to SearchResult and add related nodes
	results := make([]SearchResult, 0, len(searchResults))
	for _, r := range searchResults {
		result := SearchResult{
			ID:         r.ID,
			Type:       getLabelType(r.Labels),
			Title:      getStringProp(r.Properties, "title"),
			Similarity: r.Score,
			Properties: r.Properties,
		}

		// Get content preview
		if content := getStringProp(r.Properties, "content"); content != "" {
			if len(content) > 200 {
				result.ContentPreview = content[:200] + "..."
			} else {
				result.ContentPreview = content
			}
		}

		// Add related nodes if depth > 1
		if depth > 1 {
			result.Related = e.getRelatedNodes(ctx, r.ID, depth)
		}

		results = append(results, result)
	}

	return &DiscoverResult{
		Results: results,
		Method:  method,
		Total:   len(results),
	}, nil
}

// getRelatedNodes fetches nodes connected to the given node up to the specified depth.
func (e *QueryExecutor) getRelatedNodes(ctx context.Context, nodeID string, depth int) []RelatedNode {
	if e.searcher == nil || depth < 1 {
		return nil
	}

	var related []RelatedNode
	visited := make(map[string]bool)
	visited[nodeID] = true

	// BFS traversal
	type queueItem struct {
		id       string
		distance int
	}
	queue := []queueItem{{id: nodeID, distance: 0}}

	for len(queue) > 0 && len(related) < 100 {
		current := queue[0]
		queue = queue[1:]

		if current.distance >= depth {
			continue
		}

		// Get edges for this node
		edges, err := e.searcher.GetEdgesForNode(ctx, current.id)
		if err != nil {
			continue
		}

		for _, edge := range edges {
			var relatedID string
			var direction string
			if edge.SourceID == current.id {
				relatedID = edge.TargetID
				direction = "outgoing"
			} else {
				relatedID = edge.SourceID
				direction = "incoming"
			}

			if visited[relatedID] {
				continue
			}
			visited[relatedID] = true

			// Get node info
			node, err := e.searcher.GetNode(ctx, relatedID)
			if err != nil {
				continue
			}

			rel := RelatedNode{
				ID:           relatedID,
				Type:         getLabelType(node.Labels),
				Title:        getStringProp(node.Properties, "title"),
				Distance:     current.distance + 1,
				Relationship: edge.Type,
				Direction:    direction,
			}

			related = append(related, rel)

			// Add to queue for next level
			if current.distance+1 < depth {
				queue = append(queue, queueItem{id: relatedID, distance: current.distance + 1})
			}
		}
	}

	return related
}

// getLabelType extracts the primary label from labels slice.
func getLabelType(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}

// getStringProp extracts a string property from a map.
func getStringProp(props map[string]interface{}, key string) string {
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

// ============================================================================
// Real MetricsReader Implementation
// ============================================================================

// RealMetricsReader provides actual runtime metrics.
type RealMetricsReader struct{}

// Runtime returns current runtime metrics.
func (r *RealMetricsReader) Runtime() RuntimeMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  memStats.Alloc / 1024 / 1024,
		NumGC:          memStats.NumGC,
	}
}

// ============================================================================
// Default No-Op Logger Implementation
// ============================================================================

// DefaultLogger is a simple logger implementation.
type DefaultLogger struct {
	prefix string
}

// NewDefaultLogger creates a logger with the given prefix.
func NewDefaultLogger(prefix string) *DefaultLogger {
	return &DefaultLogger{prefix: prefix}
}

func (l *DefaultLogger) Debug(msg string, args ...interface{}) {}
func (l *DefaultLogger) Info(msg string, args ...interface{})  {}
func (l *DefaultLogger) Warn(msg string, args ...interface{})  {}
func (l *DefaultLogger) Error(msg string, args ...interface{}) {}
