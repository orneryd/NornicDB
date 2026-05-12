// Package search provides unified hybrid search with Reciprocal Rank Fusion (RRF).
//
// This package implements the same hybrid search approach used by production systems
// like Azure AI Search, Elasticsearch, and Weaviate: combining vector similarity search
// with BM25 full-text search using Reciprocal Rank Fusion.
//
// Search Capabilities:
//   - Vector similarity search (cosine similarity with HNSW index)
//   - BM25 full-text search (keyword matching with TF-IDF)
//   - RRF hybrid search (fuses vector + BM25 results)
//   - Adaptive weighting based on query characteristics
//   - Automatic fallback when one method fails
//
// Example Usage:
//
//	// Create search service
//	svc := search.NewService(storageEngine)
//
//	// Build indexes from existing nodes
//	if err := svc.BuildIndexes(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Perform hybrid search
//	query := "machine learning algorithms"
//	embedding := embedder.Embed(ctx, query) // Get from embed package
//	opts := search.DefaultSearchOptions()
//
//	response, err := svc.Search(ctx, query, embedding, opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for _, result := range response.Results {
//		fmt.Printf("[%.3f] %s\n", result.RRFScore, result.Title)
//	}
//
// How RRF Works:
//
// RRF (Reciprocal Rank Fusion) combines rankings from multiple search methods.
// Instead of merging scores directly (which can be incomparable), RRF uses rank
// positions to create a unified ranking.
//
// Formula: RRF_score = Σ (weight / (k + rank))
//
// Where:
//   - k is a constant (typically 60) to reduce the impact of high ranks
//   - rank is the position in the result list (1-indexed)
//   - weight allows emphasizing one method over another
//
// Example: A document ranked #1 in vector search and #3 in BM25:
//
//	RRF = (1.0 / (60 + 1)) + (1.0 / (60 + 3))
//	    = (1.0 / 61) + (1.0 / 63)
//	    = 0.0164 + 0.0159
//	    = 0.0323
//
// Documents that appear in both result sets get boosted scores.
//
// ELI12 (Explain Like I'm 12):
//
// Imagine two friends ranking pizza places:
//   - Friend A (vector search) ranks by taste similarity to your favorite
//   - Friend B (BM25) ranks by matching your description "spicy pepperoni"
//
// They might disagree! Friend A says place X is #1 (tastes similar), while
// Friend B says it's #5 (doesn't match keywords well).
//
// RRF solves this by:
// 1. If a place appears in BOTH lists, it gets bonus points
// 2. Higher ranks (being at the top) give more points
// 3. The magic number 60 prevents #1 from completely dominating
//
// This way, a place that's #2 in both lists beats a place that's #1 in one
// but missing from the other!
//
// Index persistence and WAL alignment:
//
// NornicDB storage uses a write-ahead log (WAL) for durability; graph state is recovered
// from WAL (and snapshots) on startup. Search indexes (BM25 and vector) are built or
// loaded after storage recovery, so they always reflect the WAL-consistent state. Index
// files use semver format versioning (Qdrant-style): only indexes with the same format
// version are loaded; older or newer versions are rejected so the caller can rebuild.
// Persisted indexes are saved on a debounced schedule after mutations and on shutdown.
//
// Result caching:
//
// Search() results are cached in-process by query + options (limit, types, rerank, MMR, etc.),
// with the same semantics as the Cypher query cache: LRU eviction (default 1000 entries),
// TTL (default 5 minutes), and full invalidation on IndexNode/RemoveNode so results stay
// correct after index changes. All call paths (HTTP search API, Cypher vector procedures,
// MCP, etc.) share this cache, so repeated identical searches return immediately.
package search

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
)

var ErrSearchIndexBuilding = errors.New("search index being built, please try again when they are complete")

// SearchableProperties defines PRIORITY properties for full-text search ranking.
// These properties are indexed first for better BM25 ranking.
// Note: ALL node properties are indexed, but these get priority weighting.
// These match Neo4j's fulltext index configuration.
var SearchableProperties = []string{
	"content",
	"text",
	"title",
	"name",
	"description",
	"path",
	"workerRole",
	"requirements",
}

const (
	// EnvSearchBM25Engine selects the BM25 engine implementation.
	// Supported values: "v1", "v2". Default: "v2".
	EnvSearchBM25Engine = "NORNICDB_SEARCH_BM25_ENGINE"
	// EnvSearchLogTimings enables per-query timing logs for search stages.
	// When true, logs vector/BM25/fusion/total timing to help identify bottlenecks.
	EnvSearchLogTimings = "NORNICDB_SEARCH_LOG_TIMINGS"
	BM25EngineV1        = "v1"
	BM25EngineV2        = "v2"
)

type bm25Index interface {
	Index(id, text string)
	Remove(id string)
	Search(query string, limit int) []indexResult
	PhraseSearch(query string, limit int) []indexResult
	GetDocument(id string) (string, bool)
	LexicalSeedDocIDs(maxTerms, perTerm int) []string
	Clear()
	Count() int
	Save(path string) error
	SaveNoCopy(path string) error
	Load(path string) error
	IsDirty() bool
	IndexBatch(entries []FulltextBatchEntry)
}

func normalizeBM25Engine(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case BM25EngineV1:
		return BM25EngineV1
	case BM25EngineV2:
		return BM25EngineV2
	default:
		return BM25EngineV2
	}
}

// DefaultBM25Engine returns the configured BM25 engine from environment.
// Valid values: "v1", "v2". Invalid/empty values fall back to "v2".
func DefaultBM25Engine() string {
	return normalizeBM25Engine(os.Getenv(EnvSearchBM25Engine))
}

func newBM25Index(engine string) (bm25Index, string) {
	switch normalizeBM25Engine(engine) {
	case BM25EngineV2:
		return NewFulltextIndexV2(), BM25EngineV2
	default:
		return NewFulltextIndex(), BM25EngineV1
	}
}

var searchablePropertiesSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(SearchableProperties))
	for _, p := range SearchableProperties {
		out[p] = struct{}{}
	}
	return out
}()

// SearchResult represents a unified search result.
type SearchResult struct {
	ID             string         `json:"id"`
	NodeID         storage.NodeID `json:"nodeId"`
	Type           string         `json:"type"`
	Labels         []string       `json:"labels"`
	Title          string         `json:"title,omitempty"`
	Description    string         `json:"description,omitempty"`
	ContentPreview string         `json:"content_preview,omitempty"`
	Properties     map[string]any `json:"properties,omitempty"`

	// Scoring
	Score      float64 `json:"score"`
	Similarity float64 `json:"similarity,omitempty"`

	// RRF metadata (vector_rank/bm25_rank are always emitted so clients see original
	// ranks even when Stage-2 reranking is applied; 0 means not in that result set)
	RRFScore   float64 `json:"rrf_score,omitempty"`
	VectorRank int     `json:"vector_rank"`
	BM25Rank   int     `json:"bm25_rank"`
}

// SearchResponse is the response from a search operation.
type SearchResponse struct {
	Status            string         `json:"status"`
	Query             string         `json:"query"`
	Results           []SearchResult `json:"results"`
	TotalCandidates   int            `json:"total_candidates"`
	Returned          int            `json:"returned"`
	SearchMethod      string         `json:"search_method"`
	FallbackTriggered bool           `json:"fallback_triggered"`
	Message           string         `json:"message,omitempty"`
	Metrics           *SearchMetrics `json:"metrics,omitempty"`
}

// SearchMetrics contains timing and statistics.
type SearchMetrics struct {
	VectorSearchTimeMs int `json:"vector_search_time_ms"`
	BM25SearchTimeMs   int `json:"bm25_search_time_ms"`
	FusionTimeMs       int `json:"fusion_time_ms"`
	TotalTimeMs        int `json:"total_time_ms"`
	VectorCandidates   int `json:"vector_candidates"`
	BM25Candidates     int `json:"bm25_candidates"`
	FusedCandidates    int `json:"fused_candidates"`
}

// SearchOptions configures the search behavior.
type SearchOptions struct {
	// Limit is the maximum number of results to return
	Limit int

	// MinSimilarity is the minimum similarity threshold for vector search.
	// nil = use service default, otherwise use the provided value.
	MinSimilarity *float64

	// Types filters results by node type (labels)
	Types []string

	// RRF configuration
	RRFK         float64 // RRF constant (default: 60)
	VectorWeight float64 // Weight for vector results (default: 1.0)
	BM25Weight   float64 // Weight for BM25 results (default: 1.0)
	MinRRFScore  float64 // Minimum RRF score threshold (default: 0.01)

	// MMR (Maximal Marginal Relevance) diversification
	// When enabled, results are re-ranked to balance relevance with diversity
	MMREnabled bool    // Enable MMR diversification (default: false)
	MMRLambda  float64 // Balance: 1.0 = pure relevance, 0.0 = pure diversity (default: 0.7)

	// Cross-encoder reranking (Stage 2)
	// When enabled, top candidates are re-scored using a cross-encoder model
	// for higher accuracy at the cost of latency
	RerankEnabled  bool    // Enable cross-encoder reranking (default: false)
	RerankTopK     int     // How many candidates to rerank (default: 100)
	RerankMinScore float64 // Minimum cross-encoder score to include (default: 0)

	// Filters pre-filters nodes by property values before top-K selection.
	// Keys are property names; values are acceptable values (OR within a key, AND across keys).
	// Scalar and array property values are both supported.
	Filters map[string][]string
}

// DefaultSearchOptions returns sensible defaults.
func DefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		Limit:          50,
		MinSimilarity:  nil, // nil = use service default or 0.5 fallback
		RRFK:           60,
		VectorWeight:   1.0,
		BM25Weight:     1.0,
		MinRRFScore:    0.01,
		MMREnabled:     false,
		MMRLambda:      0.7, // Balanced: 70% relevance, 30% diversity
		RerankEnabled:  false,
		RerankTopK:     100,
		RerankMinScore: 0.0,
	}
}

// GetMinSimilarity returns the MinSimilarity value, or the fallback if nil.
func (o *SearchOptions) GetMinSimilarity(fallback float64) float64 {
	if o.MinSimilarity != nil {
		return *o.MinSimilarity
	}
	return fallback
}

// searchResultCacheEntry holds a cached SearchResponse and expiry (same semantics as Cypher query cache).
type searchResultCacheEntry struct {
	response *SearchResponse
	expires  time.Time
}

// searchResultCache is an LRU cache for search results keyed by query + options.
// Invalidated on IndexNode/RemoveNode so results stay correct after index changes.
type searchResultCache struct {
	mu      sync.RWMutex
	entries map[string]*searchResultCacheEntry
	lru     []string // key order, oldest first for eviction
	maxSize int
	ttl     time.Duration
}

func newSearchResultCache(maxSize int, ttl time.Duration) *searchResultCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &searchResultCache{
		entries: make(map[string]*searchResultCacheEntry, maxSize),
		lru:     make([]string, 0, maxSize),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// searchCacheKey builds a deterministic key for query + options (same inputs => same key).
func searchCacheKey(query string, opts *SearchOptions) string {
	if opts == nil {
		opts = DefaultSearchOptions()
	}
	typesCopy := make([]string, len(opts.Types))
	copy(typesCopy, opts.Types)
	sort.Strings(typesCopy)

	// Build a stable representation of Filters: sorted keys, sorted values within each key.
	filterKeys := make([]string, 0, len(opts.Filters))
	for k := range opts.Filters {
		filterKeys = append(filterKeys, k)
	}
	sort.Strings(filterKeys)
	filterParts := make([]string, 0, len(filterKeys))
	for _, k := range filterKeys {
		vals := make([]string, len(opts.Filters[k]))
		copy(vals, opts.Filters[k])
		sort.Strings(vals)
		filterParts = append(filterParts, k+"="+strings.Join(vals, ","))
	}

	return strings.Join([]string{
		query,
		strconv.Itoa(opts.Limit),
		strings.Join(typesCopy, "|"),
		strconv.FormatBool(opts.RerankEnabled),
		strconv.Itoa(opts.RerankTopK),
		strconv.FormatBool(opts.MMREnabled),
		strconv.FormatFloat(opts.MMRLambda, 'g', -1, 64),
		strconv.FormatFloat(opts.RerankMinScore, 'g', -1, 64),
		strings.Join(filterParts, ";"),
	}, "\x00")
}

func (c *searchResultCache) Get(key string) *SearchResponse {
	c.mu.RLock()
	ent, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || ent == nil {
		return nil
	}
	if c.ttl > 0 && time.Now().After(ent.expires) {
		c.mu.Lock()
		delete(c.entries, key)
		for i, k := range c.lru {
			if k == key {
				c.lru = append(c.lru[:i], c.lru[i+1:]...)
				break
			}
		}
		c.mu.Unlock()
		return nil
	}
	return ent.response
}

func (c *searchResultCache) Put(key string, response *SearchResponse) {
	if response == nil {
		return
	}
	expires := time.Time{}
	if c.ttl > 0 {
		expires = time.Now().Add(c.ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[key]; !exists {
		for len(c.lru) >= c.maxSize {
			evict := c.lru[0]
			c.lru = c.lru[1:]
			delete(c.entries, evict)
		}
		c.lru = append(c.lru, key)
	}
	c.entries[key] = &searchResultCacheEntry{response: response, expires: expires}
}

func (c *searchResultCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*searchResultCacheEntry, c.maxSize)
	c.lru = c.lru[:0]
}

// Service provides unified hybrid search with automatic index management.
//
// The Service maintains:
//   - Vector index (HNSW for fast approximate nearest neighbor search)
//   - Full-text index (BM25 with inverted index)
//   - Connection to storage engine for node data enrichment
//
// Thread-safe: Multiple goroutines can call Search() concurrently.
//
// Example:
//
//	svc := search.NewService(engine)
//	defer svc.Close()
//
//	// Index existing data
//	if err := svc.BuildIndexes(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Index new nodes as they're created
//	node := &storage.Node{...}
//	if err := svc.IndexNode(node); err != nil {
//		log.Printf("Failed to index: %v", err)
//	}
type Service struct {
	engine          storage.Engine
	vectorIndex     *VectorIndex
	vectorFileStore *VectorFileStore // when set, vectors are stored on disk (low-RAM build)
	// Primary BM25 implementation used by the live search pipeline.
	fulltextIndex bm25Index
	bm25Engine    string
	reranker      Reranker
	mu            sync.RWMutex
	// indexMu serializes index mutation operations (IndexNode/RemoveNode/BuildIndexes batches)
	// without blocking read paths that use s.mu for lightweight config/state reads.
	indexMu        sync.Mutex
	ready          atomic.Bool
	buildAttempted atomic.Bool
	// resumeVectorBuild skips re-adding vectors already present in vectorFileStore during BuildIndexes.
	resumeVectorBuild bool

	// cypherMetadata is a small, in-memory view of node embedding availability and labels.
	// It allows Cypher-compatible vector queries to execute without scanning storage.
	nodeLabels       map[string][]string          // nodeID -> labels
	nodeNamedVector  map[string]map[string]string // nodeID -> vectorName -> vectorID
	nodePropVector   map[string]map[string]string // nodeID -> propertyKey -> vectorID
	nodeChunkVectors map[string][]string          // nodeID -> vectorIDs (main + chunks)

	// HNSW index for approximate nearest neighbor search (optional, lazy-initialized)
	hnswIndex           *HNSWIndex
	hnswMu              sync.RWMutex
	hnswMaintOnce       sync.Once
	hnswMaintStop       chan struct{}
	hnswRebuildInFlight atomic.Bool
	hnswLastRebuildUnix atomic.Int64
	// hnswDeferredMutations counts vector add/remove mutations that skipped live HNSW updates
	// (to keep read latency low under heavy write/embed load). Maintenance rebuild consumes this.
	hnswDeferredMutations atomic.Int64

	// Optional GPU-accelerated brute-force embedding index.
	// When enabled, this is used as an exact vector-search backend.
	gpuManager        *gpu.Manager
	gpuEmbeddingIndex *gpu.EmbeddingIndex

	// GPU k-means clustering for accelerated search (optional)
	clusterIndex     *gpu.ClusterIndex
	clusterEnabled   bool
	clusterUsesGPU   bool
	kmeansInProgress atomic.Bool

	// Optional IVF-HNSW acceleration: build one HNSW per cluster to use centroids
	// as a routing layer for CPU-only large datasets.
	clusterHNSWMu sync.RWMutex
	clusterHNSW   map[int]*HNSWIndex

	// Optional compressed ANN index (IVF/PQ), active only when
	// NORNICDB_VECTOR_ANN_QUALITY=compressed and readiness checks pass.
	ivfpqMu    sync.RWMutex
	ivfpqIndex *IVFPQIndex

	// Optional lexical profiles per cluster for hybrid lexical-semantic routing.
	// Built from BM25-indexable text and used to bias cluster routing.
	clusterLexicalMu       sync.RWMutex
	clusterLexicalProfiles map[int]map[string]float64

	// minEmbeddingsForClustering is the minimum number of embeddings needed
	// before k-means clustering provides any benefit. Configurable via
	// SetMinEmbeddingsForClustering(). Default: 1000
	minEmbeddingsForClustering int

	// defaultMinSimilarity is the minimum cosine similarity threshold for vector search.
	// Apple Intelligence embeddings produce scores in 0.2-0.8 range, bge-m3/mxbai produce 0.7-0.99.
	// Configurable via SetDefaultMinSimilarity(). Default: -1 (not set, use SearchOptions default)
	// Set to 0.0 to let RRF handle relevance filtering.
	defaultMinSimilarity float64

	// Vector search pipeline (lazy-initialized)
	vectorPipeline *VectorSearchPipeline
	pipelineMu     sync.RWMutex

	// Index persistence: when both paths are set, BuildIndexes tries to load BM25 and vector
	// indexes from disk; if both load successfully with count > 0, the full iteration is skipped.
	// When hnswIndexPath is set, the HNSW index is also saved/loaded so it does not need rebuilding.
	fulltextIndexPath string
	vectorIndexPath   string
	hnswIndexPath     string

	// Debounced persist: after IndexNode/RemoveNode we schedule a write to disk after an idle delay.
	persistMu        sync.Mutex
	persistRunMu     sync.Mutex
	persistTimer     *time.Timer
	buildInProgress  atomic.Bool
	persistEnabled   atomic.Bool
	buildPhase       atomic.Value // string
	buildStartedUnix atomic.Int64
	buildPhaseUnix   atomic.Int64
	buildTotalNodes  atomic.Int64
	buildProcessed   atomic.Int64

	// resultCache caches Search() results by query+options (same semantics as Cypher query cache).
	// All call paths (HTTP search, Cypher, etc.) benefit. Invalidated on IndexNode/RemoveNode.
	resultCache *searchResultCache

	nodeDecayFilter NodeDecayFilterFunc

	strategyTransitionMu         sync.Mutex
	strategyTransitionInProgress bool
	strategyTransitionPending    strategyMode
	strategyTransitionTimer      *time.Timer
	strategyTransitionSeq        uint64
	strategyTransitionDeltas     []strategyDeltaMutation
	strategyTransitionStarts     uint64

	// Plan 04-05-05: observability metric bag + pre-bound observers for
	// the hybrid hot path. AttachMetrics injects + binds; observeSearchStage
	// picks the bound observer when mode=hybrid (fast path), lazy-binds
	// otherwise. nil-tolerated for tests / embedded-library callers.
	// See pkg/search/observability.go for the chokepoint helpers.
	metrics            *observability.SearchMetrics
	boundDurationIndex observability.BoundLatencyObserver
	boundDurationFuse  observability.BoundLatencyObserver
}

type strategyMode int

const (
	strategyModeUnknown strategyMode = iota
	strategyModeBruteCPU
	strategyModeBruteGPU
	strategyModeHNSW
)

type strategyDeltaMutation struct {
	seq uint64
	id  string
	add bool
}

// BuildProgress reports in-progress indexing state for UI/ops visibility.
type BuildProgress struct {
	Ready           bool
	Building        bool
	Phase           string
	ProcessedNodes  int64
	TotalNodes      int64
	RateNodesPerSec float64
	ETASeconds      int64 // -1 when unknown
}

// NewService creates a new search Service with empty indexes.
//
// The service is created with:
//   - 1024-dimensional vector index (default for bge-m3)
//   - Empty full-text index
//   - Reference to storage engine for data enrichment
//
// Call BuildIndexes() after creation to populate indexes from existing data.
//
// Example:
//
//	engine, _ := storage.NewMemoryEngine()
//	svc := search.NewService(engine)
//
//	// Build indexes from all nodes
//	if err := svc.BuildIndexes(context.Background()); err != nil {
//		log.Fatal(err)
//	}
//
// Returns a new Service ready for indexing and searching.
//
// Example 1 - Basic Setup:
//
//	engine := storage.NewMemoryEngine()
//	svc := search.NewService(engine)
//	defer svc.Close()
//
//	// Build indexes from existing nodes
//	if err := svc.BuildIndexes(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Now ready to search
//	results, _ := svc.Search(ctx, "machine learning", nil, nil)
//
// Example 2 - With Embedder Integration:
//
//	engine := storage.NewBadgerEngine("./data")
//	svc := search.NewService(engine)
//
//	// Create embedder
//	embedder := embed.NewOllama(embed.DefaultOllamaConfig())
//
//	// Index documents with embeddings
//	for _, doc := range documents {
//		node := &storage.Node{
//			ID: storage.NodeID(doc.ID),
//			Labels: []string{"Document"},
//			Properties: map[string]any{
//				"title":   doc.Title,
//				"content": doc.Content,
//			},
//			Embedding: embedder.Embed(ctx, doc.Content),
//		}
//		engine.CreateNode(node)
//		svc.IndexNode(node)
//	}
//
// Example 3 - Real-time Indexing:
//
//	svc := search.NewService(engine)
//
//	// Index as nodes are created
//	onCreate := func(node *storage.Node) {
//		if err := svc.IndexNode(node); err != nil {
//			log.Printf("Index failed: %v", err)
//		}
//	}
//
//	// Hook into storage engine
//	engine.OnNodeCreate(onCreate)
//
// ELI12:
//
// Think of NewService like building a library with two special catalogs:
//  1. A "similarity catalog" (vector index) - finds books that are LIKE what you want
//  2. A "keyword catalog" (fulltext index) - finds books with specific words
//
// When you search, the library assistant checks BOTH catalogs and shows you
// the books that appear in both lists first. That's hybrid search!
//
// Performance:
//   - Vector index: HNSW algorithm, O(log n) search
//   - Fulltext index: Inverted index, O(k + m) where k = unique terms, m = matches
//   - Memory: ~4KB per 1000-dim embedding + ~500 bytes per document
//
// Thread Safety:
//
//	Safe for concurrent searches from multiple goroutines.

// DefaultVectorDimensions is the embedding size used by NewService (e.g. bge-m3).
// Use NewServiceWithDimensions or schema/query-derived dimensions when your model differs.
const DefaultVectorDimensions = 1024

func NewService(engine storage.Engine) *Service {
	return NewServiceWithDimensions(engine, DefaultVectorDimensions)
}

// NewServiceWithDimensions creates a search Service with the specified embedding dimensions.
// Use this when your embedding model produces vectors of a different size than the default 1024.
//
// Example:
//
//	// For Apple Intelligence embeddings (512 dimensions)
//	svc := search.NewServiceWithDimensions(engine, 512)
//
//	// For OpenAI text-embedding-3-small (1536 dimensions)
//	svc := search.NewServiceWithDimensions(engine, 1536)
func NewServiceWithDimensions(engine storage.Engine, dimensions int) *Service {
	return NewServiceWithDimensionsAndBM25Engine(engine, dimensions, DefaultBM25Engine())
}

// NewServiceWithDimensionsAndBM25Engine creates a search Service with explicit BM25 engine selection.
// bm25Engine values: "v1", "v2" (invalid values default to "v1").
func NewServiceWithDimensionsAndBM25Engine(engine storage.Engine, dimensions int, bm25Engine string) *Service {
	fulltextIndex, selectedBM25Engine := newBM25Index(bm25Engine)
	svc := &Service{
		engine:                     engine,
		vectorIndex:                NewVectorIndex(dimensions),
		fulltextIndex:              fulltextIndex,
		bm25Engine:                 selectedBM25Engine,
		minEmbeddingsForClustering: DefaultMinEmbeddingsForClustering,
		defaultMinSimilarity:       -1, // -1 = not set, use SearchOptions default
		nodeLabels:                 make(map[string][]string, 1024),
		nodeNamedVector:            make(map[string]map[string]string, 1024),
		nodePropVector:             make(map[string]map[string]string, 1024),
		nodeChunkVectors:           make(map[string][]string, 1024),
		clusterLexicalProfiles:     make(map[int]map[string]float64),
		resultCache:                newSearchResultCache(1000, 5*time.Minute), // same order as Cypher query cache
	}
	svc.ready.Store(false)
	svc.persistEnabled.Store(false)
	svc.buildPhase.Store("idle")
	svc.buildPhaseUnix.Store(time.Now().Unix())
	log.Printf("📇 Search: BM25 engine selected: %s", selectedBM25Engine)
	return svc
}

// IsReady reports whether the search indexes are fully built and ready to serve queries.
func (s *Service) IsReady() bool {
	return s.ready.Load()
}

// BM25Engine returns the configured BM25 engine for this service ("v1" or "v2").
func (s *Service) BM25Engine() string {
	return normalizeBM25Engine(s.bm25Engine)
}

// BuildInProgress reports whether BuildIndexes is currently running.
func (s *Service) BuildInProgress() bool {
	return s.buildInProgress.Load()
}

// GetBuildProgress returns current search build progress for status APIs.
func (s *Service) GetBuildProgress() BuildProgress {
	phase, _ := s.buildPhase.Load().(string)
	if phase == "" {
		phase = "idle"
	}
	processed := s.buildProcessed.Load()
	total := s.buildTotalNodes.Load()
	building := s.buildInProgress.Load()
	ready := s.IsReady()
	rate := 0.0
	eta := int64(-1)

	startUnix := s.buildStartedUnix.Load()
	if building && startUnix > 0 && processed > 0 {
		elapsed := time.Since(time.Unix(startUnix, 0)).Seconds()
		if elapsed > 0 {
			rate = float64(processed) / elapsed
			if total > processed && rate > 0 {
				eta = int64(float64(total-processed) / rate)
			}
		}
	}

	return BuildProgress{
		Ready:           ready,
		Building:        building,
		Phase:           phase,
		ProcessedNodes:  processed,
		TotalNodes:      total,
		RateNodesPerSec: rate,
		ETASeconds:      eta,
	}
}

// CurrentStrategy returns the currently active vector search strategy label.
// Returns "unknown" until a vector pipeline has been initialized.
func (s *Service) CurrentStrategy() string {
	return s.currentPipelineStrategy().String()
}

func (s *Service) setBuildPhase(phase string) {
	s.buildPhase.Store(phase)
	s.buildPhaseUnix.Store(time.Now().Unix())
}

// SetGPUManager enables GPU acceleration for exact brute-force vector search.
//
// This is independent from k-means clustering. When enabled, the vector pipeline
// may choose GPU brute-force search (exact) for datasets where it outperforms HNSW.
func (s *Service) SetGPUManager(manager *gpu.Manager) {
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.gpuManager = manager
	if manager == nil || !manager.IsEnabled() {
		s.gpuEmbeddingIndex = nil
		return
	}

	dimensions := 0
	if s.vectorIndex != nil {
		dimensions = s.vectorIndex.GetDimensions()
	}
	if dimensions <= 0 {
		return
	}

	cfg := gpu.DefaultEmbeddingIndexConfig(dimensions)
	cfg.GPUEnabled = true
	cfg.AutoSync = true
	cfg.BatchThreshold = 1000
	gi := gpu.NewEmbeddingIndex(manager, cfg)

	// Snapshot current vectors.
	s.vectorIndex.mu.RLock()
	ids := make([]string, 0, len(s.vectorIndex.vectors))
	embs := make([][]float32, 0, len(s.vectorIndex.vectors))
	for id, vec := range s.vectorIndex.vectors {
		if len(vec) == 0 {
			continue
		}
		ids = append(ids, id)
		embs = append(embs, vec)
	}
	s.vectorIndex.mu.RUnlock()

	_ = gi.AddBatch(ids, embs)
	_ = gi.SyncToGPU()

	s.gpuEmbeddingIndex = gi
}

// EnableClustering enables GPU k-means clustering for accelerated vector search.
//
// Performance Improvements (Real-World Benchmarks):
//   - 2,000 embeddings: ~14% faster (61ms vs 65ms avg)
//   - 4,500 embeddings: ~26% faster (35ms vs 47ms avg)
//   - 10,000+ embeddings: 10-50x faster (scales with dataset size)
//
// The speedup increases with dataset size as the cluster-based search
// avoids comparing against all vectors.
//
// Parameters:
//   - gpuManager: GPU manager for acceleration (can be nil for CPU-only)
//   - numClusters: Number of k-means clusters (0 for auto based on dataset size)
//
// Call this BEFORE BuildIndexes(), then call TriggerClustering() after indexing.
//
// Example:
//
//	gpuManager, _ := gpu.NewManager(nil)
//	svc.EnableClustering(gpuManager, 100) // 100 clusters
//	svc.BuildIndexes(ctx)
//	svc.TriggerClustering(ctx) // Run k-means
func (s *Service) EnableClustering(gpuManager *gpu.Manager, numClusters int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If clustering is already enabled, only rebuild when switching CPU<->GPU.
	if s.clusterEnabled && s.clusterIndex != nil {
		requestGPU := gpuManager != nil && gpuManager.IsEnabled()
		if s.clusterUsesGPU == requestGPU {
			return
		}
	}

	// Cluster count: env override, else 0 = auto from dataset size at trigger time (sqrt(n/2), clamped).
	envK := envutil.GetInt("NORNICDB_KMEANS_NUM_CLUSTERS", 0)
	if envK > 0 {
		numClusters = envK
	} else if numClusters <= 0 {
		numClusters = 0 // Auto: gpu.optimalK(n) when TriggerClustering runs
	}
	autoK := numClusters <= 0

	// Max iterations is a cap; clustering stops when assignments stop changing (early convergence).
	// Default 5 prioritizes startup/build speed; override via NORNICDB_KMEANS_MAX_ITERATIONS.
	maxIter := envutil.GetInt("NORNICDB_KMEANS_MAX_ITERATIONS", 5)
	if maxIter < 5 {
		maxIter = 5
	}
	if maxIter > 500 {
		maxIter = 500
	}
	kmeansConfig := &gpu.KMeansConfig{
		NumClusters:   numClusters,
		AutoK:         autoK,
		MaxIterations: maxIter,
		Tolerance:     0.001,
		InitMethod:    "kmeans++",
	}

	// Use the same dimensions as the vector index (no hardcoded fallback)
	if s.vectorIndex == nil {
		log.Printf("[K-MEANS] ⚠️ Cannot enable clustering: vector index not initialized")
		return
	}
	dimensions := s.vectorIndex.dimensions
	embConfig := gpu.DefaultEmbeddingIndexConfig(dimensions)

	s.clusterIndex = gpu.NewClusterIndex(gpuManager, embConfig, kmeansConfig)
	s.clusterEnabled = true
	s.clusterUsesGPU = gpuManager != nil && gpuManager.IsEnabled()

	// Backfill embeddings from the current vector index so clustering can run
	// immediately, even if indexes were built before clustering was enabled.
	if s.vectorIndex != nil {
		s.vectorIndex.mu.RLock()
		ids := make([]string, 0, len(s.vectorIndex.vectors))
		embs := make([][]float32, 0, len(s.vectorIndex.vectors))
		for id, vec := range s.vectorIndex.vectors {
			if len(vec) == 0 {
				continue
			}
			copyVec := make([]float32, len(vec))
			copy(copyVec, vec)
			ids = append(ids, id)
			embs = append(embs, copyVec)
		}
		s.vectorIndex.mu.RUnlock()

		_ = s.clusterIndex.AddBatch(ids, embs) // Best effort
	}

	mode := "CPU"
	if gpuManager != nil {
		mode = "GPU"
	}
	clusterDesc := fmt.Sprintf("%d", numClusters)
	if autoK {
		clusterDesc = "auto"
	}
	log.Printf("[K-MEANS] ✅ Clustering ENABLED | mode=%s clusters=%s max_iter=%d init=%s",
		mode, clusterDesc, kmeansConfig.MaxIterations, kmeansConfig.InitMethod)
}

// DefaultMinEmbeddingsForClustering is the default minimum number of embeddings
// needed before k-means clustering provides any benefit. Below this threshold,
// brute-force search is faster than cluster overhead.
//
// This value can be overridden per-service using SetMinEmbeddingsForClustering().
//
// Performance Scaling (Real-World Benchmarks):
//   - <1000 embeddings: Clustering overhead > speedup benefit
//   - 2,000 embeddings: ~14% faster with clustering
//   - 4,500 embeddings: ~26% faster with clustering
//   - 10,000+ embeddings: 10-50x faster with clustering
//
// Tuning Guidelines:
//   - 1000 (default): Safe for most workloads, proven performance benefit
//   - 500-1000: Use for latency-sensitive apps (14-26% speedup range)
//   - 100-500: Testing or small datasets (verify clustering works)
//   - 2000+: Very large datasets (maximize speedup, delay until more data)
//
// Environment Variable: NORNICDB_KMEANS_MIN_EMBEDDINGS (overrides default)
const DefaultMinEmbeddingsForClustering = 1000

// fulltextBuildBatchSize is the number of nodes to index in one fulltext IndexBatch during BuildIndexes.
const fulltextBuildBatchSize = 5000

// fulltextIndexChunkSize controls cancellability granularity during BuildIndexes.
// Large IndexBatch calls can run for a long time (especially BM25 v2), so we split
// batch writes into smaller chunks and check ctx between chunks.
const fulltextIndexChunkSize = 250

// buildIndexPersistInterval removed: we no longer checkpoint persist during BuildIndexes
// because the append-only vector store can be rebuilt from .vec on resume.

// ensureClusterIndexBackfilled backfills the cluster index from the canonical vector store
// (file-backed or in-memory) when clusterIndex has fewer than targetCount embeddings.
// This ensures k-means runs on the full set when the cluster index was lagging (e.g. after
// BuildIndexes or when stats haven't caught up). Caller must not hold s.mu.
func (s *Service) ensureClusterIndexBackfilled(targetCount int) {
	s.mu.RLock()
	clusterIndex := s.clusterIndex
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	s.mu.RUnlock()
	if clusterIndex == nil || clusterIndex.Count() >= targetCount {
		return
	}
	if vfs != nil {
		_ = vfs.IterateChunked(5000, func(ids []string, vecs [][]float32) error {
			_ = clusterIndex.AddBatch(ids, vecs)
			return nil
		})
		log.Printf("[K-MEANS] 🔍 Backfilled cluster index from file store (target=%d → count=%d)", targetCount, clusterIndex.Count())
	} else if vi != nil {
		vi.mu.RLock()
		ids := make([]string, 0, len(vi.vectors))
		embs := make([][]float32, 0, len(vi.vectors))
		for id, vec := range vi.vectors {
			if len(vec) == 0 {
				continue
			}
			copyVec := make([]float32, len(vec))
			copy(copyVec, vec)
			ids = append(ids, id)
			embs = append(embs, copyVec)
		}
		vi.mu.RUnlock()
		_ = clusterIndex.AddBatch(ids, embs)
		log.Printf("[K-MEANS] 🔍 Backfilled cluster index from in-memory store (%d vectors)", len(ids))
	}
}

// TriggerClustering runs k-means clustering on all indexed embeddings.
// Stops promptly when ctx is cancelled (e.g. process shutdown).
// Only one run executes at a time per service; if clustering is already in progress,
// returns nil immediately (debounced).
//
// Trigger Policies:
//   - After bulk loads: Automatically called after BuildIndexes() completes
//   - Periodic clustering: Background timer runs clustering at regular intervals
//   - Manual trigger: Call this after bulk data loading to enable k-means routing
//
// Once clustering completes, the vector search pipeline automatically uses
// KMeansCandidateGen for candidate generation, providing significant speedup
// for very large datasets (N > 100K).
//
// Returns nil (not error) if there are too few embeddings - clustering will
// be skipped silently as brute-force search is faster for small datasets.
// Returns error only if clustering is not enabled or fails unexpectedly.
func (s *Service) TriggerClustering(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.kmeansInProgress.CompareAndSwap(false, true) {
		log.Printf("[K-MEANS] ⏭️  SKIPPED | reason=already_running")
		return nil
	}
	defer s.kmeansInProgress.Store(false)

	s.mu.RLock()
	clusterIndex := s.clusterIndex
	threshold := s.minEmbeddingsForClustering
	s.mu.RUnlock()

	if clusterIndex == nil {
		log.Printf("[K-MEANS] ❌ SKIPPED | reason=not_enabled")
		return fmt.Errorf("clustering not enabled - call EnableClustering() first")
	}

	if threshold <= 0 {
		threshold = DefaultMinEmbeddingsForClustering
	}
	// Use canonical total from vector store so threshold and logs match reality (e.g. 917K not 733K).
	totalCount := s.EmbeddingCount()
	if totalCount < threshold {
		log.Printf("[K-MEANS] ⏭️  SKIPPED | embeddings=%d threshold=%d reason=too_few_embeddings",
			totalCount, threshold)
		return nil
	}

	// Backfill cluster index from store so k-means runs on full set when cluster index was lagging.
	s.ensureClusterIndexBackfilled(totalCount)
	embeddingCount := clusterIndex.Count()
	if embeddingCount < totalCount {
		log.Printf("[K-MEANS] ⚠️  Cluster index has %d embeddings (store has %d); clustering with %d",
			embeddingCount, totalCount, embeddingCount)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	s.applyBM25SeedHints()
	log.Printf("[K-MEANS] 🔄 STARTING | embeddings=%d", totalCount)
	startTime := time.Now()

	if err := clusterIndex.ClusterWithContext(ctx); err != nil {
		if ctx.Err() != nil {
			log.Printf("[K-MEANS] ⏹️  CANCELLED | embeddings=%d (shutdown)", totalCount)
			return err
		}
		log.Printf("[K-MEANS] ❌ FAILED | embeddings=%d error=%v", totalCount, err)
		return fmt.Errorf("clustering failed: %w", err)
	}

	elapsed := time.Since(startTime)
	stats := clusterIndex.ClusterStats()
	log.Printf("[K-MEANS] ✅ COMPLETE | clusters=%d embeddings=%d iterations=%d duration=%v avg_cluster_size=%.1f",
		stats.NumClusters, stats.EmbeddingCount, stats.Iterations, elapsed, stats.AvgClusterSize)
	s.rebuildClusterLexicalProfiles()

	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if envutil.GetBoolStrict("NORNICDB_VECTOR_IVF_HNSW_ENABLED", false) {
		if err := s.rebuildClusterHNSWIndexes(ctx, clusterIndex); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[IVF-HNSW] ⚠️  build skipped: %v", err)
		}
	}

	return nil
}

func (s *Service) rebuildClusterHNSWIndexes(ctx context.Context, clusterIndex *gpu.ClusterIndex) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if clusterIndex == nil || !clusterIndex.IsClustered() {
		return fmt.Errorf("cluster index not clustered")
	}
	s.mu.RLock()
	clusterUsesGPU := s.clusterUsesGPU
	s.mu.RUnlock()
	if clusterUsesGPU {
		log.Printf("[IVF-HNSW] ⏭️  SKIPPED | reason=gpu_cluster_routing_enabled")
		return nil
	}

	s.mu.RLock()
	hnswPath := s.hnswIndexPath
	s.mu.RUnlock()
	dims := s.VectorIndexDimensions()
	if dims <= 0 {
		return fmt.Errorf("vector index unavailable")
	}
	vectorLookup := s.getVectorLookup()

	minClusterSize := envutil.GetInt("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", 200)
	maxClusters := envutil.GetInt("NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS", 1024)
	numClusters := clusterIndex.NumClusters()
	if numClusters <= 0 {
		return fmt.Errorf("no clusters")
	}
	if numClusters > maxClusters {
		numClusters = maxClusters
	}

	config := HNSWConfigFromEnv()
	rebuilt := make(map[int]*HNSWIndex, numClusters)
	log.Printf("[IVF-HNSW] 🔨 Building per-cluster HNSW: %d clusters (min size %d)", numClusters, minClusterSize)
	const ivfProgressInterval = 50 // log every N clusters built
	var builtCount int
	for cid := 0; cid < numClusters; cid++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		memberIDs := clusterIndex.GetClusterMemberIDsForCluster(cid)
		if len(memberIDs) < minClusterSize {
			continue
		}
		// Try loading persisted IVF-HNSW cluster first only when persistence is enabled
		// (same path convention as SaveIVFHNSW).
		var idx *HNSWIndex
		if s.persistEnabled.Load() && hnswPath != "" && vectorLookup != nil {
			loaded, _ := LoadIVFHNSWCluster(hnswPath, cid, vectorLookup)
			if loaded != nil && loaded.GetDimensions() == dims {
				idx = loaded
			}
		}
		if idx == nil {
			idx = NewHNSWIndex(dims, config)
			for _, id := range memberIDs {
				vec, ok := vectorLookup(id)
				if !ok || len(vec) == 0 {
					continue
				}
				_ = idx.Add(id, vec)
			}
			builtCount++
			if builtCount%ivfProgressInterval == 0 {
				log.Printf("[IVF-HNSW] 🔨 Progress: %d cluster HNSW indexes built (cluster id %d)", builtCount, cid+1)
			}
		}
		rebuilt[cid] = idx
	}
	log.Printf("[IVF-HNSW] 🔨 Built %d cluster HNSW indexes", len(rebuilt))

	s.clusterHNSWMu.Lock()
	s.clusterHNSW = rebuilt
	s.clusterHNSWMu.Unlock()

	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()
	return nil
}

// ClusteringInProgress returns true if k-means is currently running for this service.
// Used to debounce timer ticks and avoid starting a second run while one is in progress.
func (s *Service) ClusteringInProgress() bool {
	return s.kmeansInProgress.Load()
}

// IsClusteringEnabled returns true if GPU clustering is enabled.
func (s *Service) IsClusteringEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterEnabled && s.clusterIndex != nil
}

// SetMinEmbeddingsForClustering sets the minimum number of embeddings required
// before k-means clustering is triggered. Below this threshold, brute-force
// search is used as it's faster for small datasets.
//
// This should be called BEFORE TriggerClustering() to take effect.
//
// Parameters:
//   - threshold: Minimum embeddings (must be > 0, default: 1000)
//
// Tuning Guidelines:
//   - 1000 (default): Safe for most workloads
//   - 500-1000: Latency-sensitive applications with moderate data
//   - 100-500: Testing or small datasets
//   - 2000+: Very large datasets, delay clustering until more data arrives
//
// Example:
//
//	svc := search.NewService(engine)
//	svc.SetMinEmbeddingsForClustering(500) // Lower threshold for faster clustering
//	svc.EnableClustering(gpuManager, 100)
//	svc.BuildIndexes(ctx)
//	svc.TriggerClustering(ctx) // Will cluster if >= 500 embeddings
func (s *Service) SetMinEmbeddingsForClustering(threshold int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if threshold > 0 {
		s.minEmbeddingsForClustering = threshold
	}
}

// GetMinEmbeddingsForClustering returns the current minimum embeddings threshold.
func (s *Service) GetMinEmbeddingsForClustering() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.minEmbeddingsForClustering <= 0 {
		return DefaultMinEmbeddingsForClustering
	}
	return s.minEmbeddingsForClustering
}

// SetDefaultMinSimilarity sets the default minimum cosine similarity threshold for vector search.
// Apple Intelligence embeddings produce scores in 0.2-0.8 range, bge-m3/mxbai produce 0.7-0.99.
// Default: 0.0 (let RRF ranking handle relevance filtering)
func (s *Service) SetDefaultMinSimilarity(threshold float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultMinSimilarity = threshold
}

// GetDefaultMinSimilarity returns the configured minimum similarity threshold.
func (s *Service) GetDefaultMinSimilarity() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.defaultMinSimilarity
}

// SetFulltextIndexPath sets the path for persisting the BM25 fulltext index.
// When both fulltext and vector paths are set, BuildIndexes() will try to load both;
// if both load with count > 0, the full storage iteration is skipped.
func (s *Service) SetFulltextIndexPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fulltextIndexPath = path
}

// SetVectorIndexPath sets the path for persisting the vector index.
// When both fulltext and vector paths are set, BuildIndexes() will try to load both;
// if both load with count > 0, the full storage iteration is skipped.
func (s *Service) SetVectorIndexPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectorIndexPath = path
}

// SetHNSWIndexPath sets the path for persisting the HNSW index.
// When set with persist search indexes, the HNSW index is saved after build/warmup and
// loaded on startup so the full graph does not need to be rebuilt from vectors.
func (s *Service) SetHNSWIndexPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hnswIndexPath = path
}

// SetPersistenceEnabled controls whether index persistence is allowed.
// When false, all persist paths (debounced writes, build checkpoints, and shutdown flush)
// are treated as no-ops even if index paths are configured.
func (s *Service) SetPersistenceEnabled(enabled bool) {
	s.persistEnabled.Store(enabled)
	if enabled {
		return
	}
	s.persistMu.Lock()
	if s.persistTimer != nil {
		s.persistTimer.Stop()
		s.persistTimer = nil
	}
	s.persistMu.Unlock()
	s.persistRunMu.Lock()
	s.indexMu.Lock()
	if s.vectorFileStore != nil {
		_ = s.vectorFileStore.Close()
		s.vectorFileStore = nil
	}
	s.indexMu.Unlock()
	s.persistRunMu.Unlock()
}

// schedulePersist schedules a write of BM25 and vector indexes to disk after an idle delay.
// Called after IndexNode/RemoveNode when paths are set; resets the timer on each mutation
// so we only write after activity settles. No-ops during BuildIndexes (we save at end there).
// Delay is NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC (default 30).
func (s *Service) schedulePersist() {
	if !s.persistEnabled.Load() {
		return
	}
	if s.buildInProgress.Load() {
		return
	}
	s.mu.RLock()
	ftPath := s.fulltextIndexPath
	vPath := s.vectorIndexPath
	s.mu.RUnlock()
	if ftPath == "" || vPath == "" {
		return
	}

	delay := envDurationSec("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", 30)
	if delay <= 0 {
		delay = 30 * time.Second
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if s.persistTimer != nil {
		s.persistTimer.Stop()
		s.persistTimer = nil
	}
	s.persistTimer = time.AfterFunc(delay, func() {
		s.persistMu.Lock()
		s.persistTimer = nil
		s.persistMu.Unlock()
		s.runPersist()
	})
}

// runPersist writes the current in-memory BM25, vector, HNSW, and IVF-HNSW indexes to disk.
// Used both by the debounced background timer and on shutdown (via PersistIndexesToDisk).
// Persistence is strategy-based: vectors is always written; hnsw and/or hnsw_ivf/
// when the service uses global HNSW or IVF-HNSW. Does not hold s.mu across I/O.
//
// All index Save() implementations (BM25, vector, HNSW, SaveIVFHNSW per-cluster) copy index
// data under a short read lock and then perform file I/O without holding any lock, so Search
// and IndexNode are not blocked while writing to disk.
// persistBaseIndexes writes BM25 + vector store only (no HNSW/IVF-HNSW).
// Used after BuildIndexes iteration to make base indexes durable before HNSW/k-means.
func (s *Service) persistBaseIndexes() {
	if !s.persistEnabled.Load() {
		return
	}
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	if !s.persistEnabled.Load() {
		return
	}
	s.mu.RLock()
	ftPath := s.fulltextIndexPath
	vPath := s.vectorIndexPath
	hnswPath := s.hnswIndexPath
	vfs := s.vectorFileStore
	s.mu.RUnlock()
	if ftPath == "" && vPath == "" && hnswPath == "" {
		return
	}
	if ftPath != "" {
		if !s.fulltextIndex.IsDirty() {
			log.Printf("📇 Persist: BM25 skip (unchanged)")
		} else {
			log.Printf("📇 Persist: saving BM25 to %s...", ftPath)
			if err := s.fulltextIndex.SaveNoCopy(ftPath); err != nil {
				log.Printf("⚠️ Background persist: failed to save BM25 index to %s: %v", ftPath, err)
			} else {
				log.Printf("📇 Background persist: BM25 index saved to %s", ftPath)
			}
		}
	}
	if vPath != "" {
		if vfs != nil {
			if !s.buildInProgress.Load() {
				compacted, compErr := vfs.CompactIfNeeded()
				if compErr != nil {
					log.Printf("⚠️ Background persist: vector file store compaction failed for %s: %v", vPath, compErr)
				} else if compacted {
					log.Printf("📇 Background persist: compacted %s.vec to reclaim stale vector records", vPath)
				}
			}
			log.Printf("📇 Persist: syncing %s.vec and saving %s.meta...", vPath, vPath)
			_ = vfs.Sync()
			if err := vfs.Save(); err != nil {
				log.Printf("⚠️ Background persist: failed to save vector file store to %s: %v", vPath, err)
			} else {
				log.Printf("📇 Background persist: vector file store synced; meta saved to %s.meta", vPath)
			}
		} else {
			log.Printf("⚠️ Persist: vector file store unavailable; skipping vector persist")
		}
	}
	s.persistSearchBuildSettings(ftPath, vPath, hnswPath)
}

func (s *Service) runPersist() {
	if !s.persistEnabled.Load() {
		return
	}
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	if !s.persistEnabled.Load() {
		return
	}
	s.mu.RLock()
	ftPath := s.fulltextIndexPath
	vPath := s.vectorIndexPath
	hnswPath := s.hnswIndexPath
	vfs := s.vectorFileStore
	s.mu.RUnlock()
	if ftPath == "" && vPath == "" && hnswPath == "" {
		return
	}
	for _, writer := range s.persistWriters(ftPath, vPath, hnswPath, vfs) {
		writer.run()
	}
	s.persistSearchBuildSettings(ftPath, vPath, hnswPath)
}

type persistWriter struct {
	name string
	run  func()
}

func (s *Service) persistWriters(fulltextPath, vectorPath, hnswPath string, vfs *VectorFileStore) []persistWriter {
	return []persistWriter{
		{
			name: "bm25",
			run: func() {
				s.persistBM25Background(fulltextPath)
			},
		},
		{
			name: "vector_store",
			run: func() {
				s.persistVectorStoreBackground(vectorPath, vfs)
			},
		},
		{
			name: "hnsw",
			run: func() {
				s.persistHNSWBackground(hnswPath)
			},
		},
		{
			name: "ivf_hnsw",
			run: func() {
				s.persistIVFHNSWBackground(hnswPath)
			},
		},
		{
			name: "ivfpq",
			run: func() {
				s.persistIVFPQBackground(vectorPath, hnswPath)
			},
		},
	}
}

func (s *Service) persistBM25Background(fulltextPath string) {
	// BM25: skip full rewrite during BuildIndexes checkpoints (avoids rewriting a multi-GB file every 50k nodes).
	// Vector store is append-only (.vec); we only rewrite small .meta at checkpoint. BM25 is written once at end of build.
	if fulltextPath == "" || s.buildInProgress.Load() {
		return
	}
	if !s.fulltextIndex.IsDirty() {
		log.Printf("📇 Background persist: BM25 skip (unchanged)")
		return
	}
	log.Printf("📇 Persist: saving BM25 to %s...", fulltextPath)
	if err := s.fulltextIndex.Save(fulltextPath); err != nil {
		log.Printf("⚠️ Background persist: failed to save BM25 index to %s: %v", fulltextPath, err)
		return
	}
	log.Printf("📇 Background persist: BM25 index saved to %s", fulltextPath)
}

func (s *Service) persistVectorStoreBackground(vectorPath string, vfs *VectorFileStore) {
	if vectorPath == "" {
		return
	}
	if vfs == nil {
		log.Printf("⚠️ Persist: vector file store unavailable; skipping vector persist")
		return
	}
	log.Printf("📇 Persist: syncing %s.vec and saving %s.meta...", vectorPath, vectorPath)
	_ = vfs.Sync()
	if err := vfs.Save(); err != nil {
		log.Printf("⚠️ Background persist: failed to save vector file store to %s: %v", vectorPath, err)
		return
	}
	log.Printf("📇 Background persist: vector file store synced; meta saved to %s.meta", vectorPath)
}

func (s *Service) persistHNSWBackground(hnswPath string) {
	// Only persist HNSW when we use HNSW strategy (N >= NSmallMax). Do not build on shutdown to avoid long hangs.
	if hnswPath == "" {
		return
	}
	vecCount := s.EmbeddingCount()
	if vecCount < NSmallMax {
		log.Printf("📇 Background persist: HNSW skip (vector count %d < %d)", vecCount, NSmallMax)
		return
	}
	s.hnswMu.RLock()
	idx := s.hnswIndex
	s.hnswMu.RUnlock()
	if idx == nil {
		log.Printf("📇 Background persist: HNSW skip (index not built yet; will be built during warmup or when k-means completes)")
		return
	}
	if err := idx.Save(hnswPath); err != nil {
		log.Printf("⚠️ Background persist: failed to save HNSW index to %s: %v", hnswPath, err)
		return
	}
	log.Printf("📇 Background persist: HNSW index saved to %s", hnswPath)
}

func (s *Service) persistIVFHNSWBackground(hnswPath string) {
	// When IVF-HNSW is the strategy, persist per-cluster HNSW and centroids (same dir as hnsw, in hnsw_ivf/).
	if hnswPath == "" {
		return
	}
	s.clusterHNSWMu.RLock()
	clusterHNSW := s.clusterHNSW
	s.clusterHNSWMu.RUnlock()
	if len(clusterHNSW) == 0 {
		return
	}
	if err := SaveIVFHNSW(hnswPath, clusterHNSW); err != nil {
		log.Printf("⚠️ Background persist: failed to save IVF-HNSW clusters to %s: %v", hnswPath, err)
		return
	}
	log.Printf("📇 Background persist: IVF-HNSW clusters saved to %s (%d clusters)", hnswPath, len(clusterHNSW))
}

// PersistIndexesToDisk writes the current BM25, vector, HNSW, and IVF-HNSW (per-cluster) indexes to disk immediately.
// Call this on shutdown so the latest in-memory state is saved before exit, same as every other persisted index.
// Cancels any pending debounced persist so no write runs after shutdown.
func (s *Service) PersistIndexesToDisk() {
	if !s.persistEnabled.Load() {
		return
	}
	s.mu.RLock()
	ftPath := s.fulltextIndexPath
	vPath := s.vectorIndexPath
	hnswPath := s.hnswIndexPath
	s.mu.RUnlock()
	if ftPath == "" && vPath == "" && hnswPath == "" {
		return
	}

	log.Printf("📇 Persisting search indexes (BM25, vector, HNSW/IVF-HNSW)...")
	s.persistMu.Lock()
	if s.persistTimer != nil {
		s.persistTimer.Stop()
		s.persistTimer = nil
	}
	s.persistMu.Unlock()
	s.runPersist()
}

// resolveMinSimilarity returns the MinSimilarity to use for a search.
// Priority: explicit opts value > service default > hardcoded fallback (0.5)
func (s *Service) resolveMinSimilarity(opts *SearchOptions) *float64 {
	fallback := 0.5 // hardcoded fallback
	if opts != nil && opts.MinSimilarity != nil {
		return opts.MinSimilarity
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.defaultMinSimilarity >= 0 {
		return &s.defaultMinSimilarity
	}
	return &fallback
}

// ClusterStats returns k-means clustering statistics.
func (s *Service) ClusterStats() *gpu.ClusterStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.clusterIndex == nil {
		return nil
	}
	stats := s.clusterIndex.ClusterStats()
	return &stats
}

// EmbeddingCount returns the total number of nodes with embeddings in the vector index.
func (s *Service) EmbeddingCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.embeddingCountLocked()
}

// embeddingCountLocked returns vector count; caller must hold s.mu (at least RLock).
func (s *Service) embeddingCountLocked() int {
	if s.vectorFileStore != nil {
		if n := s.vectorFileStore.Count(); n > 0 {
			return n
		}
	}
	if s.vectorIndex != nil {
		return s.vectorIndex.Count()
	}
	return 0
}

// VectorIndexDimensions returns the configured dimensions of the vector index.
func (s *Service) VectorIndexDimensions() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.vectorFileStore != nil && s.vectorFileStore.Count() > 0 {
		return s.vectorFileStore.GetDimensions()
	}
	if s.vectorIndex != nil {
		return s.vectorIndex.GetDimensions()
	}
	return 0
}

// getVectorLookup returns a VectorLookup that resolves ids from vectorFileStore (if active) or vectorIndex.
func (s *Service) getVectorLookup() VectorLookup {
	return func(id string) ([]float32, bool) {
		s.mu.RLock()
		vfs := s.vectorFileStore
		vi := s.vectorIndex
		s.mu.RUnlock()
		if vfs != nil {
			if vec, ok := vfs.GetVector(id); ok {
				return vec, true
			}
		}
		if vi != nil {
			return vi.GetVector(id)
		}
		return nil, false
	}
}

// vectorLookupGetter adapts VectorLookup to VectorGetter for CPUExactScorer.
type vectorLookupGetter struct {
	lookup VectorLookup
}

func (v *vectorLookupGetter) GetVector(id string) ([]float32, bool) {
	return v.lookup(id)
}

// getVectorForCypher returns the vector for vecID for use in Cypher vector similarity.
// When using file store only normalized vectors are available (cosine/dot correct; euclidean uses normalized).
func (s *Service) getVectorForCypher(vecID string) ([]float32, bool) {
	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	s.mu.RUnlock()
	if vfs != nil {
		return vfs.GetVector(vecID)
	}
	if vi != nil {
		vi.mu.RLock()
		v, ok := vi.rawVectors[vecID]
		vi.mu.RUnlock()
		if ok {
			out := make([]float32, len(v))
			copy(out, v)
			return out, true
		}
	}
	return nil, false
}

func firstVectorDimensions(node *storage.Node) int {
	if node == nil {
		return 0
	}
	if node.NamedEmbeddings != nil {
		for _, emb := range node.NamedEmbeddings {
			if len(emb) > 0 {
				return len(emb)
			}
		}
	}
	if len(node.ChunkEmbeddings) > 0 {
		for _, emb := range node.ChunkEmbeddings {
			if len(emb) > 0 {
				return len(emb)
			}
		}
	}
	if node.Properties != nil {
		for _, value := range node.Properties {
			vec := toFloat32SliceAny(value)
			if len(vec) > 0 {
				return len(vec)
			}
		}
	}
	return 0
}

func (s *Service) maybeAutoSetVectorDimensions(dimensions int) {
	if s == nil || dimensions <= 0 {
		return
	}

	s.mu.RLock()
	currentDims := 0
	currentCount := 0
	if s.vectorIndex != nil {
		currentDims = s.vectorIndex.GetDimensions()
		currentCount = s.vectorIndex.Count()
	}
	s.mu.RUnlock()

	// Only auto-adjust when the index is still empty (first embedding wins).
	if currentCount > 0 || currentDims == dimensions {
		return
	}

	// Lock order: pipelineMu -> mu -> hnswMu, matching pipeline construction paths.
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()

	s.mu.Lock()
	if s.vectorIndex == nil {
		s.vectorIndex = NewVectorIndex(dimensions)
	} else if s.vectorIndex.Count() == 0 && s.vectorIndex.GetDimensions() != dimensions {
		s.vectorIndex = NewVectorIndex(dimensions)
	}

	// Dimension changes invalidate all vector backends.
	if s.gpuEmbeddingIndex != nil {
		s.gpuEmbeddingIndex.Clear()
		s.gpuEmbeddingIndex = nil
	}
	if s.clusterIndex != nil {
		s.clusterIndex.Clear()
	}
	s.mu.Unlock()

	s.clusterHNSWMu.Lock()
	s.clusterHNSW = nil
	s.clusterHNSWMu.Unlock()
	s.ivfpqMu.Lock()
	s.ivfpqIndex = nil
	s.ivfpqMu.Unlock()
	s.clearClusterLexicalProfiles()

	s.hnswMu.Lock()
	if s.hnswIndex != nil {
		s.hnswIndex.Clear()
		s.hnswIndex = nil
	}
	s.hnswMu.Unlock()
}

// ClearVectorIndex removes all embeddings from the vector index.
// This is used when regenerating all embeddings to reset the index count.
// Also frees memory from HNSW tombstones which can accumulate over time.
func (s *Service) ClearVectorIndex() {
	// Lock order: pipelineMu -> mu -> hnswMu, matching pipeline construction paths.
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()

	s.mu.Lock()
	if s.vectorFileStore != nil {
		_ = s.vectorFileStore.Close()
		s.vectorFileStore = nil
	}
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	if s.gpuEmbeddingIndex != nil {
		s.gpuEmbeddingIndex.Clear()
	}
	if s.clusterIndex != nil {
		s.clusterIndex.Clear()
	}
	clear(s.nodeLabels)
	clear(s.nodeNamedVector)
	clear(s.nodePropVector)
	clear(s.nodeChunkVectors)
	s.mu.Unlock()

	s.clusterHNSWMu.Lock()
	s.clusterHNSW = nil
	s.clusterHNSWMu.Unlock()
	s.ivfpqMu.Lock()
	s.ivfpqIndex = nil
	s.ivfpqMu.Unlock()
	s.clearClusterLexicalProfiles()

	// Also clear HNSW index if it exists (frees memory from tombstones).
	// HNSW uses tombstones for deletes which never free memory, so explicit Clear() is required.
	s.hnswMu.Lock()
	if s.hnswIndex != nil {
		s.hnswIndex.Clear()
		s.hnswIndex = nil // Reset to nil so it can be recreated lazily
	}
	s.hnswMu.Unlock()
}

// IndexNode adds a node to all search indexes.
// All embeddings are stored in ChunkEmbeddings (even single chunk = array of 1).
func (s *Service) IndexNode(node *storage.Node) error {
	if !s.buildInProgress.Load() {
		defer s.schedulePersist()
		if s.resultCache != nil {
			s.resultCache.Invalidate()
		}
	}
	s.maybeAutoSetVectorDimensions(firstVectorDimensions(node))

	s.indexMu.Lock()
	err := s.indexNodeLocked(node, false)
	s.indexMu.Unlock()
	if err == nil && !s.buildInProgress.Load() {
		s.scheduleStrategyTransitionCheck()
	}
	return err
}

// addVectorLocked adds a vector to the active store (vectorFileStore if set, else vectorIndex).
// Caller holds s.indexMu.
func (s *Service) addVectorLocked(id string, vec []float32) error {
	var err error
	if s.vectorFileStore != nil {
		if s.resumeVectorBuild && s.vectorFileStore.Has(id) {
			return nil
		}
		err = s.vectorFileStore.Add(id, vec)
	} else if s.vectorIndex != nil {
		err = s.vectorIndex.Add(id, vec)
	}
	if err == nil {
		s.appendStrategyDelta(id, true)
	}
	return err
}

// allowLiveHNSWUpdatesLocked reports whether per-node live HNSW mutations should run.
// Caller must hold s.indexMu.
func (s *Service) allowLiveHNSWUpdatesLocked() bool {
	s.hnswMu.RLock()
	hasHNSW := s.hnswIndex != nil
	s.hnswMu.RUnlock()
	if !hasHNSW {
		return false
	}
	// Keep search latency prioritized for large indexes by deferring live mutations.
	// Set < 0 to force-enable live updates for all sizes.
	maxN := envutil.GetInt("NORNICDB_HNSW_LIVE_UPDATE_MAX_N", 50000)
	if maxN < 0 {
		return true
	}
	return s.embeddingCountLocked() <= maxN
}

// hnswUpdateLive applies a best-effort live HNSW vector update.
func (s *Service) hnswUpdateLive(id string, embedding []float32, allowLive bool) {
	if !allowLive {
		s.hnswMu.RLock()
		hasHNSW := s.hnswIndex != nil
		s.hnswMu.RUnlock()
		if hasHNSW {
			s.hnswDeferredMutations.Add(1)
		}
		return
	}
	s.hnswMu.RLock()
	idx := s.hnswIndex
	s.hnswMu.RUnlock()
	if idx != nil {
		_ = idx.Update(id, embedding) // Best effort
	}
}

// hnswRemoveLive applies a best-effort live HNSW vector remove.
func (s *Service) hnswRemoveLive(id string, allowLive bool) {
	if !allowLive {
		s.hnswMu.RLock()
		hasHNSW := s.hnswIndex != nil
		s.hnswMu.RUnlock()
		if hasHNSW {
			s.hnswDeferredMutations.Add(1)
		}
		return
	}
	s.hnswMu.RLock()
	idx := s.hnswIndex
	s.hnswMu.RUnlock()
	if idx != nil {
		idx.Remove(id)
	}
}

// removeVectorLocked removes a vector ID from the active stores.
// Caller holds s.indexMu.
func (s *Service) removeVectorLocked(id string) {
	if s.vectorIndex != nil {
		s.vectorIndex.Remove(id)
	}
	if s.vectorFileStore != nil {
		s.vectorFileStore.Remove(id)
	}
	s.appendStrategyDelta(id, false)
}

// ensureBuildVectorFileStore creates vectorFileStore when building with vectorIndexPath so vectors go to disk.
// Caller holds s.indexMu.
// When not resuming, BuildIndexes has already removed .vec/.meta so we start from 0; when resuming, existing files are appended to.
func (s *Service) ensureBuildVectorFileStore() {
	if !s.persistEnabled.Load() {
		return
	}
	if s.vectorIndexPath == "" {
		return
	}
	if s.vectorIndex == nil {
		return
	}
	dims := s.vectorIndex.GetDimensions()
	if dims <= 0 {
		return
	}
	if s.vectorFileStore != nil {
		return
	}
	vfs, err := NewVectorFileStore(s.vectorIndexPath, dims)
	if err != nil {
		log.Printf("⚠️ VectorFileStore create failed (using in-memory index): %v", err)
		return
	}
	_ = vfs.Load()
	s.vectorFileStore = vfs
	// Clear in-memory vectors so we don't hold 2x during indexing; metadata (nodeLabels etc.) stays in RAM.
	s.vectorIndex.vectors = make(map[string][]float32)
	s.vectorIndex.rawVectors = make(map[string][]float32)
}

// lastWriteTime returns the last known write time for the underlying storage, if available.
func (s *Service) lastWriteTime() time.Time {
	s.mu.RLock()
	engine := s.engine
	s.mu.RUnlock()
	if engine == nil {
		return time.Time{}
	}
	if p, ok := engine.(interface{ LastWriteTime() time.Time }); ok {
		return p.LastWriteTime()
	}
	return time.Time{}
}

func (s *Service) shouldIndexNode(node *storage.Node) (bool, error) {
	if node == nil {
		return false, nil
	}
	provider, ok := s.engine.(interface {
		IsCurrentTemporalNode(node *storage.Node, asOf time.Time) (bool, error)
	})
	if !ok {
		return true, nil
	}
	return provider.IsCurrentTemporalNode(node, time.Now().UTC())
}

// indexNodeLocked does the work of IndexNode. Caller must hold s.indexMu.
// When skipFulltext is true, the fulltext index block is skipped (used when BuildIndexes batches fulltext).
// When skipFulltext is true, removeNodeLocked is also skipped so we don't remove the doc just added by IndexBatch.
func (s *Service) indexNodeLocked(node *storage.Node, skipFulltext bool) error {
	nodeIDStr := string(node.ID)
	allowLiveHNSW := s.allowLiveHNSWUpdatesLocked()
	shouldIndex, err := s.shouldIndexNode(node)
	if err != nil {
		return err
	}
	if nodeIDStr != "" && !skipFulltext {
		// CRITICAL: IndexNode is called for both creates and updates.
		// When a node is re-indexed with fewer chunks or fewer named vectors,
		// we must remove the old vector IDs first, otherwise they become orphaned
		// in the in-memory index and EmbeddingCount() will drift upward over time.
		s.removeNodeLocked(nodeIDStr)
	}
	if !shouldIndex {
		return nil
	}
	if nodeIDStr != "" {
		labelsCopy := make([]string, len(node.Labels))
		copy(labelsCopy, node.Labels)
		s.nodeLabels[nodeIDStr] = labelsCopy
	}

	// When building from storage with a vector path, use file-backed store to bound RAM.
	if skipFulltext && s.persistEnabled.Load() && s.vectorIndexPath != "" {
		s.ensureBuildVectorFileStore()
	}

	// Index all embeddings: NamedEmbeddings and ChunkEmbeddings
	// Strategy:
	//   - NamedEmbeddings: Index each named vector at "node-id-named-{vectorName}"
	//   - ChunkEmbeddings: Index main at node.ID, chunks at "node-id-chunk-N"
	// This allows efficient indexed search for all embedding types
	// Embeddings are stored in struct fields (opaque to users), not in properties

	// Index NamedEmbeddings (each named vector gets its own index entry)
	if len(node.NamedEmbeddings) > 0 {
		for vectorName, embedding := range node.NamedEmbeddings {
			if len(embedding) == 0 {
				continue
			}

			// Index each named embedding with ID: "node-id-named-{vectorName}"
			namedID := fmt.Sprintf("%s-named-%s", node.ID, vectorName)
			expectedDim := s.vectorIndex.GetDimensions()
			if s.vectorFileStore != nil {
				expectedDim = s.vectorFileStore.GetDimensions()
			}
			if err := s.addVectorLocked(namedID, embedding); err != nil {
				if err == ErrDimensionMismatch {
					log.Printf("⚠️ IndexNode %s named[%s]: embedding dimension mismatch (got %d, expected %d)",
						node.ID, vectorName, len(embedding), expectedDim)
				}
				continue
			}

			if s.nodeNamedVector[nodeIDStr] == nil {
				s.nodeNamedVector[nodeIDStr] = make(map[string]string, len(node.NamedEmbeddings))
			}
			s.nodeNamedVector[nodeIDStr][vectorName] = namedID

			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Add(namedID, embedding) // Best effort
			}

			// Keep search latency priority under heavy write/embed load.
			s.hnswUpdateLive(namedID, embedding, allowLiveHNSW)

			// Also add to cluster index if enabled
			if s.clusterIndex != nil {
				_ = s.clusterIndex.Add(namedID, embedding) // Best effort
			}
		}
	}

	// Index ChunkEmbeddings (chunked documents)
	// Strategy:
	//   - Index a "main" embedding at node.ID (currently uses chunk 0 as a representative)
	//   - For multi-chunk nodes, index every chunk separately at "node-id-chunk-N"
	//
	// Vector search uses ALL chunk vectors because we overfetch and then collapse
	// chunk IDs back to a unique node ID. The "main" embedding is an additional
	// node-level entry used by some call paths and for compatibility.
	// Chunk embeddings are stored in struct field (opaque to users), not in properties
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		chunkIDs := make([]string, 0, len(node.ChunkEmbeddings)+1)
		// Always index a main embedding at the node ID (using first chunk)
		mainEmbedding := node.ChunkEmbeddings[0]
		expectedDim := s.vectorIndex.GetDimensions()
		if s.vectorFileStore != nil {
			expectedDim = s.vectorFileStore.GetDimensions()
		}
		if err := s.addVectorLocked(string(node.ID), mainEmbedding); err != nil {
			if err == ErrDimensionMismatch {
				log.Printf("⚠️ IndexNode %s main: embedding dimension mismatch (got %d, expected %d)",
					node.ID, len(mainEmbedding), expectedDim)
			}
		} else {
			chunkIDs = append(chunkIDs, nodeIDStr) // main ID
			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Add(string(node.ID), mainEmbedding) // Best effort
			}

			// Keep search latency priority under heavy write/embed load.
			// Use Update() to handle both new and existing vectors correctly.
			s.hnswUpdateLive(string(node.ID), mainEmbedding, allowLiveHNSW)

			// Also add to cluster index if enabled
			if s.clusterIndex != nil {
				_ = s.clusterIndex.Add(string(node.ID), mainEmbedding) // Best effort
			}
		}

		// For multi-chunk nodes, also index each chunk separately with chunk suffix
		// This allows granular search at the chunk level while maintaining node-level search
		// Note: chunk 0 is indexed both as main (node.ID) and as chunk-0 for consistency
		// ALL chunks are indexed in vectorIndex, HNSW, and clusterIndex for complete search coverage
		if len(node.ChunkEmbeddings) > 1 {
			for i, embedding := range node.ChunkEmbeddings {
				if len(embedding) > 0 {
					chunkID := fmt.Sprintf("%s-chunk-%d", node.ID, i)
					if err := s.addVectorLocked(chunkID, embedding); err != nil {
						if err == ErrDimensionMismatch {
							log.Printf("⚠️ IndexNode %s chunk %d: embedding dimension mismatch (got %d, expected %d)",
								node.ID, i, len(embedding), expectedDim)
						}
						// Continue indexing other chunks even if one fails
						continue
					}
					chunkIDs = append(chunkIDs, chunkID)

					if s.gpuEmbeddingIndex != nil {
						_ = s.gpuEmbeddingIndex.Add(chunkID, embedding) // Best effort
					}

					// Also add to cluster index if enabled
					if s.clusterIndex != nil {
						_ = s.clusterIndex.Add(chunkID, embedding) // Best effort
					}
				}
			}
		}
		if len(chunkIDs) > 0 {
			s.nodeChunkVectors[nodeIDStr] = chunkIDs
		}
	}

	// Index vector-shaped property values for Cypher compatibility.
	// These are indexed under IDs: "node-id-prop-{propertyKey}".
	if node.Properties != nil {
		dim := s.vectorIndex.GetDimensions()
		if s.vectorFileStore != nil {
			dim = s.vectorFileStore.GetDimensions()
		}
		for key, value := range node.Properties {
			vec, ok := vectorFromPropertyValue(value, dim)
			if !ok {
				continue
			}
			propID := fmt.Sprintf("%s-prop-%s", nodeIDStr, key)
			if err := s.addVectorLocked(propID, vec); err != nil {
				continue
			}
			if s.nodePropVector[nodeIDStr] == nil {
				s.nodePropVector[nodeIDStr] = make(map[string]string, 4)
			}
			s.nodePropVector[nodeIDStr][key] = propID
			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Add(propID, vec) // Best effort
			}
			s.hnswUpdateLive(propID, vec, allowLiveHNSW)
			if s.clusterIndex != nil {
				_ = s.clusterIndex.Add(propID, vec) // Best effort
			}
		}
	}

	// Add to fulltext index (skipped when BuildIndexes batches fulltext via IndexBatch)
	if !skipFulltext {
		text := s.extractSearchableText(node)
		if text != "" {
			s.fulltextIndex.Index(string(node.ID), text)
		}
	}

	return nil
}

// removeNodeLocked removes a node from all search indexes.
// Caller MUST hold s.indexMu.
//
// This is used by both RemoveNode (delete path) and IndexNode (update path) to ensure
// vector IDs never become orphaned when embeddings change shape over time.
func (s *Service) removeNodeLocked(nodeIDStr string) {
	if nodeIDStr == "" {
		return
	}
	allowLiveHNSW := s.allowLiveHNSWUpdatesLocked()

	// Remove main embedding
	s.removeVectorLocked(nodeIDStr)
	if s.gpuEmbeddingIndex != nil {
		_ = s.gpuEmbeddingIndex.Remove(nodeIDStr)
	}
	if s.fulltextIndex != nil {
		s.fulltextIndex.Remove(nodeIDStr)
	}

	// Keep search latency priority under heavy write/embed load.
	s.hnswRemoveLive(nodeIDStr, allowLiveHNSW)

	// Also remove from cluster index if enabled
	if s.clusterIndex != nil {
		s.clusterIndex.Remove(nodeIDStr)
	}

	delete(s.nodeLabels, nodeIDStr)

	// Remove property vectors tracked for Cypher compatibility.
	if props := s.nodePropVector[nodeIDStr]; len(props) > 0 {
		for _, propID := range props {
			s.removeVectorLocked(propID)
			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Remove(propID)
			}
			s.hnswRemoveLive(propID, allowLiveHNSW)
			if s.clusterIndex != nil {
				s.clusterIndex.Remove(propID)
			}
		}
	}
	delete(s.nodePropVector, nodeIDStr)

	// Remove all named embeddings (they're indexed as "node-id-named-{vectorName}")
	if named := s.nodeNamedVector[nodeIDStr]; len(named) > 0 {
		for _, namedID := range named {
			s.removeVectorLocked(namedID)
			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Remove(namedID)
			}

			s.hnswRemoveLive(namedID, allowLiveHNSW)

			if s.clusterIndex != nil {
				s.clusterIndex.Remove(namedID)
			}
		}
	}
	delete(s.nodeNamedVector, nodeIDStr)

	// Remove all chunk embeddings (they're indexed as "node-id-chunk-0", "node-id-chunk-1", etc.)
	if chunkIDs := s.nodeChunkVectors[nodeIDStr]; len(chunkIDs) > 0 {
		for _, chunkID := range chunkIDs {
			s.removeVectorLocked(chunkID)
			if s.gpuEmbeddingIndex != nil {
				_ = s.gpuEmbeddingIndex.Remove(chunkID)
			}

			// Keep search latency priority under heavy write/embed load.
			s.hnswRemoveLive(chunkID, allowLiveHNSW)

			if s.clusterIndex != nil {
				s.clusterIndex.Remove(chunkID)
			}
		}
	}
	delete(s.nodeChunkVectors, nodeIDStr)
}

// RemoveNode removes a node from all search indexes.
// Also removes all chunk embeddings (for nodes with multiple chunks).
func (s *Service) RemoveNode(nodeID storage.NodeID) error {
	if !s.buildInProgress.Load() {
		defer s.schedulePersist()
		if s.resultCache != nil {
			s.resultCache.Invalidate()
		}
	}
	s.indexMu.Lock()
	s.removeNodeLocked(string(nodeID))
	s.indexMu.Unlock()
	if !s.buildInProgress.Load() {
		s.scheduleStrategyTransitionCheck()
	}

	return nil
}

// handleOrphanedEmbedding handles the case where a vector/index hit refers to a node
// that no longer exists in storage (orphaned embedding). If err is storage.ErrNotFound,
// it logs once per node ID (when seenOrphans is provided), removes all embeddings for
// that node from indexes via RemoveNode, and returns true so the caller can skip the result.
// If seenOrphans is non-nil and the node ID was already seen this request, it returns true
// without logging or removing again. If err is not ErrNotFound, returns false.
func (s *Service) handleOrphanedEmbedding(ctx context.Context, nodeIDStr string, err error, seenOrphans map[string]bool) bool {
	if !errors.Is(err, storage.ErrNotFound) {
		return false
	}
	if seenOrphans != nil && seenOrphans[nodeIDStr] {
		return true // already logged and removed this request
	}
	log.Printf("[search] orphaned embedding detected, removing from indexes: nodeID=%s", nodeIDStr)
	if removeErr := s.RemoveNode(storage.NodeID(nodeIDStr)); removeErr != nil {
		log.Printf("[search] failed to remove orphaned embedding for nodeID=%s: %v", nodeIDStr, removeErr)
	}
	if seenOrphans != nil {
		seenOrphans[nodeIDStr] = true
	}
	return true
}

// NodeIterator is an interface for streaming node iteration.
type NodeIterator interface {
	IterateNodes(fn func(*storage.Node) bool) error
}

// BuildIndexes builds search indexes from all nodes in the engine.
// Call this after storage (and WAL recovery) so indexes reflect durable state.
// When both fulltext and vector index paths are set, tries to load both from disk;
// if both load with count > 0 (and semver format version matches), the full iteration
// is skipped. Otherwise iterates over storage and saves both indexes at the end when paths are set.
func (s *Service) BuildIndexes(ctx context.Context) error {
	s.buildAttempted.Store(true)
	s.ready.Store(false)
	s.buildInProgress.Store(true)
	s.buildStartedUnix.Store(time.Now().Unix())
	s.buildProcessed.Store(0)
	s.buildTotalNodes.Store(0)
	s.setBuildPhase("loading_existing_indexes")
	defer func() {
		s.buildInProgress.Store(false)
		if !s.ready.Load() {
			s.setBuildPhase("idle")
		}
	}()
	if sec := envutil.GetInt("NORNICDB_SEARCH_BUILD_PROGRESS_LOG_SEC", 15); sec > 0 {
		interval := time.Duration(sec) * time.Second
		ticker := time.NewTicker(interval)
		stop := make(chan struct{})
		bm25Engine := s.BM25Engine()
		go func() {
			for {
				select {
				case <-ticker.C:
					phase := "unknown"
					if p, ok := s.buildPhase.Load().(string); ok && p != "" {
						phase = p
					}
					processed := s.buildProcessed.Load()
					total := s.buildTotalNodes.Load()
					log.Printf("📇 BuildIndexes progress: phase=%s processed=%d/%d bm25_engine=%s", phase, processed, total, bm25Engine)
				case <-stop:
					return
				}
			}
		}()
		defer close(stop)
		defer ticker.Stop()
	}

	// Clear cache once for the whole build so IndexNode/RemoveNode don't clear per node.
	if s.resultCache != nil {
		s.resultCache.Invalidate()
	}

	s.mu.RLock()
	fulltextPath := s.fulltextIndexPath
	vectorPath := s.vectorIndexPath
	hnswPath := s.hnswIndexPath
	s.mu.RUnlock()
	if !s.persistEnabled.Load() {
		fulltextPath = ""
		vectorPath = ""
		hnswPath = ""
	}
	var storageNodeCount int64
	if n, err := s.engine.NodeCount(); err == nil {
		storageNodeCount = n
		if n > 0 {
			s.buildTotalNodes.Store(n)
		}
	}

	skipIteration := false
	restartVectorStore := false
	forceFulltextRebuild := false
	forceVectorRebuild := false
	forceHNSWRebuild := false
	forceRoutingRebuild := false
	forceStrategyRebuild := false
	hnswLoadedFromDisk := false
	hnswWarmupReason := ""

	settingsPath := searchBuildSettingsPath(fulltextPath, vectorPath, hnswPath)
	currentSettings := s.currentSearchBuildSettings()
	if savedSettings, err := loadSearchBuildSettings(settingsPath); err != nil {
		log.Printf("⚠️ BuildIndexes: failed to load build settings metadata (%s): %v", settingsPath, err)
	} else if savedSettings != nil {
		forceFulltextRebuild = savedSettings.BM25 != currentSettings.BM25
		if forceFulltextRebuild && bm25SettingsEquivalent(savedSettings.BM25, currentSettings.BM25, s.currentBM25FormatVersion()) {
			forceFulltextRebuild = false
		}
		forceVectorRebuild = savedSettings.Vector != currentSettings.Vector
		forceHNSWRebuild = savedSettings.HNSW != currentSettings.HNSW
		forceRoutingRebuild = savedSettings.Routing != currentSettings.Routing
		forceStrategyRebuild = savedSettings.Strategy != currentSettings.Strategy
		if forceFulltextRebuild {
			log.Printf("📇 BuildIndexes: BM25 settings changed; forcing BM25 rebuild")
		}
		if forceVectorRebuild {
			log.Printf("📇 BuildIndexes: vector settings changed; forcing vector rebuild")
		}
		if forceHNSWRebuild {
			log.Printf("📇 BuildIndexes: HNSW settings changed; forcing HNSW rebuild")
		}
		if forceRoutingRebuild {
			log.Printf("📇 BuildIndexes: routing/k-means settings changed; forcing routing artifact rebuild")
		}
		if forceStrategyRebuild {
			log.Printf("📇 BuildIndexes: strategy settings changed; forcing compressed ANN artifact rebuild")
		}
	}

	if fulltextPath != "" {
		_ = s.fulltextIndex.Load(fulltextPath)
		if s.fulltextIndex.Count() == 0 {
			if info, statErr := os.Stat(fulltextPath); statErr == nil {
				log.Printf("📇 BuildIndexes: BM25 file present but loaded 0 docs (%s, %d bytes); rebuilding from storage",
					fulltextPath, info.Size())
			}
		}
	}
	// Prefer file-backed vector store when .vec exists (low-RAM format); else legacy in-memory index.
	if vectorPath != "" && s.vectorIndex != nil {
		if _, err := os.Stat(vectorPath + ".vec"); err == nil {
			dims := s.vectorIndex.GetDimensions()
			if dims <= 0 {
				dims = 384
			}
			if vfs, err := NewVectorFileStore(vectorPath, dims); err == nil {
				if loadErr := vfs.Load(); loadErr != nil {
					log.Printf("⚠️ VectorFileStore load failed; rebuilding from 0: %v", loadErr)
					_ = vfs.Close()
				} else {
					s.mu.Lock()
					s.vectorFileStore = vfs
					s.mu.Unlock()
				}
			}
		}
	}

	if forceFulltextRebuild && s.fulltextIndex.Count() > 0 {
		s.fulltextIndex.Clear()
	}
	if forceVectorRebuild {
		restartVectorStore = true
		s.mu.Lock()
		if s.vectorFileStore != nil {
			_ = s.vectorFileStore.Close()
			s.vectorFileStore = nil
		}
		s.mu.Unlock()
		if s.vectorIndex != nil {
			s.vectorIndex.Clear()
		}
	}
	if forceRoutingRebuild {
		forceHNSWRebuild = true
		s.clearClusterLexicalProfiles()
	}
	if forceStrategyRebuild {
		s.ivfpqMu.Lock()
		s.ivfpqIndex = nil
		s.ivfpqMu.Unlock()
	}

	// When both paths are set and both indexes have content, skip the full iteration.
	vectorCount := s.EmbeddingCount()
	shouldClearStaleDisk := false
	if storageNodeCount == 0 && (s.fulltextIndex.Count() > 0 || vectorCount > 0) {
		// Only clear disk-backed indexes when we can prove storage changed after
		// those artifacts were written (e.g., DB dropped/recreated with same name).
		// This avoids breaking valid "disk-only bootstrap" test/restore scenarios.
		if p, ok := s.engine.(interface{ LastWriteTime() time.Time }); ok {
			if lastWrite := p.LastWriteTime(); !lastWrite.IsZero() {
				if info, statErr := os.Stat(vectorPath + ".vec"); statErr == nil {
					shouldClearStaleDisk = lastWrite.After(info.ModTime())
				}
			}
		}
	}
	if shouldClearStaleDisk {
		log.Printf("📇 BuildIndexes: storage is empty and newer than disk indexes; clearing stale search artifacts")
		s.fulltextIndex.Clear()
		vectorCount = 0
		restartVectorStore = true
		forceHNSWRebuild = true
		forceRoutingRebuild = true
		forceStrategyRebuild = true
		s.mu.Lock()
		if s.vectorFileStore != nil {
			_ = s.vectorFileStore.Close()
			s.vectorFileStore = nil
		}
		s.mu.Unlock()
		if s.vectorIndex != nil {
			s.vectorIndex.Clear()
		}
		s.hnswMu.Lock()
		if s.hnswIndex != nil {
			s.hnswIndex.Clear()
			s.hnswIndex = nil
		}
		s.hnswMu.Unlock()
	}
	if fulltextPath != "" && vectorPath != "" && s.fulltextIndex.Count() > 0 && vectorCount > 0 {
		skipIteration = true
		log.Printf("📇 Search indexes loaded from disk (BM25: %d docs, vector: %d); skipping node-iteration rebuild",
			s.fulltextIndex.Count(), vectorCount)
	}
	// When only BM25 loaded with content but vector is empty, we still iterate to build vectors
	// but skip re-indexing fulltext so we don't throw away the on-disk BM25.
	skipFulltextRebuild := fulltextPath != "" && s.fulltextIndex.Count() > 0 && vectorCount == 0
	if skipFulltextRebuild {
		log.Printf("📇 Search indexes loaded from disk (BM25: %d docs); rebuilding vector index only",
			s.fulltextIndex.Count())
	}

	// Decide whether the on-disk vector store is still valid to resume.
	// Only relevant when we are rebuilding; do not invalidate on skipIteration path.
	if !skipIteration && vectorPath != "" && s.vectorFileStore != nil {
		if lastWrite := s.lastWriteTime(); !lastWrite.IsZero() {
			if info, err := os.Stat(vectorPath + ".vec"); err == nil {
				if lastWrite.After(info.ModTime()) {
					restartVectorStore = true
					log.Printf("📇 BuildIndexes: db updated after vector store (db=%s, vec=%s); restarting vector build",
						lastWrite.Format(time.RFC3339), info.ModTime().Format(time.RFC3339))
					s.mu.Lock()
					_ = s.vectorFileStore.Close()
					s.vectorFileStore = nil
					s.mu.Unlock()
				}
			}
		}
	}

	// Resume vector build when a file-backed store already exists and we're rebuilding.
	resumeVectors := false
	if !skipIteration && !restartVectorStore && vectorPath != "" && s.vectorFileStore != nil && s.vectorFileStore.Count() > 0 {
		resumeVectors = true
		s.mu.Lock()
		s.resumeVectorBuild = true
		s.mu.Unlock()
		log.Printf("📇 BuildIndexes: resuming vector store with %d existing vectors", s.vectorFileStore.Count())
	}
	defer func() {
		s.mu.Lock()
		s.resumeVectorBuild = false
		s.mu.Unlock()
	}()

	// When skipping iteration and we use HNSW strategy (N >= NSmallMax), load HNSW from disk so warmup does not rebuild it.
	if skipIteration && hnswPath != "" && vectorCount >= NSmallMax {
		vectorLookup := s.getVectorLookup()
		dimensions := s.VectorIndexDimensions()
		var loaded *HNSWIndex
		var err error
		// Graph-only HNSW loads in lookup mode to avoid duplicating vectors in RAM
		// (canonical vectors remain in vector index / file store).
		loaded, err = LoadHNSWIndex(hnswPath, vectorLookup)
		if forceHNSWRebuild {
			hnswWarmupReason = "settings changed"
			log.Printf("📇 HNSW on-disk index ignored due to settings change; rebuilding")
		} else if err == nil && loaded != nil && dimensions > 0 && loaded.GetDimensions() == dimensions {
			want := HNSWConfigFromEnv()
			have := loaded.Config()
			if have.M != want.M || have.EfConstruction != want.EfConstruction || have.EfSearch != want.EfSearch {
				hnswWarmupReason = "config changed"
				log.Printf("📇 HNSW config changed (old m=%d efc=%d efs=%d, new m=%d efc=%d efs=%d); rebuilding",
					have.M, have.EfConstruction, have.EfSearch, want.M, want.EfConstruction, want.EfSearch)
			} else {
				s.hnswMu.Lock()
				s.hnswIndex = loaded
				s.hnswMu.Unlock()
				hnswLoadedFromDisk = true
				log.Printf("📇 HNSW index loaded from disk: vectors=%d tombstone_ratio=%.2f", loaded.Size(), loaded.TombstoneRatio())
			}
		} else if err != nil {
			hnswWarmupReason = fmt.Sprintf("load failed: %v", err)
		} else if loaded == nil {
			if info, statErr := os.Stat(hnswPath); statErr == nil {
				if info.Size() == 0 {
					hnswWarmupReason = "index file exists but is empty"
				} else {
					hnswWarmupReason = fmt.Sprintf("index file exists (%d bytes) but could not be decoded", info.Size())
				}
			} else if errors.Is(statErr, os.ErrNotExist) {
				hnswWarmupReason = "index file missing"
			} else {
				hnswWarmupReason = fmt.Sprintf("index file stat failed: %v", statErr)
			}
		} else if dimensions > 0 && loaded.GetDimensions() != dimensions {
			hnswWarmupReason = fmt.Sprintf("dimension mismatch (disk=%d current=%d)", loaded.GetDimensions(), dimensions)
		}
	}
	if skipIteration && vectorCount >= NSmallMax && !hnswLoadedFromDisk {
		if hnswWarmupReason == "" {
			if hnswPath == "" {
				hnswWarmupReason = "hnsw path not configured"
			} else {
				hnswWarmupReason = "no reusable on-disk HNSW"
			}
		}
		log.Printf("📇 BuildIndexes: base indexes loaded; warmup will build HNSW (%s)", hnswWarmupReason)
	}

	if skipIteration {
		s.setBuildPhase("warmup_hnsw_or_kmeans")
		log.Printf("📇 BuildIndexes: starting vector pipeline warmup (k-means may run)...")
		s.warmupVectorPipeline(ctx)
		go s.runPersist()
		s.ready.Store(true)
		s.setBuildPhase("ready")
		return nil
	}

	// Start from 0 only when we cannot resume (missing/corrupt .vec/.meta). Otherwise keep existing .vec/.meta.
	if vectorPath != "" && !resumeVectors {
		_ = os.Remove(vectorPath + ".vec")
		_ = os.Remove(vectorPath + ".meta")
		s.mu.Lock()
		if s.vectorFileStore != nil {
			_ = s.vectorFileStore.Close()
			s.vectorFileStore = nil
		}
		s.mu.Unlock()
	}

	if vectorPath != "" {
		log.Printf("📇 BuildIndexes: building from storage (file-backed vector store)")
	} else {
		log.Printf("📇 BuildIndexes: building from storage (vector index in memory)")
	}
	s.setBuildPhase("iterating_nodes")
	// Build indexes by iterating over storage.
	if iterator, ok := s.engine.(NodeIterator); ok {
		count := 0
		var fulltextBatch []*storage.Node
		var flushErr error
		flushFulltextBatch := func(batch []*storage.Node) error {
			if len(batch) == 0 {
				return nil
			}
			if count == 0 && len(batch) > 0 {
				s.maybeAutoSetVectorDimensions(firstVectorDimensions(batch[0]))
			}
			filtered := make([]*storage.Node, 0, len(batch))
			for _, n := range batch {
				shouldIndex, err := s.shouldIndexNode(n)
				if err != nil {
					return err
				}
				if shouldIndex {
					filtered = append(filtered, n)
				}
			}
			if !skipFulltextRebuild {
				entries := make([]FulltextBatchEntry, 0, len(filtered))
				for _, n := range filtered {
					text := s.extractSearchableText(n)
					entries = append(entries, FulltextBatchEntry{ID: string(n.ID), Text: text})
				}
				chunkSize := fulltextIndexChunkSize
				if chunkSize <= 0 {
					chunkSize = len(entries)
				}
				for i := 0; i < len(entries); i += chunkSize {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					end := i + chunkSize
					if end > len(entries) {
						end = len(entries)
					}
					s.fulltextIndex.IndexBatch(entries[i:end])
					// Keep heartbeat progress moving during heavy BM25 batch work.
					s.buildProcessed.Store(int64(count + end))
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			s.indexMu.Lock()
			for _, n := range filtered {
				_ = s.indexNodeLocked(n, true)
			}
			s.indexMu.Unlock()
			count += len(batch)
			s.buildProcessed.Store(int64(count))
			return nil
		}
		err := iterator.IterateNodes(func(node *storage.Node) bool {
			select {
			case <-ctx.Done():
				return false
			default:
				fulltextBatch = append(fulltextBatch, node)
				// Expose forward progress while accumulating a batch, so long BM25 batches
				// don't appear "stuck" at 0 until the first flush completes.
				s.buildProcessed.Store(int64(count + len(fulltextBatch)))
				if len(fulltextBatch) >= fulltextBuildBatchSize {
					if err := flushFulltextBatch(fulltextBatch); err != nil {
						flushErr = err
						return false
					}
					fulltextBatch = nil
					if count%5000 == 0 {
						fmt.Printf("📊 Indexed %d nodes...\n", count)
					}
				}
				return true
			}
		})
		if err != nil {
			return err
		}
		if flushErr != nil {
			return flushErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := flushFulltextBatch(fulltextBatch); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Printf("📊 Indexed %d total nodes\n", count)
		log.Printf("📇 BuildIndexes: base index iteration complete (processed=%d)", count)
		// Persist BM25 + vector store before HNSW/k-means build so base indexes are durable.
		s.setBuildPhase("persisting_base_indexes")
		s.persistBaseIndexes()
		// Drop BM25 from RAM during HNSW/IVF build to keep memory bounded.
		if fulltextPath != "" {
			s.fulltextIndex.Clear()
			log.Printf("📇 BuildIndexes: cleared BM25 in-memory state (will reload after warmup)")
		}
		log.Printf("📇 BuildIndexes: starting vector pipeline warmup (k-means may run)...")
		s.setBuildPhase("warmup_hnsw_or_kmeans")
		s.warmupVectorPipeline(ctx)
		if fulltextPath != "" {
			_ = s.fulltextIndex.Load(fulltextPath)
			log.Printf("📇 BuildIndexes: reloaded BM25 from disk after warmup")
		}
		go s.runPersist()
		s.ready.Store(true)
		s.setBuildPhase("ready")
		return nil
	}

	count := 0
	var fulltextBatch []*storage.Node
	flushFulltextBatchFallback := func(batch []*storage.Node) error {
		if len(batch) == 0 {
			return nil
		}
		if count == 0 && len(batch) > 0 {
			s.maybeAutoSetVectorDimensions(firstVectorDimensions(batch[0]))
		}
		filtered := make([]*storage.Node, 0, len(batch))
		for _, n := range batch {
			shouldIndex, err := s.shouldIndexNode(n)
			if err != nil {
				return err
			}
			if shouldIndex {
				filtered = append(filtered, n)
			}
		}
		if !skipFulltextRebuild {
			entries := make([]FulltextBatchEntry, 0, len(filtered))
			for _, n := range filtered {
				text := s.extractSearchableText(n)
				entries = append(entries, FulltextBatchEntry{ID: string(n.ID), Text: text})
			}
			chunkSize := fulltextIndexChunkSize
			if chunkSize <= 0 {
				chunkSize = len(entries)
			}
			for i := 0; i < len(entries); i += chunkSize {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				end := i + chunkSize
				if end > len(entries) {
					end = len(entries)
				}
				s.fulltextIndex.IndexBatch(entries[i:end])
				// Keep heartbeat progress moving during heavy BM25 batch work.
				s.buildProcessed.Store(int64(count + end))
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		s.indexMu.Lock()
		for _, n := range filtered {
			_ = s.indexNodeLocked(n, true)
		}
		s.indexMu.Unlock()
		count += len(batch)
		s.buildProcessed.Store(int64(count))
		return nil
	}
	err := storage.StreamNodesWithFallback(ctx, s.engine, 1000, func(node *storage.Node) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fulltextBatch = append(fulltextBatch, node)
		// Expose forward progress while accumulating a batch, so long BM25 batches
		// don't appear "stuck" at 0 until the first flush completes.
		s.buildProcessed.Store(int64(count + len(fulltextBatch)))
		if len(fulltextBatch) >= fulltextBuildBatchSize {
			if err := flushFulltextBatchFallback(fulltextBatch); err != nil {
				return err
			}
			fulltextBatch = nil
			if count%5000 == 0 {
				fmt.Printf("📊 Indexed %d nodes...\n", count)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := flushFulltextBatchFallback(fulltextBatch); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	fmt.Printf("📊 Indexed %d total nodes\n", count)
	log.Printf("📇 BuildIndexes: base index iteration complete (processed=%d)", count)
	// Persist BM25 + vector store before HNSW/k-means build so base indexes are durable.
	s.setBuildPhase("persisting_base_indexes")
	s.persistBaseIndexes()
	// Drop BM25 from RAM during HNSW/IVF build to keep memory bounded.
	if fulltextPath != "" {
		s.fulltextIndex.Clear()
		log.Printf("📇 BuildIndexes: cleared BM25 in-memory state (will reload after warmup)")
	}
	log.Printf("📇 BuildIndexes: starting vector pipeline warmup (k-means may run)...")
	s.setBuildPhase("warmup_hnsw_or_kmeans")
	s.warmupVectorPipeline(ctx)
	if fulltextPath != "" {
		_ = s.fulltextIndex.Load(fulltextPath)
		log.Printf("📇 BuildIndexes: reloaded BM25 from disk after warmup")
	}
	go s.runPersist()
	s.ready.Store(true)
	s.setBuildPhase("ready")
	return nil
}

// warmupVectorPipeline creates the vector search pipeline (and builds HNSW or IVF-HNSW if needed) so that
// the first user search is fast. When clustering is enabled and there are enough embeddings, runs
// k-means and builds per-cluster HNSW (IVF-HNSW) first so the pipeline uses IVF-HNSW instead of global HNSW.
func (s *Service) warmupVectorPipeline(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	n := s.EmbeddingCount()
	if n == 0 {
		return
	}
	log.Printf("🔍 Warming up vector search pipeline (%d vectors)...", n)
	start := time.Now()

	if err := s.warmupClusteredStrategy(ctx, n); err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("⚠️ Clustering during warmup failed (will use global HNSW): %v", err)
	}

	if _, err := s.getOrCreateVectorPipeline(ctx); err != nil {
		log.Printf("⚠️ Vector pipeline warmup failed (first search may be slow): %v", err)
		return
	}
	log.Printf("🔍 Vector search pipeline ready in %v", time.Since(start))
}

func (s *Service) warmupClusteredStrategy(ctx context.Context, vectorCount int) error {
	// When clustering is enabled and we have enough embeddings, restore or run k-means and build IVF-HNSW.
	// If centroids are persisted (from previous IVF-HNSW save), load them and skip k-means so warmup is fast.
	if !s.IsClusteringEnabled() || vectorCount < s.GetMinEmbeddingsForClustering() {
		return nil
	}
	s.mu.RLock()
	clusterIndex := s.clusterIndex
	hnswPath := s.hnswIndexPath
	s.mu.RUnlock()
	// Backfill cluster index from vector store so k-means sees full count (same as TriggerClustering).
	s.ensureClusterIndexBackfilled(vectorCount)
	if s.tryRestoreClusteredWarmupFromDisk(ctx, clusterIndex, hnswPath) {
		return nil
	}
	if err := s.TriggerClustering(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Service) tryRestoreClusteredWarmupFromDisk(ctx context.Context, clusterIndex *gpu.ClusterIndex, hnswPath string) bool {
	// Skip k-means only on initial load when we have persisted IVF-HNSW cluster files.
	// Centroids and idToCluster are derived from hnsw_ivf/ cluster files + vectors (graph-only).
	// The re-cluster timer still runs later and will re-run k-means when enabled.
	if clusterIndex == nil || hnswPath == "" || s.EmbeddingCount() == 0 {
		return false
	}
	vectorLookup := s.getVectorLookup()
	if vectorLookup == nil {
		return false
	}
	centroids, idToCluster, deriveErr := DeriveIVFCentroidsFromClusters(hnswPath, vectorLookup)
	if len(centroids) == 0 || len(idToCluster) == 0 {
		log.Printf("[IVF-HNSW] ⚠️ Could not restore from hnsw_ivf (path=%q); running k-means. deriveErr=%v centroids=%d idToCluster=%d",
			hnswPath, deriveErr, len(centroids), len(idToCluster))
		return false
	}
	if err := clusterIndex.RestoreClusteringState(centroids, idToCluster); err != nil {
		return false
	}
	if err := s.rebuildClusterHNSWIndexes(ctx, clusterIndex); err != nil {
		return false
	}
	s.rebuildClusterLexicalProfiles()
	log.Printf("🔍 IVF-HNSW restored from disk (%d clusters); skipping k-means", len(centroids))
	return true
}

// Search performs hybrid search with automatic fallback.
//
// Search strategy:
//  1. Try RRF hybrid search (vector + BM25) if embedding provided
//  2. Fall back to vector-only if RRF returns no results
//  3. Fall back to BM25-only if vector search fails or no embedding
//
// This ensures you always get results even if one index is empty or fails.
//
// Parameters:
//   - ctx: Context for cancellation
//   - query: Text query for BM25 search
//   - embedding: Vector embedding for similarity search (can be nil)
//   - opts: Search options (use DefaultSearchOptions() if unsure)
//
// Example:
//
//	svc := search.NewService(engine)
//
//	// Hybrid search (best results)
//	query := "graph database memory"
//	embedding, _ := embedder.Embed(ctx, query)
//	opts := search.DefaultSearchOptions()
//	opts.Limit = 10
//
//	resp, err := svc.Search(ctx, query, embedding, opts)
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Found %d results using %s\n",
//		resp.Returned, resp.SearchMethod)
//
//	for i, result := range resp.Results {
//		fmt.Printf("%d. [RRF: %.4f] %s\n",
//			i+1, result.RRFScore, result.Title)
//		fmt.Printf("   Vector rank: #%d, BM25 rank: #%d\n",
//			result.VectorRank, result.BM25Rank)
//	}
//
// Returns a SearchResponse with ranked results and metadata about the search method used.
func (s *Service) Search(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (resp *SearchResponse, err error) {
	start := time.Now()
	// Plan 04-05-05: observe requests_total + candidates_rows at the
	// single Search chokepoint so all internal call paths (vector-only,
	// BM25-only, hybrid, fallbacks) record uniformly. Mode is decided by
	// the dispatch below; defaulting to "hybrid" matches the most common
	// path and is overwritten in the early-return branches.
	mode := "hybrid"

	// TRC-19: search span wraps the entire search operation.
	ctx, searchSpan := startSearchSpan(ctx, query, len(embedding) > 0, mode)
	defer func() {
		if resp != nil {
			searchSpan.SetAttributes(
				attribute.Int("search.results", len(resp.Results)),
				attribute.Int("search.candidates", resp.TotalCandidates),
				attribute.String("search.mode", mode),
			)
		}
		recordSearchError(searchSpan, err)
		searchSpan.End()
	}()

	defer func() {
		candidates := -1
		if resp != nil {
			candidates = resp.TotalCandidates
		}
		s.observeSearchRequest(mode, classifySearchResult(resp, err), candidates)
		_ = start
	}()

	if s.buildAttempted.Load() && !s.IsReady() {
		return nil, ErrSearchIndexBuilding
	}
	if opts == nil {
		opts = DefaultSearchOptions()
	}

	// Set resolved value back for downstream use
	opts.MinSimilarity = s.resolveMinSimilarity(opts)

	// Cache key for result cache (same query+options => same key; used for Get and Put).
	cacheKey := searchCacheKey(query, opts)
	if s.resultCache != nil {
		if cached := s.resultCache.Get(cacheKey); cached != nil {
			s.maybeLogSearchTiming(query, cached, time.Since(start), true)
			return cached, nil
		}
	}

	// If no embedding provided, fall back to full-text only
	if len(embedding) == 0 {
		mode = "bm25" // Plan 04-05-05: closed AllowedSearchModes
		resp, err := s.fullTextSearchOnly(ctx, query, opts)
		if err == nil && s.resultCache != nil {
			s.resultCache.Put(cacheKey, resp)
		}
		s.maybeLogSearchTiming(query, resp, time.Since(start), false)
		return resp, err
	}

	// For vector-only calls (no text query), skip hybrid and go straight to
	// vector search. This avoids unnecessary BM25+RRF overhead and matches the
	// intended semantics of "pure embedding" search.
	if strings.TrimSpace(query) == "" {
		mode = "vector" // Plan 04-05-05: closed AllowedSearchModes
		resp, err := s.vectorSearchOnly(ctx, embedding, opts)
		if err == nil && s.resultCache != nil {
			s.resultCache.Put(cacheKey, resp)
		}
		s.maybeLogSearchTiming(query, resp, time.Since(start), false)
		return resp, err
	}

	// Try RRF hybrid search
	response, err := s.rrfHybridSearch(ctx, query, embedding, opts)
	if err == nil && len(response.Results) > 0 {
		if s.resultCache != nil {
			s.resultCache.Put(cacheKey, response)
		}
		s.maybeLogSearchTiming(query, response, time.Since(start), false)
		return response, nil
	}

	// Fallback to vector-only
	response, err = s.vectorSearchOnly(ctx, embedding, opts)
	if err == nil && len(response.Results) > 0 {
		response.FallbackTriggered = true
		response.Message = "RRF search returned no results, fell back to vector search"
		if s.resultCache != nil {
			s.resultCache.Put(cacheKey, response)
		}
		s.maybeLogSearchTiming(query, response, time.Since(start), false)
		return response, nil
	}

	// Final fallback to full-text
	mode = "bm25" // Plan 04-05-05: final fallback to BM25-only
	resp, err = s.fullTextSearchOnly(ctx, query, opts)
	if err == nil && s.resultCache != nil {
		s.resultCache.Put(cacheKey, resp)
	}
	s.maybeLogSearchTiming(query, resp, time.Since(start), false)
	return resp, err
}

// rrfHybridSearch performs Reciprocal Rank Fusion combining vector and BM25 results.
func (s *Service) rrfHybridSearch(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	totalStart := time.Now()
	// Avoid holding s.mu while interacting with the vector pipeline (pipelineMu),
	// otherwise we can deadlock with writers that lock pipelineMu and then need s.mu.
	// See: maybeAutoSetVectorDimensions(), ClearVectorIndex(), SetGPUManager().
	s.mu.RLock()
	reranker := s.reranker
	fulltextIndex := s.fulltextIndex
	s.mu.RUnlock()

	// Get more candidates for better fusion
	vectorCandidateLimit := vectorOverfetchLimit(opts.Limit)
	bm25CandidateLimit := opts.Limit * 2
	if bm25CandidateLimit < 20 {
		bm25CandidateLimit = 20
	}

	// Step 1: Vector search
	vectorStart := time.Now()
	var vectorResults []indexResult
	ctx = withQueryText(ctx, query)
	pipeline, pipelineErr := s.getOrCreateVectorPipeline(ctx)
	if pipelineErr != nil {
		return nil, pipelineErr
	}
	scored, searchErr := pipeline.Search(ctx, embedding, vectorCandidateLimit, opts.GetMinSimilarity(0.5))
	if searchErr != nil {
		return nil, searchErr
	}
	for _, r := range scored {
		vectorResults = append(vectorResults, indexResult{ID: r.ID, Score: r.Score})
	}
	vectorMs := int(time.Since(vectorStart).Milliseconds())

	// Step 2: BM25 full-text search (skip if no full-text index; ranks will have vector only)
	bm25Start := time.Now()
	var bm25Results []indexResult
	if fulltextIndex != nil {
		bm25Results = fulltextIndex.Search(query, bm25CandidateLimit)
	}
	bm25Ms := int(time.Since(bm25Start).Milliseconds())

	// Plan 04-05-05: observe combined vector+bm25 wall-clock as the
	// "index" stage per AllowedSearchStages. Both lookups execute
	// sequentially today; combining them at observation cadence captures
	// the joint stage budget for capacity planning. Per-index micro-detail
	// remains in the SearchMetrics struct response payload (vectorMs +
	// bm25Ms separately).
	s.observeSearchStage(ctx, "hybrid", "index",
		time.Since(vectorStart)) // vectorStart→bm25End wall clock

	// Collapse vector IDs back to unique node IDs.
	vectorResults = collapseIndexResultsByNodeID(vectorResults)

	seenOrphans := make(map[string]bool)

	// Step 3: Filter by type if specified
	if len(opts.Types) > 0 {
		vectorResults = s.filterByType(ctx, vectorResults, opts.Types, seenOrphans)
		bm25Results = s.filterByType(ctx, bm25Results, opts.Types, seenOrphans)
	}

	// Step 3b: Filter decayed candidates
	vectorResults = s.filterDecayedCandidates(vectorResults)
	bm25Results = s.filterDecayedCandidates(bm25Results)

	// Step 3c: Pre-filter by property values before top-K selection
	if len(opts.Filters) > 0 {
		vectorResults = s.filterByProperties(ctx, vectorResults, opts.Filters, seenOrphans)
		bm25Results = s.filterByProperties(ctx, bm25Results, opts.Filters, seenOrphans)
	}

	// Step 4: Fuse with RRF
	fusionStart := time.Now()
	fusedResults := s.fuseRRF(vectorResults, bm25Results, opts)

	// Step 5: Apply MMR diversification if enabled
	searchMethod := "rrf_hybrid"
	message := "Reciprocal Rank Fusion (Vector + BM25)"
	switch pipeline.candidateGen.(type) {
	case *KMeansCandidateGen:
		searchMethod = "rrf_hybrid_clustered"
		message = "RRF (K-means routed vector + BM25)"
	case *IVFHNSWCandidateGen:
		searchMethod = "rrf_hybrid_ivf_hnsw"
		message = "RRF (IVF-HNSW vector + BM25)"
	case *IVFPQCandidateGen:
		searchMethod = "rrf_hybrid_ivfpq"
		message = "RRF (IVFPQ compressed vector + BM25)"
	}
	if opts.MMREnabled && len(embedding) > 0 {
		fusedResults = s.applyMMR(ctx, fusedResults, embedding, opts.Limit, opts.MMRLambda, seenOrphans)
		searchMethod += "+mmr"
		message = fmt.Sprintf("%s + MMR diversification (λ=%.2f)", message, opts.MMRLambda)
	}

	// Step 6: Stage-2 reranking (optional)
	if opts.RerankEnabled && reranker != nil && reranker.Enabled() {
		fusedResults = s.applyStage2Rerank(ctx, query, fusedResults, opts, seenOrphans, reranker)
		if searchMethod == "rrf_hybrid" {
			searchMethod = "rrf_hybrid+rerank"
			message = fmt.Sprintf("RRF + Reranking (%s)", reranker.Name())
		} else {
			searchMethod += "+rerank"
			message += fmt.Sprintf(" + Reranking (%s)", reranker.Name())
		}
	}

	// Step 7: Convert to SearchResult and enrich with node data
	results := s.enrichResults(ctx, fusedResults, opts.Limit, seenOrphans)
	fusionMs := int(time.Since(fusionStart).Milliseconds())
	totalMs := int(time.Since(totalStart).Milliseconds())

	// Plan 04-05-05: observe fuse stage (RRF + MMR + rerank + enrich)
	// per AllowedSearchStages.
	s.observeSearchStage(ctx, "hybrid", "fuse", time.Since(fusionStart))

	return &SearchResponse{
		Status:          "success",
		Query:           query,
		Results:         results,
		TotalCandidates: len(fusedResults),
		Returned:        len(results),
		SearchMethod:    searchMethod,
		Message:         message,
		Metrics: &SearchMetrics{
			VectorSearchTimeMs: vectorMs,
			BM25SearchTimeMs:   bm25Ms,
			FusionTimeMs:       fusionMs,
			TotalTimeMs:        totalMs,
			VectorCandidates:   len(vectorResults),
			BM25Candidates:     len(bm25Results),
			FusedCandidates:    len(fusedResults),
		},
	}, nil
}

// SearchCandidate is a lightweight vector-search result: just the ID and score.
//
// This is intended for high-throughput call paths that don’t require node
// enrichment (e.g. Qdrant-compatible gRPC searches that return IDs+scores).
type SearchCandidate struct {
	ID    string
	Score float64
}

// VectorSearchCandidates performs vector-only search and returns lightweight
// candidates without enrichment. It is optimized for throughput: it skips BM25,
// RRF fusion, and storage fetches.
//
// This method uses the unified vector search pipeline (CandidateGen + ExactScore)
// with automatic strategy selection (brute-force for small N, HNSW for large N).
func (s *Service) VectorSearchCandidates(ctx context.Context, embedding []float32, opts *SearchOptions) ([]SearchCandidate, error) {
	if opts == nil {
		opts = DefaultSearchOptions()
	}

	opts.MinSimilarity = s.resolveMinSimilarity(opts)
	if len(embedding) == 0 {
		return nil, fmt.Errorf("vector search requires embedding")
	}

	candidateLimit := vectorOverfetchLimit(opts.Limit)

	// Use unified vector search pipeline
	pipeline, err := s.getOrCreateVectorPipeline(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get vector pipeline: %w", err)
	}

	scored, err := pipeline.Search(ctx, embedding, candidateLimit, opts.GetMinSimilarity(0.5))
	if err != nil {
		return nil, err
	}

	// Convert to SearchCandidate
	candidates := make([]SearchCandidate, len(scored))
	for i, s := range scored {
		candidates[i] = SearchCandidate{ID: s.ID, Score: s.Score}
	}

	candidates = collapseCandidatesByNodeID(candidates)

	seenOrphans := make(map[string]bool)
	// Apply type filters
	if len(opts.Types) > 0 {
		candidates = s.filterCandidatesByType(ctx, candidates, opts.Types, seenOrphans)
	}

	if len(candidates) > opts.Limit && opts.Limit > 0 {
		candidates = candidates[:opts.Limit]
	}

	return candidates, nil
}

func vectorOverfetchLimit(limit int) int {
	// Vector IDs in the index may represent:
	// - node IDs (main embedding)
	// - chunk IDs ("nodeID-chunk-i")
	// - named embedding IDs ("nodeID-named-name")
	//
	// We overfetch and then collapse back to unique node IDs.
	if limit <= 0 {
		return 20
	}
	over := limit * 10
	if over < limit {
		over = limit
	}
	// Keep worst-case work bounded for brute-force paths.
	if over > 5000 {
		return 5000
	}
	if over < 50 {
		return 50
	}
	return over
}

func normalizeVectorResultIDToNodeID(id string) string {
	// Chunk IDs are formatted as: "{nodeID}-chunk-{i}"
	// Named IDs are formatted as: "{nodeID}-named-{vectorName}"
	// Property-vector IDs are formatted as: "{nodeID}-prop-{propertyKey}"
	//
	// Node IDs can contain '-' (UUIDs etc.), so only treat these as suffixes when they match
	// the known patterns (and for chunks: when the suffix is an integer).
	if idx := strings.LastIndex(id, "-chunk-"); idx >= 0 {
		suffix := id[idx+len("-chunk-"):]
		if suffix != "" {
			if _, err := strconv.Atoi(suffix); err == nil {
				return id[:idx]
			}
		}
	}
	if idx := strings.LastIndex(id, "-named-"); idx >= 0 {
		suffix := id[idx+len("-named-"):]
		if suffix != "" {
			return id[:idx]
		}
	}
	if idx := strings.LastIndex(id, "-prop-"); idx >= 0 {
		suffix := id[idx+len("-prop-"):]
		if suffix != "" {
			return id[:idx]
		}
	}
	return id
}

func collapseCandidatesByNodeID(cands []SearchCandidate) []SearchCandidate {
	if len(cands) == 0 {
		return nil
	}
	best := make(map[string]float64, len(cands))
	for _, c := range cands {
		nodeID := normalizeVectorResultIDToNodeID(c.ID)
		if prev, ok := best[nodeID]; !ok || c.Score > prev {
			best[nodeID] = c.Score
		}
	}
	out := make([]SearchCandidate, 0, len(best))
	for id, score := range best {
		out = append(out, SearchCandidate{ID: id, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func collapseIndexResultsByNodeID(results []indexResult) []indexResult {
	if len(results) == 0 {
		return nil
	}
	best := make(map[string]float64, len(results))
	for _, r := range results {
		nodeID := normalizeVectorResultIDToNodeID(r.ID)
		if prev, ok := best[nodeID]; !ok || r.Score > prev {
			best[nodeID] = r.Score
		}
	}
	out := make([]indexResult, 0, len(best))
	for id, score := range best {
		out = append(out, indexResult{ID: id, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

// getOrCreateVectorPipeline returns the vector search pipeline, creating it if needed.
//
// The pipeline uses auto strategy selection:
//   - GPU brute-force (exact) when enabled and within configured thresholds
//   - Cluster routing when clustered (GPU ScoreSubset when GPU enabled but full brute is out-of-range;
//     otherwise CPU IVF-HNSW when available, else CPU k-means routing)
//   - CPU brute-force for small datasets (N < NSmallMax)
//   - Global HNSW for large datasets (N >= NSmallMax, no clustering)
func (s *Service) getOrCreateVectorPipeline(ctx context.Context) (*VectorSearchPipeline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.pipelineMu.RLock()
	if s.vectorPipeline != nil {
		s.pipelineMu.RUnlock()
		return s.vectorPipeline, nil
	}
	s.pipelineMu.RUnlock()

	s.pipelineMu.Lock()
	defer s.pipelineMu.Unlock()

	// Double-check after acquiring write lock
	if s.vectorPipeline != nil {
		return s.vectorPipeline, nil
	}

	s.mu.RLock()
	vectorCount := s.embeddingCountLocked()
	dimensions := s.VectorIndexDimensions()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	s.mu.RUnlock()

	strategy, err := s.resolveVectorStrategy(ctx, vectorCount, dimensions, vfs)
	if err != nil {
		return nil, err
	}

	log.Printf("🔍 Vector search strategy: %s (N=%d vectors)", strategy.name, vectorCount)

	exactScorer := s.resolveVectorExactScorer(strategy.scorerPolicy, vi, vfs)

	s.vectorPipeline = NewVectorSearchPipeline(strategy.candidateGen, exactScorer)
	return s.vectorPipeline, nil
}

func (s *Service) currentPipelineStrategy() strategyMode {
	s.pipelineMu.RLock()
	p := s.vectorPipeline
	s.pipelineMu.RUnlock()
	if p == nil {
		return strategyModeUnknown
	}
	switch p.candidateGen.(type) {
	case *GPUBruteForceCandidateGen:
		return strategyModeBruteGPU
	case *BruteForceCandidateGen, *FileStoreBruteForceCandidateGen:
		return strategyModeBruteCPU
	case *HNSWCandidateGen:
		return strategyModeHNSW
	default:
		return strategyModeUnknown
	}
}

func (s *Service) desiredRuntimeStrategy(vectorCount int) strategyMode {
	gpuEnabled := s.gpuManager != nil && s.gpuManager.IsEnabled()
	gpuMinN := envutil.GetInt("NORNICDB_VECTOR_GPU_BRUTE_MIN_N", 5000)
	gpuMaxN := envutil.GetInt("NORNICDB_VECTOR_GPU_BRUTE_MAX_N", 15000)
	if gpuEnabled && vectorCount >= gpuMinN && vectorCount <= gpuMaxN {
		return strategyModeBruteGPU
	}
	if vectorCount < NSmallMax {
		return strategyModeBruteCPU
	}
	return strategyModeHNSW
}

func (s *Service) snapshotStrategyInputs() (int, *VectorIndex, *VectorFileStore) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.embeddingCountLocked(), s.vectorIndex, s.vectorFileStore
}

func (s *Service) scheduleStrategyTransitionCheck() {
	if s.buildInProgress.Load() {
		return
	}
	vectorCount, _, _ := s.snapshotStrategyInputs()
	current := s.currentPipelineStrategy()
	if current == strategyModeUnknown {
		return
	}
	desired := s.desiredRuntimeStrategy(vectorCount)
	if desired == current {
		return
	}
	if (current == strategyModeBruteCPU || current == strategyModeBruteGPU) &&
		(desired == strategyModeBruteCPU || desired == strategyModeBruteGPU) {
		if s.switchBruteStrategy(desired) {
			log.Printf("🔍 Runtime strategy switch: %s -> %s (N=%d)", current.String(), desired.String(), vectorCount)
		}
		return
	}
	s.scheduleDebouncedStrategyTransition(desired)
}

func (s *Service) scheduleDebouncedStrategyTransition(target strategyMode) {
	const debounceDelay = 2 * time.Second
	s.strategyTransitionMu.Lock()
	defer s.strategyTransitionMu.Unlock()
	s.strategyTransitionPending = target
	if s.strategyTransitionInProgress {
		return
	}
	if s.strategyTransitionTimer != nil {
		_ = s.strategyTransitionTimer.Stop()
	}
	s.strategyTransitionTimer = time.AfterFunc(debounceDelay, func() {
		s.launchScheduledStrategyTransition()
	})
}

func (s *Service) launchScheduledStrategyTransition() {
	s.strategyTransitionMu.Lock()
	if s.strategyTransitionInProgress {
		s.strategyTransitionMu.Unlock()
		return
	}
	target := s.strategyTransitionPending
	s.strategyTransitionPending = strategyModeUnknown
	s.strategyTransitionInProgress = true
	s.strategyTransitionStarts++
	s.strategyTransitionDeltas = s.strategyTransitionDeltas[:0]
	s.strategyTransitionMu.Unlock()

	go s.runStrategyTransition(target)
}

func (s *Service) runStrategyTransition(target strategyMode) {
	defer func() {
		s.strategyTransitionMu.Lock()
		s.strategyTransitionInProgress = false
		next := s.strategyTransitionPending
		s.strategyTransitionMu.Unlock()
		if next != strategyModeUnknown {
			s.scheduleDebouncedStrategyTransition(next)
		}
	}()

	vectorCount, vi, vfs := s.snapshotStrategyInputs()
	current := s.currentPipelineStrategy()
	if current == strategyModeUnknown || current == target {
		return
	}

	var (
		targetHNSW *HNSWIndex
		err        error
	)
	if target == strategyModeHNSW {
		dim := s.VectorIndexDimensions()
		targetHNSW, err = s.buildHNSWForTransition(context.Background(), dim, vi, vfs)
		if err != nil {
			log.Printf("⚠️ Runtime strategy transition build failed: %v", err)
			return
		}
	}

	s.replayTransitionDeltas(targetHNSW, target, vi, vfs, 0)
	s.indexMu.Lock()
	lastSeq := s.replayTransitionDeltas(targetHNSW, target, vi, vfs, 0)
	s.applyTransitionSwapLocked(target, targetHNSW, vi, vfs)
	for {
		nextSeq := s.replayTransitionDeltas(targetHNSW, target, vi, vfs, lastSeq)
		if nextSeq == lastSeq {
			break
		}
		lastSeq = nextSeq
	}
	s.clearTransitionDeltaLogLocked(lastSeq)
	s.indexMu.Unlock()
	log.Printf("🔍 Runtime strategy switch complete: %s -> %s (N=%d)", current.String(), target.String(), vectorCount)
}

func (s *Service) applyTransitionSwapLocked(target strategyMode, targetHNSW *HNSWIndex, vi *VectorIndex, vfs *VectorFileStore) {
	var hnswForPipeline *HNSWIndex
	if target == strategyModeHNSW {
		s.hnswMu.Lock()
		old := s.hnswIndex
		s.hnswIndex = targetHNSW
		s.hnswMu.Unlock()
		hnswForPipeline = targetHNSW
		if old != nil && old != targetHNSW {
			old.Clear()
		}
	} else {
		s.hnswMu.Lock()
		old := s.hnswIndex
		s.hnswIndex = nil
		s.hnswMu.Unlock()
		if old != nil {
			old.Clear()
		}
	}
	if target == strategyModeBruteGPU {
		_ = s.ensureGPUIndexSynced(vi, vfs)
	}

	var p *VectorSearchPipeline
	switch target {
	case strategyModeBruteGPU:
		p = NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(s.gpuEmbeddingIndex), &IdentityExactScorer{})
	case strategyModeBruteCPU:
		if vfs != nil {
			p = NewVectorSearchPipeline(NewFileStoreBruteForceCandidateGen(vfs), NewCPUExactScorer(vfs))
		} else {
			p = NewVectorSearchPipeline(NewBruteForceCandidateGen(vi), NewCPUExactScorer(vi))
		}
	case strategyModeHNSW:
		if hnswForPipeline != nil {
			if vfs != nil {
				p = NewVectorSearchPipeline(NewHNSWCandidateGen(hnswForPipeline), NewCPUExactScorer(vfs))
			} else {
				p = NewVectorSearchPipeline(NewHNSWCandidateGen(hnswForPipeline), NewCPUExactScorer(vi))
			}
		}
	}
	s.pipelineMu.Lock()
	s.vectorPipeline = p
	s.pipelineMu.Unlock()
}

func (s *Service) buildPipelineForMode(mode strategyMode, vi *VectorIndex, vfs *VectorFileStore) *VectorSearchPipeline {
	switch mode {
	case strategyModeBruteGPU:
		return NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(s.gpuEmbeddingIndex), &IdentityExactScorer{})
	case strategyModeBruteCPU:
		if vfs != nil {
			return NewVectorSearchPipeline(NewFileStoreBruteForceCandidateGen(vfs), NewCPUExactScorer(vfs))
		}
		return NewVectorSearchPipeline(NewBruteForceCandidateGen(vi), NewCPUExactScorer(vi))
	case strategyModeHNSW:
		s.hnswMu.RLock()
		idx := s.hnswIndex
		s.hnswMu.RUnlock()
		if idx != nil {
			if vfs != nil {
				return NewVectorSearchPipeline(NewHNSWCandidateGen(idx), NewCPUExactScorer(vfs))
			}
			return NewVectorSearchPipeline(NewHNSWCandidateGen(idx), NewCPUExactScorer(vi))
		}
	}
	return nil
}

func (s *Service) switchBruteStrategy(target strategyMode) bool {
	vectorCount, vi, vfs := s.snapshotStrategyInputs()
	current := s.currentPipelineStrategy()
	if current == target || (current != strategyModeBruteCPU && current != strategyModeBruteGPU) {
		return false
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if target == strategyModeBruteGPU {
		if err := s.ensureGPUIndexSynced(vi, vfs); err != nil {
			log.Printf("⚠️ Runtime CPU->GPU brute switch skipped: %v", err)
			return false
		}
	}
	s.hnswMu.Lock()
	if s.hnswIndex != nil {
		s.hnswIndex.Clear()
		s.hnswIndex = nil
	}
	s.hnswMu.Unlock()
	p := s.buildPipelineForMode(target, vi, vfs)
	s.pipelineMu.Lock()
	s.vectorPipeline = p
	s.pipelineMu.Unlock()
	log.Printf("🔍 Runtime brute strategy switch: %s -> %s (N=%d)", current.String(), target.String(), vectorCount)
	return true
}

func (s *Service) buildHNSWForTransition(ctx context.Context, dimensions int, vi *VectorIndex, vfs *VectorFileStore) (*HNSWIndex, error) {
	config := HNSWConfigFromEnv()
	built := NewHNSWIndex(dimensions, config)
	if vfs != nil && vfs.Count() > 0 {
		if err := vfs.IterateChunked(10000, func(ids []string, vecs [][]float32) error {
			for i := range ids {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err := built.Add(ids[i], vecs[i]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return built, nil
	}
	if vi == nil {
		return nil, fmt.Errorf("vector index unavailable")
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	for id, vec := range vi.vectors {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if err := built.Add(id, vec); err != nil {
			return nil, err
		}
	}
	return built, nil
}

func (s *Service) ensureGPUIndexSynced(vi *VectorIndex, vfs *VectorFileStore) error {
	if s.gpuManager == nil || !s.gpuManager.IsEnabled() {
		return fmt.Errorf("gpu not enabled")
	}
	if s.gpuEmbeddingIndex != nil {
		return nil
	}
	dim := s.VectorIndexDimensions()
	if dim <= 0 {
		return fmt.Errorf("invalid vector dimensions")
	}
	cfg := gpu.DefaultEmbeddingIndexConfig(dim)
	cfg.GPUEnabled = true
	cfg.AutoSync = true
	cfg.BatchThreshold = 1000
	gi := gpu.NewEmbeddingIndex(s.gpuManager, cfg)

	if vfs != nil && vfs.Count() > 0 {
		if err := vfs.IterateChunked(10000, func(ids []string, vecs [][]float32) error {
			return gi.AddBatch(ids, vecs)
		}); err != nil {
			return err
		}
	} else if vi != nil {
		vi.mu.RLock()
		ids := make([]string, 0, len(vi.vectors))
		vecs := make([][]float32, 0, len(vi.vectors))
		for id, vec := range vi.vectors {
			if len(vec) == 0 {
				continue
			}
			ids = append(ids, id)
			vecs = append(vecs, vec)
		}
		vi.mu.RUnlock()
		if err := gi.AddBatch(ids, vecs); err != nil {
			return err
		}
	}
	if err := gi.SyncToGPU(); err != nil {
		return err
	}
	s.mu.Lock()
	s.gpuEmbeddingIndex = gi
	s.mu.Unlock()
	return nil
}

func (s *Service) replayTransitionDeltas(targetHNSW *HNSWIndex, mode strategyMode, vi *VectorIndex, vfs *VectorFileStore, after uint64) uint64 {
	s.strategyTransitionMu.Lock()
	deltas := make([]strategyDeltaMutation, 0, len(s.strategyTransitionDeltas))
	for _, d := range s.strategyTransitionDeltas {
		if d.seq > after {
			deltas = append(deltas, d)
		}
	}
	s.strategyTransitionMu.Unlock()
	last := after
	for _, d := range deltas {
		if mode == strategyModeHNSW && targetHNSW != nil {
			if d.add {
				if vec, ok := getTransitionDeltaVector(d.id, vi, vfs); ok {
					_ = targetHNSW.Update(d.id, vec)
				}
			} else {
				targetHNSW.Remove(d.id)
			}
		} else if mode == strategyModeBruteGPU {
			s.mu.RLock()
			gi := s.gpuEmbeddingIndex
			s.mu.RUnlock()
			if gi != nil {
				if d.add {
					if vec, ok := getTransitionDeltaVector(d.id, vi, vfs); ok {
						_ = gi.Add(d.id, vec)
					}
				} else {
					_ = gi.Remove(d.id)
				}
			}
		}
		last = d.seq
	}
	return last
}

func getTransitionDeltaVector(id string, vi *VectorIndex, vfs *VectorFileStore) ([]float32, bool) {
	if vfs != nil {
		if vec, ok := vfs.GetVector(id); ok && len(vec) > 0 {
			return vec, true
		}
	}
	if vi != nil {
		if vec, ok := vi.GetVector(id); ok && len(vec) > 0 {
			return vec, true
		}
	}
	return nil, false
}

func (s *Service) clearTransitionDeltaLogLocked(applied uint64) {
	s.strategyTransitionMu.Lock()
	if applied == 0 || len(s.strategyTransitionDeltas) == 0 {
		s.strategyTransitionDeltas = s.strategyTransitionDeltas[:0]
		s.strategyTransitionMu.Unlock()
		return
	}
	dst := s.strategyTransitionDeltas[:0]
	for _, d := range s.strategyTransitionDeltas {
		if d.seq > applied {
			dst = append(dst, d)
		}
	}
	s.strategyTransitionDeltas = dst
	s.strategyTransitionMu.Unlock()
}

func (s *Service) appendStrategyDelta(id string, add bool) {
	s.strategyTransitionMu.Lock()
	defer s.strategyTransitionMu.Unlock()
	if !s.strategyTransitionInProgress {
		return
	}
	s.strategyTransitionSeq++
	delta := strategyDeltaMutation{
		seq: s.strategyTransitionSeq,
		id:  id,
		add: add,
	}
	s.strategyTransitionDeltas = append(s.strategyTransitionDeltas, delta)
}

func (m strategyMode) String() string {
	switch m {
	case strategyModeBruteCPU:
		return "CPU brute-force"
	case strategyModeBruteGPU:
		return "GPU brute-force"
	case strategyModeHNSW:
		return "HNSW"
	default:
		return "unknown"
	}
}

type exactScorerPolicy string

const (
	exactScorerPolicyIdentity exactScorerPolicy = "identity"
	exactScorerPolicyCPU      exactScorerPolicy = "cpu"
)

type vectorStrategyDescriptor struct {
	name         string
	candidateGen CandidateGenerator
	scorerPolicy exactScorerPolicy
}

func (s *Service) resolveVectorStrategy(ctx context.Context, vectorCount, dimensions int, vfs *VectorFileStore) (*vectorStrategyDescriptor, error) {
	if ANNQualityFromEnv() == ANNQualityCompressed {
		return s.resolveCompressedVectorStrategy(ctx, vectorCount, dimensions, vfs)
	}
	return s.resolveStandardVectorStrategy(ctx, vectorCount, dimensions, vfs)
}

func (s *Service) resolveCompressedVectorStrategy(ctx context.Context, vectorCount, dimensions int, vfs *VectorFileStore) (*vectorStrategyDescriptor, error) {
	profile := ResolveCompressedANNProfile(vectorCount, dimensions, vfs != nil && vfs.Count() > 0)
	if !profile.Active {
		for _, diag := range profile.Diagnostics {
			log.Printf("[IVFPQ] ⏭️ compressed mode inactive | code=%s reason=%s", diag.Code, diag.Message)
		}
		strategy, err := s.resolveStandardVectorStrategy(ctx, vectorCount, dimensions, vfs)
		if err != nil {
			return nil, err
		}
		strategy.name = "compressed-disabled -> " + strategy.name
		return strategy, nil
	}

	idx, err := s.getOrBuildIVFPQIndex(ctx, ivfpqProfileFromCompressed(profile), vfs)
	if err != nil {
		log.Printf("[IVFPQ] ⚠️ compressed build/load failed, falling back to standard path: %v", err)
		strategy, fallbackErr := s.resolveStandardVectorStrategy(ctx, vectorCount, dimensions, vfs)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		strategy.name = "compressed-fallback -> " + strategy.name
		return strategy, nil
	}
	return &vectorStrategyDescriptor{
		name:         fmt.Sprintf("IVFPQ compressed (lists=%d segments=%d bits=%d nprobe=%d)", profile.IVFLists, profile.PQSegments, profile.PQBits, profile.NProbe),
		candidateGen: NewIVFPQCandidateGen(idx, profile.NProbe),
		scorerPolicy: exactScorerPolicyCPU,
	}, nil
}

func ivfpqProfileFromCompressed(profile CompressedANNProfile) IVFPQProfile {
	return IVFPQProfile{
		Dimensions:          profile.Dimensions,
		IVFLists:            profile.IVFLists,
		PQSegments:          profile.PQSegments,
		PQBits:              profile.PQBits,
		NProbe:              profile.NProbe,
		RerankTopK:          profile.RerankTopK,
		TrainingSampleMax:   profile.TrainingSampleMax,
		KMeansMaxIterations: profile.KMeansMaxIterations,
	}
}

func (s *Service) resolveStandardVectorStrategy(ctx context.Context, vectorCount, dimensions int, vfs *VectorFileStore) (*vectorStrategyDescriptor, error) {
	// Auto strategy: choose candidate generator based on dataset size and clustering
	gpuEnabled := s.gpuManager != nil && s.gpuManager.IsEnabled() && s.gpuEmbeddingIndex != nil
	gpuMinN := envutil.GetInt("NORNICDB_VECTOR_GPU_BRUTE_MIN_N", 5000)
	gpuMaxN := envutil.GetInt("NORNICDB_VECTOR_GPU_BRUTE_MAX_N", 15000)

	// Prefer GPU brute-force (exact) when enabled and within configured thresholds.
	// This path is exact and typically highest-throughput within its tuned N range.
	if gpuEnabled && vectorCount >= gpuMinN && vectorCount <= gpuMaxN {
		return &vectorStrategyDescriptor{
			name:         "GPU brute-force",
			candidateGen: NewGPUBruteForceCandidateGen(s.gpuEmbeddingIndex),
			scorerPolicy: exactScorerPolicyIdentity,
		}, nil
	}
	if vectorCount < NSmallMax {
		// Small dataset: use brute-force on CPU (exact) when vectors are in memory;
		// when using file-backed store, do direct file-store scan (no HNSW build).
		if vfs != nil {
			return &vectorStrategyDescriptor{
				name:         "CPU brute-force (file store scan)",
				candidateGen: NewFileStoreBruteForceCandidateGen(vfs),
				scorerPolicy: exactScorerPolicyCPU,
			}, nil
		}
		return &vectorStrategyDescriptor{
			name:         "CPU brute-force",
			candidateGen: NewBruteForceCandidateGen(s.vectorIndex),
			scorerPolicy: exactScorerPolicyCPU,
		}, nil
	}
	if s.clusterIndex != nil && s.clusterIndex.IsClustered() {
		return s.resolveClusteredVectorStrategy(vectorCount, gpuEnabled, gpuMaxN)
	}
	// Large dataset: use HNSW (lazy-initialize if needed)
	hnswIndex, err := s.getOrCreateHNSWIndex(ctx, dimensions)
	if err != nil {
		return nil, fmt.Errorf("failed to create HNSW index: %w", err)
	}
	return &vectorStrategyDescriptor{
		name:         "HNSW",
		candidateGen: NewHNSWCandidateGen(hnswIndex),
		scorerPolicy: exactScorerPolicyCPU,
	}, nil
}

func (s *Service) resolveClusteredVectorStrategy(vectorCount int, gpuEnabled bool, gpuMaxN int) (*vectorStrategyDescriptor, error) {
	// If clustering is enabled and clusters are built, use centroid routing on CPU
	// (GPU subset scoring when GPU is enabled but full brute is out-of-range, else IVF-HNSW when available,
	// else CPU k-means candidate generation).
	numClustersToSearch := 3 // Default: search 3 nearest clusters
	// When GPU is enabled but full brute-force is not selected (e.g. N too large),
	// still use k-means centroids to route and score only the cluster subset.
	// ScoreSubset uses GPU when possible and falls back to CPU gracefully.
	if gpuEnabled && vectorCount > gpuMaxN {
		return &vectorStrategyDescriptor{
			name: "GPU k-means (cluster routing)",
			candidateGen: NewGPUKMeansCandidateGen(s.clusterIndex, numClustersToSearch).
				SetClusterSelector(s.selectHybridClusters),
			scorerPolicy: exactScorerPolicyIdentity,
		}, nil
	}

	if envutil.GetBoolStrict("NORNICDB_VECTOR_IVF_HNSW_ENABLED", false) && !gpuEnabled {
		s.clusterHNSWMu.RLock()
		hasClusterHNSW := len(s.clusterHNSW) > 0
		s.clusterHNSWMu.RUnlock()
		if hasClusterHNSW {
			gen := NewIVFHNSWCandidateGen(s.clusterIndex, func(clusterID int) *HNSWIndex {
				s.clusterHNSWMu.RLock()
				defer s.clusterHNSWMu.RUnlock()
				return s.clusterHNSW[clusterID]
			}, numClustersToSearch).
				SetClusterSelector(s.selectHybridClusters)
			return &vectorStrategyDescriptor{
				name:         "IVF-HNSW (per-cluster HNSW)",
				candidateGen: gen,
				scorerPolicy: exactScorerPolicyCPU,
			}, nil
		}
	}

	gen := NewKMeansCandidateGen(s.clusterIndex, s.vectorIndex, numClustersToSearch).
		SetClusterSelector(s.selectHybridClusters)
	return &vectorStrategyDescriptor{
		name:         "CPU k-means (cluster routing)",
		candidateGen: gen,
		scorerPolicy: exactScorerPolicyCPU,
	}, nil
}

func (s *Service) resolveVectorExactScorer(policy exactScorerPolicy, vi *VectorIndex, vfs *VectorFileStore) ExactScorer {
	// Exact scoring defaults to SIMD CPU scoring.
	switch policy {
	case exactScorerPolicyIdentity:
		return &IdentityExactScorer{}
	default:
		var getter VectorGetter = vi
		if vfs != nil {
			getter = vfs
		}
		return NewCPUExactScorer(getter)
	}
}

// getOrCreateHNSWIndex returns the HNSW index, creating it if needed.
func (s *Service) getOrCreateHNSWIndex(ctx context.Context, dimensions int) (*HNSWIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.hnswMu.RLock()
	if idx := s.hnswIndex; idx != nil {
		s.hnswMu.RUnlock()
		return idx, nil
	}
	s.hnswMu.RUnlock()

	config := HNSWConfigFromEnv()
	built := NewHNSWIndex(dimensions, config)

	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	ft := s.fulltextIndex
	s.mu.RUnlock()
	seedNodeIDs := s.hnswLexicalSeedNodeSet(ft)
	if len(seedNodeIDs) > 0 {
		log.Printf("[HNSW] 🧭 Lexical seeding enabled: %d seed node IDs", len(seedNodeIDs))
	}

	const hnswProgressInterval = 50000 // log progress every N vectors
	if vfs != nil && vfs.Count() > 0 {
		total := vfs.Count()
		log.Printf("[HNSW] 🔨 Building from file store: %d vectors (chunk size 10k)", total)
		built.SetVectorLookup(s.getVectorLookup())
		chunkSize := 10000
		var added int
		addChunkVectors := func(ids []string, vecs [][]float32, seedOnly bool) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			for i := range ids {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				isSeed := vectorIDInSeedNodeSet(ids[i], seedNodeIDs)
				if seedOnly && !isSeed {
					continue
				}
				if !seedOnly && isSeed {
					continue
				}
				if err := built.Add(ids[i], vecs[i]); err != nil {
					return fmt.Errorf("failed to add vector to HNSW: %w", err)
				}
				added++
			}
			if added%hnswProgressInterval == 0 || added == total {
				log.Printf("[HNSW] 🔨 Progress: %d / %d vectors", added, total)
			}
			return nil
		}
		if len(seedNodeIDs) > 0 {
			// Seed-first pass: add vectors belonging to BM25 lexical seed docs first.
			if err := vfs.IterateChunked(chunkSize, func(ids []string, vecs [][]float32) error {
				return addChunkVectors(ids, vecs, true)
			}); err != nil {
				return nil, err
			}
			// Follow-up pass: add the remaining vectors.
			if err := vfs.IterateChunked(chunkSize, func(ids []string, vecs [][]float32) error {
				return addChunkVectors(ids, vecs, false)
			}); err != nil {
				return nil, err
			}
		} else if err := vfs.IterateChunked(chunkSize, func(ids []string, vecs [][]float32) error {
			return addChunkVectors(ids, vecs, false)
		}); err != nil {
			return nil, err
		}
		log.Printf("[HNSW] 🔨 Built from file store: %d vectors", added)
	} else if vi != nil {
		vi.mu.RLock()
		pairs := make([]struct {
			id  string
			vec []float32
		}, 0, len(vi.vectors))
		for id, vec := range vi.vectors {
			pairs = append(pairs, struct {
				id  string
				vec []float32
			}{id: id, vec: vec})
		}
		vi.mu.RUnlock()
		if len(seedNodeIDs) > 0 && len(pairs) > 0 {
			seedPairs := make([]struct {
				id  string
				vec []float32
			}, 0, len(pairs)/8)
			otherPairs := make([]struct {
				id  string
				vec []float32
			}, 0, len(pairs))
			for _, p := range pairs {
				if vectorIDInSeedNodeSet(p.id, seedNodeIDs) {
					seedPairs = append(seedPairs, p)
					continue
				}
				otherPairs = append(otherPairs, p)
			}
			pairs = append(seedPairs, otherPairs...)
			log.Printf("[HNSW] 🧭 Lexical-seeded build order: %d seeded vectors prioritized", len(seedPairs))
		}
		total := len(pairs)
		log.Printf("[HNSW] 🔨 Building from in-memory index: %d vectors", total)
		for i, p := range pairs {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err := built.Add(p.id, p.vec); err != nil {
				return nil, fmt.Errorf("failed to add vector to HNSW: %w", err)
			}
			if (i+1)%hnswProgressInterval == 0 || i+1 == total {
				log.Printf("[HNSW] 🔨 Progress: %d / %d vectors", i+1, total)
			}
		}
		log.Printf("[HNSW] 🔨 Built from memory: %d vectors", total)
	} else {
		return nil, fmt.Errorf("vector index is nil")
	}

	// Install unless someone else won the race.
	s.hnswMu.Lock()
	if s.hnswIndex != nil {
		existing := s.hnswIndex
		s.hnswMu.Unlock()
		return existing, nil
	}

	// Log configuration and stats for observability
	quality := os.Getenv("NORNICDB_VECTOR_ANN_QUALITY")
	if quality == "" {
		quality = "fast"
	}
	log.Printf("🔍 HNSW index created: quality=%s M=%d efConstruction=%d efSearch=%d vectors=%d tombstone_ratio=%.2f",
		quality, config.M, config.EfConstruction, config.EfSearch, built.Size(), built.TombstoneRatio())

	s.hnswIndex = built
	s.hnswMu.Unlock()

	s.ensureHNSWMaintenance()
	return built, nil
}

func (s *Service) hnswLexicalSeedNodeSet(ft bm25Index) map[string]struct{} {
	if ft == nil {
		return nil
	}
	maxTerms := envutil.GetInt("NORNICDB_HNSW_LEXICAL_SEED_MAX_TERMS", 256)
	if maxTerms <= 0 {
		maxTerms = 256
	}
	perTerm := envutil.GetInt("NORNICDB_HNSW_LEXICAL_SEED_PER_TERM", 8)
	if perTerm <= 0 {
		perTerm = 8
	}
	seedIDs := ft.LexicalSeedDocIDs(maxTerms, perTerm)
	if len(seedIDs) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(seedIDs))
	for _, id := range seedIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func vectorIDInSeedNodeSet(vectorID string, seedNodeIDs map[string]struct{}) bool {
	if len(seedNodeIDs) == 0 || vectorID == "" {
		return false
	}
	nodeID := normalizeVectorResultIDToNodeID(vectorID)
	_, ok := seedNodeIDs[nodeID]
	return ok
}

func (s *Service) ensureHNSWMaintenance() {
	s.hnswMaintOnce.Do(func() {
		s.hnswMaintStop = make(chan struct{})

		interval := envDurationMs("NORNICDB_HNSW_MAINT_INTERVAL_MS", 30_000)
		minRebuildInterval := envDurationSec("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", 60)
		rebuildRatio := envFloat("NORNICDB_HNSW_TOMBSTONE_REBUILD_RATIO", 0.50)
		maxOverhead := envFloat("NORNICDB_HNSW_MAX_TOMBSTONE_OVERHEAD_FACTOR", 2.0)
		enabled := envutil.GetBoolStrict("NORNICDB_HNSW_REBUILD_ENABLED", true)

		ticker := time.NewTicker(interval)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if !enabled {
						continue
					}
					_ = s.maybeRebuildHNSW(context.Background(), rebuildRatio, maxOverhead, minRebuildInterval)
				case <-s.hnswMaintStop:
					return
				}
			}
		}()
	})
}

func (s *Service) maybeRebuildHNSW(ctx context.Context, tombstoneRatioThreshold, maxOverheadFactor float64, minInterval time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if minInterval <= 0 {
		minInterval = 60 * time.Second
	}

	last := time.Unix(s.hnswLastRebuildUnix.Load(), 0)
	if !last.IsZero() && time.Since(last) < minInterval {
		return nil
	}
	deferredMutations := s.hnswDeferredMutations.Load()
	deferredThreshold := int64(envutil.GetInt("NORNICDB_HNSW_DEFERRED_REBUILD_THRESHOLD", 10000))
	rebuildForDeferred := deferredThreshold > 0 && deferredMutations >= deferredThreshold

	s.hnswMu.RLock()
	old := s.hnswIndex
	s.hnswMu.RUnlock()
	if old == nil {
		return nil
	}

	// Derive rebuild condition from a single read lock on the index.
	old.mu.RLock()
	total := len(old.nodeLevel)
	live := old.liveCount
	old.mu.RUnlock()
	if total == 0 || live <= 0 {
		return nil
	}

	deleted := total - live
	ratio := float64(deleted) / float64(total)
	overhead := float64(total) / float64(live)
	if ratio <= tombstoneRatioThreshold && overhead <= maxOverheadFactor && !rebuildForDeferred {
		return nil
	}

	if !s.hnswRebuildInFlight.CompareAndSwap(false, true) {
		return nil
	}
	defer s.hnswRebuildInFlight.Store(false)

	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	s.mu.RUnlock()

	rebuilt := NewHNSWIndex(old.dimensions, old.config)
	const rebuildProgressInterval = 50000
	if vfs != nil && vfs.Count() > 0 {
		total := vfs.Count()
		log.Printf("[HNSW] 🔄 Rebuilding: vectors=%d reason=tombstone_ratio=%.3f overhead=%.3f deferred=%d",
			total, ratio, overhead, deferredMutations)
		var added int
		if err := vfs.IterateChunked(10000, func(ids []string, vecs [][]float32) error {
			for i := range ids {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				_ = rebuilt.Add(ids[i], vecs[i])
			}
			added += len(ids)
			if added%rebuildProgressInterval == 0 || added == total {
				log.Printf("[HNSW] 🔄 Rebuild progress: %d / %d vectors", added, total)
			}
			return nil
		}); err != nil {
			return err
		}
		log.Printf("[HNSW] 🔄 Rebuild complete: %d vectors", added)
	} else if vi != nil {
		vi.mu.RLock()
		pairs := make([]struct {
			id  string
			vec []float32
		}, 0, len(vi.vectors))
		for id, vec := range vi.vectors {
			pairs = append(pairs, struct {
				id  string
				vec []float32
			}{id: id, vec: vec})
		}
		vi.mu.RUnlock()
		total := len(pairs)
		log.Printf("[HNSW] 🔄 Rebuilding: vectors=%d reason=tombstone_ratio=%.3f overhead=%.3f deferred=%d",
			total, ratio, overhead, deferredMutations)
		for i, p := range pairs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			_ = rebuilt.Add(p.id, p.vec)
			if (i+1)%rebuildProgressInterval == 0 || i+1 == total {
				log.Printf("[HNSW] 🔄 Rebuild progress: %d / %d vectors", i+1, total)
			}
		}
		log.Printf("[HNSW] 🔄 Rebuild complete: %d vectors", total)
	} else {
		return nil
	}

	// Swap only if the index hasn't changed.
	// IMPORTANT: do NOT take pipelineMu while holding hnswMu.
	// Search pipeline creation uses lock order pipelineMu -> hnswMu, so taking
	// hnswMu -> pipelineMu here can deadlock.
	swapped := false
	s.hnswMu.Lock()
	if s.hnswIndex == old {
		s.hnswIndex = rebuilt
		s.hnswLastRebuildUnix.Store(time.Now().Unix())
		swapped = true
	}
	s.hnswMu.Unlock()

	if swapped {
		// Invalidate pipeline after releasing hnswMu so lock order remains consistent.
		s.pipelineMu.Lock()
		s.vectorPipeline = nil
		s.pipelineMu.Unlock()
		s.hnswDeferredMutations.Store(0)
	}

	return nil
}

func envFloat(key string, fallback float64) float64 {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}

// bm25SettingsEquivalent treats legacy BM25 format version as compatible when schema/props match.
// This avoids unnecessary fulltext rebuilds during one-time migration from legacy bm25 to bm25.v2.
func bm25SettingsEquivalent(saved, current, currentFormat string) bool {
	if saved == current {
		return true
	}
	parse := func(raw string) map[string]string {
		out := make(map[string]string, 4)
		for _, part := range strings.Split(raw, ";") {
			if part == "" {
				continue
			}
			k, v, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
		return out
	}
	savedKV := parse(saved)
	currentKV := parse(current)
	if savedKV["schema"] == "" || currentKV["schema"] == "" {
		return false
	}
	if savedKV["schema"] != currentKV["schema"] || savedKV["props"] != currentKV["props"] {
		return false
	}
	// Treat BM25 format 1.0.0 and current V2 format as equivalent for rebuild gating.
	return savedKV["format"] == "1.0.0" && currentKV["format"] == currentFormat && currentFormat == bm25V2FormatVersion
}

func envDurationMs(key string, fallbackMs int) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return time.Duration(fallbackMs) * time.Millisecond
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return time.Duration(fallbackMs) * time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}

func envDurationSec(key string, fallbackSec int) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return time.Duration(fallbackSec) * time.Second
	}
	sec, err := strconv.Atoi(raw)
	if err != nil || sec <= 0 {
		return time.Duration(fallbackSec) * time.Second
	}
	return time.Duration(sec) * time.Second
}

// filterCandidatesByType filters candidates by node type/label.
func (s *Service) filterCandidatesByType(ctx context.Context, candidates []SearchCandidate, types []string, seenOrphans map[string]bool) []SearchCandidate {
	if len(types) == 0 {
		return candidates
	}

	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[strings.ToLower(t)] = true
	}

	filtered := make([]SearchCandidate, 0, len(candidates))
	for _, cand := range candidates {
		node, err := s.engine.GetNode(storage.NodeID(cand.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, cand.ID, err, seenOrphans) {
				continue
			}
			continue
		}

		// Check if any label matches
		matches := false
		for _, label := range node.Labels {
			if typeSet[strings.ToLower(label)] {
				matches = true
				break
			}
		}

		if matches {
			filtered = append(filtered, cand)
		}
	}

	return filtered
}

// fuseRRF implements the Reciprocal Rank Fusion (RRF) algorithm.
//
// RRF combines multiple ranked lists without requiring score normalization.
// Each ranking method votes for documents using their rank positions.
//
// Formula: RRF_score(doc) = Σ (weight_i / (k + rank_i))
//
// Where:
//   - k = constant (default 60) to smooth rank differences
//   - rank_i = position in list i (1-indexed: 1st place = rank 1)
//   - weight_i = importance weight for list i (default 1.0)
//
// Why k=60?
//   - From research by Cormack et al. (2009)
//   - Balances between giving too much weight to top results vs treating all ranks equally
//   - k=60 means rank #1 gets score 1/61=0.016, rank #2 gets 1/62=0.016
//   - Difference is small, but rank #1 is still slightly better
//
// Example calculation:
//
//	Document appears in:
//	  - Vector results at rank #2
//	  - BM25 results at rank #5
//
//	RRF_score = (1.0 / (60 + 2)) + (1.0 / (60 + 5))
//	          = (1.0 / 62) + (1.0 / 65)
//	          = 0.01613 + 0.01538
//	          = 0.03151
//
//	Document only in vector at rank #1:
//	RRF_score = (1.0 / (60 + 1)) + 0
//	          = 0.01639
//
//	First document wins! Being in both lists beats being #1 in just one.
//
// ELI12:
//
// Think of it like American Idol with two judges:
//   - Judge A ranks singers by vocal technique
//   - Judge B ranks by stage presence
//
// A singer ranked #2 by both judges should beat one ranked #1 by only one judge.
// RRF does this math automatically!
//
// Reference: Cormack, Clarke & Buettcher (2009)
// "Reciprocal Rank Fusion outperforms the best known automatic evaluation
// measures in combining results from multiple text retrieval systems."
func (s *Service) fuseRRF(vectorResults, bm25Results []indexResult, opts *SearchOptions) []rrfResult {
	// Create rank maps (1-indexed per RRF formula)
	vectorRanks := make(map[string]int)
	for i, r := range vectorResults {
		vectorRanks[r.ID] = i + 1
	}

	bm25Ranks := make(map[string]int)
	for i, r := range bm25Results {
		bm25Ranks[r.ID] = i + 1
	}

	// Get all unique document IDs
	allIDs := make(map[string]struct{})
	for _, r := range vectorResults {
		allIDs[r.ID] = struct{}{}
	}
	for _, r := range bm25Results {
		allIDs[r.ID] = struct{}{}
	}

	// Calculate RRF scores
	var results []rrfResult
	k := opts.RRFK
	if k == 0 {
		k = 60 // Default
	}
	vectorWeight := opts.VectorWeight
	if vectorWeight == 0 {
		vectorWeight = 1.0 // Default weight
	}
	bm25Weight := opts.BM25Weight
	if bm25Weight == 0 {
		bm25Weight = 1.0 // Default weight
	}

	for id := range allIDs {
		var vectorComponent, bm25Component float64

		if rank, ok := vectorRanks[id]; ok {
			vectorComponent = vectorWeight / (k + float64(rank))
		}
		if rank, ok := bm25Ranks[id]; ok {
			bm25Component = bm25Weight / (k + float64(rank))
		}

		rrfScore := vectorComponent + bm25Component

		// Skip below threshold
		if rrfScore < opts.MinRRFScore {
			continue
		}

		// Get original score (prefer vector if available)
		var originalScore float64
		if idx := findResultIndex(vectorResults, id); idx >= 0 {
			originalScore = vectorResults[idx].Score
		} else if idx := findResultIndex(bm25Results, id); idx >= 0 {
			originalScore = bm25Results[idx].Score
		}

		results = append(results, rrfResult{
			ID:            id,
			RRFScore:      rrfScore,
			VectorRank:    vectorRanks[id],
			BM25Rank:      bm25Ranks[id],
			OriginalScore: originalScore,
		})
	}

	// Sort by RRF score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].RRFScore > results[j].RRFScore
	})

	return results
}

// applyMMR applies Maximal Marginal Relevance diversification to search results.
//
// MMR re-ranks results to balance relevance with diversity, preventing redundant
// results that are too similar to each other.
//
// Formula: MMR(d) = λ * Sim(d, query) - (1-λ) * max(Sim(d, d_i))
//
// Where:
//   - λ (lambda) controls relevance vs diversity balance (0.0 to 1.0)
//   - λ = 1.0: Pure relevance (no diversity)
//   - λ = 0.0: Pure diversity (ignore relevance)
//   - λ = 0.7: Balanced (default, 70% relevance, 30% diversity)
//   - Sim(d, query) = similarity to the query (RRF score)
//   - max(Sim(d, d_i)) = max similarity to already selected results
//
// Algorithm:
//  1. Select the most relevant document first
//  2. For each remaining position:
//     - Calculate MMR score for all remaining docs
//     - Select doc with highest MMR (balancing relevance + diversity)
//  3. Repeat until limit reached
//
// ELI12:
//
// Imagine picking a playlist from your library. You don't want 5 songs that
// all sound the same! MMR is like saying:
//   - "I want songs I like (relevance)"
//   - "But also songs that are different from what I already picked (diversity)"
//
// Lambda controls how much you care about variety vs. your favorites.
//
// Reference: Carbonell & Goldstein (1998)
// "The Use of MMR, Diversity-Based Reranking for Reordering Documents
// and Producing Summaries"
func (s *Service) applyMMR(ctx context.Context, results []rrfResult, queryEmbedding []float32, limit int, lambda float64, seenOrphans map[string]bool) []rrfResult {
	if len(results) <= 1 || lambda >= 1.0 {
		// No diversification needed
		return results
	}

	// Get embeddings for all candidate documents
	type docWithEmbed struct {
		result    rrfResult
		embedding []float32
	}

	candidates := make([]docWithEmbed, 0, len(results))
	for _, r := range results {
		// Get embedding from storage
		node, err := s.engine.GetNode(storage.NodeID(r.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, r.ID, err, seenOrphans) {
				continue
			}
			candidates = append(candidates, docWithEmbed{result: r, embedding: nil})
			continue
		}
		if node == nil || len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0 {
			// No embedding - use original score only
			candidates = append(candidates, docWithEmbed{
				result:    r,
				embedding: nil,
			})
		} else {
			// Use first chunk embedding (always stored in ChunkEmbeddings, even single chunk = array of 1)
			candidates = append(candidates, docWithEmbed{
				result:    r,
				embedding: node.ChunkEmbeddings[0],
			})
		}
	}

	// MMR selection
	selected := make([]rrfResult, 0, limit)
	remaining := candidates

	for len(selected) < limit && len(remaining) > 0 {
		bestIdx := -1
		bestMMR := math.Inf(-1)

		for i, cand := range remaining {
			// Relevance component: similarity to query (using RRF score as proxy)
			relevance := cand.result.RRFScore

			// Diversity component: max similarity to already selected docs
			maxSimToSelected := 0.0
			if cand.embedding != nil && len(selected) > 0 {
				for _, sel := range selected {
					// Find embedding for selected doc
					for _, c := range candidates {
						if c.result.ID == sel.ID && c.embedding != nil {
							sim := vector.CosineSimilarity(cand.embedding, c.embedding)
							if sim > maxSimToSelected {
								maxSimToSelected = sim
							}
							break
						}
					}
				}
			}

			// MMR formula
			mmrScore := lambda*relevance - (1-lambda)*maxSimToSelected

			if mmrScore > bestMMR {
				bestMMR = mmrScore
				bestIdx = i
			}
		}

		if bestIdx >= 0 {
			selected = append(selected, remaining[bestIdx].result)
			// Remove selected from remaining
			remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
		} else {
			break
		}
	}

	return selected
}

// applyStage2Rerank applies Stage-2 reranking to RRF results.
//
// This is Stage 2 of a two-stage retrieval system:
//   - Stage 1 (fast): Bi-encoder retrieval (vector + BM25 with RRF)
//   - Stage 2 (accurate): Optional reranking of top candidates (LLM or cross-encoder)
//
// Reranking is slower than Stage 1, so it should be used on a bounded TopK.
func (s *Service) applyStage2Rerank(ctx context.Context, query string, results []rrfResult, opts *SearchOptions, seenOrphans map[string]bool, reranker Reranker) []rrfResult {
	if len(results) == 0 {
		return results
	}
	if reranker == nil || !reranker.Enabled() {
		return results
	}

	// Limit to top-K (optional; keeps prompt/service bounded).
	topK := opts.RerankTopK
	if topK <= 0 {
		topK = 100
	}
	if len(results) > topK {
		results = results[:topK]
	}

	// Build candidates with content from storage.
	candidates := make([]RerankCandidate, 0, len(results))
	for _, r := range results {
		node, err := s.engine.GetNode(storage.NodeID(r.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, r.ID, err, seenOrphans) {
				continue
			}
			continue
		}
		if node == nil {
			continue
		}

		// Extract searchable content
		content := s.extractSearchableText(node)
		if content == "" {
			continue
		}

		candidates = append(candidates, RerankCandidate{
			ID:      r.ID,
			Content: content,
			Score:   r.RRFScore,
		})
	}

	if len(candidates) == 0 {
		return results
	}

	// Log before reranking
	queryPreview := query
	if len(queryPreview) > 60 {
		queryPreview = queryPreview[:57] + "..."
	}
	log.Printf("🔄 Reranking %d candidates for query %q (%s)...", len(candidates), queryPreview, reranker.Name())
	start := time.Now()

	// Apply Stage-2 reranking.
	reranked, err := reranker.Rerank(ctx, query, candidates)
	if err != nil {
		// Fallback to original results on error
		log.Printf("⚠️ Reranking failed (%s): %v; using original order", reranker.Name(), err)
		return results
	}

	// Log after reranking
	log.Printf("✅ Reranking complete: %d results in %v (%s)", len(reranked), time.Since(start), reranker.Name())

	// If reranker produced nearly identical scores (e.g. model not discriminating),
	// keep original RRF order and scores so the user gets the better ranking.
	// This avoids replacing discriminative RRF (0.03, 0.02, ...) with flat 0.49 for all.
	const minScoreRange = 0.05
	var scoreMin, scoreMax float64
	for i, r := range reranked {
		s := r.FinalScore
		if i == 0 {
			scoreMin, scoreMax = s, s
		} else {
			if s < scoreMin {
				scoreMin = s
			}
			if s > scoreMax {
				scoreMax = s
			}
		}
	}
	if scoreMax-scoreMin < minScoreRange {
		log.Printf("ℹ️ Reranking produced nearly identical scores (range=%.4f); using RRF order and scores", scoreMax-scoreMin)
		return results
	}

	// Build map by ID so we can reliably preserve VectorRank/BM25Rank when converting
	// reranker output back to rrfResult. Without this, original ranks can be lost when
	// reranking reorders or filters results.
	resultsByID := make(map[string]*rrfResult, len(results))
	for i := range results {
		resultsByID[results[i].ID] = &results[i]
	}

	// Convert back to rrfResult format
	rerankedResults := make([]rrfResult, 0, len(reranked))
	for _, r := range reranked {
		original := resultsByID[r.ID]
		if original == nil {
			log.Printf("⚠️ Reranker returned ID %q not in pre-rerank results; preserving result with zero ranks", r.ID)
		}
		// Apply per-request MinScore filter if configured. Note that individual rerankers
		// may also apply their own MinScore internally.
		if opts.RerankMinScore > 0 && r.FinalScore < opts.RerankMinScore {
			continue
		}
		var vectorRank, bm25Rank int
		if original != nil {
			vectorRank, bm25Rank = original.VectorRank, original.BM25Rank
		}
		rerankedResults = append(rerankedResults, rrfResult{
			ID:            r.ID,
			RRFScore:      r.FinalScore, // Use cross-encoder score
			VectorRank:    vectorRank,
			BM25Rank:      bm25Rank,
			OriginalScore: r.BiScore,
		})
	}

	return rerankedResults
}

// SetCrossEncoder configures the Stage-2 reranker to use the cross-encoder implementation.
//
// Example:
//
//	svc := search.NewService(engine)
//	svc.SetCrossEncoder(search.NewCrossEncoder(&search.CrossEncoderConfig{
//		Enabled: true,
//		APIURL:  "http://localhost:8081/rerank",
//		Model:   "cross-encoder/ms-marco-MiniLM-L-6-v2",
//	}))
func (s *Service) SetCrossEncoder(ce *CrossEncoder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reranker = ce
}

// SetReranker configures the Stage-2 reranker.
// Uses a quick read-lock check to avoid write-lock contention when the value hasn't changed.
// This prevents deadlock when multiple goroutines call getOrCreateSearchService concurrently.
func (s *Service) SetReranker(r Reranker) {
	s.mu.RLock()
	current := s.reranker
	s.mu.RUnlock()
	// Skip write lock if value is already set to the same pointer (common case: nil -> nil or same instance).
	if current == r {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reranker = r
}

// CrossEncoderAvailable returns true if a cross-encoder reranker is configured and available.
func (s *Service) CrossEncoderAvailable(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.reranker == nil {
		return false
	}
	_, ok := s.reranker.(*CrossEncoder)
	if !ok {
		return false
	}
	return s.reranker.IsAvailable(ctx)
}

// RerankerAvailable returns true if Stage-2 reranking is configured and available.
func (s *Service) RerankerAvailable(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reranker != nil && s.reranker.IsAvailable(ctx)
}

// RerankCandidates applies the configured Stage-2 reranker to caller-provided candidates.
// This is a seam-friendly API for adapters that need rerank semantics without running retrieval.
func (s *Service) RerankCandidates(ctx context.Context, query string, candidates []RerankCandidate, opts *SearchOptions) ([]RerankResult, error) {
	if s == nil {
		return nil, fmt.Errorf("search service unavailable")
	}
	if opts == nil {
		opts = DefaultSearchOptions()
	}
	if len(candidates) == 0 {
		return []RerankResult{}, nil
	}

	s.mu.RLock()
	reranker := s.reranker
	s.mu.RUnlock()

	// No configured reranker: return pass-through order/scores.
	if reranker == nil || !reranker.Enabled() {
		out := make([]RerankResult, len(candidates))
		for i, c := range candidates {
			out[i] = RerankResult{
				ID:           c.ID,
				Content:      c.Content,
				OriginalRank: i + 1,
				NewRank:      i + 1,
				BiScore:      c.Score,
				CrossScore:   c.Score,
				FinalScore:   c.Score,
			}
		}
		return out, nil
	}

	topK := opts.RerankTopK
	if topK > 0 && len(candidates) > topK {
		candidates = candidates[:topK]
	}

	reranked, err := reranker.Rerank(ctx, query, candidates)
	if err != nil {
		return nil, err
	}

	if opts.RerankMinScore > 0 {
		filtered := make([]RerankResult, 0, len(reranked))
		for _, r := range reranked {
			if r.FinalScore >= opts.RerankMinScore {
				filtered = append(filtered, r)
			}
		}
		return filtered, nil
	}
	return reranked, nil
}

// vectorSearchOnly performs vector-only search.
// We do not hold s.mu across getOrCreateVectorPipeline() because it takes s.mu.RLock() internally;
// Go's RWMutex is not reentrant, so holding s.mu here would risk deadlock if a writer were waiting.
func (s *Service) vectorSearchOnly(ctx context.Context, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	totalStart := time.Now()
	searchStart := time.Now()
	pipeline, pipelineErr := s.getOrCreateVectorPipeline(ctx)
	if pipelineErr != nil {
		return nil, pipelineErr
	}
	scored, searchErr := pipeline.Search(ctx, embedding, vectorOverfetchLimit(opts.Limit), opts.GetMinSimilarity(0.5))
	if searchErr != nil {
		return nil, searchErr
	}

	var results []indexResult
	for _, r := range scored {
		results = append(results, indexResult{ID: r.ID, Score: r.Score})
	}
	searchMethod := "vector"
	message := "Vector similarity search (cosine)"

	switch pipeline.candidateGen.(type) {
	case *KMeansCandidateGen:
		searchMethod = "vector_clustered"
		message = "K-means routed vector search"
	case *IVFHNSWCandidateGen:
		searchMethod = "vector_ivf_hnsw"
		message = "IVF-HNSW (centroid routing + per-cluster HNSW)"
	case *IVFPQCandidateGen:
		searchMethod = "vector_ivfpq"
		message = "IVFPQ compressed ANN"
	case *GPUBruteForceCandidateGen:
		searchMethod = "vector_gpu_brute"
		message = "GPU brute-force vector search (exact)"
	case *HNSWCandidateGen:
		searchMethod = "vector_hnsw"
		message = "HNSW approximate nearest neighbor search"
	case *BruteForceCandidateGen:
		searchMethod = "vector_brute"
		message = "CPU brute-force vector search (exact)"
	}

	s.mu.RLock()
	clusterEnabled := s.clusterEnabled && s.clusterIndex != nil
	s.mu.RUnlock()
	if clusterEnabled {
		log.Printf("[K-MEANS] 🔍 SEARCH | mode=%s candidates=%d duration=%v",
			searchMethod, len(results), time.Since(searchStart))
	}

	// Collapse vector IDs back to unique node IDs.
	results = collapseIndexResultsByNodeID(results)

	seenOrphans := make(map[string]bool)
	if len(opts.Types) > 0 {
		results = s.filterByType(ctx, results, opts.Types, seenOrphans)
	}
	if len(opts.Filters) > 0 {
		results = s.filterByProperties(ctx, results, opts.Filters, seenOrphans)
	}

	searchResults := s.enrichIndexResults(ctx, results, opts.Limit, seenOrphans)
	// Vector-only: set vector_rank from position (1-based), bm25_rank = 0
	for i := range searchResults {
		searchResults[i].VectorRank = i + 1
		searchResults[i].BM25Rank = 0
	}
	vectorMs := int(time.Since(searchStart).Milliseconds())
	totalMs := int(time.Since(totalStart).Milliseconds())

	return &SearchResponse{
		Status:          "success",
		Results:         searchResults,
		TotalCandidates: len(results),
		Returned:        len(searchResults),
		SearchMethod:    searchMethod,
		Message:         message,
		Metrics: &SearchMetrics{
			VectorSearchTimeMs: vectorMs,
			TotalTimeMs:        totalMs,
			VectorCandidates:   len(results),
		},
	}, nil
}

// fullTextSearchOnly performs full-text BM25 search only.
func (s *Service) fullTextSearchOnly(ctx context.Context, query string, opts *SearchOptions) (*SearchResponse, error) {
	totalStart := time.Now()
	s.mu.RLock()
	ft := s.fulltextIndex
	s.mu.RUnlock()
	if ft == nil {
		return &SearchResponse{
			Status:            "success",
			Query:             query,
			Results:           nil,
			TotalCandidates:   0,
			Returned:          0,
			SearchMethod:      "fulltext",
			FallbackTriggered: true,
			Message:           "Full-text index not available",
			Metrics: &SearchMetrics{
				TotalTimeMs: int(time.Since(totalStart).Milliseconds()),
			},
		}, nil
	}
	bm25Start := time.Now()
	results := ft.Search(query, opts.Limit*2)
	bm25Ms := int(time.Since(bm25Start).Milliseconds())

	seenOrphans := make(map[string]bool)
	if len(opts.Types) > 0 {
		results = s.filterByType(ctx, results, opts.Types, seenOrphans)
	}
	if len(opts.Filters) > 0 {
		results = s.filterByProperties(ctx, results, opts.Filters, seenOrphans)
	}

	searchResults := s.enrichIndexResults(ctx, results, opts.Limit, seenOrphans)
	// Full-text only: set bm25_rank from position (1-based), vector_rank = 0
	for i := range searchResults {
		searchResults[i].VectorRank = 0
		searchResults[i].BM25Rank = i + 1
	}
	totalMs := int(time.Since(totalStart).Milliseconds())

	return &SearchResponse{
		Status:            "success",
		Query:             query,
		Results:           searchResults,
		TotalCandidates:   len(results),
		Returned:          len(searchResults),
		SearchMethod:      "fulltext",
		FallbackTriggered: true,
		Message:           "Full-text BM25 search (vector search unavailable or returned no results)",
		Metrics: &SearchMetrics{
			BM25SearchTimeMs: bm25Ms,
			TotalTimeMs:      totalMs,
			BM25Candidates:   len(results),
		},
	}, nil
}

func (s *Service) maybeLogSearchTiming(query string, resp *SearchResponse, elapsed time.Duration, cacheHit bool) {
	if !envutil.GetBoolStrict(EnvSearchLogTimings, false) || resp == nil {
		return
	}
	totalMs := int(elapsed.Milliseconds())
	vectorMs := 0
	bm25Ms := 0
	fusionMs := 0
	vectorCandidates := 0
	bm25Candidates := 0
	fusedCandidates := 0
	if resp.Metrics != nil {
		if resp.Metrics.TotalTimeMs > 0 {
			totalMs = resp.Metrics.TotalTimeMs
		}
		vectorMs = resp.Metrics.VectorSearchTimeMs
		bm25Ms = resp.Metrics.BM25SearchTimeMs
		fusionMs = resp.Metrics.FusionTimeMs
		vectorCandidates = resp.Metrics.VectorCandidates
		bm25Candidates = resp.Metrics.BM25Candidates
		fusedCandidates = resp.Metrics.FusedCandidates
	}
	queryPreview := strings.TrimSpace(query)
	if len(queryPreview) > 80 {
		queryPreview = queryPreview[:77] + "..."
	}
	log.Printf("⏱️ Search timing: method=%s cache_hit=%t fallback=%t total_ms=%d vector_ms=%d bm25_ms=%d fusion_ms=%d candidates[v=%d,b=%d,f=%d] returned=%d query=%q",
		resp.SearchMethod,
		cacheHit,
		resp.FallbackTriggered,
		totalMs,
		vectorMs,
		bm25Ms,
		fusionMs,
		vectorCandidates,
		bm25Candidates,
		fusedCandidates,
		resp.Returned,
		queryPreview)
}

// extractSearchableText extracts text from ALL node properties for full-text indexing.
// This includes:
//   - Node labels (for searching by type)
//   - All string properties
//   - String representations of other property types
//   - Priority properties (content, title, etc.) are included first for better ranking
func (s *Service) extractSearchableText(node *storage.Node) string {
	// Note: We now receive a stable copy of the node from IterateNodes,
	// so we can safely access its properties without additional locking.
	// The storage engine (AsyncEngine) makes copies during iteration to
	// prevent concurrent modification issues.

	var parts []string

	// 1. Add labels first (important for type-based search)
	for _, label := range node.Labels {
		parts = append(parts, label)
	}

	// 2. Add priority searchable properties first (better ranking for these)
	for _, prop := range SearchableProperties {
		if val, ok := node.Properties[prop]; ok {
			if str := propertyToString(val); str != "" {
				parts = append(parts, str)
			}
		}
	}

	// 3. Add ALL other properties (for comprehensive search)
	for key, val := range node.Properties {
		// Skip if already added as priority property
		if _, ok := searchablePropertiesSet[key]; ok {
			continue
		}
		// Add property name and value for searchability
		if str := propertyToString(val); str != "" {
			// Include property name to enable searches like "genre:action"
			parts = append(parts, key, str)
		}
	}

	return strings.Join(parts, " ")
}

// propertyToString converts any property value to a searchable string.
func propertyToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case []string:
		return strings.Join(v, " ")
	case []float32, []float64:
		// Avoid indexing dense numeric vectors as BM25 text.
		return ""
	case int, int64, int32, float64, float32:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []interface{}:
		if looksLikeDenseNumericSlice(v) {
			// Avoid indexing dense numeric arrays encoded as []interface{} (common in imports).
			// Treat these as vector-like payloads, not searchable BM25 text.
			return ""
		}
		var strs []string
		for _, item := range v {
			if s := propertyToString(item); s != "" {
				strs = append(strs, s)
			}
		}
		return strings.Join(strs, " ")
	default:
		// Skip complex objects/maps/arrays; indexing their fmt output is noisy and expensive.
		return ""
	}
}

func looksLikeDenseNumericSlice(v []interface{}) bool {
	if len(v) < 32 {
		return false
	}
	numeric := 0
	for _, item := range v {
		switch item.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			numeric++
		}
	}
	return float64(numeric)/float64(len(v)) >= 0.9
}

// vectorFromPropertyValue extracts a vector with the expected dimension.
// It avoids allocations for non-vector values and []float32 values.
func vectorFromPropertyValue(value any, expectedDim int) ([]float32, bool) {
	if expectedDim <= 0 {
		return nil, false
	}
	switch v := value.(type) {
	case []float32:
		if len(v) != expectedDim {
			return nil, false
		}
		return v, true
	case []float64:
		if len(v) != expectedDim {
			return nil, false
		}
		out := make([]float32, len(v))
		for i := range v {
			out[i] = float32(v[i])
		}
		return out, true
	case []any:
		if len(v) != expectedDim {
			return nil, false
		}
		out := make([]float32, len(v))
		for i := range v {
			switch n := v[i].(type) {
			case float32:
				out[i] = n
			case float64:
				out[i] = float32(n)
			case int:
				out[i] = float32(n)
			case int64:
				out[i] = float32(n)
			default:
				return nil, false
			}
		}
		return out, true
	default:
		return nil, false
	}
}

// nodeMatchesFilters reports whether node satisfies all filters (AND across keys, OR within values).
// Each filter value is matched against the property as a string; for array properties every
// element is checked individually.
func nodeMatchesFilters(node *storage.Node, filters map[string][]string) bool {
	for propName, wantVals := range filters {
		propVal, exists := node.Properties[propName]
		if !exists {
			return false
		}
		if !propValueMatchesAny(propVal, wantVals) {
			return false
		}
	}
	return true
}

// propValueMatchesAny returns true if any of wantVals matches propVal.
// For slice properties (any concrete slice type) every element is tested via
// fmt.Sprint; for scalar properties the fmt.Sprint representation is compared
// directly. Using reflect for the slice fallback means we handle whatever
// concrete type gob/msgpack produces ([]any, []string, []interface{}, etc.).
func propValueMatchesAny(propVal any, wantVals []string) bool {
	rv := reflect.ValueOf(propVal)
	if rv.Kind() == reflect.Slice {
		for i := 0; i < rv.Len(); i++ {
			elem := fmt.Sprint(rv.Index(i).Interface())
			for _, w := range wantVals {
				if elem == w {
					return true
				}
			}
		}
		return false
	}
	s := fmt.Sprint(propVal)
	for _, w := range wantVals {
		if s == w {
			return true
		}
	}
	return false
}

// filterByProperties filters results to only include nodes matching all property filters.
func (s *Service) filterByProperties(ctx context.Context, results []indexResult, filters map[string][]string, seenOrphans map[string]bool) []indexResult {
	if len(filters) == 0 {
		return results
	}
	var filtered []indexResult
	for _, r := range results {
		node, err := s.engine.GetNode(storage.NodeID(r.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, r.ID, err, seenOrphans) {
				continue
			}
			continue
		}
		if nodeMatchesFilters(node, filters) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// filterByType filters results to only include specified node types.
func (s *Service) filterByType(ctx context.Context, results []indexResult, types []string, seenOrphans map[string]bool) []indexResult {
	if len(types) == 0 {
		return results
	}

	typeSet := make(map[string]struct{})
	for _, t := range types {
		typeSet[strings.ToLower(t)] = struct{}{}
	}

	var filtered []indexResult
	for _, r := range results {
		node, err := s.engine.GetNode(storage.NodeID(r.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, r.ID, err, seenOrphans) {
				continue
			}
			continue
		}

		// Check if any label matches
		for _, label := range node.Labels {
			if _, ok := typeSet[strings.ToLower(label)]; ok {
				filtered = append(filtered, r)
				break
			}
		}

		// Also check type property
		if nodeType, ok := node.Properties["type"].(string); ok {
			if _, ok := typeSet[strings.ToLower(nodeType)]; ok {
				filtered = append(filtered, r)
			}
		}
	}

	return filtered
}

// enrichResults converts RRF results to SearchResult with full node data.
func (s *Service) enrichResults(ctx context.Context, rrfResults []rrfResult, limit int, seenOrphans map[string]bool) []SearchResult {
	var results []SearchResult

	for i, rrf := range rrfResults {
		if i >= limit {
			break
		}

		node, err := s.engine.GetNode(storage.NodeID(rrf.ID))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, rrf.ID, err, seenOrphans) {
				continue
			}
			continue
		}

		result := SearchResult{
			ID:         rrf.ID,
			NodeID:     node.ID,
			Labels:     node.Labels,
			Properties: node.Properties,
			Score:      rrf.RRFScore,
			Similarity: rrf.OriginalScore,
			RRFScore:   rrf.RRFScore,
			VectorRank: rrf.VectorRank,
			BM25Rank:   rrf.BM25Rank,
		}

		// Extract common fields
		if t, ok := node.Properties["type"].(string); ok {
			result.Type = t
		}
		if title, ok := node.Properties["title"].(string); ok {
			result.Title = title
		}
		if desc, ok := node.Properties["description"].(string); ok {
			result.Description = desc
		}
		if content, ok := node.Properties["content"].(string); ok {
			result.ContentPreview = truncate(content, 200)
		} else if text, ok := node.Properties["text"].(string); ok {
			result.ContentPreview = truncate(text, 200)
		}

		results = append(results, result)
	}

	return results
}

// enrichIndexResults converts raw index results to SearchResult.
// Maps chunk IDs (e.g., "node-id-chunk-0") back to the original node ID.
func (s *Service) enrichIndexResults(ctx context.Context, indexResults []indexResult, limit int, seenOrphans map[string]bool) []SearchResult {
	var results []SearchResult
	seenNodes := make(map[string]bool) // Track nodes we've already added to avoid duplicates

	for _, ir := range indexResults {
		if len(results) >= limit {
			break
		}

		nodeIDStr := normalizeVectorResultIDToNodeID(ir.ID)

		// Skip if we've already added this node (from a different chunk)
		if seenNodes[nodeIDStr] {
			continue
		}

		node, err := s.engine.GetNode(storage.NodeID(nodeIDStr))
		if err != nil {
			if s.handleOrphanedEmbedding(ctx, nodeIDStr, err, seenOrphans) {
				continue
			}
			continue
		}

		seenNodes[nodeIDStr] = true

		result := SearchResult{
			ID:         nodeIDStr, // Use original node ID, not chunk ID
			NodeID:     node.ID,
			Labels:     node.Labels,
			Properties: node.Properties,
			Score:      ir.Score,
			Similarity: ir.Score,
		}

		// Extract common fields
		if t, ok := node.Properties["type"].(string); ok {
			result.Type = t
		}
		if title, ok := node.Properties["title"].(string); ok {
			result.Title = title
		}
		if desc, ok := node.Properties["description"].(string); ok {
			result.Description = desc
		}
		if content, ok := node.Properties["content"].(string); ok {
			result.ContentPreview = truncate(content, 200)
		} else if text, ok := node.Properties["text"].(string); ok {
			result.ContentPreview = truncate(text, 200)
		}

		results = append(results, result)
	}

	return results
}

// GetAdaptiveRRFConfig returns optimized RRF weights based on query characteristics.
//
// This function analyzes the query and adjusts weights to favor the search method
// most likely to perform well:
//
//   - Short queries (1-2 words): Favor BM25 keyword matching
//     Example: "python" or "graph database"
//     Weights: Vector=0.5, BM25=1.5
//
//   - Long queries (6+ words): Favor vector semantic understanding
//     Example: "How do I implement a distributed consensus algorithm?"
//     Weights: Vector=1.5, BM25=0.5
//
//   - Medium queries (3-5 words): Balanced approach
//     Example: "machine learning algorithms"
//     Weights: Vector=1.0, BM25=1.0
//
// Why this works:
//   - Short queries lack context → keywords more reliable
//   - Long queries have semantic meaning → embeddings capture intent better
//
// Example:
//
//	// Automatic adaptation
//	query1 := "database"
//	opts1 := search.GetAdaptiveRRFConfig(query1)
//	fmt.Printf("Short query weights: V=%.1f, B=%.1f\n",
//		opts1.VectorWeight, opts1.BM25Weight)
//	// Output: V=0.5, B=1.5 (favors keywords)
//
//	query2 := "What are the best practices for scaling graph databases?"
//	opts2 := search.GetAdaptiveRRFConfig(query2)
//	fmt.Printf("Long query weights: V=%.1f, B=%.1f\n",
//		opts2.VectorWeight, opts2.BM25Weight)
//	// Output: V=1.5, B=0.5 (favors semantics)
//
// Returns SearchOptions with adapted weights. Other options (Limit, MinSimilarity)
// are set to defaults.
func GetAdaptiveRRFConfig(query string) *SearchOptions {
	words := strings.Fields(query)
	wordCount := len(words)

	opts := DefaultSearchOptions()

	// Short queries (1-2 words): Emphasize keyword matching
	if wordCount <= 2 {
		opts.VectorWeight = 0.5
		opts.BM25Weight = 1.5
		return opts
	}

	// Long queries (6+ words): Emphasize semantic understanding
	if wordCount >= 6 {
		opts.VectorWeight = 1.5
		opts.BM25Weight = 0.5
		return opts
	}

	// Medium queries: Balanced
	return opts
}

// Helper types
type indexResult struct {
	ID    string
	Score float64
}

type rrfResult struct {
	ID            string
	RRFScore      float64
	VectorRank    int
	BM25Rank      int
	OriginalScore float64
}

func findResultIndex(results []indexResult, id string) int {
	for i, r := range results {
		if r.ID == id {
			return i
		}
	}
	return -1
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	// Handle edge cases where maxLen is too small for ellipsis
	if maxLen <= 3 {
		if maxLen <= 0 {
			return ""
		}
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
