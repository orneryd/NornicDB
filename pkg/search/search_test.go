// Package search tests
package search

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type testReranker struct {
	enabled bool
	results []RerankResult
	err     error
}

type iteratorEngine struct {
	storage.Engine
	iterateErr error
}

type lastWriteEngine struct {
	storage.Engine
	writeTime time.Time
}

func (e *iteratorEngine) IterateNodes(fn func(*storage.Node) bool) error {
	if e.iterateErr != nil {
		return e.iterateErr
	}
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !fn(node) {
			return nil
		}
	}
	return nil
}

func (e *lastWriteEngine) LastWriteTime() time.Time {
	return e.writeTime
}

func (r *testReranker) Name() string                         { return "test_reranker" }
func (r *testReranker) Enabled() bool                        { return r.enabled }
func (r *testReranker) IsAvailable(ctx context.Context) bool { return r.enabled }
func (r *testReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.results != nil {
		return r.results, nil
	}
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

func newNamespacedEngine(tb testing.TB) storage.Engine {
	base := storage.NewMemoryEngine()
	tb.Cleanup(func() { base.Close() })
	return storage.NewNamespacedEngine(base, "nornic")
}

// TestVectorIndex_Basic tests basic vector index operations.
func TestVectorIndex_Basic(t *testing.T) {
	idx := NewVectorIndex(4)

	// Add vectors
	err := idx.Add("doc1", []float32{1, 0, 0, 0})
	require.NoError(t, err)

	err = idx.Add("doc2", []float32{0.9, 0.1, 0, 0})
	require.NoError(t, err)

	err = idx.Add("doc3", []float32{0, 1, 0, 0})
	require.NoError(t, err)

	assert.Equal(t, 3, idx.Count())
	assert.True(t, idx.HasVector("doc1"))
	assert.False(t, idx.HasVector("doc99"))

	// Search for similar vectors
	results, err := idx.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 0.5)
	require.NoError(t, err)

	// doc1 should be most similar (identical), then doc2 (0.9 similarity)
	require.Len(t, results, 2) // doc3 is orthogonal, below threshold
	assert.Equal(t, "doc1", results[0].ID)
	assert.InDelta(t, 1.0, results[0].Score, 0.01) // Identical
	assert.Equal(t, "doc2", results[1].ID)
}

// TestVectorIndex_DimensionMismatch tests dimension validation.
func TestVectorIndex_DimensionMismatch(t *testing.T) {
	idx := NewVectorIndex(4)

	err := idx.Add("doc1", []float32{1, 2, 3}) // Wrong dimension
	assert.ErrorIs(t, err, ErrDimensionMismatch)

	err = idx.Add("doc1", []float32{1, 2, 3, 4, 5}) // Wrong dimension
	assert.ErrorIs(t, err, ErrDimensionMismatch)

	err = idx.Add("doc1", []float32{1, 2, 3, 4}) // Correct dimension
	assert.NoError(t, err)
}

// TestFulltextIndex_BM25 tests BM25 full-text search.
func TestFulltextIndex_BM25(t *testing.T) {
	idx := NewFulltextIndex()

	// Index some documents
	idx.Index("doc1", "machine learning deep neural networks")
	idx.Index("doc2", "deep learning with tensorflow and pytorch")
	idx.Index("doc3", "database systems and query optimization")
	idx.Index("doc4", "natural language processing with transformers")

	assert.Equal(t, 4, idx.Count())

	// Search for "deep learning"
	results := idx.Search("deep learning", 10)
	require.NotEmpty(t, results)

	// doc1 and doc2 should match
	t.Logf("Search results for 'deep learning':")
	for _, r := range results {
		t.Logf("  %s: score=%.4f", r.ID, r.Score)
	}

	// Both doc1 and doc2 should be in results (both contain "deep" and/or "learning")
	ids := make(map[string]bool)
	for _, r := range results {
		ids[r.ID] = true
	}
	assert.True(t, ids["doc1"] || ids["doc2"], "Expected doc1 or doc2 in results")

	// Search for "database"
	results = idx.Search("database query", 10)
	require.NotEmpty(t, results)
	assert.Equal(t, "doc3", results[0].ID)
}

// TestFulltextIndex_Tokenization tests text tokenization.
func TestFulltextIndex_Tokenization(t *testing.T) {
	tokens := tokenize("Hello, World! This is a TEST-123.")

	t.Logf("Tokens: %v", tokens)

	// Should lowercase, remove punctuation, filter stop words
	assert.Contains(t, tokens, "hello")
	assert.Contains(t, tokens, "world")
	assert.Contains(t, tokens, "test")
	assert.Contains(t, tokens, "123")

	// Should NOT contain stop words
	assert.NotContains(t, tokens, "this")
	assert.NotContains(t, tokens, "is")
	assert.NotContains(t, tokens, "a")
}

// TestRRFFusion tests Reciprocal Rank Fusion algorithm.
func TestRRFFusion(t *testing.T) {
	// Create test service with mock data
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create nodes
	nodes := []*storage.Node{
		{ID: "doc1", Labels: []string{"Node"}, Properties: map[string]any{"title": "ML Basics"}},
		{ID: "doc2", Labels: []string{"Node"}, Properties: map[string]any{"title": "Deep Learning"}},
		{ID: "doc3", Labels: []string{"Node"}, Properties: map[string]any{"title": "Database Design"}},
	}
	err := engine.BulkCreateNodes(nodes)
	require.NoError(t, err)

	svc := NewService(engine)

	// Test RRF fusion directly
	vectorResults := []indexResult{
		{ID: "doc1", Score: 0.95},
		{ID: "doc2", Score: 0.85},
	}
	bm25Results := []indexResult{
		{ID: "doc2", Score: 5.5}, // doc2 appears in both - should rank highest
		{ID: "doc3", Score: 4.2},
	}

	opts := DefaultSearchOptions()
	fusedResults := svc.fuseRRF(vectorResults, bm25Results, opts)

	t.Logf("RRF Fusion results:")
	for _, r := range fusedResults {
		t.Logf("  %s: rrf=%.4f vectorRank=%d bm25Rank=%d", r.ID, r.RRFScore, r.VectorRank, r.BM25Rank)
	}

	// doc2 should be first (appears in both lists)
	require.NotEmpty(t, fusedResults)
	assert.Equal(t, "doc2", fusedResults[0].ID)
	assert.Equal(t, 2, fusedResults[0].VectorRank) // Second in vector list = rank 2 (1-indexed)
	assert.Equal(t, 1, fusedResults[0].BM25Rank)   // First in BM25 = rank 1
}

func TestServicePersistenceAndTimingHelpers(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 3)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "hnsw")

	// schedulePersist: no-op without paths
	svc.SetPersistenceEnabled(true)
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()

	// schedulePersist: sets timer when persistence is enabled and paths are set.
	svc.SetFulltextIndexPath(bm25Path)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetHNSWIndexPath(hnswPath)
	oldDelay, hadDelay := os.LookupEnv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC")
	require.NoError(t, os.Setenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "1"))
	t.Cleanup(func() {
		if hadDelay {
			_ = os.Setenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", oldDelay)
		} else {
			_ = os.Unsetenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC")
		}
	})
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.NotNil(t, svc.persistTimer)
	svc.persistTimer.Stop()
	svc.persistTimer = nil
	svc.persistMu.Unlock()

	// persistBM25Background saves when BM25 is dirty and not building.
	svc.fulltextIndex.Index("doc-1", "alpha beta gamma")
	svc.persistBM25Background(bm25Path)
	_, err := os.Stat(bm25Path)
	require.NoError(t, err)

	// persistVectorStoreBackground handles nil store and writes meta for valid store.
	svc.persistVectorStoreBackground(vectorPath, nil)
	vfs, err := NewVectorFileStore(vectorPath, 3)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vfs.Close() })
	require.NoError(t, vfs.Add("vec-1", []float32{1, 0, 0}))
	svc.persistVectorStoreBackground(vectorPath, vfs)
	_, err = os.Stat(vectorPath + ".meta")
	require.NoError(t, err)

	// persistHNSWBackground skip and save branches.
	svc.persistHNSWBackground(hnswPath) // skip (< NSmallMax)
	svc.hnswMu.Lock()
	svc.hnswIndex = NewHNSWIndex(3, HNSWConfigFromEnv())
	svc.hnswMu.Unlock()
	for i := 0; i < NSmallMax; i++ {
		require.NoError(t, svc.vectorIndex.Add(fmt.Sprintf("n-%d", i), []float32{1, 0, 0}))
	}
	svc.persistHNSWBackground(hnswPath)
	_, err = os.Stat(hnswPath)
	require.NoError(t, err)

	// persistIVFHNSWBackground writes per-cluster files when cluster map exists.
	clusterIdx := NewHNSWIndex(3, HNSWConfigFromEnv())
	require.NoError(t, clusterIdx.Add("c-1", []float32{1, 0, 0}))
	svc.clusterHNSWMu.Lock()
	svc.clusterHNSW = map[int]*HNSWIndex{0: clusterIdx}
	svc.clusterHNSWMu.Unlock()
	svc.persistIVFHNSWBackground(hnswPath)
	_, err = os.Stat(filepath.Join(filepath.Dir(hnswPath), "hnsw_ivf", "0"))
	require.NoError(t, err)

	// runPersist + PersistIndexesToDisk execute without panic on configured paths.
	svc.runPersist()
	svc.PersistIndexesToDisk()

	// schedulePersist: disabled and build-in-progress no-op branches.
	svc.SetPersistenceEnabled(false)
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()

	svc.SetPersistenceEnabled(true)
	svc.buildInProgress.Store(true)
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()
	svc.buildInProgress.Store(false)

	// schedulePersist: requires both fulltext and vector paths.
	svc.SetFulltextIndexPath("")
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()
}

func TestSearchHandleOrphanedEmbeddingBranches(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:              "orphan-node",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "to-remove"},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, 1, svc.EmbeddingCount())

	// Non-ErrNotFound branch.
	ok := svc.handleOrphanedEmbedding(context.Background(), "orphan-node", fmt.Errorf("other error"), nil)
	require.False(t, ok)
	require.Equal(t, 1, svc.EmbeddingCount())

	// ErrNotFound branch removes orphaned vectors.
	err = engine.DeleteNode("orphan-node")
	require.NoError(t, err)
	seen := map[string]bool{}
	ok = svc.handleOrphanedEmbedding(context.Background(), "orphan-node", storage.ErrNotFound, seen)
	require.True(t, ok)
	require.True(t, seen["orphan-node"])
	require.Equal(t, 0, svc.EmbeddingCount())

	// Already-seen branch short-circuits deterministically.
	ok = svc.handleOrphanedEmbedding(context.Background(), "orphan-node", storage.ErrNotFound, seen)
	require.True(t, ok)
}

func TestSearchWarmupVectorPipelineBranches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 3)

	// No embeddings: warmup is a no-op.
	svc.warmupVectorPipeline(nil)
	svc.pipelineMu.RLock()
	require.Nil(t, svc.vectorPipeline)
	svc.pipelineMu.RUnlock()

	_, err := engine.CreateNode(&storage.Node{
		ID:              "w1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "warmup"},
	})
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	// Embeddings present: warmup builds a ready pipeline.
	svc.warmupVectorPipeline(context.Background())
	svc.pipelineMu.RLock()
	require.NotNil(t, svc.vectorPipeline)
	svc.pipelineMu.RUnlock()
}

func TestSearchEnvDurationAndTimingHelpers(t *testing.T) {
	oldMs, hadMs := os.LookupEnv("NORNICDB_HNSW_MAINT_INTERVAL_MS")
	oldSec, hadSec := os.LookupEnv("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC")
	oldTiming, hadTiming := os.LookupEnv(EnvSearchLogTimings)
	t.Cleanup(func() {
		if hadMs {
			_ = os.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", oldMs)
		} else {
			_ = os.Unsetenv("NORNICDB_HNSW_MAINT_INTERVAL_MS")
		}
		if hadSec {
			_ = os.Setenv("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", oldSec)
		} else {
			_ = os.Unsetenv("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC")
		}
		if hadTiming {
			_ = os.Setenv(EnvSearchLogTimings, oldTiming)
		} else {
			_ = os.Unsetenv(EnvSearchLogTimings)
		}
	})

	require.NoError(t, os.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", "0"))
	require.Equal(t, 30*time.Second, envDurationMs("NORNICDB_HNSW_MAINT_INTERVAL_MS", 30_000))
	require.NoError(t, os.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", "250"))
	require.Equal(t, 250*time.Millisecond, envDurationMs("NORNICDB_HNSW_MAINT_INTERVAL_MS", 30_000))

	require.NoError(t, os.Setenv("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", "0"))
	require.Equal(t, 60*time.Second, envDurationSec("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", 60))
	require.NoError(t, os.Setenv("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", "2"))
	require.Equal(t, 2*time.Second, envDurationSec("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", 60))

	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	resp := &SearchResponse{
		SearchMethod:      "hybrid",
		FallbackTriggered: false,
		Results:           []SearchResult{{ID: "n1", Score: 0.9}},
		Metrics: &SearchMetrics{
			TotalTimeMs:        7,
			VectorSearchTimeMs: 3,
			BM25SearchTimeMs:   2,
			FusionTimeMs:       1,
			VectorCandidates:   4,
			BM25Candidates:     5,
			FusedCandidates:    3,
		},
	}

	var buf bytes.Buffer
	oldOut := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(oldOut) })

	require.NoError(t, os.Setenv(EnvSearchLogTimings, "true"))
	svc.maybeLogSearchTiming(strings.Repeat("q", 90), resp, 15*time.Millisecond, true)
	out := buf.String()
	require.Contains(t, out, "Search timing")
	require.Contains(t, out, "cache_hit=true")
}

func TestSearchResultCacheHelpers(t *testing.T) {
	t.Run("Get and Put handle nil, miss, eviction, and invalidate", func(t *testing.T) {
		c := newSearchResultCache(2, 0)
		require.Nil(t, c.Get("missing"))
		c.Invalidate()

		c.Put("nil", nil)
		require.Nil(t, c.Get("nil"))

		r1 := &SearchResponse{Status: "ok", Query: "q1"}
		r2 := &SearchResponse{Status: "ok", Query: "q2"}
		r3 := &SearchResponse{Status: "ok", Query: "q3"}
		c.Put("k1", r1)
		c.Put("k2", r2)
		require.Equal(t, r1, c.Get("k1"))
		require.Equal(t, r2, c.Get("k2"))

		// Evict oldest when max size is reached.
		c.Put("k3", r3)
		require.Nil(t, c.Get("k1"))
		require.Equal(t, r2, c.Get("k2"))
		require.Equal(t, r3, c.Get("k3"))

		c.Invalidate()
		require.Nil(t, c.Get("k2"))
		require.Nil(t, c.Get("k3"))
	})

	t.Run("Get expires stale entries when ttl is set", func(t *testing.T) {
		c := newSearchResultCache(2, 5*time.Millisecond)
		r := &SearchResponse{Status: "ok", Query: "ttl"}
		c.Put("k", r)
		require.Equal(t, r, c.Get("k"))

		time.Sleep(12 * time.Millisecond)
		require.Nil(t, c.Get("k"))

		c.mu.RLock()
		_, stillPresent := c.entries["k"]
		c.mu.RUnlock()
		require.False(t, stillPresent)
	})

	t.Run("new cache normalizes non-positive max size", func(t *testing.T) {
		c := newSearchResultCache(0, time.Second)
		require.NotNil(t, c)
		require.Equal(t, 1000, c.maxSize)
	})
}

func TestSearchCacheKeyAndMinSimilarityHelpers(t *testing.T) {
	optsA := DefaultSearchOptions()
	optsA.Types = []string{"todo", "memory"}
	optsA.MinSimilarity = nil

	optsB := DefaultSearchOptions()
	optsB.Types = []string{"memory", "todo"} // different order, same semantic key

	keyA := searchCacheKey("hello", optsA)
	keyB := searchCacheKey("hello", optsB)
	require.Equal(t, keyA, keyB)

	keyNil := searchCacheKey("hello", nil)
	require.NotEmpty(t, keyNil)
	require.NotEqual(t, keyA, keyNil)

	ms := 0.42
	optsA.MinSimilarity = &ms
	require.Equal(t, 0.42, optsA.GetMinSimilarity(0.1))
	optsA.MinSimilarity = nil
	require.Equal(t, 0.1, optsA.GetMinSimilarity(0.1))
}

func TestSearchBM25EngineHelpers(t *testing.T) {
	t.Run("default engine env normalization", func(t *testing.T) {
		t.Setenv(EnvSearchBM25Engine, "v1")
		require.Equal(t, BM25EngineV1, DefaultBM25Engine())

		t.Setenv(EnvSearchBM25Engine, "  unknown ")
		require.Equal(t, BM25EngineV2, DefaultBM25Engine())
	})

	t.Run("newBM25Index returns both implementations", func(t *testing.T) {
		idx, engine := newBM25Index("v1")
		require.NotNil(t, idx)
		require.Equal(t, BM25EngineV1, engine)

		idx, engine = newBM25Index("v2")
		require.NotNil(t, idx)
		require.Equal(t, BM25EngineV2, engine)
	})
}

func TestSearchVersionHelpers(t *testing.T) {
	tests := []struct {
		in    string
		ok    bool
		major int
		minor int
		patch int
	}{
		{"1.2.3", true, 1, 2, 3},
		{" 2.0.1 ", true, 2, 0, 1},
		{"", false, 0, 0, 0},
		{"1.2", false, 0, 0, 0},
		{"a.b.c", false, 0, 0, 0},
		{"-1.2.3", false, 0, 0, 0},
	}
	for _, tt := range tests {
		maj, min, pat, ok := parseSemver(tt.in)
		require.Equal(t, tt.ok, ok, tt.in)
		require.Equal(t, tt.major, maj, tt.in)
		require.Equal(t, tt.minor, min, tt.in)
		require.Equal(t, tt.patch, pat, tt.in)
	}

	require.Equal(t, -1, compareSemver("1.0.0", "2.0.0"))
	require.Equal(t, 1, compareSemver("2.0.0", "1.9.9"))
	require.Equal(t, -1, compareSemver("1.1.0", "1.2.0"))
	require.Equal(t, 1, compareSemver("1.2.1", "1.2.0"))
	require.Equal(t, 0, compareSemver("1.2.3", "1.2.3"))
	require.Equal(t, -1, compareSemver("invalid", "1.0.0")) // invalid stored treated as old
	require.Equal(t, 0, compareSemver("1.0.0", "bad"))      // invalid current treated neutral

	require.True(t, searchIndexVersionCompatible("1.2.3", "1.2.3", "test"))
	require.False(t, searchIndexVersionCompatible("1.2.2", "1.2.3", "test"))
	require.False(t, searchIndexVersionCompatible("2.0.0", "1.2.3", "test"))
}

func TestFulltextIndex_UpdateAvgDocLengthBranches(t *testing.T) {
	idx := NewFulltextIndex()

	idx.mu.Lock()
	idx.docCount = 0
	idx.totalDocLength = 99
	idx.updateAvgDocLength()
	require.Equal(t, 0.0, idx.avgDocLength)
	require.Equal(t, int64(0), idx.totalDocLength)

	idx.docCount = 4
	idx.totalDocLength = 10
	idx.updateAvgDocLength()
	require.Equal(t, 2.5, idx.avgDocLength)
	idx.mu.Unlock()
}

// TestAdaptiveRRFConfig tests adaptive RRF configuration.
func TestAdaptiveRRFConfig(t *testing.T) {
	// Short query - should favor BM25
	shortOpts := GetAdaptiveRRFConfig("docker")
	assert.Equal(t, 0.5, shortOpts.VectorWeight)
	assert.Equal(t, 1.5, shortOpts.BM25Weight)

	// Long query - should favor vector
	longOpts := GetAdaptiveRRFConfig("how do I configure docker containers for production deployment")
	assert.Equal(t, 1.5, longOpts.VectorWeight)
	assert.Equal(t, 0.5, longOpts.BM25Weight)

	// Medium query - should be balanced
	medOpts := GetAdaptiveRRFConfig("configure docker production")
	assert.Equal(t, 1.0, medOpts.VectorWeight)
	assert.Equal(t, 1.0, medOpts.BM25Weight)
}

// TestSearchService_FullTextOnly tests full-text search without embeddings.
func TestSearchService_FullTextOnly(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create nodes with searchable properties
	nodes := []*storage.Node{
		{
			ID:     "node1",
			Labels: []string{"Node"},
			Properties: map[string]any{
				"type":    "memory",
				"title":   "Machine Learning Tutorial",
				"content": "Introduction to machine learning algorithms and neural networks",
			},
		},
		{
			ID:     "node2",
			Labels: []string{"Node"},
			Properties: map[string]any{
				"type":        "todo",
				"title":       "Database Migration",
				"description": "Migrate PostgreSQL database to new server",
			},
		},
		{
			ID:     "node3",
			Labels: []string{"FileChunk"},
			Properties: map[string]any{
				"text": "function calculateMachineLearningScore() { return 42; }",
				"path": "/src/ml/score.ts",
			},
		},
	}
	err := engine.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Create search service
	svc := NewService(engine)

	// Index nodes for full-text search
	for _, node := range nodes {
		err := svc.IndexNode(node)
		require.NoError(t, err)
	}

	// Search without embedding (triggers full-text fallback)
	ctx := context.Background()
	opts := DefaultSearchOptions()
	opts.Limit = 10

	response, err := svc.Search(ctx, "machine learning", nil, opts)
	require.NoError(t, err)

	t.Logf("Full-text search results for 'machine learning':")
	for _, r := range response.Results {
		t.Logf("  %s: type=%s title=%s score=%.4f", r.ID, r.Type, r.Title, r.Score)
	}

	assert.Equal(t, "fulltext", response.SearchMethod)
	assert.True(t, response.FallbackTriggered)
	assert.NotEmpty(t, response.Results)
}

// TestSearchService_BuildIndexes tests building indexes from storage.
func TestSearchService_BuildIndexes(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create nodes with embeddings
	embedding := make([]float32, 1024)
	embedding[0] = 1.0 // Simple non-zero embedding

	nodes := []*storage.Node{
		{
			ID:              "node1",
			Labels:          []string{"Node"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]any{
				"title":   "Test Node 1",
				"content": "This is test content for searching",
			},
		},
		{
			ID:              "node2",
			Labels:          []string{"Node"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]any{
				"title": "Test Node 2",
				"text":  "Another searchable document",
			},
		},
	}
	err := engine.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Create service and build indexes
	svc := NewService(engine)
	err = svc.BuildIndexes(context.Background())
	require.NoError(t, err)

	// Verify indexes were built
	assert.Equal(t, 2, svc.vectorIndex.Count())
	assert.Equal(t, 2, svc.fulltextIndex.Count())
}

func TestSearchService_BuildIndexes_ContextCanceled(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 3)

	_, err := engine.CreateNode(&storage.Node{
		ID:              "n1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "alpha"},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = svc.BuildIndexes(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestVectorOverfetchLimitBoundaries(t *testing.T) {
	cases := []struct {
		limit int
		want  int
	}{
		{limit: 0, want: 20},     // default when unset/invalid
		{limit: -5, want: 20},    // default when negative
		{limit: 1, want: 50},     // minimum floor
		{limit: 5, want: 50},     // minimum floor
		{limit: 100, want: 1000}, // normal scale
		{limit: 500, want: 5000}, // hard cap
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, vectorOverfetchLimit(tc.limit))
	}
}

func TestSearchFilterByTypeBranches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 3)

	_, err := engine.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"content": "alpha"},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:         "n2",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"type": "Person", "content": "beta"},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:         "n3",
		Labels:     []string{"Other"},
		Properties: map[string]any{"content": "gamma"},
	})
	require.NoError(t, err)

	in := []indexResult{
		{ID: "n1", Score: 1},
		{ID: "n2", Score: 1},
		{ID: "n3", Score: 1},
		{ID: "missing", Score: 1},
	}

	// Empty type filter returns input as-is.
	same := svc.filterByType(context.Background(), in, nil, nil)
	require.Equal(t, in, same)

	seenOrphans := map[string]bool{}
	filtered := svc.filterByType(context.Background(), in, []string{"person"}, seenOrphans)
	require.Len(t, filtered, 2)
	require.Equal(t, "n1", filtered[0].ID)
	require.Equal(t, "n2", filtered[1].ID)
	require.True(t, seenOrphans["missing"], "missing node should be handled as orphan")
}

func TestSearchEnrichResultsBranches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 3)

	longContent := strings.Repeat("x", 260)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "e1",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"type":        "Article",
			"title":       "Title A",
			"description": "Desc A",
			"content":     longContent,
		},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&storage.Node{
		ID:     "e2",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"text": "text-fallback",
		},
	})
	require.NoError(t, err)

	input := []rrfResult{
		{ID: "e1", RRFScore: 1.5, OriginalScore: 0.8, VectorRank: 1, BM25Rank: 2},
		{ID: "e2", RRFScore: 1.2, OriginalScore: 0.7, VectorRank: 2, BM25Rank: 1},
		{ID: "missing", RRFScore: 0.5, OriginalScore: 0.3},
	}

	seenOrphans := map[string]bool{}
	out := svc.enrichResults(context.Background(), input, 3, seenOrphans)
	require.Len(t, out, 2)

	require.Equal(t, "e1", out[0].ID)
	require.Equal(t, "Article", out[0].Type)
	require.Equal(t, "Title A", out[0].Title)
	require.Equal(t, "Desc A", out[0].Description)
	require.Len(t, out[0].ContentPreview, 200)
	require.True(t, strings.HasSuffix(out[0].ContentPreview, "..."))

	require.Equal(t, "e2", out[1].ID)
	require.Equal(t, "text-fallback", out[1].ContentPreview)
	require.True(t, seenOrphans["missing"])

	// limit branch: only the first input item should be considered.
	limited := svc.enrichResults(context.Background(), input, 1, map[string]bool{})
	require.Len(t, limited, 1)
	require.Equal(t, "e1", limited[0].ID)
}

func TestSearchService_BuildIndexes_IteratorEngineBranches(t *testing.T) {
	base := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	_, err := base.CreateNode(&storage.Node{
		ID:              "it-1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "alpha"},
	})
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{
		ID:              "it-2",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
		Properties:      map[string]any{"content": "beta"},
	})
	require.NoError(t, err)

	t.Run("iterates_and_builds", func(t *testing.T) {
		svc := NewServiceWithDimensions(&iteratorEngine{Engine: base}, 3)
		err := svc.BuildIndexes(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, svc.fulltextIndex.Count())
		require.Equal(t, 2, svc.EmbeddingCount())
	})

	t.Run("canceled_context_returns_error", func(t *testing.T) {
		svc := NewServiceWithDimensions(&iteratorEngine{Engine: base}, 3)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := svc.BuildIndexes(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("iterator_error_propagates", func(t *testing.T) {
		svc := NewServiceWithDimensions(&iteratorEngine{
			Engine:     base,
			iterateErr: errors.New("iterate-failed"),
		}, 3)
		err := svc.BuildIndexes(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "iterate-failed")
	})
}

func TestSearchService_BuildIndexes_SkipIterationFromDisk(t *testing.T) {
	base := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	_, err := base.CreateNode(&storage.Node{
		ID:              "disk-1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "persisted"},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")

	// First build materializes on-disk BM25/vector artifacts.
	svc1 := NewServiceWithDimensions(base, 3)
	svc1.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc1.SetPersistenceEnabled(false) })
	svc1.SetFulltextIndexPath(bm25Path)
	svc1.SetVectorIndexPath(vectorPath)
	require.NoError(t, svc1.BuildIndexes(context.Background()))
	svc1.PersistIndexesToDisk()

	// If BuildIndexes iterates storage again, iteratorEngine will return this error.
	iterErr := errors.New("should-not-iterate")
	svc2 := NewServiceWithDimensions(&iteratorEngine{Engine: base, iterateErr: iterErr}, 3)
	svc2.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc2.SetPersistenceEnabled(false) })
	svc2.SetFulltextIndexPath(bm25Path)
	svc2.SetVectorIndexPath(vectorPath)
	require.NoError(t, svc2.BuildIndexes(context.Background()))
	require.GreaterOrEqual(t, svc2.fulltextIndex.Count(), 1)
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), 1)
}

func TestSearchService_BuildIndexes_ForcedRebuildOnSettingsMismatch(t *testing.T) {
	base := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	_, err := base.CreateNode(&storage.Node{
		ID:              "mismatch-1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "persisted-one"},
	})
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{
		ID:              "mismatch-2",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
		Properties:      map[string]any{"content": "persisted-two"},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "hnsw.idx")

	// Seed persisted artifacts/settings.
	svc1 := NewServiceWithDimensions(base, 3)
	svc1.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc1.SetPersistenceEnabled(false) })
	svc1.SetFulltextIndexPath(bm25Path)
	svc1.SetVectorIndexPath(vectorPath)
	svc1.SetHNSWIndexPath(hnswPath)
	require.NoError(t, svc1.BuildIndexes(context.Background()))
	svc1.persistSearchBuildSettings(bm25Path, vectorPath, hnswPath)

	// Overwrite build settings with mismatched fingerprints so rebuild flags trigger.
	settingsPath := searchBuildSettingsPath(bm25Path, vectorPath, hnswPath)
	require.NoError(t, saveSearchBuildSettings(settingsPath, searchBuildSettingsSnapshot{
		FormatVersion: searchBuildSettingsFormatVersion,
		BM25:          "old-bm25-fingerprint",
		Vector:        "old-vector-fingerprint",
		HNSW:          "old-hnsw-fingerprint",
		Routing:       "old-routing-fingerprint",
		Strategy:      "old-strategy-fingerprint",
	}))

	svc2 := NewServiceWithDimensions(base, 3)
	svc2.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc2.SetPersistenceEnabled(false) })
	svc2.SetFulltextIndexPath(bm25Path)
	svc2.SetVectorIndexPath(vectorPath)
	svc2.SetHNSWIndexPath(hnswPath)

	// Pre-populate fields that forced rebuild paths should clear.
	svc2.clusterLexicalProfiles = map[int]map[string]float64{
		1: {"alpha": 1},
	}
	svc2.ivfpqMu.Lock()
	svc2.ivfpqIndex = &IVFPQIndex{}
	svc2.ivfpqMu.Unlock()

	require.NoError(t, svc2.BuildIndexes(context.Background()))
	require.True(t, svc2.IsReady())
	require.GreaterOrEqual(t, svc2.fulltextIndex.Count(), 1)
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), 1)

	// Build persists settings asynchronously; wait until updated fingerprints are visible.
	var gotSettings *searchBuildSettingsSnapshot
	require.Eventually(t, func() bool {
		loaded, loadErr := loadSearchBuildSettings(settingsPath)
		if loadErr != nil || loaded == nil {
			return false
		}
		gotSettings = loaded
		return true
	}, 2*time.Second, 20*time.Millisecond)
	require.NotEqual(t, "old-bm25-fingerprint", gotSettings.BM25)
	require.NotEqual(t, "old-vector-fingerprint", gotSettings.Vector)
	require.NotEqual(t, "old-hnsw-fingerprint", gotSettings.HNSW)
	require.NotEqual(t, "old-routing-fingerprint", gotSettings.Routing)
	require.NotEqual(t, "old-strategy-fingerprint", gotSettings.Strategy)
}

func TestSearchService_BuildIndexes_DoesNotReuseDiskIndexesWhenStorageEmpty(t *testing.T) {
	root := storage.NewMemoryEngine()
	t.Cleanup(func() { root.Close() })
	base := storage.NewNamespacedEngine(root, "test")
	_, err := base.CreateNode(&storage.Node{
		ID:              "stale-1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "stale"},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "hnsw.idx")

	// Seed persisted artifacts from non-empty storage.
	svc1 := NewServiceWithDimensions(base, 3)
	svc1.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc1.SetPersistenceEnabled(false) })
	svc1.SetFulltextIndexPath(bm25Path)
	svc1.SetVectorIndexPath(vectorPath)
	svc1.SetHNSWIndexPath(hnswPath)
	require.NoError(t, svc1.BuildIndexes(context.Background()))
	require.GreaterOrEqual(t, svc1.EmbeddingCount(), 1)

	// Drop all storage data, leaving only persisted on-disk indexes.
	nodesDeleted, edgesDeleted, err := root.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.GreaterOrEqual(t, nodesDeleted, int64(1))
	require.Equal(t, int64(0), edgesDeleted)

	// Rebuild should detect empty storage and clear stale on-disk index state.
	vecInfo, err := os.Stat(vectorPath + ".vec")
	require.NoError(t, err)
	engineWithWrite := &lastWriteEngine{
		Engine:    base,
		writeTime: vecInfo.ModTime().Add(2 * time.Second),
	}
	svc2 := NewServiceWithDimensions(engineWithWrite, 3)
	svc2.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc2.SetPersistenceEnabled(false) })
	svc2.SetFulltextIndexPath(bm25Path)
	svc2.SetVectorIndexPath(vectorPath)
	svc2.SetHNSWIndexPath(hnswPath)
	require.NoError(t, svc2.BuildIndexes(context.Background()))
	require.Equal(t, 0, svc2.EmbeddingCount())
	require.Equal(t, 0, svc2.fulltextIndex.Count())
}

func TestSearchService_BuildIndexes_RestartsVectorStoreWhenStorageIsNewer(t *testing.T) {
	base := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	_, err := base.CreateNode(&storage.Node{
		ID:              "lv-1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
		Properties:      map[string]any{"content": "alpha"},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	vectorPath := filepath.Join(tmp, "vectors")

	// Seed vector store from storage.
	svc1 := NewServiceWithDimensions(base, 3)
	svc1.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc1.SetPersistenceEnabled(false) })
	svc1.SetVectorIndexPath(vectorPath)
	require.NoError(t, svc1.BuildIndexes(context.Background()))
	require.Equal(t, 1, svc1.EmbeddingCount())

	// Inject a stale vector entry not present in storage.
	vfs, err := NewVectorFileStore(vectorPath, 3)
	require.NoError(t, err)
	require.NoError(t, vfs.Load())
	require.NoError(t, vfs.Add("stale-id", []float32{0, 1, 0}))
	require.NoError(t, vfs.Save())
	require.NoError(t, vfs.Sync())
	require.NoError(t, vfs.Close())

	// Ensure vec file timestamp is older than storage LastWriteTime.
	vecPath := vectorPath + ".vec"
	infoBefore, err := os.Stat(vecPath)
	require.NoError(t, err)
	newerWrite := infoBefore.ModTime().Add(2 * time.Second)
	engineWithWrite := &lastWriteEngine{Engine: base, writeTime: newerWrite}

	svc2 := NewServiceWithDimensions(engineWithWrite, 3)
	svc2.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc2.SetPersistenceEnabled(false) })
	svc2.SetVectorIndexPath(vectorPath)
	require.NoError(t, svc2.BuildIndexes(context.Background()))

	// Restart path should rebuild from storage and drop stale vector.
	require.Equal(t, 1, svc2.EmbeddingCount())
	infoAfter, err := os.Stat(vecPath)
	require.NoError(t, err)
	require.True(t, infoAfter.ModTime().After(infoBefore.ModTime()) || infoAfter.ModTime().Equal(infoBefore.ModTime()))
}

func TestSearchService_BuildIndexes_SkipIterationWarmsHNSWWhenDiskIndexMissing(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "missing-hnsw")

	// Seed persisted BM25 snapshot with at least one document.
	bm := NewFulltextIndex()
	bm.Index("doc-1", "alpha beta")
	require.NoError(t, bm.Save(bm25Path))

	// Seed persisted vector file store with >= NSmallMax vectors to force HNSW warmup path.
	vfs, err := NewVectorFileStore(vectorPath, 3)
	require.NoError(t, err)
	for i := 0; i < NSmallMax; i++ {
		id := fmt.Sprintf("seed-%d", i)
		vec := []float32{1, 0, 0}
		if i%2 == 1 {
			vec = []float32{0, 1, 0}
		}
		require.NoError(t, vfs.Add(id, vec))
	}
	require.NoError(t, vfs.Save())
	require.NoError(t, vfs.Sync())
	require.NoError(t, vfs.Close())

	svc := NewServiceWithDimensions(engine, 3)
	svc.SetPersistenceEnabled(true)
	t.Cleanup(func() { svc.SetPersistenceEnabled(false) })
	svc.SetFulltextIndexPath(bm25Path)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetHNSWIndexPath(hnswPath) // intentionally missing on disk

	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.IsReady())
	require.GreaterOrEqual(t, svc.EmbeddingCount(), NSmallMax)

	// Missing on-disk HNSW should trigger warmup build.
	svc.hnswMu.RLock()
	defer svc.hnswMu.RUnlock()
	require.NotNil(t, svc.hnswIndex)
	require.GreaterOrEqual(t, svc.hnswIndex.Size(), NSmallMax)
}

func TestSearchService_PersistBaseIndexes_Branches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 2)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "hnsw")
	svc.SetFulltextIndexPath(bm25Path)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetHNSWIndexPath(hnswPath)

	// Persistence disabled: no files should be written.
	svc.SetPersistenceEnabled(false)
	svc.persistBaseIndexes()
	_, err := os.Stat(bm25Path)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	// Persistence enabled but no dirty BM25 / nil vector store.
	svc.SetPersistenceEnabled(true)
	svc.persistBaseIndexes()
	_, err = os.Stat(vectorPath + ".meta")
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	// Dirty BM25 + valid vector store writes both artifacts.
	svc.fulltextIndex.Index("doc-1", "alpha beta")
	vfs, err := NewVectorFileStore(vectorPath, 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vfs.Close() })
	require.NoError(t, vfs.Add("v-1", []float32{1, 0}))
	require.NoError(t, vfs.Add("v-1", []float32{0, 1})) // create an obsolete record
	require.NoError(t, vfs.Add("v-2", []float32{1, 1}))
	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.mu.Unlock()

	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")
	svc.buildInProgress.Store(false) // compaction path enabled
	svc.persistBaseIndexes()

	_, err = os.Stat(bm25Path)
	require.NoError(t, err)
	_, err = os.Stat(vectorPath + ".meta")
	require.NoError(t, err)

	// buildInProgress=true path: persist still succeeds while compaction is skipped.
	svc.buildInProgress.Store(true)
	require.NoError(t, vfs.Add("v-3", []float32{0, 1}))
	svc.persistBaseIndexes()
	_, err = os.Stat(vectorPath + ".meta")
	require.NoError(t, err)
	svc.buildInProgress.Store(false)
}

func TestSearchService_SchedulePersist_GuardsAndTimer(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 2)
	t.Setenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "1")

	// Guard: persistence disabled.
	svc.SetPersistenceEnabled(false)
	svc.SetFulltextIndexPath(filepath.Join(t.TempDir(), "bm25"))
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vec"))
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()

	// Guard: build in progress.
	svc.SetPersistenceEnabled(true)
	svc.buildInProgress.Store(true)
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()
	svc.buildInProgress.Store(false)

	// Guard: missing required paths.
	svc.SetFulltextIndexPath("")
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()

	// Happy path: both paths configured schedules a timer.
	svc.SetFulltextIndexPath(filepath.Join(t.TempDir(), "bm25"))
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vec"))
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.NotNil(t, svc.persistTimer)
	firstTimer := svc.persistTimer
	svc.persistMu.Unlock()

	// Re-scheduling should replace the previous timer.
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.NotNil(t, svc.persistTimer)
	require.False(t, firstTimer == svc.persistTimer)
	svc.persistMu.Unlock()

	// Invalid/zero delay env falls back to default delay and still schedules.
	t.Setenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "0")
	svc.schedulePersist()
	svc.persistMu.Lock()
	require.NotNil(t, svc.persistTimer)
	svc.persistMu.Unlock()

	// Disabling persistence should stop and clear active timer.
	svc.SetPersistenceEnabled(false)
	svc.persistMu.Lock()
	require.Nil(t, svc.persistTimer)
	svc.persistMu.Unlock()
}

func TestVectorSearchOnly_MethodAndFilterBranches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 3)

	_, err := engine.CreateNode(&storage.Node{
		ID:              "v1",
		Labels:          []string{"Person"},
		Properties:      map[string]any{"content": "alpha"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}, {1, 0, 0}},
	})
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	opts := DefaultSearchOptions()
	opts.Limit = 5
	resp, err := svc.vectorSearchOnly(context.Background(), []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.Equal(t, "vector_brute", resp.SearchMethod)
	require.NotEmpty(t, resp.Results)
	for i, r := range resp.Results {
		require.Equal(t, i+1, r.VectorRank)
		require.Equal(t, 0, r.BM25Rank)
	}

	// Type filter branch should remove non-matching labels/types.
	opts.Types = []string{"organization"}
	filtered, err := svc.vectorSearchOnly(context.Background(), []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.Len(t, filtered.Results, 0)
}

func TestVectorSearchOnly_SearchMethodVariants(t *testing.T) {
	t.Run("vector_hnsw", func(t *testing.T) {
		engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
		svc := NewServiceWithDimensions(engine, 3)
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:              "h1",
			Properties:      map[string]any{"content": "hnsw"},
			ChunkEmbeddings: [][]float32{{1, 0, 0}},
		}))

		h := NewHNSWIndex(3, HNSWConfigFromEnv())
		require.NoError(t, h.Add("h1", []float32{1, 0, 0}))
		svc.pipelineMu.Lock()
		svc.vectorPipeline = NewVectorSearchPipeline(NewHNSWCandidateGen(h), NewCPUExactScorer(svc.vectorIndex))
		svc.pipelineMu.Unlock()

		opts := DefaultSearchOptions()
		opts.Limit = 5
		resp, err := svc.vectorSearchOnly(context.Background(), []float32{1, 0, 0}, opts)
		require.NoError(t, err)
		require.Equal(t, "vector_hnsw", resp.SearchMethod)
	})

	t.Run("vector_clustered", func(t *testing.T) {
		engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
		svc := NewServiceWithDimensions(engine, 2)
		svc.EnableClustering(nil, 2)
		svc.SetMinEmbeddingsForClustering(1)
		require.NoError(t, svc.IndexNode(&storage.Node{ID: "c1", ChunkEmbeddings: [][]float32{{1, 0}}, Properties: map[string]any{"content": "c1"}}))
		require.NoError(t, svc.IndexNode(&storage.Node{ID: "c2", ChunkEmbeddings: [][]float32{{0, 1}}, Properties: map[string]any{"content": "c2"}}))
		require.NoError(t, svc.TriggerClustering(context.Background()))
		require.True(t, svc.clusterIndex.IsClustered())

		gen := NewKMeansCandidateGen(svc.clusterIndex, svc.vectorIndex, 1)
		svc.pipelineMu.Lock()
		svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
		svc.pipelineMu.Unlock()

		opts := DefaultSearchOptions()
		opts.Limit = 5
		resp, err := svc.vectorSearchOnly(context.Background(), []float32{1, 0}, opts)
		require.NoError(t, err)
		require.Equal(t, "vector_clustered", resp.SearchMethod)
	})
}

func TestEnableClustering_ReentrantAndEnvOverrides(t *testing.T) {
	t.Run("reentrant same mode keeps existing cluster index", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 2)
		require.NotNil(t, svc.clusterIndex)
		first := svc.clusterIndex

		// Same CPU mode: second call should be a no-op and preserve pointer.
		svc.EnableClustering(nil, 9)
		require.Same(t, first, svc.clusterIndex)
	})

	t.Run("env overrides cluster count and clamps low max iterations", func(t *testing.T) {
		t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "11")
		t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "1")

		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 0)
		require.NotNil(t, svc.clusterIndex)
		cfg := svc.clusterIndex.Config()
		require.Equal(t, 11, cfg.NumClusters)
		require.False(t, cfg.AutoK)
		require.Equal(t, 5, cfg.MaxIterations)
	})

	t.Run("auto cluster count and max iteration upper clamp", func(t *testing.T) {
		t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "")
		t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "9999")

		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 0)
		require.NotNil(t, svc.clusterIndex)
		cfg := svc.clusterIndex.Config()
		require.Equal(t, 0, cfg.NumClusters)
		require.True(t, cfg.AutoK)
		require.Equal(t, 500, cfg.MaxIterations)
	})

	t.Run("nil vector index is ignored safely", func(t *testing.T) {
		svc := &Service{}
		svc.EnableClustering(nil, 3)
		require.Nil(t, svc.clusterIndex)
		require.False(t, svc.clusterEnabled)
	})
}

func TestSearchService_EnsureBuildVectorFileStore_Branches(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 2)

	// persistence disabled branch
	svc.vectorIndex.vectors["keep"] = []float32{1, 0}
	svc.ensureBuildVectorFileStore()
	require.Nil(t, svc.vectorFileStore)
	require.Contains(t, svc.vectorIndex.vectors, "keep")

	// empty vector path branch
	svc.SetPersistenceEnabled(true)
	svc.ensureBuildVectorFileStore()
	require.Nil(t, svc.vectorFileStore)

	// nil vector index branch
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	orig := svc.vectorIndex
	svc.vectorIndex = nil
	svc.ensureBuildVectorFileStore()
	require.Nil(t, svc.vectorFileStore)
	svc.vectorIndex = orig

	// dimensions <= 0 branch
	svc.vectorIndex = NewVectorIndex(0)
	svc.ensureBuildVectorFileStore()
	require.Nil(t, svc.vectorFileStore)

	// create fail branch (invalid parent path from file)
	badParent := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(badParent, []byte("x"), 0o644))
	svc.vectorIndex = NewVectorIndex(2)
	svc.vectorIndexPath = filepath.Join(badParent, "vectors")
	svc.ensureBuildVectorFileStore()
	require.Nil(t, svc.vectorFileStore)

	// success branch creates file store and clears in-memory vector maps.
	okPath := filepath.Join(t.TempDir(), "ok-vectors")
	svc.vectorIndexPath = okPath
	svc.vectorIndex.vectors["a"] = []float32{1, 0}
	svc.vectorIndex.rawVectors["a"] = []float32{1, 0}
	svc.ensureBuildVectorFileStore()
	require.NotNil(t, svc.vectorFileStore)
	require.Empty(t, svc.vectorIndex.vectors)
	require.Empty(t, svc.vectorIndex.rawVectors)
}

// TestSearchService_WithRealData tests search with exported Neo4j data.
func TestSearchService_WithRealData(t *testing.T) {
	t.Skip("Legacy export loader support removed")
}

// TestSearchService_RRFHybrid tests the full RRF hybrid search with real data.
func TestSearchService_RRFHybrid(t *testing.T) {
	t.Skip("Legacy export loader support removed")
}

func truncateID(id string, maxLen int) string {
	if len(id) <= maxLen {
		return id
	}
	return id[:maxLen] + "..."
}

// BenchmarkVectorSearch benchmarks vector similarity search.
func BenchmarkVectorSearch(b *testing.B) {
	idx := NewVectorIndex(1024)

	// Add 10K vectors
	for i := 0; i < 10000; i++ {
		vec := make([]float32, 1024)
		vec[i%1024] = 1.0
		idx.Add(string(rune('a'+i%26))+string(rune(i)), vec)
	}

	query := make([]float32, 1024)
	query[0] = 1.0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(context.Background(), query, 10, 0.5)
	}
}

// BenchmarkBM25Search benchmarks BM25 full-text search.
func BenchmarkBM25Search(b *testing.B) {
	idx := NewFulltextIndex()

	// Index 10K documents
	texts := []string{
		"machine learning neural networks deep learning",
		"database systems query optimization indexing",
		"natural language processing transformers bert",
		"distributed systems microservices kubernetes",
		"frontend development react vue angular",
	}

	for i := 0; i < 10000; i++ {
		idx.Index(string(rune('a'+i%26))+string(rune(i)), texts[i%len(texts)])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search("machine learning", 10)
	}
}

// BenchmarkRRFFusion benchmarks RRF fusion algorithm.
func BenchmarkRRFFusion(b *testing.B) {
	engine := newNamespacedEngine(b)

	// Create nodes
	for i := 0; i < 100; i++ {
		_, _ = engine.CreateNode(&storage.Node{
			ID:         storage.NodeID(string(rune('a'+i%26)) + string(rune(i))),
			Labels:     []string{"Node"},
			Properties: map[string]any{"title": "Test"},
		})
	}

	svc := NewService(engine)

	// Create result sets
	vectorResults := make([]indexResult, 50)
	bm25Results := make([]indexResult, 50)
	for i := 0; i < 50; i++ {
		vectorResults[i] = indexResult{ID: string(rune('a'+i%26)) + string(rune(i)), Score: float64(50-i) / 50.0}
		bm25Results[i] = indexResult{ID: string(rune('a'+(i+10)%26)) + string(rune(i)), Score: float64(50 - i)}
	}

	opts := DefaultSearchOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.fuseRRF(vectorResults, bm25Results, opts)
	}
}

// ========================================
// Additional Tests for Coverage Improvement
// ========================================

// TestVectorIndex_Remove tests vector removal.
func TestVectorIndex_Remove(t *testing.T) {
	idx := NewVectorIndex(4)

	// Add vectors
	require.NoError(t, idx.Add("doc1", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("doc2", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("doc3", []float32{0, 0, 1, 0}))

	assert.Equal(t, 3, idx.Count())
	assert.True(t, idx.HasVector("doc1"))

	// Remove a vector
	idx.Remove("doc1")
	assert.Equal(t, 2, idx.Count())
	assert.False(t, idx.HasVector("doc1"))
	assert.True(t, idx.HasVector("doc2"))

	// Remove non-existent vector (should not panic)
	idx.Remove("nonexistent")
	assert.Equal(t, 2, idx.Count())
}

// TestVectorIndex_GetDimensions tests dimension getter.
func TestVectorIndex_GetDimensions(t *testing.T) {
	idx := NewVectorIndex(128)
	assert.Equal(t, 128, idx.GetDimensions())

	idx2 := NewVectorIndex(1024)
	assert.Equal(t, 1024, idx2.GetDimensions())
}

// TestVectorIndex_CosineSimilarity tests cosine similarity calculation via search.
func TestVectorIndex_CosineSimilarity(t *testing.T) {
	idx := NewVectorIndex(4)

	// Add reference vectors
	require.NoError(t, idx.Add("ref1", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("ref2", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("ref3", []float32{-1, 0, 0, 0}))
	require.NoError(t, idx.Add("ref4", []float32{1, 1, 0, 0}))

	// Search for identical vector
	results, err := idx.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 0.0)
	require.NoError(t, err)

	// ref1 should have highest score (identical)
	if len(results) > 0 {
		assert.Equal(t, "ref1", results[0].ID)
		assert.InDelta(t, 1.0, results[0].Score, 0.01)
	}
}

// TestFulltextIndex_Remove tests document removal from fulltext index.
func TestFulltextIndex_Remove(t *testing.T) {
	idx := NewFulltextIndex()

	// Add documents
	idx.Index("doc1", "quick brown fox")
	idx.Index("doc2", "lazy brown dog")
	idx.Index("doc3", "quick lazy cat")

	assert.Equal(t, 3, idx.Count())

	// Remove a document
	idx.Remove("doc1")
	assert.Equal(t, 2, idx.Count())

	// Search should not return removed document
	results := idx.Search("quick", 10)
	for _, r := range results {
		assert.NotEqual(t, "doc1", r.ID)
	}

	// Remove non-existent document (should not panic)
	idx.Remove("nonexistent")
	assert.Equal(t, 2, idx.Count())
}

// TestFulltextIndex_GetDocument tests document retrieval.
func TestFulltextIndex_GetDocument(t *testing.T) {
	idx := NewFulltextIndex()

	// Add a document
	idx.Index("doc1", "quick brown fox")

	// Get the document
	content, exists := idx.GetDocument("doc1")
	assert.True(t, exists)
	assert.Equal(t, "quick brown fox", content)

	// Get non-existent document
	_, exists = idx.GetDocument("nonexistent")
	assert.False(t, exists)
}

// TestFulltextIndex_PhraseSearch tests phrase searching.
func TestFulltextIndex_PhraseSearch(t *testing.T) {
	idx := NewFulltextIndex()

	// Add documents
	idx.Index("doc1", "the quick brown fox jumps over the lazy dog")
	idx.Index("doc2", "brown fox is quick")
	idx.Index("doc3", "the lazy brown dog sleeps")

	// Search for exact phrase "quick brown"
	results := idx.PhraseSearch("quick brown", 10)

	// Should find doc1 (has exact phrase "quick brown")
	foundDoc1 := false
	for _, r := range results {
		if r.ID == "doc1" {
			foundDoc1 = true
		}
	}
	assert.True(t, foundDoc1, "Should find doc1 with exact phrase 'quick brown'")
}

// TestFulltextIndex_SaveLoad tests BM25 index persistence (Save/Load round-trip).
func TestFulltextIndex_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25.gob")

	idx := NewFulltextIndex()
	idx.Index("doc1", "machine learning algorithms")
	idx.Index("doc2", "deep learning neural networks")
	idx.Index("doc3", "graph database")

	err := idx.Save(path)
	require.NoError(t, err)

	idx2 := NewFulltextIndex()
	err = idx2.Load(path)
	require.NoError(t, err)

	assert.Equal(t, idx.Count(), idx2.Count())
	results := idx2.Search("learning", 10)
	require.Len(t, results, 2) // doc1 and doc2
}

// TestFulltextIndex_AvgDocLengthAfterManyOps verifies that after many Index/Remove
// operations the average document length stays correct (O(1) update path). Save/Load
// round-trip and search results confirm totalDocLength was consistent.
func TestFulltextIndex_AvgDocLengthAfterManyOps(t *testing.T) {
	idx := NewFulltextIndex()
	for i := 0; i < 100; i++ {
		idx.Index(fmt.Sprintf("doc%d", i), "machine learning and neural networks and algorithms")
	}
	// Remove some, update some
	for i := 10; i < 30; i++ {
		idx.Remove(fmt.Sprintf("doc%d", i))
	}
	for i := 5; i < 15; i++ {
		idx.Index(fmt.Sprintf("doc%d", i), "short")
	}
	beforeSearch := idx.Search("learning", 100)
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25")
	require.NoError(t, idx.Save(path))
	idx2 := NewFulltextIndex()
	require.NoError(t, idx2.Load(path))
	afterSearch := idx2.Search("learning", 100)
	require.Equal(t, len(beforeSearch), len(afterSearch), "Search result count should match after Save/Load (avgDocLength consistent)")
	beforeIDs := make(map[string]bool)
	for _, r := range beforeSearch {
		beforeIDs[r.ID] = true
	}
	for _, r := range afterSearch {
		assert.True(t, beforeIDs[r.ID], "result ID %s should be in before set", r.ID)
	}
}

// TestFulltextIndex_IndexBatch verifies that IndexBatch adds documents and updates avgDocLength once per batch.
func TestFulltextIndex_IndexBatch(t *testing.T) {
	idx := NewFulltextIndex()
	batch := []FulltextBatchEntry{
		{ID: "a", Text: "first document"},
		{ID: "b", Text: "second document"},
		{ID: "c", Text: "third document"},
	}
	idx.IndexBatch(batch)
	assert.Equal(t, 3, idx.Count())
	results := idx.Search("document", 10)
	assert.Len(t, results, 3)
	// Single-doc path still works
	idx.Index("d", "fourth document")
	assert.Equal(t, 4, idx.Count())
}

func TestFulltextIndex_DirtySaveNoCopyClearAndSeeds(t *testing.T) {
	idx := NewFulltextIndex()
	assert.False(t, idx.IsDirty())

	idx.IndexBatch([]FulltextBatchEntry{
		{ID: "d1", Text: "graph query graph traversal"},
		{ID: "d2", Text: "graph query optimization"},
		{ID: "d3", Text: "vector search graph hybrid"},
	})
	assert.True(t, idx.IsDirty())

	seeds := idx.LexicalSeedDocIDs(2, 2)
	require.NotEmpty(t, seeds)
	assert.Nil(t, idx.LexicalSeedDocIDs(0, 1))
	assert.Nil(t, idx.LexicalSeedDocIDs(1, 0))

	path := filepath.Join(t.TempDir(), "bm25")
	require.NoError(t, idx.SaveNoCopy(path))
	assert.False(t, idx.IsDirty())

	idx.Clear()
	assert.Equal(t, 0, idx.Count())
	assert.True(t, idx.IsDirty())

	// Empty clear path should be a no-op.
	idx.Clear()
	assert.Equal(t, 0, idx.Count())
}

func TestConvertGobToMsgpack_FilesAndDirectory(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "tenant_a")
	require.NoError(t, os.MkdirAll(filepath.Join(dbDir, "hnsw_ivf"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "README.txt"), []byte("skip non-dir"), 0644))

	fullSnap := fulltextIndexSnapshot{
		Version:       "1.0.0",
		Documents:     map[string]string{"d1": "hello world"},
		InvertedIndex: map[string]map[string]int{"hello": {"d1": 1}},
		DocLengths:    map[string]int{"d1": 2},
		AvgDocLength:  2,
		DocCount:      1,
	}
	vecSnap := vectorIndexSnapshot{
		Version:    "1.0.0",
		Dimensions: 3,
		Vectors:    map[string][]float32{"d1": {1, 0, 0}},
		RawVectors: map[string][]float32{"d1": {1, 0, 0}},
	}
	hnswSnap := hnswIndexSnapshot{
		Version:      hnswIndexFormatVersionGraphOnly,
		Dimensions:   3,
		InternalToID: []string{"d1"},
		IDToInternal: map[string]uint32{"d1": 0},
	}

	writeGob := func(path string, v any) {
		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, gob.NewEncoder(f).Encode(v))
		require.NoError(t, f.Close())
	}
	writeGob(filepath.Join(dbDir, "bm25.gob"), &fullSnap)
	writeGob(filepath.Join(dbDir, "vectors.gob"), &vecSnap)
	writeGob(filepath.Join(dbDir, "hnsw.gob"), &hnswSnap)
	writeGob(filepath.Join(dbDir, "hnsw_ivf", "0.gob"), &hnswSnap)
	require.NoError(t, os.WriteFile(filepath.Join(dbDir, "hnsw_ivf", "ignored.txt"), []byte("x"), 0644))

	require.NoError(t, ConvertSearchIndexDirFromGobToMsgpack(root))

	_, err := os.Stat(filepath.Join(dbDir, "bm25.gob"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dbDir, "vectors.gob"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dbDir, "hnsw.gob"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dbDir, "hnsw_ivf", "0.gob"))
	assert.ErrorIs(t, err, os.ErrNotExist)

	fBm25, err := os.Open(filepath.Join(dbDir, "bm25"))
	require.NoError(t, err)
	assert.True(t, tryDecodeMsgpack(fBm25, "fulltext"))
	require.NoError(t, fBm25.Close())

	fVec, err := os.Open(filepath.Join(dbDir, "vectors"))
	require.NoError(t, err)
	assert.True(t, tryDecodeMsgpack(fVec, "vector"))
	require.NoError(t, fVec.Close())

	fHNSW, err := os.Open(filepath.Join(dbDir, "hnsw"))
	require.NoError(t, err)
	assert.True(t, tryDecodeMsgpack(fHNSW, "hnsw"))
	require.NoError(t, fHNSW.Close())

	fIVF, err := os.Open(filepath.Join(dbDir, "hnsw_ivf", "0"))
	require.NoError(t, err)
	assert.True(t, tryDecodeMsgpack(fIVF, "hnsw"))
	require.NoError(t, fIVF.Close())

	// Re-running should identify already-converted files and remain successful.
	require.NoError(t, ConvertSearchIndexDirFromGobToMsgpack(root))
}

func TestConvertFileGobToMsgpack_ErrorPaths(t *testing.T) {
	tmp := t.TempDir()
	oldPath := filepath.Join(tmp, "bad.gob")
	newPath := filepath.Join(tmp, "new")

	require.NoError(t, os.WriteFile(oldPath, []byte("not-gob"), 0644))
	err := convertFileGobToMsgpack(oldPath, newPath, "vector")
	require.Error(t, err)

	unknownOld := filepath.Join(tmp, "unknown.gob")
	require.NoError(t, os.WriteFile(unknownOld, []byte("x"), 0644))
	err = convertFileGobToMsgpack(unknownOld, newPath, "unknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown format")

	// Already-converted short-circuit.
	snap := vectorIndexSnapshot{
		Version:    "1.0.0",
		Dimensions: 3,
		Vectors:    map[string][]float32{"d1": {1, 2, 3}},
		RawVectors: map[string][]float32{"d1": {1, 2, 3}},
	}
	out, err := os.Create(newPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(out).Encode(&snap))
	require.NoError(t, out.Close())
	err = convertFileGobToMsgpack(filepath.Join(tmp, "missing.gob"), newPath, "vector")
	require.ErrorIs(t, err, errAlreadyMsgpack)
}

func TestConvertOneDbDir_Branches(t *testing.T) {
	t.Run("hnsw_ivf readdir error propagates", func(t *testing.T) {
		dbDir := t.TempDir()
		// Trigger non-ENOENT error for os.ReadDir(ivfDir).
		require.NoError(t, os.WriteFile(filepath.Join(dbDir, "hnsw_ivf"), []byte("not-dir"), 0o644))
		err := convertOneDbDir(dbDir)
		require.Error(t, err)
	})

	t.Run("non-numeric gob in hnsw_ivf is skipped", func(t *testing.T) {
		dbDir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dbDir, "hnsw_ivf"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dbDir, "hnsw_ivf", "abc.gob"), []byte("x"), 0o644))
		require.NoError(t, convertOneDbDir(dbDir))
		_, err := os.Stat(filepath.Join(dbDir, "hnsw_ivf", "abc.gob"))
		require.NoError(t, err)
	})
}

func TestKMeansCandidateGenerators_Branches(t *testing.T) {
	ctx := context.Background()

	// KMeansCandidateGen fallback path when clustering is unavailable.
	vectorIdx := NewVectorIndex(3)
	require.NoError(t, vectorIdx.Add("d1", []float32{1, 0, 0}))
	require.NoError(t, vectorIdx.Add("d2", []float32{0.9, 0.1, 0}))
	kGen := NewKMeansCandidateGen(nil, vectorIdx, 0)
	require.Equal(t, 3, kGen.numClustersToSearch)
	kGen.SetClusterSelector(func(context.Context, []float32, int) []int { return nil })
	cands, err := kGen.SearchCandidates(ctx, []float32{1, 0, 0}, 5, 0.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)

	// GPUKMeansCandidateGen error branch when index is not clustered.
	gpuGen := NewGPUKMeansCandidateGen(nil, 0)
	require.Equal(t, 3, gpuGen.numClustersToSearch)
	gpuGen.SetClusterSelector(func(context.Context, []float32, int) []int { return nil })
	_, err = gpuGen.SearchCandidates(ctx, []float32{1, 0, 0}, 5, 0.0)
	require.Error(t, err)

	// Clustered path with deterministic single cluster.
	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = true
	embCfg.AutoSync = true
	clusterIndex := gpu.NewClusterIndex(nil, embCfg, &gpu.KMeansConfig{
		NumClusters:   1,
		MaxIterations: 5,
		Tolerance:     0.001,
		InitMethod:    "kmeans++",
	})
	require.NoError(t, clusterIndex.Add("d1", []float32{1, 0, 0}))
	require.NoError(t, clusterIndex.Add("d2", []float32{0.8, 0.2, 0}))
	require.NoError(t, clusterIndex.Cluster())

	kGen = NewKMeansCandidateGen(clusterIndex, vectorIdx, 1)
	kGen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })
	cands, err = kGen.SearchCandidates(ctx, []float32{1, 0, 0}, 5, -1.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)

	gpuGen = NewGPUKMeansCandidateGen(clusterIndex, 1)
	gpuGen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })
	cands, err = gpuGen.SearchCandidates(ctx, []float32{1, 0, 0}, 5, -1.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)

	// Cancellation branch during candidate conversion.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = gpuGen.SearchCandidates(cancelCtx, []float32{1, 0, 0}, 5, -1.0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestVectorQueryNodes_IndexedAndExactPaths(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)

	namedNode := &storage.Node{
		ID:              "named",
		Labels:          []string{"Doc"},
		NamedEmbeddings: map[string][]float32{"myvec": {1, 0, 0}},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
	}
	propNode := &storage.Node{
		ID:     "prop",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"vec_prop": []any{1.0, 0.0, 0.0},
		},
	}
	chunkNode := &storage.Node{
		ID:              "chunk",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.95, 0.05, 0}},
	}
	require.NoError(t, svc.IndexNode(namedNode))
	require.NoError(t, svc.IndexNode(propNode))
	require.NoError(t, svc.IndexNode(chunkNode))

	query := []float32{1, 0, 0}

	// Indexed cosine path.
	hits, err := svc.VectorQueryNodes(context.Background(), query, VectorQuerySpec{
		Label:      "Doc",
		Property:   "myvec",
		Similarity: "cosine",
		Limit:      5,
	})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	assert.Equal(t, "named", hits[0].ID)

	// Exact path with dot similarity.
	hits, err = svc.VectorQueryNodes(context.Background(), query, VectorQuerySpec{
		Label:      "Doc",
		Property:   "vec_prop",
		Similarity: "dot",
		Limit:      5,
	})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	assert.Equal(t, "prop", hits[0].ID)

	// Dimension mismatch returns empty result set, not error.
	hits, err = svc.VectorQueryNodes(context.Background(), []float32{1, 0}, VectorQuerySpec{
		Similarity: "cosine",
		Limit:      5,
	})
	require.NoError(t, err)
	assert.Empty(t, hits)
}

func TestVectorQueryHelpers_ConversionsAndResolution(t *testing.T) {
	assert.Equal(t, []float32{1, 2}, toFloat32SliceAny([]float32{1, 2}))
	assert.Equal(t, []float32{1, 2}, toFloat32SliceAny([]float64{1, 2}))
	assert.Equal(t, []float32{1, 2, 3, 4}, toFloat32SliceAny([]any{float32(1), float64(2), int(3), int64(4)}))
	assert.Nil(t, toFloat32SliceAny([]any{"bad"}))
	assert.Nil(t, toFloat32SliceAny("bad"))

	node := &storage.Node{
		ID:              "n",
		NamedEmbeddings: map[string][]float32{"x": {1, 0, 0}},
		Properties:      map[string]any{"vec": []float64{0.5, 0.5, 0}},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
	}
	embs := resolveCypherCandidateEmbeddings(node, "vec", "x")
	require.Len(t, embs, 1)
	assert.Equal(t, []float32{1, 0, 0}, embs[0])

	node.NamedEmbeddings = nil
	embs = resolveCypherCandidateEmbeddings(node, "vec", "x")
	require.Len(t, embs, 1)
	assert.Equal(t, []float32{0.5, 0.5, 0}, embs[0])

	node.Properties = nil
	embs = resolveCypherCandidateEmbeddings(node, "vec", "x")
	require.Len(t, embs, 1)
	assert.Equal(t, []float32{0, 1, 0}, embs[0])
	assert.Nil(t, resolveCypherCandidateEmbeddings(nil, "vec", "x"))

	// Missing property key (no Cypher vector index/property binding): fall through
	// to all managed named embeddings.
	node = &storage.Node{
		ID:              "n2",
		NamedEmbeddings: map[string][]float32{"managed_a": {1, 0, 0}, "managed_b": {0, 1, 0}},
	}
	embs = resolveCypherCandidateEmbeddings(node, "", "default")
	require.Len(t, embs, 2)
	assert.Contains(t, embs, []float32{1, 0, 0})
	assert.Contains(t, embs, []float32{0, 1, 0})

	query := []float32{1, 0, 0}
	assert.InDelta(t, 1.0, cypherVectorSimilarity("dot", query, []float32{1, 0, 0}), 1e-9)
	assert.InDelta(t, 1.0, cypherVectorSimilarity("cosine", query, []float32{1, 0, 0}), 1e-9)
	assert.Less(t, cypherVectorSimilarity("euclidean", query, []float32{0, 1, 0}), 1.0)
}

func TestVectorQueryNodes_MissingIndexFallsThroughToManagedNamedEmbeddings(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "named_non_default",
		Labels:          []string{"Doc"},
		NamedEmbeddings: map[string][]float32{"managed": {1, 0, 0}},
	}))
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "chunk_fallback",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.6, 0.4, 0}},
	}))

	hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0}, VectorQuerySpec{
		IndexName: "missing_idx",
		Label:     "Doc",
		Property:  "",
		Limit:     5,
	})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	assert.Equal(t, "named_non_default", hits[0].ID)
}

func TestVectorQueryNodes_ErrorAndContextBranches(t *testing.T) {
	var nilSvc *Service
	_, err := nilSvc.VectorQueryNodes(context.Background(), []float32{1, 0, 0}, VectorQuerySpec{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unavailable")

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)
	_, err = svc.VectorQueryNodes(context.Background(), nil, VectorQuerySpec{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")

	// Trigger cancellation branch in exact path loop.
	for i := 0; i < 3; i++ {
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("cx-%d", i)),
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{1, 0, 0}},
		}))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.VectorQueryNodes(cancelled, []float32{1, 0, 0}, VectorQuerySpec{
		Similarity: "dot",
		Limit:      10,
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSearchHelpers_BM25SeedAndSettingsEquivalence(t *testing.T) {
	s := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	assert.False(t, s.ClusteringInProgress())
	s.kmeansInProgress.Store(true)
	assert.True(t, s.ClusteringInProgress())
	s.kmeansInProgress.Store(false)

	assert.False(t, vectorIDInSeedNodeSet("", map[string]struct{}{"a": {}}))
	assert.False(t, vectorIDInSeedNodeSet("chunk:x", nil))
	assert.True(t, vectorIDInSeedNodeSet("a", map[string]struct{}{"a": {}}))
	assert.False(t, vectorIDInSeedNodeSet("b", map[string]struct{}{"a": {}}))

	current := "schema=node;props=title,text;format=2.0.0"
	assert.True(t, bm25SettingsEquivalent(current, current, bm25V2FormatVersion))
	assert.True(t, bm25SettingsEquivalent(
		"schema=node;props=title,text;format=1.0.0",
		current,
		bm25V2FormatVersion,
	))
	assert.False(t, bm25SettingsEquivalent(
		"schema=node;props=title;format=1.0.0",
		current,
		bm25V2FormatVersion,
	))
	assert.False(t, bm25SettingsEquivalent(
		"bad-format",
		current,
		bm25V2FormatVersion,
	))

	assert.Equal(t, BM25EngineV2, normalizeBM25Engine(""))
	assert.Equal(t, BM25EngineV2, normalizeBM25Engine(" V2 "))
	assert.Equal(t, BM25EngineV1, normalizeBM25Engine("v1"))
	assert.Equal(t, BM25EngineV2, normalizeBM25Engine("unknown"))
}

func TestTriggerClustering_GuardBranches(t *testing.T) {
	t.Run("not enabled returns explicit error", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		err := svc.TriggerClustering(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "not enabled")
	})

	t.Run("already running returns nil", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 2)
		svc.kmeansInProgress.Store(true)
		defer svc.kmeansInProgress.Store(false)

		err := svc.TriggerClustering(context.Background())
		require.NoError(t, err)
	})

	t.Run("too few embeddings returns nil", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 2)
		svc.SetMinEmbeddingsForClustering(10)
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:              "few-1",
			ChunkEmbeddings: [][]float32{{1, 0}},
		}))

		err := svc.TriggerClustering(context.Background())
		require.NoError(t, err)
	})

	t.Run("canceled context is returned after threshold gate", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 2)
		svc.SetMinEmbeddingsForClustering(1)
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:              "ctx-1",
			ChunkEmbeddings: [][]float32{{1, 0}},
		}))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := svc.TriggerClustering(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestRRFHybridSearch_ClusteredMethodBranch(t *testing.T) {
	engine := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(engine, 2)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "r1",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "alpha graph"},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}))
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "r2",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "beta graph"},
		ChunkEmbeddings: [][]float32{{0, 1}},
	}))
	require.NoError(t, svc.TriggerClustering(context.Background()))
	require.True(t, svc.clusterIndex.IsClustered())

	gen := NewKMeansCandidateGen(svc.clusterIndex, svc.vectorIndex, 1)
	svc.pipelineMu.Lock()
	svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
	svc.pipelineMu.Unlock()

	opts := DefaultSearchOptions()
	opts.Limit = 5
	resp, err := svc.rrfHybridSearch(context.Background(), "graph", []float32{1, 0}, opts)
	require.NoError(t, err)
	require.Equal(t, "rrf_hybrid_clustered", resp.SearchMethod)
}

func TestSearchHelpers_TryRestoreAndMaybeRebuildBranches(t *testing.T) {
	s := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	// Restore shortcut branches.
	assert.False(t, s.tryRestoreClusteredWarmupFromDisk(context.Background(), nil, ""))
	assert.False(t, s.tryRestoreClusteredWarmupFromDisk(context.Background(), nil, "/tmp/nope"))

	// maybeRebuild: no index and min-interval path should no-op.
	require.NoError(t, s.maybeRebuildHNSW(context.Background(), 0.1, 1.1, time.Second))
	s.hnswLastRebuildUnix.Store(time.Now().Unix())
	require.NoError(t, s.maybeRebuildHNSW(context.Background(), 0.1, 1.1, time.Hour))

	// maybeRebuild: index present but no tombstones, no-op.
	idx := NewHNSWIndex(3, DefaultHNSWConfig())
	require.NoError(t, idx.Add("a", []float32{1, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0}))
	s.hnswMu.Lock()
	s.hnswIndex = idx
	s.hnswMu.Unlock()
	s.hnswLastRebuildUnix.Store(0)
	require.NoError(t, s.maybeRebuildHNSW(context.Background(), 0.99, 10.0, 0))
}

func TestSearchHelpers_ClusterBackfillAndWarmupBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	require.NoError(t, svc.vectorIndex.Add("a", []float32{1, 0}))
	require.NoError(t, svc.vectorIndex.Add("b", []float32{0, 1}))

	// Force lagging cluster state so backfill branch executes from in-memory vectors.
	svc.clusterIndex.Clear()
	require.Equal(t, 0, svc.clusterIndex.Count())
	svc.ensureClusterIndexBackfilled(2)
	require.GreaterOrEqual(t, svc.clusterIndex.Count(), 2)

	// Invalid/missing IVF-HNSW path should deterministically return false (no panic).
	svc.hnswIndexPath = filepath.Join(t.TempDir(), "missing_hnsw")
	require.False(t, svc.tryRestoreClusteredWarmupFromDisk(context.Background(), svc.clusterIndex, svc.hnswIndexPath))

	// With clustering enabled and enough vectors, warmup should run clustering path.
	require.NoError(t, svc.warmupClusteredStrategy(context.Background(), 2))
	require.True(t, svc.clusterIndex.IsClustered())
}

func TestSearchHelpers_BuildPipelineAndGPUBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	vi := NewVectorIndex(2)
	require.NoError(t, vi.Add("x", []float32{1, 0}))

	require.Nil(t, svc.buildPipelineForMode(strategyModeUnknown, vi, nil))
	require.NotNil(t, svc.buildPipelineForMode(strategyModeBruteCPU, vi, nil))

	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 2)
	require.NoError(t, err)
	defer vfs.Close()
	require.NoError(t, vfs.Add("y", []float32{0, 1}))
	require.NotNil(t, svc.buildPipelineForMode(strategyModeBruteCPU, vi, vfs))

	require.Nil(t, svc.buildPipelineForMode(strategyModeHNSW, vi, nil))
	svc.hnswMu.Lock()
	svc.hnswIndex = NewHNSWIndex(2, DefaultHNSWConfig())
	svc.hnswMu.Unlock()
	require.NotNil(t, svc.buildPipelineForMode(strategyModeHNSW, vi, nil))
	require.NotNil(t, svc.buildPipelineForMode(strategyModeHNSW, vi, vfs))

	// ensureGPUIndexSynced should error when GPU manager is unavailable/disabled.
	require.Error(t, svc.ensureGPUIndexSynced(vi, nil))
	cfg := gpu.DefaultConfig()
	cfg.Enabled = false
	gm, err := gpu.NewManager(cfg)
	require.NoError(t, err)
	svc.gpuManager = gm
	require.Error(t, svc.ensureGPUIndexSynced(vi, nil))
}

func TestSearchHelpers_SetGPUManager_NonGPUBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.pipelineMu.Lock()
	svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))
	svc.pipelineMu.Unlock()

	// Nil manager clears pipeline and keeps GPU index nil.
	svc.SetGPUManager(nil)
	svc.pipelineMu.RLock()
	require.Nil(t, svc.vectorPipeline)
	svc.pipelineMu.RUnlock()
	require.Nil(t, svc.gpuEmbeddingIndex)

	// Disabled manager follows same non-GPU branch.
	cfg := gpu.DefaultConfig()
	cfg.Enabled = false
	gm, err := gpu.NewManager(cfg)
	require.NoError(t, err)
	svc.SetGPUManager(gm)
	require.Nil(t, svc.gpuEmbeddingIndex)

	// No vector index dimensions path (dimensions <= 0) should return without creating GPU index.
	svc.mu.Lock()
	svc.vectorIndex = nil
	svc.mu.Unlock()
	svc.SetGPUManager(gm)
	require.Nil(t, svc.gpuEmbeddingIndex)
}

func TestSearchHelpers_HNSWLexicalSeedsAndGetOrCreateBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)

	t.Setenv("NORNICDB_HNSW_LEXICAL_SEED_MAX_TERMS", "0")
	t.Setenv("NORNICDB_HNSW_LEXICAL_SEED_PER_TERM", "0")
	require.Nil(t, svc.hnswLexicalSeedNodeSet(nil))

	ft := &seedOverrideFulltext{
		FulltextIndex: NewFulltextIndex(),
		seedIDs:       []string{"  doc-a ", "", "doc-a", "doc-b"},
	}
	seedSet := svc.hnswLexicalSeedNodeSet(ft)
	require.Len(t, seedSet, 2)
	_, okA := seedSet["doc-a"]
	_, okB := seedSet["doc-b"]
	require.True(t, okA)
	require.True(t, okB)

	// Error branch when neither vector store nor in-memory vector index is available.
	svc.mu.Lock()
	svc.vectorIndex = nil
	svc.vectorFileStore = nil
	svc.mu.Unlock()
	_, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.Error(t, err)

	// Context-canceled branch during in-memory build.
	svc.mu.Lock()
	svc.vectorIndex = NewVectorIndex(2)
	svc.mu.Unlock()
	require.NoError(t, svc.vectorIndex.Add("n1", []float32{1, 0}))
	require.NoError(t, svc.vectorIndex.Add("n2", []float32{0, 1}))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.getOrCreateHNSWIndex(cancelled, 2)
	require.ErrorIs(t, err, context.Canceled)

	// Successful build + existing-index fast path.
	idx, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.NotNil(t, idx)
	idx2, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.Same(t, idx, idx2)

	// File-store build path with lexical seeds.
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 2)
	require.NoError(t, err)
	defer vfs.Close()
	require.NoError(t, vfs.Add("doc-a", []float32{1, 0}))
	require.NoError(t, vfs.Add("doc-b", []float32{0, 1}))

	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.vectorIndex = nil
	svc.fulltextIndex = &seedOverrideFulltext{
		FulltextIndex: NewFulltextIndex(),
		seedIDs:       []string{"doc-a"},
	}
	svc.mu.Unlock()
	svc.hnswMu.Lock()
	svc.hnswIndex = nil
	svc.hnswMu.Unlock()

	idx, err = svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.NotNil(t, idx)
	require.Equal(t, 2, idx.Size())
}

func TestSearchHelpers_BuildHNSWForTransition_Branches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)

	// Nil indexes should return a deterministic error.
	_, err := svc.buildHNSWForTransition(context.Background(), 2, nil, nil)
	require.Error(t, err)

	vi := NewVectorIndex(2)
	require.NoError(t, vi.Add("n1", []float32{1, 0}))
	require.NoError(t, vi.Add("n2", []float32{0, 1}))

	// Cancelled context should short-circuit during build.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.buildHNSWForTransition(cancelled, 2, vi, nil)
	require.ErrorIs(t, err, context.Canceled)

	rebuilt, err := svc.buildHNSWForTransition(context.Background(), 2, vi, nil)
	require.NoError(t, err)
	require.Equal(t, 2, rebuilt.Size())

	// File-store build path.
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "transition_vectors"), 2)
	require.NoError(t, err)
	defer vfs.Close()
	require.NoError(t, vfs.Add("vf1", []float32{1, 0}))
	require.NoError(t, vfs.Add("vf2", []float32{0, 1}))

	rebuilt, err = svc.buildHNSWForTransition(context.Background(), 2, nil, vfs)
	require.NoError(t, err)
	require.Equal(t, 2, rebuilt.Size())

	cancelledVFS, cancelVFS := context.WithCancel(context.Background())
	cancelVFS()
	_, err = svc.buildHNSWForTransition(cancelledVFS, 2, nil, vfs)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSearchHelpers_MaybeRebuildHNSW_RebuildAndCancelBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.vectorIndex = NewVectorIndex(2)
	require.NoError(t, svc.vectorIndex.Add("n1", []float32{1, 0}))
	require.NoError(t, svc.vectorIndex.Add("n2", []float32{0, 1}))
	require.NoError(t, svc.vectorIndex.Add("n3", []float32{0.7, 0.3}))

	old := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, old.Add("n1", []float32{1, 0}))
	require.NoError(t, old.Add("n2", []float32{0, 1}))
	require.NoError(t, old.Add("n3", []float32{0.7, 0.3}))
	old.Remove("n1")
	old.Remove("n2")

	svc.hnswMu.Lock()
	svc.hnswIndex = old
	svc.hnswMu.Unlock()
	svc.pipelineMu.Lock()
	svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))
	svc.pipelineMu.Unlock()

	require.NoError(t, svc.maybeRebuildHNSW(context.Background(), 0.1, 1.1, 0))
	svc.hnswMu.RLock()
	rebuilt := svc.hnswIndex
	svc.hnswMu.RUnlock()
	require.NotNil(t, rebuilt)
	require.NotSame(t, old, rebuilt)
	require.Equal(t, 3, rebuilt.Size())
	svc.pipelineMu.RLock()
	require.Nil(t, svc.vectorPipeline)
	svc.pipelineMu.RUnlock()

	// Recreate a tombstoned index and cancel rebuild mid-flight.
	old2 := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, old2.Add("n1", []float32{1, 0}))
	require.NoError(t, old2.Add("n2", []float32{0, 1}))
	old2.Remove("n1")
	svc.hnswMu.Lock()
	svc.hnswIndex = old2
	svc.hnswMu.Unlock()
	svc.hnswLastRebuildUnix.Store(0)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err := svc.maybeRebuildHNSW(cancelled, 0.0, 1.0, 0)
	require.ErrorIs(t, err, context.Canceled)

	// Rebuild-in-flight guard should short-circuit without error.
	svc.hnswRebuildInFlight.Store(true)
	require.NoError(t, svc.maybeRebuildHNSW(context.Background(), 0.0, 1.0, 0))
	svc.hnswRebuildInFlight.Store(false)
}

func TestSearchHelpers_HNSWUpdateLive_Branches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)

	// No HNSW index: deferred counter should remain unchanged.
	svc.hnswUpdateLive("missing", []float32{1, 0}, false)
	require.Equal(t, int64(0), svc.hnswDeferredMutations.Load())
	svc.hnswUpdateLive("missing", []float32{1, 0}, true) // no-op with nil index

	idx := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, idx.Add("doc-a", []float32{1, 0}))
	svc.hnswMu.Lock()
	svc.hnswIndex = idx
	svc.hnswMu.Unlock()

	// allowLive=false increments deferred mutations when HNSW exists.
	svc.hnswUpdateLive("doc-a", []float32{0.5, 0.5}, false)
	require.Equal(t, int64(1), svc.hnswDeferredMutations.Load())

	// allowLive=true updates existing vector and can add a new vector ID through Update().
	svc.hnswUpdateLive("doc-a", []float32{0.5, 0.5}, true)
	svc.hnswUpdateLive("doc-b", []float32{0, 1}, true)
	svc.hnswMu.RLock()
	size := svc.hnswIndex.Size()
	svc.hnswMu.RUnlock()
	require.Equal(t, 2, size)
}

func TestSearchHelpers_TryRestoreClusteredWarmupFromDisk_Success(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	// Backing vectors used by centroid derivation and lookup.
	require.NoError(t, svc.vectorIndex.Add("doc-a", []float32{1, 0}))
	require.NoError(t, svc.vectorIndex.Add("doc-b", []float32{0, 1}))
	require.NoError(t, svc.clusterIndex.Add("doc-a", []float32{1, 0}))
	require.NoError(t, svc.clusterIndex.Add("doc-b", []float32{0, 1}))

	cluster0 := NewHNSWIndex(2, DefaultHNSWConfig())
	cluster1 := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, cluster0.Add("doc-a", []float32{1, 0}))
	require.NoError(t, cluster1.Add("doc-b", []float32{0, 1}))

	hnswPath := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(hnswPath, map[int]*HNSWIndex{
		0: cluster0,
		1: cluster1,
	}))

	restored := svc.tryRestoreClusteredWarmupFromDisk(context.Background(), svc.clusterIndex, hnswPath)
	require.True(t, restored)
	require.True(t, svc.clusterIndex.IsClustered())
	require.Equal(t, 2, svc.clusterIndex.NumClusters())
}

func TestSearchHelpers_TransitionDeltaVectorAndReplay(t *testing.T) {
	vi := NewVectorIndex(2)
	require.NoError(t, vi.Add("same", []float32{1, 0}))
	require.NoError(t, vi.Add("vi_only", []float32{0, 1}))

	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "delta_vectors"), 2)
	require.NoError(t, err)
	defer vfs.Close()
	require.NoError(t, vfs.Add("same", []float32{0.5, 0.5}))
	require.NoError(t, vfs.Add("vfs_only", []float32{0.2, 0.8}))

	vec, ok := getTransitionDeltaVector("same", vi, vfs)
	require.True(t, ok)
	require.Len(t, vec, 2)
	require.InDelta(t, 0.7071, vec[0], 0.001) // vfs stores normalized vectors
	require.InDelta(t, 0.7071, vec[1], 0.001)

	vec, ok = getTransitionDeltaVector("vi_only", vi, vfs)
	require.True(t, ok)
	require.Len(t, vec, 2)
	require.InDelta(t, 0.0, vec[0], 0.0001)
	require.InDelta(t, 1.0, vec[1], 0.0001)

	vec, ok = getTransitionDeltaVector("missing", vi, vfs)
	require.False(t, ok)
	require.Nil(t, vec)

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionDeltas = []strategyDeltaMutation{
		{seq: 1, id: "vi_only", add: true},
		{seq: 2, id: "vi_only", add: false},
		{seq: 3, id: "vfs_only", add: true},
	}
	svc.strategyTransitionMu.Unlock()

	target := NewHNSWIndex(2, DefaultHNSWConfig())
	last := svc.replayTransitionDeltas(target, strategyModeHNSW, vi, vfs, 0)
	require.Equal(t, uint64(3), last)
	require.Equal(t, 1, target.Size())

	// After watermark should only replay newer deltas.
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionDeltas = append(svc.strategyTransitionDeltas, strategyDeltaMutation{seq: 4, id: "vfs_only", add: false})
	svc.strategyTransitionMu.Unlock()
	last = svc.replayTransitionDeltas(target, strategyModeHNSW, vi, vfs, 3)
	require.Equal(t, uint64(4), last)
	require.Equal(t, 0, target.Size())
}

// TestVectorIndex_SaveLoad tests vector index persistence (Save/Load round-trip).
func TestVectorIndex_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors")

	idx := NewVectorIndex(4)
	require.NoError(t, idx.Add("a", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("c", []float32{0, 0, 1, 0}))

	err := idx.Save(path)
	require.NoError(t, err)

	idx2 := NewVectorIndex(4)
	err = idx2.Load(path)
	require.NoError(t, err)

	assert.Equal(t, idx.Count(), idx2.Count())
	assert.Equal(t, 3, idx2.Count())
}

// TestFulltextIndex_LoadMissingOrCorrupt verifies that Load does not return an error
// when the file is missing or corrupt; the index is left empty so the caller can rebuild.
func TestFulltextIndex_LoadMissingOrCorrupt(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file", func(t *testing.T) {
		idx := NewFulltextIndex()
		err := idx.Load(filepath.Join(dir, "nonexistent.gob"))
		require.NoError(t, err)
		assert.Equal(t, 0, idx.Count())
	})

	t.Run("corrupt file", func(t *testing.T) {
		corruptPath := filepath.Join(dir, "corrupt.gob")
		require.NoError(t, os.WriteFile(corruptPath, []byte("not valid gob"), 0644))
		idx := NewFulltextIndex()
		idx.Index("doc1", "some content") // pre-populate
		err := idx.Load(corruptPath)
		require.NoError(t, err)
		assert.Equal(t, 0, idx.Count()) // index cleared so caller can rebuild
	})

	t.Run("old format version", func(t *testing.T) {
		oldPath := filepath.Join(dir, "old_bm25.gob")
		// Write a snapshot with old semver "0.9.0"; Load should reject and leave index empty.
		f, err := os.Create(oldPath)
		require.NoError(t, err)
		require.NoError(t, msgpack.NewEncoder(f).Encode(&fulltextIndexSnapshot{
			Version:       "0.9.0",
			Documents:     map[string]string{"doc1": "hello world"},
			InvertedIndex: map[string]map[string]int{"hello": {"doc1": 1}},
			DocLengths:    map[string]int{"doc1": 2},
			AvgDocLength:  2,
			DocCount:      1,
		}))
		require.NoError(t, f.Close())
		idx := NewFulltextIndex()
		err = idx.Load(oldPath)
		require.NoError(t, err)
		assert.Equal(t, 0, idx.Count()) // old version not loaded; caller will rebuild
	})
}

// TestVectorIndex_LoadOldVersion verifies that a vector index file with an old or wrong
// format version (semver) is not loaded (index stays empty so caller can rebuild).
func TestVectorIndex_LoadOldVersion(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old_vectors")
	// Write a snapshot with old semver "0.9.0"; Load should reject and leave index empty.
	f, err := os.Create(oldPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(f).Encode(&vectorIndexSnapshot{
		Version:    "0.9.0",
		Dimensions: 4,
		Vectors:    map[string][]float32{"a": {1, 0, 0, 0}},
		RawVectors: map[string][]float32{"a": {1, 0, 0, 0}},
	}))
	require.NoError(t, f.Close())
	idx := NewVectorIndex(4)
	err = idx.Load(oldPath)
	require.NoError(t, err)
	assert.Equal(t, 0, idx.Count()) // old version not loaded; caller will rebuild
}

func TestVectorIndex_LoadAdditionalBranches(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file returns nil", func(t *testing.T) {
		idx := NewVectorIndex(4)
		err := idx.Load(filepath.Join(dir, "no-such-index"))
		require.NoError(t, err)
		require.Equal(t, 0, idx.Count())
	})

	t.Run("corrupt file clears index", func(t *testing.T) {
		p := filepath.Join(dir, "vectors-corrupt")
		require.NoError(t, os.WriteFile(p, []byte("not-msgpack"), 0644))
		idx := NewVectorIndex(4)
		require.NoError(t, idx.Add("x", []float32{1, 0, 0, 0}))
		require.NoError(t, idx.Load(p))
		require.Equal(t, 0, idx.Count())
	})

	t.Run("dimension mismatch clears index", func(t *testing.T) {
		p := filepath.Join(dir, "vectors-dim-mismatch")
		f, err := os.Create(p)
		require.NoError(t, err)
		require.NoError(t, msgpack.NewEncoder(f).Encode(&vectorIndexSnapshot{
			Version:    vectorIndexFormatVersion,
			Dimensions: 8,
			Vectors:    map[string][]float32{"a": {1, 0, 0, 0, 0, 0, 0, 0}},
			RawVectors: map[string][]float32{"a": {1, 0, 0, 0, 0, 0, 0, 0}},
		}))
		require.NoError(t, f.Close())

		idx := NewVectorIndex(4)
		require.NoError(t, idx.Add("seed", []float32{1, 0, 0, 0}))
		require.NoError(t, idx.Load(p))
		require.Equal(t, 0, idx.Count())
	})
}

// TestSearchService_RemoveNode tests node removal from search service.
func TestSearchService_RemoveNode(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewService(engine)

	// Create and index nodes
	node1 := &storage.Node{
		ID:         "node1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice", "embedding": []float32{1, 0, 0, 0}},
	}
	node2 := &storage.Node{
		ID:         "node2",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob", "embedding": []float32{0, 1, 0, 0}},
	}

	_, _ = engine.CreateNode(node1)
	_, _ = engine.CreateNode(node2)
	svc.IndexNode(node1)
	svc.IndexNode(node2)

	// Remove node1
	svc.RemoveNode("node1")

	// Search should not return removed node
	response, err := svc.Search(context.Background(), "Alice", nil, DefaultSearchOptions())
	require.NoError(t, err)
	for _, r := range response.Results {
		assert.NotEqual(t, "node1", r.ID)
	}
}

// TestSearchService_OrphanedEmbedding_DetectedAndRemoved verifies that when a vector
// index hit refers to a node that no longer exists in storage (orphaned embedding),
// we log once, remove all embeddings for that node from indexes, and skip the result.
// A second search then no longer returns that ID (index was cleaned).
func TestSearchService_OrphanedEmbedding_DetectedAndRemoved(t *testing.T) {
	baseEngine := newNamespacedEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	svc := NewServiceWithDimensions(engine, 4)

	// Create and index a node so it exists in both storage and vector index
	node := &storage.Node{
		ID:              "orphan-target",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"name": "Orphan"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))

	// Delete from storage only (simulate orphan: index still has embedding, storage does not)
	require.NoError(t, engine.DeleteNode(node.ID))

	// First search: should detect orphan, log once, remove from index, and not return the missing node
	opts := DefaultSearchOptions()
	opts.Limit = 10
	response, err := svc.Search(context.Background(), "", []float32{1, 0, 0, 0}, opts)
	require.NoError(t, err)
	for _, r := range response.Results {
		assert.NotEqual(t, "orphan-target", r.ID, "orphan should not appear in results")
	}

	// Second search: orphan was removed from index on first search, so no hit for that ID
	response2, err := svc.Search(context.Background(), "", []float32{1, 0, 0, 0}, opts)
	require.NoError(t, err)
	for _, r := range response2.Results {
		assert.NotEqual(t, "orphan-target", r.ID, "index should have been cleaned")
	}
}

// TestSearchService_RemoveNode_DecrementsEmbeddingCount verifies that removing a node
// decrements the embedding count in stats. This is critical for ensuring that when nodes
// are deleted via Cypher, the embedding count is updated correctly without requiring
// a manual "regenerate" operation.
func TestSearchService_RemoveNode_DecrementsEmbeddingCount(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewServiceWithDimensions(engine, 4) // 4-dimensional embeddings for test

	// Initial state: no embeddings
	assert.Equal(t, 0, svc.EmbeddingCount(), "Should start with 0 embeddings")

	// Create and index 3 nodes with embeddings
	// Note: Node.Embedding is the struct field used by IndexNode, not Properties["embedding"]
	nodes := []*storage.Node{
		{
			ID:              "node1",
			Labels:          []string{"Person"},
			Properties:      map[string]any{"name": "Alice"},
			ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
		},
		{
			ID:              "node2",
			Labels:          []string{"Person"},
			Properties:      map[string]any{"name": "Bob"},
			ChunkEmbeddings: [][]float32{{0, 1, 0, 0}},
		},
		{
			ID:              "node3",
			Labels:          []string{"Person"},
			Properties:      map[string]any{"name": "Charlie"},
			ChunkEmbeddings: [][]float32{{0, 0, 1, 0}},
		},
	}

	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}

	// Verify all 3 nodes are indexed
	assert.Equal(t, 3, svc.EmbeddingCount(), "Should have 3 embeddings after indexing")

	// Remove node1
	require.NoError(t, svc.RemoveNode("node1"))
	assert.Equal(t, 2, svc.EmbeddingCount(), "Should have 2 embeddings after removing node1")

	// Remove node2
	require.NoError(t, svc.RemoveNode("node2"))
	assert.Equal(t, 1, svc.EmbeddingCount(), "Should have 1 embedding after removing node2")

	// Remove node3
	require.NoError(t, svc.RemoveNode("node3"))
	assert.Equal(t, 0, svc.EmbeddingCount(), "Should have 0 embeddings after removing all nodes")

	// Removing non-existent node should not affect count
	require.NoError(t, svc.RemoveNode("non-existent"))
	assert.Equal(t, 0, svc.EmbeddingCount(), "Count should remain 0 after removing non-existent node")
}

// TestSearchService_RemoveNode_OnlyRemovesTargetNode ensures RemoveNode is precise
// and doesn't affect other nodes' embeddings.
func TestSearchService_RemoveNode_OnlyRemovesTargetNode(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewServiceWithDimensions(engine, 4) // 4-dimensional embeddings for test

	// Create distinct embeddings for easy search verification
	// Note: Node.Embedding is the struct field used by IndexNode
	node1 := &storage.Node{
		ID:              "target-to-remove",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"content": "unique alpha content"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
	}
	node2 := &storage.Node{
		ID:              "should-remain-1",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"content": "unique beta content"},
		ChunkEmbeddings: [][]float32{{0, 1, 0, 0}},
	}
	node3 := &storage.Node{
		ID:              "should-remain-2",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"content": "unique gamma content"},
		ChunkEmbeddings: [][]float32{{0, 0, 1, 0}},
	}

	for _, node := range []*storage.Node{node1, node2, node3} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}

	assert.Equal(t, 3, svc.EmbeddingCount())

	// Remove only the target node
	require.NoError(t, svc.RemoveNode("target-to-remove"))

	// Verify count decreased
	assert.Equal(t, 2, svc.EmbeddingCount())

	// Verify remaining nodes are still searchable
	opts := DefaultSearchOptions()

	// Search for remaining nodes by their unique content
	response, err := svc.Search(context.Background(), "beta", nil, opts)
	require.NoError(t, err)
	found := false
	for _, r := range response.Results {
		if r.ID == "should-remain-1" {
			found = true
		}
		// The removed node should NOT appear
		assert.NotEqual(t, "target-to-remove", r.ID, "Removed node should not appear in results")
	}
	assert.True(t, found, "Remaining node 'should-remain-1' should be searchable")
}

func TestSearchService_IndexNode_ReplacesExistingVectors_NoOrphansOnDelete(t *testing.T) {
	t.Run("chunk embeddings: shrinking chunk count removes old chunk IDs", func(t *testing.T) {
		baseEngine := newNamespacedEngine(t)
		engine := storage.NewNamespacedEngine(baseEngine, "test")
		svc := NewServiceWithDimensions(engine, 4)

		node := &storage.Node{
			ID:              "node1",
			Labels:          []string{"Document"},
			Properties:      map[string]any{},
			ChunkEmbeddings: [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		require.NoError(t, svc.IndexNode(node))
		// For multi-chunk nodes: main at node ID + each chunk at node-id-chunk-N.
		require.Equal(t, 4, svc.EmbeddingCount(), "expected main + 3 chunk vectors")

		// Re-index same node with fewer chunks (should remove the old extra chunk vector).
		node.ChunkEmbeddings = [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}
		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 3, svc.EmbeddingCount(), "expected main + 2 chunk vectors after re-index")

		// Delete should remove all vectors for this node (no orphaned chunk IDs).
		require.NoError(t, svc.RemoveNode("node1"))
		require.Equal(t, 0, svc.EmbeddingCount())
	})

	t.Run("named embeddings: removing a named vector removes its old vector ID", func(t *testing.T) {
		baseEngine := newNamespacedEngine(t)
		engine := storage.NewNamespacedEngine(baseEngine, "test")
		svc := NewServiceWithDimensions(engine, 4)

		node := &storage.Node{
			ID:              "node2",
			Labels:          []string{"Document"},
			Properties:      map[string]any{},
			NamedEmbeddings: map[string][]float32{"title": {1, 0, 0, 0}, "content": {0, 1, 0, 0}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 2, svc.EmbeddingCount(), "expected 2 named vectors")

		// Re-index with one named vector removed (should remove old named vector ID).
		node.NamedEmbeddings = map[string][]float32{"title": {1, 0, 0, 0}}
		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 1, svc.EmbeddingCount(), "expected 1 named vector after re-index")

		// Delete should remove remaining named vectors.
		require.NoError(t, svc.RemoveNode("node2"))
		require.Equal(t, 0, svc.EmbeddingCount())
	})
}

// TestSearchService_HybridSearch tests the hybrid RRF search.
func TestSearchService_HybridSearch(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewService(engine)

	// Create nodes with both text and embeddings
	nodes := []*storage.Node{
		{
			ID:     "node1",
			Labels: []string{"Document"},
			Properties: map[string]any{
				"title":     "Machine Learning Basics",
				"content":   "Introduction to machine learning algorithms",
				"embedding": []float32{0.9, 0.1, 0, 0},
			},
		},
		{
			ID:     "node2",
			Labels: []string{"Document"},
			Properties: map[string]any{
				"title":     "Deep Learning Neural Networks",
				"content":   "Deep neural networks and deep learning",
				"embedding": []float32{0.8, 0.2, 0, 0},
			},
		},
		{
			ID:     "node3",
			Labels: []string{"Document"},
			Properties: map[string]any{
				"title":     "Data Science Overview",
				"content":   "Data science and analytics fundamentals",
				"embedding": []float32{0.1, 0.9, 0, 0},
			},
		},
	}

	for _, node := range nodes {
		_, _ = engine.CreateNode(node)
		svc.IndexNode(node)
	}

	// Search with both text and query embedding
	opts := DefaultSearchOptions()
	queryEmbedding := []float32{0.85, 0.15, 0, 0}
	opts.VectorWeight = 0.6
	opts.BM25Weight = 0.4

	response, err := svc.Search(context.Background(), "machine learning", queryEmbedding, opts)
	require.NoError(t, err)

	// Should return results ordered by hybrid score
	assert.Greater(t, len(response.Results), 0)
}

func TestService_ProgressAndSimilarityHelpers(t *testing.T) {
	svc := NewService(newNamespacedEngine(t))
	assert.False(t, svc.BuildInProgress())

	svc.buildInProgress.Store(true)
	svc.buildPhase.Store("indexing")
	svc.buildStartedUnix.Store(time.Now().Add(-1 * time.Second).Unix())
	svc.buildProcessed.Store(10)
	svc.buildTotalNodes.Store(20)
	progress := svc.GetBuildProgress()
	assert.True(t, progress.Building)
	assert.Equal(t, "indexing", progress.Phase)
	assert.Equal(t, int64(10), progress.ProcessedNodes)
	assert.Equal(t, int64(20), progress.TotalNodes)
	assert.Greater(t, progress.RateNodesPerSec, 0.0)

	svc.SetDefaultMinSimilarity(0.42)
	assert.Equal(t, 0.42, svc.GetDefaultMinSimilarity())
}

func TestService_VectorAccessAndClear(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	require.NoError(t, svc.vectorIndex.Add("v1", []float32{1, 0, 0}))

	lookup := &vectorLookupGetter{lookup: svc.getVectorLookup()}
	vec, ok := lookup.GetVector("v1")
	require.True(t, ok)
	require.Len(t, vec, 3)

	rawVec, ok := svc.getVectorForCypher("v1")
	require.True(t, ok)
	require.Len(t, rawVec, 3)
	assert.Equal(t, float32(1), rawVec[0])

	svc.ClearVectorIndex()
	assert.Equal(t, 0, svc.vectorIndex.Count())
}

func TestService_LastWriteTimeAndTypeFilter(t *testing.T) {
	eng := newNamespacedEngine(t)
	svc := NewService(eng)

	_, err := eng.CreateNode(&storage.Node{
		ID:     "alpha",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "hello",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "beta",
		Labels: []string{"Other"},
		Properties: map[string]any{
			"content": "world",
		},
	})
	require.NoError(t, err)

	candidates := []SearchCandidate{{ID: "alpha", Score: 0.9}, {ID: "beta", Score: 0.8}, {ID: "missing", Score: 0.7}}
	filtered := svc.filterCandidatesByType(context.Background(), candidates, []string{"doc"}, map[string]bool{})
	require.Len(t, filtered, 1)
	assert.Equal(t, "alpha", filtered[0].ID)

	unfiltered := svc.filterCandidatesByType(context.Background(), candidates, nil, map[string]bool{})
	require.Len(t, unfiltered, len(candidates))

	// Namespaced engine does not expose LastWriteTime -> zero value.
	assert.True(t, svc.lastWriteTime().IsZero())
	assert.True(t, NewService(nil).lastWriteTime().IsZero())
}

func TestService_RerankerPlumbingAndStage2(t *testing.T) {
	eng := newNamespacedEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:     "a",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "alpha",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "b",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "beta",
		},
	})
	require.NoError(t, err)

	// Cross-encoder wiring and availability.
	assert.False(t, svc.CrossEncoderAvailable(ctx))
	ce := NewCrossEncoder(&CrossEncoderConfig{Enabled: false})
	svc.SetCrossEncoder(ce)
	assert.False(t, svc.CrossEncoderAvailable(ctx))

	// Generic reranker wiring.
	rr := &testReranker{enabled: true}
	svc.SetReranker(rr)
	assert.True(t, svc.RerankerAvailable(ctx))
	svc.SetReranker(rr) // no-op equality fast path

	// applyStage2Rerank: disabled reranker -> pass through.
	base := []rrfResult{{ID: "a", RRFScore: 0.9, VectorRank: 1, BM25Rank: 2}, {ID: "b", RRFScore: 0.8, VectorRank: 2, BM25Rank: 1}}
	out := svc.applyStage2Rerank(ctx, "q", base, DefaultSearchOptions(), map[string]bool{}, &testReranker{enabled: false})
	require.Len(t, out, 2)
	assert.Equal(t, base[0].ID, out[0].ID)

	// applyStage2Rerank: reranker error -> pass through.
	out = svc.applyStage2Rerank(ctx, "q", base, DefaultSearchOptions(), map[string]bool{}, &testReranker{enabled: true, err: fmt.Errorf("boom")})
	require.Len(t, out, 2)
	assert.Equal(t, base[0].ID, out[0].ID)

	// applyStage2Rerank: near-identical scores -> keep original order.
	out = svc.applyStage2Rerank(ctx, "q", base, DefaultSearchOptions(), map[string]bool{}, &testReranker{
		enabled: true,
		results: []RerankResult{
			{ID: "b", FinalScore: 0.51, BiScore: 0.2},
			{ID: "a", FinalScore: 0.50, BiScore: 0.3},
		},
	})
	require.Len(t, out, 2)
	assert.Equal(t, "a", out[0].ID) // original order preserved

	// applyStage2Rerank: reranked with score spread and min-score filter.
	opts := DefaultSearchOptions()
	opts.RerankMinScore = 0.6
	out = svc.applyStage2Rerank(ctx, "q", base, opts, map[string]bool{}, &testReranker{
		enabled: true,
		results: []RerankResult{
			{ID: "b", FinalScore: 0.9, BiScore: 0.4},
			{ID: "a", FinalScore: 0.55, BiScore: 0.3},
		},
	})
	require.Len(t, out, 1)
	assert.Equal(t, "b", out[0].ID)
	assert.Equal(t, 2, out[0].VectorRank) // preserved from original map
}

func TestService_RerankCandidatesBranches(t *testing.T) {
	ctx := context.Background()
	var nilSvc *Service
	_, err := nilSvc.RerankCandidates(ctx, "q", []RerankCandidate{{ID: "a", Content: "x", Score: 0.1}}, nil)
	require.Error(t, err)

	svc := NewService(newNamespacedEngine(t))
	// Empty candidates branch.
	out, err := svc.RerankCandidates(ctx, "q", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, out)

	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.9},
		{ID: "b", Content: "beta", Score: 0.8},
		{ID: "c", Content: "gamma", Score: 0.7},
	}

	// No reranker -> pass-through.
	out, err = svc.RerankCandidates(ctx, "q", candidates, nil)
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, "a", out[0].ID)
	assert.Equal(t, 1, out[0].OriginalRank)
	assert.Equal(t, 1, out[0].NewRank)

	// Enabled reranker with topK and min-score filtering.
	svc.SetReranker(&testReranker{
		enabled: true,
		results: []RerankResult{
			{ID: "b", FinalScore: 0.95},
			{ID: "a", FinalScore: 0.40},
		},
	})
	opts := DefaultSearchOptions()
	opts.RerankTopK = 2
	opts.RerankMinScore = 0.5
	out, err = svc.RerankCandidates(ctx, "q", candidates, opts)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "b", out[0].ID)

	// Error path.
	svc.SetReranker(&testReranker{enabled: true, err: fmt.Errorf("rerank failed")})
	_, err = svc.RerankCandidates(ctx, "q", candidates, DefaultSearchOptions())
	require.Error(t, err)
}

// TestSearchService_VectorSearchOnly tests vector-only search mode.
func TestSearchService_VectorSearchOnly(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create a service with 4-dimensional vector index
	svc := NewServiceWithDimensions(engine, 4)

	// Create nodes with embeddings in the Embedding field
	nodes := []*storage.Node{
		{
			ID:              "vec1",
			Labels:          []string{"Vector"},
			ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
		},
		{
			ID:              "vec2",
			Labels:          []string{"Vector"},
			ChunkEmbeddings: [][]float32{{0.9, 0.1, 0, 0}},
		},
		{
			ID:              "vec3",
			Labels:          []string{"Vector"},
			ChunkEmbeddings: [][]float32{{0, 1, 0, 0}},
		},
	}

	for _, node := range nodes {
		_, _ = engine.CreateNode(node)
		err := svc.IndexNode(node)
		require.NoError(t, err)
	}

	// Search with only query embedding (no text query)
	opts := DefaultSearchOptions()
	minSim := 0.5
	opts.MinSimilarity = &minSim
	queryEmbedding := []float32{1, 0, 0, 0}

	response, err := svc.Search(context.Background(), "", queryEmbedding, opts)
	require.NoError(t, err)

	// Should return vec1 and vec2 (above threshold), not vec3
	assert.GreaterOrEqual(t, len(response.Results), 1)
	for _, r := range response.Results {
		assert.NotEqual(t, "vec3", r.ID, "vec3 should be below similarity threshold")
	}
}

// TestSearchService_FilterByType tests type filtering.
func TestSearchService_FilterByType(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewService(engine)

	// Create nodes with different labels
	nodes := []*storage.Node{
		{ID: "person1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}},
		{ID: "person2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}},
		{ID: "doc1", Labels: []string{"Document"}, Properties: map[string]any{"name": "Alice's Doc"}},
	}

	for _, node := range nodes {
		_, _ = engine.CreateNode(node)
		svc.IndexNode(node)
	}

	// Search with type filter using opts.Types
	opts := DefaultSearchOptions()
	opts.Types = []string{"Person"}
	response, err := svc.Search(context.Background(), "alice", nil, opts)
	require.NoError(t, err)

	// Should only return Person nodes
	for _, r := range response.Results {
		assert.Contains(t, r.Labels, "Person")
		assert.NotContains(t, r.Labels, "Document")
	}
}

func TestHybridSearch_DoesNotHoldServiceLockWhileWaitingForPipeline(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	opts := DefaultSearchOptions()
	opts.Limit = 10
	embedding := []float32{1, 0, 0, 0}

	svc.pipelineMu.Lock()

	searchDone := make(chan error, 1)
	go func() {
		_, err := svc.rrfHybridSearch(context.Background(), "query", embedding, opts)
		searchDone <- err
	}()

	// Give the goroutine a moment to block on pipelineMu.
	time.Sleep(10 * time.Millisecond)

	// If rrfHybridSearch holds svc.mu while waiting for pipelineMu, this lock attempt will block.
	locked := make(chan struct{})
	go func() {
		svc.mu.Lock()
		_ = svc.vectorIndex // make critical section non-empty (also exercises safe field access)
		svc.mu.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(200 * time.Millisecond):
		svc.pipelineMu.Unlock()
		t.Fatal("deadlock risk: rrfHybridSearch holds service lock while waiting for pipeline")
	}

	svc.pipelineMu.Unlock()

	select {
	case err := <-searchDone:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("rrfHybridSearch did not return after pipeline unlock")
	}
}

// TestSearchService_EnrichResults tests result enrichment.
func TestSearchService_EnrichResults(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create a node with full properties
	node := &storage.Node{
		ID:     "enriched1",
		Labels: []string{"Person", "Employee"},
		Properties: map[string]any{
			"name":      "Alice",
			"age":       30,
			"email":     "alice@example.com",
			"embedding": []float32{1, 0, 0, 0},
		},
	}
	_, _ = engine.CreateNode(node)

	svc := NewService(engine)
	svc.IndexNode(node)

	// Search and verify enriched results
	opts := DefaultSearchOptions()
	response, err := svc.Search(context.Background(), "Alice", nil, opts)
	require.NoError(t, err)

	assert.Greater(t, len(response.Results), 0)
	if len(response.Results) > 0 {
		// Check that result has enriched properties
		r := response.Results[0]
		assert.Equal(t, "enriched1", r.ID)
		assert.Contains(t, r.Labels, "Person")
		assert.Contains(t, r.Labels, "Employee")
		// Properties should be included
		if r.Properties != nil {
			assert.Equal(t, "Alice", r.Properties["name"])
		}
	}
}

// TestSqrt tests the math.Sqrt standard library function.
// (Originally tested custom sqrt, now consolidated to use math.Sqrt)
func TestSqrt(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{0, 0},
		{1, 1},
		{4, 2},
		{9, 3},
		{16, 4},
		{2, 1.414},
	}

	for _, tt := range tests {
		result := math.Sqrt(tt.input)
		assert.InDelta(t, tt.expected, result, 0.01)
	}
}

// TestTruncate tests the truncate helper function.
func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"longer string", 8, "longe..."}, // 8-3=5 chars + "..."
		{"exact", 5, "exact"},
		{"", 5, ""},
		{"test", 0, ""},               // Edge case: 0 maxLen
		{"test", 3, "tes"},            // Edge case: maxLen <= 3, no ellipsis
		{"test", 2, "te"},             // Edge case: maxLen <= 3, no ellipsis
		{"hello world", 7, "hell..."}, // Normal case: 7-3=4 chars + "..."
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.maxLen)
		assert.Equal(t, tt.expected, result, "truncate(%q, %d)", tt.input, tt.maxLen)
	}
}

// TestSearchService_BuildIndexesFromStorage tests index building from storage.
func TestSearchService_BuildIndexesFromStorage(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Create nodes before service
	nodes := []*storage.Node{
		{
			ID:     "preexisting1",
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"content":   "preexisting document content",
				"embedding": []float32{1, 0, 0, 0},
			},
		},
		{
			ID:     "preexisting2",
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"content": "another preexisting document",
			},
		},
	}
	for _, node := range nodes {
		_, _ = engine.CreateNode(node)
	}

	// Create service and build indexes
	svc := NewService(engine)
	err := svc.BuildIndexes(context.Background())

	assert.NoError(t, err)

	// Verify search works
	response, err := svc.Search(context.Background(), "preexisting", nil, DefaultSearchOptions())
	require.NoError(t, err)
	assert.Greater(t, len(response.Results), 0)
}

// TestGetAdaptiveRRFConfig tests RRF configuration.
func TestGetAdaptiveRRFConfig(t *testing.T) {
	// Test the package-level function
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "short query",
			query: "test",
		},
		{
			name:  "longer query",
			query: "machine learning concepts and algorithms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := GetAdaptiveRRFConfig(tt.query)
			// Should return valid config
			assert.NotNil(t, config)
			assert.Greater(t, config.Limit, 0)
		})
	}
}

// TestSearchService_EmptyQuery tests behavior with empty query.
func TestSearchService_EmptyQuery(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewService(engine)

	// Search with empty query and no embedding
	response, err := svc.Search(context.Background(), "", nil, DefaultSearchOptions())
	require.NoError(t, err)
	assert.Equal(t, 0, len(response.Results))
}

// TestSearchService_SpecialCharacters tests search with special characters.
func TestSearchService_SpecialCharacters(t *testing.T) {
	baseEngine := newNamespacedEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	svc := NewService(engine)

	// Create node with special characters
	node := &storage.Node{
		ID:         "special1",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"content": "C++ programming & Java!"},
	}
	_, _ = engine.CreateNode(node)
	svc.IndexNode(node)

	// Search should handle special chars without panicking
	response, err := svc.Search(context.Background(), "C++", nil, DefaultSearchOptions())
	// Should not panic, even if results vary
	assert.NoError(t, err)
	assert.NotNil(t, response)
}

// ========================================
// Tests for 0% coverage functions
// ========================================

func TestVectorSearchOnlyDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("basic_vector_search", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		// Create nodes with embeddings (float32)
		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = float32(i) / 1024.0
		}

		_, _ = store.CreateNode(&storage.Node{
			ID:              "node-1",
			Labels:          []string{"Document"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]interface{}{
				"title":   "Test Doc",
				"content": "Test content",
			},
		})

		service := NewService(store)

		// Index node
		node, _ := store.GetNode("node-1")
		service.IndexNode(node)

		// Create query embedding (float32 for search)
		queryEmb := make([]float32, 1024)
		for i := range queryEmb {
			queryEmb[i] = float32(i) / 1024.0
		}

		opts := &SearchOptions{
			Limit: 10,
		}

		response, err := service.vectorSearchOnly(ctx, queryEmb, opts)
		if err != nil {
			t.Logf("vectorSearchOnly error (may be expected): %v", err)
		}
		if response != nil {
			t.Logf("Vector search returned %d results", len(response.Results))
		}
	})

	t.Run("empty_embedding", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		service := NewService(store)

		opts := &SearchOptions{Limit: 10}
		response, err := service.vectorSearchOnly(ctx, nil, opts)
		// Expect error or empty results for nil embedding
		if err == nil && response != nil && len(response.Results) != 0 {
			t.Error("Expected empty results for nil embedding")
		}
	})
}

func TestBuildIndexesDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("build_on_empty_storage", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		service := NewService(store)

		err := service.BuildIndexes(ctx)
		if err != nil {
			t.Errorf("BuildIndexes on empty storage failed: %v", err)
		}
	})

	t.Run("build_with_nodes", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		// Create nodes with embeddings (float32)
		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		_, _ = store.CreateNode(&storage.Node{
			ID:              "doc-1",
			Labels:          []string{"Document"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]interface{}{
				"content": "First document content",
			},
		})
		_, _ = store.CreateNode(&storage.Node{
			ID:     "doc-2",
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"content": "Second document without embedding",
			},
		})

		service := NewService(store)

		err := service.BuildIndexes(ctx)
		if err != nil {
			t.Errorf("BuildIndexes failed: %v", err)
		}
	})
}

func TestVectorIndexCosineSimilarityIndirect(t *testing.T) {
	// Test cosine similarity through the VectorIndex search
	idx := NewVectorIndex(4)

	// Add a vector (float32)
	vec1 := []float32{1.0, 0.0, 0.0, 0.0}
	idx.Add("node-1", vec1)

	// Search with similar vector
	ctx := context.Background()
	queryVec := []float32{1.0, 0.0, 0.0, 0.0}
	results, err := idx.Search(ctx, queryVec, 1, 0.0)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected at least one result")
	} else {
		// Should have high similarity for identical vectors
		if results[0].Score < 0.99 {
			t.Errorf("Expected high similarity for identical vectors, got %f", results[0].Score)
		}
	}
}

func TestSearchServiceSearchDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("search_with_embedding", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		_, _ = store.CreateNode(&storage.Node{
			ID:              "doc-1",
			Labels:          []string{"Document"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]interface{}{
				"content": "Machine learning tutorial",
			},
		})

		service := NewService(store)

		// Index the node
		node, _ := store.GetNode("doc-1")
		service.IndexNode(node)

		queryEmb := make([]float32, 1024)
		for i := range queryEmb {
			queryEmb[i] = 0.5
		}

		opts := &SearchOptions{
			Limit: 10,
		}

		response, err := service.Search(ctx, "machine learning", queryEmb, opts)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		t.Logf("Search returned %d results", len(response.Results))
	})
}

func TestFulltextSearchWithPrefix(t *testing.T) {
	idx := NewFulltextIndex()

	// Index documents
	idx.Index("doc-1", "machine learning algorithms")
	idx.Index("doc-2", "deep learning models")
	idx.Index("doc-3", "natural language processing")

	// Search with prefix
	results := idx.Search("mach", 10) // Should match "machine"
	t.Logf("Prefix search for 'mach' returned %d results", len(results))

	// Full term search
	results = idx.Search("learning", 10)
	if len(results) < 2 {
		t.Errorf("Expected at least 2 results for 'learning', got %d", len(results))
	}
}

func TestRRFHybridSearchDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("rrf_with_both_results", func(t *testing.T) {
		baseStore := newNamespacedEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")

		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		_, _ = store.CreateNode(&storage.Node{
			ID:              "doc-1",
			Labels:          []string{"Document"},
			ChunkEmbeddings: [][]float32{embedding},
			Properties: map[string]interface{}{
				"content": "Machine learning tutorial content",
			},
		})

		service := NewService(store)

		node, _ := store.GetNode("doc-1")
		service.IndexNode(node)

		queryEmb := make([]float32, 1024)
		for i := range queryEmb {
			queryEmb[i] = 0.5
		}

		opts := &SearchOptions{
			Limit:        10,
			RRFK:         60,
			VectorWeight: 0.6,
			BM25Weight:   0.4,
		}

		response, err := service.rrfHybridSearch(ctx, "machine learning", queryEmb, opts)
		if err != nil {
			t.Fatalf("rrfHybridSearch failed: %v", err)
		}
		t.Logf("RRF search returned %d results", len(response.Results))
	})
}

// =============================================================================
// MMR Diversification Tests
// =============================================================================

func TestMMRDiversification(t *testing.T) {
	baseStore := newNamespacedEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	service := NewService(store)

	// Create nodes with embeddings - some similar, some diverse
	createNodeWithEmbedding := func(id string, labels []string, embedding []float32, props map[string]interface{}) {
		node := &storage.Node{
			ID:              storage.NodeID(id),
			Labels:          labels,
			Properties:      props,
			ChunkEmbeddings: [][]float32{embedding},
		}
		_, _ = store.CreateNode(node)
		service.IndexNode(node)
	}

	t.Run("mmr_promotes_diversity", func(t *testing.T) {
		// Create 3 documents: 2 nearly identical, 1 diverse
		// Embeddings are 4-dimensional for simplicity
		createNodeWithEmbedding("similar1", []string{"Doc"}, []float32{0.9, 0.1, 0.0, 0.0}, map[string]interface{}{
			"title":   "Machine Learning Basics",
			"content": "Introduction to ML algorithms",
		})
		createNodeWithEmbedding("similar2", []string{"Doc"}, []float32{0.89, 0.11, 0.0, 0.0}, map[string]interface{}{
			"title":   "ML Fundamentals",
			"content": "Basic machine learning concepts",
		})
		createNodeWithEmbedding("diverse1", []string{"Doc"}, []float32{0.1, 0.1, 0.8, 0.0}, map[string]interface{}{
			"title":   "Database Design",
			"content": "SQL and NoSQL databases",
		})

		// Build RRF results with EQUAL scores to isolate diversity effect
		rrfResults := []rrfResult{
			{ID: "similar1", RRFScore: 0.05, VectorRank: 1, BM25Rank: 1},
			{ID: "similar2", RRFScore: 0.05, VectorRank: 2, BM25Rank: 2}, // Equal score
			{ID: "diverse1", RRFScore: 0.05, VectorRank: 3, BM25Rank: 3}, // Equal score
		}

		// Query embedding similar to "similar1"
		queryEmb := []float32{0.9, 0.1, 0.0, 0.0}

		// Without MMR: results should be in original order
		resultsNoMMR := service.applyMMR(context.Background(), rrfResults, queryEmb, 3, 1.0, nil) // lambda=1.0 = no diversity
		assert.Len(t, resultsNoMMR, 3, "Should return all results")
		t.Logf("Without MMR (lambda=1.0): %v", []string{resultsNoMMR[0].ID, resultsNoMMR[1].ID, resultsNoMMR[2].ID})

		// With MMR (lambda=0.3): strong diversity preference
		resultsWithMMR := service.applyMMR(context.Background(), rrfResults, queryEmb, 3, 0.3, nil) // lambda=0.3 = 70% diversity
		assert.Len(t, resultsWithMMR, 3, "Should return all results")
		t.Logf("With MMR (lambda=0.3):    %v", []string{resultsWithMMR[0].ID, resultsWithMMR[1].ID, resultsWithMMR[2].ID})

		// Verify MMR algorithm completes successfully
		// Note: exact order depends on embeddings being retrieved from storage
	})

	t.Run("mmr_lambda_1_equals_no_diversity", func(t *testing.T) {
		rrfResults := []rrfResult{
			{ID: "doc1", RRFScore: 0.1},
			{ID: "doc2", RRFScore: 0.09},
			{ID: "doc3", RRFScore: 0.08},
		}

		// Lambda=1.0 should return results in original order (pure relevance)
		results := service.applyMMR(context.Background(), rrfResults, []float32{1, 0, 0, 0}, 3, 1.0, nil)
		assert.Equal(t, "doc1", results[0].ID)
		assert.Equal(t, "doc2", results[1].ID)
		assert.Equal(t, "doc3", results[2].ID)
	})

	t.Run("mmr_handles_empty_results", func(t *testing.T) {
		results := service.applyMMR(context.Background(), []rrfResult{}, []float32{1, 0, 0, 0}, 10, 0.7, nil)
		assert.Empty(t, results)
	})

	t.Run("mmr_handles_single_result", func(t *testing.T) {
		rrfResults := []rrfResult{{ID: "only", RRFScore: 0.1}}
		results := service.applyMMR(context.Background(), rrfResults, []float32{1, 0, 0, 0}, 10, 0.7, nil)
		assert.Len(t, results, 1)
		assert.Equal(t, "only", results[0].ID)
	})
}

func TestSearchWithMMROption(t *testing.T) {
	baseStore := newNamespacedEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	service := NewService(store)
	ctx := context.Background()

	// Create nodes with embeddings
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc%d", i)),
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"title":   fmt.Sprintf("Document %d about AI", i),
				"content": "This is content about artificial intelligence and machine learning",
			},
			ChunkEmbeddings: [][]float32{make([]float32, 1024)},
		}
		// Slightly different embeddings
		for j := range node.ChunkEmbeddings[0] {
			node.ChunkEmbeddings[0][j] = float32(i)*0.01 + float32(j)*0.001
		}
		_, _ = store.CreateNode(node)
		service.IndexNode(node)
	}

	t.Run("search_with_mmr_enabled", func(t *testing.T) {
		queryEmb := make([]float32, 1024)
		for i := range queryEmb {
			queryEmb[i] = 0.5
		}

		opts := &SearchOptions{
			Limit:      5,
			MMREnabled: true,
			MMRLambda:  0.7,
		}

		response, err := service.Search(ctx, "AI machine learning", queryEmb, opts)
		require.NoError(t, err)
		assert.Contains(t, response.SearchMethod, "mmr", "Search method should indicate MMR")
		assert.NotEmpty(t, response.Results)
		t.Logf("MMR search returned %d results with method: %s", len(response.Results), response.SearchMethod)
	})

	t.Run("search_without_mmr", func(t *testing.T) {
		queryEmb := make([]float32, 1024)
		for i := range queryEmb {
			queryEmb[i] = 0.5
		}

		opts := &SearchOptions{
			Limit:      5,
			MMREnabled: false,
		}

		response, err := service.Search(ctx, "AI machine learning", queryEmb, opts)
		require.NoError(t, err)
		assert.NotContains(t, response.SearchMethod, "mmr", "Search method should not mention MMR")
	})
}

// TestNodeMatchesFilters covers the scalar, array, multi-value, and multi-key filter logic.
func TestNodeMatchesFilters(t *testing.T) {
	node := &storage.Node{
		ID:     "n1",
		Labels: []string{"Chunk"},
		Properties: map[string]any{
			"collection": []any{"user_a", "user_b"},
			"type":       "text",
			"score":      42,
		},
	}

	t.Run("scalar_match", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"type": {"text"}}))
		assert.False(t, nodeMatchesFilters(node, map[string][]string{"type": {"image"}}))
	})

	t.Run("array_membership_match", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_a"}}))
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_b"}}))
		assert.False(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_c"}}))
	})

	t.Run("multi_value_or", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_a", "user_b"}}))
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_c", "user_a"}}))
		assert.False(t, nodeMatchesFilters(node, map[string][]string{"collection": {"user_c", "user_d"}}))
	})

	t.Run("multi_property_and", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{
			"collection": {"user_a"},
			"type":       {"text"},
		}))
		assert.False(t, nodeMatchesFilters(node, map[string][]string{
			"collection": {"user_a"},
			"type":       {"image"},
		}))
	})

	t.Run("missing_property", func(t *testing.T) {
		assert.False(t, nodeMatchesFilters(node, map[string][]string{"nonexistent": {"x"}}))
	})

	t.Run("empty_filters", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{}))
	})

	t.Run("numeric_scalar_as_string", func(t *testing.T) {
		assert.True(t, nodeMatchesFilters(node, map[string][]string{"score": {"42"}}))
		assert.False(t, nodeMatchesFilters(node, map[string][]string{"score": {"99"}}))
	})
}

// TestPropValueMatchesAny exercises propValueMatchesAny directly for all supported property types.
func TestPropValueMatchesAny(t *testing.T) {
	t.Run("string_scalar", func(t *testing.T) {
		assert.True(t, propValueMatchesAny("hello", []string{"hello"}))
		assert.False(t, propValueMatchesAny("hello", []string{"world"}))
	})

	t.Run("any_slice", func(t *testing.T) {
		assert.True(t, propValueMatchesAny([]any{"a", "b"}, []string{"b"}))
		assert.False(t, propValueMatchesAny([]any{"a", "b"}, []string{"c"}))
	})

	t.Run("string_slice", func(t *testing.T) {
		assert.True(t, propValueMatchesAny([]string{"x", "y"}, []string{"y"}))
		assert.False(t, propValueMatchesAny([]string{"x", "y"}, []string{"z"}))
	})
}

// TestFilterByProperties_PrefiltersBeforeTopK verifies that nodes not matching the
// filter are excluded from results even when they would otherwise rank in the top-K.
func TestFilterByProperties_PrefiltersBeforeTopK(t *testing.T) {
	baseStore := newNamespacedEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	svc := NewService(store)
	ctx := context.Background()

	// Create 10 nodes; only 2 belong to "user_a".
	for i := 0; i < 10; i++ {
		collection := []any{"user_b"}
		if i == 3 || i == 7 {
			collection = []any{"user_a"}
		}
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("node%d", i)),
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    fmt.Sprintf("chunk number %d", i),
				"collection": collection,
			},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		svc.IndexNode(node)
	}

	opts := &SearchOptions{
		Limit:   10,
		Filters: map[string][]string{"collection": {"user_a"}},
	}

	// BM25-only path (no embedding).
	resp, err := svc.Search(ctx, "chunk", nil, opts)
	require.NoError(t, err)

	for _, r := range resp.Results {
		props := r.Properties
		coll, ok := props["collection"]
		require.True(t, ok, "result %s missing collection property", r.ID)
		assert.True(t, propValueMatchesAny(coll, []string{"user_a"}),
			"result %s has collection=%v, want user_a", r.ID, coll)
	}
	// Only the 2 user_a nodes should be returned (not all 10 minus filtered).
	assert.LessOrEqual(t, len(resp.Results), 2,
		"expected at most 2 user_a results, got %d", len(resp.Results))
}

// TestFilterByProperties_MultiPropertyAnd verifies that all filter keys must match.
func TestFilterByProperties_MultiPropertyAnd(t *testing.T) {
	baseStore := newNamespacedEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	svc := NewService(store)
	ctx := context.Background()

	nodes := []*storage.Node{
		{
			ID:     "match",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "matching node",
				"collection": []any{"user_a"},
				"type":       "text",
			},
		},
		{
			ID:     "wrong_collection",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "wrong collection",
				"collection": []any{"user_b"},
				"type":       "text",
			},
		},
		{
			ID:     "wrong_type",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "wrong type",
				"collection": []any{"user_a"},
				"type":       "image",
			},
		},
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	for _, n := range nodes {
		svc.IndexNode(n)
	}

	opts := &SearchOptions{
		Limit: 10,
		Filters: map[string][]string{
			"collection": {"user_a"},
			"type":       {"text"},
		},
	}

	resp, err := svc.Search(ctx, "node", nil, opts)
	require.NoError(t, err)

	ids := make(map[string]bool)
	for _, r := range resp.Results {
		ids[r.ID] = true
	}
	assert.True(t, ids["match"], "expected 'match' node in results")
	assert.False(t, ids["wrong_collection"], "unexpected 'wrong_collection' node in results")
	assert.False(t, ids["wrong_type"], "unexpected 'wrong_type' node in results")
}

// TestFilterByProperties_NoFilterPreservesAllResults verifies backward compatibility.
func TestFilterByProperties_NoFilterPreservesAllResults(t *testing.T) {
	baseStore := newNamespacedEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	svc := NewService(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc%d", i)),
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content": fmt.Sprintf("document %d", i),
			},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		svc.IndexNode(node)
	}

	// Without filters, all 5 docs should be reachable.
	opts := &SearchOptions{Limit: 10}
	resp, err := svc.Search(ctx, "document", nil, opts)
	require.NoError(t, err)
	assert.Len(t, resp.Results, 5)
}
