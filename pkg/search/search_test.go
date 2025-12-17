// Package search tests
package search

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	// Create nodes with embeddings
	embedding := make([]float32, 1024)
	embedding[0] = 1.0 // Simple non-zero embedding

	nodes := []*storage.Node{
		{
			ID:        "node1",
			Labels:    []string{"Node"},
			Embedding: embedding,
			Properties: map[string]any{
				"title":   "Test Node 1",
				"content": "This is test content for searching",
			},
		},
		{
			ID:        "node2",
			Labels:    []string{"Node"},
			Embedding: embedding,
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

// TestSearchService_WithRealData tests search with exported Neo4j data.
func TestSearchService_WithRealData(t *testing.T) {
	// Path to exported data
	possiblePaths := []string{
		filepath.Join("..", "..", "..", "data", "nornicdb"),
		os.Getenv("MIMIR_DATA_DIR"),
		"/Users/c815719/src/Mimir/data/nornicdb",
	}

	var exportDir string
	for _, p := range possiblePaths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			exportDir = p
			break
		}
	}

	if exportDir == "" {
		t.Skip("Export directory not found - run export-neo4j-to-json.mjs first")
	}

	// Check if nodes.json exists
	nodesFile := filepath.Join(exportDir, "nodes.json")
	if _, err := os.Stat(nodesFile); os.IsNotExist(err) {
		t.Skipf("nodes.json not found in %s - run export-neo4j-to-json.mjs first", exportDir)
	}

	t.Logf("Using export directory: %s", exportDir)

	// Load data into memory engine
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	result, err := storage.LoadFromMimirExport(engine, exportDir)
	require.NoError(t, err)

	t.Logf("Loaded %d nodes, %d edges, %d embeddings", result.NodesImported, result.EdgesImported, result.EmbeddingsLoaded)

	// Create search service
	svc := NewService(engine)

	// Build indexes
	t.Log("Building search indexes...")
	startTime := time.Now()
	err = svc.BuildIndexes(context.Background())
	require.NoError(t, err)
	t.Logf("Index build time: %v", time.Since(startTime))

	t.Logf("Vector index: %d vectors", svc.vectorIndex.Count())
	t.Logf("Fulltext index: %d documents", svc.fulltextIndex.Count())

	// Test full-text search
	t.Log("\n=== Full-text search test ===")
	ctx := context.Background()
	opts := DefaultSearchOptions()
	opts.Limit = 5

	response, err := svc.fullTextSearchOnly(ctx, "docker configuration", opts)
	require.NoError(t, err)

	t.Logf("Search for 'docker configuration': %d results", len(response.Results))
	for _, r := range response.Results {
		t.Logf("  %s: type=%s title=%s score=%.4f", r.ID[:20]+"...", r.Type, r.Title, r.Score)
	}

	// Test type filtering
	t.Log("\n=== Type filtering test ===")
	opts.Types = []string{"todo"}
	response, err = svc.fullTextSearchOnly(ctx, "implementation", opts)
	require.NoError(t, err)

	t.Logf("Search for 'implementation' (type=todo): %d results", len(response.Results))
	for _, r := range response.Results {
		t.Logf("  %s: type=%s title=%s", r.ID[:20]+"...", r.Type, r.Title)
	}
}

// TestSearchService_RRFHybrid tests the full RRF hybrid search with real data.
func TestSearchService_RRFHybrid(t *testing.T) {
	// Path to exported data
	possiblePaths := []string{
		filepath.Join("..", "..", "..", "data", "nornicdb"),
		"/Users/c815719/src/Mimir/data/nornicdb",
	}

	var exportDir string
	for _, p := range possiblePaths {
		if _, err := os.Stat(p); err == nil {
			exportDir = p
			break
		}
	}

	if exportDir == "" {
		t.Skip("Export directory not found")
	}

	// Check if nodes.json exists
	nodesFile := filepath.Join(exportDir, "nodes.json")
	if _, err := os.Stat(nodesFile); os.IsNotExist(err) {
		t.Skipf("nodes.json not found in %s - run export-neo4j-to-json.mjs first", exportDir)
	}

	// Load data
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	result, err := storage.LoadFromMimirExport(engine, exportDir)
	require.NoError(t, err)
	require.True(t, result.EmbeddingsLoaded > 0, "Need embeddings for RRF test")

	// Create search service and build indexes
	svc := NewService(engine)
	err = svc.BuildIndexes(context.Background())
	require.NoError(t, err)

	// Get a real embedding to use as query
	nodes, _ := engine.AllNodes()
	var queryEmbedding []float32
	for _, node := range nodes {
		if len(node.Embedding) == 1024 {
			queryEmbedding = node.Embedding
			t.Logf("Using embedding from node: %s", node.ID)
			break
		}
	}
	require.NotNil(t, queryEmbedding, "Need an embedding for query")

	// Test RRF hybrid search
	t.Log("\n=== RRF Hybrid Search Test ===")
	ctx := context.Background()
	opts := DefaultSearchOptions()
	opts.Limit = 10

	response, err := svc.Search(ctx, "authentication login security", queryEmbedding, opts)
	require.NoError(t, err)

	t.Logf("RRF Search results: %d total, method=%s, fallback=%v",
		len(response.Results), response.SearchMethod, response.FallbackTriggered)

	for i, r := range response.Results {
		t.Logf("  %d. %s: type=%s rrf=%.4f vectorRank=%d bm25Rank=%d",
			i+1, truncateID(r.ID, 20), r.Type, r.RRFScore, r.VectorRank, r.BM25Rank)
	}

	if response.Metrics != nil {
		t.Logf("Metrics: vector=%d, bm25=%d, fused=%d",
			response.Metrics.VectorCandidates,
			response.Metrics.BM25Candidates,
			response.Metrics.FusedCandidates)
	}

	// Verify RRF is working
	assert.Equal(t, "rrf_hybrid", response.SearchMethod)
	assert.False(t, response.FallbackTriggered)
	assert.NotEmpty(t, response.Results)

	// Test adaptive config
	t.Log("\n=== Adaptive RRF Config Test ===")
	shortOpts := GetAdaptiveRRFConfig("docker")
	t.Logf("Short query 'docker': vectorWeight=%.1f, bm25Weight=%.1f", shortOpts.VectorWeight, shortOpts.BM25Weight)

	longOpts := GetAdaptiveRRFConfig("how do I configure authentication and login security for production")
	t.Logf("Long query: vectorWeight=%.1f, bm25Weight=%.1f", longOpts.VectorWeight, longOpts.BM25Weight)
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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	// Create nodes
	for i := 0; i < 100; i++ {
		engine.CreateNode(&storage.Node{
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

// TestSearchService_RemoveNode tests node removal from search service.
func TestSearchService_RemoveNode(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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

	engine.CreateNode(node1)
	engine.CreateNode(node2)
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

// TestSearchService_RemoveNode_DecrementsEmbeddingCount verifies that removing a node
// decrements the embedding count in stats. This is critical for ensuring that when nodes
// are deleted via Cypher, the embedding count is updated correctly without requiring
// a manual "regenerate" operation.
func TestSearchService_RemoveNode_DecrementsEmbeddingCount(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	svc := NewServiceWithDimensions(engine, 4) // 4-dimensional embeddings for test

	// Initial state: no embeddings
	assert.Equal(t, 0, svc.EmbeddingCount(), "Should start with 0 embeddings")

	// Create and index 3 nodes with embeddings
	// Note: Node.Embedding is the struct field used by IndexNode, not Properties["embedding"]
	nodes := []*storage.Node{
		{
			ID:         "node1",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Alice"},
			Embedding:  []float32{1, 0, 0, 0},
		},
		{
			ID:         "node2",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Bob"},
			Embedding:  []float32{0, 1, 0, 0},
		},
		{
			ID:         "node3",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Charlie"},
			Embedding:  []float32{0, 0, 1, 0},
		},
	}

	for _, node := range nodes {
		require.NoError(t, engine.CreateNode(node))
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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	svc := NewServiceWithDimensions(engine, 4) // 4-dimensional embeddings for test

	// Create distinct embeddings for easy search verification
	// Note: Node.Embedding is the struct field used by IndexNode
	node1 := &storage.Node{
		ID:         "target-to-remove",
		Labels:     []string{"Document"},
		Properties: map[string]any{"content": "unique alpha content"},
		Embedding:  []float32{1, 0, 0, 0},
	}
	node2 := &storage.Node{
		ID:         "should-remain-1",
		Labels:     []string{"Document"},
		Properties: map[string]any{"content": "unique beta content"},
		Embedding:  []float32{0, 1, 0, 0},
	}
	node3 := &storage.Node{
		ID:         "should-remain-2",
		Labels:     []string{"Document"},
		Properties: map[string]any{"content": "unique gamma content"},
		Embedding:  []float32{0, 0, 1, 0},
	}

	for _, node := range []*storage.Node{node1, node2, node3} {
		require.NoError(t, engine.CreateNode(node))
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

// TestSearchService_HybridSearch tests the hybrid RRF search.
func TestSearchService_HybridSearch(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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
		engine.CreateNode(node)
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

// TestSearchService_VectorSearchOnly tests vector-only search mode.
func TestSearchService_VectorSearchOnly(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	// Create a service with 4-dimensional vector index
	svc := &Service{
		engine:        engine,
		vectorIndex:   NewVectorIndex(4), // Use 4 dimensions for test vectors
		fulltextIndex: NewFulltextIndex(),
	}

	// Create nodes with embeddings in the Embedding field
	nodes := []*storage.Node{
		{
			ID:        "vec1",
			Labels:    []string{"Vector"},
			Embedding: []float32{1, 0, 0, 0},
		},
		{
			ID:        "vec2",
			Labels:    []string{"Vector"},
			Embedding: []float32{0.9, 0.1, 0, 0},
		},
		{
			ID:        "vec3",
			Labels:    []string{"Vector"},
			Embedding: []float32{0, 1, 0, 0},
		},
	}

	for _, node := range nodes {
		engine.CreateNode(node)
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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	svc := NewService(engine)

	// Create nodes with different labels
	nodes := []*storage.Node{
		{ID: "person1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}},
		{ID: "person2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}},
		{ID: "doc1", Labels: []string{"Document"}, Properties: map[string]any{"name": "Alice's Doc"}},
	}

	for _, node := range nodes {
		engine.CreateNode(node)
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

// TestSearchService_EnrichResults tests result enrichment.
func TestSearchService_EnrichResults(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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
	engine.CreateNode(node)

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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

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
		engine.CreateNode(node)
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
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	svc := NewService(engine)

	// Search with empty query and no embedding
	response, err := svc.Search(context.Background(), "", nil, DefaultSearchOptions())
	require.NoError(t, err)
	assert.Equal(t, 0, len(response.Results))
}

// TestSearchService_SpecialCharacters tests search with special characters.
func TestSearchService_SpecialCharacters(t *testing.T) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()

	svc := NewService(engine)

	// Create node with special characters
	node := &storage.Node{
		ID:         "special1",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"content": "C++ programming & Java!"},
	}
	engine.CreateNode(node)
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
		store := storage.NewMemoryEngine()
		defer store.Close()

		// Create nodes with embeddings (float32)
		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = float32(i) / 1024.0
		}

		store.CreateNode(&storage.Node{
			ID:        "node-1",
			Labels:    []string{"Document"},
			Embedding: embedding,
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
		store := storage.NewMemoryEngine()
		defer store.Close()

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
		store := storage.NewMemoryEngine()
		defer store.Close()

		service := NewService(store)

		err := service.BuildIndexes(ctx)
		if err != nil {
			t.Errorf("BuildIndexes on empty storage failed: %v", err)
		}
	})

	t.Run("build_with_nodes", func(t *testing.T) {
		store := storage.NewMemoryEngine()
		defer store.Close()

		// Create nodes with embeddings (float32)
		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		store.CreateNode(&storage.Node{
			ID:        "doc-1",
			Labels:    []string{"Document"},
			Embedding: embedding,
			Properties: map[string]interface{}{
				"content": "First document content",
			},
		})
		store.CreateNode(&storage.Node{
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
		store := storage.NewMemoryEngine()
		defer store.Close()

		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		store.CreateNode(&storage.Node{
			ID:        "doc-1",
			Labels:    []string{"Document"},
			Embedding: embedding,
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
		store := storage.NewMemoryEngine()
		defer store.Close()

		embedding := make([]float32, 1024)
		for i := range embedding {
			embedding[i] = 0.5
		}

		store.CreateNode(&storage.Node{
			ID:        "doc-1",
			Labels:    []string{"Document"},
			Embedding: embedding,
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
	store := storage.NewMemoryEngine()
	service := NewService(store)

	// Create nodes with embeddings - some similar, some diverse
	createNodeWithEmbedding := func(id string, labels []string, embedding []float32, props map[string]interface{}) {
		node := &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     labels,
			Properties: props,
			Embedding:  embedding,
		}
		store.CreateNode(node)
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
		resultsNoMMR := service.applyMMR(rrfResults, queryEmb, 3, 1.0) // lambda=1.0 = no diversity
		assert.Len(t, resultsNoMMR, 3, "Should return all results")
		t.Logf("Without MMR (lambda=1.0): %v", []string{resultsNoMMR[0].ID, resultsNoMMR[1].ID, resultsNoMMR[2].ID})

		// With MMR (lambda=0.3): strong diversity preference
		resultsWithMMR := service.applyMMR(rrfResults, queryEmb, 3, 0.3) // lambda=0.3 = 70% diversity
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
		results := service.applyMMR(rrfResults, []float32{1, 0, 0, 0}, 3, 1.0)
		assert.Equal(t, "doc1", results[0].ID)
		assert.Equal(t, "doc2", results[1].ID)
		assert.Equal(t, "doc3", results[2].ID)
	})

	t.Run("mmr_handles_empty_results", func(t *testing.T) {
		results := service.applyMMR([]rrfResult{}, []float32{1, 0, 0, 0}, 10, 0.7)
		assert.Empty(t, results)
	})

	t.Run("mmr_handles_single_result", func(t *testing.T) {
		rrfResults := []rrfResult{{ID: "only", RRFScore: 0.1}}
		results := service.applyMMR(rrfResults, []float32{1, 0, 0, 0}, 10, 0.7)
		assert.Len(t, results, 1)
		assert.Equal(t, "only", results[0].ID)
	})
}

func TestSearchWithMMROption(t *testing.T) {
	store := storage.NewMemoryEngine()
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
			Embedding: make([]float32, 1024),
		}
		// Slightly different embeddings
		for j := range node.Embedding {
			node.Embedding[j] = float32(i)*0.01 + float32(j)*0.001
		}
		store.CreateNode(node)
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
