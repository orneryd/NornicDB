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
package search

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// SearchableProperties defines which node properties are included in full-text search.
// These match Mimir's Neo4j fulltext index configuration.
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

	// RRF metadata
	RRFScore   float64 `json:"rrf_score,omitempty"`
	VectorRank int     `json:"vector_rank,omitempty"`
	BM25Rank   int     `json:"bm25_rank,omitempty"`
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

	// MinSimilarity is the minimum similarity threshold for vector search
	MinSimilarity float64

	// Types filters results by node type (labels)
	Types []string

	// RRF configuration
	RRFK         float64 // RRF constant (default: 60)
	VectorWeight float64 // Weight for vector results (default: 1.0)
	BM25Weight   float64 // Weight for BM25 results (default: 1.0)
	MinRRFScore  float64 // Minimum RRF score threshold (default: 0.01)
}

// DefaultSearchOptions returns sensible defaults.
func DefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		Limit:         50,
		MinSimilarity: 0.5,
		RRFK:          60,
		VectorWeight:  1.0,
		BM25Weight:    1.0,
		MinRRFScore:   0.01,
	}
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
	engine        storage.Engine
	vectorIndex   *VectorIndex
	fulltextIndex *FulltextIndex
	mu            sync.RWMutex
}

// NewService creates a new search Service with empty indexes.
//
// The service is created with:
//   - 1024-dimensional vector index (default for mxbai-embed-large)
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
func NewService(engine storage.Engine) *Service {
	return &Service{
		engine:        engine,
		vectorIndex:   NewVectorIndex(1024), // Default to 1024 dimensions (mxbai-embed-large)
		fulltextIndex: NewFulltextIndex(),
	}
}

// IndexNode adds a node to all search indexes.
func (s *Service) IndexNode(node *storage.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add to vector index if node has embedding
	if len(node.Embedding) > 0 {
		// DEBUG: Print embedding info
		// fmt.Printf("DEBUG: IndexNode %s has %d-dim embedding\n", node.ID, len(node.Embedding))
		if err := s.vectorIndex.Add(string(node.ID), node.Embedding); err != nil {
			return err
		}
	}

	// Add to fulltext index
	text := s.extractSearchableText(node)
	if text != "" {
		s.fulltextIndex.Index(string(node.ID), text)
	}

	return nil
}

// RemoveNode removes a node from all search indexes.
func (s *Service) RemoveNode(nodeID storage.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.vectorIndex.Remove(string(nodeID))
	s.fulltextIndex.Remove(string(nodeID))
	return nil
}

// BuildIndexes builds search indexes from all nodes in the engine.
func (s *Service) BuildIndexes(ctx context.Context) error {
	// Get all nodes via the storage engine
	if exportable, ok := s.engine.(storage.ExportableEngine); ok {
		nodes, err := exportable.AllNodes()
		if err != nil {
			return err
		}

		for _, node := range nodes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := s.IndexNode(node); err != nil {
					// Log error but continue
					continue
				}
			}
		}
	}

	return nil
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
func (s *Service) Search(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	if opts == nil {
		opts = DefaultSearchOptions()
	}

	// If no embedding provided, fall back to full-text only
	if len(embedding) == 0 {
		return s.fullTextSearchOnly(ctx, query, opts)
	}

	// Try RRF hybrid search
	response, err := s.rrfHybridSearch(ctx, query, embedding, opts)
	if err == nil && len(response.Results) > 0 {
		return response, nil
	}

	// Fallback to vector-only
	response, err = s.vectorSearchOnly(ctx, embedding, opts)
	if err == nil && len(response.Results) > 0 {
		response.FallbackTriggered = true
		response.Message = "RRF search returned no results, fell back to vector search"
		return response, nil
	}

	// Final fallback to full-text
	return s.fullTextSearchOnly(ctx, query, opts)
}

// rrfHybridSearch performs Reciprocal Rank Fusion combining vector and BM25 results.
func (s *Service) rrfHybridSearch(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get more candidates for better fusion
	candidateLimit := opts.Limit * 2

	// Step 1: Vector search
	vectorResults, err := s.vectorIndex.Search(ctx, embedding, candidateLimit, opts.MinSimilarity)
	if err != nil {
		return nil, err
	}

	// Step 2: BM25 full-text search
	bm25Results := s.fulltextIndex.Search(query, candidateLimit)

	// Step 3: Filter by type if specified
	if len(opts.Types) > 0 {
		vectorResults = s.filterByType(vectorResults, opts.Types)
		bm25Results = s.filterByType(bm25Results, opts.Types)
	}

	// Step 4: Fuse with RRF
	fusedResults := s.fuseRRF(vectorResults, bm25Results, opts)

	// Step 5: Convert to SearchResult and enrich with node data
	results := s.enrichResults(fusedResults, opts.Limit)

	return &SearchResponse{
		Status:          "success",
		Query:           query,
		Results:         results,
		TotalCandidates: len(fusedResults),
		Returned:        len(results),
		SearchMethod:    "rrf_hybrid",
		Message:         "Reciprocal Rank Fusion (Vector + BM25)",
		Metrics: &SearchMetrics{
			VectorCandidates: len(vectorResults),
			BM25Candidates:   len(bm25Results),
			FusedCandidates:  len(fusedResults),
		},
	}, nil
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

	for id := range allIDs {
		var vectorComponent, bm25Component float64

		if rank, ok := vectorRanks[id]; ok {
			vectorComponent = opts.VectorWeight / (k + float64(rank))
		}
		if rank, ok := bm25Ranks[id]; ok {
			bm25Component = opts.BM25Weight / (k + float64(rank))
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

// vectorSearchOnly performs vector-only search.
func (s *Service) vectorSearchOnly(ctx context.Context, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results, err := s.vectorIndex.Search(ctx, embedding, opts.Limit*2, opts.MinSimilarity)
	if err != nil {
		return nil, err
	}

	if len(opts.Types) > 0 {
		results = s.filterByType(results, opts.Types)
	}

	searchResults := s.enrichIndexResults(results, opts.Limit)

	return &SearchResponse{
		Status:          "success",
		Results:         searchResults,
		TotalCandidates: len(results),
		Returned:        len(searchResults),
		SearchMethod:    "vector",
		Message:         "Vector similarity search (cosine)",
	}, nil
}

// fullTextSearchOnly performs full-text BM25 search only.
func (s *Service) fullTextSearchOnly(ctx context.Context, query string, opts *SearchOptions) (*SearchResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := s.fulltextIndex.Search(query, opts.Limit*2)

	if len(opts.Types) > 0 {
		results = s.filterByType(results, opts.Types)
	}

	searchResults := s.enrichIndexResults(results, opts.Limit)

	return &SearchResponse{
		Status:            "success",
		Query:             query,
		Results:           searchResults,
		TotalCandidates:   len(results),
		Returned:          len(searchResults),
		SearchMethod:      "fulltext",
		FallbackTriggered: true,
		Message:           "Full-text BM25 search (vector search unavailable or returned no results)",
	}, nil
}

// extractSearchableText extracts text from searchable properties.
func (s *Service) extractSearchableText(node *storage.Node) string {
	var parts []string

	for _, prop := range SearchableProperties {
		if val, ok := node.Properties[prop]; ok {
			switch v := val.(type) {
			case string:
				if v != "" {
					parts = append(parts, v)
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// filterByType filters results to only include specified node types.
func (s *Service) filterByType(results []indexResult, types []string) []indexResult {
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
func (s *Service) enrichResults(rrfResults []rrfResult, limit int) []SearchResult {
	var results []SearchResult

	for i, rrf := range rrfResults {
		if i >= limit {
			break
		}

		node, err := s.engine.GetNode(storage.NodeID(rrf.ID))
		if err != nil {
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
func (s *Service) enrichIndexResults(indexResults []indexResult, limit int) []SearchResult {
	var results []SearchResult

	for i, ir := range indexResults {
		if i >= limit {
			break
		}

		node, err := s.engine.GetNode(storage.NodeID(ir.ID))
		if err != nil {
			continue
		}

		result := SearchResult{
			ID:         ir.ID,
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

// sqrt returns the square root of x.
func sqrt(x float64) float64 {
	return math.Sqrt(x)
}
