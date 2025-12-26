// Package search - Vector search pipeline with CandidateGen + ExactScore interfaces.
//
// This file implements the unified vector search pipeline as described in the
// hybrid-ann-plan.md. It provides:
//   - CandidateGenerator interface for approximate candidate generation (brute/HNSW)
//   - ExactScorer interface for exact scoring of candidates (CPU/GPU)
//   - Auto strategy selection (brute vs HNSW based on dataset size)
//   - GPU-accelerated exact scoring when available
package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// Configuration constants for the vector search pipeline.
const (
	// NSmallMax is the maximum dataset size for which brute-force search is used.
	// Below this threshold, brute-force is often faster than ANN overhead.
	NSmallMax = 5000

	// CandidateMultiplier determines how many candidates to generate relative to k.
	// Formula: C = max(k * CandidateMultiplier, 200)
	CandidateMultiplier = 20

	// MaxCandidates is the hard cap on candidate set size.
	MaxCandidates = 5000
)

// CandidateGenerator generates candidate vectors for approximate search.
//
// Implementations:
//   - BruteForceCandidateGen: Exact search over all vectors (for small N)
//   - HNSWCandidateGen: Approximate search using HNSW graph (for large N)
//
// The generator returns candidate IDs and approximate scores. These candidates
// will be re-scored exactly by ExactScorer before final ranking.
type CandidateGenerator interface {
	// SearchCandidates generates candidate vectors for the given query.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - query: Normalized query vector
	//   - k: Desired number of results
	//   - minSimilarity: Minimum similarity threshold (approximate, may be relaxed)
	//
	// Returns:
	//   - candidates: Candidate IDs with approximate scores (may be more than k)
	//   - error: Context cancellation or other errors
	SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error)
}

// ExactScorer computes exact similarity scores for candidate vectors.
//
// Implementations:
//   - CPUExactScorer: CPU-based exact scoring (SIMD-optimized)
//   - GPUExactScorer: GPU-accelerated exact scoring (when available)
//
// The scorer takes candidate IDs and returns exact scores using the true metric
// (cosine/dot/euclid), ensuring final ranking accuracy.
type ExactScorer interface {
	// ScoreCandidates computes exact similarity scores for the given candidates.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - query: Normalized query vector
	//   - candidates: Candidate IDs to score (may include approximate scores for reference)
	//
	// Returns:
	//   - scored: Candidates with exact scores
	//   - error: Context cancellation or other errors
	ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error)
}

// Candidate represents a candidate vector with approximate score.
type Candidate struct {
	ID    string
	Score float64 // Approximate score from candidate generation
}

// ScoredCandidate represents a candidate with exact score.
type ScoredCandidate struct {
	ID    string
	Score float64 // Exact score from exact scorer
}

// BruteForceCandidateGen implements CandidateGenerator using brute-force search.
//
// This is optimal for small datasets (N < NSmallMax) where ANN overhead
// dominates brute-force computation time.
type BruteForceCandidateGen struct {
	vectorIndex *VectorIndex
}

// NewBruteForceCandidateGen creates a new brute-force candidate generator.
func NewBruteForceCandidateGen(vectorIndex *VectorIndex) *BruteForceCandidateGen {
	return &BruteForceCandidateGen{
		vectorIndex: vectorIndex,
	}
}

// SearchCandidates generates candidates using brute-force search.
func (b *BruteForceCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	// For brute-force, we generate more candidates than k to allow for filtering
	candidateLimit := calculateCandidateLimit(k)
	
	results, err := b.vectorIndex.Search(ctx, query, candidateLimit, minSimilarity)
	if err != nil {
		return nil, err
	}

	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{
			ID:    r.ID,
			Score: r.Score,
		}
	}

	return candidates, nil
}

// HNSWCandidateGen implements CandidateGenerator using HNSW approximate search.
//
// This is optimal for large datasets (N >= NSmallMax) where ANN provides
// significant speedup over brute-force.
type HNSWCandidateGen struct {
	hnswIndex *HNSWIndex
}

// NewHNSWCandidateGen creates a new HNSW candidate generator.
func NewHNSWCandidateGen(hnswIndex *HNSWIndex) *HNSWCandidateGen {
	return &HNSWCandidateGen{
		hnswIndex: hnswIndex,
	}
}

// SearchCandidates generates candidates using HNSW approximate search.
func (h *HNSWCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	// For HNSW, we generate more candidates than k for exact reranking
	candidateLimit := calculateCandidateLimit(k)
	
	results, err := h.hnswIndex.Search(ctx, query, candidateLimit, minSimilarity)
	if err != nil {
		return nil, err
	}

	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{
			ID:    r.ID,
			Score: float64(r.Score),
		}
	}

	return candidates, nil
}

// CPUExactScorer implements ExactScorer using CPU-based exact scoring.
//
// Uses SIMD-optimized dot product for cosine similarity computation.
type CPUExactScorer struct {
	vectorIndex *VectorIndex
}

// NewCPUExactScorer creates a new CPU-based exact scorer.
func NewCPUExactScorer(vectorIndex *VectorIndex) *CPUExactScorer {
	return &CPUExactScorer{
		vectorIndex: vectorIndex,
	}
}

// ScoreCandidates computes exact scores for candidates using CPU.
func (c *CPUExactScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	c.vectorIndex.mu.RLock()
	defer c.vectorIndex.mu.RUnlock()

	normalizedQuery := vector.Normalize(query)
	scored := make([]ScoredCandidate, 0, len(candidates))

	for _, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		vec, exists := c.vectorIndex.vectors[cand.ID]
		if !exists {
			continue // Skip candidates that no longer exist
		}

		// Exact cosine similarity: dot product of normalized vectors
		score := vector.DotProduct(normalizedQuery, vec)
		scored = append(scored, ScoredCandidate{
			ID:    cand.ID,
			Score: float64(score),
		})
	}

	// Sort by exact score descending
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	return scored, nil
}

// GPUExactScorer implements ExactScorer using GPU-accelerated exact scoring.
//
// Falls back to CPU if GPU is unavailable or unhealthy.
type GPUExactScorer struct {
	embeddingIndex *gpu.EmbeddingIndex
	cpuFallback    *CPUExactScorer
}

// NewGPUExactScorer creates a new GPU-based exact scorer.
func NewGPUExactScorer(embeddingIndex *gpu.EmbeddingIndex, cpuFallback *CPUExactScorer) *GPUExactScorer {
	return &GPUExactScorer{
		embeddingIndex: embeddingIndex,
		cpuFallback:    cpuFallback,
	}
}

// ScoreCandidates computes exact scores for candidates using GPU when available.
//
// Note: Since EmbeddingIndex.Search() searches all vectors, we use it for
// candidate scoring by searching with a large k, then filtering to only
// the candidates we care about. This is not optimal but works with the
// current EmbeddingIndex API. A future PR will add ScoreSubset() for
// true subset scoring.
func (g *GPUExactScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	if g.embeddingIndex == nil {
		// Fall back to CPU if no GPU index
		return g.cpuFallback.ScoreCandidates(ctx, query, candidates)
	}

	// For now, use CPU fallback since EmbeddingIndex doesn't have ScoreSubset.
	// TODO: Implement ScoreSubset on EmbeddingIndex in PR5
	// When ScoreSubset is available, we can check GPU availability via Stats()
	// and use GPU path for subset scoring.
	return g.cpuFallback.ScoreCandidates(ctx, query, candidates)
}

// calculateCandidateLimit calculates the number of candidates to generate.
//
// Formula: C = max(k * CandidateMultiplier, 200) capped by MaxCandidates
func calculateCandidateLimit(k int) int {
	candidateLimit := k * CandidateMultiplier
	if candidateLimit < 200 {
		candidateLimit = 200
	}
	if candidateLimit > MaxCandidates {
		candidateLimit = MaxCandidates
	}
	return candidateLimit
}

// VectorSearchPipeline implements the unified vector search pipeline.
//
// Pipeline stages:
//   1. CandidateGen: Generate candidates (brute-force or HNSW)
//   2. ExactScore: Re-score candidates exactly (CPU or GPU)
//   3. Filter: Apply minSimilarity threshold
//   4. TopK: Return top-k results
type VectorSearchPipeline struct {
	candidateGen CandidateGenerator
	exactScorer  ExactScorer
}

// NewVectorSearchPipeline creates a new vector search pipeline.
func NewVectorSearchPipeline(candidateGen CandidateGenerator, exactScorer ExactScorer) *VectorSearchPipeline {
	return &VectorSearchPipeline{
		candidateGen: candidateGen,
		exactScorer:  exactScorer,
	}
}

// Search performs vector search using the pipeline.
//
// Parameters:
//   - ctx: Context for cancellation
//   - query: Query vector (will be normalized)
//   - k: Desired number of results
//   - minSimilarity: Minimum similarity threshold
//
// Returns:
//   - candidates: Top-k candidates with exact scores
//   - error: Context cancellation or other errors
func (p *VectorSearchPipeline) Search(ctx context.Context, query []float32, k int, minSimilarity float64) ([]ScoredCandidate, error) {
	// Stage 1: Candidate generation
	candidates, err := p.candidateGen.SearchCandidates(ctx, query, k, minSimilarity)
	if err != nil {
		return nil, fmt.Errorf("candidate generation failed: %w", err)
	}

	if len(candidates) == 0 {
		return []ScoredCandidate{}, nil
	}

	// Stage 2: Exact scoring
	scored, err := p.exactScorer.ScoreCandidates(ctx, query, candidates)
	if err != nil {
		return nil, fmt.Errorf("exact scoring failed: %w", err)
	}

	// Stage 3: Filter by minSimilarity
	filtered := make([]ScoredCandidate, 0, len(scored))
	for _, s := range scored {
		if s.Score >= minSimilarity {
			filtered = append(filtered, s)
		}
	}

	// Stage 4: Top-k
	if len(filtered) > k {
		filtered = filtered[:k]
	}

	return filtered, nil
}

