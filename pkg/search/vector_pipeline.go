// Package search - Vector search pipeline with CandidateGen + ExactScore interfaces.
//
// This file implements the unified vector search pipeline as described in the
// hybrid-ann-plan.md. It provides:
//   - CandidateGenerator interface for approximate candidate generation (brute/HNSW)
//   - ExactScorer interface for exact scoring of candidates (CPU/GPU)
//   - Auto strategy selection (HNSW by default; brute-force via explicit thresholds)
//   - GPU-accelerated exact scoring when available
package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Configuration constants for the vector search pipeline.
const (
	// NSmallMax is the default maximum dataset size for automatic CPU brute-force
	// search. Zero makes HNSW the default unless NORNICDB_VECTOR_CPU_BRUTE_MAX_N
	// opts into CPU brute-force for datasets below that threshold.
	NSmallMax = 0

	// CandidateMultiplier determines how many candidates to generate relative to k.
	// Formula: C = max(k * CandidateMultiplier, 200)
	CandidateMultiplier = 20

	// MaxCandidates is the hard cap on candidate set size.
	MaxCandidates = 5000
)

func cpuBruteForceMaxN() int {
	n := envutil.GetInt("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", NSmallMax)
	if n < 0 {
		return 0
	}
	return n
}

// CandidateGenerator generates candidate vectors for approximate search.
//
// Implementations:
//   - BruteForceCandidateGen: Exact search over all vectors (explicit opt-in)
//   - HNSWCandidateGen: Approximate search using HNSW graph (default)
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
// This is available for explicit brute-force paths. The default vector pipeline
// uses HNSW unless NORNICDB_VECTOR_CPU_BRUTE_MAX_N is set above zero.
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

// FileStoreBruteForceCandidateGen implements CandidateGenerator using brute-force search
// directly over the file-backed vector store.
type FileStoreBruteForceCandidateGen struct {
	vectorStore *VectorFileStore
}

// NewFileStoreBruteForceCandidateGen creates a brute-force candidate generator over VectorFileStore.
func NewFileStoreBruteForceCandidateGen(vectorStore *VectorFileStore) *FileStoreBruteForceCandidateGen {
	return &FileStoreBruteForceCandidateGen{vectorStore: vectorStore}
}

// SearchCandidates scans all vectors from the file store and returns top candidates by exact cosine score.
func (b *FileStoreBruteForceCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	if b == nil || b.vectorStore == nil {
		return nil, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	candidateLimit := calculateCandidateLimit(k)
	normalizedQuery := vector.Normalize(query)
	initialCap := candidateLimit
	if initialCap > 1024 {
		initialCap = 1024
	}
	candidates := make([]Candidate, 0, initialCap)

	// Explicit brute-force path over file-backed vectors.
	if err := b.vectorStore.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for i := range ids {
			score := float64(vector.DotProduct(normalizedQuery, vecs[i]))
			if score < minSimilarity {
				continue
			}
			candidates = append(candidates, Candidate{ID: ids[i], Score: score})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > candidateLimit {
		candidates = candidates[:candidateLimit]
	}
	return candidates, nil
}

// HNSWCandidateGen implements CandidateGenerator using HNSW approximate search.
//
// This is the default vector-search candidate generator.
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
	resultLimit := candidateLimit
	searchBeam := h.hnswIndex.config.EfSearch
	if searchBeam < resultLimit {
		searchBeam = resultLimit
	}

	results, err := h.hnswIndex.SearchWithEf(ctx, query, resultLimit, minSimilarity, searchBeam)
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

// VectorGetter is implemented by *VectorIndex and by adapters for VectorLookup (e.g. file-backed store).
type VectorGetter interface {
	GetVector(id string) ([]float32, bool)
}

// CPUExactScorer implements ExactScorer using CPU-based exact scoring.
//
// Uses SIMD-optimized dot product for cosine similarity computation.
type CPUExactScorer struct {
	getter VectorGetter
}

// NewCPUExactScorer creates a new CPU-based exact scorer.
// getter can be *VectorIndex or any type that implements GetVector (e.g. file-store lookup adapter).
func NewCPUExactScorer(getter VectorGetter) *CPUExactScorer {
	return &CPUExactScorer{
		getter: getter,
	}
}

// ScoreCandidates computes exact scores for candidates using CPU.
func (c *CPUExactScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	if c.getter == nil {
		return nil, nil
	}
	normalizedQuery := vector.Normalize(query)
	if vfs, ok := c.getter.(*VectorFileStore); ok {
		return vfs.scoreCandidatesDot(ctx, normalizedQuery, candidates)
	}
	scored := make([]ScoredCandidate, 0, len(candidates))

	for _, cand := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		vec, exists := c.getter.GetVector(cand.ID)
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

	// NOTE: GPU exact scoring is intentionally disabled here for now.
	// The best use of GPU in the pipeline is subset re-scoring, but this path can
	// regress latency/throughput depending on GPU sync state and workload shape.
	// We keep the interface for future work but default to the SIMD CPU scorer.
	return g.cpuFallback.ScoreCandidates(ctx, query, candidates)
}

// GPUBruteForceCandidateGen uses gpu.EmbeddingIndex.Search() as an exact candidate generator.
//
// Note: gpu.EmbeddingIndex.Search() does not currently accept a context, so this
// generator cannot cancel mid-kernel. It checks ctx before invoking the search.
type GPUBruteForceCandidateGen struct {
	embeddingIndex *gpu.EmbeddingIndex
}

func NewGPUBruteForceCandidateGen(embeddingIndex *gpu.EmbeddingIndex) *GPUBruteForceCandidateGen {
	return &GPUBruteForceCandidateGen{embeddingIndex: embeddingIndex}
}

func (g *GPUBruteForceCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	if g.embeddingIndex == nil {
		return nil, fmt.Errorf("gpu embedding index unavailable")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	candidateLimit := calculateCandidateLimit(k)
	results, err := g.embeddingIndex.Search(query, candidateLimit)
	if err != nil {
		return nil, err
	}

	candidates := make([]Candidate, 0, len(results))
	for _, r := range results {
		score := float64(r.Score)
		if score < minSimilarity {
			continue
		}
		candidates = append(candidates, Candidate{ID: r.ID, Score: score})
	}
	return candidates, nil
}

// IdentityExactScorer is used when the candidate generator already returns exact scores.
type IdentityExactScorer struct{}

func (i *IdentityExactScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	scored := make([]ScoredCandidate, len(candidates))
	for idx, c := range candidates {
		scored[idx] = ScoredCandidate{ID: c.ID, Score: c.Score}
	}
	sort.Slice(scored, func(a, b int) bool { return scored[a].Score > scored[b].Score })
	return scored, nil
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
//  1. CandidateGen: Generate candidates (brute-force or HNSW)
//  2. ExactScore: Re-score candidates exactly (CPU or GPU)
//  3. Filter: Apply minSimilarity threshold
//  4. TopK: Return top-k results
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

	// Stage 3+4: Since scored is descending, apply threshold and top-k in one pass.
	if k <= 0 {
		k = len(scored)
	}
	limit := k
	if limit > len(scored) {
		limit = len(scored)
	}
	filtered := make([]ScoredCandidate, 0, util.SafePreallocCap(limit, len(scored)))
	for _, s := range scored {
		if s.Score < minSimilarity {
			break
		}
		filtered = append(filtered, s)
		if len(filtered) >= k {
			break
		}
	}
	return filtered, nil
}
