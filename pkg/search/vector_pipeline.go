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
	"cmp"
	"context"
	"math"
	"slices"
	"sort"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Configuration constants for the vector search pipeline.
const (
	// NSmallMax is the default maximum dataset size for automatic CPU brute-force
	// search. Zero makes HNSW the default unless NORNICDB_VECTOR_CPU_BRUTE_MAX_N
	// opts into CPU brute-force for datasets below that threshold.
	NSmallMax = 0
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

// approximateCandidateDepthPlanner separates ANN recall depth from the final
// result depth. Approximate generators need a wider exact-rescoring pool before
// chunk vectors are collapsed to their owning nodes.
type approximateCandidateDepthPlanner interface {
	preferredCandidateDepth(target, maximum int) int
}

// candidatePopulationReporter is a generator that knows how many vectors it
// searches. A request for at least that many candidates reaches all it ever
// will, so the adaptive widening stops there (requestCoversCandidatePopulation)
// instead of repeating the same search with ever larger limits.
type candidatePopulationReporter interface {
	candidatePopulation(ctx context.Context) int
}

// candidateGeneratorWithExhaustion lets generators distinguish a genuinely
// exhausted index from a short approximate result. That distinction keeps the
// adaptive search loop from treating an ANN miss as end-of-data.
type candidateGeneratorWithExhaustion interface {
	searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error)
}

type candidateGeneratorWithLexicalEntries interface {
	searchCandidatesWithLexicalEntries(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, bool, error)
}

// candidateEvidence is what a generator knows about the pool it selected.
// exhausted: the pool holds every live vector of its population, or every
// vector it left out scores below the threshold it was given. routedComplete:
// the pool holds everything the pass's routing reaches, so a wider limit on
// the same routing repeats the pass and only the population-wide routing can
// add more. A generator whose routing is its whole population reports
// routedComplete as exhausted.
type candidateEvidence struct {
	exhausted      bool
	routedComplete bool
}

// candidateGeneratorWithRouting is a generator that routes a request to part
// of its population (clusters, lists) and reports when a pass was bound by
// that routing rather than by its limit.
type candidateGeneratorWithRouting interface {
	searchCandidatesWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, candidateEvidence, error)
}

// candidatePrefilter is the threshold a generator may apply to its own scores
// on behalf of the exact stage, which alone decides qualification. A
// generator scores with its own routine (SIMD or GPU dot products, cosine over
// its copy of the vectors), so its value for a vector can differ from the
// stage's in the last float32 bits, and a vector at the threshold must not
// be dropped on that difference.
//
// Bound, with u = 2⁻²⁴ and d dimensions: a float32 dot product in any
// summation order differs from the real one by at most γ_d·Σ|aᵢbᵢ| ≤ d·u for
// normalized vectors (Σ|aᵢbᵢ| ≤ 1, Cauchy–Schwarz), and normalizing each
// vector perturbs it by at most d/2·u + 2u relative, so each routine's cosine
// is within (2d+4)·u of the real one and two routines within (4d+8)·u =
// (d+2)·2⁻²². ε = (d+4)·2⁻²² adds headroom for the second-order terms.
// Compressed scores (scalar, binary, PQ) have no such bound: their
// generators apply no threshold at all.
func candidatePrefilter(minSimilarity float64, dimensions int) float64 {
	if minSimilarity <= -1 {
		return minSimilarity
	}
	return minSimilarity - float64(dimensions+4)*math.Ldexp(1, -22)
}

// vectorMembersKey carries the Service's index-time node → vector membership
// to the exact stage of a query (withVectorMembers).
type vectorMembersKey struct{}

// vectorMemberResolver returns, for each node, every vector the node has
// indexed (main, chunks, named and property vectors). Nodes it doesn't know
// are absent from the result.
type vectorMemberResolver func(nodeIDs []string) map[string][]string

func withVectorMembers(ctx context.Context, members vectorMemberResolver) context.Context {
	return context.WithValue(ctx, vectorMembersKey{}, members)
}

func vectorMembersFromContext(ctx context.Context) vectorMemberResolver {
	members, _ := ctx.Value(vectorMembersKey{}).(vectorMemberResolver)
	return members
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
	candidates, _, err := b.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

// searchCandidatesWithExhaustion scores every vector exactly, so a page
// shorter than the limit holds every vector above minSimilarity.
func (b *BruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	candidateLimit := boundCandidateLimit(k)

	results, err := b.vectorIndex.Search(ctx, query, candidateLimit, minSimilarity)
	if err != nil {
		return nil, false, err
	}

	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{
			ID:    r.ID,
			Score: r.Score,
		}
	}

	return candidates, len(results) < candidateLimit, nil
}

func (b *BruteForceCandidateGen) candidatePopulation(ctx context.Context) int {
	return b.vectorIndex.Count()
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
	candidates, _, err := b.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

func (b *FileStoreBruteForceCandidateGen) candidatePopulation(ctx context.Context) int {
	if b == nil || b.vectorStore == nil {
		return 0
	}
	return b.vectorStore.Count()
}

// searchCandidatesWithExhaustion scans every stored vector exactly; it is
// exhausted when the limit cut none of those above minSimilarity.
func (b *FileStoreBruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	if b == nil || b.vectorStore == nil {
		return nil, true, nil
	}
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}

	candidateLimit := boundCandidateLimit(k)
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
		return nil, false, err
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	exhausted := len(candidates) <= candidateLimit
	if !exhausted {
		candidates = candidates[:candidateLimit]
	}
	return candidates, exhausted, nil
}

// HNSWCandidateGen implements CandidateGenerator using HNSW approximate search.
//
// This is the default vector-search candidate generator.
type HNSWCandidateGen struct {
	hnswIndex *HNSWIndex
}

func (h *HNSWCandidateGen) preferredCandidateDepth(target, maximum int) int {
	preferred := max(200, target*4)
	return min(preferred, maximum)
}

// NewHNSWCandidateGen creates a new HNSW candidate generator.
func NewHNSWCandidateGen(hnswIndex *HNSWIndex) *HNSWCandidateGen {
	return &HNSWCandidateGen{
		hnswIndex: hnswIndex,
	}
}

// candidatePopulation is the number of live vectors in the HNSW index: a
// search clamps its result count and beam to it, so a wider request repeats
// the same traversal.
func (h *HNSWCandidateGen) candidatePopulation(ctx context.Context) int {
	return h.hnswIndex.Size()
}

// SearchCandidates generates candidates using HNSW approximate search.
func (h *HNSWCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := h.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

func (h *HNSWCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	return h.searchCandidatesWithLexicalEntries(ctx, query, k, minSimilarity, nil)
}

func (h *HNSWCandidateGen) searchCandidatesWithLexicalEntries(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, bool, error) {
	quantized := h.quantizationActive(ctx)
	candidateLimit := quantizedCandidateLimit(ctx, k, quantized)
	searchBeam := h.searchBeam(ctx, candidateLimit)
	if quantized {
		searchBeam = max(searchBeam, candidateLimit)
	}

	// Ask HNSW to retain the whole beam. Selecting only k vectors inside the
	// graph traversal clamps recall to ef == k even when traversal was wider.
	// The widened result also gives chunked corpora room to produce k distinct
	// owning nodes rather than spending the entire budget on one document.
	results, exhausted, err := h.hnswIndex.searchWithEfExhaustionFromEntries(ctx, query, searchBeam, minSimilarity, searchBeam, entryIDs)
	if err != nil {
		return nil, false, err
	}

	// The limit counts distinct nodes, so a chunked corpus gets candidateLimit
	// documents to rescore rather than one document's chunks. That is
	// selection only: every vector the beam surfaced for a selected node is
	// kept, and the exact stage scores all of the node's vectors before it
	// decides the node's score.
	candidates := make([]Candidate, 0, min(candidateLimit, len(results)))
	selected := make(map[string]struct{}, min(candidateLimit, len(results)))
	for _, r := range results {
		nodeID := normalizeVectorResultIDToNodeID(r.ID)
		if _, ok := selected[nodeID]; !ok {
			if len(selected) == candidateLimit {
				// A further node was cut here, so the search isn't exhausted
				// even when the beam covered the whole index. A filtered
				// search widens past it rather than ending with the page short.
				exhausted = false
				break
			}
			selected[nodeID] = struct{}{}
		}
		candidates = append(candidates, Candidate{
			ID:    r.ID,
			Score: float64(r.Score),
		})
	}

	return candidates, exhausted, nil
}

// searchBeam is the HNSW beam width (ef) for a request for candidateLimit
// candidates: the request's own ef (SearchOptions.HNSWEf) when it has one,
// widened only to hold the candidates asked for; otherwise the configured
// EfSearch or candidateLimit times the beam factor, whichever is larger.
func (h *HNSWCandidateGen) searchBeam(ctx context.Context, candidateLimit int) int {
	if ef := hnswEfFromContext(ctx); ef > 0 {
		return max(ef, candidateLimit)
	}
	config := h.hnswIndex.Config()
	return hnswSearchBeam(candidateLimit, config.EfSearch, config.SearchBeamFactor)
}

func hnswSearchBeam(resultLimit, configuredEf, factor int) int {
	if factor <= 0 {
		factor = defaultHNSWSearchBeamFactor
	}
	beam, ok := util.SafeIntProduct(resultLimit, factor)
	if !ok {
		beam = int(^uint(0) >> 1)
	}
	return max(configuredEf, beam)
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

		var vec []float32
		var exists bool
		if index, ok := c.getter.(*VectorIndex); ok {
			vec, exists = index.getVectorRef(cand.ID) // read-only: no copy
		} else {
			vec, exists = c.getter.GetVector(cand.ID)
		}
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
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

func (g *GPUBruteForceCandidateGen) candidatePopulation(ctx context.Context) int {
	if g.embeddingIndex == nil {
		return 0
	}
	return g.embeddingIndex.Count()
}

// searchCandidatesWithExhaustion scores every vector: a page shorter than the
// limit, or one that already reaches below minSimilarity, holds every vector
// above it.
func (g *GPUBruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	if g.embeddingIndex == nil {
		return nil, false, localizedError(localization.SearchGPUEmbeddingUnavailable(), nil)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	candidateLimit := boundCandidateLimit(k)
	results, err := g.embeddingIndex.Search(query, candidateLimit)
	if err != nil {
		return nil, false, err
	}

	exhausted := len(results) < candidateLimit
	candidates := make([]Candidate, 0, len(results))
	for _, r := range results {
		score := float64(r.Score)
		if score < minSimilarity {
			exhausted = true // results are best first: the rest score lower
			continue
		}
		candidates = append(candidates, Candidate{ID: r.ID, Score: score})
	}
	return candidates, exhausted, nil
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

func boundCandidateLimit(k int) int {
	if k <= 0 {
		return 0
	}
	return k
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
	results, _, err := p.searchWithExhaustion(ctx, query, k, minSimilarity)
	return results, err
}

func (p *VectorSearchPipeline) searchWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]ScoredCandidate, bool, error) {
	return p.searchWithExhaustionFromEntries(ctx, query, k, minSimilarity, nil)
}

func (p *VectorSearchPipeline) searchWithExhaustionFromEntries(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]ScoredCandidate, bool, error) {
	results, evidence, err := p.searchWithEvidence(ctx, query, k, minSimilarity, entryIDs)
	return results, evidence.exhausted, err
}

// searchWithEvidence selects candidates with the generator and hands them to
// the exact stage (scoreNodes), which alone decides each result's node,
// score, qualification and the cut to k nodes. The generator gets the
// threshold widened by candidatePrefilter, so a vector its own routine scores
// a hair below the threshold still reaches the stage.
func (p *VectorSearchPipeline) searchWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]ScoredCandidate, candidateEvidence, error) {
	candidates, evidence, err := p.selectCandidates(ctx, query, k, candidatePrefilter(minSimilarity, len(query)), entryIDs)
	if err != nil {
		return nil, candidateEvidence{}, localizedError(localization.SearchCandidateGenerationFailed(err), err)
	}
	if len(candidates) == 0 {
		return []ScoredCandidate{}, evidence, nil
	}
	scored, cut, err := p.scoreNodes(ctx, query, candidates, k, minSimilarity)
	if err != nil {
		return nil, candidateEvidence{}, err
	}
	if cut {
		// Nodes above the threshold cut here weren't returned: a wider
		// request returns them.
		evidence = candidateEvidence{}
	}
	return scored, evidence, nil
}

// selectCandidates asks the generator which vectors to score, with the
// evidence it reports for them. A generator that doesn't report exhaustion
// is never exhausted: a short page from an approximate generator is no
// evidence that nothing else matches.
func (p *VectorSearchPipeline) selectCandidates(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, candidateEvidence, error) {
	if generator, ok := p.candidateGen.(candidateGeneratorWithLexicalEntries); ok && len(entryIDs) > 0 {
		candidates, exhausted, err := generator.searchCandidatesWithLexicalEntries(ctx, query, k, minSimilarity, entryIDs)
		return candidates, candidateEvidence{exhausted: exhausted, routedComplete: exhausted}, err
	}
	if generator, ok := p.candidateGen.(candidateGeneratorWithRouting); ok {
		return generator.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	}
	if generator, ok := p.candidateGen.(candidateGeneratorWithExhaustion); ok {
		candidates, exhausted, err := generator.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
		return candidates, candidateEvidence{exhausted: exhausted, routedComplete: exhausted}, err
	}
	candidates, err := p.candidateGen.SearchCandidates(ctx, query, k, minSimilarity)
	return candidates, candidateEvidence{}, err
}

// scoreNodes is the exact stage, the one owner of a vector result's identity,
// score, qualification and cut. Candidates only name the nodes to score: the
// stage expands each to every vector it has indexed (expandToNodeMembers),
// scores them with the request's scoring policy (scoreFinal), and gives the
// node its best vector's score. Then the threshold applies and the ranking
// is cut to k nodes (k <= 0 keeps all); cut reports that a node above the
// threshold was cut. Each result carries its node's best vector ID.
func (p *VectorSearchPipeline) scoreNodes(ctx context.Context, query []float32, candidates []Candidate, k int, minSimilarity float64) ([]ScoredCandidate, bool, error) {
	scored, err := p.scoreFinal(ctx, query, p.expandToNodeMembers(ctx, candidates))
	if err != nil {
		return nil, false, err
	}
	nodes := make([]ScoredCandidate, 0, len(scored))
	position := make(map[string]int, len(scored))
	for _, s := range scored {
		nodeID := normalizeVectorResultIDToNodeID(s.ID)
		if i, ok := position[nodeID]; ok {
			if s.Score > nodes[i].Score {
				nodes[i] = s
			}
			continue
		}
		position[nodeID] = len(nodes)
		nodes = append(nodes, s)
	}
	slices.SortFunc(nodes, func(a, b ScoredCandidate) int {
		if order := cmp.Compare(b.Score, a.Score); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	qualifying := 0
	for qualifying < len(nodes) && nodes[qualifying].Score >= minSimilarity {
		qualifying++
	}
	if k > 0 && qualifying > k {
		return nodes[:k], true, nil
	}
	return nodes[:qualifying], false, nil
}

// expandToNodeMembers returns the candidates' vectors followed by every other
// vector their nodes have indexed, from the Service's membership
// (withVectorMembers). Approximate order chooses which nodes reach the stage;
// it never decides which of a node's vectors are scored. A pipeline used
// without a Service, or one whose scorer passes candidate scores through
// (IdentityExactScorer), scores the candidates alone.
func (p *VectorSearchPipeline) expandToNodeMembers(ctx context.Context, candidates []Candidate) []Candidate {
	members := vectorMembersFromContext(ctx)
	if members == nil {
		return candidates
	}
	if _, passThrough := p.exactScorer.(*IdentityExactScorer); passThrough {
		return candidates
	}
	pool := make([]Candidate, 0, len(candidates))
	inPool := make(map[string]struct{}, len(candidates))
	nodeIDs := make([]string, 0, len(candidates))
	nodeSeen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if _, ok := inPool[candidate.ID]; ok {
			continue
		}
		inPool[candidate.ID] = struct{}{}
		pool = append(pool, candidate)
		nodeID := normalizeVectorResultIDToNodeID(candidate.ID)
		if _, ok := nodeSeen[nodeID]; !ok {
			nodeSeen[nodeID] = struct{}{}
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	byNode := members(nodeIDs)
	for _, nodeID := range nodeIDs {
		for _, vectorID := range byNode[nodeID] {
			if _, ok := inPool[vectorID]; ok {
				continue
			}
			inPool[vectorID] = struct{}{}
			pool = append(pool, Candidate{ID: vectorID})
		}
	}
	return pool
}

// scoreFinal scores the pool with the request's scoring policy: the exact
// scorer, or, when the request turned rescoring off on a compressed graph,
// that graph's codes (quantizedScoreReader, which the indexed query uses
// too), so every decision of the request is made on one scale.
func (p *VectorSearchPipeline) scoreFinal(ctx context.Context, query []float32, pool []Candidate) ([]ScoredCandidate, error) {
	if h, ok := p.candidateGen.(*HNSWCandidateGen); ok && h.quantizationActive(ctx) && !quantizationOptions(ctx).rescore {
		reader, release := h.hnswIndex.quantizedScoreReader(vector.Normalize(query))
		defer release()
		if reader != nil {
			scored := make([]ScoredCandidate, 0, len(pool))
			for _, candidate := range pool {
				if score, ok := reader(candidate.ID); ok {
					scored = append(scored, ScoredCandidate{ID: candidate.ID, Score: score})
				}
			}
			return scored, ctx.Err()
		}
	}
	scored, err := p.exactScorer.ScoreCandidates(ctx, query, pool)
	if err != nil {
		return nil, localizedError(localization.SearchExactScoringFailed(err), err)
	}
	return scored, nil
}
