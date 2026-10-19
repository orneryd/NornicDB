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
// routedComplete as exhausted. scoreBound (when bounded): how far a
// candidate's exact score can lie above the score the generator gave it,
// before float32 slack; the exact stage skips nodes whose bound can no
// longer qualify or reach the top k.
type candidateEvidence struct {
	exhausted      bool
	routedComplete bool
	scoreBound     float64
	bounded        bool
}

// bound is the evidence's score bound, +Inf when the generator gave none.
func (e candidateEvidence) bound() float64 {
	if !e.bounded {
		return math.Inf(1)
	}
	return e.scoreBound
}

// withBound returns e reporting score bound b.
func (e candidateEvidence) withBound(b float64) candidateEvidence {
	e.scoreBound, e.bounded = b, true
	return e
}

// candidateGeneratorWithEntryEvidence is the HNSW generator: lexical entry
// points and evidence, score bound included, in one search.
type candidateGeneratorWithEntryEvidence interface {
	searchCandidatesWithEntryEvidence(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, candidateEvidence, error)
}

// candidateScoreBounder is a generator whose scores are exact up to float32
// slack (bound 0).
type candidateScoreBounder interface {
	candidateScoreBound() float64
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
// Compressed scores (scalar, binary, PQ) get no threshold at all: their
// generators bound their error separately (candidateEvidence.scoreBound).
func candidatePrefilter(minSimilarity float64, dimensions int) float64 {
	if minSimilarity <= -1 {
		return minSimilarity
	}
	return minSimilarity - exactScoreSlack(dimensions)
}

// exactScoreSlack is ε = (d+4)·2⁻²² (candidatePrefilter): the most two
// float32 routines' cosines of the same unit vectors differ.
func exactScoreSlack(dimensions int) float64 {
	return float64(dimensions+4) * math.Ldexp(1, -22)
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

// candidateScoreBound: its scores are exact up to float32 slack.
func (b *BruteForceCandidateGen) candidateScoreBound() float64 { return 0 }

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

// candidateScoreBound: its scores are exact up to float32 slack.
func (b *FileStoreBruteForceCandidateGen) candidateScoreBound() float64 { return 0 }

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
	candidates, evidence, err := h.searchCandidatesWithEntryEvidence(ctx, query, k, minSimilarity, entryIDs)
	return candidates, evidence.exhausted, err
}

// searchCandidatesWithEntryEvidence searches the graph from the lexical entry
// points (none: its entry point). The graph scores original coordinates, so
// its scores are exact up to float32 slack (bound 0).
func (h *HNSWCandidateGen) searchCandidatesWithEntryEvidence(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, candidateEvidence, error) {
	candidateLimit := boundCandidateLimit(k)
	config := h.hnswIndex.Config()
	searchBeam := hnswSearchBeam(candidateLimit, config.EfSearch, config.SearchBeamFactor)

	// Ask HNSW to retain the whole beam. Selecting only k vectors inside the
	// graph traversal clamps recall to ef == k even when traversal was wider.
	// The widened result also gives chunked corpora room to produce k distinct
	// owning nodes rather than spending the entire budget on one document.
	results, exhausted, err := h.hnswIndex.searchWithEfExhaustionFromEntries(ctx, query, searchBeam, minSimilarity, searchBeam, entryIDs)
	if err != nil {
		return nil, candidateEvidence{}, err
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

	return candidates, candidateEvidence{exhausted: exhausted, routedComplete: exhausted}.withBound(0), nil
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
	scored, err := c.scoreCandidateValues(ctx, query, candidates)
	if err != nil {
		return nil, err
	}
	if _, fileScored := c.getter.(*VectorFileStore); fileScored {
		return scored, nil // the file scorer already returns score order
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	return scored, nil
}

// scoreCandidateValues owns CPU scoring without imposing an output order.
// The pipeline collapses member scores to nodes and orders those nodes itself;
// sorting all member vectors before that collapse repeats discarded work.
func (c *CPUExactScorer) scoreCandidateValues(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	if c.getter == nil {
		return nil, nil
	}
	if vfs, ok := c.getter.(*VectorFileStore); ok {
		return vfs.scoreCandidatesDot(ctx, vector.Normalize(query), candidates)
	}
	scored := make([]ScoredCandidate, 0, len(candidates))
	if len(candidates) == 0 {
		return scored, nil
	}
	err := c.withVectorScores(ctx, query, func(read vectorScoreReader) error {
		for _, candidate := range candidates {
			score, ok, err := read(candidate.ID)
			if err != nil {
				return err
			}
			if ok {
				scored = append(scored, ScoredCandidate{ID: candidate.ID, Score: score})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return scored, nil
}

type vectorScoreReader func(string) (float64, bool, error)

// withVectorScores owns CPU value computation for both batch output and
// direct node accumulation. An in-memory batch borrows the index once; its
// callback reads no Service state while holding that read lock.
func (c *CPUExactScorer) withVectorScores(ctx context.Context, query []float32, visit func(vectorScoreReader) error) error {
	if c.getter == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	normalizedQuery := vector.Normalize(query)
	lookup := c.getter.GetVector
	if index, ok := c.getter.(*VectorIndex); ok {
		index.mu.RLock()
		defer index.mu.RUnlock()
		lookup = index.getVectorRefLocked
	}
	return visit(func(id string) (float64, bool, error) {
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
		}
		vec, ok := lookup(id)
		if !ok {
			return 0, false, nil
		}
		return float64(vector.DotProduct(normalizedQuery, vec)), true, nil
	})
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

// candidateScoreBound: its scores are exact up to float32 slack.
func (g *GPUBruteForceCandidateGen) candidateScoreBound() float64 { return 0 }

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
	scored, cut, err := p.scoreNodes(ctx, query, candidates, k, minSimilarity, evidence.bound()+exactScoreSlack(len(query)))
	if err != nil {
		return nil, candidateEvidence{}, err
	}
	if cut {
		// Nodes that can still qualify weren't returned: a wider request
		// returns them.
		evidence.exhausted, evidence.routedComplete = false, false
	}
	return scored, evidence, nil
}

// selectCandidates asks the generator which vectors to score, with the
// evidence it reports for them. A generator that doesn't report exhaustion
// is never exhausted: a short page from an approximate generator is no
// evidence that nothing else matches.
func (p *VectorSearchPipeline) selectCandidates(ctx context.Context, query []float32, k int, minSimilarity float64, entryIDs []string) ([]Candidate, candidateEvidence, error) {
	if generator, ok := p.candidateGen.(candidateGeneratorWithEntryEvidence); ok {
		return generator.searchCandidatesWithEntryEvidence(ctx, query, k, minSimilarity, entryIDs)
	}
	if generator, ok := p.candidateGen.(candidateGeneratorWithLexicalEntries); ok && len(entryIDs) > 0 {
		candidates, exhausted, err := generator.searchCandidatesWithLexicalEntries(ctx, query, k, minSimilarity, entryIDs)
		return candidates, candidateEvidence{exhausted: exhausted, routedComplete: exhausted}, err
	}
	if generator, ok := p.candidateGen.(candidateGeneratorWithRouting); ok {
		return generator.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	}
	var evidence candidateEvidence
	if bounder, ok := p.candidateGen.(candidateScoreBounder); ok {
		evidence = evidence.withBound(bounder.candidateScoreBound())
	}
	if generator, ok := p.candidateGen.(candidateGeneratorWithExhaustion); ok {
		candidates, exhausted, err := generator.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
		evidence.exhausted, evidence.routedComplete = exhausted, exhausted
		return candidates, evidence, err
	}
	candidates, err := p.candidateGen.SearchCandidates(ctx, query, k, minSimilarity)
	return candidates, evidence, err
}

// stageNode is a node in the exact stage: the vectors its generator
// surfaced, the most its exact score can be (upper), and, once scored, its
// best vector.
type stageNode struct {
	id     string
	upper  float64
	first  Candidate
	extra  []Candidate
	best   ScoredCandidate
	scored bool
}

// stageBatch is how many nodes the exact stage scores at a time, at least.
const stageBatch = 32

// scoreNodes is the exact stage, the one owner of a vector result's identity,
// score, qualification and cut. Candidates only name the nodes to score; each
// key is decoded once. A node's upper bound is its best surfaced score plus
// bound (how far an exact score can lie above a candidate's score). Nodes are
// scored best bound first, a batch at a time: every vector the node has
// indexed (vectorMembersFromContext), with the request's scoring policy
// (scoreFinal), and the node takes its best vector's score. Scoring stops
// when no remaining node's bound reaches the threshold or, once k nodes
// qualify, the kth score: those nodes can't enter the page. Then the
// threshold applies and the ranking is cut to k nodes (k <= 0 keeps all).
// cut reports that a node that can still qualify isn't in the result: one
// cut from the page, or one not scored. Each result carries its node's best
// vector ID. With bound +Inf (no bound) every node is scored.
func (p *VectorSearchPipeline) scoreNodes(ctx context.Context, query []float32, candidates []Candidate, k int, minSimilarity float64, bound float64) ([]ScoredCandidate, bool, error) {
	_, passThrough := p.exactScorer.(*IdentityExactScorer)
	if passThrough {
		bound = 0 // candidate scores are final
	}
	nodes := groupCandidatesByNode(candidates, bound, minSimilarity, k)
	slices.SortStableFunc(nodes, func(a, b *stageNode) int { return cmp.Compare(b.upper, a.upper) })

	var members vectorMemberResolver
	if !passThrough {
		members = vectorMembersFromContext(ctx)
	}
	qualifying := make([]float64, 0, max(k, 0)) // min-heap of the best k qualifying scores
	next := 0
	for next < len(nodes) {
		bar := minSimilarity
		if k > 0 && len(qualifying) >= k {
			bar = max(bar, qualifying[0])
		}
		if nodes[next].upper < bar {
			break
		}
		end := next + 1
		batch := max(stageBatch, k-len(qualifying))
		if math.IsInf(bound, 1) {
			batch = len(nodes) // no bound: every node is scored anyway
		}
		for end < len(nodes) && end-next < batch && nodes[end].upper >= bar {
			end++
		}
		if err := p.scoreStageBatch(ctx, query, nodes[next:end], members); err != nil {
			return nil, false, err
		}
		for _, n := range nodes[next:end] {
			if n.scored && n.best.Score >= minSimilarity {
				qualifying = pushBestScores(qualifying, n.best.Score, k)
			}
		}
		next = end
	}

	cut := false
	for _, n := range nodes[next:] {
		if n.upper >= minSimilarity {
			cut = true // could qualify, not scored
			break
		}
	}
	out := make([]ScoredCandidate, 0, min(next, max(k, 0)+1))
	for _, n := range nodes[:next] {
		if n.scored && n.best.Score >= minSimilarity {
			out = append(out, n.best)
		}
	}
	slices.SortFunc(out, func(a, b ScoredCandidate) int {
		if order := cmp.Compare(b.Score, a.Score); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	if k > 0 && len(out) > k {
		return out[:k], true, nil
	}
	return out, cut, nil
}

// scoreAllNodes is the exact stage for the completion pass, whose vectors
// are every vector of every node it can return: scored exactly (scoreFinal),
// a node's first vector in score order is its best, so the ranking stops at
// the threshold and decodes only the vectors above it. Each result carries
// its node's best vector ID, best first.
func (p *VectorSearchPipeline) scoreAllNodes(ctx context.Context, query []float32, vectors []Candidate, minSimilarity float64) ([]ScoredCandidate, error) {
	scored, err := p.scoreFinal(ctx, query, vectors)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(scored, func(a, b ScoredCandidate) int {
		if order := cmp.Compare(b.Score, a.Score); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	out := make([]ScoredCandidate, 0)
	seen := make(map[string]struct{})
	for _, s := range scored {
		if s.Score < minSimilarity {
			break
		}
		nodeID := normalizeVectorResultIDToNodeID(s.ID)
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, s)
	}
	return out, nil
}

// groupCandidatesByNode groups candidates by owning node, in first-seen
// order, decoding each eligible key once. Candidates whose upper bound is
// below the threshold cannot contribute to a qualifying node's best score.
// The nodes share one backing allocation, and their first candidate is inline;
// only nodes with additional surfaced vectors need a separate candidate slice.
func groupCandidatesByNode(candidates []Candidate, bound, minSimilarity float64, k int) []*stageNode {
	eligible := 0
	for _, candidate := range candidates {
		if !(candidate.Score+bound < minSimilarity) {
			eligible++
		}
	}
	capacity := eligible
	if k > 0 {
		capacity = min(capacity, k)
	}
	storage := make([]stageNode, 0, capacity)
	position := make(map[string]int, capacity)
	for _, candidate := range candidates {
		upper := candidate.Score + bound
		if upper < minSimilarity {
			continue
		}
		nodeID := normalizeVectorResultIDToNodeID(candidate.ID)
		if i, ok := position[nodeID]; ok {
			n := &storage[i]
			n.extra = append(n.extra, candidate)
			n.upper = max(n.upper, upper)
			continue
		}
		position[nodeID] = len(storage)
		storage = append(storage, stageNode{id: nodeID, upper: upper, first: candidate})
	}
	// Publish pointers only after storage has stopped growing.
	nodes := make([]*stageNode, len(storage))
	for i := range storage {
		nodes[i] = &storage[i]
	}
	return nodes
}

// scoreStageBatch scores the batch's nodes: their surfaced vectors and every
// other vector they indexed (members, when given), each node keeping its best.
func (p *VectorSearchPipeline) scoreStageBatch(ctx context.Context, query []float32, batch []*stageNode, members vectorMemberResolver) error {
	var byNode map[string][]string
	if members != nil {
		ids := make([]string, len(batch))
		for i, n := range batch {
			ids[i] = n.id
		}
		byNode = members(ids)
	}
	if cpu, ok := p.exactScorer.(*CPUExactScorer); ok {
		if _, fileScored := cpu.getter.(*VectorFileStore); !fileScored {
			return exactScoreError(p.scoreCPUStageBatch(ctx, query, batch, byNode, cpu))
		}
	}
	capacity := 0
	for _, n := range batch {
		capacity += max(1+len(n.extra), len(byNode[n.id]))
	}
	owner := make(map[string]*stageNode, capacity)
	pool := make([]Candidate, 0, capacity)
	for _, n := range batch {
		owner[n.first.ID] = n
		pool = append(pool, n.first)
		for _, candidate := range n.extra {
			if _, ok := owner[candidate.ID]; !ok {
				owner[candidate.ID] = n
				pool = append(pool, candidate)
			}
		}
		for _, vectorID := range byNode[n.id] {
			if _, ok := owner[vectorID]; !ok {
				owner[vectorID] = n
				pool = append(pool, Candidate{ID: vectorID})
			}
		}
	}
	scored, err := p.scoreFinal(ctx, query, pool)
	if err != nil {
		return err
	}
	for _, s := range scored {
		n := owner[s.ID]
		if n != nil {
			recordStageScore(n, s.ID, s.Score)
		}
	}
	return nil
}

// recordStageScore is the single writer of a scored node's best member.
func recordStageScore(node *stageNode, id string, score float64) {
	if !node.scored || score > node.best.Score {
		node.best, node.scored = ScoredCandidate{ID: id, Score: score}, true
	}
}

// scoreCPUStageBatch feeds values straight into the node accumulator. The
// common one-surfaced-vector case needs no expanded candidate slice, owner
// map or member-score slice. Additional surfaced vectors use a node-local
// set to keep the same vector from being scored twice.
func (p *VectorSearchPipeline) scoreCPUStageBatch(ctx context.Context, query []float32, batch []*stageNode, byNode map[string][]string, cpu *CPUExactScorer) error {
	return cpu.withVectorScores(ctx, query, func(read vectorScoreReader) error {
		for _, node := range batch {
			score := func(id string) error {
				value, ok, err := read(id)
				if err == nil && ok {
					recordStageScore(node, id, value)
				}
				return err
			}
			if len(node.extra) == 0 {
				if err := score(node.first.ID); err != nil {
					return err
				}
				for _, id := range byNode[node.id] {
					if id != node.first.ID {
						if err := score(id); err != nil {
							return err
						}
					}
				}
				continue
			}
			seen := make(map[string]struct{}, max(1+len(node.extra), len(byNode[node.id])))
			scoreOnce := func(id string) error {
				if _, ok := seen[id]; ok {
					return nil
				}
				seen[id] = struct{}{}
				return score(id)
			}
			if err := scoreOnce(node.first.ID); err != nil {
				return err
			}
			for _, candidate := range node.extra {
				if err := scoreOnce(candidate.ID); err != nil {
					return err
				}
			}
			for _, id := range byNode[node.id] {
				if err := scoreOnce(id); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// pushBestScores keeps the best k scores in a min-heap (scores[0] lowest);
// with k <= 0 it keeps none.
func pushBestScores(scores []float64, score float64, k int) []float64 {
	if k <= 0 {
		return scores
	}
	if len(scores) < k {
		scores = append(scores, score)
		for i := len(scores) - 1; i > 0; {
			parent := (i - 1) / 2
			if scores[parent] <= scores[i] {
				break
			}
			scores[parent], scores[i] = scores[i], scores[parent]
			i = parent
		}
		return scores
	}
	if score <= scores[0] {
		return scores
	}
	scores[0] = score
	for i := 0; ; {
		smallest, left, right := i, 2*i+1, 2*i+2
		if left < len(scores) && scores[left] < scores[smallest] {
			smallest = left
		}
		if right < len(scores) && scores[right] < scores[smallest] {
			smallest = right
		}
		if smallest == i {
			return scores
		}
		scores[i], scores[smallest] = scores[smallest], scores[i]
		i = smallest
	}
}

func exactScoreError(err error) error {
	if err == nil {
		return nil
	}
	return localizedError(localization.SearchExactScoringFailed(err), err)
}

// scoreFinal scores the pool with the pipeline's exact scorer.
func (p *VectorSearchPipeline) scoreFinal(ctx context.Context, query []float32, pool []Candidate) ([]ScoredCandidate, error) {
	var scored []ScoredCandidate
	var err error
	if cpu, ok := p.exactScorer.(*CPUExactScorer); ok {
		scored, err = cpu.scoreCandidateValues(ctx, query, pool)
	} else {
		scored, err = p.exactScorer.ScoreCandidates(ctx, query, pool)
	}
	if err != nil {
		return nil, exactScoreError(err)
	}
	return scored, nil
}
