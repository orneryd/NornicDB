package search

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// IVFPQCandidateGen implements CandidateGenerator using IVF/PQ compressed ANN.
type IVFPQCandidateGen struct {
	index   *IVFPQIndex
	nprobe  int
	overlay *annMutationOverlay
}

func NewIVFPQCandidateGen(index *IVFPQIndex, nprobe int) *IVFPQCandidateGen {
	if nprobe <= 0 && index != nil {
		nprobe = index.profile.NProbe
	}
	if nprobe <= 0 {
		nprobe = 1
	}
	return &IVFPQCandidateGen{
		index:  index,
		nprobe: nprobe,
	}
}

// NewIVFPQCandidateGenWithOverlay creates a compressed candidate generator
// that merges live mutations with the immutable IVF/PQ base index.
func NewIVFPQCandidateGenWithOverlay(index *IVFPQIndex, nprobe int, overlay *annMutationOverlay) *IVFPQCandidateGen {
	g := NewIVFPQCandidateGen(index, nprobe)
	g.overlay = overlay
	return g
}

func (g *IVFPQCandidateGen) preferredCandidateDepth(target, maximum int) int {
	preferred := target
	if g != nil && g.index != nil && g.index.profile.RerankTopK > preferred {
		preferred = g.index.profile.RerankTopK
	}
	return min(preferred, maximum)
}

func (g *IVFPQCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

// candidatePopulation is every vector the index and its mutation overlay can
// return.
func (g *IVFPQCandidateGen) candidatePopulation(context.Context) int {
	if g == nil || g.index == nil {
		return 0
	}
	return g.index.population() + g.overlay.size()
}

func (g *IVFPQCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	candidates, evidence, err := g.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	return candidates, evidence.exhausted, err
}

// searchCandidatesWithEvidence searches nprobe inverted lists, or all of them
// when the request is as wide as the population. The pass is routing-bound
// when the probed lists were returned whole (fewer than the limit), the
// overlay returned all it holds above minSimilarity, and the limit cut none
// of the merged candidates: a wider limit probes the same lists. It is
// exhausted only when, besides, every list was probed.
func (g *IVFPQCandidateGen) searchCandidatesWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, candidateEvidence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g == nil || g.index == nil {
		return nil, candidateEvidence{}, localizedError(localization.SearchIVFPQIndexNotConfigured(), nil)
	}
	limit := boundCandidateLimit(k)
	nprobe := g.nprobe
	if limit >= g.index.population()+g.overlay.size() {
		nprobe = g.index.listCount()
	}
	// PQ scores are only an ordering signal and can underestimate exact cosine
	// similarity. Apply the caller's threshold after exact rescoring in the
	// shared pipeline instead of dropping viable candidates here.
	base, err := g.index.SearchApprox(ctx, query, k, -1, nprobe)
	if err != nil {
		return nil, candidateEvidence{}, err
	}
	probedAll := nprobe > 0 && nprobe >= g.index.listCount()
	complete := probedAll && limit >= g.index.population()
	routed := complete || len(base) < limit
	if g.overlay == nil {
		return base, candidateEvidence{exhausted: complete, routedComplete: routed}, nil
	}
	merged := make(map[string]Candidate, len(base)+k)
	for _, candidate := range base {
		if !g.overlay.IsRemoved(candidate.ID) {
			merged[candidate.ID] = candidate
		}
	}
	overlayCandidates := g.overlay.Search(ctx, query, k, minSimilarity)
	if err := ctx.Err(); err != nil {
		return nil, candidateEvidence{}, err
	}
	routed = routed && len(overlayCandidates) < limit
	for _, candidate := range overlayCandidates {
		merged[candidate.ID] = candidate
	}
	out := make([]Candidate, 0, len(merged))
	for _, candidate := range merged {
		out = append(out, candidate)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].ID < out[j].ID
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > k {
		out = out[:k]
		routed = false
	}
	return out, candidateEvidence{exhausted: complete && routed, routedComplete: routed}, nil
}
