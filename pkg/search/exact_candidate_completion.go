package search

import (
	"context"
	"math"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// exactCandidatePage describes the generator's widened prefilter, not the
// caller's final qualification. population may be capped above the window:
// one omitted prefilter match proves only that this raw page is incomplete.
type exactCandidatePage struct {
	candidates []Candidate
	population int
}

func (p exactCandidatePage) complete() bool { return len(p.candidates) == p.population }

type exactCandidateGenerator interface {
	searchExactCandidates(context.Context, []float32, int, float64) (exactCandidatePage, error)
}

// Direct generator callers ask about their supplied threshold. The pipeline
// instead consumes the receipt through searchExactWithEvidence, where the
// final scoring policy and original threshold decide query completion.
func exactCandidateResults(page exactCandidatePage, err error) ([]Candidate, bool, error) {
	if err != nil {
		return nil, false, err
	}
	return page.candidates, page.complete(), nil
}

func (b *BruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minimum float64) ([]Candidate, bool, error) {
	return exactCandidateResults(b.searchExactCandidates(ctx, query, k, minimum))
}

func (b *FileStoreBruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minimum float64) ([]Candidate, bool, error) {
	return exactCandidateResults(b.searchExactCandidates(ctx, query, k, minimum))
}

func (g *GPUBruteForceCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minimum float64) ([]Candidate, bool, error) {
	return exactCandidateResults(g.searchExactCandidates(ctx, query, k, minimum))
}

// searchExactWithEvidence owns final qualification and completion for every
// exact receipt producer. Safety slack remains on candidate generation. Its
// raw overflow cannot establish final overflow: rejected near duplicates may
// be the only unseen candidates. A lookahead window is scored with the same
// node/member policy as ordinary results, until either a final-qualified
// overflow witness exists or the raw receipt proves nothing was left out.
// These internal proof windows do not change the public issued depth or k.
func (p *VectorSearchPipeline) searchExactWithEvidence(ctx context.Context, query []float32, k int, minimum float64, generator exactCandidateGenerator) ([]ScoredCandidate, candidateEvidence, error) {
	depth := max(k, 0)
	if depth < math.MaxInt {
		depth++
	}
	work := newSearchBatch(ctx)
	for {
		if err := work.check(); err != nil {
			return nil, candidateEvidence{}, err
		}
		page, err := generator.searchExactCandidates(ctx, query, depth, candidatePrefilter(minimum, len(query)))
		if err != nil {
			return nil, candidateEvidence{}, localizedError(localization.SearchCandidateGenerationFailed(err), err)
		}
		// A kth-score pruning cut only says unseen nodes COULD qualify. For
		// completion evidence we must resolve that uncertainty at the final
		// threshold. The raw window length bounds its distinct nodes, so that
		// limit scores every potentially qualifying node without a kth cut.
		scored, _, err := p.scoreNodes(ctx, query, page.candidates, len(page.candidates), minimum, exactScoreSlack(len(query)))
		if err != nil {
			return nil, candidateEvidence{}, err
		}
		evidence := candidateEvidence{exhausted: page.complete(), routedComplete: page.complete()}.withBound(0)
		if k > 0 && len(scored) > k {
			evidence.exhausted, evidence.routedComplete = false, false
			return scored[:k], evidence, nil
		}
		if evidence.exhausted {
			return scored, evidence, nil
		}
		// Widen only when final rejection leaves the receipt inconclusive.
		// Cancellation is checked at each scan and every final-scoring batch.
		if depth > math.MaxInt/2 {
			depth = math.MaxInt
		} else {
			depth *= 2
		}
	}
}
