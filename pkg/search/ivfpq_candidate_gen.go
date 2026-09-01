package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// IVFPQCandidateGen implements CandidateGenerator using IVF/PQ compressed ANN.
type IVFPQCandidateGen struct {
	index  *IVFPQIndex
	nprobe int
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

func (g *IVFPQCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g == nil || g.index == nil {
		return nil, localizedError(localization.SearchIVFPQIndexNotConfigured(), nil)
	}
	return g.index.SearchApprox(ctx, query, k, minSimilarity, g.nprobe)
}
