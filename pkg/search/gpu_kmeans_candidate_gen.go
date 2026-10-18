package search

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// GPUKMeansCandidateGen routes queries to the nearest k-means clusters and then
// scores only the vectors in those clusters using the GPU embedding index when
// available.
//
// This is used as a high-throughput fallback when:
//   - GPU brute-force is enabled but the dataset is outside the configured full-scan range, and
//   - clustering is available (centroids + cluster membership already built).
//
// It preserves correctness by returning exact cosine scores for the candidate set.
// When the GPU cannot score (not synced / unhealthy), ScoreSubset falls back to CPU.
type GPUKMeansCandidateGen struct {
	clusterIndex        *gpu.ClusterIndex
	numClustersToSearch int
	clusterSelector     func(ctx context.Context, query []float32, defaultN int) []int
	serving             *clusterServing // when set: the published generation, per query
}

func NewGPUKMeansCandidateGen(clusterIndex *gpu.ClusterIndex, numClustersToSearch int) *GPUKMeansCandidateGen {
	if numClustersToSearch <= 0 {
		numClustersToSearch = 3
	}
	return &GPUKMeansCandidateGen{
		clusterIndex:        clusterIndex,
		numClustersToSearch: numClustersToSearch,
	}
}

// SetClusterSelector sets an optional custom cluster selector used for routing.
func (g *GPUKMeansCandidateGen) SetClusterSelector(fn func(ctx context.Context, query []float32, defaultN int) []int) *GPUKMeansCandidateGen {
	g.clusterSelector = fn
	return g
}

// SetServing makes the generator serve each query from the cluster
// generation the query holds (see clusterServing); the selector is then
// called with lock held.
func (g *GPUKMeansCandidateGen) SetServing(lock sync.Locker, view func(context.Context) (context.Context, clusterView, func())) *GPUKMeansCandidateGen {
	g.serving = &clusterServing{lock: lock, view: view}
	return g
}

func (g *GPUKMeansCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

// candidatePopulation is the number of vectors the clustered index holds.
func (g *GPUKMeansCandidateGen) candidatePopulation(ctx context.Context) int {
	return g.serving.population(ctx, g.clusterIndex)
}

func (g *GPUKMeansCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	candidates, evidence, err := g.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	return candidates, evidence.exhausted, err
}

// searchCandidatesWithEvidence scores the routed clusters' members. The pass
// is routing-bound when the limit cut none above minSimilarity: a wider limit
// on the same clusters scores the same members. It is exhausted only when,
// besides, every vector of the index was scored (every cluster routed, see
// routeClusters).
func (g *GPUKMeansCandidateGen) searchCandidatesWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, candidateEvidence, error) {
	// The routing and the routed members come from the query's cluster
	// generation; scoring follows on its population, which stays held (and
	// its GPU buffers unreleased) until done.
	ctx, view, unlock, done := g.serving.open(ctx, ownClusterView(g.clusterIndex, g.numClustersToSearch, nil))
	defer done()
	defer unlock()
	if view.routing == nil {
		unlock()
		return nil, candidateEvidence{}, localizedError(localization.SearchGPUKMeansClusteredIndexRequired(), nil)
	}

	limit := boundCandidateLimit(k)
	clusterIDs := routeClusters(ctx, query, view, g.clusterSelector, limit)
	ids := view.routing.MemberIDsOf(clusterIDs)
	unlock()
	scoredAll := len(ids) >= view.index.Count()
	if len(ids) == 0 {
		return []Candidate{}, candidateEvidence{exhausted: scoredAll, routedComplete: true}, nil
	}

	results, err := view.index.ScoreSubset(query, ids)
	if err != nil {
		return nil, candidateEvidence{}, err
	}
	if len(results) == 0 {
		return []Candidate{}, candidateEvidence{exhausted: scoredAll, routedComplete: true}, nil
	}

	routed := len(results) <= limit
	if limit > len(results) {
		limit = len(results)
	}

	out := make([]Candidate, 0, util.SafePreallocCap(limit, len(results)))
	for i := 0; i < limit; i++ {
		select {
		case <-ctx.Done():
			return nil, candidateEvidence{}, ctx.Err()
		default:
		}
		score := float64(results[i].Score)
		if score < minSimilarity {
			routed = true // best first: the rest score lower
			continue
		}
		out = append(out, Candidate{ID: results[i].ID, Score: score})
	}

	return out, candidateEvidence{exhausted: scoredAll && routed, routedComplete: routed}, nil
}
