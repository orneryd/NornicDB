package search

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/gpu"
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

func (g *GPUKMeansCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	if g.clusterIndex == nil || !g.clusterIndex.IsClustered() {
		return nil, fmt.Errorf("gpu k-means candidate gen requires clustered index")
	}

	clusterIDs := []int(nil)
	if g.clusterSelector != nil {
		clusterIDs = g.clusterSelector(ctx, query, g.numClustersToSearch)
	}
	if len(clusterIDs) == 0 {
		clusterIDs = g.clusterIndex.FindNearestClusters(query, g.numClustersToSearch)
	}
	if len(clusterIDs) == 0 {
		return []Candidate{}, nil
	}

	ids := g.clusterIndex.GetClusterMemberIDs(clusterIDs)
	if len(ids) == 0 {
		return []Candidate{}, nil
	}

	results, err := g.clusterIndex.ScoreSubset(query, ids)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return []Candidate{}, nil
	}

	limit := calculateCandidateLimit(k)
	if limit > len(results) {
		limit = len(results)
	}

	out := make([]Candidate, 0, util.SafePreallocCap(limit, len(results)))
	for i := 0; i < limit; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		score := float64(results[i].Score)
		if score < minSimilarity {
			continue
		}
		out = append(out, Candidate{ID: results[i].ID, Score: score})
	}

	return out, nil
}
