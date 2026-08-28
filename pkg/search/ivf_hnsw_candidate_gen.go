package search

import (
	"context"
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

type clusterHNSWLookup func(clusterID int) *HNSWIndex

// IVFHNSWCandidateGen implements IVF-HNSW: centroid routing (IVF) into per-cluster HNSW indexes.
//
// This is designed for large CPU-only datasets:
//   - K-means provides a coarse routing layer (choose nearest clusters)
//   - Per-cluster HNSW provides fast ANN within each cluster
//
// For GPU-enabled setups, prefer the existing GPU brute-force and GPU k-means paths.
type IVFHNSWCandidateGen struct {
	clusterIndex        *gpu.ClusterIndex
	getClusterHNSW      clusterHNSWLookup
	numClustersToSearch int
	clusterSelector     func(ctx context.Context, query []float32, defaultN int) []int
}

func NewIVFHNSWCandidateGen(clusterIndex *gpu.ClusterIndex, getClusterHNSW clusterHNSWLookup, numClustersToSearch int) *IVFHNSWCandidateGen {
	if numClustersToSearch <= 0 {
		numClustersToSearch = 3
	}
	return &IVFHNSWCandidateGen{
		clusterIndex:        clusterIndex,
		getClusterHNSW:      getClusterHNSW,
		numClustersToSearch: numClustersToSearch,
	}
}

// SetClusterSelector sets an optional custom cluster selector used for routing.
func (g *IVFHNSWCandidateGen) SetClusterSelector(fn func(ctx context.Context, query []float32, defaultN int) []int) *IVFHNSWCandidateGen {
	g.clusterSelector = fn
	return g
}

func (g *IVFHNSWCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	if g.clusterIndex == nil || !g.clusterIndex.IsClustered() {
		return nil, fmt.Errorf("cluster index not clustered")
	}
	if g.getClusterHNSW == nil {
		return nil, fmt.Errorf("cluster HNSW lookup not configured")
	}
	if ctx == nil {
		ctx = context.Background()
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

	// If any required per-cluster index is missing, fall back to the existing
	// exact cluster search (which still benefits from centroid routing).
	for _, cid := range clusterIDs {
		if g.getClusterHNSW(cid) == nil {
			results, err := g.clusterIndex.SearchWithClusters(query, boundCandidateLimit(k), len(clusterIDs))
			if err != nil {
				return nil, err
			}
			out := make([]Candidate, 0, len(results))
			for _, r := range results {
				score := float64(r.Score)
				if score < minSimilarity {
					continue
				}
				out = append(out, Candidate{ID: r.ID, Score: score})
			}
			return out, nil
		}
	}

	totalLimit := boundCandidateLimit(k)
	perCluster := (totalLimit + len(clusterIDs) - 1) / len(clusterIDs)

	best := make(map[string]float64, totalLimit)
	for _, cid := range clusterIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		idx := g.getClusterHNSW(cid)
		if idx == nil {
			continue
		}
		results, err := idx.Search(ctx, query, perCluster, minSimilarity)
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			score := float64(r.Score)
			if prev, ok := best[r.ID]; !ok || score > prev {
				best[r.ID] = score
			}
		}
	}

	out := make([]Candidate, 0, len(best))
	for id, score := range best {
		out = append(out, Candidate{ID: id, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > totalLimit {
		out = out[:totalLimit]
	}
	return out, nil
}
