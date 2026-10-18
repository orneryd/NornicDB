package search

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, k, minSimilarity)
	return candidates, err
}

// candidatePopulation is the live vectors the populated clusters' graphs
// hold together: what IVF-HNSW searches when it routes every cluster.
func (g *IVFHNSWCandidateGen) candidatePopulation(context.Context) int {
	_, population := g.graphPopulation()
	return population
}

// graphPopulation is every populated cluster and the live vectors their
// graphs hold together. A populated cluster without a graph adds nothing.
func (g *IVFHNSWCandidateGen) graphPopulation() (clusters []int, population int) {
	if g.clusterIndex == nil || !g.clusterIndex.IsClustered() || g.getClusterHNSW == nil {
		return nil, 0
	}
	clusters = g.clusterIndex.PopulatedClusters()
	for _, cluster := range clusters {
		if graph := g.getClusterHNSW(cluster); graph != nil {
			population += graph.Size()
		}
	}
	return clusters, population
}

// searchCandidatesWithExhaustion searches the routed clusters' graphs. A
// request as wide as the graphs' population routes every populated cluster
// and searches each graph whole. It is exhausted only then, when every graph
// search was itself exhausted and the merge cut nothing.
func (g *IVFHNSWCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error) {
	if g.clusterIndex == nil || !g.clusterIndex.IsClustered() {
		return nil, false, localizedError(localization.SearchClusterIndexNotClustered(), nil)
	}
	if g.getClusterHNSW == nil {
		return nil, false, localizedError(localization.SearchClusterHNSWLookupNotConfigured(), nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	totalLimit := boundCandidateLimit(k)
	clusterIDs, population := g.graphPopulation()
	routedAll := totalLimit >= population
	if !routedAll {
		clusterIDs = nil
		if g.clusterSelector != nil {
			clusterIDs = g.clusterSelector(ctx, query, g.numClustersToSearch)
		}
		if len(clusterIDs) == 0 {
			clusterIDs = g.clusterIndex.FindNearestClusters(query, g.numClustersToSearch)
		}
	}
	if len(clusterIDs) == 0 {
		return []Candidate{}, routedAll, nil
	}

	// If any required per-cluster index is missing, fall back to the existing
	// exact cluster search (which still benefits from centroid routing). It
	// scores the routed clusters' members exactly, but not as a proof that
	// every vector was searched.
	for _, cid := range clusterIDs {
		if g.getClusterHNSW(cid) == nil {
			results, err := g.clusterIndex.SearchWithClusters(query, totalLimit, len(clusterIDs))
			if err != nil {
				return nil, false, err
			}
			out := make([]Candidate, 0, len(results))
			for _, r := range results {
				score := float64(r.Score)
				if score < minSimilarity {
					continue
				}
				out = append(out, Candidate{ID: r.ID, Score: score})
			}
			return out, false, nil
		}
	}

	perCluster := (totalLimit + len(clusterIDs) - 1) / len(clusterIDs)

	complete := routedAll
	best := make(map[string]float64, totalLimit)
	for _, cid := range clusterIDs {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		idx := g.getClusterHNSW(cid)
		if idx == nil {
			continue
		}
		limit, ef := perCluster, idx.Config().EfSearch
		if routedAll {
			// Search the whole graph: a beam as wide as its population scores
			// all of it (searchWithEfExhaustion).
			limit, ef = totalLimit, totalLimit
		}
		results, exhausted, err := idx.searchWithEfExhaustion(ctx, query, limit, minSimilarity, ef)
		if err != nil {
			return nil, false, err
		}
		complete = complete && exhausted
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
		complete = false
	}
	return out, complete, nil
}
