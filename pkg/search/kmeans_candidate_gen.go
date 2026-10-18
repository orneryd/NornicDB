// Package search - K-means cluster routing candidate generator.
//
// This file implements KMeansCandidateGen, which uses k-means clustering
// to route queries to the most relevant clusters, then generates candidates
// from those clusters. This is optimal for very large datasets (N > 100K)
// where cluster routing provides significant speedup over HNSW.
//
// Trigger Policies:
//
// K-means clustering is triggered automatically:
//   - After bulk loads: When BuildIndexes() completes, clustering runs automatically
//   - Periodic clustering: Background timer runs clustering at regular intervals
//     (configurable via clustering interval)
//   - Manual trigger: Call TriggerClustering() after bulk data loading
//
// The candidate generator automatically uses k-means routing when:
//   - Clustering is enabled (EnableClustering() called)
//   - ClusterIndex is clustered (Cluster() has been run)
//   - Dataset is large enough to benefit (typically N > 100K)
//
// For smaller datasets, the pipeline automatically falls back to HNSW or brute-force.
package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// KMeansCandidateGen implements CandidateGenerator using k-means cluster routing.
//
// This candidate generator:
//  1. Finds the top numClustersToSearch clusters nearest to the query
//  2. Gets all node IDs from those clusters as candidates
//  3. Returns candidates with approximate scores (centroid similarity)
//
// This is optimal for very large datasets (N > 100K) where cluster routing
// provides significant speedup. For smaller datasets, HNSW or brute-force
// may be faster.
type KMeansCandidateGen struct {
	clusterIndex        *gpu.ClusterIndex
	vectorIndex         *VectorIndex // For fallback and ID mapping
	numClustersToSearch int          // Number of clusters to search (default: 3)
	clusterSelector     func(ctx context.Context, query []float32, defaultN int) []int
}

// NewKMeansCandidateGen creates a new k-means candidate generator.
//
// Parameters:
//   - clusterIndex: The GPU ClusterIndex (must be clustered)
//   - vectorIndex: The VectorIndex for ID mapping and fallback
//   - numClustersToSearch: Number of clusters to search (default: 3)
func NewKMeansCandidateGen(clusterIndex *gpu.ClusterIndex, vectorIndex *VectorIndex, numClustersToSearch int) *KMeansCandidateGen {
	if numClustersToSearch <= 0 {
		numClustersToSearch = 3 // Default: search 3 nearest clusters
	}
	return &KMeansCandidateGen{
		clusterIndex:        clusterIndex,
		vectorIndex:         vectorIndex,
		numClustersToSearch: numClustersToSearch,
	}
}

// SetClusterSelector sets an optional custom cluster selector used for routing.
// When nil, the generator uses the default nearest-centroid routing.
func (k *KMeansCandidateGen) SetClusterSelector(fn func(ctx context.Context, query []float32, defaultN int) []int) *KMeansCandidateGen {
	k.clusterSelector = fn
	return k
}

// SearchCandidates generates candidates using k-means cluster routing.
//
// Algorithm:
//  1. Find top numClustersToSearch clusters nearest to query (centroid similarity)
//  2. Get all node IDs from those clusters
//  3. Return candidates with approximate scores (centroid similarity)
//
// If clustering is not available or not clustered, falls back to brute-force.
func (k *KMeansCandidateGen) SearchCandidates(ctx context.Context, query []float32, limit int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := k.searchCandidatesWithExhaustion(ctx, query, limit, minSimilarity)
	return candidates, err
}

// candidatePopulation is the number of vectors the clustered index holds
// (the vector index's when not clustered: the brute-force fallback).
func (k *KMeansCandidateGen) candidatePopulation(context.Context) int {
	if k.clusterIndex == nil || !k.clusterIndex.IsClustered() {
		return k.vectorIndex.Count()
	}
	return k.clusterIndex.Count()
}

func (k *KMeansCandidateGen) searchCandidatesWithExhaustion(ctx context.Context, query []float32, limit int, minSimilarity float64) ([]Candidate, bool, error) {
	candidates, evidence, err := k.searchCandidatesWithEvidence(ctx, query, limit, minSimilarity)
	return candidates, evidence.exhausted, err
}

// searchCandidatesWithEvidence scores the routed clusters' members exactly.
// A request as wide as the population routes every populated cluster. The
// pass is routing-bound when the limit cut none above minSimilarity (on the
// default nearest-centroid route, which returns at most the limit, when it
// returned fewer): a wider limit on the same clusters scores the same
// members. It is exhausted only when, besides, every vector of the index was
// scored.
func (k *KMeansCandidateGen) searchCandidatesWithEvidence(ctx context.Context, query []float32, limit int, minSimilarity float64) ([]Candidate, candidateEvidence, error) {
	// Check if clustering is available and clustered
	if k.clusterIndex == nil || !k.clusterIndex.IsClustered() {
		// Fall back to brute-force
		candidates, exhausted, err := NewBruteForceCandidateGen(k.vectorIndex).searchCandidatesWithExhaustion(ctx, query, limit, minSimilarity)
		return candidates, candidateEvidence{exhausted: exhausted, routedComplete: exhausted}, err
	}
	if dims := k.clusterIndex.Dimensions(); dims > 0 && len(query) != dims {
		return nil, candidateEvidence{}, localizedError(localization.SearchClusterQueryDimensionsMismatch(len(query), dims), nil)
	}

	candidateLimit := boundCandidateLimit(limit)
	population := k.clusterIndex.Count()
	clusterIDs := []int(nil)
	if candidateLimit >= population {
		clusterIDs = k.clusterIndex.PopulatedClusters()
	} else if k.clusterSelector != nil {
		clusterIDs = k.clusterSelector(ctx, query, k.numClustersToSearch)
	}
	var results []gpu.SearchResult
	var err error
	scoredAll := false
	if len(clusterIDs) > 0 {
		ids := k.clusterIndex.GetClusterMemberIDs(clusterIDs)
		scoredAll = len(ids) >= population
		results, err = k.clusterIndex.ScoreSubset(query, ids)
	} else {
		// Default nearest-centroid routing.
		results, err = k.clusterIndex.SearchWithClusters(query, candidateLimit, k.numClustersToSearch)
	}
	if err != nil {
		return nil, candidateEvidence{}, localizedError(localization.SearchClusterFailed(err), err)
	}

	if len(results) == 0 {
		return []Candidate{}, candidateEvidence{exhausted: scoredAll, routedComplete: true}, nil
	}

	// Convert SearchResult to Candidate with approximate scores.
	candidates := make([]Candidate, 0, len(results))

	for _, result := range results {
		select {
		case <-ctx.Done():
			return nil, candidateEvidence{}, ctx.Err()
		default:
		}

		if result.Score >= float32(minSimilarity) {
			candidates = append(candidates, Candidate{
				ID:    result.ID,
				Score: float64(result.Score), // Use exact score as approximate (already computed)
			})
		}
	}

	routed := len(candidates) <= candidateLimit
	if len(clusterIDs) == 0 {
		routed = routed && len(results) < candidateLimit
	}
	if len(candidates) > candidateLimit {
		candidates = candidates[:candidateLimit]
	}
	return candidates, candidateEvidence{exhausted: scoredAll && routed, routedComplete: routed}, nil
}
