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
	"fmt"

	"github.com/orneryd/nornicdb/pkg/gpu"
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
	// Check if clustering is available and clustered
	if k.clusterIndex == nil || !k.clusterIndex.IsClustered() {
		// Fall back to brute-force
		bruteGen := NewBruteForceCandidateGen(k.vectorIndex)
		return bruteGen.SearchCandidates(ctx, query, limit, minSimilarity)
	}
	if dims := k.clusterIndex.Dimensions(); dims > 0 && len(query) != dims {
		return nil, fmt.Errorf("cluster search failed: query dimensions %d != index dimensions %d", len(query), dims)
	}

	candidateLimit := boundCandidateLimit(limit)
	clusterIDs := []int(nil)
	if k.clusterSelector != nil {
		clusterIDs = k.clusterSelector(ctx, query, k.numClustersToSearch)
	}
	var results []gpu.SearchResult
	var err error
	if len(clusterIDs) > 0 {
		ids := k.clusterIndex.GetClusterMemberIDs(clusterIDs)
		results, err = k.clusterIndex.ScoreSubset(query, ids)
	} else {
		// Default nearest-centroid routing.
		results, err = k.clusterIndex.SearchWithClusters(query, candidateLimit, k.numClustersToSearch)
	}
	if err != nil {
		return nil, fmt.Errorf("cluster search failed: %w", err)
	}

	if len(results) == 0 {
		return []Candidate{}, nil
	}

	// Convert SearchResult to Candidate with approximate scores.
	candidates := make([]Candidate, 0, len(results))

	for _, result := range results {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if result.Score >= float32(minSimilarity) {
			candidates = append(candidates, Candidate{
				ID:    result.ID,
				Score: float64(result.Score), // Use exact score as approximate (already computed)
			})
		}
	}

	if len(candidates) > candidateLimit {
		candidates = candidates[:candidateLimit]
	}
	return candidates, nil
}
