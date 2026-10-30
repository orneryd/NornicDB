//go:build darwin && cgo && !nometal

package search

import (
	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/simd"
)

// batchScoreCandidatesMetal scores candidates using Metal GPU batch operations.
//
// This is much faster than individual dot products when Metal is available.
// For small batches (< 100), CPU SIMD may be faster due to GPU overhead.
func (h *HNSWIndex) batchScoreCandidatesMetal(normalizedQuery []float32, candidateIDs []uint32, minSim32 float32) ([]ANNResult, error) {
	// Metal has non-trivial per-call overhead (dispatch + buffer setup). For typical HNSW
	// efSearch values (100-1000), CPU SIMD is faster on most machines.
	//
	// To avoid throughput regressions, Metal scoring is opt-in via threshold:
	//   - NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES <= 0 disables Metal
	//   - Otherwise, Metal is only used when len(candidateIDs) >= threshold
	threshold := envutil.GetInt("NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES", 0)
	if threshold <= 0 || !simd.MetalAvailable() || len(candidateIDs) < threshold {
		// Fall back to CPU for small batches or when Metal unavailable
		return h.batchScoreCandidatesCPU(normalizedQuery, candidateIDs, minSim32)
	}

	dimensions := h.dimensions
	numCandidates := len(candidateIDs)

	// Extract vectors into contiguous array for Metal batch operation
	// Format: [vec0[0..dim-1], vec1[0..dim-1], ..., vecN[0..dim-1]]
	// Track valid candidates and their indices
	embeddings := make([]float32, 0, numCandidates*dimensions)
	validIndices := make([]int, 0, numCandidates)

	for i, candidateID := range candidateIDs {
		if int(candidateID) >= len(h.vecOff) || h.deleted[candidateID] {
			continue
		}
		off := int(h.vecOff[candidateID])
		if off < 0 || off+dimensions > len(h.vectors) {
			continue
		}
		// Append vector to embeddings array
		embeddings = append(embeddings, h.vectors[off:off+dimensions]...)
		validIndices = append(validIndices, i)
	}

	// If no valid candidates, return empty
	if len(validIndices) == 0 {
		return []ANNResult{}, nil
	}

	actualNumCandidates := len(validIndices)
	embeddings = embeddings[:actualNumCandidates*dimensions]

	// Batch compute dot products using Metal GPU
	scores := make([]float32, actualNumCandidates)
	if err := simd.BatchDotProductMetal(embeddings, normalizedQuery, scores); err != nil {
		// Fall back to CPU if Metal fails
		return h.batchScoreCandidatesCPU(normalizedQuery, candidateIDs, minSim32)
	}

	// Filter and convert to results (only for valid candidates)
	results := make([]ANNResult, 0, actualNumCandidates)
	for idx, i := range validIndices {
		candidateID := candidateIDs[i]
		if int(candidateID) >= len(h.internalToID) {
			continue
		}
		if scores[idx] >= minSim32 {
			results = append(results, ANNResult{
				ID:    h.internalToID[candidateID],
				Score: scores[idx],
			})
		}
	}

	return results, nil
}

// batchScoreCandidatesCPU scores candidates using CPU SIMD (fallback).
func (h *HNSWIndex) batchScoreCandidatesCPU(normalizedQuery []float32, candidateIDs []uint32, minSim32 float32) ([]ANNResult, error) {
	results := make([]ANNResult, 0, len(candidateIDs))
	for _, candidateID := range candidateIDs {
		if int(candidateID) >= len(h.nodeLevel) || h.deleted[candidateID] {
			continue
		}
		similarity := vector.DotProductSIMD(normalizedQuery, h.vectorAtLocked(candidateID))
		if similarity >= minSim32 {
			results = append(results, ANNResult{
				ID:    h.internalToID[candidateID],
				Score: similarity,
			})
		}
	}
	return results, nil
}
