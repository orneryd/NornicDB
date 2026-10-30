//go:build !darwin || !cgo || nometal

package search

import (
	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// batchScoreCandidatesMetal is a stub that always uses CPU SIMD.
func (h *HNSWIndex) batchScoreCandidatesMetal(normalizedQuery []float32, candidateIDs []uint32, minSim32 float32) ([]ANNResult, error) {
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
