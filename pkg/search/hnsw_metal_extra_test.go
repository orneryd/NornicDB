//go:build darwin && cgo && !nometal

package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHNSWBatchScoreCandidatesCPUFiltersInvalidDeletedAndThreshold(t *testing.T) {
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	require.NoError(t, idx.Add("exact", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("close", []float32{0.8, 0.2, 0, 0}))
	require.NoError(t, idx.Add("deleted", []float32{0, 1, 0, 0}))

	idx.mu.Lock()
	idx.deleted[2] = true
	results, err := idx.batchScoreCandidatesCPU([]float32{1, 0, 0, 0}, []uint32{0, 1, 2, 99}, 0.99)
	idx.mu.Unlock()

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "exact", results[0].ID)
	require.InDelta(t, 1.0, results[0].Score, 0.0001)
}

func TestHNSWBatchScoreCandidatesMetalFallsBackWhenDisabled(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES", "0")
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	require.NoError(t, idx.Add("exact", []float32{1, 0, 0, 0}))

	idx.mu.Lock()
	results, err := idx.batchScoreCandidatesMetal([]float32{1, 0, 0, 0}, []uint32{0}, 0.99)
	idx.mu.Unlock()

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "exact", results[0].ID)
}
