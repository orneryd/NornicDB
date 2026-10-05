package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The sparse top-K keeps the best documents whatever order the score map
// yields them in; with many candidates and k=1, later better candidates
// replace the kept one.
func TestTopKFromSparseScoresIgnoresMapOrder(t *testing.T) {
	const n = 64
	docIDs := make([]string, n)
	for i := range docIDs {
		docIDs[i] = fmt.Sprintf("doc-%02d", i)
	}
	for round := 0; round < 20; round++ {
		scores := make(map[uint32]float64, n)
		for i := 0; i < n; i++ {
			scores[uint32(i)] = float64(i)
		}
		require.Equal(t, []scoredDoc{{docNum: n - 1, score: n - 1}}, topKFromSparseScores(scores, nil, 1, nil))
		require.Equal(t, []scoredDoc{{docNum: n - 1, score: n - 1}}, topKFromSparseScores(scores, docIDs, 1, nil))
	}
}
