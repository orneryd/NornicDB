package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Offering candidates low then high fills the heap, replaces its weakest
// document with a better one, and keeps it over a worse one; with document
// IDs, a tie goes to the lexically smaller ID.
func TestOfferScoredDoc(t *testing.T) {
	var h minScoreHeap
	for _, candidate := range []scoredDoc{{docNum: 0, score: 1}, {docNum: 1, score: 3}, {docNum: 2, score: 0.5}} {
		h = offerScoredDoc(h, candidate, nil, 1)
	}
	require.Equal(t, []scoredDoc{{docNum: 1, score: 3}}, sortScoreHeapDescending(h, nil))

	docIDs := []string{"b", "a"}
	h = nil
	for _, candidate := range []scoredDoc{{docNum: 0, score: 2}, {docNum: 1, score: 2}} {
		h = offerScoredDoc(h, candidate, docIDs, 1)
	}
	require.Equal(t, []scoredDoc{{docNum: 1, score: 2}}, sortScoreHeapDescending(h, docIDs))
	require.Equal(t, 2.0, topKMinScore(map[uint32]float64{0: 1, 1: 3, 2: 2}, 2))
}
