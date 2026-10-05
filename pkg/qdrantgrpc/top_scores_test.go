package qdrantgrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The kept results are the highest scores whatever order candidates arrive in.
func TestTopScoresKeepsHighestScores(t *testing.T) {
	top := newTopScores(3, 3)
	for _, candidate := range []searchResult{{"a", 0.5}, {"b", 0.2}, {"c", 0.3}, {"d", 0.9}, {"e", 0.4}, {"f", 0.1}} {
		top.offer(candidate.ID, candidate.Score)
	}
	require.Equal(t, []searchResult{{"d", 0.9}, {"a", 0.5}, {"e", 0.4}}, top.sorted())
}
