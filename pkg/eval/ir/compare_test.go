package ir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareDeterministicPairedBootstrap(t *testing.T) {
	qrels := Qrels{
		"q1": {"d1": 1},
		"q2": {"d2": 1},
		"q3": {"d3": 1},
	}
	baseline := map[string][]string{"q1": {"miss"}, "q2": {"d2"}, "q3": {"miss"}}
	candidate := map[string][]string{"q1": {"d1"}, "q2": {"d2"}, "q3": {"d3"}}
	first, err := Compare(qrels, baseline, candidate, 7, 1000)
	require.NoError(t, err)
	second, err := Compare(qrels, baseline, candidate, 7, 1000)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.InDelta(t, 2.0/3, first.AbsoluteDelta.RecallAt100, 0.000001)
	assert.InDelta(t, 2.0/3, first.AbsoluteDelta.NDCGAt10, 0.000001)
	assert.Greater(t, first.RecallAt100CI.Upper, 0.6)
}

func TestCompareRejectsInvalidInput(t *testing.T) {
	_, err := Compare(nil, nil, nil, 1, 100)
	require.Error(t, err)
	_, err = Compare(Qrels{"q": {}}, nil, nil, 1, 0)
	require.Error(t, err)
}
