package ir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectQueryIDsDeterministic(t *testing.T) {
	input := []string{"q5", "q1", "q4", "q2", "q3"}
	first, err := SelectQueryIDs(input, 3, 42)
	require.NoError(t, err)
	second, err := SelectQueryIDs([]string{"q3", "q2", "q1", "q4", "q5"}, 3, 42)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Len(t, first, 3)
	assert.True(t, first[0] < first[1] && first[1] < first[2])
}

func TestSelectQueryIDsReturnsAllSmallSets(t *testing.T) {
	selected, err := SelectQueryIDs([]string{"q2", "q1"}, 1000, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"q1", "q2"}, selected)
}

func TestSelectQueryIDsRejectsInvalidInput(t *testing.T) {
	_, err := SelectQueryIDs([]string{"q1", "q1"}, 1, 1)
	require.Error(t, err)
	_, err = SelectQueryIDs([]string{"q1"}, 0, 1)
	require.Error(t, err)
}
