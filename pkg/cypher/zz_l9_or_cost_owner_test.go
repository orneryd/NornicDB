package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9IndexedORPlannerOwner(t *testing.T) {
	executor := l9OrCostFixture(t)
	ctx := t.Context()
	pattern := nodePatternInfo{variable: "n", labels: []string{"O"}}
	const orOnly = "n.k=1 OR n.id='n00002'"
	const nested = "(" + orOnly + ") AND n.status='common'"
	_, planned, err := executor.planPropertyIndexOrEquality(ctx, pattern, nested)
	require.NoError(t, err)
	require.False(t, planned)
	var current nodeIndexSeekSet
	require.NoError(t, executor.addPropertyIndexEqualityCompoundSeeks(ctx, pattern, nested, &current))
	currentIndex, err := bestNodeIndexSeek(ctx, &current)
	require.NoError(t, err)
	currentSeek := current.at(currentIndex)
	require.NotNil(t, currentSeek)
	require.Equal(t, "status", currentSeek.postings[0].property)
	full, err := currentSeek.estimate(ctx, 10000)
	require.NoError(t, err)

	alternative, planned, err := executor.planPropertyIndexOrEquality(ctx, pattern, orOnly)
	require.NoError(t, err)
	require.True(t, planned)
	current.add(alternative)
	revisedIndex, err := bestNodeIndexSeek(ctx, &current)
	require.NoError(t, err)
	selective, err := current.at(revisedIndex).estimate(ctx, 10000)
	require.NoError(t, err)
	t.Logf("nested OR planned=%v, equality postings=%d, after admitting the OR conjunct=%d", false, full, selective)
	require.EqualValues(t, 1000, full)
	require.EqualValues(t, 2, selective)
}
