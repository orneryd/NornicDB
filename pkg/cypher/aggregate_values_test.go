package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Aggregates and ORDER BY read values as Neo4j 5.26.30 does (#907):
// durations by average length (a month 2,629,746 seconds), then months,
// days, seconds; -0.0 before 0.0 and NaN after every number; an integer sum
// in floating point once it overflows; avg as a running mean.
func TestAggregateValuesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_values"))
	ctx := context.Background()
	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"UNWIND [duration('P1M'), duration('P31D')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"P1M", "P31D"}}},
		{"UNWIND [duration('P1D'), duration('PT25H')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"P1D", "PT25H"}}},
		{"UNWIND [duration('PT10H'), duration('PT9H')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"PT9H", "PT10H"}}},
		{"UNWIND [duration('P1D'), duration('PT24H')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"PT24H", "P1D"}}},
		{"UNWIND [duration('P1M'), duration('P30D')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"P30D", "P1M"}}},
		{"UNWIND [duration('P1Y'), duration('P365D')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"P365D", "P1Y"}}},
		{"UNWIND [duration('PT1S'), duration('PT0.5S')] AS x RETURN toString(min(x)) AS mn, toString(max(x)) AS mx", [][]interface{}{{"PT0.5S", "PT1S"}}},
		{"UNWIND [duration('P1M'), duration('P31D'), duration('PT10H'), duration('PT9H')] AS x RETURN toString(x) AS s ORDER BY x", [][]interface{}{{"PT9H"}, {"PT10H"}, {"P1M"}, {"P31D"}}},
		{"UNWIND [9223372036854775807, 1, -5] AS x RETURN sum(x) AS s, avg(x) AS a", [][]interface{}{{9.223372036854776e+18, 3.074457345618259e+18}}},
		{"UNWIND [-9223372036854775808, -1] AS x RETURN sum(x) AS s", [][]interface{}{{-9.223372036854776e+18}}},
		{"UNWIND [9223372036854775807, -1] AS x RETURN sum(x) AS s", [][]interface{}{{int64(9223372036854775806)}}},
		{"UNWIND [1, 2.5] AS x RETURN sum(x) AS s", [][]interface{}{{3.5}}},
		{"UNWIND [0.1, 0.2, 0.3] AS x RETURN avg(x) AS a", [][]interface{}{{0.2}}},
		{"UNWIND [0.1, 0.2, 0.3, 0.4, 0.7] AS x RETURN avg(x) AS a", [][]interface{}{{0.33999999999999997}}},
		{"UNWIND [1e308, 1e308, -1e308] AS x RETURN avg(x) AS a", [][]interface{}{{math.Inf(-1)}}},
		{"UNWIND [0.0 / 0.0, 1.0] AS x RETURN percentileDisc(x, 0.5) AS a, min(x) AS mn", [][]interface{}{{1.0, 1.0}}},
		{"UNWIND [0.0 / 0.0, 1.0, 2.0] AS x RETURN percentileCont(x, 0.5) AS b", [][]interface{}{{2.0}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	// -0.0 orders before 0.0, whichever comes first; NaN after every number.
	for _, query := range []string{"UNWIND [0.0, -0.0] AS x RETURN max(x) AS mx, min(x) AS mn", "UNWIND [-0.0, 0.0] AS x RETURN max(x) AS mx, min(x) AS mn"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.False(t, math.Signbit(result.Rows[0][0].(float64)), query)
		require.True(t, math.Signbit(result.Rows[0][1].(float64)), query)
	}
	result, err := exec.Execute(ctx, "UNWIND [0.0 / 0.0, 1.0, 2.0] AS x RETURN percentileDisc(x, 1.0) AS a, max(x) AS c", nil)
	require.NoError(t, err)
	require.True(t, math.IsNaN(result.Rows[0][0].(float64)))
	require.True(t, math.IsNaN(result.Rows[0][1].(float64)))
}

func TestAggregateSumHelpers(t *testing.T) {
	total, overflow := addInt64(math.MaxInt64, 1)
	require.True(t, overflow)
	_, overflow = addInt64(math.MinInt64, -1)
	require.True(t, overflow)
	total, overflow = addInt64(5, -7)
	require.False(t, overflow)
	require.Equal(t, int64(-2), total)

	state := &pipelineAggregateState{name: "sum"}
	state.addSum(1, 1, true)
	partial := &pipelineAggregateState{name: "sum", integerTotal: math.MaxInt64}
	state.mergeSum(partial)
	require.True(t, state.hasFloat)
	require.Equal(t, 9.223372036854776e+18, state.floatingTotal)
	floating := &pipelineAggregateState{name: "sum", hasFloat: true, floatingTotal: 0.5}
	exact := &pipelineAggregateState{name: "sum", integerTotal: 2}
	exact.mergeSum(floating)
	require.Equal(t, 2.5, exact.floatingTotal)

	require.Equal(t, 0, compareDurationOrdering(&CypherDuration{Days: 1}, &CypherDuration{Days: 1}))
	require.Equal(t, -1, compareDurationOrdering(&CypherDuration{Hours: 24}, &CypherDuration{Days: 1}))
	require.Equal(t, 1, compareDurationOrdering(&CypherDuration{Days: 1, Nanos: 2}, &CypherDuration{Days: 1, Nanos: 1}))
	// The same length: more months is the larger one.
	require.Equal(t, 1, compareDurationOrdering(&CypherDuration{Months: 1}, &CypherDuration{Seconds: 2629746}))
}

// A group whose rows several workers aggregate merges their partial sums
// (mergeSum); enough rows that every worker takes some.
func TestParallelAggregateSumMergesPartials(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	others := make([]*storage.Node, 20000)
	for index := range others {
		others[index] = &storage.Node{}
	}
	patterns := []struct {
		variable string
		nodes    []*storage.Node
	}{
		{"a", []*storage.Node{
			{Properties: map[string]interface{}{"key": "k", "value": int64(1)}},
			{Properties: map[string]interface{}{"key": "k", "value": int64(2)}},
			{Properties: map[string]interface{}{"key": "k", "value": int64(3)}},
			{Properties: map[string]interface{}{"key": "j", "value": int64(4)}},
		}},
		{"b", others},
	}
	clause := "RETURN a.key AS key, sum(a.value) AS total ORDER BY key"
	for _, workers := range []int{1, 4} {
		ctx := withExpressionFailureSlot(context.Background())
		groups, handled, err := exec.tryCartesianAggregatePartitions(ctx, patterns, returnProjectionPlanFor(clause), workers)
		require.True(t, handled)
		require.NoError(t, err)
		result, err := exec.projectMergeReturnSource(ctx, nil, clause, nil, groups)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"j", int64(80000)}, {"k", int64(120000)}}, result.Rows, workers)
	}
}
