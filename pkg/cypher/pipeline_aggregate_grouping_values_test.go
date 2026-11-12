package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProfileAggregateGroupingKeysEvaluateOncePerRow proves an aggregating
// clause evaluates each grouping item once per input row, at the occurrence the
// item was written: projecting a group reports the value the group was formed
// on instead of running the same expression a second time over the group's
// first row, and two equal written keys stay two operators.
func TestProfileAggregateGroupingKeysEvaluateOncePerRow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
			unions      int
			executions  int64
		}{
			{
				// Two written grouping keys plus an aggregate over one input row:
				// each key's program runs once and is its own operator.
				name:       "two keys over one row",
				query:      "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, COUNT { RETURN 1 UNION ALL RETURN 1 } AS b, count(*) AS n",
				want:       [][]interface{}{{int64(2), int64(2), int64(1)}},
				unions:     2,
				executions: 1,
			},
			{
				// One written key over three input rows forming two groups: the
				// program runs once per row, not once per row plus once per group.
				name:       "one key over several rows",
				query:      "UNWIND [1, 1, 2] AS x RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } + x AS key, count(*) AS n",
				want:       [][]interface{}{{int64(3), int64(2)}, {int64(4), int64(1)}},
				unions:     1,
				executions: 3,
			},
			{
				// An aggregating WITH projects its groups through the same
				// captured keys, at the WITH clause's own written occurrences.
				name:       "aggregating WITH keys",
				query:      "UNWIND [1, 2] AS x WITH COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, COUNT { RETURN 1 UNION ALL RETURN 1 } + x AS b, count(*) AS n RETURN a, b, n",
				want:       [][]interface{}{{int64(2), int64(3), int64(1)}, {int64(2), int64(4), int64(1)}},
				unions:     2,
				executions: 2,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
				result, err := exec.Execute(ctx, "PROFILE "+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
				plan := executionPlanFromResult(t, result)
				unions := profileScalarUnionOperators(plan)
				require.Len(t, unions, tc.unions, "%s", formatPlan(plan))
				for _, union := range unions {
					require.True(t, union.RowsObserved)
					require.Equal(t, tc.executions, union.Arguments["executions"], "%s", formatPlan(plan))
					require.Equal(t, 2*tc.executions, union.ActualRows)
				}
			})
		}
	})
}

// TestAggregateGroupingKeysAreCapturedWithTheirGroup pins the mechanism behind
// the evaluation count: a group's projection reports the grouping value the
// group was formed on, so projecting the group never depends on its retained
// first row still evaluating to that value.
func TestAggregateGroupingKeysAreCapturedWithTheirGroup(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := withExpressionFailureSlot(context.Background())
	projections := returnProjectionPlanFor(ctx, "RETURN k AS key, count(*) AS n").projections
	groups, ok := exec.pipelineAggregateGroups(ctx, pipelineRowsSource([]pipelineRow{
		{"k": int64(1)},
		{"k": int64(1)},
		{"k": nil},
	}), projections, false)
	require.True(t, ok)
	require.Len(t, groups, 2)

	key, ok := groups[0].value(ctx, exec, 0, -1)
	require.True(t, ok)
	require.Equal(t, int64(1), key)
	// The group's retained first row no longer yields its key; the group still
	// reports the value it grouped by, because it is not evaluated again.
	groups[0].first["k"] = int64(99)
	key, ok = groups[0].value(ctx, exec, 0, -1)
	require.True(t, ok)
	require.Equal(t, int64(1), key)

	// A nil key groups on its own and reports nil rather than nothing.
	key, ok = groups[1].value(ctx, exec, 0, -1)
	require.True(t, ok)
	require.Nil(t, key)
	count, ok := groups[1].value(ctx, exec, 1, -1)
	require.True(t, ok)
	require.Equal(t, int64(1), count)
}

// TestAggregateGroupingKeysKeepIdentityAndOrder covers what capturing a group's
// key must not change: a nil key grouping once while staying distinct from other
// keys, aliases, the order groups first appear in, and a clause with no input
// rows.
func TestAggregateGroupingKeysKeepIdentityAndOrder(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
		}{
			{
				name:  "nil key groups once and stays distinct",
				query: "UNWIND [null, 1, null, 'a'] AS k RETURN k AS key, count(*) AS n",
				want:  [][]interface{}{{nil, int64(2)}, {int64(1), int64(1)}, {"a", int64(1)}},
			},
			{
				name:  "groups keep first-appearance order",
				query: "UNWIND [2, 3, 1, 2] AS x RETURN x > 1 AS big, count(*) AS n",
				want:  [][]interface{}{{true, int64(3)}, {false, int64(1)}},
			},
			{
				name:  "no input rows forms no keyed group",
				query: "MATCH (n:Nothing) RETURN n.k AS key, count(*) AS n",
				want:  [][]interface{}{},
			},
			{
				name:  "no input rows keeps the global aggregate",
				query: "MATCH (n:Nothing) RETURN count(*) AS n",
				want:  [][]interface{}{{int64(0)}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, len(tc.want))
				for index, want := range tc.want {
					require.Equal(t, want, result.Rows[index], "row %d of %s", index, tc.query)
				}
			})
		}
	})
}
