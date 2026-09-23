package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineWithOrderPreservesProjectedRows(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, `
		CREATE (:A {num: 1, num2: 4}), // sum = 5
		       (:A {num: 5, num2: 2}), // sum = 7
		       (:A {num: 9, num2: 0}), // sum = 9
		       (:A {num: 3, num2: 3}), // sum = 6
		       (:A {num: 7, num2: 1})  // sum = 8
	`, nil)
	require.NoError(t, err)

	result, err = executor.Execute(ctx, `
		MATCH (a:A)
		WITH a, a.num + a.num2 AS sum
		ORDER BY sum
		LIMIT 3
		RETURN a, sum
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	require.Equal(t, []int64{5, 6, 7}, []int64{
		result.Rows[0][1].(int64),
		result.Rows[1][1].(int64),
		result.Rows[2][1].(int64),
	})
	for _, row := range result.Rows {
		node, ok := row[0].(*storage.Node)
		require.True(t, ok)
		require.Len(t, node.Properties, 2)
	}
}

func TestPipelineWithOrderUsesProjectedAggregateValue(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, `
		CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}),
		       (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}),
		       (:A {num: 7, num2: 1})
	`, nil)
	require.NoError(t, err)
	result, err := executor.Execute(ctx, `
		MATCH (a:A)
		WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS total
		ORDER BY sum(a.num + a.num2)
		LIMIT 2
		RETURN mod, total
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(7)}, {int64(1), int64(13)}}, result.Rows)
}

func TestPipelineMinMaxUseCypherAggregateValueOrder(t *testing.T) {
	executor := &StorageExecutor{}
	rows := []pipelineRow{
		{"x": []interface{}{int64(1)}},
		{"x": []interface{}{int64(2)}},
		{"x": []interface{}{int64(2), int64(1)}},
	}
	maximum, ok := executor.evaluatePipelineAggregate(rows, "max", "x", false)
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(2), int64(1)}, maximum)

	mixed := []pipelineRow{{"x": int64(1)}, {"x": "a"}, {"x": []interface{}{int64(1), int64(2)}}, {"x": 0.2}, {"x": "b"}}
	maximum, ok = executor.evaluatePipelineAggregate(mixed, "max", "x", false)
	require.True(t, ok)
	require.Equal(t, int64(1), maximum)
	minimum, ok := executor.evaluatePipelineAggregate(mixed, "min", "x", false)
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(1), int64(2)}, minimum)
}
