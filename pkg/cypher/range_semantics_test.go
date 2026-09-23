package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRangeRequiresIntegerArgumentsAndNonzeroStep(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_argument_semantics"))
	for _, query := range []string{
		"RETURN range(0.0, 1, 1)",
		"RETURN range(0, true, 1)",
		"RETURN range(0, 1, '1')",
	} {
		_, err := executor.Execute(context.Background(), query, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.True(t, errors.As(err, &semanticError))
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}

	_, err := executor.Execute(context.Background(), "RETURN range(0, 1, 0)", nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "NumberOutOfRange", semanticError.Detail)
}

func TestRangeReturnsEmptyWhenDirectionAndStepDisagree(t *testing.T) {
	for _, arguments := range [][]interface{}{
		{int64(0), int64(1), int64(-1)},
		{int64(0), int64(-1), int64(1)},
	} {
		values, err := evaluateCypherRange(arguments)
		require.NoError(t, err)
		require.Empty(t, values)
	}
}

func TestQuantifiedAggregateConfirmsEveryInconsistentRangeIsEmpty(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_quantified_aggregate"))
	result, err := executor.Execute(context.Background(), `
		WITH 0 AS start, [1, 2, 500, 1000, 1500] AS stopList, [-1000, -3, -2, -1, 1, 2, 3, 1000] AS stepList
		UNWIND stopList AS stop
		UNWIND stepList AS step
		WITH start, stop, step, range(start, stop, step) AS list
		WITH start, stop, step, list, sign(stop-start) <> sign(step) AS empty
		RETURN ALL(ok IN collect((size(list) = 0) = empty) WHERE ok) AS okay
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"okay"}, result.Columns)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
