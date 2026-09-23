package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestListSubscriptRejectsNonListReceiversAndNonIntegerIndexes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_subscript_types"))
	queries := []string{
		"WITH true AS value RETURN value[0]",
		"WITH [1, 2] AS value RETURN value[false]",
		"WITH [1, 2] AS value RETURN value[1.0]",
	}

	for _, query := range queries {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.True(t, errors.As(err, &semanticError))
		require.Equal(t, "Neo.ClientError.Statement.TypeError", semanticError.Code)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}
}

func TestListSliceEvaluatesBoundsAndPropagatesNull(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_slice_bounds"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "WITH [1, 2, 3] AS value, 1 AS lower, 3 AS upper RETURN value[lower..upper]", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(2), int64(3)}, result.Rows[0][0])

	result, err = exec.Execute(ctx, "WITH [1, 2, 3] AS value RETURN value[null..2]", nil)
	require.NoError(t, err)
	require.Nil(t, result.Rows[0][0])
}
