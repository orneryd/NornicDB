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

func TestMapSubscriptPropagatesNullAndClassifiesNonStringKeys(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_subscript_types"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "WITH {name: 'Mats'} AS value RETURN value[null]", nil)
	require.NoError(t, err)
	require.Nil(t, result.Rows[0][0])

	_, err = exec.Execute(ctx, "WITH $value AS value, $key AS key RETURN value[key]", map[string]interface{}{
		"value": map[string]interface{}{"name": "Mats"},
		"key":   int64(0),
	})
	require.Error(t, err)
	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "MapElementAccessByNonString", semanticError.Detail)
}

func TestSizeRejectsPathsAndPatternPredicates(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "size_argument_types"))
	rows := []pipelineRow{{"p": map[string]interface{}{"_pathResult": PathResult{}}}}

	err := exec.validatePipelineSizeArguments(rows, "RETURN size(p)", "RETURN")
	require.Error(t, err)
	err = exec.validatePipelineSizeArguments(nil, "RETURN size((a)-->(b))", "RETURN")
	require.Error(t, err)
}

func TestStaticSizeRejectsPathBindingsAcrossProjectionHorizons(t *testing.T) {
	for _, query := range []string{
		"MATCH p = (a)-[*]->(b) RETURN size(p)",
		"MATCH p = (a)-->(b) WITH p AS route RETURN size(route)",
	} {
		err := validateStaticSizeArguments(query)
		require.Error(t, err)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}
}
