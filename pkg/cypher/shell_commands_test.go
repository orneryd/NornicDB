package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestShellParamCommand_PersistsAndOverrides(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, ":param name => 'Alice'", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "RETURN $name AS name", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "Alice", result.Rows[0][0])

	result, err = exec.Execute(ctx, "RETURN $name AS name", map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	require.Equal(t, "Bob", result.Rows[0][0])
}

func TestShellParamCommand_MapListAndClear(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, ":param {name: 'Alice', age: 1 + 1}", nil)
	require.NoError(t, err)

	listResult, err := exec.Execute(ctx, ":params", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "value"}, listResult.Columns)
	require.Len(t, listResult.Rows, 2)
	require.Equal(t, "age", listResult.Rows[0][0])
	require.EqualValues(t, int64(2), listResult.Rows[0][1])
	require.Equal(t, "name", listResult.Rows[1][0])
	require.Equal(t, "Alice", listResult.Rows[1][1])

	queryResult, err := exec.Execute(ctx, "RETURN $name AS name, $age AS age", nil)
	require.NoError(t, err)
	require.Equal(t, "Alice", queryResult.Rows[0][0])
	require.EqualValues(t, int64(2), queryResult.Rows[0][1])

	_, err = exec.Execute(ctx, ":param clear", nil)
	require.NoError(t, err)

	emptyResult, err := exec.Execute(ctx, ":params", nil)
	require.NoError(t, err)
	require.Empty(t, emptyResult.Rows)
}

func TestShellParamCommand_ANTLRMode(t *testing.T) {
	cleanup := config.WithANTLRParser()
	defer cleanup()

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, ":param answer => 40 + 2", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "RETURN $answer AS answer", nil)
	require.NoError(t, err)
	require.EqualValues(t, int64(42), result.Rows[0][0])

	_, err = exec.Execute(ctx, ":params {phrase: 'hello'}\nRETURN $phrase AS phrase", nil)
	require.NoError(t, err)

	phraseResult, err := exec.Execute(ctx, "RETURN $phrase AS phrase", nil)
	require.NoError(t, err)
	require.Equal(t, "hello", phraseResult.Rows[0][0])
}
