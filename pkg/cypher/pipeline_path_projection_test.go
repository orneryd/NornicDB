package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineForwardsZeroLengthPathAcrossWith(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "pipeline_path_projection")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE ()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH p = (a) WITH p RETURN p", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"p"}, result.Columns)
	require.Len(t, result.Rows, 1)
	path, ok := result.Rows[0][0].(map[string]interface{})
	require.True(t, ok, "expected path value, got %T", result.Rows[0][0])
	require.Len(t, path["nodes"], 1)
	require.Empty(t, path["relationships"])
}

func TestPipelinePreservesWildcardColumnsWhenRequiredMatchEliminatesRows(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "pipeline_empty_wildcard")
	exec := NewStorageExecutor(store)

	result, err := exec.Execute(context.Background(), "OPTIONAL MATCH (a:Start) WITH a MATCH (a)-->(b) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, result.Columns)
	require.Empty(t, result.Rows)
}
