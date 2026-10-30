package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreate_ReturnsCreatedPath(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "CREATE p=(:A)-[:RELATES_TO]->(:B) RETURN p", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.NotNil(t, res.Rows[0][0], "expected RETURN p to return a path, got nil")

	pathMap, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok, "expected RETURN p to return map[string]interface{}, got %T", res.Rows[0][0])
	require.Equal(t, int64(1), pathMap["length"])

	nodes, ok := pathMap["nodes"].([]interface{})
	require.True(t, ok, "expected path nodes to be []interface{}, got %T", pathMap["nodes"])
	require.Len(t, nodes, 2)

	rels, ok := pathMap["relationships"].([]interface{})
	require.True(t, ok, "expected path relationships to be []interface{}, got %T", pathMap["relationships"])
	require.Len(t, rels, 1)
}
