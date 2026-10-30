package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreate_ReturnsCreatedRelationship(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "CREATE (:Test)-[r:RELATES_TO]->(:Test) RETURN r", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.NotNil(t, res.Rows[0][0], "expected RETURN r to return a relationship, got nil")
	_, ok := res.Rows[0][0].(*storage.Edge)
	require.True(t, ok, "expected RETURN r to return *storage.Edge, got %T", res.Rows[0][0])
}
