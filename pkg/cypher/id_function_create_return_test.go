package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIDFunction_CreateReturn(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "CREATE (a:Test) RETURN a, id(a), elementId(a)", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 3)

	// Ensure id(a) is not null for CREATE-returned variables.
	require.NotNil(t, res.Rows[0][1], "id(a) returned nil (a=%T)", res.Rows[0][0])
	// Ensure elementId(a) is not null as well.
	require.NotNil(t, res.Rows[0][2], "elementId(a) returned nil (a=%T)", res.Rows[0][0])
}
