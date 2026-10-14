package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchOrderByLimit_TopKFastPath(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 100; i >= 1; i-- {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TopK {v:%d})", i), nil)
		require.NoError(t, err)
	}

	asc, err := exec.Execute(ctx, "MATCH (n:TopK) RETURN n.v ORDER BY n.v LIMIT 5", nil)
	require.NoError(t, err)
	require.Len(t, asc.Rows, 5)
	for i := 0; i < 5; i++ {
		require.Equal(t, int64(i+1), asc.Rows[i][0])
	}

	desc, err := exec.Execute(ctx, "MATCH (n:TopK) RETURN n.v ORDER BY n.v DESC LIMIT 5", nil)
	require.NoError(t, err)
	require.Len(t, desc.Rows, 5)
	for i := 0; i < 5; i++ {
		require.Equal(t, int64(100-i), desc.Rows[i][0])
	}
}
