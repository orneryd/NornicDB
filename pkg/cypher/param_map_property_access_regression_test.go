package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParamMapPropertyAccess_ReturnShapes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "param_map_access")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"d": map[string]interface{}{
			"uuid": "k",
			"name": "Ada",
		},
	}

	result, err := exec.Execute(ctx, "RETURN $d.uuid AS uuid", params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "k", result.Rows[0][0])

	result, err = exec.Execute(ctx, "RETURN $d['uuid'] AS uuid", params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "k", result.Rows[0][0])
}

func TestParamMapPropertyAccess_WithAliasProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "param_map_with_alias")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"d": map[string]interface{}{
			"uuid": "k",
			"name": "Ada",
		},
	}

	result, err := exec.Execute(ctx, "WITH $d AS d RETURN d.uuid AS uuid", params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "k", result.Rows[0][0])

	result, err = exec.Execute(ctx, "WITH $d AS d RETURN d['uuid'] AS uuid", params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "k", result.Rows[0][0])
}
