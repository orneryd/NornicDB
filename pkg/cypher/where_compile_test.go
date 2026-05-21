package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompileSimpleWhere_SupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	node := &storage.Node{
		ID: storage.NodeID("n1"),
		Properties: map[string]interface{}{
			"sourceId": "src-1",
			"count":    int64(2),
		},
	}
	ctx := context.Background()

	fn, ok := exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId IS NOT NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", "n.missing IS NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId = 'src-1'")
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId <> 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId != 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId IN ['src-1','src-3']")
	require.True(t, ok)
	require.True(t, fn(node))
}

func TestCompileSimpleWhere_UnsupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	ctx := context.Background()

	_, ok := exec.getCompiledSimpleWhere(ctx, "n", "n.sourceId = 'src-1' AND n.count = 2")
	require.False(t, ok)

	_, ok = exec.getCompiledSimpleWhere(ctx, "n", "size(n.sourceId) > 0")
	require.False(t, ok)
}
