package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Product check for the native physical-read contract: a later pattern reads
// an already-bound node. Its literal property constraint must see the stored
// property even when RETURN reads a different property.
func TestPhysicalPatternReadPublic(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_reads"))
	_, err := exec.Execute(ctx, "CREATE (:ReadPattern {a:1,b:42}), (:ReadPattern {a:2,b:99})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (p:ReadPattern) MATCH (p {a:1}) RETURN p.b",
		"MATCH (p:ReadPattern) WITH p MATCH (p {a:1}) RETURN p.b",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)
		})
	}
	result, err := exec.Execute(ctx, "MATCH (p:ReadPattern) MATCH (p {a:1}) RETURN p", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, int64(1), node.Properties["a"])
	require.Equal(t, int64(42), node.Properties["b"])
}
