package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnwindWildcardKeepsLocalGroupingBinding(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		_, err := exec.Execute(ctx, "CREATE (:Target {n:1}),(:Target {n:2})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN *,count(*) AS total", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "k", "total"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Len(t, result.Rows[0], 3)
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok)
		require.Equal(t, map[string]any{"n": int64(1)}, node.Properties)
		require.Equal(t, int64(1), result.Rows[0][1])
		require.Equal(t, int64(1), result.Rows[0][2])
	})
}
