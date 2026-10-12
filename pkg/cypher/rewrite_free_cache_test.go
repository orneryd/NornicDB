package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A statement whose text the front-end passes leave unchanged is remembered,
// so a repeat skips them; a statement that uses their forms is rewritten on
// every run, and the language version is part of what is remembered (#907).
func TestRewriteFreeTexts(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	plain := "RETURN 1 + 2 AS rewriteFreeProbe"
	for range 2 {
		result, err := exec.Execute(ctx, plain, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	}
	require.True(t, rewriteFree(rewriteFreeFunctionAliases, false, plain))
	require.True(t, rewriteFree(rewriteFreeCypher25Expressions, false, plain))
	require.True(t, rewriteFree(rewriteFreeQueryStructure, false, plain))
	require.False(t, rewriteFree(rewriteFreeCypher25Expressions, true, plain), "Cypher 25 is remembered apart")

	// Forms the passes rewrite are rewritten on every run.
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"UNWIND [1, 2] AS x RETURN collect_list(x) AS l", []interface{}{int64(1), int64(2)}},
		{"CYPHER 25 RETURN ALL 5 AS v", int64(5)},
		{"CYPHER 25 RETURN 1 AS a NEXT RETURN a + 1 AS b", int64(2)},
	} {
		for range 2 {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.want, result.Rows[0][0], tc.query)
		}
	}
}
