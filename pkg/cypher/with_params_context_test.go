package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWithParamsPreservesRequiredContext(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancelCause(context.WithValue(t.Context(), key{}, "origin"))
	defer cancel(nil)
	require.Same(t, parent, withParams(parent, nil))
	params := map[string]interface{}{"v": int64(7)}
	child := withParams(parent, params)
	require.Equal(t, "origin", child.Value(key{}))
	require.Equal(t, params, getParamsFromContext(child))
	require.Equal(t, params, getParamsFromContext(withParams(child, params)))
	first := parameterRowValues(child)
	second := parameterRowValues(child)
	require.True(t, sameParameterMap(first, second), "row cache is reused")
	require.Equal(t, parent.Done(), child.Done())
	cancel(context.Canceled)
	require.ErrorIs(t, child.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(child), context.Canceled)
}

func TestPatternParameterPublicPathRetainsStatementContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parameter_context"))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result, err := exec.Execute(ctx, "WITH 7 AS value MERGE (n:RequiredParameterContext {v:value}) RETURN n.v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		cancel()
		result, err = exec.Execute(ctx, "WITH 8 AS value MERGE (n:RequiredParameterContext {v:value}) RETURN n.v", nil)
		require.Error(t, err)
		require.Nil(t, result)
		result, err = exec.Execute(t.Context(), "MATCH (n:RequiredParameterContext) RETURN n.v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, "canceled statement must not add a second node")
	})
}

// Rebinding is a new parameter evaluation, even if the input map is the same
// mutable object: converted rows from the previous binding must not survive.
func TestWithParamsRebindRefreshesMutableMapRows(t *testing.T) {
	params := map[string]interface{}{"v": int64(7)}
	original := withParams(t.Context(), params)
	require.Equal(t, int64(7), parameterRowValues(original)["$v"])
	params["v"] = int64(8)
	rebound := withParams(original, params)
	require.Equal(t, int64(8), parameterRowValues(rebound)["$v"])
	distinct := map[string]interface{}{"v": int64(8)}
	another := withParams(rebound, distinct)
	require.True(t, sameParameterMap(distinct, getParamsFromContext(another)))
	distinct["v"] = int64(9)
	require.Equal(t, int64(9), parameterRowValues(another)["$v"])
	require.Equal(t, int64(8), parameterRowValues(rebound)["$v"], "a distinct binding cannot replace the old cache")
}
