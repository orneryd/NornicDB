package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteWith_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "with_branch_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("standalone RETURN is valid", func(t *testing.T) {
		res, err := exec.Execute(ctx, "RETURN 1", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
	})

	t.Run("WITH requires a continuation", func(t *testing.T) {
		res, err := exec.Execute(ctx, "WITH 7 AS x, 'v' AS y", nil)
		requireSyntaxErrorStatus(t, err, "WITH 7 AS x, 'v' AS y")
		require.Nil(t, res)
	})

	t.Run("WHERE filtered non-aggregate returns zero rows", func(t *testing.T) {
		res, err := exec.Execute(ctx, "WITH 1 AS x WHERE false RETURN x", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"x"}, res.Columns)
		require.Empty(t, res.Rows)
	})

	t.Run("WHERE filtered aggregate returns identity row", func(t *testing.T) {
		res, err := exec.Execute(ctx, "WITH 1 AS x WHERE false RETURN collect(x) AS xs, count(x) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"xs", "c"}, res.Columns)
		require.Len(t, res.Rows, 1)
		require.Equal(t, []interface{}{}, res.Rows[0][0])
		require.EqualValues(t, 0, res.Rows[0][1])
	})

	t.Run("map literal binding used in CREATE remainder", func(t *testing.T) {
		res, err := exec.Execute(ctx, "WITH {name:'Neo'} AS m CREATE (n:Tmp {name: m.name}) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"c"}, res.Columns)
		require.Len(t, res.Rows, 1)

		verify, err := exec.Execute(ctx, "MATCH (n:Tmp {name:'Neo'}) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, verify.Rows[0][0])
	})

	t.Run("nested list binding used in UNWIND remainder", func(t *testing.T) {
		res, err := exec.Execute(ctx, "WITH [[1,2],[3,4]] AS matrix UNWIND matrix AS row RETURN row", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"row"}, res.Columns)
		require.Len(t, res.Rows, 2)
		first, ok := res.Rows[0][0].([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{int64(1), int64(2)}, first)
	})

	t.Run("param substitution path", func(t *testing.T) {
		ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": int64(11)})
		res, err := exec.Execute(ctxParams, "WITH $x AS v RETURN v", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"v"}, res.Columns)
		require.EqualValues(t, 11, res.Rows[0][0])
	})
}

func TestExecuteOptionalMatch_ErrorAndEmptyBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "opt_match_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "OPTIONAL MATCH (n:Missing) RETURN ,", getParamsFromContext(ctx))
	require.Error(t, err)

	res, err := exec.Execute(ctx, "OPTIONAL MATCH (n:Missing) RETURN n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Nil(t, res.Rows[0][0])

	_, err = exec.Execute(ctx, "CREATE (n:Person {name:'A'})", nil)
	require.NoError(t, err)
	res, err = exec.Execute(ctx, "OPTIONAL MATCH (n:Person {name:'A'}) RETURN n.name AS name", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Equal(t, "A", res.Rows[0][0])
}
