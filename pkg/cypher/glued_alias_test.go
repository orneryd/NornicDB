package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// AS right after a closing bracket or quote is the alias keyword, as in
// Neo4j: RETURN [1, 2][0]AS w is the column w (#907). It was "variable w
// is not defined": every clause reader split items at a spaced AS.
func TestAliasGluedToClosingBracket(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "glued_alias"))
	ctx := context.Background()
	for query, want := range map[string][]interface{}{
		"RETURN [1, 2][0]AS w":                        {int64(1)},
		"RETURN (1 + 2)AS x":                          {int64(3)},
		"RETURN {a: 1}AS m":                           {map[string]interface{}{"a": int64(1)}},
		"RETURN 'a'AS s":                              {"a"},
		"RETURN \"b\"as s":                            {"b"},
		"WITH [1]AS l RETURN l[0]AS v":                {int64(1)},
		"RETURN 'x)AS y' AS s":                        {"x)AS y"},
		"RETURN size([1])AS`n n`":                     {int64(1)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{want}, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "UNWIND [1, 2]AS x RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
	result, err = exec.Execute(ctx, "RETURN [1, 2][0]AS w, (2)AS `x y`", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"w", "x y"}, result.Columns)

	require.True(t, gluedAliasKeywordAt(")AS x", 1))
	require.True(t, gluedAliasKeywordAt(")as", 1))
	require.False(t, gluedAliasKeywordAt(")ASC", 1))
	require.False(t, gluedAliasKeywordAt(")A", 1))
	require.True(t, queryMayNeedCanonicalRewrite("RETURN [1][0]AS w"))
	require.False(t, queryMayNeedCanonicalRewrite("RETURN [1][0] AS w ORDER BY (w)ASC"))
}
