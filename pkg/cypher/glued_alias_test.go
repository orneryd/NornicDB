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
		"RETURN [1, 2][0]AS w":         {int64(1)},
		"RETURN (1 + 2)AS x":           {int64(3)},
		"RETURN {a: 1}AS m":            {map[string]interface{}{"a": int64(1)}},
		"RETURN 'a'AS s":               {"a"},
		"RETURN \"b\"as s":             {"b"},
		"WITH [1]AS l RETURN l[0]AS v": {int64(1)},
		"RETURN 'x)AS y' AS s":         {"x)AS y"},
		"RETURN size([1])AS`n n`":      {int64(1)},
		"RETURN 1 AS`n`":               {int64(1)},
		"RETURN 1 as`n`, 2 AS ` m`":    {int64(1), int64(2)},
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
	// A quoted column name keeps its spaces and backticks (Neo4j 5.26 and
	// 2026.09); it was unquoted twice, so ` m` came back as m.
	for query, want := range map[string][]string{
		"WITH 1 AS`x` RETURN x, 2 AS ` m`":                 {"x", " m"},
		"RETURN 1 AS ```a```":                              {"`a`"},
		"WITH 1 AS ` x` RETURN ` x`":                       {" x"},
		"WITH 1 AS ` x` RETURN ` x` AS `y `":               {"y "},
		"UNWIND [1] AS ` u` RETURN ` u` + 1 AS ` v`, ` u`": {" v", " u"},
	} {
		result, err = exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Columns, query)
	}

	require.True(t, aliasKeywordAt(")AS x", 1, aliasAfterValue))
	require.True(t, aliasKeywordAt(")as", 1, aliasAfterValue))
	require.False(t, aliasKeywordAt(")ASC", 1, aliasAfterValue))
	require.False(t, aliasKeywordAt(")A", 1, aliasAfterValue))
	require.True(t, aliasKeywordAt(" AS`", 3, aliasBeforeQuotedName))
	require.False(t, aliasKeywordAt("HAS`", 3, aliasBeforeQuotedName))
	require.False(t, aliasKeywordAt("S`", 1, aliasBeforeQuotedName))
	require.True(t, queryMayNeedCanonicalRewrite("RETURN 1 AS`n`"))
	// Each spacing is memoized like the other canonical rewrites: the
	// second read of a statement returns the first's rewrite.
	for query, want := range map[string]string{
		"RETURN 1 AS`n`":     "RETURN 1 AS `n`",
		"RETURN 'a'AS s":     "RETURN 'a' AS s",
		"RETURN [1][0]AS w ": "RETURN [1][0] AS w ",
	} {
		first, rewrite := canonicalizeQueryText(query)
		require.Equal(t, want, first, query)
		second, memoized := canonicalizeQueryText(query)
		require.Equal(t, want, second, query)
		require.Same(t, rewrite, memoized, query)
	}
	require.True(t, queryMayNeedCanonicalRewrite("RETURN [1][0]AS w"))
	require.False(t, queryMayNeedCanonicalRewrite("RETURN [1][0] AS w ORDER BY (w)ASC"))
}

func TestAliasKeywordBoundaryPolicies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		at       int
		position aliasKeywordPosition
		want     bool
	}{
		{name: "after value folded keyword", query: ")as x", at: 1, position: aliasAfterValue, want: true},
		{name: "after value unicode whitespace", query: ")AS\u00a0x", at: 1, position: aliasAfterValue, want: true},
		{name: "after value quoted name", query: ")AS`x`", at: 1, position: aliasAfterValue, want: true},
		{name: "after value keyword end", query: ")AS", at: 1, position: aliasAfterValue, want: true},
		{name: "after value rejects name suffix", query: ")ASC", at: 1, position: aliasAfterValue},
		{name: "after value preceding token is caller owned", query: "HAS x", at: 1, position: aliasAfterValue, want: true},
		{name: "before name folded keyword", query: " as`x`", at: 3, position: aliasBeforeQuotedName, want: true},
		{name: "before name at source start", query: "AS`x`", at: 2, position: aliasBeforeQuotedName, want: true},
		{name: "before name rejects name prefix", query: "HAS`x`", at: 3, position: aliasBeforeQuotedName},
		{name: "before name quote is caller owned", query: " ASx", at: 3, position: aliasBeforeQuotedName, want: true},
		{name: "before name incomplete keyword", query: "S`x`", at: 1, position: aliasBeforeQuotedName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aliasKeywordAt(tc.query, tc.at, tc.position))
		})
	}
}
