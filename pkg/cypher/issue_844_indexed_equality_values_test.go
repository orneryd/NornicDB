package cypher

// Regression coverage for NornicDB #844: an equality on an indexed property
// returned no rows for list values, temporal values and computed values
// (function calls), and a uniqueness constraint accepted duplicate lists. An
// index changes how a match is found, never what it finds: every statement
// here returns the same rows on the indexed label :P as on the unindexed :Q.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue844IndexedEqualityMatchesUnindexed(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	run := func(query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	for _, property := range []string{"tags", "d", "name", "n"} {
		run("CREATE INDEX p_"+property+" FOR (p:P) ON (p."+property+")", nil)
	}
	for _, label := range []string{"P", "Q"} {
		run("CREATE (:"+label+" {id: 1, tags: [1, 2], d: date('2020-01-02'), name: 'A', n: 3})", nil)
		run("CREATE (:"+label+" {id: 2, tags: [2, 1], d: date('2020-01-03'), name: 'B', n: 4})", nil)
	}
	params := map[string]interface{}{"tags": []interface{}{int64(1), int64(2)}, "name": "a"}
	for _, where := range []string{
		"p.tags = [1, 2]",
		"p.tags = [1.0, 2.0]",
		"p.tags = $tags",
		"[1, 2] = p.tags",
		"p.tags IN [[1, 2], [9]]",
		"p.d = date('2020-01-02')",
		"p.name = toUpper('a')",
		"p.name = toUpper($name)",
		"toUpper($name) = p.name",
		"p.n = size([1, 2, 3])",
		"p.n = abs(-3)",
		"p.n = 1 + 2",
		"p.name IN [toUpper('a'), 'Z']",
		"p.name IN [p.name, 'Z']",
		"p.name = p.name",
		"p.tags = [3]",
	} {
		t.Run(where, func(t *testing.T) {
			indexed := run("MATCH (p:P) WHERE "+where+" RETURN p.id ORDER BY p.id", params)
			plain := run("MATCH (p:Q) WHERE "+strings.ReplaceAll(where, "(p:P)", "(p:Q)")+" RETURN p.id ORDER BY p.id", params)
			require.Equal(t, plain, indexed)
		})
	}
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (p:P) WHERE p.name = toUpper('a') RETURN p.id", nil))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (p:P) WHERE p.tags = [1, 2] RETURN p.id", nil))
}

func TestIssue844UniquenessConstraintCoversLists(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT u_tags FOR (u:U) REQUIRE u.tags IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:U {tags: [1, 2]})", nil)
	require.NoError(t, err)
	for _, query := range []string{"CREATE (:U {tags: [1, 2]})", "CREATE (:U {tags: [1.0, 2.0]})"} {
		_, err = exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "already exists", query)
	}
	_, err = exec.Execute(ctx, "CREATE (:U {tags: [2, 1]})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (u:U) RETURN count(u)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

func TestIndexSeekConstant(t *testing.T) {
	exec, _ := newUnitExecutor(t)
	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"x": "a"})
	for expr, want := range map[string]interface{}{"toUpper($x)": "A", "[1, 2]": []interface{}{int64(1), int64(2)}, "'s'": "s", "1 + 2": int64(3)} {
		value, ok := exec.indexSeekConstant(ctx, expr)
		require.True(t, ok, expr)
		require.Equal(t, want, value, expr)
	}
	for _, expr := range []string{"", "p.b", "[p.b]", "unknownFn(1)", "'n1' OR"} {
		_, ok := exec.indexSeekConstant(ctx, expr)
		require.False(t, ok, expr)
	}
}
