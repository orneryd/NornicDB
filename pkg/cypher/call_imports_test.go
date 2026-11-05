package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A CALL subquery imports the same variables whatever the whitespace,
// comments or quoting of its scope clause or importing WITH.
func TestCallSubqueryImports(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scope  string
		scoped bool
		body   string
		want   []string
	}{
		{"with", "", false, "WITH x MATCH (n) RETURN n", []string{"x"}},
		{"with_list", "", false, "with a, b match (n) return n", []string{"a", "b"}},
		{"with_tab", "", false, "WITH\tx RETURN x", []string{"x"}},
		{"with_newline", "", false, "WITH\nx RETURN x", []string{"x"}},
		{"with_block_comment", "", false, "WITH/* import */ x RETURN x", []string{"x"}},
		{"with_line_comment", "", false, "WITH // import\n x RETURN x", []string{"x"}},
		{"with_star", "", false, "WITH * RETURN x", []string{"*"}},
		{"with_star_comment", "", false, "WITH/**/* RETURN x", []string{"*"}},
		{"with_quoted", "", false, "WITH `a``b`, `odd name` RETURN 1 AS v", []string{"a`b", "odd name"}},
		{"with_projection", "", false, "WITH x, $k AS k RETURN x + k", []string{"x"}},
		{"with_alias", "", false, "WITH x AS y RETURN y", nil},
		{"with_literal", "", false, "WITH 1 AS x RETURN x", nil},
		{"no_with", "", false, "MATCH (n) RETURN n", nil},
		{"withx", "", false, "WITHX something", nil},
		{"without", "", false, "WITHOUT something", nil},
		{"scope", "x, y", true, "RETURN x", []string{"x", "y"}},
		{"scope_comment", "x /* import */", true, "RETURN x", []string{"x"}},
		{"scope_comments", "/* a */ x // b\n, y", true, "RETURN x", []string{"x", "y"}},
		{"scope_star", "*", true, "RETURN x", []string{"*"}},
		{"scope_empty", "", true, "WITH x RETURN x", []string{}},
		{"scope_comment_only", " /* none */ ", true, "WITH x RETURN x", []string{}},
		{"scope_quoted", "`a``b`, `odd name`", true, "RETURN 1", []string{"a`b", "odd name"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := callSubqueryImportedVariables(t.Context(), tt.scope, tt.scoped, tt.body)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
	// A scope clause item that is not a variable is a syntax error.
	for _, scope := range []string{"1", "x + 1", "x, 1", "x, *", "x AS y", "$p"} {
		_, err := callSubqueryImportedVariables(t.Context(), scope, true, "RETURN 1")
		require.ErrorContains(t, err, "CALL imports must contain only variable names", scope)
	}
	// Single-database execution reads scope clauses with the same reader.
	declared, err := parseCallSubqueryImportVariables(t.Context(), "CALL (`a``b` /* c */, x) { RETURN 1 }")
	require.NoError(t, err)
	require.Equal(t, []string{"a`b", "x"}, declared)
	declared, err = parseCallSubqueryImportVariables(t.Context(), "CALL (/* all */ *) { RETURN 1 }")
	require.NoError(t, err)
	require.Nil(t, declared)
	_, err = parseCallSubqueryImportVariables(t.Context(), "CALL (1) { RETURN 1 }")
	require.ErrorContains(t, err, "CALL imports must contain only variable names")
	require.True(t, callSubqueryHasScopeClause(t.Context(), "CALL /* scope */ (x) { RETURN x }"))
}
