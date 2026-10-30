package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoundationASTMutationItems(t *testing.T) {
	for _, test := range []struct {
		query, variable, property string
		remove                    bool
	}{
		{"MATCH (n) REMOVE n.`a,b` RETURN n", "n", "a,b", true},
		{"MATCH (n) SET n.`a=b` = 1 RETURN n", "n", "a=b", false},
		{"MATCH (n) SET n.`a.b` = 1 RETURN n", "n", "a.b", false},
		{"MATCH (`n.x`) SET `n.x`.`a``=b.c` = 1 RETURN `n.x`", "n.x", "a`=b.c", false},
		{"MATCH (`n.x`) REMOVE `n.x`.`a``:,b.c` RETURN `n.x`", "n.x", "a`:,b.c", true},
	} {
		t.Run(test.query, func(t *testing.T) {
			built, err := NewASTBuilder().Build(test.query)
			require.NoError(t, err)
			info := mustAnalyze(t, NewQueryAnalyzer(4), t.Context(), test.query)
			for _, ast := range []*AST{built, info.GetAST()} {
				require.NotNil(t, ast)
				require.Len(t, ast.Clauses, 3)
				if test.remove {
					require.NotNil(t, ast.Clauses[1].Remove)
					require.Len(t, ast.Clauses[1].Remove.Items, 1)
					item := ast.Clauses[1].Remove.Items[0]
					require.Equal(t, test.variable, item.Variable)
					require.Equal(t, test.property, item.Property)
					require.Empty(t, item.Labels)
				} else {
					require.NotNil(t, ast.Clauses[1].Set)
					require.Len(t, ast.Clauses[1].Set.Items, 1)
					item := ast.Clauses[1].Set.Items[0]
					require.Equal(t, test.variable, item.Variable)
					require.Equal(t, test.property, item.Property)
					require.Equal(t, "1", item.RawValue)
					require.Equal(t, ASTExprLiteral, item.Value.Type)
					require.Equal(t, int64(1), item.Value.Literal)
				}
			}
			require.Same(t, info.GetAST(), info.GetAST())
		})
	}
	t.Run("label branch and mixed assignments", func(t *testing.T) {
		removed := NewASTBuilder().parseRemove(cyphertext.NormalizeSource("REMOVE n:`a,b`:`a:b`, n.`c,d`"))
		require.Len(t, removed.Items, 2)
		require.Equal(t, []string{"a,b", "a:b"}, removed.Items[0].Labels)
		require.Equal(t, "c,d", removed.Items[1].Property)
		items := NewASTBuilder().parseSetItems(cyphertext.NormalizeSource("n.`a=b` = {text: 'x=y,z'}, n.x += 1"))
		require.Len(t, items, 2)
		require.Equal(t, "a=b", items[0].Property)
		require.Equal(t, "{text: 'x=y,z'}", items[0].RawValue)
		require.Equal(t, "x", items[1].Property)
		require.Equal(t, "1", items[1].RawValue)
	})
}

func TestFoundationCommentExtentConsumers(t *testing.T) {
	for _, ending := range []string{"\r", "\n", "\r\n"} {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(ending, "\r", "CR"), "\n", "LF"), func(t *testing.T) {
			require.Equal(t, [2]string{"TIME", "ZONE"}, nextSignificantWords(" // text"+ending+"TIME /* c */ ZONE", 0))
			before := "STARTS // text" + ending
			require.Equal(t, "STARTS", previousSignificantWord(before, len(before)))
			query := "\ufeff // text" + ending + "CREATE DATABASE d"
			require.Equal(t, strings.Index(query, "CREATE"), firstExecutableCypherOffset(query))
			require.True(t, classifyStatement(query).adminPermission)
			expression := "'value' STARTS // text" + ending + "WITH 'v'"
			at := strings.Index(expression, "WITH")
			require.True(t, isExpressionWith(expression, at, at+len("WITH")))
			use, rest, found, err := parseUseClause(t.Context(), "USE nornic // text"+ending+"RETURN 1", false, cyphertext.RawSource)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "nornic", use.Name)
			require.Equal(t, "// text"+ending+"RETURN 1", rest)
		})
	}
	before := "STARTS /* comment containing /* text */ "
	require.Equal(t, "STARTS", previousSignificantWord(before, len(before)))
	quoted := "'not // a comment' ENDS /* text */ "
	require.Equal(t, "ENDS", previousSignificantWord(quoted, len(quoted)))
	require.Empty(t, nextSignificantWords(" /* unfinished", 0)[0])
}
