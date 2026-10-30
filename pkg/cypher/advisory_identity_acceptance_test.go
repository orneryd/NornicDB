package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdvisoryDecodedIdentityAcceptance(t *testing.T) {
	builder := NewASTBuilder()
	t.Run("REMOVE preserves decoded label whitespace", func(t *testing.T) {
		ast, err := builder.Build("MATCH (`a``b`) REMOVE `a``b`:` Label ` RETURN 1")
		require.NoError(t, err)
		require.Len(t, ast.Clauses, 3)
		require.Equal(t, "a`b", ast.Clauses[1].Remove.Items[0].Variable)
		require.Equal(t, []string{" Label "}, ast.Clauses[1].Remove.Items[0].Labels)
	})
	t.Run("UNWIND uses executable AS and decoded binding", func(t *testing.T) {
		ast, err := builder.Build("UNWIND ['x AS y']/* gap */AS/* gap */` a ` RETURN 1")
		require.NoError(t, err)
		require.Len(t, ast.Clauses, 2)
		unwind := ast.Clauses[0].Unwind
		require.Equal(t, " a ", unwind.Variable)
		require.Equal(t, "['x AS y']/* gap */", unwind.RawExpr)
		require.Len(t, unwind.Expression.List, 1)
		require.Equal(t, "x AS y", unwind.Expression.List[0].Literal)
	})
	t.Run("expression identities and string escapes", func(t *testing.T) {
		parse := func(source string) ASTExpression {
			ast, err := builder.Build("RETURN " + source + " AS result")
			require.NoError(t, err)
			require.Len(t, ast.Clauses, 1)
			require.Len(t, ast.Clauses[0].Return.Items, 1)
			return ast.Clauses[0].Return.Items[0].Expression
		}
		for _, input := range []struct{ source, variable, property string }{
			{"`a.b`.` p `", "a.b", " p "},
			{"`a``b`.`p``q`", "a`b", "p`q"},
		} {
			expr := parse(input.source)
			require.Equal(t, ASTExprProperty, expr.Type)
			require.Equal(t, input.variable, expr.Property.Variable)
			require.Equal(t, input.property, expr.Property.Property)
		}
		require.Equal(t, "a`b", parse("`a``b`").Variable)
		require.Equal(t, " p ", parse("$` p `").Parameter)
		require.Equal(t, "0", parse("$0").Parameter)
		require.Equal(t, "line\nnext", parse(`'line\nnext'`).Literal)
	})
	t.Run("UNWIND retains original expression trivia at AS", func(t *testing.T) {
		for _, raw := range []string{
			"['x AS y']/* first *//* second */",
			"['日本 AS 語'] // before binding\n",
			"[1] /* AS inside comment */ ",
		} {
			ast, err := builder.Build("UNWIND " + raw + "AS ` a ` RETURN 1")
			require.NoError(t, err)
			require.Len(t, ast.Clauses, 2)
			require.Equal(t, strings.TrimSpace(raw), ast.Clauses[0].Unwind.RawExpr)
			require.Equal(t, " a ", ast.Clauses[0].Unwind.Variable)
			require.Len(t, ast.Clauses[0].Unwind.Expression.List, 1)
		}
	})
}
