package cypher

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// Reflection lets this same public-product regression run on the published
// baseline that has no argument records. It does not supply missing extents.
func requireUseArgumentProducts(t *testing.T, product interface{}, source string, expressions []string) {
	t.Helper()
	arguments := reflect.ValueOf(product).FieldByName("Arguments")
	require.True(t, arguments.IsValid(), "USE product lost its grammar-owned argument extents")
	require.Equal(t, reflect.Slice, arguments.Kind())
	require.Equal(t, len(expressions), arguments.Len())
	after := 0
	for index, expression := range expressions {
		argument := arguments.Index(index)
		span, ok := argument.FieldByName("Span").Interface().(cyphertext.Span)
		require.True(t, ok, "argument extent must use the shared byte-span product")
		relative := strings.Index(source[after:], expression)
		require.NotEqual(t, -1, relative, "test expression must be in its source")
		start := after + relative
		require.Equal(t, cyphertext.Span{Start: start, End: start + len(expression)}, span)
		require.Equal(t, expression, argument.FieldByName("Text").String())
		require.Equal(t, source[span.Start:span.End], argument.FieldByName("Text").String())
		after = span.End
	}
}

func TestFoundationUseArgumentSourceProducts(t *testing.T) {
	cases := []struct {
		name        string
		expressions []string
	}{
		{"quoted_parameter_function_expression", []string{"`a``b`", "$`quoted key`", "coalesce(/* inner é */ $g, 'c')", "(1 /* plus */ + 2)"}},
		{"string_comment_and_line_trivia", []string{"'x,y//é'", "$g", "toLower('CÓMP.A')", "1 // RETURN decoy\r\n + 2"}},
		{"repeated_argument_text", []string{"$g", "$g", "coalesce($g, $g)", "'é' + 'é'"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			selector := "USE graph.byName( /* before */ " + strings.Join(test.expressions, " /* after */, \t") + " /* final */ )"
			query := " \t\u00a0" + selector + "\nRETURN 1 AS out"
			t.Run("leading_grammar", func(t *testing.T) {
				reference, err := cypherantlr.ReadUseReference(context.Background(), cyphertext.NewSource(context.Background(), query, cyphertext.RawSource))
				require.NoError(t, err)
				require.Equal(t, test.expressions, useArgumentTexts(reference.Arguments))
				requireUseArgumentProducts(t, reference, query, test.expressions)
			})
			t.Run("routing_reference", func(t *testing.T) {
				reference, remaining, hasUse, err := fabric.ReadUseClause(t.Context(), query, false, fabric.RawSource)
				require.NoError(t, err)
				require.True(t, hasUse)
				require.Equal(t, "RETURN 1 AS out", remaining)
				require.Equal(t, strings.Index(query, "\nRETURN"), reference.End)
				requireUseArgumentProducts(t, reference.UseClause, query, test.expressions)
			})
			t.Run("routing_clause", func(t *testing.T) {
				clause, _, hasUse, err := fabric.ParseUseClause(t.Context(), query, false, fabric.RawSource)
				require.NoError(t, err)
				require.True(t, hasUse)
				requireUseArgumentProducts(t, clause, query, test.expressions)
			})
			for _, nested := range []bool{false, true} {
				name, full := "complete_statement", "/* original é */ "+query
				if nested {
					name, full = "nested_statement", "/* original é */ CALL { /* outer */ "+query+" } RETURN out"
				}
				t.Run(name, func(t *testing.T) {
					syntax, err := cypherantlr.ParseSyntax(context.Background(), full, cypherantlr.StatementEntry, false, cypherantlr.RawSource)
					require.NoError(t, err)
					count := 0
					syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
						if node.Rule() == cypherantlr.CypherParserRULE_useClause {
							count++
							requireUseArgumentProducts(t, node.UseReference(nil), full, test.expressions)
						}
						return nil
					})
					require.Equal(t, 1, count)
				})
			}
		})
	}
	t.Run("ordinary_static_and_empty_function_controls", func(t *testing.T) {
		clause, remaining, hasUse, err := fabric.ParseUseClause(t.Context(), "USE `cmp` . `a` RETURN 1", false, fabric.RawSource)
		require.NoError(t, err)
		require.True(t, hasUse)
		require.Equal(t, "cmp.a", clause.Name)
		require.Equal(t, "RETURN 1", remaining)
		require.Empty(t, clause.Args)
		reference, err := cypherantlr.ReadUseReference(context.Background(), cyphertext.NewSource(context.Background(), "USE graph.byName() RETURN 1", cyphertext.RawSource))
		require.NoError(t, err)
		require.Equal(t, "graph.byName", reference.Function)
		require.Empty(t, reference.Arguments)
	})
}
