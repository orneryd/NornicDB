package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are contracts of the approved canonical dependency, without executing
// the original public matrix or depending on the aggregate caller correction.
func TestDevelopCanonicalSubqueryDependencies(t *testing.T) {
	t.Run("shared_boundaries", func(t *testing.T) {
		for _, kind := range []string{"COUNT", "EXISTS", "COLLECT"} {
			for _, gap := range []string{" ", "/*gap*/", " //gap\n ", "/* ' // */", "\u00a0/*gap*/\u2003"} {
				expression := kind + gap + "{ RETURN 1 AS v }"
				t.Run(expression, func(t *testing.T) {
					brace := strings.IndexByte(expression, '{')
					keyword, open, scalar := subqueryExpressionKeywordAt(expression, 0)
					require.True(t, scalar)
					require.Equal(t, kind, keyword)
					require.Equal(t, brace, open)
					require.True(t, precedingSubqueryExpressionKeyword(expression, brace))
					require.True(t, isSubqueryBrace(expression, brace))
					require.True(t, hasPatternOrSubquery(expression))
					receiver, projected := staticMapProjectionReceiver(expression)
					require.False(t, projected, receiver)
					_, _, split := staticMapProjectionSplit(expression)
					require.False(t, split)
					require.Empty(t, expressionFreeVariables(expression))
					require.NoError(t, checkExpressionPropertyAccesses(expression, map[string]string{kind: "Integer"}, nil))
				})
			}
		}
	})
	t.Run("keyword_selectors_and_lexical_negatives", func(t *testing.T) {
		for _, expression := range []string{
			"COUNT {.v}", "COUNT {RETURN}", "COUNT {RETURN, other}", "COUNT {RETURN: 1}",
			"xCOUNT { RETURN 1 AS v }", "n:COUNT { RETURN 1 AS v }",
			"`COUNT` { RETURN 1 AS v }", "'COUNT { RETURN 1 AS v }'",
			"\"COUNT { RETURN 1 AS v }\"", "COUNT { unknown }",
		} {
			t.Run(expression, func(t *testing.T) {
				brace := strings.IndexByte(expression, '{')
				require.False(t, precedingSubqueryExpressionKeyword(expression, brace))
				require.False(t, isSubqueryBrace(expression, brace))
				require.Empty(t, findSubqueryExpressions(expression))
			})
		}
		for _, kind := range []string{"count", "exists", "collect"} {
			for _, selector := range []string{".v", "RETURN", "RETURN, other", "RETURN: 1"} {
				expression := kind + " {" + selector + "}"
				receiver, projected := staticMapProjectionReceiver(expression)
				require.True(t, projected, expression)
				require.Equal(t, kind, receiver)
				variable, items, split := staticMapProjectionSplit(expression)
				require.True(t, split, expression)
				require.Equal(t, kind, variable)
				require.Equal(t, selector, items)
			}
		}
	})
	t.Run("clause_body_boundary_contract", func(t *testing.T) {
		for _, body := range []string{
			"MATCH (n) RETURN n", "OPTIONAL MATCH (n) RETURN n", "CALL { RETURN 1 AS v } RETURN v",
			"WITH 1 AS v RETURN v", "UNWIND [1] AS v RETURN v", "RETURN 1 AS v",
			"ORDER BY 1 RETURN 1 AS v", "LIMIT 1 RETURN 1 AS v", "SKIP 1 RETURN 1 AS v",
			"OFFSET 1 RETURN 1 AS v", "()-->()", "/*body*/ RETURN 1 AS v",
		} {
			expression := "COUNT /*gap*/ { " + body + " }"
			require.True(t, precedingSubqueryExpressionKeyword(expression, strings.IndexByte(expression, '{')), expression)
		}
		for _, body := range []string{"", "/*body*/", "-", "1", ".v", "other", "RETURN", "RETURN, other", "RETURN: 1"} {
			expression := "COUNT { " + body + " }"
			require.False(t, precedingSubqueryExpressionKeyword(expression, strings.IndexByte(expression, '{')), expression)
		}
	})
	t.Run("resumable_scanner", func(t *testing.T) {
		query := "'quoted { }' + /* comment { } */ COUNT\u00a0/*gap*/\u2003{ RETURN 1 AS v } + {a: 1} + EXISTS //gap\n { RETURN true AS v }"
		scanner := newSubqueryBraceScanner(query, 0)
		require.False(t, scanner.at(-1))
		require.False(t, scanner.at(len(query)))
		require.False(t, scanner.at(0))
		countBrace := strings.Index(query, "{ RETURN 1")
		existsBrace := strings.Index(query, "{ RETURN true")
		for brace := 0; brace < len(query); brace++ {
			if query[brace] != '{' {
				continue
			}
			want := brace == countBrace || brace == existsBrace
			require.Equal(t, want, scanner.at(brace), "brace %d", brace)
			require.Equal(t, want, scanner.at(brace), "repeat brace %d", brace)
		}
		for _, query := range []string{
			"CALL { RETURN 1 AS v }", "CALL (x) { RETURN x }",
			"CALL () { RETURN 1 AS v }", "{value: COUNT /*gap*/ { RETURN 1 AS v }}",
		} {
			brace := strings.LastIndexByte(query, '{')
			require.True(t, precedingSubqueryExpressionKeyword(query, brace), query)
		}
		prefix := "ignored prefix "
		tail := "COUNT /*gap*/ { RETURN 1 AS v }"
		scoped := newSubqueryBraceScanner(prefix+tail, len(prefix))
		require.True(t, scoped.at(strings.IndexByte(prefix+tail, '{')))
	})
	t.Run("whole_operand_mask_offsets", func(t *testing.T) {
		count := "COUNT /*gap*/ { WITH 1 AS hidden RETURN hidden }"
		exists := "EXISTS //gap\n { RETURN true AS hidden }"
		collect := "COLLECT /*gap*/ { RETURN 1 AS hidden }"
		expression := "left + " + count + " + " + exists + " + " + collect + " + right"
		want := "left + 0" + strings.Repeat(" ", len(count)-1) + " + true" + strings.Repeat(" ", len(exists)-4) + " + []" + strings.Repeat(" ", len(collect)-2) + " + right"
		masked := maskSubqueryBodies(expression)
		require.Equal(t, want, masked)
		require.Len(t, masked, len(expression))
		require.Equal(t, strings.Index(expression, "right"), strings.Index(masked, "right"))
		require.Equal(t, want, maskSemanticBraceBodies(expression))
		require.Equal(t, []string{"left", "right"}, expressionFreeVariables(expression))
		for _, expression := range []string{"plain + right", "'COUNT { RETURN hidden }' + right", "count {.v}"} {
			require.Equal(t, expression, maskSubqueryBodies(expression))
		}
	})
	t.Run("existing_map_validation_baseline", func(t *testing.T) {
		for _, query := range []string{
			"RETURN {ok: 1}", "RETURN {`odd key`: 1}", "RETURN n {.v}",
			"RETURN 'COUNT {bad}'", "RETURN 1 /* {bad: } */",
			"RETURN COUNT /*gap*/ { RETURN 1 AS v }",
			"RETURN [" + strings.TrimSuffix(strings.Repeat("{ok: 1},", 128), ",") + "]",
		} {
			require.NoError(t, validateStaticMapKeys(query), query)
		}
		for _, query := range []string{"RETURN {1: 2}", "MATCH (count {bad}) RETURN count", "MATCH (n:COUNT {bad}) RETURN n"} {
			require.Error(t, validateStaticMapKeys(query), query)
		}
	})
	t.Run("receiver_guards", func(t *testing.T) {
		for _, expression := range []string{"1 {.v}", "n.prop {.v}", "head(xs) {.v}", "{a: 1} {.a}"} {
			_, projected := staticMapProjectionReceiver(expression)
			require.True(t, projected, expression)
		}
		for _, expression := range []string{"1", "head(xs {.v}", "n {.v", "{a: 1}", "1 + {a: 1}"} {
			_, projected := staticMapProjectionReceiver(expression)
			require.False(t, projected, expression)
		}
	})
	t.Run("existing_order_and_property_consumers", func(t *testing.T) {
		query := "RETURN COUNT /*gap*/ { LIMIT 1 RETURN 1 AS v } AS v"
		rewritten, mapping := desugarStandaloneOrderClauses(query)
		require.NotNil(t, mapping)
		require.Contains(t, rewritten, "WITH * LIMIT 1 RETURN 1 AS v")
		require.NoError(t, checkExpressionPropertyAccesses("COUNT /*gap*/ { RETURN n.value AS v }", map[string]string{"COUNT": "Integer", "n": "Node"}, nil))
		require.NoError(t, checkExpressionPropertyAccesses("n.value", map[string]string{"n": "Map"}, nil))
		require.False(t, hasPatternOrSubquery("n.value = 1"))
		require.False(t, hasPatternOrSubquery("'COUNT { RETURN 1 }'"))
		require.True(t, hasPatternOrSubquery("(n)-->(m)"))
	})
	t.Log("INNER_AGG_DEPENDENCY_CONTRACT_COMPLETE")
}
