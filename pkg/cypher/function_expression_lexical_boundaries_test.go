package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Comments and quoted delimiters must preserve function receivers and groups.
func TestFunctionAndSubqueryPostfixLexicalBoundaries(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		cases := []struct {
			name, query string
			want        [][]interface{}
			wantErr     bool
		}{
			{"map-aggregate", "UNWIND [1,2] AS x RETURN {c:count(x)} AS value", [][]interface{}{{map[string]interface{}{"c": int64(2)}}}, false},
			{"slice-bound-aggregate", "UNWIND [1,2] AS x RETURN {c:[1,2,3][..count(x)]} AS v", [][]interface{}{{map[string]interface{}{"c": []interface{}{int64(1), int64(2)}}}}, false},
			{"leading-block-comment", "/* } { ' \" */ RETURN 'ok' AS v", [][]interface{}{{"ok"}}, false},
			{"commented-invalid-list-access", "RETURN /* [1]['bad'] */ 1 AS v", [][]interface{}{{int64(1)}}, false},
			{"commented-invalid-map-access", "RETURN /* {a:1}[2] */ 1 AS v", [][]interface{}{{int64(1)}}, false},
			{"leading-line-comment", "// } { ' \"\nRETURN 'ok' AS v", [][]interface{}{{"ok"}}, false},
			{"comment-before-paren-list-access", "RETURN keys /*gap*/ ({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
			{"quoted-comment-markers", "RETURN keys /* '/*' ) [0] { */ ({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
			{"collect-subquery-comment-index", "RETURN COLLECT /*gap*/ { RETURN 1 AS x }[0] AS v", [][]interface{}{{int64(1)}}, false},
			{"ideographic-space-before-bracket", "RETURN keys({a:1})\u3000[0] AS v", [][]interface{}{{"a"}}, false},
			{"whitespace-before-paren-list-access", "RETURN keys ({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
			{"line-comment-before-paren", "RETURN keys // c\n({a:1})[0] AS v", [][]interface{}{{"a"}}, false},
			{"quoted-paren-grouped", "RETURN (toUpper(')')) AS v", [][]interface{}{{")"}}, false},
			{"spaced-variable-receiver", "WITH [5] AS x RETURN x [0] AS v", [][]interface{}{{int64(5)}}, false},
			{"commented-map-receiver", "WITH {a:7} AS m RETURN m /* gap */ ['a'] AS v", [][]interface{}{{int64(7)}}, false},
			{"spaced-literal-receiver", "RETURN [5] [0] AS v", [][]interface{}{{int64(5)}}, false},
			{"spaced-invalid-list-index", "RETURN [1] ['a'] AS v", nil, true},
			{"spaced-invalid-map-index", "WITH {a:7} AS m RETURN m [1] AS v", nil, true},
			{"keyword-named-receiver", "WITH [5] AS count RETURN count[0] AS v", [][]interface{}{{int64(5)}}, false},
			{"prefix-not-integer-empty-input", "UNWIND [] AS r MATCH (n) WITH n, 1 AS v RETURN NOT v AS a", nil, true},
		}
		for _, normalize := range []bool{false, true} {
			name := "raw"
			if normalize {
				name = "normalized"
			}
			t.Run(name, func(t *testing.T) {
				prior := config.IsCypherQueryNormalizationEnabled()
				t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
				config.SetCypherQueryNormalizationEnabled(normalize)
				for _, c := range cases {
					c := c
					t.Run(c.name, func(t *testing.T) {
						store := newTestMemoryEngine(t)
						result, err := NewStorageExecutor(store).Execute(t.Context(), c.query, nil)
						if c.wantErr {
							require.Error(t, err, c.query)
							return
						}
						require.NoError(t, err, c.query)
						require.Equal(t, c.want, result.Rows, c.query)
					})
				}
			})
		}
	})
}

func TestVectorProjectionRetainsQuotedParenthesisGroup(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		base := newTestMemoryEngine(t)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
		ctx := t.Context()
		_, err := exec.Execute(ctx, "CREATE (:Lex {embedding:[1.0,0.0]})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, `MATCH (n:Lex)
WITH n, vector.similarity.cosine(n.embedding,[1.0,0.0]) AS s
RETURN s, (toUpper(')')) AS v ORDER BY s DESC LIMIT 1`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.InDelta(t, 1.0, result.Rows[0][0], 1e-6)
		require.Equal(t, ")", result.Rows[0][1])
	})
}

// Backticks quote function names; comments separate arguments without becoming
// part of the native vector metric/type token. These are NornicDB extensions
// under Cypher 5, independently of Neo4j 5's dotted similarity functions.
func TestNativeVectorCallLexicalBoundaries(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, query := range []string{
			"RETURN `vector_norm`(vector([3,4], 2, FLOAT), EUCLIDEAN) AS v",
			"RETURN vector_norm /* gap */ (vector([3,4], 2, FLOAT), /* metric */ EUCLIDEAN /* end */) AS v",
			"RETURN `vector_norm`(`vector`([3,4], 2, /* type */ FLOAT), EUCLIDEAN) AS v",
			"RETURN vector_norm(vector([3,4], 2, FLOAT), // metric\n EUCLIDEAN) AS v",
		} {
			exec, _ := newTestExecutor(t)
			result, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1)
			require.InDelta(t, 5, result.Rows[0][0], 1e-9, query)
		}
		for _, query := range []string{
			"RETURN vector_norm(vector([3,4], 2, FLOAT), 'EUCLIDEAN') AS v",
			"RETURN vector([3,4], 2, 'FLOAT') AS v",
			"RETURN vector([3,4], 0, FLOAT) AS v",
			"RETURN vector_norm(vector([3,4], 2, FLOAT), /* absent */) AS v",
		} {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), query, nil)
			require.Error(t, err, query)
		}
	})
}

// Predicate/clause keywords before a group are not function names. Extending
// receiver lookup across comments must preserve operator precedence.
func TestPostfixReceiverPreservesPredicatePrecedence(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, sample := range []struct {
			query string
			want  interface{}
		}{
			{"RETURN NOT ([true])[0] AS v", false},
			{"MATCH (n:Lex) WHERE NOT (n.list)[0] = 9 RETURN n.k AS v", "x"},
			{"MATCH (n:Lex) RETURN NOT (n.list)[0] = 9 AS v", true},
			{"MATCH (n:Lex) WITH n.list AS l RETURN NOT (l)[0] = 9 AS v", true},
			{"RETURN NOT /* gap */ ([true])[0] AS v", false},
			{"RETURN NOT ({a:true})['a'] AS v", false},
			{"RETURN true AND ([true])[0] AS v", true},
			{"RETURN false OR ([true])[0] AS v", true},
			{"RETURN true XOR ([true])[0] AS v", false},
			{"RETURN 1 IN ([[1]])[0] AS v", true},
			{"RETURN 1 IN ([1,2])[0..1] AS v", true},
			{"WITH [1,2] AS l RETURN 1 IN (l)[0..1] AS v", true},
			{"WITH [1,2] AS l RETURN NOT (l)[0] = 1 AS v", false},
			{"WITH {a:true} AS m RETURN NOT (m)['a'] AS v", false},
			{"WITH {} AS m RETURN NOT (m)['missing'] AS v", nil},
			{"RETURN 'a' CONTAINS (['a'])[0] AS v", true},
			{"RETURN 'ab' STARTS WITH (['a'])[0] AS v", true},
			{"RETURN 'ab' ENDS WITH (['b'])[0] AS v", true},
		} {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE (:Lex {list:[1,2], k:'x'})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), sample.query, nil)
			require.NoError(t, err, sample.query)
			require.Equal(t, [][]interface{}{{sample.want}}, result.Rows, sample.query)
		}
	})
}
