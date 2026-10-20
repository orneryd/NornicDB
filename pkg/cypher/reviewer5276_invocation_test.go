package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestReviewer5276InvocationFramesStrict(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	cases := []struct {
		name, body string
		bad, good  interface{}
	}{
		{"property_import", "CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v", int64(1), map[string]interface{}{"a": int64(7)}},
		{"property_alias", "WITH x AS z RETURN z.a AS v", true, map[string]interface{}{"a": int64(7)}},
		{"operator_import", "CALL (x) { RETURN x - 1 AS v } RETURN v", "bad", int64(7)},
		{"function_import", "CALL (x) { RETURN abs(x) AS v } RETURN v", true, int64(7)},
		{"property_comprehension", "RETURN [z IN [x] | z.a] AS v", int64(1), map[string]interface{}{"a": int64(7)}},
		{"function_comprehension", "RETURN [z IN [x] | abs(z)] AS v", true, int64(7)},
		{"operator_with", "WITH x AS z RETURN z - 1 AS v", "bad", int64(7)},
		{"property_expression_query", "RETURN COLLECT { WITH x AS z RETURN z.a AS v } AS v", int64(1), map[string]interface{}{"a": int64(7)}},
	}
	for _, norm := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(norm)
		for _, route := range []string{"native", "use"} {
			for _, tx := range []bool{false, true} {
				for _, empty := range []bool{false, true} {
					for _, c := range cases {
						t.Run(fmt.Sprintf("%s/norm_%v/tx_%v/empty_%v/%s", route, norm, tx, empty, c.name), func(t *testing.T) {
							e := newFabricCallRowsExecutor(t)
							ctx := context.Background()
							if route == "use" {
								e = fabricCallRowsCompositeExecutor(t, e)
							}

							q := "WITH $p AS x "
							if empty {
								q += "UNWIND $empty AS stop "
							}
							q += c.body
							if route == "use" {
								q = "USE comp.one " + q
							}
							// Reuse the same executor and text across valid and invalid invocations.
							for i, v := range []interface{}{c.good, c.bad, c.good, c.bad} {
								invoke := func() (*ExecuteResult, error) {
									if tx {
										_, err := e.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer e.Execute(ctx, "ROLLBACK", nil)
									}
									return e.Execute(ctx, q, map[string]interface{}{"p": v, "empty": []interface{}{}})
								}
								out, err := invoke()
								if i%2 == 0 {
									require.NoError(t, err, q)
									if empty {
										require.Empty(t, out.Rows)
									}
								} else {
									require.Error(t, err, "query=%s result=%v", q, out)
									code, _ := nornicerrors.Neo4jStatus(err)
									require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "query=%s err=%v result=%v", q, err, out)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestReviewer5276NoDemandProjectionPopulationStrict(t *testing.T) {
	cases := []string{
		"UNWIND $items AS q RETURN q AS v",
		"UNWIND $items AS q WITH q AS z RETURN z AS v",
		"UNWIND $items AS q WITH * RETURN q AS v ORDER BY v LIMIT 2",
		"WITH $one AS x UNWIND $items AS q RETURN q + x AS v",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			input := newSemanticStatementInput(q, nil)
			require.Nil(t, input.syntax())
			clauses, ok := input.pipeline(true)
			require.True(t, ok)
			for _, c := range clauses {
				require.True(t, c.noLocalScopes)
				require.Nil(t, c.syntax)
			}
			out, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(context.Background(), q, map[string]interface{}{"items": []interface{}{int64(1), int64(2), int64(3)}, "one": int64(1)})
			require.NoError(t, err)
			require.Equal(t, []string{"v"}, out.Columns)
			require.NotEmpty(t, out.Rows)
		})
	}
}
