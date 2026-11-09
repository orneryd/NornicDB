package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These public controls preserve supported execution while the semantic reader
// changes identity representation. The pre-change public samples already passed;
// TestSemanticReferenceIdentityBoundaries records the separate validator defect.
func TestQuotedVariableSemanticReferencesThroughExecute(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, variable := range []string{"m", "`n.dot`", "`n``.dot`"} {
			t.Run(variable, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "quoted_semantic_reference")
				for _, tc := range []struct {
					name, query string
					columns     []string
					rows        [][]interface{}
				}{
					{"distinct property", "UNWIND [3,1,2,1] AS v WITH {x:v} AS %[1]s RETURN DISTINCT %[1]s.x AS x ORDER BY %[1]s.x", []string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
					{"grouped property", "UNWIND [3,1,2,1] AS v WITH {x:v} AS %[1]s RETURN %[1]s.x AS x, count(*) AS c ORDER BY %[1]s.x", []string{"x", "c"}, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}, {int64(3), int64(1)}}},
					{"distinct entity", "UNWIND [2,1] AS v WITH {x:v} AS %[1]s RETURN DISTINCT %[1]s AS r ORDER BY %[1]s.x", []string{"r"}, [][]interface{}{{map[string]interface{}{"x": int64(1)}}, {map[string]interface{}{"x": int64(2)}}}},
					{"Cypher 25 correlated constant", "CYPHER 25 WITH {x:7} AS %[1]s RETURN COLLECT { UNWIND [1,2] AS y RETURN count(*) + %[1]s.x AS total } AS totals", []string{"totals"}, [][]interface{}{{[]interface{}{int64(9)}}}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						query := fmt.Sprintf(tc.query, variable)
						result, err := exec.Execute(t.Context(), query, nil)
						require.NoError(t, err, query)
						require.Equal(t, tc.columns, result.Columns)
						require.Equal(t, tc.rows, result.Rows)
					})
				}
				query := fmt.Sprintf("WITH {x:7} AS %s RETURN COLLECT { UNWIND [1,2] AS y RETURN count(*) + %s.x AS total } AS totals", variable, variable)
				_, err := exec.Execute(t.Context(), query, nil)
				require.Error(t, err, "Cypher 5 retains its explicit grouping requirement")
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				query = fmt.Sprintf("WITH {x:1} AS %s RETURN DISTINCT missing.x AS x ORDER BY missing.x", variable)
				_, err = exec.Execute(t.Context(), query, nil)
				require.Error(t, err)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
			})
		}
	})
}

func TestOrderKeywordVariablesThroughExecute(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, name := range []string{"skip", "limit"} {
			t.Run(name, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "order_keyword_variable")
				query := fmt.Sprintf("UNWIND [3,1,2] AS %s RETURN %s AS value ORDER BY %s LIMIT 2", name, name, name)
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err, query)
				require.Equal(t, []string{"value"}, result.Columns)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
				for _, query := range []string{
					"WITH 1 AS other RETURN other AS value ORDER BY " + name,
					fmt.Sprintf("WITH 1 AS %s RETURN DISTINCT 1 AS value ORDER BY %s", name, name),
				} {
					_, err := exec.Execute(t.Context(), query, nil)
					require.Error(t, err, query)
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				}
			})
		}
	})
}
