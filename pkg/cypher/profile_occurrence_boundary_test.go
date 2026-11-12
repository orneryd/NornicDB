package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileOccurrencesAcrossEvaluationBoundaries(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query string
			rows        [][]interface{}
		}{
			{"case branches", "UNWIND [true,false] AS flag RETURN CASE WHEN flag THEN COUNT { RETURN 1 UNION ALL RETURN 1 } ELSE COUNT { RETURN 1 UNION ALL RETURN 1 } END AS c", [][]interface{}{{int64(2)}, {int64(2)}}},
			{"simple Unicode case branches", "UNWIND ['火','水'] AS flag RETURN CASE flag WHEN '火' THEN COUNT { RETURN 1 UNION ALL RETURN 1 } ELSE COUNT { RETURN 1 UNION ALL RETURN 1 } END AS c", [][]interface{}{{int64(2)}, {int64(2)}}},
			{"aggregate grouping keys", "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, COUNT { RETURN 1 UNION ALL RETURN 1 } AS b, count(*) AS n", [][]interface{}{{int64(2), int64(2), int64(1)}}},
			{"map fields", "RETURN {a: COUNT { RETURN 1 UNION ALL RETURN 1 }, b: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value", [][]interface{}{{map[string]interface{}{"a": int64(2), "b": int64(2)}}}},
			{"range arguments", "RETURN range(COUNT { RETURN 1 UNION ALL RETURN 1 }, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value", [][]interface{}{{[]interface{}{int64(2)}}}},
			{"unwind expression", "UNWIND [COUNT { RETURN 1 UNION ALL RETURN 1 }, COUNT { RETURN 1 UNION ALL RETURN 1 }] AS value RETURN value", [][]interface{}{{int64(2)}, {int64(2)}}},
			{"comprehension predicate and projection", "RETURN [x IN [0] WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } > 0 | COUNT { RETURN 1 UNION ALL RETURN 1 }] AS value", [][]interface{}{{[]interface{}{int64(2)}}}},
			{"Unicode comprehension", "RETURN [火 IN [0] WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } > 0 | COUNT { RETURN 1 UNION ALL RETURN 1 }] AS value", [][]interface{}{{[]interface{}{int64(2)}}}},
			{"logical AND", "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } > 1 AS value", [][]interface{}{{true}}},
			{"logical OR", "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } = 0 OR COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AS value", [][]interface{}{{true}}},
			{"quantifier predicate", "RETURN all(x IN [0] WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } > 1) AS value", [][]interface{}{{true}}},
			{"map projection fields", "WITH {} AS m RETURN m { a: COUNT { RETURN 1 UNION ALL RETURN 1 }, b: COUNT { RETURN 1 UNION ALL RETURN 1 } } AS value", [][]interface{}{{map[string]interface{}{"a": int64(2), "b": int64(2)}}}},
			{"math function arguments", "RETURN round(COUNT { RETURN 1 UNION ALL RETURN 1 }, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value", [][]interface{}{{float64(2)}}},
		} {
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/programs=%t", tc.name, programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
					result, err := exec.Execute(ctx, "PROFILE "+tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, tc.rows, result.Rows)
					plan := executionPlanFromResult(t, result)
					unions := profileScalarUnionOperators(plan)
					require.Len(t, unions, 2, "%s", formatPlan(plan))
					for _, union := range unions {
						require.True(t, union.RowsObserved)
						require.Equal(t, int64(2), union.ActualRows)
						require.Equal(t, int64(1), union.Arguments["executions"])
					}
				})
			}
		}
	})
}
