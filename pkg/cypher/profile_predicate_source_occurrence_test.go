package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProfilePredicateSubqueriesFollowWrittenExpressions is the public
// regression matrix for a WHERE predicate's scalar subqueries. Two equal bodies
// written at two positions of one predicate are two operators, each reporting
// the executions and the rows of its own written occurrence; an operand written
// twice stays one operand, which the canonical logical planner eliminates; and
// the rows the statement returns are unchanged. A MATCH clause's WHERE and a
// WITH clause's WHERE are the two predicate positions the pipeline evaluates.
// Every case runs cold and warm, with the compiled row programs both disabled
// and enabled, because the text reader and the compiled reader must report the
// same occurrences.
func TestProfilePredicateSubqueriesFollowWrittenExpressions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name string
			// seeds is how many (:Seed) nodes the statement matches: every
			// candidate row evaluates the predicate once.
			seeds      int
			query      string
			want       [][]interface{}
			unions     int
			executions int64
		}{
			{
				name:       "MATCH WHERE over one row",
				seeds:      1,
				query:      "MATCH (n:Seed) WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } > 1 RETURN count(*) AS n",
				want:       [][]interface{}{{int64(1)}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "MATCH WHERE over many rows",
				seeds:      3,
				query:      "MATCH (n:Seed) WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } > 1 RETURN count(*) AS n",
				want:       [][]interface{}{{int64(3)}},
				unions:     2,
				executions: 3,
			},
			{
				name:       "WITH WHERE over many rows",
				seeds:      3,
				query:      "MATCH (n:Seed) WITH n WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } > 1 RETURN count(*) AS n",
				want:       [][]interface{}{{int64(3)}},
				unions:     2,
				executions: 3,
			},
			{
				name:       "MATCH WHERE with one written body twice",
				seeds:      3,
				query:      "MATCH (n:Seed) WHERE COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 RETURN count(*) AS n",
				want:       [][]interface{}{{int64(3)}},
				unions:     1,
				executions: 3,
			},
		} {
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/programs=%t", tc.name, programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					seeds := make([]string, tc.seeds)
					for index := range seeds {
						seeds[index] = "CREATE (:Seed)"
					}
					exec, ctx := newProfileRuntimeTestExecutor(t, seeds...)
					for _, run := range []string{"cold", "warm"} {
						result, err := exec.Execute(ctx, "PROFILE "+tc.query, nil)
						require.NoError(t, err, run)
						require.Equal(t, tc.want, result.Rows, "%s: %s", run, tc.query)
						plan := executionPlanFromResult(t, result)
						unions := profileScalarUnionOperators(plan)
						require.Len(t, unions, tc.unions, "%s: %s", run, formatPlan(plan))
						for _, union := range unions {
							require.True(t, union.RowsObserved, "%s: %s", run, formatPlan(plan))
							require.Equal(t, tc.executions, union.Arguments["executions"], "%s: %s", run, formatPlan(plan))
							require.Equal(t, 2*tc.executions, union.ActualRows, "%s: %s", run, formatPlan(plan))
						}
					}
				})
			}
		}
	})
}
