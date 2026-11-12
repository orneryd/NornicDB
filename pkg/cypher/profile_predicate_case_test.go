package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileCasePredicateKeepsLazySourceOccurrences(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, programs := range []bool{false, true} {
			t.Run(fmt.Sprintf("programs=%t", programs), func(t *testing.T) {
				previous := rowValueProgramsDisabled.Swap(!programs)
				t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
				exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed {k:0}), (:Seed {k:1}), (:Seed {k:2})")
				const query = "PROFILE MATCH (n:Seed) WHERE CASE WHEN n.k = 0 THEN COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 ELSE COUNT { RETURN 1 UNION ALL RETURN 1 } > 1 END RETURN count(*) AS n"
				for _, run := range []string{"cold", "warm"} {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, run)
					require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows, run)
					plan := executionPlanFromResult(t, result)
					unions := profileScalarUnionOperators(plan)
					require.Len(t, unions, 2, "%s: %s", run, formatPlan(plan))
					var executions []int64
					for _, union := range unions {
						count, ok := union.Arguments["executions"].(int64)
						require.True(t, ok)
						executions = append(executions, count)
						require.True(t, union.RowsObserved)
						require.Equal(t, 2*count, union.ActualRows, run)
					}
					require.ElementsMatch(t, []int64{1, 2}, executions, "CASE evaluates only the selected branch on each row")
				}
			})
		}
	})
}
