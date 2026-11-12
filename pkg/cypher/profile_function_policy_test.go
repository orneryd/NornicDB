package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProfileBuiltinScalarReadsRemainObservable(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		for _, tc := range []struct {
			query string
			want  interface{}
		}{
			{"RETURN size([1,2]) AS n", int64(2)},
			{"RETURN coalesce(null,7) AS n", int64(7)},
			{"RETURN toUpper('abc') AS n", "ABC"},
		} {
			result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
			plan := executionPlanFromResult(t, result)
			require.Empty(t, plan.Unobserved, tc.query)
			require.True(t, plan.TotalDBHitsObserved, tc.query)
			require.Zero(t, plan.TotalDBHits)
		}
	})
}
