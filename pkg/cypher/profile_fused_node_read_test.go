package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileFusedNodeReadReportsExecutedReduction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := profileReaderExecutor(t)
		_, indexErr := exec.Execute(t.Context(), "CREATE INDEX profile_name FOR (n:Person) ON (n.name)", nil)
		require.NoError(t, indexErr)
		for _, tc := range []struct {
			query, kind string
			rows        int
			params      map[string]interface{}
		}{
			{"MATCH (n:Person) RETURN n.name AS name LIMIT 2", "NodeProjection", 2, nil},
			{"MATCH (n:Person) WHERE n.email IN $emails OR n.name IN $names RETURN n.name AS name", "NodeProjection", 3, map[string]interface{}{"emails": []interface{}{"dup@x"}, "names": []interface{}{"third"}}},
			{"MATCH (n:Person) WHERE n.score > 35 RETURN count(n) AS c", "FilteredNodeCount", 1, nil},
		} {
			t.Run(tc.query, func(t *testing.T) {
				plain, err := exec.Execute(t.Context(), tc.query, tc.params)
				require.NoError(t, err)
				result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, tc.params)
				require.NoError(t, err)
				require.Equal(t, plain.Columns, result.Columns)
				require.ElementsMatch(t, plain.Rows, result.Rows)
				require.Len(t, result.Rows, tc.rows)
				plan := executionPlanFromResult(t, result)
				require.Empty(t, plan.Unobserved)
				require.True(t, plan.TotalDBHitsObserved)
				require.Positive(t, plan.TotalDBHits)
				op := planOperatorOfType(plan.Root, tc.kind)
				require.NotNil(t, op)
				require.True(t, op.RowsObserved)
				require.EqualValues(t, tc.rows, op.ActualRows)
				if tc.kind == "FilteredNodeCount" {
					require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
					scan := planOperatorOfType(plan.Root, profileKindNodeByLabelScan)
					require.NotNil(t, scan)
					require.EqualValues(t, 5, scan.ActualRows)
					require.EqualValues(t, 5, scan.DBHits)
				}
			})
		}
	})
}

func BenchmarkFusedNodeReadOrdinaryExecution(b *testing.B) {
	exec := perRowDocGraph(b)
	for _, tc := range []struct {
		name, query string
		count       int64
	}{
		{"filtered-count", "MATCH (n:Doc) WHERE n.id <> '' RETURN count(n)", 150},
		{"bounded-projection", "MATCH (n:Doc) RETURN n.id LIMIT 1", -1},
	} {
		b.Run(tc.name, func(b *testing.B) { runPerRowBench(b, exec, tc.query, nil, tc.count) })
	}
}
