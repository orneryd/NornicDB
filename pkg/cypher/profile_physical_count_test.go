package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A fused count emits one scalar row; the matched population is the value in
// that row, not the operator's cardinality or a fabricated entity-read count.
func TestProfilePhysicalCountReportsItsOwnRowsAndReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := countStoreGraph(t)
		for _, tc := range []struct {
			query string
			kind  string
			count int64
		}{
			{"MATCH (n:User) RETURN count(n) AS c", profileKindNodeCountFromCountStore, 3},
			{"MATCH (n:Missing) RETURN count(n) AS c", profileKindNodeCountFromCountStore, 0},
			{"MATCH ()-[r:Friend]->() RETURN count(r) AS c", profileKindRelationshipCountFromCountStore, 7},
			{"MATCH ()-[r:Friend|Likes]->() RETURN count(r) AS c", profileKindRelationshipCountFromCountStore, 9},
			{"MATCH (:User)-[r:Friend]->() RETURN count(r) AS c", profileKindRelationshipCountFromCountStore, 7},
		} {
			t.Run(tc.query, func(t *testing.T) {
				result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.count}}, result.Rows)
				plan := executionPlanFromResult(t, result)
				require.Empty(t, plan.Unobserved, "a proven count-store reduction is fully observed")
				require.EqualValues(t, 1, plan.Root.ActualRows)
				require.EqualValues(t, 1, plan.TotalDBHits)
				require.True(t, plan.TotalDBHitsObserved)
				counter := planOperatorOfType(plan.Root, tc.kind)
				require.NotNil(t, counter)
				require.True(t, counter.RowsObserved)
				require.True(t, counter.DBHitsObserved)
				require.EqualValues(t, 1, counter.ActualRows)
				require.EqualValues(t, 1, counter.DBHits)
				require.Nil(t, planOperatorOfType(plan.Root, "NodeByLabelScan"))
				require.Nil(t, planOperatorOfType(plan.Root, "Expand"))
			})
		}
	})
}

// BenchmarkPhysicalCountOrdinaryExecution reuses the public 150-document,
// 450-relationship corpus and uncached query loop. PROFILE state must not add
// allocations or entity reads to ordinary count-store execution.
func BenchmarkPhysicalCountOrdinaryExecution(b *testing.B) {
	exec := perRowDocGraph(b)
	for _, tc := range []struct {
		name, query string
		count       int64
	}{
		{"label", "MATCH (n:Doc) RETURN count(n)", 150},
		{"type", "MATCH ()-[r:REL]->() RETURN count(r)", 450},
	} {
		b.Run(tc.name, func(b *testing.B) { runPerRowBench(b, exec, tc.query, nil, tc.count) })
	}
}

func TestProfilePhysicalCountKeepsVisibilityAndTraversalProvenance(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)
		internalCountFixture(t, ctx, exec)
		for _, tc := range []struct {
			query  string
			count  int64
			folded bool
		}{
			{"MATCH (n:_internal) RETURN count(n) AS c", 0, true},
			{"MATCH (n:_absent) RETURN count(n) AS c", 0, true},
			{"MATCH (n:_internal)-[r:NOR177_LINKED]->() RETURN count(r) AS c", 0, true},
			{"MATCH ()-[r:NOR177_LINKED]->(n:_internal) RETURN count(r) AS c", 0, true},
			{"MATCH (n:Nor177Count) RETURN count(n) AS c", 3, false},
			{"MATCH ()-[r:NOR177_LINKED]-() RETURN count(r) AS c", 2, false},
		} {
			t.Run(tc.query, func(t *testing.T) {
				result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.count}}, result.Rows)
				plan := executionPlanFromResult(t, result)
				require.EqualValues(t, 1, plan.Root.ActualRows)
				require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeCountFromCountStore))
				require.Nil(t, planOperatorOfType(plan.Root, profileKindRelationshipCountFromCountStore))
				if tc.folded {
					require.Empty(t, plan.Unobserved)
					require.Zero(t, plan.TotalDBHits)
					fold := planOperatorOfType(plan.Root, "CountVisibilityFold")
					require.NotNil(t, fold)
					require.True(t, fold.RowsObserved)
					require.True(t, fold.DBHitsObserved)
					require.EqualValues(t, 1, fold.ActualRows)
					require.Zero(t, fold.DBHits)
				} else {
					require.Empty(t, plan.Unobserved, "scan-backed reductions supply their own observation, never counter provenance")
					require.True(t, plan.TotalDBHitsObserved)
					require.Positive(t, plan.TotalDBHits)
					require.Nil(t, planOperatorOfType(plan.Root, "CountVisibilityFold"))
				}
			})
		}
	})
}
