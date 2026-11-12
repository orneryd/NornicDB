package cypher

// PROFILE states the reads a paged db.retrieve actually performs. The complete
// continuation build is synchronous in the statement that asked for it, so its
// node scan and its schema constraint catalog acquisition belong to that
// statement; a later qid pull reads only the page it hydrates and neither
// replays the build's records nor inherits its observation context.

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// continuationProfileExecutor is the paged db.retrieve fixture: four documents
// the catalog scan reads, and one search service over the same engine. It also
// answers the constraint catalog population the build's projection reads.
func continuationProfileExecutor(t *testing.T) (*StorageExecutor, int64) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for index := 0; index < 4; index++ {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("doc-%d", index)),
			Properties: map[string]any{"content": "continuation page document"},
		})
		require.NoError(t, err)
	}
	options := search.DefaultServiceOptions("test")
	service := search.NewService(store, 3, &options)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	exec := NewStorageExecutor(store)
	exec.SetSearchService(service)
	return exec, int64(len(store.GetSchema().GetAllConstraints()))
}

// continuationPlanOperator returns the one operator of operatorType the plan
// tree holds: a read this statement made is reported once, never twice.
func continuationPlanOperator(t *testing.T, plan *ExecutionPlan, operatorType string) *PlanOperator {
	t.Helper()
	operators := profilePlanOperators(plan.Root, operatorType)
	require.Len(t, operators, 1, operatorType)
	return operators[0]
}

// requireContinuationHasNoOpaqueReads certifies the fully observed built-in
// continuation request. Custom installed callbacks have separate opaque controls.
func requireContinuationHasNoOpaqueReads(t *testing.T, plan *ExecutionPlan) {
	t.Helper()
	require.Empty(t, plan.Unobserved)
	require.True(t, plan.TotalDBHitsObserved)
}

// continuationPage runs one paged db.retrieve under PROFILE and returns the
// statement's result and plan.
func continuationPage(t *testing.T, exec *StorageExecutor, request map[string]any) (*ExecuteResult, *ExecutionPlan) {
	t.Helper()
	result, err := exec.Execute(t.Context(), "PROFILE CALL db.retrieve($request) YIELD page RETURN page", map[string]any{"request": request})
	require.NoError(t, err)
	return result, executionPlanFromResult(t, result)
}

func TestProfileContinuationIDPageReportsItsScanAndPageHydration(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, constraints := continuationProfileExecutor(t)

		first, firstPlan := continuationPage(t, exec, map[string]any{"mode": "id", "n": int64(2)})
		scan := continuationPlanOperator(t, firstPlan, profileKindAllNodesScan)
		require.True(t, scan.RowsObserved)
		require.EqualValues(t, 4, scan.ActualRows, "the catalog build scans the whole node population")
		require.EqualValues(t, 4, scan.DBHits, "every scanned node is one read")
		catalog := continuationPlanOperator(t, firstPlan, profileKindSchemaConstraintCatalog)
		require.True(t, catalog.RowsObserved)
		require.EqualValues(t, constraints, catalog.ActualRows, "the constraint catalog the build acquired")
		require.EqualValues(t, 1, catalog.DBHits, "one acquisition")
		hydration := continuationPlanOperator(t, firstPlan, queryprofile.KindNodeBatchByIDSeek)
		require.True(t, hydration.RowsObserved)
		require.EqualValues(t, 2, hydration.ActualRows, "the first page hydrates exactly its window")
		require.EqualValues(t, 1, hydration.DBHits, "one hydration request")
		requireContinuationHasNoOpaqueReads(t, firstPlan)

		require.Len(t, first.Rows, 1, "a paged request answers one row holding the page map")
		page, ok := first.Rows[0][0].(map[string]any)
		require.True(t, ok)
		qid, ok := page["qid"].(string)
		require.True(t, ok)
		require.NotEmpty(t, qid)
		require.Len(t, page["results"], 2)
		hasMore, ok := page["has_more"].(bool)
		require.True(t, ok)
		require.True(t, hasMore, "four documents answer a second page")

		next, nextPlan := continuationPage(t, exec, map[string]any{"qid": qid, "n": int64(2)})
		require.Empty(t, profilePlanOperators(nextPlan.Root, profileKindAllNodesScan),
			"a qid pull does not build the catalog again")
		require.Empty(t, profilePlanOperators(nextPlan.Root, profileKindSchemaConstraintCatalog),
			"the build's catalog acquisition belongs to the statement that made it")
		hydration = continuationPlanOperator(t, nextPlan, queryprofile.KindNodeBatchByIDSeek)
		require.True(t, hydration.RowsObserved)
		require.EqualValues(t, 2, hydration.ActualRows, "the pull hydrates only the page it returns")
		require.EqualValues(t, 1, hydration.DBHits)
		require.Empty(t, profilePlanOperators(nextPlan.Root, profileKindNodeByIdSeek),
			"a page read is one batch request, not a point read per node")
		requireContinuationHasNoOpaqueReads(t, nextPlan)

		require.Len(t, next.Rows, 1)
		nextPage, ok := next.Rows[0][0].(map[string]any)
		require.True(t, ok)
		require.Len(t, nextPage["results"], 2)
		require.Equal(t, false, nextPage["has_more"])
		eligibility := continuationPlanOperator(t, firstPlan, queryprofile.KindTemporalEligibilityRequest)
		require.EqualValues(t, 4, eligibility.ActualRows, "one successful eligibility decision per scanned node")
		require.EqualValues(t, 4, eligibility.DBHits)
		require.Empty(t, profilePlanOperators(nextPlan.Root, queryprofile.KindTemporalEligibilityRequest), "a pull does not repeat catalog eligibility")
		require.EqualValues(t, 10, firstPlan.TotalDBHits, "four entities, four eligibility decisions, one constraint acquisition, one page batch")
		require.EqualValues(t, 1, nextPlan.TotalDBHits, "only the next page batch")
		var ids []storage.NodeID
		for _, p := range []map[string]any{page, nextPage} {
			require.EqualValues(t, 2, p["returned"])
			for _, value := range p["results"].([]interface{}) {
				node, ok := value.(map[string]interface{})["node"].(*storage.Node)
				require.True(t, ok)
				ids = append(ids, node.ID)
			}
		}
		require.ElementsMatch(t, []storage.NodeID{"doc-0", "doc-1", "doc-2", "doc-3"}, ids, "paging preserves the exact full population without duplicates")

	})
}
