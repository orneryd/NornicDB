package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fixes #303: correlated EXISTS predicates must apply their inner WHERE clause.
func TestCorrelatedExistsPredicatesApplyInnerFilter(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "correlated_exists")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fn1 := &storage.Node{ID: "fn-1", Labels: []string{"Function"}, Properties: map[string]interface{}{"uid": "fn1"}}
	fn2 := &storage.Node{ID: "fn-2", Labels: []string{"Function"}, Properties: map[string]interface{}{"uid": "fn2"}}
	w1 := &storage.Node{ID: "w-1", Labels: []string{"Workload"}, Properties: map[string]interface{}{"id": "w1"}}
	w2 := &storage.Node{ID: "w-2", Labels: []string{"Workload"}, Properties: map[string]interface{}{"id": "w2"}}
	w3 := &storage.Node{ID: "w-3", Labels: []string{"Workload"}, Properties: map[string]interface{}{"id": "w3"}}
	for _, node := range []*storage.Node{fn1, fn2, w1, w2, w3} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "fn1-w1", Type: "RUNS_IN", StartNode: fn1.ID, EndNode: w1.ID},
		{ID: "fn2-w2", Type: "RUNS_IN", StartNode: fn2.ID, EndNode: w2.ID},
		{ID: "fn2-w3", Type: "RUNS_IN", StartNode: fn2.ID, EndNode: w3.ID},
	} {
		require.NoError(t, store.CreateEdge(edge))
	}

	require.False(t, exec.pathSubqueryMatches(ctx, PathContext{nodes: map[string]*storage.Node{"fn": fn1, "w": w1}}, `
		MATCH (fn)-[:RUNS_IN]->(o:Workload)
		WHERE o.id <> w.id
	`))
	require.True(t, exec.pathSubqueryMatches(ctx, PathContext{nodes: map[string]*storage.Node{"fn": fn2, "w": w2}}, `
		MATCH (fn)-[:RUNS_IN]->(o:Workload)
		WHERE o.id <> w.id
	`))

	assertUIDs := func(t *testing.T, query string, expected ...string) {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"fn.uid"}, result.Columns)
		require.Len(t, result.Rows, len(expected))
		for index, uid := range expected {
			require.Equal(t, uid, result.Rows[index][0])
		}
	}

	t.Run("correlated NOT EXISTS keeps functions without another workload", func(t *testing.T) {
		assertUIDs(t, `
			MATCH (fn:Function)-[:RUNS_IN]->(w:Workload)
			WHERE NOT EXISTS {
				MATCH (fn)-[:RUNS_IN]->(o:Workload)
				WHERE o.id <> w.id
			}
			RETURN DISTINCT fn.uid
			ORDER BY fn.uid
		`, "fn1")
	})

	t.Run("correlated EXISTS keeps functions with another workload", func(t *testing.T) {
		assertUIDs(t, `
			MATCH (fn:Function)-[:RUNS_IN]->(w:Workload)
			WHERE EXISTS {
				MATCH (fn)-[:RUNS_IN]->(o:Workload)
				WHERE o.id <> w.id
			}
			RETURN DISTINCT fn.uid
			ORDER BY fn.uid
		`, "fn2")
	})

	t.Run("uncorrelated NOT EXISTS remains false when a matching workload exists", func(t *testing.T) {
		assertUIDs(t, `
			MATCH (fn:Function)
			WHERE NOT EXISTS { MATCH (:Workload {id: 'w1'}) }
			RETURN fn.uid
			ORDER BY fn.uid
		`)
	})
}
