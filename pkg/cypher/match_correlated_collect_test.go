package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fixes #297: collect must retain rows after correlated relationship matches.
func TestCollectAfterCorrelatedRelationshipMatches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "correlated_collect")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	function := &storage.Node{
		ID:         "fn-1",
		Labels:     []string{"Function"},
		Properties: map[string]interface{}{"id": "fn-1"},
	}
	cloudAction := &storage.Node{
		ID:         "action-1",
		Labels:     []string{"CloudAction"},
		Properties: map[string]interface{}{"id": "action-1"},
	}
	secondCloudAction := &storage.Node{
		ID:         "action-2",
		Labels:     []string{"CloudAction"},
		Properties: map[string]interface{}{"id": "action-2"},
	}
	workload := &storage.Node{
		ID:         "workload-1",
		Labels:     []string{"Workload"},
		Properties: map[string]interface{}{"id": "workload-1"},
	}
	for _, node := range []*storage.Node{function, cloudAction, secondCloudAction, workload} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "fn-invokes-action",
		Type:      "INVOKES_CLOUD_ACTION",
		StartNode: function.ID,
		EndNode:   cloudAction.ID,
	}))
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "fn-invokes-second-action",
		Type:      "INVOKES_CLOUD_ACTION",
		StartNode: function.ID,
		EndNode:   secondCloudAction.ID,
	}))
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "fn-runs-in-workload",
		Type:      "RUNS_IN",
		StartNode: function.ID,
		EndNode:   workload.ID,
	}))

	t.Run("control: collect after one relationship match", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (fn:Function)-[:RUNS_IN]->(w:Workload)
			WITH fn, collect(DISTINCT w) AS ws
			RETURN size(ws) AS n
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"n"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), result.Rows[0][0])
	})

	t.Run("collect after two correlated relationship matches", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (fn:Function)-[:INVOKES_CLOUD_ACTION]->(a:CloudAction)
			MATCH (fn)-[:RUNS_IN]->(w:Workload)
			WITH fn, collect(DISTINCT w) AS ws
			RETURN size(ws) AS n
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"n"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), result.Rows[0][0])
	})
}
