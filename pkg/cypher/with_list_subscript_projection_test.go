package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fixes #298: a node selected from a collected list must remain a node binding.
func TestWithListSubscriptPreservesNodeProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "list_subscript_projection")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	function := &storage.Node{ID: "fn-1", Labels: []string{"Function"}, Properties: map[string]interface{}{"id": "fn-1"}}
	workload := &storage.Node{ID: "workload-1", Labels: []string{"Workload"}, Properties: map[string]interface{}{"name": "checkout"}}
	_, err := store.CreateNode(function)
	require.NoError(t, err)
	_, err = store.CreateNode(workload)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "fn-runs-in-workload",
		Type:      "RUNS_IN",
		StartNode: function.ID,
		EndNode:   workload.ID,
	}))

	query := `
		MATCH (fn:Function)-[:RUNS_IN]->(w:Workload)
		WITH fn, collect(DISTINCT w) AS ws
		WITH fn, ws[0] AS w2
		RETURN w2.name AS name
	`

	pipelineResult, handled, err := exec.executePipeline(ctx, query)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"name"}, pipelineResult.Columns)
	require.Len(t, pipelineResult.Rows, 1)
	require.Equal(t, "checkout", pipelineResult.Rows[0][0])

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "checkout", result.Rows[0][0])

	nodeResult, err := exec.Execute(ctx, `
		MATCH (fn:Function)-[:RUNS_IN]->(w:Workload)
		WITH fn, collect(DISTINCT w) AS ws
		WITH ws[0] AS w2
		RETURN w2
	`, nil)
	require.NoError(t, err)
	require.Len(t, nodeResult.Rows, 1)
	selectedWorkload, ok := nodeResult.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, workload.ID, selectedWorkload.ID)
}
