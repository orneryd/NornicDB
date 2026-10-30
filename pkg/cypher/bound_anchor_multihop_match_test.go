package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regression: initially reported in #302.
func TestBoundAnchorCompoundTraversalReturnsEndpoint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "bound_anchor_multihop")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	workload := &storage.Node{ID: "w1", Labels: []string{"Workload"}, Properties: map[string]interface{}{"id": "w1"}}
	instance := &storage.Node{ID: "i1", Labels: []string{"WorkloadInstance"}, Properties: map[string]interface{}{"id": "i1"}}
	resource := &storage.Node{ID: "p1", Labels: []string{"CloudResource"}, Properties: map[string]interface{}{"id": "p1"}}
	for _, node := range []*storage.Node{workload, instance, resource} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "instance-of", Type: "INSTANCE_OF", StartNode: instance.ID, EndNode: workload.ID}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "instance-uses-resource", Type: "USES", StartNode: instance.ID, EndNode: resource.ID}))

	assertEndpoint := func(t *testing.T, query string) {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"p.id"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "p1", result.Rows[0][0])
	}

	t.Run("control: fresh anchor compound traversal", func(t *testing.T) {
		assertEndpoint(t, `MATCH (w:Workload {id: 'w1'})<-[:INSTANCE_OF]-(i:WorkloadInstance)-[:USES]->(p:CloudResource) RETURN p.id`)
	})

	t.Run("control: bound anchor split traversal", func(t *testing.T) {
		assertEndpoint(t, `MATCH (w:Workload {id: 'w1'}) MATCH (w)<-[:INSTANCE_OF]-(i:WorkloadInstance) MATCH (i)-[:USES]->(p:CloudResource) RETURN p.id`)
	})

	t.Run("bound anchor compound traversal", func(t *testing.T) {
		assertEndpoint(t, `MATCH (w:Workload {id: 'w1'}) MATCH (w)<-[:INSTANCE_OF]-(i:WorkloadInstance)-[:USES]->(p:CloudResource) RETURN p.id`)
	})
}

// Regression: initially reported in #372.
func TestBoundAnchorMultiHopTraversalBindsIntermediateNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "bound_anchor_intermediate")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	repository := &storage.Node{ID: "repo", Labels: []string{"Repository"}, Properties: map[string]interface{}{"id": "ag-repo"}}
	file := &storage.Node{ID: "file", Labels: []string{"File"}, Properties: map[string]interface{}{"id": "ag-f", "relative_path": "x.go"}}
	otherFile := &storage.Node{ID: "other-file", Labels: []string{"File"}, Properties: map[string]interface{}{"id": "other", "relative_path": "other.go"}}
	function := &storage.Node{ID: "function", Labels: []string{"Function"}, Properties: map[string]interface{}{"id": "ag-e"}}
	for _, node := range []*storage.Node{repository, file, otherFile, function} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "repo-file", Type: "REPO_CONTAINS", StartNode: repository.ID, EndNode: file.ID}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "file-function", Type: "CONTAINS", StartNode: file.ID, EndNode: function.ID}))

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "anchor at right endpoint",
			query: `MATCH (e:Function {id: 'ag-e'}) MATCH (r:Repository)-[:REPO_CONTAINS]->(f:File)-[:CONTAINS]->(e) RETURN f.relative_path AS fp, r.id AS rid`,
		},
		{
			name:  "anchor at left endpoint",
			query: `MATCH (e:Function {id: 'ag-e'}) MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(r:Repository) RETURN f.relative_path AS fp, r.id AS rid`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"x.go", "ag-repo"}}, result.Rows)
		})
	}

	t.Run("conflicting intermediate binding rejects path", func(t *testing.T) {
		result, err := exec.Execute(ctx, `MATCH (f:File {id: 'other'}) MATCH (e:Function {id: 'ag-e'}) MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(r:Repository) RETURN f.relative_path AS fp, r.id AS rid`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
	})
}
