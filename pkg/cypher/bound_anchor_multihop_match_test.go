package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fixes #302: compound traversal must use an anchor bound by an earlier MATCH.
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
