package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTraversalWhere_NotRelationshipPattern tests MATCH (n)-[r:TYPE]->(ph) WHERE n.prop = $ AND NOT (n)-[:OTHER]->()
// so that relationship-pattern WHERE works in the traversal path (e.g. head-only OrderStatus).
func TestTraversalWhere_NotRelationshipPattern(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// OrderStatus with no SUPERSEDED_BY
	o := &storage.Node{
		ID:     "ord-1",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"userId":  "u1",
			"orderId": "ord-1",
		},
	}
	_, err := store.CreateNode(o)
	require.NoError(t, err)

	ph := &storage.Node{
		ID:         "ph-1",
		Labels:     []string{"Pharmacy"},
		Properties: map[string]interface{}{"id": "ph-1"},
	}
	_, err = store.CreateNode(ph)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "e1", Type: "FILLED_AT", StartNode: o.ID, EndNode: ph.ID, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	// Traversal query with WHERE: n.userId = 'u1' AND NOT (n)-[:SUPERSEDED_BY]->()
	query := `MATCH (n:OrderStatus)-[:FILLED_AT]->(ph:Pharmacy) WHERE n.userId = 'u1' AND NOT (n)-[:SUPERSEDED_BY]->() RETURN n.orderId AS orderId, ph.id AS pharmacyId`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "WHERE NOT (n)-[:SUPERSEDED_BY]->() should pass for head node")
	require.Equal(t, "ord-1", rowStringAny(result.Rows[0][0]))
	require.Equal(t, "ph-1", rowStringAny(result.Rows[0][1]))
}

// TestTraversalWhere_NotRelationshipPattern_FiltersOut tests that when the node HAS the relationship,
// NOT (n)-[:X]->() filters the row out.
func TestTraversalWhere_NotRelationshipPattern_FiltersOut(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	o := &storage.Node{
		ID:         "ord-2",
		Labels:     []string{"OrderStatus"},
		Properties: map[string]interface{}{"userId": "u1", "orderId": "ord-2"},
	}
	_, err := store.CreateNode(o)
	require.NoError(t, err)

	ph := &storage.Node{ID: "ph-2", Labels: []string{"Pharmacy"}, Properties: map[string]interface{}{"id": "ph-2"}}
	_, err = store.CreateNode(ph)
	require.NoError(t, err)

	// This order HAS SUPERSEDED_BY (superseded by another node)
	sup := &storage.Node{ID: "ord-2-new", Labels: []string{"OrderStatus"}, Properties: map[string]interface{}{}}
	_, err = store.CreateNode(sup)
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "FILLED_AT", StartNode: o.ID, EndNode: ph.ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", Type: "SUPERSEDED_BY", StartNode: o.ID, EndNode: sup.ID, Properties: map[string]interface{}{}}))

	query := `MATCH (n:OrderStatus)-[:FILLED_AT]->(ph:Pharmacy) WHERE n.userId = 'u1' AND NOT (n)-[:SUPERSEDED_BY]->() RETURN n.orderId AS orderId, ph.id AS pharmacyId`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 0, "WHERE NOT (n)-[:SUPERSEDED_BY]->() should filter out superseded nodes")
}

func TestTraversalStartPropertyScanPushdown_Equality(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)

	// Two starts share same label; only one matches WHERE equality.
	_, err := store.CreateNode(&storage.Node{
		ID:     "o-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"originalText": "Get it delivered",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "o-2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"originalText": "Different",
		},
	})
	require.NoError(t, err)

	nodes, used, err := exec.tryCollectNodesFromStartPropertyScan(
		context.Background(),
		nodePatternInfo{variable: "node", labels: []string{"OriginalText"}},
		"node.originalText = 'Get it delivered'",
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("o-1"), nodes[0].ID)
}

func TestTraversalStartPropertyScanPushdown_IsNotNull(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:     "o-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"sourceId": "s1",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:         "o-2",
		Labels:     []string{"OriginalText"},
		Properties: map[string]interface{}{},
	})
	require.NoError(t, err)

	nodes, used, err := exec.tryCollectNodesFromStartPropertyScan(
		context.Background(),
		nodePatternInfo{variable: "node", labels: []string{"OriginalText"}},
		"node.sourceId IS NOT NULL",
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("o-1"), nodes[0].ID)
}
