package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMatchSetBatch_UsesIndexedLookupWithoutCreatingMissingNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	schema := store.GetSchema()
	require.NotNil(t, schema)
	// The constraint owns the uid index (#875); an index of its own on the
	// same property is what Neo4j rejects (#884).
	require.NoError(t, schema.AddUniqueConstraint("unique_function_uid", "Function", "uid"))

	existingID, err := store.CreateNode(&storage.Node{
		ID:     "fn-1",
		Labels: []string{"Function"},
		Properties: map[string]interface{}{
			"uid":  "content-entity:e_1",
			"name": "before",
		},
	})
	require.NoError(t, err)
	require.NoError(t, schema.BackfillPropertyIndex("Function", "uid", map[storage.NodeID]interface{}{existingID: "content-entity:e_1"}))

	exec := NewStorageExecutor(&noScanMergeLookupEngine{Engine: store})
	_, err = exec.Execute(context.Background(), `
UNWIND $rows AS row
MATCH (n:Function {uid: row.entity_id})
SET n.name = row.name,
    n.semantic_kind = row.semantic_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":     "content-entity:e_1",
				"name":          "after",
				"semantic_kind": "function",
			},
			{
				"entity_id":     "content-entity:e_missing",
				"name":          "missing",
				"semantic_kind": "function",
			},
		},
	})
	require.NoError(t, err)

	node, err := store.GetNode(existingID)
	require.NoError(t, err)
	require.Equal(t, "after", node.Properties["name"])
	require.Equal(t, "function", node.Properties["semantic_kind"])

	nodes, err := store.GetNodesByLabel("Function")
	require.NoError(t, err)
	require.Len(t, nodes, 1, "MATCH/SET hot path must not create nodes for missing matches")
	require.NotEqual(t, "content-entity:e_missing", nodes[0].Properties["uid"])
}

func TestUnwindMatchSetBatch_FallsBackForNonUniqueMatchSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	schema := store.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))

	for _, id := range []storage.NodeID{"fn-1", "fn-2"} {
		nodeID, err := store.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{"Function"},
			Properties: map[string]interface{}{
				"uid":  "content-entity:e_1",
				"name": "before",
			},
		})
		require.NoError(t, err)
		require.NoError(t, schema.PropertyIndexInsert("Function", "uid", nodeID, "content-entity:e_1"))
	}

	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), `
UNWIND $rows AS row
MATCH (n:Function {uid: row.entity_id})
SET n.name = row.name
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id": "content-entity:e_1",
				"name":      "after",
			},
		},
	})
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("Function")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, node := range nodes {
		require.Equal(t, "after", node.Properties["name"])
	}
}
