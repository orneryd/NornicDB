package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Node writes keep an index current through the public write paths: the
// create files the value, the update moves it, the delete removes it.
func TestMaintainPropertyIndexesOnNodeCreated_Updated_Deleted(t *testing.T) {
	engine := newTestEngine(t)
	sm := engine.GetSchemaForNamespace("tenant")
	require.NoError(t, sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"}))

	node := &Node{
		ID:         NodeID("tenant:n1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice", "age": int64(41)},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.Equal(t, []NodeID{node.ID}, sm.PropertyIndexLookup("Person", "name", "Alice"))

	require.NoError(t, engine.UpdateNode(&Node{ID: node.ID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Alicia", "age": int64(41)}}))
	require.Empty(t, sm.PropertyIndexLookup("Person", "name", "Alice"))
	require.Equal(t, []NodeID{node.ID}, sm.PropertyIndexLookup("Person", "name", "Alicia"))

	// An update that leaves the indexed value alone keeps its entry.
	require.NoError(t, engine.UpdateNode(&Node{ID: node.ID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Alicia", "age": int64(42)}}))
	require.Equal(t, []NodeID{node.ID}, sm.PropertyIndexLookup("Person", "name", "Alicia"))

	require.NoError(t, engine.DeleteNode(node.ID))
	require.Empty(t, sm.PropertyIndexLookup("Person", "name", "Alicia"))
}

func TestMaintainPropertyIndexesOnNodeDeletedWithNode_EarlyReturns(t *testing.T) {
	engine := newTestEngine(t)
	nodeID := NodeID("tenant:n2")

	// No snapshot, and a snapshot without labels.
	engine.maintainPropertyIndexesOnNodeDeletedWithNode(nodeID, nil)
	engine.maintainPropertyIndexesOnNodeDeletedWithNode(nodeID, &Node{ID: nodeID})

	// Labels with no index.
	sm := engine.GetSchemaForNamespace("tenant")
	require.False(t, sm.HasAnyPropertyIndexForLabel("Person"))
	engine.maintainPropertyIndexesOnNodeDeletedWithNode(nodeID, &Node{ID: nodeID, Labels: []string{"Person"}})

	// Unprefixed ID: no namespace schema.
	engine.maintainPropertyIndexesOnNodeDeletedWithNode("unprefixed", &Node{ID: "unprefixed", Labels: []string{"Person"}})
}

// Deleting a node removes its index entries, scalar values and list elements,
// on the auto-commit and the explicit-transaction delete paths. Both clear the
// node cache before index maintenance runs, so the entries must come from the
// deleted node itself.
func TestDeleteNodeRemovesPropertyIndexEntries(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			engine := newTestEngine(t)
			sm := engine.GetSchemaForNamespace("db")
			require.NoError(t, sm.AddPropertyIndex("idx_name", "Identity", []string{"name"}))
			require.NoError(t, sm.AddPropertyIndex("idx_alias_keys", "Identity", []string{"alias_keys"}))
			sm.MarkPropertyIndexesFilled()

			_, err := engine.CreateNode(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]any{"name": "acme", "alias_keys": []any{"x", "y"}}})
			require.NoError(t, err)
			require.Equal(t, []NodeID{"db:a"}, sm.PropertyIndexLookup("Identity", "name", "acme"))
			require.Equal(t, []NodeID{"db:a"}, sm.PropertyIndexMemberLookup("Identity", "alias_keys", "x"))

			if explicit {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.DeleteNode("db:a"))
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, engine.DeleteNode("db:a"))
			}
			require.Empty(t, sm.PropertyIndexLookup("Identity", "name", "acme"))
			require.Empty(t, sm.PropertyIndexLookup("Identity", "alias_keys", []any{"x", "y"}))
			require.Empty(t, sm.PropertyIndexMemberLookup("Identity", "alias_keys", "x"))
			require.Empty(t, sm.PropertyIndexMemberLookup("Identity", "alias_keys", "y"))
		})
	}
}

func TestSchemaForNodeID_ReturnsNamespaceSchemaOrNil(t *testing.T) {
	engine := newTestEngine(t)
	_ = engine.GetSchemaForNamespace("tenant")
	require.NotNil(t, engine.schemaForNodeID("tenant:n1"))
	require.Nil(t, engine.schemaForNodeID("unprefixed"))
}

func TestMaintainPropertyIndexesOnNodeUpdated_NilInputs_NoPanic(t *testing.T) {
	engine := newTestEngine(t)

	engine.maintainPropertyIndexesOnNodeCreated(nil)
	engine.maintainPropertyIndexesOnNodeUpdated(nil, nil)
	engine.maintainPropertyIndexesOnNodeUpdated(&Node{ID: "unprefixed"}, nil)
}
