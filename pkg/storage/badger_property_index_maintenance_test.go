package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaintainPropertyIndexesOnNodeCreated_Updated_Deleted(t *testing.T) {
	engine := newTestEngine(t)
	ns := "tenant"
	sm := engine.GetSchemaForNamespace(ns)
	require.NoError(t, sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"}))

	node := &Node{
		ID:         NodeID("tenant:n1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice", "age": int64(41)},
	}

	engine.maintainPropertyIndexesOnNodeCreated(node)
	require.Equal(t, []NodeID{node.ID}, sm.PropertyIndexLookup("Person", "name", "Alice"))

	oldNode := &Node{
		ID:         node.ID,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	node.Properties["name"] = "Alicia"
	engine.maintainPropertyIndexesOnNodeUpdated(node, oldNode)
	require.Empty(t, sm.PropertyIndexLookup("Person", "name", "Alice"))
	require.Equal(t, []NodeID{node.ID}, sm.PropertyIndexLookup("Person", "name", "Alicia"))

	engine.nodeCacheMu.Lock()
	engine.nodeCache[node.ID] = &Node{ID: node.ID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Alicia"}}
	engine.nodeCacheMu.Unlock()

	engine.maintainPropertyIndexesOnNodeDeletedWithNode(node.ID, node)
	require.Empty(t, sm.PropertyIndexLookup("Person", "name", "Alicia"))
}

func TestMaintainPropertyIndexesOnNodeDeletedWithNode_EarlyReturns(t *testing.T) {
	engine := newTestEngine(t)
	engine.maintainPropertyIndexesOnNodeDeletedWithNode("tenant:n2", nil)
	engine.maintainPropertyIndexesOnNodeDeletedWithNode("tenant:n2", &Node{ID: "tenant:n2"})
	engine.maintainPropertyIndexesOnNodeDeletedWithNode("unprefixed", &Node{ID: "unprefixed", Labels: []string{"Person"}})
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
