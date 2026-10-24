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
	engine.nodeCache[node.ID] = nodeCacheEntry{Node: &Node{ID: node.ID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Alicia"}}, readTs: engine.nodeCacheCommitTs.Load()}
	engine.nodeCacheMu.Unlock()

	engine.maintainPropertyIndexesOnNodeDeletedWithLabels(node.ID, []string{"Person"})
	require.Empty(t, sm.PropertyIndexLookup("Person", "name", "Alicia"))
}

func TestMaintainPropertyIndexesOnNodeDeletedWithLabels_EarlyReturns(t *testing.T) {
	engine := newTestEngine(t)
	nodeID := NodeID("tenant:n2")

	// No labels branch.
	engine.maintainPropertyIndexesOnNodeDeletedWithLabels(nodeID, nil)

	// Labels with no index branch.
	sm := engine.GetSchemaForNamespace("tenant")
	require.False(t, sm.HasAnyPropertyIndexForLabel("Person"))
	engine.maintainPropertyIndexesOnNodeDeletedWithLabels(nodeID, []string{"Person"})

	// Indexed label but cache miss branch.
	require.NoError(t, sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"}))
	engine.maintainPropertyIndexesOnNodeDeletedWithLabels(nodeID, []string{"Person"})
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
