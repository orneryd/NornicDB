package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// commitStatesFor folds one acknowledged frame into the publication states the
// engine hands to index maintenance, asserting the fold accepted the frame.
func commitStatesFor(t *testing.T, operations []Operation) commitStates {
	t.Helper()
	states, err := newCommitStates(t.Context(), operations)
	require.NoError(t, err)
	return states
}

func TestPublishIndexMembershipCreatedUpdatedDeleted(t *testing.T) {
	engine := newTestEngine(t)
	ns := "tenant"
	sm := engine.GetSchemaForNamespace(ns)
	require.NoError(t, sm.AddPropertyIndex("idx_person_name", "Person", []string{"name"}))

	node := &Node{
		ID:         NodeID("tenant:n1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice", "age": int64(41)},
	}

	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpCreateNode, Node: node}}))
	require.Equal(t, []NodeID{node.ID}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "Person", "name", "Alice")))

	oldNode := &Node{
		ID:         node.ID,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	node.Properties["name"] = "Alicia"
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpUpdateNode, OldNode: oldNode, Node: node}}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "Person", "name", "Alice")))
	require.Equal(t, []NodeID{node.ID}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "Person", "name", "Alicia")))

	engine.nodeCacheMu.Lock()
	engine.nodeCache[node.ID] = &Node{ID: node.ID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Alicia"}}
	engine.nodeCacheMu.Unlock()

	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpDeleteNode, NodeID: node.ID, OldNode: node}}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "Person", "name", "Alicia")))
}

func TestPublishIndexMembershipUncoveredNamespace(t *testing.T) {
	engine := newTestEngine(t)
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpDeleteNode, NodeID: "tenant:n2"}}))
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpDeleteNode, NodeID: "tenant:n2", OldNode: &Node{ID: "tenant:n2"}}}))
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpDeleteNode, NodeID: "unprefixed", OldNode: &Node{ID: "unprefixed", Labels: []string{"Person"}}}}))
}

func TestSchemaForNodeID_ReturnsNamespaceSchemaOrNil(t *testing.T) {
	engine := newTestEngine(t)
	_ = engine.GetSchemaForNamespace("tenant")
	require.NotNil(t, engine.schemaForNodeID("tenant:n1"))
	require.Nil(t, engine.schemaForNodeID("unprefixed"))
}

func TestPublishIndexMembershipEmptyOperations(t *testing.T) {
	engine := newTestEngine(t)

	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpCreateNode}, {Type: OpUpdateNode}, {Type: OpUpdateNode, Node: &Node{ID: "unprefixed"}}}))
}
