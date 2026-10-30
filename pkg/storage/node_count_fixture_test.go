package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// nodeCountCycle describes the repeated create/delete/recreate cycle the
// storage count tests exercise. The same cycle runs directly against a
// BadgerEngine and through the WAL wrapper so each layer's count accounting is
// covered separately with identical populations and expected counts. Each test
// supplies its exact expected counts, because a case may deliberately deviate
// from the population sizes it declares (for example a recreate that reuses
// deleted IDs).
type nodeCountCycle struct {
	// createIDs are created first, then all deleted, leaving an empty store.
	createIDs []string
	// recreateIDs are written after the store is empty. recreateIsUpdate
	// selects UpdateNode (the AsyncEngine flush path) over CreateNode (the
	// Cypher MERGE path); the two exercise different wasInsert decisions.
	recreateIDs      []string
	recreateIsUpdate bool
	recreateLabel    string
	// nameProperties adds a "name" property to every seeded node, preserving
	// the property-bearing populations established by some count cases.
	nameProperties bool
	// wantCounts is the exact NodeCount expectation after each cycle step:
	// after creation, after deleting the entire population, and after the
	// recreate.
	wantCounts [3]int64
}

// seedNode builds a cycle node, including the optional name property.
func (c nodeCountCycle) seedNode(id, label string, index int, recreated bool) *Node {
	node := &Node{ID: NodeID(prefixTestID(id)), Labels: []string{label}}
	if c.nameProperties {
		name := fmt.Sprintf("Node %d", index)
		if recreated {
			name = fmt.Sprintf("New Node %d", index)
		}
		node.Properties = map[string]any{"name": name}
	}
	return node
}

// run executes the full create → delete → recreate cycle against engine and
// asserts the exact node count at every step.
func (c nodeCountCycle) run(t *testing.T, engine Engine) {
	t.Helper()
	want := c.wantCounts

	for index, id := range c.createIDs {
		_, err := engine.CreateNode(c.seedNode(id, "Test", index, false))
		require.NoError(t, err, "create node %s", id)
	}
	count, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, want[0], count, "count after initial creation")

	for _, id := range c.createIDs {
		require.NoError(t, engine.DeleteNode(NodeID(prefixTestID(id))), "delete node %s", id)
	}
	count, err = engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, want[1], count, "count after deleting the entire population")

	label := c.recreateLabel
	if label == "" {
		label = "NewTest"
	}
	for index, id := range c.recreateIDs {
		if c.recreateIsUpdate {
			require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: c.seedNode(id, label, index, true)}), "update node %s", id)
			continue
		}
		_, err := engine.CreateNode(c.seedNode(id, label, index, true))
		require.NoError(t, err, "create node %s", id)
	}
	count, err = engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, want[2], count, "count after recreating the population")
}
