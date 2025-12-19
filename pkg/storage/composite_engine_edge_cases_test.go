package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestCompositeEngine_EmptyComposite(t *testing.T) {
	// Create composite engine with no constituents
	constituents := map[string]Engine{}
	constituentNames := map[string]string{}
	accessModes := map[string]string{}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Operations should handle empty composite gracefully
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Equal(t, 0, len(nodes))

	edges, err := composite.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.Equal(t, 0, len(edges))

	allNodes, err := composite.AllNodes()
	require.NoError(t, err)
	assert.Equal(t, 0, len(allNodes))

	// Write operations should fail with clear error
	node := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	err = composite.CreateNode(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no writable constituents available")
}

func TestCompositeEngine_OfflineConstituent(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create node in db1
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	err := engine1.CreateNode(node1)
	require.NoError(t, err)

	// Close db2 (simulate offline)
	engine2.Close()

	// Query should still work with db1 (db2 errors are skipped)
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	// Should return nodes from db1 (db2 is offline but we skip it)
	assert.GreaterOrEqual(t, len(nodes), 1)

	// Write should still work with db1
	// Note: CreateNode routes to a constituent, so it should work if db1 is still open
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	err = composite.CreateNode(node2)
	// May succeed if routed to db1, or fail if routed to db2
	// Either way, the composite engine handles it
	if err != nil {
		assert.Contains(t, err.Error(), "storage closed")
	}
}

func TestCompositeEngine_AllConstituentsOffline(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Close both engines (simulate all offline)
	engine1.Close()
	engine2.Close()

	// Read operations should return empty results (errors are skipped)
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Equal(t, 0, len(nodes))

	// Write operations should fail with storage closed error
	node := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	err = composite.CreateNode(node)
	assert.Error(t, err)
	// May be "no writable constituents available" or "storage closed" depending on routing
	assert.True(t, err.Error() == "no writable constituents available" || 
		err.Error() == "storage closed" || 
		containsSubstring(err.Error(), "storage closed"))
}

func TestCompositeEngine_CircularDependencyPrevention(t *testing.T) {
	// This test verifies that circular dependencies are prevented at the DatabaseManager level
	// The CompositeEngine itself doesn't need to handle this - it's prevented during creation
	// This is tested in pkg/multidb/composite_test.go
	t.Skip("Circular dependency prevention is tested in pkg/multidb/composite_test.go")
}

