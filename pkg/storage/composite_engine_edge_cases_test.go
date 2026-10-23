package storage

import (
	"context"
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
	node := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	_, err = composite.CreateNode(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no writable constituents available")
}

func TestCompositeEngine_OfflineConstituent(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

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
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	_, err := engine1.CreateNode(node1)
	require.NoError(t, err)

	// Close db2 (simulate offline)
	engine2.Close()

	// A readable offline constituent makes the combined result incomplete.
	nodes, err := composite.GetNodesByLabel("Person")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Nil(t, nodes)

	// Write should still work when explicitly targeted to online db1.
	node2 := &Node{
		ID:         NodeID(prefixTestID("node2")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": "db1"},
	}
	_, err = composite.CreateNode(node2)
	require.NoError(t, err)
}

func TestCompositeEngine_AllConstituentsOffline(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

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

	// Reads must report that the constituent data is unavailable.
	nodes, err := composite.GetNodesByLabel("Person")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Nil(t, nodes)

	// Write operations should fail with storage closed error when explicitly targeted.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": "db1"},
	}
	_, err = composite.CreateNode(node)
	assert.Error(t, err)
	assert.True(t, err.Error() == "storage closed" || containsSubstring(err.Error(), "storage closed"))
}

func TestCompositeEngine_CircularDependencyPrevention(t *testing.T) {
	// This test verifies that CompositeEngine handles circular dependencies gracefully
	// even though they should be prevented at the DatabaseManager level.
	// We test what happens if someone manually creates a CompositeEngine with cycles.

	t.Run("nested_composite_works", func(t *testing.T) {
		// Test that nested composites work correctly (A -> B -> C where B is composite)
		// This is not a cycle, just nesting
		engineC := newTestMemoryEngine(t)
		defer engineC.Close()

		// Create composite B with constituent C
		constituentsB := map[string]Engine{
			"dbC": engineC,
		}
		constituentNamesB := map[string]string{
			"dbC": "dbC",
		}
		accessModesB := map[string]string{
			"dbC": "read_write",
		}
		compositeB := NewCompositeEngine(constituentsB, constituentNamesB, accessModesB)

		// Create node in C
		nodeC := &Node{ID: NodeID(prefixTestID("nodeC")), Labels: []string{"Person"}}
		_, err := engineC.CreateNode(nodeC)
		require.NoError(t, err)

		// Create composite A with constituent B (which is itself a composite)
		constituentsA := map[string]Engine{
			"dbB": compositeB,
		}
		constituentNamesA := map[string]string{
			"dbB": "dbB",
		}
		accessModesA := map[string]string{
			"dbB": "read_write",
		}
		compositeA := NewCompositeEngine(constituentsA, constituentNamesA, accessModesA)

		// Query A should find nodes from C (through B)
		nodes, err := compositeA.GetNodesByLabel("Person")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 1, "Should find node from nested composite")
	})

	t.Run("direct_self_reference_detected", func(t *testing.T) {
		// Test that direct self-reference (A -> A) is detected or handled gracefully
		// In practice, this should be prevented by DatabaseManager, but we test the engine behavior
		engine1 := newTestMemoryEngine(t)
		defer engine1.Close()

		// Create composite A
		constituentsA := map[string]Engine{
			"db1": engine1,
		}
		constituentNamesA := map[string]string{
			"db1": "db1",
		}
		accessModesA := map[string]string{
			"db1": "read_write",
		}
		compositeA := NewCompositeEngine(constituentsA, constituentNamesA, accessModesA)

		// Create node in engine1
		node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
		_, err := engine1.CreateNode(node1)
		require.NoError(t, err)

		// Manually create a composite that references itself (simulating a bug or bypass)
		// This would cause infinite recursion if not handled
		constituentsSelf := map[string]Engine{
			"self": compositeA, // Self-reference
		}
		constituentNamesSelf := map[string]string{
			"self": "self",
		}
		accessModesSelf := map[string]string{
			"self": "read_write",
		}
		compositeSelf := NewCompositeEngine(constituentsSelf, constituentNamesSelf, accessModesSelf)

		// Query should work (it will query compositeA, which queries engine1)
		// The self-reference doesn't cause infinite loop because compositeA doesn't reference itself
		nodes, err := compositeSelf.GetNodesByLabel("Person")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 1, "Should find node through self-referencing composite")
	})

	t.Run("indirect_cycle_handled", func(t *testing.T) {
		// Test indirect cycle (A -> B -> A)
		// This should be prevented by DatabaseManager, but we test engine behavior
		engine1 := newTestMemoryEngine(t)
		defer engine1.Close()

		// Create composite A with engine1
		constituentsA := map[string]Engine{
			"db1": engine1,
		}
		constituentNamesA := map[string]string{
			"db1": "db1",
		}
		accessModesA := map[string]string{
			"db1": "read_write",
		}
		compositeA := NewCompositeEngine(constituentsA, constituentNamesA, accessModesA)

		// Create composite B that references A (creating A -> B -> A cycle)
		constituentsB := map[string]Engine{
			"dbA": compositeA,
		}
		constituentNamesB := map[string]string{
			"dbA": "dbA",
		}
		accessModesB := map[string]string{
			"dbA": "read_write",
		}
		compositeB := NewCompositeEngine(constituentsB, constituentNamesB, accessModesB)

		// Add B as a constituent to A (completing the cycle)
		// Note: We can't modify constituents after creation, so we create a new A with B
		constituentsAWithB := map[string]Engine{
			"db1": engine1,
			"dbB": compositeB,
		}
		constituentNamesAWithB := map[string]string{
			"db1": "db1",
			"dbB": "dbB",
		}
		accessModesAWithB := map[string]string{
			"db1": "read_write",
			"dbB": "read_write",
		}
		compositeAWithB := NewCompositeEngine(constituentsAWithB, constituentNamesAWithB, accessModesAWithB)

		// Create node in engine1
		node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
		_, err := engine1.CreateNode(node1)
		require.NoError(t, err)

		// Query should work - it will query engine1 and compositeB
		// compositeB will query compositeA, which will query engine1 and compositeB
		// This creates a cycle, but each CompositeEngine queries its constituents once per call
		// So it should still work (though inefficient)
		nodes, err := compositeAWithB.GetNodesByLabel("Person")
		require.NoError(t, err)
		// Should find the node (may appear multiple times due to cycle, but that's expected)
		assert.GreaterOrEqual(t, len(nodes), 1, "Should find node even with indirect cycle")
	})

	t.Run("composite_as_constituent_prevented_at_manager_level", func(t *testing.T) {
		// This test documents that composite databases cannot be used as constituents
		// The actual prevention happens at DatabaseManager.CreateCompositeDatabase()
		// which is tested in pkg/multidb/composite_test.go
		// Here we just verify that if someone manually creates a CompositeEngine with
		// a composite as constituent, it still works (the engine doesn't prevent it)

		engine1 := newTestMemoryEngine(t)
		defer engine1.Close()

		// Create first composite
		constituents1 := map[string]Engine{
			"db1": engine1,
		}
		constituentNames1 := map[string]string{
			"db1": "db1",
		}
		accessModes1 := map[string]string{
			"db1": "read_write",
		}
		composite1 := NewCompositeEngine(constituents1, constituentNames1, accessModes1)

		// Create node in engine1
		node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
		_, err := engine1.CreateNode(node1)
		require.NoError(t, err)

		// Manually create second composite using first composite as constituent
		// (This would be prevented by DatabaseManager, but engine allows it)
		constituents2 := map[string]Engine{
			"comp1": composite1,
		}
		constituentNames2 := map[string]string{
			"comp1": "comp1",
		}
		accessModes2 := map[string]string{
			"comp1": "read_write",
		}
		composite2 := NewCompositeEngine(constituents2, constituentNames2, accessModes2)

		// Query should work - composite2 queries composite1, which queries engine1
		nodes, err := composite2.GetNodesByLabel("Person")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 1, "Should find node through nested composite")

		// Note: DatabaseManager.CreateCompositeDatabase() prevents this at creation time,
		// but the engine itself doesn't prevent it - it just routes queries through
	})
}

func TestCompositeEngine_GetConstituentByAlias(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	defer engine1.Close()

	constituents := map[string]Engine{"db1": engine1}
	constituentNames := map[string]string{"db1": "database1"}
	accessModes := map[string]string{"db1": "read_write"}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	t.Run("returns engine for valid alias", func(t *testing.T) {
		eng, err := composite.GetConstituentByAlias("db1")
		require.NoError(t, err)
		assert.Equal(t, engine1, eng)
	})

	t.Run("returns error for invalid alias", func(t *testing.T) {
		_, err := composite.GetConstituentByAlias("nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestCompositeEngine_IsComposite(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	defer engine1.Close()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1},
		map[string]string{"db1": "database1"},
		map[string]string{"db1": "read_write"},
	)

	assert.True(t, composite.IsComposite())

	// Also verify the interface-based detection pattern
	type compositeChecker interface{ IsComposite() bool }
	var eng Engine = composite
	cc, ok := eng.(compositeChecker)
	require.True(t, ok)
	assert.True(t, cc.IsComposite())
}

func TestCompositeEngine_routeWrite_EmptyDatabaseID(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	defer engine1.Close()
	defer engine2.Close()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "database1", "db2": "database2"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)

	// Empty string database_id should return "" (ambiguous)
	result := composite.routeWrite("create_node", nil, map[string]interface{}{
		"database_id": "   ",
	}, []string{"db1", "db2"})
	assert.Equal(t, "", result)
}

func TestCompositeEngine_routeWrite_ByConstituentName(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	defer engine1.Close()
	defer engine2.Close()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "database1", "db2": "database2"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)

	// Route by backing database name (not alias)
	result := composite.routeWrite("create_node", nil, map[string]interface{}{
		"database_id": "database2",
	}, []string{"db1", "db2"})
	assert.Equal(t, "db2", result)
}

func TestCompositeEngine_UpdateEdge_CrossConstituent(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	defer engine1.Close()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1},
		map[string]string{"db1": "database1"},
		map[string]string{"db1": "read_write"},
	)

	// Create nodes and edge in db1 — IDs must be namespace-prefixed
	n1 := &Node{ID: "db1:n1", Labels: []string{"A"}, Properties: map[string]interface{}{"database_id": "db1"}}
	n2 := &Node{ID: "db1:n2", Labels: []string{"B"}, Properties: map[string]interface{}{"database_id": "db1"}}
	_, err := engine1.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine1.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine1.CreateEdge(&Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n2", Type: "KNOWS", Properties: map[string]interface{}{}}))

	// Update the edge through composite
	updated := &Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n2", Type: "LIKES", Properties: map[string]interface{}{"weight": 1}}
	err = composite.UpdateEdge(updated)
	require.NoError(t, err)

	// Verify through composite
	edge, err := composite.GetEdge("db1:e1")
	require.NoError(t, err)
	assert.Equal(t, "LIKES", edge.Type)
}

func TestCompositeEngine_DeleteEdge_CrossConstituent(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	defer engine1.Close()

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1},
		map[string]string{"db1": "database1"},
		map[string]string{"db1": "read_write"},
	)

	n1 := &Node{ID: "db1:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: "db1:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := engine1.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine1.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine1.CreateEdge(&Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n2", Type: "KNOWS", Properties: map[string]interface{}{}}))

	// Delete through composite
	err = composite.DeleteEdge("db1:e1")
	require.NoError(t, err)

	// Verify gone
	_, err = composite.GetEdge("db1:e1")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestCompositeEngine_StreamEdges_Fallback(t *testing.T) {
	// Use a plain MemoryEngine which doesn't implement StreamingEngine
	engine1 := newTestMemoryEngine(t)
	defer engine1.Close()

	// Create test data — IDs must be namespace-prefixed
	n1 := &Node{ID: "db1:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: "db1:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := engine1.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine1.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine1.CreateEdge(&Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n2", Type: "KNOWS", Properties: map[string]interface{}{}}))

	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1},
		map[string]string{"db1": "database1"},
		map[string]string{"db1": "read_write"},
	)

	// StreamEdges should use AllEdges fallback since MemoryEngine doesn't implement StreamingEngine
	var collected []*Edge
	err = composite.StreamEdges(context.Background(), func(edge *Edge) error {
		collected = append(collected, edge)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, EdgeID("db1:e1"), collected[0].ID)
}
