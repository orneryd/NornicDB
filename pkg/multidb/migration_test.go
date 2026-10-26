package multidb

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigration_LegacyDataMigration verifies that existing unprefixed data
// is automatically migrated to the default database namespace.
//
// This test simulates the upgrade scenario:
//  1. Create unprefixed data (simulating old version)
//  2. Create DatabaseManager (triggers automatic migration)
//  3. Verify migrated data is accessible through default database
//  4. Verify all properties and relationships are preserved
func TestMigration_LegacyDataMigration(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Step 1: Create unprefixed data (simulate old version)
	// Directly create nodes/edges without namespace prefix
	oldNode := &storage.Node{
		ID:     storage.NodeID("legacy-node-1"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"created_by": "old-version",
			"version":    "1.0",
		},
	}
	_, err := inner.CreateNode(oldNode)
	require.NoError(t, err)

	oldNode2 := &storage.Node{
		ID:     storage.NodeID("legacy-node-2"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"version": "1.0",
		},
	}
	_, err = inner.CreateNode(oldNode2)
	require.NoError(t, err)

	oldEdge := &storage.Edge{
		ID:        storage.EdgeID("legacy-edge-1"),
		StartNode: storage.NodeID("legacy-node-1"),
		EndNode:   storage.NodeID("legacy-node-2"),
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"created_at": "2024-01-01",
		},
	}
	err = inner.CreateEdge(oldEdge)
	require.NoError(t, err)

	// Step 2: Create DatabaseManager (simulates upgrade to multi-db version)
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Step 3: Verify migration completed
	assert.True(t, foundationMigrationCompleteForTest(t, manager), "Migration should be marked as complete")

	// Step 4: Verify old data is accessible through default database
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	// Should be able to retrieve migrated nodes
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2, "Should have at least 2 migrated nodes")

	// Find our migrated nodes and verify properties are preserved
	var foundNode1, foundNode2 bool
	for _, node := range nodes {
		if string(node.ID) == "legacy-node-1" {
			foundNode1 = true
			assert.Equal(t, "old-version", node.Properties["created_by"])
			assert.Equal(t, "1.0", node.Properties["version"])
		}
		if string(node.ID) == "legacy-node-2" {
			foundNode2 = true
			assert.Equal(t, "1.0", node.Properties["version"])
		}
	}
	assert.True(t, foundNode1, "legacy-node-1 should be accessible with all properties")
	assert.True(t, foundNode2, "legacy-node-2 should be accessible with all properties")

	// Verify edge was migrated with properties preserved
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1, "Should have at least 1 migrated edge")

	var foundEdge bool
	for _, edge := range edges {
		if string(edge.ID) == "legacy-edge-1" {
			foundEdge = true
			assert.Equal(t, "RELATES_TO", edge.Type)
			assert.Equal(t, storage.NodeID("legacy-node-1"), edge.StartNode)
			assert.Equal(t, storage.NodeID("legacy-node-2"), edge.EndNode)
			assert.Equal(t, "2024-01-01", edge.Properties["created_at"])
		}
	}
	assert.True(t, foundEdge, "legacy-edge-1 should be accessible with all properties and relationships")
}

// TestMigration_NoUnprefixedData verifies that migration is skipped
// when there's no unprefixed data (fresh installation).
func TestMigration_NoUnprefixedData(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Create DatabaseManager with fresh storage (no unprefixed data)
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Migration should be marked as complete (nothing to migrate)
	assert.True(t, foundationMigrationCompleteForTest(t, manager), "Migration should be marked as complete")

	// Create new data through namespaced engine (already prefixed)
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	newNode := &storage.Node{
		ID:     storage.NodeID("new-node"),
		Labels: []string{"New"},
	}
	_, err = store.CreateNode(newNode)
	require.NoError(t, err)

	// Verify new data is accessible
	retrieved, err := store.GetNode(storage.NodeID("new-node"))
	require.NoError(t, err)
	assert.Equal(t, "new-node", string(retrieved.ID))
}

// TestMigration_AlreadyPrefixedData verifies that migration doesn't
// affect data that already has namespace prefixes (already migrated).
func TestMigration_AlreadyPrefixedData(t *testing.T) {
	seed := &storage.Node{
		ID:         storage.NodeID("nornic:already-prefixed"),
		Labels:     []string{"Prefixed"},
		Properties: map[string]any{"status": "migrated"},
	}
	// NamespacedEngine unprefixes the ID, so we read "already-prefixed".
	runMigrationVisibility(t, seed, "already-prefixed", "status", "migrated", false)
}

// TestMigration_MixedData verifies migration handles mix of prefixed and unprefixed data.
func TestMigration_MixedData(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Create mix of prefixed and unprefixed data
	unprefixedNode := &storage.Node{
		ID:     storage.NodeID("unprefixed-node"),
		Labels: []string{"Mixed"},
		Properties: map[string]any{
			"type": "unprefixed",
		},
	}
	_, err := inner.CreateNode(unprefixedNode)
	require.NoError(t, err)

	prefixedNode := &storage.Node{
		ID:     storage.NodeID("nornic:prefixed-node"),
		Labels: []string{"Mixed"},
		Properties: map[string]any{
			"type": "prefixed",
		},
	}
	_, err = inner.CreateNode(prefixedNode)
	require.NoError(t, err)

	// Create DatabaseManager
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Migration should complete
	assert.True(t, foundationMigrationCompleteForTest(t, manager))

	// Verify both nodes are accessible
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	nodes, err := store.AllNodes()
	require.NoError(t, err)

	var foundUnprefixed, foundPrefixed bool
	for _, node := range nodes {
		if string(node.ID) == "unprefixed-node" {
			foundUnprefixed = true
			assert.Equal(t, "unprefixed", node.Properties["type"])
		}
		if string(node.ID) == "prefixed-node" {
			foundPrefixed = true
			assert.Equal(t, "prefixed", node.Properties["type"])
		}
	}
	assert.True(t, foundUnprefixed, "Unprefixed node should be migrated and accessible")
	assert.True(t, foundPrefixed, "Prefixed node should remain accessible")
}

// TestMigration_NoMigrationOnSecondStart verifies that migration
// doesn't run again if already completed (idempotency).
func TestMigration_NoMigrationOnSecondStart(t *testing.T) {
	seed := &storage.Node{
		ID:         storage.NodeID("legacy-node"),
		Labels:     []string{"Legacy"},
		Properties: map[string]any{"migrated": true},
	}
	runMigrationVisibility(t, seed, "legacy-node", "migrated", true, true)
}

// TestMigration_EdgeRelationshipsPreserved verifies that edge relationships
// are correctly maintained after migration (both start and end nodes prefixed).
func TestMigration_EdgeRelationshipsPreserved(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Create unprefixed nodes and edge
	node1 := &storage.Node{
		ID:     storage.NodeID("node-1"),
		Labels: []string{"Test"},
	}
	_, err := inner.CreateNode(node1)
	require.NoError(t, err)

	node2 := &storage.Node{
		ID:     storage.NodeID("node-2"),
		Labels: []string{"Test"},
	}
	_, err = inner.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:        storage.EdgeID("edge-1"),
		StartNode: storage.NodeID("node-1"),
		EndNode:   storage.NodeID("node-2"),
		Type:      "CONNECTS",
	}
	err = inner.CreateEdge(edge)
	require.NoError(t, err)

	// Migrate
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	// Verify edge relationships are preserved
	retrieved, err := store.GetEdge(storage.EdgeID("edge-1"))
	require.NoError(t, err)
	assert.Equal(t, storage.NodeID("node-1"), retrieved.StartNode)
	assert.Equal(t, storage.NodeID("node-2"), retrieved.EndNode)
	assert.Equal(t, "CONNECTS", retrieved.Type)

	// Verify we can traverse from node1
	outgoing, err := store.GetOutgoingEdges(storage.NodeID("node-1"))
	require.NoError(t, err)
	assert.Len(t, outgoing, 1)
	assert.Equal(t, storage.EdgeID("edge-1"), outgoing[0].ID)

	// Verify we can traverse to node2
	incoming, err := store.GetIncomingEdges(storage.NodeID("node-2"))
	require.NoError(t, err)
	assert.Len(t, incoming, 1)
	assert.Equal(t, storage.EdgeID("edge-1"), incoming[0].ID)
}

// TestMigration_LabelIndexesPreserved verifies that label indexes
// are correctly maintained after migration.
func TestMigration_LabelIndexesPreserved(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Create unprefixed nodes with labels
	node1 := &storage.Node{
		ID:     storage.NodeID("person-1"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
		},
	}
	_, err := inner.CreateNode(node1)
	require.NoError(t, err)

	node2 := &storage.Node{
		ID:     storage.NodeID("person-2"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Bob",
		},
	}
	_, err = inner.CreateNode(node2)
	require.NoError(t, err)

	// Migrate
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	// Verify label index works after migration
	people, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, people, 2)

	var foundAlice, foundBob bool
	for _, person := range people {
		if person.Properties["name"] == "Alice" {
			foundAlice = true
		}
		if person.Properties["name"] == "Bob" {
			foundBob = true
		}
	}
	assert.True(t, foundAlice, "Alice should be findable by label")
	assert.True(t, foundBob, "Bob should be findable by label")
}

// TestMigration_CustomDefaultDatabase verifies migration works with
// custom default database names.
func TestMigration_CustomDefaultDatabase(t *testing.T) {
	inner := newLegacyTestEngine(t)

	// Create unprefixed data
	oldNode := &storage.Node{
		ID:     storage.NodeID("legacy-node"),
		Labels: []string{"Legacy"},
	}
	_, err := inner.CreateNode(oldNode)
	require.NoError(t, err)

	// Create manager with custom default database
	config := &Config{
		DefaultDatabase:  "customdb",
		SystemDatabase:   "system",
		MaxDatabases:     0,
		AllowDropDefault: false,
	}
	manager, err := NewDatabaseManager(t.Context(), inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Verify migration completed
	assert.True(t, foundationMigrationCompleteForTest(t, manager))

	// Verify data is accessible through custom default database
	store, err := manager.GetStorage("customdb")
	require.NoError(t, err)

	retrieved, err := store.GetNode(storage.NodeID("legacy-node"))
	require.NoError(t, err)
	assert.Equal(t, "legacy-node", string(retrieved.ID))
}

// TestMigration_DoesNotInjectDbProperty verifies migration keeps user properties clean.
// Both ingestion stages must preserve user properties without injected metadata.
func TestMigration_NodePropertiesRemainUserOwned(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			inner := newLegacyTestEngine(t)
			id, name, label := "test-node", "Test Node", "Test"
			if legacy {
				id, name, label = "legacy-node", "Legacy Node", "Legacy"
			}
			node := &storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: map[string]any{"name": name}}
			if legacy {
				_, err := inner.CreateNode(node)
				require.NoError(t, err)
			}
			manager, err := NewDatabaseManager(t.Context(), inner, &Config{DefaultDatabase: "nornic", SystemDatabase: "system", MaxDatabases: 0, AllowDropDefault: false})
			require.NoError(t, err)
			defer manager.Close()
			if legacy {
				assert.True(t, foundationMigrationCompleteForTest(t, manager))
			}
			store, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			if !legacy {
				_, err = store.CreateNode(node)
				require.NoError(t, err)
			}
			retrieved, err := store.GetNode(storage.NodeID(id))
			require.NoError(t, err)
			assert.Equal(t, id, string(retrieved.ID))
			_, exists := retrieved.Properties["db"]
			assert.False(t, exists, "node should not have injected db property")
		})
	}
}

// runMigrationVisibility seeds one node on a fresh legacy engine, opens a
// DatabaseManager (optionally a second on the same engine to model a restart),
// asserts migration completion, and verifies the node is readable through the
// default storage with the expected property preserved.
func runMigrationVisibility(t *testing.T, seed *storage.Node, readID string, prop string, want any, restart bool) {
	t.Helper()
	inner := newLegacyTestEngine(t)
	_, err := inner.CreateNode(seed)
	require.NoError(t, err)

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Verify migration completed
	assert.True(t, foundationMigrationCompleteForTest(t, manager))

	if restart {
		manager2, err := NewDatabaseManager(t.Context(), inner, nil)
		require.NoError(t, err)
		defer manager2.Close()
		assert.True(t, foundationMigrationCompleteForTest(t, manager2), "Migration should already be complete")
		manager = manager2
	} else {
		defer manager.Close()
	}

	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)
	retrieved, err := store.GetNode(storage.NodeID(readID))
	require.NoError(t, err)
	assert.Equal(t, readID, string(retrieved.ID))
	assert.Equal(t, want, retrieved.Properties[prop])
}

// Author: Lane #1644 (Codex). The private status reader now carries failures;
// existing migration value assertions also require a successful status read.
func foundationMigrationCompleteForTest(t *testing.T, manager *DatabaseManager) bool {
	t.Helper()
	complete, err := manager.isMigrationComplete()
	require.NoError(t, err)
	return complete
}
