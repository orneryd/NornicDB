package multidb

import (
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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

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
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Step 3: Verify migration completed
	assert.True(t, manager.isMigrationComplete(), "Migration should be marked as complete")

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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

	// Create DatabaseManager with fresh storage (no unprefixed data)
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Migration should be marked as complete (nothing to migrate)
	assert.True(t, manager.isMigrationComplete(), "Migration should be marked as complete")

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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

	// Create data with namespace prefix (simulating already-migrated data)
	prefixedNode := &storage.Node{
		ID:     storage.NodeID("nornic:already-prefixed"),
		Labels: []string{"Prefixed"},
		Properties: map[string]any{
			"status": "migrated",
		},
	}
	_, err := inner.CreateNode(prefixedNode)
	require.NoError(t, err)

	// Create DatabaseManager
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Migration should complete (no unprefixed data to migrate)
	assert.True(t, manager.isMigrationComplete())

	// Verify prefixed data is still accessible
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	// Note: NamespacedEngine will unprefix the ID, so we look for "already-prefixed"
	retrieved, err := store.GetNode(storage.NodeID("already-prefixed"))
	require.NoError(t, err)
	assert.Equal(t, "already-prefixed", string(retrieved.ID))
	assert.Equal(t, "migrated", retrieved.Properties["status"])
}

// TestMigration_MixedData verifies migration handles mix of prefixed and unprefixed data.
func TestMigration_MixedData(t *testing.T) {
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

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
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Migration should complete
	assert.True(t, manager.isMigrationComplete())

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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

	// First startup - create unprefixed data and migrate
	oldNode := &storage.Node{
		ID:     storage.NodeID("legacy-node"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"migrated": true,
		},
	}
	_, err := inner.CreateNode(oldNode)
	require.NoError(t, err)

	manager1, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	assert.True(t, manager1.isMigrationComplete())
	// Don't close manager1 - just create a new one with the same storage

	// Second startup - migration should be skipped
	manager2, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager2.Close()

	assert.True(t, manager2.isMigrationComplete(), "Migration should already be complete")

	// Verify data is still accessible
	store, err := manager2.GetDefaultStorage()
	require.NoError(t, err)

	retrieved, err := store.GetNode(storage.NodeID("legacy-node"))
	require.NoError(t, err)
	assert.Equal(t, "legacy-node", string(retrieved.ID))
	assert.Equal(t, true, retrieved.Properties["migrated"])
}

// TestMigration_EdgeRelationshipsPreserved verifies that edge relationships
// are correctly maintained after migration (both start and end nodes prefixed).
func TestMigration_EdgeRelationshipsPreserved(t *testing.T) {
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

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
	manager, err := NewDatabaseManager(inner, nil)
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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

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
	manager, err := NewDatabaseManager(inner, nil)
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
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

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
	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Verify migration completed
	assert.True(t, manager.isMigrationComplete())

	// Verify data is accessible through custom default database
	store, err := manager.GetStorage("customdb")
	require.NoError(t, err)

	retrieved, err := store.GetNode(storage.NodeID("legacy-node"))
	require.NoError(t, err)
	assert.Equal(t, "legacy-node", string(retrieved.ID))
}

// TestMigration_AddsDbProperty verifies that migrated nodes get the db property set
func TestMigration_AddsDbProperty(t *testing.T) {
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

	// Create unprefixed node (simulating pre-multi-db data)
	oldNode := &storage.Node{
		ID:     storage.NodeID("legacy-node"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"name": "Legacy Node",
			// No db property
		},
	}
	_, err := inner.CreateNode(oldNode)
	require.NoError(t, err)

	// Create DatabaseManager - this should trigger migration
	config := &Config{
		DefaultDatabase:  "nornic",
		SystemDatabase:   "system",
		MaxDatabases:     0,
		AllowDropDefault: false,
	}
	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Verify migration completed
	assert.True(t, manager.isMigrationComplete())

	// Get default database storage
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)

	// Retrieve migrated node
	retrieved, err := store.GetNode(storage.NodeID("legacy-node"))
	require.NoError(t, err)
	assert.Equal(t, "legacy-node", string(retrieved.ID))

	// Verify db property was added during migration
	dbValue, exists := retrieved.Properties["db"]
	require.True(t, exists, "migrated node should have db property")
	assert.Equal(t, "nornic", dbValue, "db property should be set to default database name")
}

// TestEnsureDefaultDatabaseProperty verifies that ensureDefaultDatabaseProperty adds db property
func TestEnsureDefaultDatabaseProperty(t *testing.T) {
	baseInner := storage.NewMemoryEngine()

	inner := storage.NewNamespacedEngine(baseInner, "test")
	defer inner.Close()

	config := &Config{
		DefaultDatabase:  "nornic",
		SystemDatabase:   "system",
		MaxDatabases:     0,
		AllowDropDefault: false,
	}
	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Get default database storage
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)

	// Create a node without db property (simulating old data)
	node := &storage.Node{
		ID:     storage.NodeID("test-node"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "Test Node",
			// No db property
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Call ensureDefaultDatabaseProperty
	err = manager.ensureDefaultDatabaseProperty()
	require.NoError(t, err)

	// Retrieve the node
	retrieved, err := store.GetNode(storage.NodeID("test-node"))
	require.NoError(t, err)

	// Verify db property was added
	dbValue, exists := retrieved.Properties["db"]
	require.True(t, exists, "node should have db property after ensureDefaultDatabaseProperty")
	assert.Equal(t, "nornic", dbValue, "db property should be set to default database name")
}
