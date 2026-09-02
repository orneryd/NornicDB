package multidb

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseManager_DefaultConfig(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Default database should be "nornic"
	assert.Equal(t, "nornic", manager.DefaultDatabaseName())

	// System database should exist
	assert.True(t, manager.Exists("system"))

	// Default database should exist
	assert.True(t, manager.Exists("nornic"))

	// Default database should be marked as default
	info, err := manager.GetDatabase("nornic")
	require.NoError(t, err)
	assert.True(t, info.IsDefault)
	assert.Equal(t, "standard", info.Type)
	assert.Equal(t, "online", info.Status)
}

func TestDatabaseManager_CustomConfig(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	config := &Config{
		DefaultDatabase:  "customdb",
		SystemDatabase:   "system",
		MaxDatabases:     10,
		AllowDropDefault: false,
	}

	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	assert.Equal(t, "customdb", manager.DefaultDatabaseName())
	assert.True(t, manager.Exists("customdb"))
	assert.True(t, manager.Exists("system"))
}

func TestDatabaseManager_CreateDatabase(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create a new database
	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Verify it exists
	assert.True(t, manager.Exists("tenant_a"))

	// Get database info
	info, err := manager.GetDatabase("tenant_a")
	require.NoError(t, err)
	assert.Equal(t, "tenant_a", info.Name)
	assert.Equal(t, "standard", info.Type)
	assert.Equal(t, "online", info.Status)
	assert.False(t, info.IsDefault)

	// Create another database
	err = manager.CreateDatabase("tenant_b")
	require.NoError(t, err)
	assert.True(t, manager.Exists("tenant_b"))
}

func TestDatabaseManager_CreateDatabase_Duplicate(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Try to create duplicate
	err = manager.CreateDatabase("tenant_a")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseExists, err)
}

func TestDatabaseManagerDropClearsLimitChecker(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("tenant"))

	_, err = manager.GetStorage("tenant")
	require.NoError(t, err)
	first := manager.limitCheckers["tenant"]
	require.NotNil(t, first)
	require.NoError(t, manager.DropDatabase("tenant"))
	require.NotContains(t, manager.limitCheckers, "tenant")

	require.NoError(t, manager.CreateDatabase("tenant"))
	_, err = manager.GetStorage("tenant")
	require.NoError(t, err)
	require.NotSame(t, first, manager.limitCheckers["tenant"])
}

func TestResolveConnectionLimitOwner(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("tenant"))
	require.NoError(t, manager.CreateAlias("tenant-alias", "tenant"))
	require.NoError(t, manager.CreateCompositeDatabase("fabric", []ConstituentRef{{
		Alias: "part", DatabaseName: "tenant", Type: "local", AccessMode: "read_write",
	}}))

	owner, err := manager.ResolveConnectionLimitOwner("tenant-alias")
	require.NoError(t, err)
	require.Equal(t, "tenant", owner)
	owner, err = manager.ResolveConnectionLimitOwner("fabric.part")
	require.NoError(t, err)
	require.Equal(t, "tenant", owner)
}

func TestDatabaseManagerStorageEnforcesRuntimeLimitReplacement(t *testing.T) {
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	limits := &Limits{Storage: StorageLimits{MaxNodes: 1}}
	require.NoError(t, manager.SetDatabaseLimits("nornic", limits))
	limits.Storage.MaxNodes = 100

	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "first"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)

	require.NoError(t, manager.SetDatabaseLimits("nornic", &Limits{Storage: StorageLimits{MaxNodes: 2}}))
	_, err = engine.CreateNode(&storage.Node{ID: "second"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "still-blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
}

func TestDatabaseManagerStorageEnforcesMaxBytesUpdateDelta(t *testing.T) {
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "node", Properties: map[string]any{"value": "small"}})
	require.NoError(t, err)
	currentBytes, _, _ := manager.GetStorageSize("nornic")
	require.Positive(t, currentBytes)
	require.NoError(t, manager.SetDatabaseLimits("nornic", &Limits{Storage: StorageLimits{MaxBytes: currentBytes + 32}}))

	err = engine.UpdateNode(&storage.Node{ID: "node", Properties: map[string]any{"value": string(make([]byte, 4096))}})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)

	stored, err := engine.GetNode("node")
	require.NoError(t, err)
	require.Equal(t, "small", stored.Properties["value"])
}

func TestDatabaseManager_CreateDatabase_InvalidName(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Empty name should fail
	err = manager.CreateDatabase("")
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidDatabaseName, err)
}

func TestDatabaseManager_CreateDatabase_MaxLimit(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	config := &Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		MaxDatabases:    3, // system + nornic + 1 more
	}

	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Create one more (should succeed)
	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Try to create another (should fail)
	err = manager.CreateDatabase("tenant_b")
	assert.Error(t, err)
	assert.Equal(t, ErrMaxDatabasesReached, err)
}

func TestDatabaseManager_DropDatabase(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create database
	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Add some data to the database
	store, err := manager.GetStorage("tenant_a")
	require.NoError(t, err)

	node := &storage.Node{
		ID:     storage.NodeID("test-node"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "test",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Drop database
	err = manager.DropDatabase("tenant_a")
	require.NoError(t, err)

	// Verify it's gone
	assert.False(t, manager.Exists("tenant_a"))

	// Verify data is deleted (try to get storage - should fail)
	_, err = manager.GetStorage("tenant_a")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
}

func TestDatabaseManager_DropDatabase_NotFound(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	err = manager.DropDatabase("nonexistent")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
}

func TestDatabaseManager_DropDatabase_System(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Cannot drop system database
	err = manager.DropDatabase("system")
	assert.Error(t, err)
	assert.Equal(t, ErrCannotDropSystemDB, err)
}

func TestDatabaseManager_DropDatabase_Default(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Cannot drop default database (by default)
	err = manager.DropDatabase("nornic")
	assert.Error(t, err)
	assert.Equal(t, ErrCannotDropDefaultDB, err)
}

func TestDatabaseManager_DropDatabase_Default_Allowed(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	config := &Config{
		DefaultDatabase:  "nornic",
		SystemDatabase:   "system",
		AllowDropDefault: true,
	}

	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Should be able to drop default if allowed
	err = manager.DropDatabase("nornic")
	require.NoError(t, err)
	assert.False(t, manager.Exists("nornic"))
}

// TestDatabaseManager_DropDatabase_DeletesNodesAndEdges verifies that dropping
// a database actually deletes all nodes and edges from the underlying storage.
// This is a critical test to ensure DeleteByPrefix works correctly.
func TestDatabaseManager_DropDatabase_DeletesNodesAndEdges(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create a test database
	dbName := "test_db"
	err = manager.CreateDatabase(dbName)
	require.NoError(t, err)

	// Get namespaced storage for the database
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create multiple nodes in the database
	node1 := &storage.Node{
		ID:     storage.NodeID("node1"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
			"age":  30,
		},
	}
	_, err = store.CreateNode(node1)
	require.NoError(t, err)

	node2 := &storage.Node{
		ID:     storage.NodeID("node2"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Bob",
			"age":  25,
		},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	node3 := &storage.Node{
		ID:     storage.NodeID("node3"),
		Labels: []string{"Company"},
		Properties: map[string]any{
			"name": "Acme Corp",
		},
	}
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	// Create edges between nodes
	edge1 := &storage.Edge{
		ID:        storage.EdgeID("edge1"),
		StartNode: storage.NodeID("node1"),
		EndNode:   storage.NodeID("node3"),
		Type:      "WORKS_FOR",
		Properties: map[string]any{
			"since": "2020-01-01",
		},
	}
	err = store.CreateEdge(edge1)
	require.NoError(t, err)

	edge2 := &storage.Edge{
		ID:        storage.EdgeID("edge2"),
		StartNode: storage.NodeID("node2"),
		EndNode:   storage.NodeID("node3"),
		Type:      "WORKS_FOR",
		Properties: map[string]any{
			"since": "2021-01-01",
		},
	}
	err = store.CreateEdge(edge2)
	require.NoError(t, err)

	// Verify nodes exist in the database
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Equal(t, 3, len(nodes), "Should have 3 nodes before drop")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Equal(t, 2, len(edges), "Should have 2 edges before drop")

	// Verify nodes exist in underlying storage (with namespace prefix)
	// The actual IDs in storage should be prefixed: "test_db:node1", etc.
	prefixedNode1ID := storage.NodeID(dbName + ":node1")
	prefixedNode2ID := storage.NodeID(dbName + ":node2")
	prefixedNode3ID := storage.NodeID(dbName + ":node3")

	_, err = inner.GetNode(prefixedNode1ID)
	require.NoError(t, err, "Node1 should exist in underlying storage")

	_, err = inner.GetNode(prefixedNode2ID)
	require.NoError(t, err, "Node2 should exist in underlying storage")

	_, err = inner.GetNode(prefixedNode3ID)
	require.NoError(t, err, "Node3 should exist in underlying storage")

	// Verify edges exist in underlying storage (with namespace prefix)
	prefixedEdge1ID := storage.EdgeID(dbName + ":edge1")
	prefixedEdge2ID := storage.EdgeID(dbName + ":edge2")

	_, err = inner.GetEdge(prefixedEdge1ID)
	require.NoError(t, err, "Edge1 should exist in underlying storage")

	_, err = inner.GetEdge(prefixedEdge2ID)
	require.NoError(t, err, "Edge2 should exist in underlying storage")

	// Count total nodes in underlying storage (should include our 3 nodes)
	allNodesBefore, err := inner.AllNodes()
	require.NoError(t, err)
	initialNodeCount := len(allNodesBefore)
	assert.GreaterOrEqual(t, initialNodeCount, 3, "Underlying storage should have at least 3 nodes")

	// Count total edges in underlying storage
	allEdgesBefore, err := inner.AllEdges()
	require.NoError(t, err)
	initialEdgeCount := len(allEdgesBefore)
	assert.GreaterOrEqual(t, initialEdgeCount, 2, "Underlying storage should have at least 2 edges")

	// Drop the database
	err = manager.DropDatabase(dbName)
	require.NoError(t, err)

	// Verify database is gone from metadata
	assert.False(t, manager.Exists(dbName))

	// Verify nodes are deleted from underlying storage
	_, err = inner.GetNode(prefixedNode1ID)
	assert.Error(t, err, "Node1 should be deleted from underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	_, err = inner.GetNode(prefixedNode2ID)
	assert.Error(t, err, "Node2 should be deleted from underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	_, err = inner.GetNode(prefixedNode3ID)
	assert.Error(t, err, "Node3 should be deleted from underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	// Verify edges are deleted from underlying storage
	_, err = inner.GetEdge(prefixedEdge1ID)
	assert.Error(t, err, "Edge1 should be deleted from underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	_, err = inner.GetEdge(prefixedEdge2ID)
	assert.Error(t, err, "Edge2 should be deleted from underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	// Verify node count decreased
	allNodesAfter, err := inner.AllNodes()
	require.NoError(t, err)
	finalNodeCount := len(allNodesAfter)
	assert.Equal(t, initialNodeCount-3, finalNodeCount, "Node count should decrease by 3 after drop")

	// Verify edge count decreased
	allEdgesAfter, err := inner.AllEdges()
	require.NoError(t, err)
	finalEdgeCount := len(allEdgesAfter)
	assert.Equal(t, initialEdgeCount-2, finalEdgeCount, "Edge count should decrease by 2 after drop")

	// Verify we can't access the database storage anymore
	_, err = manager.GetStorage(dbName)
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
}

// TestDatabaseManager_NodeNamespacePrefix verifies that nodes created in a database
// get the proper namespace prefix. This ensures database isolation works correctly.
func TestDatabaseManager_NodeNamespacePrefix(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create a test database
	dbName := "test_db"
	err = manager.CreateDatabase(dbName)
	require.NoError(t, err)

	// Get namespaced storage for the database
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create a node through the namespaced storage
	nodeID := storage.NodeID("test-node-1")
	node := &storage.Node{
		ID:     nodeID,
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Test Person",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Verify the node exists in the namespaced storage
	retrieved, err := store.GetNode(nodeID)
	require.NoError(t, err)
	assert.Equal(t, "Test Person", retrieved.Properties["name"])

	// Verify the underlying storage has the prefixed ID
	prefixedID := storage.NodeID(dbName + ":" + string(nodeID))
	prefixedNode, err := inner.GetNode(prefixedID)
	require.NoError(t, err)
	assert.Equal(t, prefixedID, prefixedNode.ID, "Node ID should be prefixed in underlying storage")
	assert.Equal(t, "Test Person", prefixedNode.Properties["name"])

	// Verify the unprefixed ID does NOT exist in underlying storage
	_, err = inner.GetNode(nodeID)
	assert.Error(t, err, "Unprefixed node ID should not exist in underlying storage")
	assert.Equal(t, storage.ErrNotFound, err)

	// Verify AllNodes in namespaced storage only returns this node
	allNodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(allNodes), "Namespaced storage should only see 1 node")
	assert.Equal(t, nodeID, allNodes[0].ID, "Node ID should be unprefixed in namespaced view")

	// Verify the node is NOT visible in default database
	defaultStore, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	defaultNodes, err := defaultStore.AllNodes()
	require.NoError(t, err)
	// Filter out system nodes
	nonSystemNodes := []*storage.Node{}
	for _, n := range defaultNodes {
		isSystem := false
		for _, label := range n.Labels {
			if label == "_System" {
				isSystem = true
				break
			}
		}
		if !isSystem {
			nonSystemNodes = append(nonSystemNodes, n)
		}
	}
	assert.Equal(t, 0, len(nonSystemNodes), "Default database should not see nodes from test_db")
}

func TestDatabaseManager_GetStorage(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Get storage for default database
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Create a node
	node := &storage.Node{
		ID:     storage.NodeID("test-node"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "test",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Get the same storage again (should be cached)
	store2, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	assert.Equal(t, store, store2) // Should be same instance (cached)

	t.Run("composite storage resolves aliases and is not cached as namespaced engine", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase("tenant_a"))
		require.NoError(t, manager.CreateDatabase("tenant_b"))
		require.NoError(t, manager.CreateAlias("tenant_b_alias", "tenant_b"))

		tenantAStore, err := manager.GetStorage("tenant_a")
		require.NoError(t, err)
		_, err = tenantAStore.CreateNode(&storage.Node{ID: "node-a", Labels: []string{"Shared"}})
		require.NoError(t, err)

		tenantBStore, err := manager.GetStorage("tenant_b")
		require.NoError(t, err)
		_, err = tenantBStore.CreateNode(&storage.Node{ID: "node-b", Labels: []string{"Shared"}})
		require.NoError(t, err)

		err = manager.CreateCompositeDatabase("composite_db", []ConstituentRef{
			{DatabaseName: "tenant_a", Alias: "a", Type: "local", AccessMode: "read_write"},
			{DatabaseName: "tenant_b_alias", Alias: "b", Type: "local", AccessMode: "read"},
		})
		require.NoError(t, err)

		compositeStore, err := manager.GetStorage("composite_db")
		require.NoError(t, err)
		_, ok := compositeStore.(*storage.CompositeEngine)
		assert.True(t, ok)

		compositeStore2, err := manager.GetStorage("composite_db")
		require.NoError(t, err)
		assert.NotSame(t, compositeStore, compositeStore2)
	})
}

func TestNewDatabaseManagerReconcilesStorageSizesAtStartup(t *testing.T) {
	base := storage.NewMemoryEngine()
	seed, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	engine, err := seed.GetStorage(seed.DefaultDatabaseName())
	require.NoError(t, err)
	node := &storage.Node{ID: "persisted", Labels: []string{"Record"}, Properties: map[string]any{"name": "alpha"}}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	restarted, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	restartedStorage, err := restarted.GetStorage(restarted.DefaultDatabaseName())
	require.NoError(t, err)
	storedNode, err := restartedStorage.GetNode("persisted")
	require.NoError(t, err)
	wantNodeSize, err := calculateNodeSize(storedNode)
	require.NoError(t, err)
	restarted.mu.RLock()
	info := restarted.databases[restarted.DefaultDatabaseName()]
	restarted.mu.RUnlock()
	require.NotNil(t, info)
	info.sizeMu.RLock()
	defer info.sizeMu.RUnlock()
	require.True(t, info.sizeInitialized)
	require.Equal(t, wantNodeSize, info.nodeSize)
	require.Equal(t, wantNodeSize, info.totalSize)
}

func TestDatabaseManager_GetStorage_NotFound(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.GetStorage("nonexistent")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
}

func TestDatabaseManager_GetStorage_Isolation(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create two databases
	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)
	err = manager.CreateDatabase("tenant_b")
	require.NoError(t, err)

	// Get storage for each
	storeA, err := manager.GetStorage("tenant_a")
	require.NoError(t, err)
	storeB, err := manager.GetStorage("tenant_b")
	require.NoError(t, err)

	// Create node in tenant_a
	nodeA := &storage.Node{
		ID:     storage.NodeID("node-a"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"tenant": "a",
		},
	}
	_, err = storeA.CreateNode(nodeA)
	require.NoError(t, err)

	// Create node in tenant_b
	nodeB := &storage.Node{
		ID:     storage.NodeID("node-b"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"tenant": "b",
		},
	}
	_, err = storeB.CreateNode(nodeB)
	require.NoError(t, err)

	// Verify isolation: tenant_a should only see its node
	nodesA, err := storeA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesA, 1)
	assert.Equal(t, "node-a", string(nodesA[0].ID))

	// Verify isolation: tenant_b should only see its node
	nodesB, err := storeB.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesB, 1)
	assert.Equal(t, "node-b", string(nodesB[0].ID))
}

func TestDatabaseManager_ListDatabases(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create some databases
	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)
	err = manager.CreateDatabase("tenant_b")
	require.NoError(t, err)

	// List all databases
	databases := manager.ListDatabases()

	// Should have system, nornic, tenant_a, tenant_b
	assert.GreaterOrEqual(t, len(databases), 4)

	names := make(map[string]bool)
	for _, db := range databases {
		names[db.Name] = true
	}

	assert.True(t, names["system"])
	assert.True(t, names["nornic"])
	assert.True(t, names["tenant_a"])
	assert.True(t, names["tenant_b"])
}

func TestDatabaseManager_SetDatabaseStatus(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Set to offline
	err = manager.SetDatabaseStatus("tenant_a", "offline")
	require.NoError(t, err)

	info, err := manager.GetDatabase("tenant_a")
	require.NoError(t, err)
	assert.Equal(t, "offline", info.Status)

	// Try to get storage for offline database
	_, err = manager.GetStorage("tenant_a")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseOffline, err)

	// Set back to online
	err = manager.SetDatabaseStatus("tenant_a", "online")
	require.NoError(t, err)

	// Should be able to get storage now
	_, err = manager.GetStorage("tenant_a")
	require.NoError(t, err)
}

func TestDatabaseManager_SetDatabaseStatus_Invalid(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	err = manager.CreateDatabase("tenant_a")
	require.NoError(t, err)

	// Invalid status
	err = manager.SetDatabaseStatus("tenant_a", "invalid")
	assert.Error(t, err)
}

func TestDatabaseManager_GetDefaultStorage(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	storage, err := manager.GetDefaultStorage()
	require.NoError(t, err)
	assert.NotNil(t, storage)

	// Should be the same as getting "nornic"
	storage2, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	assert.Equal(t, storage, storage2)
}

func TestDatabaseManager_InternalStorageAndAliasHelpers(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	t.Run("getStorageInternal handles cache not found and offline", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase("tenant_a"))
		manager.mu.Lock()
		engine1, err := manager.getStorageInternal("tenant_a")
		require.NoError(t, err)
		engine2, err := manager.getStorageInternal("tenant_a")
		require.NoError(t, err)
		manager.mu.Unlock()
		assert.Same(t, engine1, engine2)

		manager.mu.Lock()
		_, err = manager.getStorageInternal("missing")
		require.ErrorIs(t, err, ErrDatabaseNotFound)
		manager.databases["tenant_a"].Status = "offline"
		delete(manager.engines, "tenant_a")
		_, err = manager.getStorageInternal("tenant_a")
		require.ErrorIs(t, err, ErrDatabaseOffline)
		manager.mu.Unlock()
	})

	t.Run("default database name and alias validation branches", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DefaultDatabase = cfg.SystemDatabase
		manager2, err := NewDatabaseManager(storage.NewMemoryEngine(), cfg)
		require.NoError(t, err)
		defer manager2.Close()
		assert.Equal(t, "nornic", manager2.DefaultDatabaseName())

		require.ErrorIs(t, manager.validateAliasName(""), ErrInvalidAliasName)
		require.ErrorContains(t, manager.validateAliasName("bad alias"), "whitespace")
		require.ErrorContains(t, manager.validateAliasName("system"), "reserved")
		require.ErrorContains(t, manager.validateAliasName("nornic"), "reserved")
		require.NoError(t, manager.validateAliasName("tenant_alias"))
	})

	t.Run("list aliases all databases and decrement size underflow", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase("tenant_b"))
		require.NoError(t, manager.CreateAlias("alias_a", "nornic"))
		require.NoError(t, manager.CreateAlias("alias_b", "tenant_b"))

		allAliases := manager.ListAliases("")
		assert.Equal(t, "nornic", allAliases["alias_a"])
		assert.Equal(t, "tenant_b", allAliases["alias_b"])
		assert.Empty(t, manager.ListAliases("missing"))

		manager.IncrementStorageSize("tenant_b", 100, 50)
		manager.DecrementStorageSize("tenant_b", 200, 100)
		total, nodeSize, edgeSize := manager.GetStorageSize("tenant_b")
		assert.Zero(t, total)
		assert.Zero(t, nodeSize)
		assert.Zero(t, edgeSize)

		manager.DecrementStorageSize("missing", 10, 10)
	})
}

func TestDatabaseManager_MetadataPersistence(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	// Create first manager
	manager1, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)

	err = manager1.CreateDatabase("tenant_a")
	require.NoError(t, err)
	err = manager1.CreateDatabase("tenant_b")
	require.NoError(t, err)

	// Don't close the manager - just create a new one with same storage
	// (In real usage, the storage would persist to disk, but for memory engine
	// we just reuse the same instance)
	manager2, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager2.Close()

	// Should still see the databases (metadata persisted in storage)
	assert.True(t, manager2.Exists("tenant_a"))
	assert.True(t, manager2.Exists("tenant_b"))
	assert.True(t, manager2.Exists("nornic"))
	assert.True(t, manager2.Exists("system"))
}

func TestDatabaseManager_BackwardsCompatibility(t *testing.T) {
	// Test that existing code without multi-db still works
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	// Create manager with default config (simulates old behavior)
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Default database should work
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)

	// Can create nodes in default database (backwards compatible)
	node := &storage.Node{
		ID:     storage.NodeID("legacy-node"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"name": "legacy",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Can query nodes
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
}

func TestDatabaseManager_GetStorageSize_LazyInitFromRealData(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	require.NoError(t, manager.CreateDatabase("tenant_size"))
	store, err := manager.GetStorage("tenant_size")
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     storage.NodeID("a"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "alice",
			"age":  int64(42),
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     storage.NodeID("b"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "bob",
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:         storage.EdgeID("ab"),
		StartNode:  storage.NodeID("a"),
		EndNode:    storage.NodeID("b"),
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2020)},
	}))

	total, nodeBytes, edgeBytes := manager.GetStorageSize("tenant_size")
	assert.Greater(t, total, int64(0))
	assert.Greater(t, nodeBytes, int64(0))
	assert.Greater(t, edgeBytes, int64(0))
	assert.Equal(t, total, nodeBytes+edgeBytes)

	base := storage.NewNamespacedEngine(inner, "tenant_size")
	wantNodeBytes, wantEdgeBytes, calcErr := manager.calculateStorageSizeFromEngine(base)
	require.NoError(t, calcErr)
	assert.Equal(t, wantNodeBytes+wantEdgeBytes, total)
	assert.Equal(t, wantNodeBytes, nodeBytes)
	assert.Equal(t, wantEdgeBytes, edgeBytes)
}

func TestDatabaseManager_GetStorageSize_IncrementalTrackingMatchesExactBytes(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	require.NoError(t, manager.CreateDatabase("tenant_delta"))
	store, err := manager.GetStorage("tenant_delta")
	require.NoError(t, err)

	// Seed cache at zero to force incremental path for later writes.
	total, nodeBytes, edgeBytes := manager.GetStorageSize("tenant_delta")
	assert.Zero(t, total)
	assert.Zero(t, nodeBytes)
	assert.Zero(t, edgeBytes)

	_, err = store.CreateNode(&storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name": "one",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     storage.NodeID("n2"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name": "two",
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:         storage.EdgeID("e1"),
		StartNode:  storage.NodeID("n1"),
		EndNode:    storage.NodeID("n2"),
		Type:       "REL",
		Properties: map[string]any{"weight": 1.5},
	}))

	// Update node to change byte footprint.
	require.NoError(t, store.UpdateNode(&storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name":  "one",
			"extra": "payload",
		},
	}))

	// Delete node with connected edge; cached edge bytes should also decrement.
	require.NoError(t, store.DeleteNode(storage.NodeID("n1")))

	gotTotal, gotNodeBytes, gotEdgeBytes := manager.GetStorageSize("tenant_delta")
	base := storage.NewNamespacedEngine(inner, "tenant_delta")
	wantNodeBytes, wantEdgeBytes, calcErr := manager.calculateStorageSizeFromEngine(base)
	require.NoError(t, calcErr)
	assert.Equal(t, wantNodeBytes+wantEdgeBytes, gotTotal)
	assert.Equal(t, wantNodeBytes, gotNodeBytes)
	assert.Equal(t, wantEdgeBytes, gotEdgeBytes)
}

func TestDatabaseManager_StorageSizeSnapshot_NoRaceWithListDatabases(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	require.NoError(t, manager.CreateDatabase("tenant_race"))
	store, err := manager.GetStorage("tenant_race")
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name": "one",
		},
	})
	require.NoError(t, err)

	stop := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// Force re-init writes on size cache while other goroutine snapshots metadata.
				manager.markStorageSizeDirty("tenant_race")
				manager.GetStorageSize("tenant_race")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				dbs := manager.ListDatabases()
				if len(dbs) == 0 {
					select {
					case errCh <- "expected non-empty database list":
					default:
					}
					return
				}
				if _, getErr := manager.GetDatabase("tenant_race"); getErr != nil {
					select {
					case errCh <- getErr.Error():
					default:
					}
					return
				}
			}
		}
	}()

	time.Sleep(75 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatalf("concurrent metadata read failed: %s", msg)
	default:
	}
}
