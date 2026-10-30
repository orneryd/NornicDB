package multidb

import (
	"context"
	"encoding/json"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseManagerPersistentIdentity(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "identity_test"}))
	metadata := func() (*storage.Node, map[string]map[string]interface{}) {
		node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
		require.NoError(t, err)
		var databases map[string]map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(node.Properties["data"].(string)), &databases))
		return node, databases
	}
	node, databases := metadata()
	const uuidPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	serverID, ok := node.Properties["server_id"].(string)
	require.True(t, ok, "server identity must be persisted")
	require.Regexp(t, uuidPattern, serverID)
	databaseID, ok := databases["identity_test"]["id"].(string)
	require.True(t, ok, "database identity must be persisted")
	require.Regexp(t, uuidPattern, databaseID)
	info, err := manager.GetDatabase("identity_test")
	require.NoError(t, err)
	require.Equal(t, databaseID, info.ID, "database snapshots preserve identity")
	require.NotEqual(t, databaseID, databases["nornic"]["id"])
	require.NotEqual(t, databaseID, serverID)
	createdAt, err := time.Parse(time.RFC3339Nano, node.Properties["server_created_at"].(string))
	require.NoError(t, err)
	require.False(t, createdAt.IsZero())
	require.WithinDuration(t, node.CreatedAt, createdAt, time.Second)
	reloaded, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	nextNode, nextDatabases := metadata()
	require.Equal(t, serverID, nextNode.Properties["server_id"])
	require.Equal(t, node.Properties["server_created_at"], nextNode.Properties["server_created_at"])
	require.Equal(t, databaseID, nextDatabases["identity_test"]["id"])
	require.NoError(t, reloaded.DropDatabase(t.Context(), "identity_test"))
	require.NoError(t, reloaded.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "identity_test"}))
	nextNode, nextDatabases = metadata()
	require.Equal(t, serverID, nextNode.Properties["server_id"])
	require.NotEqual(t, databaseID, nextDatabases["identity_test"]["id"])
}

type identityReadOnlyEngine struct {
	storage.Engine
}

func (e *identityReadOnlyEngine) IsLeader() bool { return false }

func TestDatabaseManagerIdentityLegacyMetadata(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	engine := storage.NewNamespacedEngine(inner, "system")
	node, err := engine.GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	var databases map[string]map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(node.Properties["data"].(string)), &databases))
	for _, database := range databases {
		delete(database, "id")
	}
	data, err := json.Marshal(databases)
	require.NoError(t, err)
	node.Properties["data"] = string(data)
	delete(node.Properties, "server_id")
	delete(node.Properties, "server_created_at")
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: node}))
	standby, err := NewDatabaseManager(t.Context(), &identityReadOnlyEngine{Engine: inner}, nil)
	require.NoError(t, err)
	serverID, createdAt := standby.ServerIdentity()
	require.Empty(t, serverID)
	require.Equal(t, node.CreatedAt.UTC(), createdAt.UTC())
	unchanged, err := engine.GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	require.Equal(t, node.Properties, unchanged.Properties)
	upgraded, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	serverID, createdAt = upgraded.ServerIdentity()
	require.NotEmpty(t, serverID)
	require.Equal(t, node.CreatedAt.UTC(), createdAt.UTC(), "legacy creation time is preserved")
	for _, database := range manager.ListDatabases() {
		id, actualCreatedAt := upgraded.DatabaseIdentity(database.Name)
		require.NotEmpty(t, id)
		require.Equal(t, database.CreatedAt.UTC(), actualCreatedAt.UTC())
	}
	id, unknownCreatedAt := upgraded.DatabaseIdentity("missing")
	require.Empty(t, id)
	require.True(t, unknownCreatedAt.IsZero())
	reloaded, err := NewDatabaseManager(t.Context(), &identityReadOnlyEngine{Engine: inner}, nil)
	require.NoError(t, err)
	actualID, actualCreatedAt := reloaded.ServerIdentity()
	require.Equal(t, serverID, actualID)
	require.Equal(t, createdAt.UTC(), actualCreatedAt.UTC())
	node, err = engine.GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	node.Properties["server_created_at"] = "not-a-date"
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: node}))
	_, err = NewDatabaseManager(t.Context(), inner, nil)
	require.Error(t, err)
}

// TestDatabaseManager_ConfigCatalogFixture preserves the original independent
// default and custom constructor fixtures and every assertion each made: the
// nil default config yields the built-in nornic/system catalog, while the exact
// custom config yields customdb/system with its own default-database metadata.
func TestDatabaseManager_ConfigCatalogFixture(t *testing.T) {
	for _, test := range []struct {
		name      string
		config    *Config
		wantName  string
		wantDBs   []string
		assertion func(t *testing.T, manager *DatabaseManager)
	}{
		{
			name:     "TestDatabaseManager_DefaultConfig",
			config:   nil,
			wantName: "nornic",
			wantDBs:  []string{"system", "nornic"},
			assertion: func(t *testing.T, manager *DatabaseManager) {
				info, err := manager.GetDatabase("nornic")
				require.NoError(t, err)
				assert.True(t, info.IsDefault)
				assert.Equal(t, "standard", info.Type)
				assert.Equal(t, "online", info.Status)
			},
		},
		{
			name:     "TestDatabaseManager_CustomConfig",
			config:   &Config{DefaultDatabase: "customdb", SystemDatabase: "system", MaxDatabases: 10, AllowDropDefault: false},
			wantName: "customdb",
			wantDBs:  []string{"customdb", "system"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			defer inner.Close()

			manager, err := NewDatabaseManager(t.Context(), inner, test.config)
			require.NoError(t, err)
			defer manager.Close()

			assert.Equal(t, test.wantName, manager.DefaultDatabaseName())
			for _, database := range test.wantDBs {
				assert.True(t, manager.Exists(database), "expected database %q to exist", database)
			}
			if test.assertion != nil {
				test.assertion(t, manager)
			}
		})
	}
}

func TestDatabaseManager_CreateDatabase(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create a new database
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
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

	// Duplicate admission shares the same created database and must not replace it.
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseExists, err)

	// Create another database
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"})
	require.NoError(t, err)
	assert.True(t, manager.Exists("tenant_b"))
}

func TestDatabaseManagerDropClearsLimitChecker(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant"}))

	_, err = manager.GetStorage("tenant")
	require.NoError(t, err)
	first := manager.limitCheckers["tenant"]
	require.NotNil(t, first)
	require.NoError(t, manager.DropDatabase(t.Context(), "tenant"))
	require.NotContains(t, manager.limitCheckers, "tenant")

	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant"}))
	_, err = manager.GetStorage("tenant")
	require.NoError(t, err)
	require.NotSame(t, first, manager.limitCheckers["tenant"])
}

func TestResolveConnectionLimitOwner(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant"}))
	require.NoError(t, manager.CreateAlias(t.Context(), "tenant-alias", "tenant"))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "fabric", []ConstituentRef{{
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
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = inner.Close() })
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	limits := &Limits{Storage: StorageLimits{MaxNodes: 1}}
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "nornic", limits))
	limits.Storage.MaxNodes = 100

	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "first"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)

	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "nornic", &Limits{Storage: StorageLimits{MaxNodes: 2}}))
	_, err = engine.CreateNode(&storage.Node{ID: "second"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "still-blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
}

func TestDatabaseManagerStorageEnforcesMaxBytesUpdateDelta(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = inner.Close() })
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "node", Properties: map[string]any{"value": "small"}})
	require.NoError(t, err)
	currentBytes, _, _ := manager.GetStorageSize(context.Background(), "nornic")
	require.Positive(t, currentBytes)
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "nornic", &Limits{Storage: StorageLimits{MaxBytes: currentBytes + 32}}))

	err = engine.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "node", Properties: map[string]any{"value": string(make([]byte, 4096))}}})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)

	stored, err := engine.GetNode("node")
	require.NoError(t, err)
	require.Equal(t, "small", stored.Properties["value"])
}

func TestDatabaseManager_CreateDatabase_MaxLimit(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	config := &Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		MaxDatabases:    3, // system + nornic + 1 more
	}

	manager, err := NewDatabaseManager(t.Context(), inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Create one more (should succeed)
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)

	// Try to create another (should fail)
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"})
	assert.Error(t, err)
	assert.Equal(t, ErrMaxDatabasesReached, err)
}

func TestDatabaseManager_DropDatabase_Default(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("allowed=%t", allowed), func(t *testing.T) {
			inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			defer inner.Close()
			manager, err := NewDatabaseManager(t.Context(), inner, &Config{DefaultDatabase: "nornic", SystemDatabase: "system", AllowDropDefault: allowed})
			require.NoError(t, err)
			defer manager.Close()
			err = manager.DropDatabase(t.Context(), "nornic")
			if allowed {
				require.NoError(t, err)
				assert.False(t, manager.Exists("nornic"))
			} else {
				assert.Error(t, err)
				assert.Equal(t, ErrCannotDropDefaultDB, err)
			}
		})
	}
}

func TestDatabaseManager_DropDatabase_DeletesNodesAndEdges(t *testing.T) {
	cases := []struct {
		name  string
		nodes []*storage.Node
		edges []*storage.Edge
	}{
		{name: "tenant_a", nodes: []*storage.Node{{ID: "test-node", Labels: []string{"Test"}, Properties: map[string]any{"name": "test"}}}},
		{name: "test_db", nodes: []*storage.Node{
			{ID: "node1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice", "age": 30}},
			{ID: "node2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob", "age": 25}},
			{ID: "node3", Labels: []string{"Company"}, Properties: map[string]any{"name": "Acme Corp"}},
		}, edges: []*storage.Edge{
			{ID: "edge1", StartNode: "node1", EndNode: "node3", Type: "WORKS_FOR", Properties: map[string]any{"since": "2020-01-01"}},
			{ID: "edge2", StartNode: "node2", EndNode: "node3", Type: "WORKS_FOR", Properties: map[string]any{"since": "2021-01-01"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			defer inner.Close()
			manager, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			defer manager.Close()
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: tc.name}))
			store, err := manager.GetStorage(tc.name)
			require.NoError(t, err)
			for _, node := range tc.nodes {
				_, err = store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, edge := range tc.edges {
				require.NoError(t, store.CreateEdge(edge))
			}
			nodes, err := store.AllNodes()
			require.NoError(t, err)
			assert.Len(t, nodes, len(tc.nodes))
			edges, err := store.AllEdges()
			require.NoError(t, err)
			assert.Len(t, edges, len(tc.edges))
			for _, node := range tc.nodes {
				_, err = inner.GetNode(storage.NodeID(tc.name + ":" + string(node.ID)))
				require.NoError(t, err)
			}
			for _, edge := range tc.edges {
				_, err = inner.GetEdge(storage.EdgeID(tc.name + ":" + string(edge.ID)))
				require.NoError(t, err)
			}
			beforeNodes, err := inner.AllNodes()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(beforeNodes), len(tc.nodes))
			beforeEdges, err := inner.AllEdges()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(beforeEdges), len(tc.edges))
			require.NoError(t, manager.DropDatabase(t.Context(), tc.name))
			assert.False(t, manager.Exists(tc.name))
			for _, node := range tc.nodes {
				_, err = inner.GetNode(storage.NodeID(tc.name + ":" + string(node.ID)))
				assert.Error(t, err)
				assert.Equal(t, storage.ErrNotFound, err)
			}
			for _, edge := range tc.edges {
				_, err = inner.GetEdge(storage.EdgeID(tc.name + ":" + string(edge.ID)))
				assert.Error(t, err)
				assert.Equal(t, storage.ErrNotFound, err)
			}
			afterNodes, err := inner.AllNodes()
			require.NoError(t, err)
			assert.Len(t, afterNodes, len(beforeNodes)-len(tc.nodes))
			afterEdges, err := inner.AllEdges()
			require.NoError(t, err)
			assert.Len(t, afterEdges, len(beforeEdges)-len(tc.edges))
			_, err = manager.GetStorage(tc.name)
			assert.Error(t, err)
			assert.Equal(t, ErrDatabaseNotFound, err)
		})
	}
}

func TestDatabaseManager_NodeNamespacePrefix(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create a test database
	dbName := "test_db"
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: dbName})
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

func TestDatabaseManagerStorageRoutes(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
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

	t.Run("configured default route", func(t *testing.T) {
		storage, err := manager.GetDefaultStorage()
		require.NoError(t, err)
		assert.NotNil(t, storage)

		// GetDefaultStorage must resolve the configured default database name.
		assert.Equal(t, "nornic", manager.DefaultDatabaseName())

		// Should be the same as getting "nornic"
		storage2, err := manager.GetStorage("nornic")
		require.NoError(t, err)
		assert.Equal(t, storage, storage2)
	})

	t.Run("composite storage resolves aliases and is not cached as namespaced engine", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"}))
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"}))
		require.NoError(t, manager.CreateAlias(t.Context(), "tenant_b_alias", "tenant_b"))

		tenantAStore, err := manager.GetStorage("tenant_a")
		require.NoError(t, err)
		_, err = tenantAStore.CreateNode(&storage.Node{ID: "node-a", Labels: []string{"Shared"}})
		require.NoError(t, err)

		tenantBStore, err := manager.GetStorage("tenant_b")
		require.NoError(t, err)
		_, err = tenantBStore.CreateNode(&storage.Node{ID: "node-b", Labels: []string{"Shared"}})
		require.NoError(t, err)

		err = manager.CreateCompositeDatabase(t.Context(), "composite_db", []ConstituentRef{
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

func TestDatabaseManagerLazilyReconcilesStorageSizes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		restart  bool
		database string
		nodes    []*storage.Node
		edge     *storage.Edge
	}{
		{name: "restart-default-node", restart: true, nodes: []*storage.Node{{ID: "persisted", Labels: []string{"Record"}, Properties: map[string]any{"name": "alpha"}}}},
		{name: "live-tenant-graph", database: "tenant_size", nodes: []*storage.Node{{ID: "a", Labels: []string{"Person"}, Properties: map[string]any{"name": "alice", "age": int64(42)}}, {ID: "b", Labels: []string{"Person"}, Properties: map[string]any{"name": "bob"}}}, edge: &storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			seed, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = seed.Close() })
			database := tc.database
			if database == "" {
				database = seed.DefaultDatabaseName()
			} else {
				require.NoError(t, seed.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: database}))
			}
			store, err := seed.GetStorage(database)
			require.NoError(t, err)
			for _, node := range tc.nodes {
				_, err = store.CreateNode(node)
				require.NoError(t, err)
			}
			if tc.edge != nil {
				require.NoError(t, store.CreateEdge(tc.edge))
			}
			manager := seed
			var wantSingle int64
			if tc.restart {
				manager, err = NewDatabaseManager(t.Context(), inner, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				restartedStorage, err := manager.GetStorage(database)
				require.NoError(t, err)
				storedNode, err := restartedStorage.GetNode("persisted")
				require.NoError(t, err)
				wantSingle, err = calculateNodeSize(storedNode)
				require.NoError(t, err)
			}
			manager.mu.RLock()
			info := manager.databases[database]
			manager.mu.RUnlock()
			require.NotNil(t, info)
			info = info.accountingOwner()
			info.sizeMu.RLock()
			initialized := info.sizeInitialized
			info.sizeMu.RUnlock()
			require.False(t, initialized)
			total, nodeBytes, edgeBytes := manager.GetStorageSize(context.Background(), database)
			wantNodes, wantEdges, calcErr := manager.calculateStorageSizeFromEngine(context.Background(), storage.NewNamespacedEngine(inner, database))
			require.NoError(t, calcErr)
			assert.Equal(t, wantNodes+wantEdges, total)
			assert.Equal(t, wantNodes, nodeBytes)
			assert.Equal(t, wantEdges, edgeBytes)
			if tc.restart {
				require.Equal(t, wantSingle, total)
				require.Equal(t, wantSingle, nodeBytes)
				require.Zero(t, edgeBytes)
			} else {
				assert.Greater(t, total, int64(0))
				assert.Greater(t, nodeBytes, int64(0))
				assert.Greater(t, edgeBytes, int64(0))
				assert.Equal(t, total, nodeBytes+edgeBytes)
			}
			info.sizeMu.RLock()
			initialized = info.sizeInitialized
			cachedNodes, cachedTotal := info.nodeSize, info.totalSize
			info.sizeMu.RUnlock()
			require.True(t, initialized)
			require.Equal(t, nodeBytes, cachedNodes)
			require.Equal(t, total, cachedTotal)
		})
	}
}

func TestDatabaseManager_GetStorage_Isolation(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create two databases
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"})
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
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Create some databases
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"})
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
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)

	// Reject invalid transitions before checking the valid offline/online cycle.
	err = manager.SetDatabaseStatus(t.Context(), "tenant_a", "invalid")
	assert.Error(t, err)

	// Set to offline
	err = manager.SetDatabaseStatus(t.Context(), "tenant_a", "offline")
	require.NoError(t, err)

	info, err := manager.GetDatabase("tenant_a")
	require.NoError(t, err)
	assert.Equal(t, "offline", info.Status)

	// Try to get storage for offline database
	_, err = manager.GetStorage("tenant_a")
	assert.Error(t, err)
	assert.Equal(t, ErrDatabaseOffline, err)

	// Set back to online
	err = manager.SetDatabaseStatus(t.Context(), "tenant_a", "online")
	require.NoError(t, err)

	// Should be able to get storage now
	_, err = manager.GetStorage("tenant_a")
	require.NoError(t, err)
}

func TestDatabaseManager_InternalStorageAndAliasHelpers(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	t.Run("getStorageInternal handles cache not found and offline", func(t *testing.T) {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"}))
		manager.mu.Lock()
		engine1, _, err := manager.databaseViewLocked("tenant_a", true)
		require.NoError(t, err)
		engine2, _, err := manager.databaseViewLocked("tenant_a", true)
		require.NoError(t, err)
		manager.mu.Unlock()
		assert.Same(t, engine1, engine2)

		manager.mu.Lock()
		_, _, err = manager.databaseViewLocked("missing", true)
		require.ErrorIs(t, err, ErrDatabaseNotFound)
		manager.databases["tenant_a"].Status = "offline"
		delete(manager.engines, "tenant_a")
		_, _, err = manager.databaseViewLocked("tenant_a", true)
		require.ErrorIs(t, err, ErrDatabaseOffline)
		manager.mu.Unlock()
	})

	t.Run("default database name and alias validation branches", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DefaultDatabase = cfg.SystemDatabase
		manager2, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
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
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"}))
		require.NoError(t, manager.CreateAlias(t.Context(), "alias_a", "nornic"))
		require.NoError(t, manager.CreateAlias(t.Context(), "alias_b", "tenant_b"))

		allAliases := manager.ListAliases("")
		assert.Equal(t, "nornic", allAliases["alias_a"])
		assert.Equal(t, "tenant_b", allAliases["alias_b"])
		assert.Empty(t, manager.ListAliases("missing"))

		manager.IncrementStorageSize("tenant_b", 100, 50)
		manager.DecrementStorageSize("tenant_b", 200, 100)
		total, nodeSize, edgeSize := manager.GetStorageSize(context.Background(), "tenant_b")
		assert.Zero(t, total)
		assert.Zero(t, nodeSize)
		assert.Zero(t, edgeSize)

		manager.DecrementStorageSize("missing", 10, 10)
	})
}

func TestDatabaseManager_MetadataPersistence(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Create first manager
	manager1, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)

	err = manager1.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)
	err = manager1.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_b"})
	require.NoError(t, err)

	// Don't close the manager - just create a new one with same storage
	// (In real usage, the storage would persist to disk, but for memory engine
	// we just reuse the same instance)
	manager2, err := NewDatabaseManager(t.Context(), inner, nil)
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
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Create manager with default config (simulates old behavior)
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
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

func TestDatabaseManager_GetStorageSize_IncrementalTrackingMatchesExactBytes(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_delta"}))
	store, err := manager.GetStorage("tenant_delta")
	require.NoError(t, err)

	// Seed cache at zero to force incremental path for later writes.
	total, nodeBytes, edgeBytes := manager.GetStorageSize(context.Background(), "tenant_delta")
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
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{
		ID:     storage.NodeID("n1"),
		Labels: []string{"Item"},
		Properties: map[string]any{
			"name":  "one",
			"extra": "payload",
		},
	}}))

	// Delete node with connected edge; cached edge bytes should also decrement.
	require.NoError(t, store.DeleteNode(storage.NodeID("n1")))

	gotTotal, gotNodeBytes, gotEdgeBytes := manager.GetStorageSize(context.Background(), "tenant_delta")
	base := storage.NewNamespacedEngine(inner, "tenant_delta")
	wantNodeBytes, wantEdgeBytes, calcErr := manager.calculateStorageSizeFromEngine(context.Background(), base)
	require.NoError(t, calcErr)
	assert.Equal(t, wantNodeBytes+wantEdgeBytes, gotTotal)
	assert.Equal(t, wantNodeBytes, gotNodeBytes)
	assert.Equal(t, wantEdgeBytes, gotEdgeBytes)
}

func TestDatabaseManager_StorageSizeSnapshot_NoRaceWithListDatabases(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant_race"}))
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
				manager.GetStorageSize(context.Background(), "tenant_race")
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

// TestDatabaseManagerRejectedCatalogRequests preserves the original native
// error identities and isolated manager fixture for each rejected operation.
func TestDatabaseManagerRejectedCatalogRequests(t *testing.T) {
	for _, test := range []struct {
		name, operation, database string
		want                      error
	}{
		{"TestDatabaseManager_CreateDatabase_InvalidName", "create", "", ErrInvalidDatabaseName},
		{"TestDatabaseManager_DropDatabase_NotFound", "drop", "nonexistent", ErrDatabaseNotFound},
		{"TestDatabaseManager_DropDatabase_System", "drop", "system", ErrCannotDropSystemDB},
		{"TestDatabaseManager_GetStorage_NotFound", "storage", "nonexistent", ErrDatabaseNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			defer inner.Close()
			manager, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			defer manager.Close()
			switch test.operation {
			case "create":
				err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: test.database})
			case "drop":
				err = manager.DropDatabase(t.Context(), test.database)
			case "storage":
				_, err = manager.GetStorage(test.database)
			}
			assert.Error(t, err)
			assert.Equal(t, test.want, err)
		})
	}
}
