package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackwardsCompatibility_ExistingCode verifies that existing code
// that doesn't specify a database continues to work after multi-database
// support is added.
func TestBackwardsCompatibility_ExistingCode(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Simulate old code: create manager with defaults
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Old code path: get default storage (no database specified)
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Old code: create nodes (no database awareness)
	node := &storage.Node{
		ID:     storage.NodeID("legacy-node-1"),
		Labels: []string{"Legacy"},
		Properties: map[string]any{
			"created_by": "old-code",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	// Old code: query nodes
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, "legacy-node-1", string(nodes[0].ID))

	// Old code: create edges
	node2 := &storage.Node{
		ID:     storage.NodeID("legacy-node-2"),
		Labels: []string{"Legacy"},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:        storage.EdgeID("legacy-edge-1"),
		StartNode: storage.NodeID("legacy-node-1"),
		EndNode:   storage.NodeID("legacy-node-2"),
		Type:      "RELATES_TO",
	}
	err = store.CreateEdge(edge)
	require.NoError(t, err)

	// Verify edge exists
	retrieved, err := store.GetEdge(storage.EdgeID("legacy-edge-1"))
	require.NoError(t, err)
	assert.Equal(t, "RELATES_TO", retrieved.Type)
}

// TestBackwardsCompatibility_LegacyDatabaseName verifies that the
// legacy "nornicdb" database name still works if configured.
func TestBackwardsCompatibility_LegacyDatabaseName(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Support legacy "nornicdb" name via config
	config := NewConfigFromDefaultDatabase("nornicdb")
	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()

	// Should work with legacy name
	assert.Equal(t, "nornicdb", manager.DefaultDatabaseName())
	assert.True(t, manager.Exists("nornicdb"))

	// Can use legacy database
	store, err := manager.GetStorage("nornicdb")
	require.NoError(t, err)

	node := &storage.Node{
		ID:     storage.NodeID("legacy"),
		Labels: []string{"Test"},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)
}

// TestBackwardsCompatibility_NoBreakingChanges verifies that
// the Engine interface hasn't changed in a breaking way.
func TestBackwardsCompatibility_NoBreakingChanges(t *testing.T) {
	// This test ensures that all existing Engine methods still work
	// and that we haven't introduced breaking changes.

	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Create namespaced engine (new functionality)
	namespaced := storage.NewNamespacedEngine(inner, "test")

	// All existing Engine methods should work
	node := &storage.Node{
		ID:     storage.NodeID("test"),
		Labels: []string{"Test"},
	}

	// CreateNode - should work
	_, err := namespaced.CreateNode(node)
	require.NoError(t, err)

	// GetNode - should work
	retrieved, err := namespaced.GetNode(storage.NodeID("test"))
	require.NoError(t, err)
	assert.NotNil(t, retrieved)

	// AllNodes - should work
	nodes, err := namespaced.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 1)

	// NodeCount - should work
	count, err := namespaced.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// DeleteByPrefix - namespace-scoped deletion now works through the engine.
	_, err = namespaced.CreateNode(&storage.Node{ID: "del:temp", Labels: []string{"Test"}})
	require.NoError(t, err)
	deletedNodes, _, err := namespaced.DeleteByPrefix("del:")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedNodes)
	_, err = namespaced.GetNode("del:temp")
	assert.Error(t, err) // Scoped record is gone.
	retrieved, err = namespaced.GetNode(storage.NodeID("test"))
	require.NoError(t, err)
	assert.NotNil(t, retrieved) // Out-of-scope record survives.
}

// TestBackwardsCompatibility_ConfigurationPrecedence verifies that
// configuration precedence matches the existing pattern.
func TestBackwardsCompatibility_ConfigurationPrecedence(t *testing.T) {
	// Configuration precedence should be:
	// CLI args > Env vars > Config file > Defaults

	// Test 1: Default value
	config := DefaultConfig()
	assert.Equal(t, "nornic", config.DefaultDatabase)

	// Test 2: Config file value (simulated)
	configFileValue := "from-config-file"
	config = NewConfigFromDefaultDatabase(configFileValue)
	assert.Equal(t, configFileValue, config.DefaultDatabase)

	// Test 3: Env var would override (tested in config_precedence_test.go)
	// The actual env var parsing is in pkg/config, but we verify
	// that NewConfigFromDefaultDatabase accepts the value correctly
}
