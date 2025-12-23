package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDatabaseManager_RejectsNamespacedInnerEngine(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := storage.NewNamespacedEngine(base, "nornic")
	_, err := NewDatabaseManager(inner, DefaultConfig())
	require.Error(t, err)
}

func TestDatabaseManager_DefaultDatabaseName_NeverSystem(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	manager, err := NewDatabaseManager(engine, &Config{
		DefaultDatabase: "system",
		SystemDatabase:  "system",
	})
	require.NoError(t, err)

	// Regression: UI discovery must never advertise system as the default DB,
	// otherwise browser queries like MATCH (n) will show system metadata nodes.
	require.Equal(t, "nornic", manager.DefaultDatabaseName())
	require.True(t, manager.Exists("system"))
	require.True(t, manager.Exists("nornic"))
}

func TestDatabaseManager_ConfigDefaults_AreApplied(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	manager, err := NewDatabaseManager(engine, &Config{
		// Intentionally empty to exercise defaults.
		DefaultDatabase: "",
		SystemDatabase:  "",
	})
	require.NoError(t, err)

	require.Equal(t, "nornic", manager.DefaultDatabaseName())
	require.True(t, manager.Exists("system"))
	require.True(t, manager.Exists("nornic"))
}

func TestDatabaseManager_CleanupLeakedSystemNodes_FromDefaultDatabase(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	// Simulate the historical bug: system metadata stored under the default namespace
	// (e.g., "nornic:system:databases:metadata"), which would appear as
	// "system:databases:metadata" when querying the default DB.
	_, err := engine.CreateNode(&storage.Node{
		ID:     storage.NodeID("nornic:system:databases:metadata"),
		Labels: []string{"_System", "_Metadata"},
		Properties: map[string]any{
			"type": "databases",
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     storage.NodeID("nornic:system:migration:legacy_data"),
		Labels: []string{"_System", "_Migration"},
		Properties: map[string]any{
			"status": "complete",
		},
	})
	require.NoError(t, err)

	manager, err := NewDatabaseManager(engine, &Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
	})
	require.NoError(t, err)

	defaultStorage, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	nodes, err := defaultStorage.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 0, "leaked system nodes should be removed from default database")

	systemStorage, err := manager.GetStorage("system")
	require.NoError(t, err)
	sysNodes, err := systemStorage.AllNodes()
	require.NoError(t, err)
	require.NotEmpty(t, sysNodes, "system database should still contain metadata nodes")
}
