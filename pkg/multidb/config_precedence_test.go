package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigurationPrecedence verifies that configuration precedence works correctly:
// CLI args > Env vars > Config file > Defaults
//
// Note: The actual precedence logic is in pkg/config, but this test verifies
// that DatabaseManager correctly uses the value that comes from the config system.
func TestConfigurationPrecedence(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	// Save original env
	originalEnv := os.Getenv("NORNICDB_DEFAULT_DATABASE")
	originalNeo4jEnv := os.Getenv("NEO4J_dbms_default__database")
	defer func() {
		if originalEnv != "" {
			os.Setenv("NORNICDB_DEFAULT_DATABASE", originalEnv)
		} else {
			os.Unsetenv("NORNICDB_DEFAULT_DATABASE")
		}
		if originalNeo4jEnv != "" {
			os.Setenv("NEO4J_dbms_default__database", originalNeo4jEnv)
		} else {
			os.Unsetenv("NEO4J_dbms_default__database")
		}
	}()

	t.Run("default value", func(t *testing.T) {
		// No env vars set - should use default
		os.Unsetenv("NORNICDB_DEFAULT_DATABASE")
		os.Unsetenv("NEO4J_dbms_default__database")

		config := DefaultConfig()
		assert.Equal(t, "nornic", config.DefaultDatabase)

		manager, err := NewDatabaseManager(t.Context(), inner, config)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, "nornic", manager.DefaultDatabaseName())
		assert.True(t, manager.Exists("nornic"))
	})

	t.Run("from NewConfigFromDefaultDatabase", func(t *testing.T) {
		// Use a fresh storage instance for this test
		testInner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		defer testInner.Close()

		// Simulate config file value
		configFileValue := "from-config-file"
		config := NewConfigFromDefaultDatabase(configFileValue)

		manager, err := NewDatabaseManager(t.Context(), testInner, config)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, configFileValue, manager.DefaultDatabaseName())
		assert.True(t, manager.Exists(configFileValue))
	})

	t.Run("env var NORNICDB_DEFAULT_DATABASE", func(t *testing.T) {
		// Use a fresh storage instance for this test
		testInner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		defer testInner.Close()

		// Set env var (simulating what pkg/config would do)
		os.Setenv("NORNICDB_DEFAULT_DATABASE", "env-database")
		defer os.Unsetenv("NORNICDB_DEFAULT_DATABASE")

		// Simulate config system reading env var and passing to DatabaseManager
		config := NewConfigFromDefaultDatabase("env-database")

		manager, err := NewDatabaseManager(t.Context(), testInner, config)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, "env-database", manager.DefaultDatabaseName())
	})

	t.Run("env var NEO4J_dbms_default__database (backwards compat)", func(t *testing.T) {
		// Use a fresh storage instance for this test
		testInner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		defer testInner.Close()

		// Set Neo4j-compatible env var
		os.Setenv("NEO4J_dbms_default__database", "neo4j-compat")
		defer os.Unsetenv("NEO4J_dbms_default__database")

		// Simulate config system reading Neo4j env var
		config := NewConfigFromDefaultDatabase("neo4j-compat")

		manager, err := NewDatabaseManager(t.Context(), testInner, config)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, "neo4j-compat", manager.DefaultDatabaseName())
	})
}

// TestBackwardsCompatibility verifies that existing code continues to work
// after multi-database support is added.
func TestBackwardsCompatibility(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	t.Run("default database works without specifying", func(t *testing.T) {
		// Old code that didn't specify database should still work
		manager, err := NewDatabaseManager(t.Context(), inner, nil)
		require.NoError(t, err)
		defer manager.Close()

		// Get default storage (old code path)
		store, err := manager.GetDefaultStorage()
		require.NoError(t, err)
		assert.NotNil(t, store)

		// Can still create nodes
		node := &storage.Node{
			ID:     storage.NodeID("legacy-node"),
			Labels: []string{"Legacy"},
		}
		_, err = store.CreateNode(node)
		require.NoError(t, err)
	})

	t.Run("legacy nornicdb name still works", func(t *testing.T) {
		// Use a fresh storage instance for this test
		testInner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		defer testInner.Close()

		// Support legacy "nornicdb" name
		config := NewConfigFromDefaultDatabase("nornicdb")
		manager, err := NewDatabaseManager(t.Context(), testInner, config)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, "nornicdb", manager.DefaultDatabaseName())
		assert.True(t, manager.Exists("nornicdb"))
	})

	t.Run("existing data in default namespace accessible", func(t *testing.T) {
		// Use a fresh storage instance for this test
		testInner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		defer testInner.Close()

		// Simulate existing data in "nornic" namespace
		manager, err := NewDatabaseManager(t.Context(), testInner, nil)
		require.NoError(t, err)
		defer manager.Close()

		// Create data in default database
		store, err := manager.GetStorage("nornic")
		require.NoError(t, err)

		node := &storage.Node{
			ID:     storage.NodeID("existing-node"),
			Labels: []string{"Existing"},
			Properties: map[string]any{
				"legacy": true,
			},
		}
		_, err = store.CreateNode(node)
		require.NoError(t, err)

		// Should be retrievable
		retrieved, err := store.GetNode(storage.NodeID("existing-node"))
		require.NoError(t, err)
		assert.True(t, retrieved.Properties["legacy"].(bool))
	})
}
