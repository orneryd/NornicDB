package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompositeDatabase_EndToEnd tests full end-to-end composite database functionality
func TestCompositeDatabase_EndToEnd(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Create constituent databases
	err := adapter.CreateDatabase("tenant_a")
	require.NoError(t, err)
	err = adapter.CreateDatabase("tenant_b")
	require.NoError(t, err)

	// Create composite database
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "tenant_a",
			"database_name": "tenant_a",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "tenant_b",
			"database_name": "tenant_b",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("analytics", constituents)
	require.NoError(t, err)

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage("analytics")
	require.NoError(t, err)

	// Create executor with composite storage
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create nodes in tenant_a (via composite)
	_, err = exec.Execute(ctx, `CREATE (a:Person {name: "Alice", tenant: "a"})`, nil)
	require.NoError(t, err)

	// Create nodes in tenant_b (via composite)
	_, err = exec.Execute(ctx, `CREATE (b:Person {name: "Bob", tenant: "b"})`, nil)
	require.NoError(t, err)

	// Query across both constituents
	result, err := exec.Execute(ctx, `MATCH (n:Person) RETURN n.name as name ORDER BY n.name`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])
	assert.Equal(t, "Bob", result.Rows[1][0])

	// Count across all constituents
	result, err = exec.Execute(ctx, `MATCH (n:Person) RETURN count(n) as total`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

// TestCompositeDatabase_ComplexQuery tests complex queries across constituents
func TestCompositeDatabase_ComplexQuery(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Create constituent databases
	err := adapter.CreateDatabase("db1")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "db1",
			"database_name": "db1",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "db2",
			"database_name": "db2",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("composite", constituents)
	require.NoError(t, err)

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage("composite")
	require.NoError(t, err)

	// Create executor
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create data in both constituents
	_, err = exec.Execute(ctx, `CREATE (a:Person {name: "Alice", age: 30})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Person {name: "Bob", age: 25})`, nil)
	require.NoError(t, err)

	// Complex query with WHERE and aggregation
	result, err := exec.Execute(ctx, `
		MATCH (n:Person)
		WHERE n.age > 20
		WITH n.age as age, count(n) as count
		RETURN age, count
		ORDER BY age
	`, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Rows), 1)

	// Query with WITH clause
	result, err = exec.Execute(ctx, `
		MATCH (n:Person)
		WITH n
		WHERE n.age > 20
		RETURN n.name as name
		ORDER BY name
	`, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

// TestCompositeDatabase_QueryWithRelationships tests queries with relationships across constituents
func TestCompositeDatabase_QueryWithRelationships(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Create constituent databases
	err := adapter.CreateDatabase("db1")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "db1",
			"database_name": "db1",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "db2",
			"database_name": "db2",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("composite", constituents)
	require.NoError(t, err)

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage("composite")
	require.NoError(t, err)

	// Create executor
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create nodes and relationships in a single statement
	// This ensures nodes are created in the same constituent for edge creation
	_, err = exec.Execute(ctx, `
		CREATE (a:Person {name: "Alice", tenant_id: "db1"})
		CREATE (b:Person {name: "Bob", tenant_id: "db1"})
		WITH a, b
		CREATE (a)-[:KNOWS {since: 2020}]->(b)
	`, nil)
	require.NoError(t, err)

	// Query with relationship pattern
	result, err := exec.Execute(ctx, `
		MATCH (a:Person)-[r:KNOWS]->(b:Person)
		RETURN a.name as from, b.name as to, r.since as since
	`, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

// TestCompositeDatabase_AlterCompositeDatabase tests ALTER COMPOSITE DATABASE commands
func TestCompositeDatabase_AlterCompositeDatabase(t *testing.T) {
	inner := storage.NewMemoryEngine()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Create constituent databases
	err := adapter.CreateDatabase("db1")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db2")
	require.NoError(t, err)
	err = adapter.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 2 constituents
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "db1",
			"database_name": "db1",
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         "db2",
			"database_name": "db2",
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase("composite", constituents)
	require.NoError(t, err)

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage("composite")
	require.NoError(t, err)

	// Create executor
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Add constituent using ALTER COMPOSITE DATABASE
	_, err = exec.Execute(ctx, `ALTER COMPOSITE DATABASE composite ADD ALIAS db3 FOR DATABASE db3`, nil)
	require.NoError(t, err)

	// Verify constituent was added
	constituentsList, err := adapter.GetCompositeConstituents("composite")
	require.NoError(t, err)
	assert.Equal(t, 3, len(constituentsList))

	// Remove constituent
	_, err = exec.Execute(ctx, `ALTER COMPOSITE DATABASE composite DROP ALIAS db3`, nil)
	require.NoError(t, err)

	// Verify constituent was removed
	constituentsList, err = adapter.GetCompositeConstituents("composite")
	require.NoError(t, err)
	assert.Equal(t, 2, len(constituentsList))
}
