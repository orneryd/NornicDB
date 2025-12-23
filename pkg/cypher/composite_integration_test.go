package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompositeDatabase_EndToEnd tests full end-to-end composite database functionality
func TestCompositeDatabase_EndToEnd(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()
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
	defer inner.Close()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Use unique database names for this test
	db1Name := "complex_db1"
	db2Name := "complex_db2"
	compositeName := "complex_composite"

	// Cleanup: drop databases if they exist
	_ = adapter.DropCompositeDatabase(compositeName)
	_ = adapter.DropDatabase(db1Name)
	_ = adapter.DropDatabase(db2Name)

	// Create constituent databases
	err := adapter.CreateDatabase(db1Name)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(db1Name)
	}()

	err = adapter.CreateDatabase(db2Name)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(db2Name)
	}()

	// Create composite database
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         db1Name,
			"database_name": db1Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         db2Name,
			"database_name": db2Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase(compositeName, constituents)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropCompositeDatabase(compositeName)
	}()

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage(compositeName)
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
	defer inner.Close()
	manager, err := multidb.NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Use unique database names for this test to avoid conflicts
	// Include timestamp to ensure uniqueness even if cleanup fails
	db1Name := "rel_db1"
	db2Name := "rel_db2"
	compositeName := "rel_composite"

	// Cleanup: drop databases if they exist (from previous test runs)
	_ = adapter.DropCompositeDatabase(compositeName)
	_ = adapter.DropDatabase(db1Name)
	_ = adapter.DropDatabase(db2Name)

	// Ensure cleanup happens even if test fails
	t.Cleanup(func() {
		_ = adapter.DropCompositeDatabase(compositeName)
		_ = adapter.DropDatabase(db1Name)
		_ = adapter.DropDatabase(db2Name)
	})

	// Create constituent databases
	err = adapter.CreateDatabase(db1Name)
	require.NoError(t, err)

	err = adapter.CreateDatabase(db2Name)
	require.NoError(t, err)

	// Create composite database
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         db1Name,
			"database_name": db1Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         db2Name,
			"database_name": db2Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase(compositeName, constituents)
	require.NoError(t, err)

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage(compositeName)
	require.NoError(t, err)

	// Create executor
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Create nodes and relationships in a single statement using WITH
	// This ensures nodes are in context and passed directly to edge creation
	// Wait 50ms after to ensure everything is persisted
	createResult, err := exec.Execute(ctx, `
		CREATE (a:Person {name: "Alice", database_id: "`+db1Name+`"})
		CREATE (b:Person {name: "Bob", database_id: "`+db1Name+`"})
		WITH a, b
		CREATE (a)-[:KNOWS {since: 2020}]->(b)
	`, nil)
	require.NoError(t, err)
	t.Logf("CREATE result: %d nodes created, %d relationships created",
		createResult.Stats.NodesCreated, createResult.Stats.RelationshipsCreated)

	// Wait to ensure edge creation is fully persisted (timing test with consistent keys)
	time.Sleep(100 * time.Millisecond)

	// First verify nodes exist and check their IDs
	nodesResult, err := exec.Execute(ctx, `MATCH (n:Person) RETURN n.name as name, id(n) as nodeId`, nil)
	require.NoError(t, err)
	t.Logf("Found %d Person nodes", len(nodesResult.Rows))
	for _, row := range nodesResult.Rows {
		t.Logf("  Node: %v, ID: %v", row[0], row[1])
	}

	// Check edges directly for Alice node
	if len(nodesResult.Rows) > 0 {
		aliceID := nodesResult.Rows[0][1]
		// Try to get edges for Alice
		aliceNode, _ := compositeStorage.GetNode(storage.NodeID(aliceID.(string)))
		if aliceNode != nil {
			outEdges, _ := compositeStorage.GetOutgoingEdges(aliceNode.ID)
			t.Logf("Alice node ID: %s, outgoing edges: %d", aliceNode.ID, len(outEdges))
			for _, edge := range outEdges {
				t.Logf("  Edge: %s -> %s (type: %s)", edge.StartNode, edge.EndNode, edge.Type)
			}
		}
	}

	// Query with relationship pattern
	result, err := exec.Execute(ctx, `
		MATCH (a:Person)-[r:KNOWS]->(b:Person)
		RETURN a.name as from, b.name as to, r.since as since
	`, nil)
	require.NoError(t, err)
	t.Logf("Found %d relationship rows", len(result.Rows))
	if len(result.Rows) == 0 {
		// Debug: check if edges exist at all
		edgesResult, _ := exec.Execute(ctx, `MATCH ()-[r:KNOWS]->() RETURN count(r) as count`, nil)
		if edgesResult != nil && len(edgesResult.Rows) > 0 {
			t.Logf("Total KNOWS edges: %v", edgesResult.Rows[0][0])
		}
	}
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

// TestCompositeDatabase_AlterCompositeDatabase tests ALTER COMPOSITE DATABASE commands
func TestCompositeDatabase_AlterCompositeDatabase(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()
	manager, _ := multidb.NewDatabaseManager(inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Use unique database names for this test
	db1Name := "alter_db1"
	db2Name := "alter_db2"
	db3Name := "alter_db3"
	compositeName := "alter_composite"

	// Cleanup: drop databases if they exist
	_ = adapter.DropCompositeDatabase(compositeName)
	_ = adapter.DropDatabase(db1Name)
	_ = adapter.DropDatabase(db2Name)
	_ = adapter.DropDatabase(db3Name)

	// Create constituent databases
	err := adapter.CreateDatabase(db1Name)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(db1Name)
	}()

	err = adapter.CreateDatabase(db2Name)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(db2Name)
	}()

	err = adapter.CreateDatabase(db3Name)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(db3Name)
	}()

	// Create composite database with 2 constituents
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         db1Name,
			"database_name": db1Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
		map[string]interface{}{
			"alias":         db2Name,
			"database_name": db2Name,
			"type":          "local",
			"access_mode":   "read_write",
		},
	}
	err = adapter.CreateCompositeDatabase(compositeName, constituents)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropCompositeDatabase(compositeName)
	}()

	// Get storage for composite database
	compositeStorage, err := manager.GetStorage(compositeName)
	require.NoError(t, err)

	// Create executor
	exec := NewStorageExecutor(compositeStorage)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Add constituent using ALTER COMPOSITE DATABASE
	_, err = exec.Execute(ctx, `ALTER COMPOSITE DATABASE `+compositeName+` ADD ALIAS `+db3Name+` FOR DATABASE `+db3Name, nil)
	require.NoError(t, err)

	// Verify constituent was added
	constituentsList, err := adapter.GetCompositeConstituents(compositeName)
	require.NoError(t, err)
	assert.Equal(t, 3, len(constituentsList))

	// Remove constituent
	_, err = exec.Execute(ctx, `ALTER COMPOSITE DATABASE `+compositeName+` DROP ALIAS `+db3Name, nil)
	require.NoError(t, err)

	// Verify constituent was removed
	constituentsList, err = adapter.GetCompositeConstituents(compositeName)
	require.NoError(t, err)
	assert.Equal(t, 2, len(constituentsList))
}
