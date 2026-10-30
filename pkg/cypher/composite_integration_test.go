package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompositeDatabase_EndToEnd tests full end-to-end composite database functionality.
// Neo4j requires USE <composite>.<alias> to target specific constituents; plain queries
// on composite root are rejected.
func TestCompositeDatabase_EndToEnd(t *testing.T) {
	inner := newTestMemoryEngine(t)
	defer inner.Close()
	manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Create constituent databases
	err := adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant_a"})
	require.NoError(t, err)
	err = adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant_b"})
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
	err = adapter.CreateCompositeDatabase(t.Context(), "analytics", constituents)
	require.NoError(t, err)

	// Plain queries on composite root should be rejected.
	compositeStorage, err := manager.GetStorage("analytics")
	require.NoError(t, err)
	compositeExec := NewStorageExecutor(compositeStorage)
	compositeExec.SetDatabaseManager(adapter)

	_, err = compositeExec.Execute(context.Background(),
		`CREATE (a:Person {name: "Alice"})`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "composite databases require explicit graph targeting")

	// Target a specific constituent to run data queries.
	tenantAStorage, err := manager.GetStorage("tenant_a")
	require.NoError(t, err)
	execA := NewStorageExecutor(tenantAStorage)

	tenantBStorage, err := manager.GetStorage("tenant_b")
	require.NoError(t, err)
	execB := NewStorageExecutor(tenantBStorage)

	ctx := context.Background()

	// Create nodes in tenant_a
	_, err = execA.Execute(ctx, `CREATE (a:Person {name: "Alice", tenant: "a"})`, nil)
	require.NoError(t, err)

	// Create nodes in tenant_b
	_, err = execB.Execute(ctx, `CREATE (b:Person {name: "Bob", tenant: "b"})`, nil)
	require.NoError(t, err)

	// Query tenant_a
	result, err := execA.Execute(ctx, `MATCH (n:Person) RETURN n.name as name`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])

	// Query tenant_b
	result, err = execB.Execute(ctx, `MATCH (n:Person) RETURN n.name as name`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Bob", result.Rows[0][0])
}

// TestCompositeDatabase_ComplexQuery tests complex queries on constituent databases.
// Neo4j requires USE <composite>.<alias> for data queries; here we target constituents directly.
func TestCompositeDatabase_ComplexQuery(t *testing.T) {
	inner := newTestMemoryEngine(t)
	defer inner.Close()
	manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Use unique database names for this test
	db1Name := "complex_db1"

	// Cleanup: drop databases if they exist
	_ = adapter.DropDatabase(t.Context(), db1Name)

	// Create constituent database
	err := adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db1Name})
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(t.Context(), db1Name)
	}()

	// Target constituent directly for data queries.
	db1Storage, err := manager.GetStorage(db1Name)
	require.NoError(t, err)
	exec := NewStorageExecutor(db1Storage)

	ctx := context.Background()

	// Create data in the constituent.
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
	require.Equal(t, 2, len(result.Rows))

	// Query with WITH clause
	result, err = exec.Execute(ctx, `
		MATCH (n:Person)
		WITH n
		WHERE n.age > 20
		RETURN n.name as name
		ORDER BY name
	`, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])
	assert.Equal(t, "Bob", result.Rows[1][0])
}

// TestCompositeDatabase_QueryWithRelationships tests queries with relationships on a constituent.
// Neo4j requires USE <composite>.<alias> for data queries; here we target the constituent directly.
func TestCompositeDatabase_QueryWithRelationships(t *testing.T) {
	inner := newTestMemoryEngine(t)
	defer inner.Close()
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	adapter := &testDatabaseManagerAdapter{manager: manager}

	db1Name := "rel_db1"

	// Cleanup: drop databases if they exist (from previous test runs)
	_ = adapter.DropDatabase(t.Context(), db1Name)

	t.Cleanup(func() {
		_ = adapter.DropDatabase(t.Context(), db1Name)
	})

	// Create constituent database
	err = adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db1Name})
	require.NoError(t, err)

	// Target constituent directly for data queries.
	db1Storage, err := manager.GetStorage(db1Name)
	require.NoError(t, err)

	exec := NewStorageExecutor(db1Storage)

	ctx := context.Background()

	// Create nodes and relationships in a single statement using WITH.
	createResult, err := exec.Execute(ctx, `
		CREATE (a:Person {name: "Alice"})
		CREATE (b:Person {name: "Bob"})
		WITH a, b
		CREATE (a)-[:KNOWS {since: 2020}]->(b)
	`, nil)
	require.NoError(t, err)
	t.Logf("CREATE result: %d nodes created, %d relationships created",
		createResult.Stats.NodesCreated, createResult.Stats.RelationshipsCreated)

	// Wait to ensure edge creation is fully persisted.
	time.Sleep(100 * time.Millisecond)

	// Verify nodes exist.
	nodesResult, err := exec.Execute(ctx, `MATCH (n:Person) RETURN n.name as name ORDER BY name`, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(nodesResult.Rows))
	assert.Equal(t, "Alice", nodesResult.Rows[0][0])
	assert.Equal(t, "Bob", nodesResult.Rows[1][0])

	// Query with relationship pattern.
	result, err := exec.Execute(ctx, `
		MATCH (a:Person)-[r:KNOWS]->(b:Person)
		RETURN a.name as from, b.name as to, r.since as since
	`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])
	assert.Equal(t, "Bob", result.Rows[0][1])
}

// TestCompositeDatabase_AlterCompositeDatabase tests ALTER COMPOSITE DATABASE commands
func TestCompositeDatabase_AlterCompositeDatabase(t *testing.T) {
	inner := newTestMemoryEngine(t)
	defer inner.Close()
	manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}

	// Use unique database names for this test
	db1Name := "alter_db1"
	db2Name := "alter_db2"
	db3Name := "alter_db3"
	compositeName := "alter_composite"

	// Cleanup: drop databases if they exist
	_ = adapter.DropCompositeDatabase(t.Context(), compositeName)
	_ = adapter.DropDatabase(t.Context(), db1Name)
	_ = adapter.DropDatabase(t.Context(), db2Name)
	_ = adapter.DropDatabase(t.Context(), db3Name)

	// Create constituent databases
	err := adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db1Name})
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(t.Context(), db1Name)
	}()

	err = adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db2Name})
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(t.Context(), db2Name)
	}()

	err = adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db3Name})
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropDatabase(t.Context(), db3Name)
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
	err = adapter.CreateCompositeDatabase(t.Context(), compositeName, constituents)
	require.NoError(t, err)
	defer func() {
		_ = adapter.DropCompositeDatabase(t.Context(), compositeName)
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
