// Package cypher - Comprehensive E2E tests for all Cypher query syntax.
//
// These tests cover every type of Cypher query syntax supported by the StorageExecutor.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// TEST HELPERS
// =============================================================================

func setupE2EExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	return exec, ctx
}

func setupE2ETestData(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	// Create test nodes
	queries := []string{
		"CREATE (a:Person {name: 'Alice', age: 30, city: 'NYC'})",
		"CREATE (b:Person {name: 'Bob', age: 25, city: 'LA'})",
		"CREATE (c:Person {name: 'Charlie', age: 35, city: 'NYC'})",
		"CREATE (d:Person {name: 'Diana', age: 28, city: 'Chicago'})",
		"CREATE (e:Company {name: 'Acme', size: 100})",
		"CREATE (f:Company {name: 'TechCorp', size: 500})",
		"CREATE (g:Product {name: 'Widget', price: 9.99})",
		"CREATE (h:Product {name: 'Gadget', price: 19.99})",
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Setup query failed: %s", q)
	}
}

// =============================================================================
// BASIC MATCH QUERIES
// =============================================================================

func TestE2E_Match_AllNodes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 8, len(result.Rows), "Should match all 8 nodes")
}

func TestE2E_Match_ByLabel(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(result.Rows), "Should match 4 Person nodes")
}

func TestE2E_Match_ByMultipleLabels(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create a node with multiple labels
	_, err := exec.Execute(ctx, "CREATE (n:Person:Employee {name: 'Eve'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Person:Employee) RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Eve", result.Rows[0][0])
}

func TestE2E_Match_ByProperty(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'Alice'}) RETURN n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, int64(30), result.Rows[0][0])
}

func TestE2E_Match_ByMultipleProperties(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person {city: 'NYC', age: 30}) RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])
}

// =============================================================================
// WHERE CLAUSE TESTS
// =============================================================================

func TestE2E_Where_Equals(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, int64(25), result.Rows[0][0])
}

func TestE2E_Where_NotEquals(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.city <> 'NYC' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Bob and Diana are not in NYC")
}

func TestE2E_Where_LessThan(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age < 30 RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Bob (25) and Diana (28)")
}

func TestE2E_Where_LessThanOrEqual(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age <= 30 RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows), "Alice (30), Bob (25), Diana (28)")
}

func TestE2E_Where_GreaterThan(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age > 30 RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Charlie", result.Rows[0][0])
}

func TestE2E_Where_GreaterThanOrEqual(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Alice (30), Charlie (35)")
}

func TestE2E_Where_AND(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.city = 'NYC' AND n.age > 25 RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Alice (30) and Charlie (35) in NYC")
}

func TestE2E_Where_OR(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestE2E_Where_NOT(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE NOT n.city = 'NYC' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Bob (LA) and Diana (Chicago)")
}

func TestE2E_Where_IsNull(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create node without email property
	exec.Execute(ctx, "CREATE (n:Person {name: 'NoEmail'})", nil)
	exec.Execute(ctx, "CREATE (n:Person {name: 'HasEmail', email: 'test@test.com'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.email IS NULL RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "NoEmail", result.Rows[0][0])
}

func TestE2E_Where_IsNotNull(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	exec.Execute(ctx, "CREATE (n:Person {name: 'NoEmail'})", nil)
	exec.Execute(ctx, "CREATE (n:Person {name: 'HasEmail', email: 'test@test.com'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "HasEmail", result.Rows[0][0])
}

func TestE2E_Where_Contains(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Alice and Charlie contain 'li'")
}

func TestE2E_Where_StartsWith(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0][0])
}

func TestE2E_Where_EndsWith(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Alice and Charlie end with 'e'")
}

func TestE2E_Where_IN_List(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob', 'Eve'] RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows), "Alice and Bob exist")
}

// =============================================================================
// CREATE TESTS
// =============================================================================

func TestE2E_Create_SimpleNode(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (n:Test)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestE2E_Create_NodeWithProperties(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Test', age: 99, active: true})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'Test'}) RETURN n.age, n.active", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, int64(99), result.Rows[0][0])
	assert.Equal(t, true, result.Rows[0][1])
}

func TestE2E_Create_MultipleNodes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	_, err := exec.Execute(ctx, "CREATE (a:Person {name: 'A'}), (b:Person {name: 'B'})", nil)
	require.NoError(t, err)
	// Note: Current implementation creates nodes sequentially via pattern parts
	// This tests the multi-pattern CREATE syntax
}

func TestE2E_Create_NodeWithRelationship(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestE2E_Create_ChainedRelationships(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})-[:KNOWS]->(c:Person {name: 'C'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Stats.NodesCreated)
	assert.Equal(t, 2, result.Stats.RelationshipsCreated)
}

func TestE2E_Create_RelationshipWithProperties(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS {since: 2020}]->(b:Person {name: 'B'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestE2E_Create_ReverseRelationship(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'A'})<-[:KNOWS]-(b:Person {name: 'B'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

// =============================================================================
// MERGE TESTS
// =============================================================================

func TestE2E_Merge_CreatesWhenNotExists(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'NewPerson'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify it exists
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'NewPerson'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
}

func TestE2E_Merge_MatchesWhenExists(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create first
	exec.Execute(ctx, "CREATE (n:Person {name: 'Existing'})", nil)

	// Merge should not create
	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'Existing'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesCreated)

	// Only one should exist
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'Existing'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
}

func TestE2E_Merge_OnCreate(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'OnCreateTest'}) ON CREATE SET n.created = true", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify ON CREATE ran
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'OnCreateTest'}) RETURN n.created", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, true, result.Rows[0][0])
}

func TestE2E_Merge_OnMatch(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create first
	exec.Execute(ctx, "CREATE (n:Person {name: 'OnMatchTest', updated: false})", nil)

	// Merge with ON MATCH
	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'OnMatchTest'}) ON MATCH SET n.updated = true", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesCreated) // Already exists

	// Verify ON MATCH ran
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'OnMatchTest'}) RETURN n.updated", nil)
	require.NoError(t, err)
	assert.Equal(t, true, result.Rows[0][0])
}

// =============================================================================
// DELETE TESTS
// =============================================================================

func TestE2E_Delete_SingleNode(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:ToDelete {name: 'Delete Me'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:ToDelete) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify deleted
	result, err = exec.Execute(ctx, "MATCH (n:ToDelete) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Rows))
}

func TestE2E_Delete_MultipleNodes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:ToDelete {id: 1})", nil)
	exec.Execute(ctx, "CREATE (n:ToDelete {id: 2})", nil)
	exec.Execute(ctx, "CREATE (n:ToDelete {id: 3})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:ToDelete) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Stats.NodesDeleted)
}

func TestE2E_DetachDelete(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create nodes with relationship
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)

	// DETACH DELETE removes node and its relationships
	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'A'}) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, 1, result.Stats.RelationshipsDeleted)
}

// =============================================================================
// SET TESTS
// =============================================================================

func TestE2E_Set_SingleProperty(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'SetTest'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'SetTest'}) SET n.age = 25", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)

	// Verify
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'SetTest'}) RETURN n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(25), result.Rows[0][0])
}

func TestE2E_Set_MultipleProperties(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'MultiSet'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'MultiSet'}) SET n.age = 30, n.city = 'NYC'", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.PropertiesSet)
}

func TestE2E_Set_OverwriteProperty(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'Overwrite', age: 20})", nil)

	exec.Execute(ctx, "MATCH (n:Person {name: 'Overwrite'}) SET n.age = 99", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'Overwrite'}) RETURN n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(99), result.Rows[0][0])
}

// =============================================================================
// REMOVE TESTS
// =============================================================================

func TestE2E_Remove_Property(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'RemoveTest', age: 25, city: 'NYC'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person {name: 'RemoveTest'}) REMOVE n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet) // Property removal counts as set

	// Verify property is gone (returns null)
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'RemoveTest'}) RETURN n.age", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

// =============================================================================
// RETURN CLAUSE TESTS
// =============================================================================

func TestE2E_Return_SingleColumn(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"n.name"}, result.Columns)
	assert.Equal(t, 4, len(result.Rows))
}

func TestE2E_Return_MultipleColumns(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age, n.city", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Columns))
	assert.Equal(t, 4, len(result.Rows))
}

func TestE2E_Return_WithAlias(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name AS personName", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"personName"}, result.Columns)
}

func TestE2E_Return_Star(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {a: 1, b: 2})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN *", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
}

func TestE2E_Return_Distinct(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN DISTINCT n.city", nil)
	require.NoError(t, err)
	// Should be 3 distinct cities: NYC, LA, Chicago
	assert.Equal(t, 3, len(result.Rows))
}

func TestE2E_Return_Literal_String(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 'hello' AS greeting", nil)
	require.NoError(t, err)
	assert.Equal(t, "hello", result.Rows[0][0])
}

func TestE2E_Return_Literal_Number(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 42 AS answer", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.Rows[0][0])
}

func TestE2E_Return_Literal_Boolean(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN true AS flag", nil)
	require.NoError(t, err)
	assert.Equal(t, true, result.Rows[0][0])
}

func TestE2E_Return_Null(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN null AS nothing", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

// =============================================================================
// AGGREGATION TESTS
// =============================================================================

func TestE2E_Aggregation_Count(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN COUNT(n) AS total", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.Rows[0][0])
}

func TestE2E_Aggregation_CountStar(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN COUNT(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.Rows[0][0])
}

func TestE2E_Aggregation_Sum(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN SUM(n.age) AS totalAge", nil)
	require.NoError(t, err)
	// 30 + 25 + 35 + 28 = 118
	assert.Equal(t, int64(118), result.Rows[0][0])
}

func TestE2E_Aggregation_Avg(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN AVG(n.age) AS avgAge", nil)
	require.NoError(t, err)
	// (30 + 25 + 35 + 28) / 4 = 29.5
	assert.Equal(t, 29.5, result.Rows[0][0])
}

func TestE2E_Aggregation_Min(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN MIN(n.age) AS minAge", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(25), result.Rows[0][0])
}

func TestE2E_Aggregation_Max(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN MAX(n.age) AS maxAge", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(35), result.Rows[0][0])
}

func TestE2E_Aggregation_Collect(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN COLLECT(n.name) AS names", nil)
	require.NoError(t, err)
	names := result.Rows[0][0].([]interface{})
	assert.Equal(t, 4, len(names))
}

func TestE2E_Aggregation_GroupBy(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.city, COUNT(n) AS count", nil)
	require.NoError(t, err)
	// NYC: 2, LA: 1, Chicago: 1
	assert.Equal(t, 3, len(result.Rows))
}

// =============================================================================
// ORDER BY TESTS
// =============================================================================

func TestE2E_OrderBy_Ascending(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(result.Rows))
	assert.Equal(t, "Bob", result.Rows[0][0])     // 25
	assert.Equal(t, "Diana", result.Rows[1][0])   // 28
	assert.Equal(t, "Alice", result.Rows[2][0])   // 30
	assert.Equal(t, "Charlie", result.Rows[3][0]) // 35
}

func TestE2E_OrderBy_Descending(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC", nil)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", result.Rows[0][0]) // 35
	assert.Equal(t, "Alice", result.Rows[1][0])   // 30
}

func TestE2E_OrderBy_MultipleFields(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.city, n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(result.Rows))
}

// =============================================================================
// SKIP AND LIMIT TESTS
// =============================================================================

func TestE2E_Limit(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name LIMIT 2", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestE2E_Skip(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name SKIP 2", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestE2E_SkipAndLimit(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 1 LIMIT 2", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	// Skip Bob (25), get Diana (28) and Alice (30)
	assert.Equal(t, "Diana", result.Rows[0][0])
	assert.Equal(t, "Alice", result.Rows[1][0])
}

// =============================================================================
// WITH CLAUSE TESTS
// =============================================================================

func TestE2E_With_SimpleProjection(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person) 
		WITH n.name AS name, n.age AS age 
		RETURN name, age
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, len(result.Rows))
}

func TestE2E_With_Aggregation(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person) 
		WITH n.city AS city, COUNT(n) AS count 
		RETURN city, count
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))
}

func TestE2E_With_OrderBy(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person) 
		WITH n ORDER BY n.age DESC 
		RETURN n.name
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", result.Rows[0][0])
}

func TestE2E_With_Limit(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person) 
		WITH n ORDER BY n.age DESC LIMIT 2 
		RETURN n.name
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

// =============================================================================
// UNWIND TESTS
// =============================================================================

func TestE2E_Unwind_List(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "UNWIND [1, 2, 3] AS x RETURN x", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, int64(1), result.Rows[0][0])
	assert.Equal(t, int64(2), result.Rows[1][0])
	assert.Equal(t, int64(3), result.Rows[2][0])
}

func TestE2E_Unwind_StringList(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "UNWIND ['a', 'b', 'c'] AS letter RETURN letter", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, "a", result.Rows[0][0])
}

// =============================================================================
// OPTIONAL MATCH TESTS
// =============================================================================

func TestE2E_OptionalMatch_NoResults(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'Alone'})", nil)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alone'}) 
		OPTIONAL MATCH (n)-[:KNOWS]->(friend) 
		RETURN n.name, friend
	`, nil)
	require.NoError(t, err)
	// Should still return a row, friend is null
	assert.Equal(t, 1, len(result.Rows))
}

// =============================================================================
// RELATIONSHIP MATCHING TESTS
// =============================================================================

func TestE2E_Match_OutgoingRelationship(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)

	result, err := exec.Execute(ctx, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "A", result.Rows[0][0])
	assert.Equal(t, "B", result.Rows[0][1])
}

func TestE2E_Match_IncomingRelationship(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)

	result, err := exec.Execute(ctx, "MATCH (b:Person)<-[:KNOWS]-(a:Person) RETURN a.name, b.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
}

func TestE2E_Match_AnyDirection(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)

	result, err := exec.Execute(ctx, "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name", nil)
	require.NoError(t, err)
	// Should match in both directions
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

func TestE2E_Match_RelationshipVariable(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS {since: 2020}]->(b:Person {name: 'B'})", nil)

	result, err := exec.Execute(ctx, "MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
}

func TestE2E_Match_MultipleRelationshipTypes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:LIKES]->(c:Person {name: 'C'})", nil)

	_, err := exec.Execute(ctx, "MATCH (a:Person {name: 'A'})-[:KNOWS|LIKES]->(other) RETURN other.name", nil)
	require.NoError(t, err)
	// Should match both relationships
}

// =============================================================================
// PARAMETER TESTS
// =============================================================================

func TestE2E_Parameters_InMatch(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	params := map[string]interface{}{"name": "Alice"}
	result, err := exec.Execute(ctx, "MATCH (n:Person {name: $name}) RETURN n.age", params)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, int64(30), result.Rows[0][0])
}

func TestE2E_Parameters_InWhere(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	params := map[string]interface{}{"minAge": int64(30)}
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name", params)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows)) // Alice (30) and Charlie (35)
}

func TestE2E_Parameters_InCreate(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	params := map[string]interface{}{"name": "Param Person", "age": int64(42)}
	result, err := exec.Execute(ctx, "CREATE (n:Person {name: $name, age: $age})", params)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify
	result, err = exec.Execute(ctx, "MATCH (n:Person {name: 'Param Person'}) RETURN n.age", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.Rows[0][0])
}

// =============================================================================
// CALL PROCEDURE TESTS
// =============================================================================

func TestE2E_Call_DbLabels(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"label"}, result.Columns)
	assert.GreaterOrEqual(t, len(result.Rows), 3) // Person, Company, Product
}

func TestE2E_Call_DbRelationshipTypes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person)-[:KNOWS]->(b:Person)", nil)
	exec.Execute(ctx, "CREATE (c:Person)-[:LIKES]->(d:Person)", nil)

	result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"relationshipType"}, result.Columns)
	assert.GreaterOrEqual(t, len(result.Rows), 2)
}

func TestE2E_Call_DbPropertyKeys(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, "CALL db.propertyKeys()", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"propertyKey"}, result.Columns)
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

func TestE2E_Call_DbIndexes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CALL db.indexes()", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Columns, "name")
}

func TestE2E_Call_DbConstraints(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "CALL db.constraints()", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Columns, "name")
}

// =============================================================================
// ARITHMETIC EXPRESSION TESTS
// =============================================================================

func TestE2E_Arithmetic_Addition(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 1 + 2 AS sum", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestE2E_Arithmetic_Subtraction(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 5 - 3 AS diff", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestE2E_Arithmetic_Multiplication(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 4 * 3 AS product", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(12), result.Rows[0][0])
}

func TestE2E_Arithmetic_Division(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 10 / 2 AS quotient", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), result.Rows[0][0])
}

func TestE2E_Arithmetic_Modulo(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "RETURN 10 % 3 AS remainder", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

func TestE2E_Arithmetic_PropertyMath(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {age: 30})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.age + 10 AS nextDecade", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(40), result.Rows[0][0])
}

// =============================================================================
// COMPLEX QUERY TESTS
// =============================================================================

func TestE2E_Complex_MultiMatch(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})", nil)
	exec.Execute(ctx, "CREATE (c:Company {name: 'Acme'})", nil)

	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		MATCH (c:Company)
		RETURN p.name, c.name
	`, nil)
	require.NoError(t, err)
	// Cartesian product: 2 people * 1 company
	assert.Equal(t, 2, len(result.Rows))
}

func TestE2E_Complex_ChainedWith(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	setupE2ETestData(t, exec, ctx)

	result, err := exec.Execute(ctx, `
		MATCH (n:Person)
		WITH n.city AS city, COUNT(n) AS count
		WITH city, count WHERE count > 1
		RETURN city, count
	`, nil)
	require.NoError(t, err)
	// Only NYC has more than 1 person
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "NYC", result.Rows[0][0])
}

func TestE2E_Complex_MatchCreateReturn(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Person {name: 'Existing'})", nil)

	result, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Existing'})
		CREATE (b:Person {name: 'New'})
		RETURN a.name, b.name
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

func TestE2E_EmptyResult(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "MATCH (n:NonExistent) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Rows))
}

func TestE2E_InvalidSyntax(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	_, err := exec.Execute(ctx, "INVALID QUERY SYNTAX", nil)
	assert.Error(t, err)
}

func TestE2E_EmptyQuery(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	result, err := exec.Execute(ctx, "", nil)
	// Empty query should either error or return empty result
	if err == nil {
		assert.Equal(t, 0, len(result.Rows))
	}
}

// =============================================================================
// DATA TYPE TESTS
// =============================================================================

func TestE2E_DataType_Integer(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {value: 42})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.Rows[0][0])
}

func TestE2E_DataType_Float(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {value: 3.14})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value", nil)
	require.NoError(t, err)
	assert.Equal(t, 3.14, result.Rows[0][0])
}

func TestE2E_DataType_String(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {value: 'hello world'})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value", nil)
	require.NoError(t, err)
	assert.Equal(t, "hello world", result.Rows[0][0])
}

func TestE2E_DataType_Boolean(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {active: true, deleted: false})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.active, n.deleted", nil)
	require.NoError(t, err)
	assert.Equal(t, true, result.Rows[0][0])
	assert.Equal(t, false, result.Rows[0][1])
}

func TestE2E_DataType_List(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)
	exec.Execute(ctx, "CREATE (n:Test {tags: ['a', 'b', 'c']})", nil)

	result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.tags", nil)
	require.NoError(t, err)
	tags := result.Rows[0][0].([]interface{})
	assert.Equal(t, 3, len(tags))
}

// =============================================================================
// BENCHMARK STYLE TESTS (for performance validation)
// =============================================================================

func TestE2E_Performance_ManyNodes(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		exec.Execute(ctx, "CREATE (n:Perf {id: $id})", map[string]interface{}{"id": int64(i)})
	}

	result, err := exec.Execute(ctx, "MATCH (n:Perf) RETURN COUNT(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(100), result.Rows[0][0])
}

func TestE2E_Performance_FilteredMatch(t *testing.T) {
	exec, ctx := setupE2EExecutor(t)

	// Create nodes
	for i := 0; i < 50; i++ {
		exec.Execute(ctx, "CREATE (n:Perf {id: $id, even: $even})", map[string]interface{}{
			"id":   int64(i),
			"even": i%2 == 0,
		})
	}

	result, err := exec.Execute(ctx, "MATCH (n:Perf) WHERE n.even = true RETURN COUNT(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(25), result.Rows[0][0])
}
