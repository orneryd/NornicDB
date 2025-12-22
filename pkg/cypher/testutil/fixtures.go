// Package testutil provides shared test utilities for the NornicDB Cypher package.
//
// This package contains common test fixtures, setup functions, and assertion
// helpers used across Cypher executor tests. By centralizing test utilities,
// we reduce code duplication and ensure consistent test setup.
//
// # Features
//
//   - Test executor setup with in-memory storage
//   - Standard test node creation
//   - Query result assertions
//   - Common test data fixtures
//
// # Usage
//
//	func TestSomething(t *testing.T) {
//	    exec := testutil.SetupTestExecutor(t)
//	    testutil.CreateTestNodes(t, exec)
//	    
//	    result, err := exec.Execute(ctx, "MATCH (p:Person) RETURN p", nil)
//	    testutil.AssertQueryResult(t, result, 3)
//	}
//
// # ELI12
//
// Think of this package like a toolbox for building tests:
//   - SetupTestExecutor: Like getting a fresh sandbox to play in
//   - CreateTestNodes: Like pre-placing toy figures in the sandbox
//   - AssertQueryResult: Like checking if you built the right shape
//
// Instead of each test building everything from scratch, they can
// use these shared tools to get started quickly!
package testutil

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SetupTestExecutor creates a new Cypher executor with in-memory storage.
//
// This is the standard way to set up a test executor. The in-memory storage
// provides fast, isolated test execution without disk I/O.
//
// Parameters:
//   - t: The testing context for error reporting
//
// Returns:
//   - A fully initialized StorageExecutor ready for query execution
//
// Example:
//
//	func TestMyFeature(t *testing.T) {
//	    exec := testutil.SetupTestExecutor(t)
//	    result, err := exec.Execute(ctx, "RETURN 1", nil)
//	    require.NoError(t, err)
//	}
func SetupTestExecutor(t *testing.T) *cypher.StorageExecutor {
	t.Helper()
	// Wrap with NamespacedEngine to handle ID prefixing (required by BadgerEngine)
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	return cypher.NewStorageExecutor(store)
}

// SetupTestExecutorWithStore creates a test executor with a provided storage engine.
//
// Use this when you need to test with a specific storage configuration or
// when you need direct access to the storage for verification.
//
// Parameters:
//   - t: The testing context for error reporting
//   - store: The storage engine to use
//
// Returns:
//   - A StorageExecutor using the provided storage
//
// Example:
//
//	func TestWithCustomStorage(t *testing.T) {
//	    store := storage.NewMemoryEngine()
//	    exec := testutil.SetupTestExecutorWithStore(t, store)
//	    // Test something and verify storage directly
//	    nodes := store.GetAllNodes()
//	}
func SetupTestExecutorWithStore(t *testing.T, store storage.Engine) *cypher.StorageExecutor {
	t.Helper()
	return cypher.NewStorageExecutor(store)
}

// CreateTestNodes creates a standard set of Person nodes for testing.
//
// Creates three Person nodes with varying ages:
//   - Alice (age 30)
//   - Bob (age 25)
//   - Charlie (age 35)
//
// Parameters:
//   - t: The testing context for error reporting
//   - exec: The executor to use for node creation
//
// Example:
//
//	func TestPersonQueries(t *testing.T) {
//	    exec := testutil.SetupTestExecutor(t)
//	    testutil.CreateTestNodes(t, exec)
//	    
//	    result, err := exec.Execute(ctx, "MATCH (p:Person) RETURN p.name", nil)
//	    require.NoError(t, err)
//	    assert.Len(t, result.Rows, 3)
//	}
func CreateTestNodes(t *testing.T, exec *cypher.StorageExecutor) {
	t.Helper()
	ctx := context.Background()

	queries := []string{
		`CREATE (a:Person {name: 'Alice', age: 30})`,
		`CREATE (b:Person {name: 'Bob', age: 25})`,
		`CREATE (c:Person {name: 'Charlie', age: 35})`,
	}

	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to create test node with query: %s", q)
	}
}

// CreateTestGraph creates a more complex graph with nodes and relationships.
//
// Creates a graph with:
//   - Person nodes: Alice, Bob, Charlie
//   - Company nodes: Acme Corp, TechStart
//   - Relationships: KNOWS (between people), WORKS_AT (person -> company)
//
// Parameters:
//   - t: The testing context for error reporting
//   - exec: The executor to use for graph creation
//
// Example:
//
//	func TestRelationshipQueries(t *testing.T) {
//	    exec := testutil.SetupTestExecutor(t)
//	    testutil.CreateTestGraph(t, exec)
//	    
//	    result, err := exec.Execute(ctx, 
//	        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name", nil)
//	    require.NoError(t, err)
//	}
func CreateTestGraph(t *testing.T, exec *cypher.StorageExecutor) {
	t.Helper()
	ctx := context.Background()

	queries := []string{
		// Create Person nodes
		`CREATE (a:Person {name: 'Alice', age: 30})`,
		`CREATE (b:Person {name: 'Bob', age: 25})`,
		`CREATE (c:Person {name: 'Charlie', age: 35})`,
		// Create Company nodes
		`CREATE (acme:Company {name: 'Acme Corp', founded: 1990})`,
		`CREATE (tech:Company {name: 'TechStart', founded: 2015})`,
	}

	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to execute: %s", q)
	}

	// Create relationships
	relQueries := []string{
		`MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)`,
		`MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS {since: 2018}]->(c)`,
		`MATCH (a:Person {name: 'Alice'}), (acme:Company {name: 'Acme Corp'}) CREATE (a)-[:WORKS_AT {role: 'Engineer'}]->(acme)`,
		`MATCH (b:Person {name: 'Bob'}), (tech:Company {name: 'TechStart'}) CREATE (b)-[:WORKS_AT {role: 'Developer'}]->(tech)`,
	}

	for _, q := range relQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "Failed to execute: %s", q)
	}
}

// AssertQueryResult verifies that a query result has the expected number of rows.
//
// Parameters:
//   - t: The testing context for error reporting
//   - result: The query result to check
//   - expectedRows: The expected number of rows
//
// Example:
//
//	result, err := exec.Execute(ctx, "MATCH (p:Person) RETURN p", nil)
//	require.NoError(t, err)
//	testutil.AssertQueryResult(t, result, 3)
func AssertQueryResult(t *testing.T, result *cypher.ExecuteResult, expectedRows int) {
	t.Helper()
	require.NotNil(t, result, "Result should not be nil")
	assert.Len(t, result.Rows, expectedRows, "Expected %d rows, got %d", expectedRows, len(result.Rows))
}

// AssertQueryResultColumns verifies result columns and row count.
//
// Parameters:
//   - t: The testing context for error reporting
//   - result: The query result to check
//   - expectedColumns: The expected column names
//   - expectedRows: The expected number of rows
//
// Example:
//
//	result, err := exec.Execute(ctx, "MATCH (p:Person) RETURN p.name, p.age", nil)
//	require.NoError(t, err)
//	testutil.AssertQueryResultColumns(t, result, []string{"p.name", "p.age"}, 3)
func AssertQueryResultColumns(t *testing.T, result *cypher.ExecuteResult, expectedColumns []string, expectedRows int) {
	t.Helper()
	require.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, expectedColumns, result.Columns, "Column mismatch")
	assert.Len(t, result.Rows, expectedRows, "Row count mismatch")
}

// ExecuteQuery is a helper that executes a query and returns the result.
//
// Parameters:
//   - t: The testing context for error reporting
//   - exec: The executor to use
//   - query: The Cypher query to execute
//   - params: Optional query parameters
//
// Returns:
//   - The query result
//
// Example:
//
//	result := testutil.ExecuteQuery(t, exec, "RETURN 1 + 2 as sum", nil)
//	assert.Equal(t, int64(3), result.Rows[0][0])
func ExecuteQuery(t *testing.T, exec *cypher.StorageExecutor, query string, params map[string]interface{}) *cypher.ExecuteResult {
	t.Helper()
	ctx := context.Background()
	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err, "Query failed: %s", query)
	return result
}

// MustExecute executes a query and fails the test if there's an error.
//
// Unlike ExecuteQuery, this doesn't return the result - use it for setup queries
// where you don't need to check the result.
//
// Parameters:
//   - t: The testing context for error reporting
//   - exec: The executor to use
//   - query: The Cypher query to execute
//
// Example:
//
//	testutil.MustExecute(t, exec, "CREATE (n:Node {value: 42})")
func MustExecute(t *testing.T, exec *cypher.StorageExecutor, query string) {
	t.Helper()
	ctx := context.Background()
	_, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "Query failed: %s", query)
}

// GetSingleValue extracts a single value from a query result.
//
// Useful for testing queries that return exactly one row with one column.
//
// Parameters:
//   - t: The testing context for error reporting
//   - result: The query result
//
// Returns:
//   - The value in the first row, first column
//
// Example:
//
//	result := testutil.ExecuteQuery(t, exec, "RETURN 42 as answer", nil)
//	value := testutil.GetSingleValue(t, result)
//	assert.Equal(t, int64(42), value)
func GetSingleValue(t *testing.T, result *cypher.ExecuteResult) interface{} {
	t.Helper()
	require.NotNil(t, result, "Result should not be nil")
	require.Len(t, result.Rows, 1, "Expected exactly 1 row")
	require.Len(t, result.Rows[0], 1, "Expected exactly 1 column")
	return result.Rows[0][0]
}

// GetColumnValues extracts all values from a specific column.
//
// Parameters:
//   - t: The testing context for error reporting
//   - result: The query result
//   - colIndex: The zero-based column index
//
// Returns:
//   - A slice of all values in that column
//
// Example:
//
//	result := testutil.ExecuteQuery(t, exec, "MATCH (p:Person) RETURN p.name", nil)
//	names := testutil.GetColumnValues(t, result, 0)
//	// names = ["Alice", "Bob", "Charlie"]
func GetColumnValues(t *testing.T, result *cypher.ExecuteResult, colIndex int) []interface{} {
	t.Helper()
	require.NotNil(t, result, "Result should not be nil")
	require.Greater(t, len(result.Columns), colIndex, "Column index %d out of range", colIndex)

	values := make([]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		values[i] = row[colIndex]
	}
	return values
}
