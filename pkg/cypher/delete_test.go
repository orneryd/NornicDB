package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetachDeleteAll(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create some nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', id: 'alice-1'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', id: 'bob-1'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Person {name: 'Charlie', id: 'charlie-1'})`, nil)
	require.NoError(t, err)

	// Verify nodes exist
	countResult, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(3), count, "Should have 3 nodes")

	// Delete all nodes with DETACH DELETE
	deleteResult, err := executor.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, deleteResult.Stats.NodesDeleted, "Should delete 3 nodes")

	// Verify all nodes are gone
	countResult, err = executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count = countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(0), count, "Should have 0 nodes after delete")
}

func TestDetachDeleteWithRelationships(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes with relationships
	_, err := executor.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', id: 'alice-2'})
		CREATE (b:Person {name: 'Bob', id: 'bob-2'})
		CREATE (a)-[:KNOWS]->(b)
	`, nil)
	require.NoError(t, err)

	// Verify nodes and edges exist
	countResult, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(2), count, "Should have 2 nodes")

	// Delete all with DETACH (should delete edges too)
	deleteResult, err := executor.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete 2 nodes")
	// Edges should also be deleted
	assert.GreaterOrEqual(t, deleteResult.Stats.RelationshipsDeleted, 1, "Should delete at least 1 relationship")

	// Verify all gone
	countResult, err = executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count = countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(0), count, "Should have 0 nodes")
}

func TestDeleteWithFilter(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', age: 30})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', age: 25})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Animal {name: 'Charlie'})`, nil)
	require.NoError(t, err)

	// Delete only Person nodes
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Person) DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete 2 Person nodes")

	// Animal should still exist
	countResult, err := executor.Execute(ctx, `MATCH (n:Animal) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(1), count, "Should have 1 Animal node")
}

func TestDeleteWithWhereClause(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', age: 30})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', age: 25})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Person {name: 'Charlie', age: 35})`, nil)
	require.NoError(t, err)

	// Delete only people over 28
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Person) WHERE n.age > 28 DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete Alice and Charlie")

	// Bob should still exist
	countResult, err := executor.Execute(ctx, `MATCH (n:Person) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(1), count, "Should have 1 Person node (Bob)")
}

func TestDeleteWithParameters(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Task {id: 'task-1', status: 'pending'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Task {id: 'task-2', status: 'completed'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Task {id: 'task-3', status: 'pending'})`, nil)
	require.NoError(t, err)

	// Delete completed tasks using parameters
	deleteResult, err := executor.Execute(ctx, `MATCH (t:Task) WHERE t.status = $status DELETE t`, map[string]interface{}{
		"status": "completed",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, deleteResult.Stats.NodesDeleted, "Should delete 1 completed task")

	// 2 pending tasks should remain
	countResult, err := executor.Execute(ctx, `MATCH (t:Task) RETURN count(t)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(2), count, "Should have 2 pending tasks")
}

func TestDeleteContentWithCypherKeywords(t *testing.T) {
	engine := storage.NewMemoryEngine()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create node with content containing Cypher-like text (regression test)
	_, err := executor.Execute(ctx, `CREATE (n:Memory {id: 'mem-1', content: $content})`, map[string]interface{}{
		"content": "Example: MATCH (n) DELETE n - this is just text, not a command",
	})
	require.NoError(t, err)

	// Verify created
	countResult, err := executor.Execute(ctx, `MATCH (n:Memory) RETURN count(n)`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countResult.Rows[0][0].(int64))

	// Delete it
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Memory) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, deleteResult.Stats.NodesDeleted)

	// Verify deleted
	countResult, err = executor.Execute(ctx, `MATCH (n:Memory) RETURN count(n)`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), countResult.Rows[0][0].(int64))
}
