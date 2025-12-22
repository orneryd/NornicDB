// Tests for MATCH ... CREATE relationship patterns
// Covers both Neo4j styles:
// 1. Multiple MATCH clauses: MATCH (a) MATCH (b) CREATE (a)-[r]->(b)
// 2. Comma-separated: MATCH (a), (b) CREATE (a)-[r]->(b)

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchCreateRelationship_CommaSeparated(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create two nodes
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	t.Run("comma-separated MATCH creates relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
			CREATE (a)-[r:KNOWS {since: 2020}]->(b)
			RETURN r
		`, nil)
		require.NoError(t, err, "Comma-separated MATCH CREATE should work")
		require.Len(t, result.Rows, 1, "Should return 1 created relationship")
		assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	})

	t.Run("verify relationship was created", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
			RETURN r.since
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(2020), result.Rows[0][0])
	})
}

func TestMatchCreateRelationship_MultipleMATCH(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create two nodes
	_, err := exec.Execute(ctx, `CREATE (a:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Employee {name: 'John'})`, nil)
	require.NoError(t, err)

	t.Run("multiple MATCH clauses creates relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (c:Company {name: 'Acme'})
			MATCH (e:Employee {name: 'John'})
			CREATE (e)-[r:WORKS_AT {role: 'Engineer'}]->(c)
			RETURN r
		`, nil)
		require.NoError(t, err, "Multiple MATCH clauses CREATE should work")
		require.Len(t, result.Rows, 1, "Should return 1 created relationship")
		assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	})

	t.Run("verify relationship was created", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Employee)-[r:WORKS_AT]->(c:Company)
			RETURN e.name, r.role, c.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "John", result.Rows[0][0])
		assert.Equal(t, "Engineer", result.Rows[0][1])
		assert.Equal(t, "Acme", result.Rows[0][2])
	})
}

func TestMatchCreateRelationship_MixedStyles(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create three nodes
	_, err := exec.Execute(ctx, `CREATE (a:Node {id: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b:Node {id: 'B'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Node {id: 'C'})`, nil)
	require.NoError(t, err)

	t.Run("mixed: MATCH with comma then another MATCH", func(t *testing.T) {
		// This pattern: MATCH (a), (b) MATCH (c) CREATE ...
		result, err := exec.Execute(ctx, `
			MATCH (a:Node {id: 'A'}), (b:Node {id: 'B'})
			MATCH (c:Node {id: 'C'})
			CREATE (a)-[r1:LINKS]->(b), (b)-[r2:LINKS]->(c)
			RETURN r1, r2
		`, nil)
		require.NoError(t, err, "Mixed MATCH styles should work")
		require.Len(t, result.Rows, 1)
		assert.Equal(t, 2, result.Stats.RelationshipsCreated)
	})
}

func TestMatchCreateRelationship_WithProperties(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	_, err := exec.Execute(ctx, `CREATE (s:Source {id: 'src-1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (t:Target {id: 'tgt-1'})`, nil)
	require.NoError(t, err)

	t.Run("create relationship with multiple properties", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (s:Source {id: 'src-1'}), (t:Target {id: 'tgt-1'})
			CREATE (s)-[e:EDGE {
				id: 'edge-123',
				type: 'depends_on',
				created: '2025-01-01',
				weight: 0.95
			}]->(t)
			RETURN e
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("verify edge properties", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH ()-[e:EDGE {id: 'edge-123'}]->()
			RETURN e.type, e.weight
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "depends_on", result.Rows[0][0])
	})
}

func TestMatchCreateRelationship_ByID(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup - Mimir-style nodes with id property
	_, err := exec.Execute(ctx, `
		CREATE (n:Node {id: 'node-source-123', type: 'task', title: 'Task 1'})
	`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		CREATE (n:Node {id: 'node-target-456', type: 'task', title: 'Task 2'})
	`, nil)
	require.NoError(t, err)

	t.Run("Mimir addEdge pattern - comma separated", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (s:Node {id: 'node-source-123'}), (t:Node {id: 'node-target-456'})
			CREATE (s)-[e:EDGE {id: 'edge-001', type: 'depends_on'}]->(t)
			RETURN e
		`, nil)
		require.NoError(t, err, "Mimir addEdge pattern should work")
		require.Len(t, result.Rows, 1)
	})

	t.Run("Mimir addEdge pattern - multiple MATCH", func(t *testing.T) {
		// Create new nodes for this test
		_, _ = exec.Execute(ctx, `CREATE (n:Node {id: 'node-a', type: 'doc'})`, nil)
		_, _ = exec.Execute(ctx, `CREATE (n:Node {id: 'node-b', type: 'doc'})`, nil)

		result, err := exec.Execute(ctx, `
			MATCH (s:Node {id: 'node-a'})
			MATCH (t:Node {id: 'node-b'})
			CREATE (s)-[e:REFERENCES {id: 'ref-001'}]->(t)
			RETURN e
		`, nil)
		require.NoError(t, err, "Multiple MATCH addEdge pattern should work")
		require.Len(t, result.Rows, 1)
	})
}

func TestMatchCreateRelationship_NoReturn(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	_, _ = exec.Execute(ctx, `CREATE (a:X {id: '1'})`, nil)
	_, _ = exec.Execute(ctx, `CREATE (b:X {id: '2'})`, nil)

	t.Run("create relationship without RETURN", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:X {id: '1'}), (b:X {id: '2'})
			CREATE (a)-[:CONNECTS]->(b)
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	})

	t.Run("verify relationship exists", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:X)-[r:CONNECTS]->(b:X)
			RETURN count(r)
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(1), result.Rows[0][0])
	})
}

func TestMatchCreateRelationship_ReverseDirection(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	_, _ = exec.Execute(ctx, `CREATE (a:Start {name: 'start'})`, nil)
	_, _ = exec.Execute(ctx, `CREATE (b:End {name: 'end'})`, nil)

	t.Run("create reverse direction relationship", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a:Start), (b:End)
			CREATE (a)<-[:POINTS_TO]-(b)
			RETURN a.name, b.name
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	})
}
