// Neo4j Compatibility Tests for NornicDB
//
// These tests verify that NornicDB supports the same Cypher query patterns as Neo4j.
// Any query that works in Neo4j should also work in NornicDB (Neo4j drop-in replacement).
//
// Issues discovered from Mimir integration testing:
// 1. CREATE...SET - Neo4j allows SET after CREATE, NornicDB currently requires MATCH before SET
// 2. Property access in RETURN after YIELD - Neo4j allows node.property in RETURN after YIELD
// 3. DETACH DELETE with WHERE STARTS WITH - May hang instead of returning quickly
// 4. Fulltext index queries without index - Should error quickly, not hang

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Issue #1: CREATE...SET (Neo4j allows SET immediately after CREATE)
// ============================================================================

// TestCreateWithSetNeo4jCompat tests that CREATE...SET works like Neo4j
// Neo4j allows: CREATE (n:Node {id: 'test'}) SET n.content = 'value' RETURN n
// NornicDB currently errors: "SET requires a MATCH clause first"
func TestCreateWithSetNeo4jCompat(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("CREATE single node then SET property", func(t *testing.T) {
		// This is the exact query pattern from Mimir that fails
		query := `CREATE (n:Node {id: 'test_update_123', type: 'memory', title: 'Update Test'})
SET n.content = 'Updated content for testing'
RETURN n`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CREATE...SET should work like Neo4j")
		require.Len(t, result.Rows, 1, "Should return the created node")

		// Verify the node was created with both initial and SET properties
		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "test_update_123", node.Properties["id"])
		assert.Equal(t, "memory", node.Properties["type"])
		assert.Equal(t, "Update Test", node.Properties["title"])
		assert.Equal(t, "Updated content for testing", node.Properties["content"])
	})

	t.Run("CREATE with parameterized SET", func(t *testing.T) {
		// Test with parameters like Mimir uses
		query := `CREATE (n:Node {id: $id, type: 'memory', title: 'Update Test'})
SET n.content = $newContent
RETURN n`

		params := map[string]interface{}{
			"id":         "test_param_123",
			"newContent": "Parameterized content",
		}

		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, "CREATE...SET with params should work")
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "test_param_123", node.Properties["id"])
		assert.Equal(t, "Parameterized content", node.Properties["content"])
	})

	t.Run("CREATE multiple nodes then SET", func(t *testing.T) {
		// Neo4j allows setting properties on multiple created nodes
		query := `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
SET a.age = 30, b.age = 25
RETURN a, b`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CREATE multiple nodes with SET should work")
		require.Len(t, result.Rows, 1)

		nodeA := result.Rows[0][0].(*storage.Node)
		nodeB := result.Rows[0][1].(*storage.Node)
		assert.Equal(t, int64(30), nodeA.Properties["age"])
		assert.Equal(t, int64(25), nodeB.Properties["age"])
	})

	t.Run("CREATE node and relationship then SET", func(t *testing.T) {
		// Create relationship and set properties on it
		query := `CREATE (a:Person {name: 'Charlie'})-[r:KNOWS]->(b:Person {name: 'Diana'})
SET r.since = 2020
RETURN a, r, b`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CREATE with relationship then SET should work")
		require.Len(t, result.Rows, 1)
	})

	t.Run("CREATE with SET using += operator", func(t *testing.T) {
		// Neo4j supports += for merging properties
		query := `CREATE (n:Node {id: 'merge_test'})
SET n += {extra: 'value', count: 5}
RETURN n`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CREATE with SET += should work")
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "merge_test", node.Properties["id"])
		assert.Equal(t, "value", node.Properties["extra"])
		assert.Equal(t, int64(5), node.Properties["count"])
	})
}

// ============================================================================
// Issue #2: Property access in RETURN after YIELD
// ============================================================================

// TestPropertyAccessAfterYieldNeo4jCompat tests that node.property works in RETURN after YIELD
// Neo4j allows: CALL procedure() YIELD node, score RETURN node.id, node.type, score
// NornicDB may only return [node, score] without allowing property access
func TestPropertyAccessAfterYieldNeo4jCompat(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create some test data
	_, err := exec.Execute(ctx, `
		CREATE (n1:TestNode {id: 'node1', type: 'memory', title: 'Test Node 1'})
		CREATE (n2:TestNode {id: 'node2', type: 'file', title: 'Test Node 2'})
	`, nil)
	require.NoError(t, err)

	t.Run("property access in RETURN after YIELD from procedure", func(t *testing.T) {
		// This is the pattern that Mimir uses for type filtering
		// The test will use a simpler procedure since we may not have vector index
		query := `
MATCH (n:TestNode)
WITH n, 0.5 as score
RETURN n.id as id, n.type as type, score
LIMIT 10`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Property access in RETURN should work")
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// Verify we can access individual properties, not just the whole node
		assert.Contains(t, result.Columns, "id")
		assert.Contains(t, result.Columns, "type")
		assert.Contains(t, result.Columns, "score")
	})

	t.Run("property access with WHERE after YIELD", func(t *testing.T) {
		// Neo4j allows WHERE on yielded node properties
		query := `
MATCH (n:TestNode)
WITH n, 0.5 as score
WHERE n.type IN ['memory', 'file']
RETURN n.id as id, n.type as type, score`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WHERE on node properties after YIELD should work")
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})
}

// ============================================================================
// Issue #3: DETACH DELETE with WHERE pattern matching
// ============================================================================

// TestDetachDeleteWithWhereNeo4jCompat tests that DETACH DELETE with WHERE completes quickly
// Neo4j executes this immediately, NornicDB may hang
func TestDetachDeleteWithWhereNeo4jCompat(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test nodes with a specific prefix
	for i := 0; i < 10; i++ {
		_, err := exec.Execute(ctx, `
			CREATE (n:TestCleanup {id: $id, value: $value})
		`, map[string]interface{}{
			"id":    "integration_test_" + string(rune('A'+i)),
			"value": i,
		})
		require.NoError(t, err)
	}

	t.Run("DETACH DELETE with STARTS WITH", func(t *testing.T) {
		// This is the exact cleanup query from Mimir that hangs
		query := `
MATCH (n:TestCleanup)
WHERE n.id STARTS WITH 'integration_test_'
DETACH DELETE n`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "DETACH DELETE with WHERE STARTS WITH should complete")

		// Verify nodes were deleted
		countResult, err := exec.Execute(ctx, `
			MATCH (n:TestCleanup) 
			WHERE n.id STARTS WITH 'integration_test_' 
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, countResult.Rows, 1)
		count := countResult.Rows[0][0]
		assert.Equal(t, int64(0), count, "All test nodes should be deleted")

		_ = result // Use result to avoid unused variable warning
	})

	t.Run("DETACH DELETE with IN list", func(t *testing.T) {
		// Create nodes to delete
		_, _ = exec.Execute(ctx, `CREATE (n:ToDelete {id: 'del1'})`, nil)
		_, _ = exec.Execute(ctx, `CREATE (n:ToDelete {id: 'del2'})`, nil)

		query := `
MATCH (n:ToDelete)
WHERE n.id IN ['del1', 'del2']
DETACH DELETE n`

		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "DETACH DELETE with IN should work")
	})
}

// ============================================================================
// Issue #4: Fulltext index behavior without index
// ============================================================================

// TestFulltextWithoutIndexNeo4jCompat tests that fulltext queries error appropriately
// Neo4j returns an error immediately if the index doesn't exist
// NornicDB should do the same, not hang
func TestFulltextWithoutIndexNeo4jCompat(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("fulltext query on non-existent index should error", func(t *testing.T) {
		query := `
CALL db.index.fulltext.queryNodes('nonexistent_index', 'test query')
YIELD node, score
RETURN node.id as id, score
LIMIT 5`

		_, err := exec.Execute(ctx, query, nil)
		// Should error, not hang
		assert.Error(t, err, "Fulltext query on non-existent index should return error immediately")
		if err != nil {
			assert.Contains(t, err.Error(), "index", "Error should mention the missing index")
		}
	})
}

// ============================================================================
// Whitespace Compatibility Tests
// ============================================================================

// TestCreateSetWhitespaceVariations tests that CREATE...SET works with various whitespace
func TestCreateSetWhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "single line",
			query: `CREATE (n:Node {id: 'ws1'}) SET n.value = 1 RETURN n`,
		},
		{
			name: "newline before SET",
			query: `CREATE (n:Node {id: 'ws2'})
SET n.value = 2 RETURN n`,
		},
		{
			name: "newline after SET",
			query: `CREATE (n:Node {id: 'ws3'}) SET
n.value = 3 RETURN n`,
		},
		{
			name: "multiple newlines",
			query: `CREATE (n:Node {id: 'ws4'})

SET n.value = 4

RETURN n`,
		},
		{
			name: "tabs instead of spaces",
			query: "CREATE (n:Node {id: 'ws5'})\tSET n.value = 5\tRETURN n",
		},
		{
			name: "mixed whitespace",
			query: `CREATE (n:Node {id: 'ws6'})
	SET n.value = 6
	RETURN n`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, "Query should execute regardless of whitespace: %s", tc.name)
			require.Len(t, result.Rows, 1, "Should return one row")
		})
	}
}

// ============================================================================
// Complex Neo4j Patterns from Mimir
// ============================================================================

// TestMimirSearchPatternNeo4jCompat tests the complex search query pattern from Mimir
func TestMimirSearchPatternNeo4jCompat(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data that mimics Mimir's structure
	_, err := exec.Execute(ctx, `
		CREATE (f:File {id: 'file1', path: '/test/file.ts', name: 'file.ts', type: 'file'})
		CREATE (c1:FileChunk {id: 'chunk1', type: 'file_chunk', content: 'function test() {}'})
		CREATE (c2:FileChunk {id: 'chunk2', type: 'file_chunk', content: 'class Example {}'})
	`, nil)
	require.NoError(t, err)

	// Create relationships
	_, err = exec.Execute(ctx, `
		MATCH (f:File {id: 'file1'}), (c:FileChunk)
		WHERE c.id IN ['chunk1', 'chunk2']
		CREATE (f)-[:HAS_CHUNK]->(c)
	`, nil)
	require.NoError(t, err)

	// Verify data was created correctly
	t.Run("verify test data exists", func(t *testing.T) {
		// Check FileChunks exist
		result, err := exec.Execute(ctx, `MATCH (n:FileChunk) RETURN n.id, n.type`, nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Rows), "Should have 2 FileChunk nodes")

		// Check relationships exist
		result, err = exec.Execute(ctx, `MATCH (f:File)-[:HAS_CHUNK]->(c:FileChunk) RETURN f.id, c.id`, nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Rows), "Should have 2 HAS_CHUNK relationships")
	})

	t.Run("simple WITH clause with literal value", func(t *testing.T) {
		// Simplest case: WITH adding a literal column
		query := `MATCH (node:FileChunk) WITH node, 0.75 as score RETURN node.id, score`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Simple WITH literal should work")
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows (one per FileChunk)")
		if len(result.Rows) > 0 {
			t.Logf("Row 0: %+v", result.Rows[0])
		}
	})

	t.Run("WITH clause followed by WHERE", func(t *testing.T) {
		// WITH + WHERE filtering
		query := `MATCH (node:FileChunk) WITH node, 0.75 as score WHERE score >= 0.5 RETURN node.id, score`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WITH + WHERE should work")
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows (score 0.75 >= 0.5)")
	})

	t.Run("OPTIONAL MATCH after WITH", func(t *testing.T) {
		// WITH + OPTIONAL MATCH
		query := `
MATCH (node:FileChunk)
WITH node, 0.75 as score
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN node.id, score, parentFile.id`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WITH + OPTIONAL MATCH should work")
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
		if len(result.Rows) > 0 {
			t.Logf("WITH + OPTIONAL MATCH Row 0: %+v", result.Rows[0])
		}
	})

	t.Run("simple CASE expression in RETURN", func(t *testing.T) {
		// Test CASE expression alone
		query := `
MATCH (node:FileChunk)
RETURN CASE WHEN node.type = 'file_chunk' THEN 'yes' ELSE 'no' END AS is_chunk`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Simple CASE should work")
		t.Logf("Simple CASE returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("CASE with property access", func(t *testing.T) {
		// Test CASE that returns a property
		query := `
MATCH (node:FileChunk)
RETURN CASE WHEN node.type = 'file_chunk' THEN node.id ELSE 'unknown' END AS result_id`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CASE with property access should work")
		t.Logf("CASE property returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("CASE with IS NOT NULL check", func(t *testing.T) {
		// Test CASE with IS NOT NULL
		query := `
MATCH (node:FileChunk)
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN CASE WHEN parentFile IS NOT NULL THEN parentFile.path ELSE node.id END AS result`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CASE with IS NOT NULL should work")
		t.Logf("CASE IS NOT NULL returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("COALESCE function", func(t *testing.T) {
		// Test COALESCE
		query := `
MATCH (node:FileChunk)
RETURN COALESCE(node.title, node.name, node.id) AS display_name`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "COALESCE should work")
		t.Logf("COALESCE returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("CASE with compound AND condition", func(t *testing.T) {
		// Test CASE with AND in condition
		query := `
MATCH (node:FileChunk)
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN CASE 
         WHEN node.type = 'file_chunk' AND parentFile IS NOT NULL 
         THEN parentFile.path 
         ELSE node.id
       END AS result`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "CASE with AND condition should work")
		t.Logf("CASE AND returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("WITH then OPTIONAL MATCH then CASE", func(t *testing.T) {
		// This is the pattern in the complex query - WITH before OPTIONAL MATCH
		// WITHOUT the WHERE clause - this should work
		query := `
MATCH (node:FileChunk)
WITH node, 0.75 as score
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN node.id, parentFile.path, score`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WITH + OPTIONAL MATCH + simple RETURN should work")
		t.Logf("WITH->OPTIONAL (no WHERE) returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
		// NOTE: score shows as <nil> instead of 0.75 - this is a known bug
		// The WITH-introduced variable is not preserved through OPTIONAL MATCH
	})

	t.Run("WITH + WHERE + OPTIONAL MATCH", func(t *testing.T) {
		// This tests the pattern: MATCH ... WITH ... WHERE ... OPTIONAL MATCH ... RETURN
		// The WHERE after WITH filters the WITH results, then OPTIONAL MATCH executes
		query := `
MATCH (node:FileChunk)
WITH node, 0.75 as score
WHERE score >= 0.5
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN node.id, parentFile.path, score`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WITH + WHERE + OPTIONAL MATCH should not error")
		t.Logf("WITH->WHERE->OPTIONAL returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		// This MUST return 2 rows for Neo4j compatibility
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows - WHERE should filter WITH results, not cause 0 rows")
	})

	t.Run("WITH then OPTIONAL MATCH then CASE expression", func(t *testing.T) {
		// WITH then OPTIONAL MATCH then CASE
		query := `
MATCH (node:FileChunk)
WITH node, 0.75 as score
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN CASE 
         WHEN parentFile IS NOT NULL 
         THEN parentFile.path 
         ELSE node.id
       END AS result, score`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "WITH + OPTIONAL MATCH + CASE should work")
		t.Logf("WITH->OPTIONAL->CASE returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("multiple CASE expressions in RETURN", func(t *testing.T) {
		// Multiple CASE expressions
		query := `
MATCH (node:FileChunk)
OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)
RETURN CASE WHEN parentFile IS NOT NULL THEN parentFile.path ELSE node.id END AS id,
       node.type AS type`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Multiple CASE expressions should work")
		t.Logf("Multiple CASE returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		require.Equal(t, 2, len(result.Rows), "Should return 2 rows")
	})

	t.Run("complex aggregation query pattern", func(t *testing.T) {
		// This is similar to Mimir's vector search result aggregation
		query := `
MATCH (node:FileChunk)
WITH node, 0.75 as score
WHERE score >= 0.5

OPTIONAL MATCH (node)<-[:HAS_CHUNK]-(parentFile:File)

RETURN CASE 
         WHEN node.type = 'file_chunk' AND parentFile IS NOT NULL 
         THEN parentFile.path 
         ELSE COALESCE(node.id, node.path)
       END AS id,
       node.type AS type,
       CASE 
         WHEN node.type = 'file_chunk' AND parentFile IS NOT NULL 
         THEN parentFile.name 
         ELSE COALESCE(node.title, node.name)
       END AS title,
       score AS similarity
ORDER BY score DESC
LIMIT 10`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "Complex Mimir search pattern should execute")
		t.Logf("Complex query returned %d rows, columns: %v", len(result.Rows), result.Columns)
		for i, row := range result.Rows {
			t.Logf("Row %d: %+v", i, row)
		}
		// Should return results from the file chunks
		require.GreaterOrEqual(t, len(result.Rows), 1)
		assert.Contains(t, result.Columns, "id")
		assert.Contains(t, result.Columns, "type")
		assert.Contains(t, result.Columns, "similarity")
	})
}
