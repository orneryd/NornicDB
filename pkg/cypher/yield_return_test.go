package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestYieldReturnIntegration tests the CALL...YIELD...RETURN pattern
// which is critical for fulltext search functionality.
// This covers:
// - RETURN clause after YIELD with property projection
// - ORDER BY, LIMIT, SKIP clauses
// - Multi-line query formatting (whitespace handling)
func TestYieldReturnIntegration(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with various content for fulltext search
	testNodes := []struct {
		id     string
		labels []string
		props  map[string]interface{}
	}{
		{"node-1", []string{"Memory"}, map[string]interface{}{"title": "Authentication System", "content": "JWT tokens and OAuth2 authentication flow", "type": "memory", "priority": int64(1)}},
		{"node-2", []string{"Memory"}, map[string]interface{}{"title": "Database Design", "content": "PostgreSQL schema with authentication tables", "type": "memory", "priority": int64(2)}},
		{"node-3", []string{"Memory"}, map[string]interface{}{"title": "API Endpoints", "content": "REST API with JWT authentication middleware", "type": "memory", "priority": int64(3)}},
		{"node-4", []string{"File"}, map[string]interface{}{"title": "auth.go", "content": "Authentication handler implementation", "type": "file", "priority": int64(4)}},
		{"node-5", []string{"File"}, map[string]interface{}{"title": "db.go", "content": "Database connection pooling", "type": "file", "priority": int64(5)}},
		{"node-6", []string{"Memory"}, map[string]interface{}{"title": "Security Review", "content": "Authentication security audit findings", "type": "memory", "priority": int64(6)}},
		{"node-7", []string{"Memory"}, map[string]interface{}{"title": "Login Flow", "content": "User authentication and session management", "type": "memory", "priority": int64(7)}},
		{"node-8", []string{"File"}, map[string]interface{}{"title": "session.go", "content": "Session authentication tokens", "type": "file", "priority": int64(8)}},
		{"node-9", []string{"Memory"}, map[string]interface{}{"title": "Unrelated Note", "content": "Something about testing frameworks", "type": "memory", "priority": int64(9)}},
		{"node-10", []string{"Memory"}, map[string]interface{}{"title": "Another Note", "content": "Documentation about deployment", "type": "memory", "priority": int64(10)}},
	}

	for _, n := range testNodes {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     n.labels,
			Properties: n.props,
		})
		require.NoError(t, err)
	}

	t.Run("basic YIELD without RETURN", func(t *testing.T) {
		// Basic fulltext search - should return node and score columns
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"node", "score"}, result.Columns)
		assert.GreaterOrEqual(t, len(result.Rows), 5, "Should find multiple authentication-related nodes")
	})

	t.Run("YIELD with RETURN projection", func(t *testing.T) {
		// Project specific properties from the node
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN elementId(node) AS id, node.title AS title, score
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "title", "score"}, result.Columns)
		assert.GreaterOrEqual(t, len(result.Rows), 5)

		// Verify first row has correct types
		if len(result.Rows) > 0 {
			assert.IsType(t, "", result.Rows[0][0], "id should be string")
			assert.IsType(t, "", result.Rows[0][1], "title should be string")
			assert.IsType(t, float64(0), result.Rows[0][2], "score should be float64")
		}
	})

	t.Run("YIELD with LIMIT", func(t *testing.T) {
		// Limit results to 3
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id AS id, score
			LIMIT 3
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows), "LIMIT 3 should return exactly 3 rows")
	})

	t.Run("YIELD with ORDER BY and LIMIT", func(t *testing.T) {
		// Order by score descending and limit
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN elementId(node) AS id, node.title AS title, score
			ORDER BY score DESC
			LIMIT 5
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows), "LIMIT 5 should return exactly 5 rows")

		// Verify scores are in descending order
		if len(result.Rows) >= 2 {
			for i := 0; i < len(result.Rows)-1; i++ {
				score1 := result.Rows[i][2].(float64)
				score2 := result.Rows[i+1][2].(float64)
				assert.GreaterOrEqual(t, score1, score2, "Scores should be in descending order")
			}
		}
	})

	t.Run("YIELD with ORDER BY ASC", func(t *testing.T) {
		// Order by score ascending
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id AS id, score
			ORDER BY score ASC
			LIMIT 5
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows))

		// Verify scores are in ascending order
		if len(result.Rows) >= 2 {
			for i := 0; i < len(result.Rows)-1; i++ {
				score1 := result.Rows[i][1].(float64)
				score2 := result.Rows[i+1][1].(float64)
				assert.LessOrEqual(t, score1, score2, "Scores should be in ascending order")
			}
		}
	})

	t.Run("YIELD with SKIP and LIMIT", func(t *testing.T) {
		// Get all results first for comparison
		allResult, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id AS id, score
			ORDER BY score DESC
		`, nil)
		require.NoError(t, err)

		// Now skip 2 and limit 3
		skipResult, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id AS id, score
			ORDER BY score DESC
			SKIP 2
			LIMIT 3
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(skipResult.Rows), "SKIP 2 LIMIT 3 should return 3 rows")

		// Verify the first result after skip matches the 3rd result from all
		if len(allResult.Rows) >= 3 && len(skipResult.Rows) >= 1 {
			assert.Equal(t, allResult.Rows[2][0], skipResult.Rows[0][0], "First skipped result should match 3rd overall result")
		}
	})

	t.Run("YIELD with LIMIT", func(t *testing.T) {
		// The YIELD's own LIMIT pages the yielded rows before the RETURN.
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			LIMIT 2
			RETURN node, score
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows), "YIELD … LIMIT 2 should return 2 rows")

		// A standalone call can't page its rows, as in Neo4j.
		_, err = exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			LIMIT 2
		`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot use standalone call")
	})

	t.Run("single line query format", func(t *testing.T) {
		// Test single-line query formatting
		result, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('node_search', 'authentication') YIELD node, score RETURN node.id AS id, score LIMIT 3", nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows))
		assert.Equal(t, []string{"id", "score"}, result.Columns)
	})

	t.Run("multiline query with tabs", func(t *testing.T) {
		// Test with tabs instead of spaces
		result, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('node_search', 'authentication')\n\tYIELD node, score\n\tRETURN node.id AS id, score\n\tLIMIT 3", nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows))
	})

	t.Run("RETURN without AS alias", func(t *testing.T) {
		// Return without aliasing
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id, node.title, score
			LIMIT 3
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"node.id", "node.title", "score"}, result.Columns)
		assert.Equal(t, 3, len(result.Rows))
	})

	t.Run("mixed alias and non-alias", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id AS id, node.title, score AS relevance
			LIMIT 3
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "node.title", "relevance"}, result.Columns)
	})

	t.Run("YIELD * with LIMIT", func(t *testing.T) {
		// YIELD * is only valid standalone, and a standalone call can't page
		// its rows, as in Neo4j.
		_, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD *
			LIMIT 2
		`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot use standalone call")
	})

	t.Run("empty result set with LIMIT", func(t *testing.T) {
		// Search for something that doesn't exist
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'xyznonexistent123')
			YIELD node, score
			RETURN node.id, score
			LIMIT 10
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(result.Rows), "Non-matching search should return empty result")
	})

	t.Run("LIMIT larger than result set", func(t *testing.T) {
		// LIMIT larger than actual results
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'authentication')
			YIELD node, score
			RETURN node.id, score
			LIMIT 1000
		`, nil)
		require.NoError(t, err)
		// Should return all matching rows, not error
		assert.LessOrEqual(t, len(result.Rows), 1000)
		assert.Greater(t, len(result.Rows), 0)
	})

	t.Run("node_search built-in index", func(t *testing.T) {
		// Verify node_search works without explicit index creation (Neo4j compatibility)
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'database')
			YIELD node, score
			RETURN elementId(node) AS id, node.title AS title, score
			ORDER BY score DESC
			LIMIT 5
		`, nil)
		require.NoError(t, err, "node_search should work without explicit index creation")
		assert.Greater(t, len(result.Rows), 0, "Should find database-related nodes")
	})

	t.Run("default built-in index", func(t *testing.T) {
		// Verify default index works
		_, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('default', 'database')
			YIELD node, score
			RETURN node.id, score
			LIMIT 3
		`, nil)
		require.NoError(t, err, "default index should work without explicit creation")
	})

	t.Run("non-existent index should error", func(t *testing.T) {
		// Custom index names that aren't built-in should error
		_, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('custom_nonexistent_index', 'test')
			YIELD node, score
			RETURN node.id, score
		`, nil)
		assert.Error(t, err, "Non-existent custom index should return error")
		assert.Contains(t, err.Error(), "index", "Error should mention missing index")
	})
}

func TestCallDbIndexFulltextQueryNodes_DirectBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"content": "alpha beta"},
	})
	require.NoError(t, err)

	// Empty query branch returns empty result without error.
	res, err := exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('default', '')")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.Rows)

	// Query with only negated terms yields no include terms.
	res, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('default', '-alpha NOT beta')")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.Rows)

	// Malformed call extraction path.
	res, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.Rows)

	// Non-built-in missing index should error (Neo4j compatibility path).
	_, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('missing_idx', 'alpha')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "there is no such fulltext schema index")

	// Create explicit fulltext index to exercise label/property filtering branches.
	_, err = exec.Execute(context.Background(), "CREATE FULLTEXT INDEX idx_ft IF NOT EXISTS FOR (n:Doc) ON EACH [n.content]", nil)
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:         "n2",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"content": "alpha must keep"},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:         "n3",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"content": "alpha beta must"}, // excluded by NOT beta
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:         "n4",
		Labels:     []string{"Other"},
		Properties: map[string]interface{}{"content": "alpha must"}, // filtered by label
	})
	require.NoError(t, err)

	// Must-have/exclude branches: only n2 should remain.
	res, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('idx_ft', '+must alpha NOT beta')")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	node, ok := res.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	assert.Equal(t, storage.NodeID("n2"), node.ID)
	_, scoreOK := res.Rows[0][1].(float64)
	assert.True(t, scoreOK)

	// Neo4j compatibility: 3rd options MAP arg supports skip/limit windowing.
	res, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('idx_ft', 'alpha', {skip: 1, limit: 1})")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)

	// Non-map third argument should fail like Neo4j signature enforcement.
	_, err = exec.callDbIndexFulltextQueryNodes("CALL db.index.fulltext.queryNodes('idx_ft', 'alpha', 5)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAP")
}

func TestCallDbIndexFulltextQueryNodes_ExecuteAcceptsThreeArgs(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Doc {content:'alpha'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX idx_exec IF NOT EXISTS FOR (n:Doc) ON EACH [n.content]", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('idx_exec', 'alpha', {limit: 1}) YIELD node, score RETURN node, score", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
}

// TestYieldReturnWithOtherProcedures tests YIELD...RETURN with various procedures
func TestYieldReturnWithOtherProcedures(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	store.CreateNode(&storage.Node{
		ID:     "person-1",
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"name": "Alice",
			"age":  int64(30),
		},
	})
	store.CreateNode(&storage.Node{
		ID:     "person-2",
		Labels: []string{"Person", "Developer"},
		Properties: map[string]interface{}{
			"name": "Bob",
			"age":  int64(25),
		},
	})

	t.Run("db.labels with YIELD and LIMIT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL db.labels()
			YIELD label
			RETURN label
			LIMIT 2
		`, nil)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(result.Rows), 2)
	})

	t.Run("dbms.procedures with YIELD and projection", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL dbms.procedures()
			YIELD name, signature
			RETURN name, signature
			LIMIT 5
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 5, len(result.Rows))
		assert.Equal(t, []string{"name", "signature"}, result.Columns)
	})

	t.Run("db.relationshipTypes with ORDER BY", func(t *testing.T) {
		// Create a relationship first
		store.CreateEdge(&storage.Edge{
			ID:        "rel-1",
			StartNode: "person-1",
			EndNode:   "person-2",
			Type:      "KNOWS",
		})

		result, err := exec.Execute(ctx, `
			CALL db.relationshipTypes()
			YIELD relationshipType
			RETURN relationshipType
			ORDER BY relationshipType ASC
		`, nil)
		require.NoError(t, err)
		assert.Greater(t, len(result.Rows), 0)
	})
}

// TestYieldReturnEdgeCases tests edge cases in YIELD...RETURN parsing
func TestYieldReturnEdgeCases(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create minimal test data
	store.CreateNode(&storage.Node{
		ID:         "test-1",
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "test", "content": "searchable content"},
	})

	t.Run("LIMIT 0 returns empty", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'searchable')
			YIELD node, score
			RETURN node.id, score
			LIMIT 0
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(result.Rows))
	})

	t.Run("SKIP beyond results returns empty", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'searchable')
			YIELD node, score
			RETURN node.id, score
			SKIP 1000
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(result.Rows))
	})

	t.Run("ORDER BY non-existent column gracefully handles", func(t *testing.T) {
		// Should not crash, may ignore the order or return error
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'searchable')
			YIELD node, score
			RETURN node.id AS id, score
			ORDER BY nonexistent DESC
			LIMIT 3
		`, nil)
		// Either succeeds with results or returns error - should not panic
		if err == nil {
			assert.LessOrEqual(t, len(result.Rows), 3)
		}
	})

	t.Run("multiple spaces between keywords", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			CALL db.index.fulltext.queryNodes('node_search', 'searchable')
			YIELD    node,    score
			RETURN   node.id   AS   id,   score
			ORDER    BY   score   DESC
			LIMIT    1
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})

	t.Run("case insensitive keywords", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			call db.index.fulltext.queryNodes('node_search', 'searchable')
			yield node, score
			return node.id as id, score
			order by score desc
			limit 1
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
	})
}
