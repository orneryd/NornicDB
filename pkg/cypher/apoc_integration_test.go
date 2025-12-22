package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAPOCFunctionsIntegration tests APOC functions work end-to-end
func TestAPOCFunctionsIntegration(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})
		CREATE (b:Person {name: 'Bob', age: 25})
		CREATE (c:Person {name: 'Carol', age: 35})
		CREATE (a)-[:KNOWS]->(b)
		CREATE (b)-[:KNOWS]->(c)
		CREATE (a)-[:KNOWS]->(c)
	`, nil)
	require.NoError(t, err)

	t.Run("nornicdb.version", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.version()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Version: %v", result.Rows[0])
	})

	t.Run("nornicdb.stats", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.stats()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Stats: %v", result.Rows[0])
	})

	t.Run("db.labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.labels()", nil)
		require.NoError(t, err)
		assert.True(t, len(result.Rows) > 0)
		t.Logf("Labels: %v", result.Rows)
	})

	t.Run("db.relationshipTypes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
		require.NoError(t, err)
		t.Logf("Relationship Types: %v", result.Rows)
	})

	t.Run("apoc.algo.pageRank", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.pageRank(nodes, {iterations: 20})
			YIELD node, score
			RETURN node.name as name, score
			ORDER BY score DESC
		`, nil)
		require.NoError(t, err)
		t.Logf("PageRank results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.algo.betweenness", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.betweenness(nodes)
			YIELD node, score
			RETURN node.name as name, score
		`, nil)
		require.NoError(t, err)
		t.Logf("Betweenness results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.neighbors.tohop", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'})
			CALL apoc.neighbors.tohop(start, 'KNOWS>', 2)
			YIELD node
			RETURN node.name as neighbor
		`, nil)
		require.NoError(t, err)
		t.Logf("Neighbors within 2 hops: %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v", row[0])
		}
	})

	t.Run("dbms.procedures", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL dbms.procedures()", nil)
		require.NoError(t, err)
		t.Logf("Available procedures: %d", len(result.Rows))
		// Show first 10
		for i, row := range result.Rows {
			if i >= 10 {
				t.Logf("  ... and %d more", len(result.Rows)-10)
				break
			}
			t.Logf("  %v", row[0])
		}
	})
}
