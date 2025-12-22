package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// DEAD CODE REMOVAL VERIFICATION TESTS
// =============================================================================
//
// This file documents dead code that was removed and prevents regression by
// verifying that all functionality works through the current code paths.
//
// REMOVED DEAD CODE (2024):
//
// 1. executeCreateRelationship (create.go) - 94 lines
//    - Was superseded by executeCreate which handles relationships via
//      parseCreateRelPatternWithVars
//    - Never called after the refactoring to unified CREATE handling
//
// 2. parseCreateRelPattern (create.go) - 118 lines
//    - Only called by executeCreateRelationship (which was dead)
//    - Replaced by parseCreateRelPatternWithVars
//
// 3. extractVariablesFromMatch (create.go) - 16 lines
//    - Never called from any code path
//    - Variable extraction is handled elsewhere in MATCH processing
//
// 4. executeImplicit (executor.go) - 25 lines
//    - Replaced by executeImplicitAsync for better performance
//    - Comment in code explicitly stated it was superseded
//
// TOTAL: ~253 lines of dead code removed
// =============================================================================

// TestRegressionPrevention_CreateRelationship verifies that all relationship
// creation patterns work through executeCreate, ensuring we don't need to
// re-add the removed executeCreateRelationship function.
func TestRegressionPrevention_CreateRelationship(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "simple relationship",
			query: `CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})`,
		},
		{
			name:  "reverse relationship",
			query: `CREATE (a:Person {name: 'Alice'})<-[:FOLLOWS]-(b:Person {name: 'Bob'})`,
		},
		{
			name:  "relationship with properties",
			query: `CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})`,
		},
		{
			name:  "multiple chained relationships",
			query: `CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)`,
		},
		{
			name:  "relationship with variable",
			query: `CREATE (a:Person)-[r:FRIEND]->(b:Person) RETURN r`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err, "All relationship patterns must work via executeCreate")
			assert.True(t, result.Stats.NodesCreated > 0 || result.Stats.RelationshipsCreated > 0,
				"Query should create nodes or relationships")
		})
	}
}

// TestRegressionPrevention_ImplicitTransaction verifies that all queries
// work through executeImplicitAsync, ensuring we don't need to re-add the
// removed executeImplicit function.
func TestRegressionPrevention_ImplicitTransaction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// All query types should work via executeImplicitAsync
	queries := []string{
		`CREATE (n:Test {name: 'test'})`,
		`MATCH (n:Test) RETURN n`,
		`MATCH (n:Test) SET n.updated = true RETURN n`,
		`MATCH (n:Test) DELETE n`,
	}

	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		if err != nil && !strings.Contains(err.Error(), "not found") {
			t.Errorf("Query %q failed: %v", q, err)
		}
	}
}

// TestRegressionPrevention_MatchVariableExtraction verifies that MATCH queries
// work correctly without extractVariablesFromMatch function.
func TestRegressionPrevention_MatchVariableExtraction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	// All MATCH patterns should work without extractVariablesFromMatch
	matchTests := []struct {
		query string
		desc  string
	}{
		{`MATCH (n:Person) RETURN n.name`, "simple node match"},
		{`MATCH (n:Person {name: 'Alice'}) RETURN n`, "match with properties"},
		{`MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name`, "relationship match"},
		{`MATCH (n) WHERE n.name = 'Alice' RETURN n`, "match with WHERE"},
	}

	for _, tt := range matchTests {
		t.Run(tt.desc, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err, "Query should work: %s", tt.query)
			assert.True(t, len(result.Rows) > 0, "Should return results for: %s", tt.query)
		})
	}
}
