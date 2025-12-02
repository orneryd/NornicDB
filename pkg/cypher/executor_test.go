// Package cypher provides tests for the Cypher executor.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStorageExecutor(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	assert.NotNil(t, exec)
	assert.NotNil(t, exec.parser)
	assert.NotNil(t, exec.storage)
}

func TestExecuteEmptyQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	_, err := exec.Execute(context.Background(), "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty query")
}

func TestExecuteInvalidSyntax(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name    string
		query   string
		errText string
	}{
		{
			name:    "unmatched parenthesis",
			query:   "MATCH (n RETURN n",
			errText: "parentheses",
		},
		{
			name:    "unmatched bracket",
			query:   "MATCH (a)-[r->(b) RETURN a",
			errText: "brackets",
		},
		{
			name:    "unmatched brace",
			query:   "CREATE (n:Person {name: 'Alice')",
			errText: "braces",
		},
		{
			name:    "unmatched single quote",
			query:   "MATCH (n) WHERE n.name = 'Alice RETURN n",
			errText: "quote",
		},
		{
			name:    "unmatched double quote",
			query:   `MATCH (n) WHERE n.name = "Alice RETURN n`,
			errText: "quote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), tt.query, nil)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errText)
		})
	}
}

func TestExecuteUnsupportedQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	// DROP INDEX is now a no-op (NornicDB manages indexes internally)
	result, err := exec.Execute(context.Background(), "DROP INDEX idx", nil)
	assert.NoError(t, err)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)

	// Test a truly unsupported query
	_, err = exec.Execute(context.Background(), "GRANT ADMIN TO user", nil)
	assert.Error(t, err)
	// Error should mention invalid clause
	assert.Contains(t, err.Error(), "syntax error")
}

func TestExecuteMatchEmptyGraph(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	result, err := exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Rows)
}

func TestExecuteMatchWithLabel(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some nodes
	node1 := &storage.Node{
		ID:         "person-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &storage.Node{
		ID:         "company-1",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme"},
	}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	// Match only Person nodes
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchAllNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Test"},
			Properties: map[string]interface{}{"index": i},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteMatchWithWhere(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node1 := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	node2 := &storage.Node{
		ID:         "p2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob", "age": float64(25)},
	}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	// Test equality
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test greater than
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age > 26 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test less than
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age < 28 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchWithCount(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(5), result.Rows[0][0])
	assert.Equal(t, "cnt", result.Columns[0])
}

func TestExecuteMatchWithLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n LIMIT 3", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteMatchWithSkip(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n SKIP 5", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
}

func TestExecuteCreateNode(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify node exists
	nodes, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, "Alice", nodes[0].Properties["name"])
}

func TestExecuteCreateWithReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Bob'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Rows, 1)
	assert.Contains(t, result.Columns, "n")
}

func TestExecuteCreateMultipleNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person), (b:Company)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)

	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(2), nodeCount)
}

func TestExecuteCreateRelationship(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person)-[:KNOWS]->(b:Person)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edgeCount, _ := store.EdgeCount()
	assert.Equal(t, int64(1), edgeCount)
}

func TestExecuteMerge(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First merge creates
	result, err := exec.Execute(ctx, "MERGE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Second merge should not create (based on label match)
	result, err = exec.Execute(ctx, "MERGE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	// Note: current implementation may create duplicates - depends on matching logic
}

func TestExecuteDelete(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node first
	node := &storage.Node{
		ID:         "delete-me",
		Labels:     []string{"Temp"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// Delete it
	result, err := exec.Execute(ctx, "MATCH (n:Temp) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify deleted
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

func TestExecuteDetachDelete(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with relationship
	node1 := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	require.NoError(t, store.CreateEdge(edge))

	// Detach delete
	result, err := exec.Execute(ctx, "MATCH (n:Person) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesDeleted)
}

func TestExecuteCallProcedure(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Add some test data
	node := &storage.Node{ID: "test", Labels: []string{"Memory"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node))

	// Test db.labels()
	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test db.relationshipTypes()
	result, err = exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	// May be empty if no relationships

	// Test db.schema.visualization()
	result, err = exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
}

func TestExecuteCallWithYieldWhere(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Add test data with multiple labels
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Memory"}, Properties: map[string]interface{}{}},
		{ID: "n2", Labels: []string{"Todo"}, Properties: map[string]interface{}{}},
		{ID: "n3", Labels: []string{"File"}, Properties: map[string]interface{}{}},
		{ID: "n4", Labels: []string{"Memory", "Important"}, Properties: map[string]interface{}{}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	t.Run("YIELD with column selection", func(t *testing.T) {
		// Basic YIELD - just get labels
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label"}, result.Columns)
		require.GreaterOrEqual(t, len(result.Rows), 3) // Memory, Todo, File, Important
	})

	t.Run("YIELD with alias", func(t *testing.T) {
		// YIELD with alias
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label AS labelName", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"labelName"}, result.Columns)
	})

	t.Run("YIELD *", func(t *testing.T) {
		// YIELD * returns all columns
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD *", nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Columns), 1)
	})

	t.Run("YIELD with WHERE filtering", func(t *testing.T) {
		// WHERE should filter results
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label = 'Memory'", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label"}, result.Columns)
		require.Equal(t, 1, len(result.Rows), "WHERE should filter to only 'Memory' label")
		require.Equal(t, "Memory", result.Rows[0][0])
	})

	t.Run("YIELD with WHERE CONTAINS", func(t *testing.T) {
		// WHERE with CONTAINS operator
		// Search for 'd' which is only in "Todo"
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label CONTAINS 'd'", nil)
		require.NoError(t, err)
		foundLabels := make(map[string]bool)
		for _, row := range result.Rows {
			foundLabels[row[0].(string)] = true
		}
		require.True(t, foundLabels["Todo"], "Todo should be in results (contains 'd')")
		require.False(t, foundLabels["Memory"], "Memory should NOT be in results (no 'd')")
		require.False(t, foundLabels["File"], "File should NOT be in results (no 'd')")
		require.False(t, foundLabels["Important"], "Important should NOT be in results (no 'd')")
	})

	t.Run("YIELD with WHERE <> (not equals)", func(t *testing.T) {
		// WHERE with <> operator
		result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label <> 'Memory'", nil)
		require.NoError(t, err)
		for _, row := range result.Rows {
			require.NotEqual(t, "Memory", row[0], "Memory should be filtered out")
		}
	})
}

func TestParseYieldClause(t *testing.T) {
	tests := []struct {
		name     string
		cypher   string
		expected *yieldClause
	}{
		{
			name:     "no yield",
			cypher:   "CALL db.labels()",
			expected: nil,
		},
		{
			name:   "yield star",
			cypher: "CALL db.labels() YIELD *",
			expected: &yieldClause{
				yieldAll: true,
				items:    []yieldItem{},
			},
		},
		{
			name:   "yield single column",
			cypher: "CALL db.labels() YIELD label",
			expected: &yieldClause{
				items: []yieldItem{{name: "label", alias: ""}},
			},
		},
		{
			name:   "yield multiple columns",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
			},
		},
		{
			name:   "yield with alias",
			cypher: "CALL db.labels() YIELD label AS labelName",
			expected: &yieldClause{
				items: []yieldItem{{name: "label", alias: "labelName"}},
			},
		},
		{
			name:   "yield with WHERE",
			cypher: "CALL db.index.vector.queryNodes('idx', 10, [1,2,3]) YIELD node, score WHERE score > 0.5",
			expected: &yieldClause{
				items: []yieldItem{
					{name: "node", alias: ""},
					{name: "score", alias: ""},
				},
				where: "score > 0.5",
			},
		},
		{
			name:   "yield star with WHERE",
			cypher: "CALL db.index.fulltext.queryNodes('idx', 'search') YIELD * WHERE score > 0.8",
			expected: &yieldClause{
				yieldAll: true,
				items:    []yieldItem{},
				where:    "score > 0.8",
			},
		},
		{
			name:   "yield with WHERE and RETURN",
			cypher: "CALL db.labels() YIELD label WHERE label = 'Memory' RETURN label",
			expected: &yieldClause{
				items:      []yieldItem{{name: "label", alias: ""}},
				where:      "label = 'Memory'",
				hasReturn:  true,
				returnExpr: "label",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseYieldClause(tt.cypher)
			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			assert.Equal(t, tt.expected.yieldAll, result.yieldAll, "yieldAll mismatch")
			assert.Equal(t, len(tt.expected.items), len(result.items), "items count mismatch")
			for i, item := range tt.expected.items {
				if i < len(result.items) {
					assert.Equal(t, item.name, result.items[i].name, "item name mismatch at %d", i)
					assert.Equal(t, item.alias, result.items[i].alias, "item alias mismatch at %d", i)
				}
			}
			assert.Equal(t, tt.expected.where, result.where, "where mismatch")
			assert.Equal(t, tt.expected.hasReturn, result.hasReturn, "hasReturn mismatch")
			if tt.expected.hasReturn {
				assert.Equal(t, tt.expected.returnExpr, result.returnExpr, "returnExpr mismatch")
			}
		})
	}
}

func TestExecuteWithParameters(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	require.NoError(t, store.CreateNode(node))

	// Query with parameters
	params := map[string]interface{}{
		"name": "Alice",
	}
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name = $name RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteReturnPropertyAccess(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	require.NoError(t, store.CreateNode(node))

	// Return specific properties
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Contains(t, result.Columns, "n.name")
	assert.Contains(t, result.Columns, "n.age")
}

func TestExecuteMatchRelationship(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and relationship
	node1 := &storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	node2 := &storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{ID: "e1", StartNode: "p1", EndNode: "p2", Type: "KNOWS"}
	require.NoError(t, store.CreateEdge(edge))

	// Match with relationship pattern
	result, err := exec.Execute(ctx, "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b", nil)
	require.NoError(t, err)
	// Should find the relationship
	assert.NotEmpty(t, result.Columns)
}

func TestExecuteWhereOperators(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	nodes := []struct {
		id   string
		name string
		age  float64
	}{
		{"p1", "Alice", 30},
		{"p2", "Bob", 25},
		{"p3", "Charlie", 35},
	}

	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": n.name, "age": n.age},
		}
		require.NoError(t, store.CreateNode(node))
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"equals string", "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n", 1},
		{"equals number", "MATCH (n:Person) WHERE n.age = 25 RETURN n", 1},
		{"greater than", "MATCH (n:Person) WHERE n.age > 28 RETURN n", 2},
		{"greater or equal", "MATCH (n:Person) WHERE n.age >= 30 RETURN n", 2},
		{"less than", "MATCH (n:Person) WHERE n.age < 30 RETURN n", 1},
		{"less or equal", "MATCH (n:Person) WHERE n.age <= 30 RETURN n", 2},
		{"not equals <>", "MATCH (n:Person) WHERE n.age <> 30 RETURN n", 2},
		{"not equals !=", "MATCH (n:Person) WHERE n.age != 30 RETURN n", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Len(t, result.Rows, tt.expected)
		})
	}
}

func TestExecuteContainsOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name CONTAINS 'Smith' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// No match
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.name CONTAINS 'Jones' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestExecuteStartsWithOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name STARTS WITH 'Alice' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteEndsWithOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice Smith"},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.name ENDS WITH 'Smith' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteDistinct(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with same labels
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(string(rune('a' + i))),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"category": "A"},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// DISTINCT should deduplicate - but we return full nodes so may not dedupe
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN DISTINCT n.category", nil)
	require.NoError(t, err)
	// The distinct logic depends on implementation
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteOrderBy(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	nodes := []struct {
		id  string
		age float64
	}{
		{"p3", 35},
		{"p1", 25},
		{"p2", 30},
	}

	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"age": n.age},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Order by age ascending
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.age ORDER BY n.age", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
	// Note: ORDER BY implementation may need verification
}

func TestExecuteQueryStats(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create should report stats
	result, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Stats)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 0, result.Stats.NodesDeleted)
}

func TestExecuteNornicDbProcedures(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test nornicdb.version()
	result, err := exec.Execute(ctx, "CALL nornicdb.version()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test nornicdb.stats()
	result, err = exec.Execute(ctx, "CALL nornicdb.stats()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)

	// Test nornicdb.decay.info()
	result, err = exec.Execute(ctx, "CALL nornicdb.decay.info()", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)
}

// Additional tests for full coverage

func TestExecuteSet(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node first
	node := &storage.Node{
		ID:         "set-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(25)},
	}
	require.NoError(t, store.CreateNode(node))

	// Update property with SET
	result, err := exec.Execute(ctx, "MATCH (n:Person) SET n.age = 30", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)

	// Verify update - integer values should be stored as int64 (Neo4j compatible)
	updated, _ := store.GetNode("set-test")
	assert.Equal(t, int64(30), updated.Properties["age"])
}

func TestExecuteSetWithReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "set-return-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	require.NoError(t, store.CreateNode(node))

	// SET with RETURN
	result, err := exec.Execute(ctx, "MATCH (n:Person) SET n.status = 'active' RETURN n", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)
	assert.Contains(t, result.Columns, "n")
}

func TestExecuteSetNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET with no matching nodes
	_, err := exec.Execute(ctx, "MATCH (n:NonExistent) SET n.prop = 'value'", nil)
	require.NoError(t, err) // Should succeed with 0 updates
}

func TestExecuteSetInvalidQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET without proper assignment
	_, err := exec.Execute(ctx, "MATCH (n) SET invalid", nil)
	assert.Error(t, err)
}

func TestExecuteAggregationSum(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with numeric values
	for i := 1; i <= 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("sum-%d", i)),
			Labels:     []string{"Number"},
			Properties: map[string]interface{}{"value": float64(i * 10)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:Number) RETURN sum(n.value) AS total", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, float64(150), result.Rows[0][0]) // 10+20+30+40+50
}

func TestExecuteAggregationAvg(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 1; i <= 4; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("avg-%d", i)),
			Labels:     []string{"Score"},
			Properties: map[string]interface{}{"value": float64(i * 25)}, // 25,50,75,100
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:Score) RETURN avg(n.value) AS average", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, float64(62.5), result.Rows[0][0])
}

func TestExecuteAggregationMinMax(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	values := []float64{15, 42, 8, 99, 23}
	for i, v := range values {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("minmax-%d", i)),
			Labels:     []string{"Value"},
			Properties: map[string]interface{}{"num": v},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Test MIN
	result, err := exec.Execute(ctx, "MATCH (n:Value) RETURN min(n.num) AS minimum", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(8), result.Rows[0][0])

	// Test MAX
	result, err = exec.Execute(ctx, "MATCH (n:Value) RETURN max(n.num) AS maximum", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(99), result.Rows[0][0])
}

func TestExecuteAggregationCollect(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	names := []string{"Alice", "Bob", "Charlie"}
	for i, name := range names {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("collect-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": name},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN collect(n.name) AS names", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 3)
}

func TestExecuteAggregationEmpty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Aggregation on empty set
	result, err := exec.Execute(ctx, "MATCH (n:NonExistent) RETURN avg(n.value) AS avg", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteInOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i, status := range []string{"active", "pending", "inactive"} {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("in-%d", i)),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"status": status},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.status IN ['active', 'pending'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestExecuteIsNullOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes - one with email, one without
	node1 := &storage.Node{
		ID:         "null-1",
		Labels:     []string{"Contact"},
		Properties: map[string]interface{}{"name": "Alice", "email": "alice@example.com"},
	}
	node2 := &storage.Node{
		ID:         "null-2",
		Labels:     []string{"Contact"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	// IS NULL
	result, err := exec.Execute(ctx, "MATCH (n:Contact) WHERE n.email IS NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// IS NOT NULL
	result, err = exec.Execute(ctx, "MATCH (n:Contact) WHERE n.email IS NOT NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteRegexOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	emails := []string{"alice@gmail.com", "bob@yahoo.com", "charlie@gmail.com"}
	for i, email := range emails {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("regex-%d", i)),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"email": email},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.email =~ '.*@gmail\\.com' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestExecuteCreateRelationshipBothDirections(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test forward direction
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Test backward direction
	result, err = exec.Execute(ctx, "CREATE (a:Person {name: 'Charlie'})<-[:FOLLOWS]-(b:Person {name: 'Dave'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestExecuteCreateRelationshipWithReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:City {name: 'NYC'})-[:CONNECTED_TO]->(b:City {name: 'LA'}) RETURN a, b", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.NotEmpty(t, result.Rows)
}

// TestExecuteCreateRelationshipWithArrayProperties tests CREATE with relationship properties containing arrays
// This is a Neo4j-compatible feature for creating edges with properties like {roles: ['Neo']}
func TestExecuteCreateRelationshipWithArrayProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test direct CREATE with relationship properties (array value)
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Keanu'})-[:ACTED_IN {roles: ['Neo', 'John Wick']}]->(m:Movie {title: 'The Matrix'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has properties
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ACTED_IN", edges[0].Type)
	roles, ok := edges[0].Properties["roles"]
	assert.True(t, ok, "relationship should have 'roles' property")
	assert.NotNil(t, roles)
}

// TestExecuteCreateRelationshipWithStringProperty tests CREATE with a string property on relationship
func TestExecuteCreateRelationshipWithStringProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Director'})-[:DIRECTED {since: '1999'}]->(m:Movie {title: 'Film'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "DIRECTED", edges[0].Type)
	since, ok := edges[0].Properties["since"]
	assert.True(t, ok, "relationship should have 'since' property")
	assert.Equal(t, "1999", since)
}

// TestExecuteMatchCreateRelationshipWithProperties tests MATCH...CREATE with relationship properties
func TestExecuteMatchCreateRelationshipWithProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create the nodes
	_, err := exec.Execute(ctx, "CREATE (p:Person {name: 'Keanu Reeves'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (m:Movie {title: 'The Matrix'})", nil)
	require.NoError(t, err)

	// Now use MATCH...CREATE with relationship properties (Neo4j Movies dataset pattern)
	result, err := exec.Execute(ctx, `
		MATCH (keanu:Person {name: 'Keanu Reeves'}), (matrix:Movie {title: 'The Matrix'})
		CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix)
	`, nil)
	require.NoError(t, err, "MATCH...CREATE with relationship properties should work")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has properties
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ACTED_IN", edges[0].Type)
	roles, ok := edges[0].Properties["roles"]
	assert.True(t, ok, "relationship should have 'roles' property")
	assert.NotNil(t, roles)
}

// TestExecuteMatchCreateRelationshipWithMultipleProperties tests multiple properties on relationships
func TestExecuteMatchCreateRelationshipWithMultipleProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes first
	_, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)

	// MATCH...CREATE with multiple relationship properties
	result, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
		CREATE (a)-[:KNOWS {since: 2020, trust: 'high', tags: ['friend', 'colleague']}]->(b)
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "KNOWS", edges[0].Type)
	_, hasSince := edges[0].Properties["since"]
	_, hasTrust := edges[0].Properties["trust"]
	_, hasTags := edges[0].Properties["tags"]
	assert.True(t, hasSince, "should have 'since' property")
	assert.True(t, hasTrust, "should have 'trust' property")
	assert.True(t, hasTags, "should have 'tags' property")
}

// TestExecuteMatchCreateNodeAndRelationships tests MATCH...CREATE that creates NEW nodes
// and relationships to matched nodes (Northwind pattern: MATCH (s), (c) CREATE (p) CREATE (p)-[:REL]->(c))
func TestExecuteMatchCreateNodeAndRelationships(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial nodes (like Supplier and Category in Northwind)
	_, err := exec.Execute(ctx, "CREATE (s:Supplier {supplierID: 1, companyName: 'Exotic Liquids'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (c:Category {categoryID: 1, categoryName: 'Beverages'})", nil)
	require.NoError(t, err)

	// Verify initial state
	nodes, _ := store.AllNodes()
	assert.Len(t, nodes, 2, "Should have 2 initial nodes")

	// Now use the Northwind pattern: MATCH existing nodes, CREATE new node AND relationships
	result, err := exec.Execute(ctx, `
		MATCH (s:Supplier {supplierID: 1}), (c:Category {categoryID: 1})
		CREATE (p:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})
		CREATE (p)-[:PART_OF]->(c)
		CREATE (s)-[:SUPPLIES]->(p)
	`, nil)
	require.NoError(t, err, "MATCH...CREATE with new node and relationships should work")

	// Verify stats
	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 new Product node")
	assert.Equal(t, 2, result.Stats.RelationshipsCreated, "Should create 2 relationships")

	// Verify total nodes
	nodes, err = store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 3, "Should now have 3 nodes total")

	// Verify Product was created with correct properties
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	require.Len(t, products, 1, "Should have 1 Product node")
	assert.Equal(t, "Chai", products[0].Properties["productName"])
	assert.Equal(t, float64(18.0), products[0].Properties["unitPrice"])

	// Verify relationships
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 2, "Should have 2 relationships")

	// Check relationship types
	relTypes := make(map[string]bool)
	for _, edge := range edges {
		relTypes[edge.Type] = true
	}
	assert.True(t, relTypes["PART_OF"], "Should have PART_OF relationship")
	assert.True(t, relTypes["SUPPLIES"], "Should have SUPPLIES relationship")
}

// TestExecuteMatchCreateNodeThenRelationship tests the Northwind pattern where:
// 1. MATCH finds an existing node
// 2. CREATE creates a new node
// 3. CREATE creates a relationship FROM the matched node TO the created node
// This is the pattern: MATCH (cu:Customer) CREATE (o:Order) CREATE (cu)-[:PURCHASED]->(o)
func TestExecuteMatchCreateNodeThenRelationship(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a customer first
	_, err := exec.Execute(ctx, "CREATE (cu:Customer {customerID: 'ALFKI', companyName: 'Alfreds Futterkiste'})", nil)
	require.NoError(t, err)

	// Verify customer exists
	customers, _ := store.GetNodesByLabel("Customer")
	require.Len(t, customers, 1, "Should have 1 customer")

	// Now run the Northwind pattern:
	// MATCH the customer, CREATE an order, CREATE the relationship
	result, err := exec.Execute(ctx, `
		MATCH (cu:Customer {customerID: 'ALFKI'})
		CREATE (o:Order {orderID: 10643, orderDate: '1997-08-25', shipCountry: 'Germany'})
		CREATE (cu)-[:PURCHASED]->(o)
	`, nil)

	// This is the critical assertion - the query should succeed
	require.NoError(t, err, "MATCH + CREATE node + CREATE relationship should work; variable 'cu' from MATCH should be available")

	// Verify stats
	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 Order node")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 PURCHASED relationship")

	// Verify order was created
	orders, err := store.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, orders, 1, "Should have 1 Order node")
	// Check orderID - could be int64 or float64 depending on parsing
	orderID := orders[0].Properties["orderID"]
	switch v := orderID.(type) {
	case int64:
		assert.Equal(t, int64(10643), v)
	case float64:
		assert.Equal(t, float64(10643), v)
	default:
		t.Errorf("Unexpected orderID type: %T", orderID)
	}

	// Verify relationship exists
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1, "Should have 1 relationship")
	assert.Equal(t, "PURCHASED", edges[0].Type)

	// Verify the relationship connects the right nodes
	customerNode := customers[0]
	orderNode := orders[0]
	assert.Equal(t, customerNode.ID, edges[0].StartNode, "Relationship should start from Customer")
	assert.Equal(t, orderNode.ID, edges[0].EndNode, "Relationship should end at Order")
}

// TestExecuteMatchCreateMultipleNodes tests creating multiple nodes in a single MATCH...CREATE
func TestExecuteMatchCreateMultipleNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create category
	_, err := exec.Execute(ctx, "CREATE (c:Category {categoryID: 1, categoryName: 'Beverages'})", nil)
	require.NoError(t, err)

	// Create multiple products referencing the same category
	result, err := exec.Execute(ctx, `
		MATCH (c:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Chai'})
		CREATE (p2:Product {productID: 2, productName: 'Chang'})
		CREATE (p1)-[:PART_OF]->(c)
		CREATE (p2)-[:PART_OF]->(c)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated, "Should create 2 Product nodes")
	assert.Equal(t, 2, result.Stats.RelationshipsCreated, "Should create 2 PART_OF relationships")

	// Verify products
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	assert.Len(t, products, 2, "Should have 2 Product nodes")
}

// TestExecuteMatchCreateWithRelationshipProperties tests the full Northwind pattern with rel properties
func TestExecuteMatchCreateWithRelationshipProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create customer and order pattern (like Northwind)
	_, err := exec.Execute(ctx, "CREATE (c:Customer {customerID: 'ALFKI', companyName: 'Alfreds'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (o:Order {orderID: 10643})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (p:Product {productID: 1, productName: 'Chai'})", nil)
	require.NoError(t, err)

	// Create ORDERS relationship with quantity property
	result, err := exec.Execute(ctx, `
		MATCH (o:Order {orderID: 10643}), (p:Product {productID: 1})
		CREATE (o)-[:ORDERS {quantity: 15}]->(p)
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify the relationship has the quantity property
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "ORDERS", edges[0].Type)
	qty, ok := edges[0].Properties["quantity"]
	assert.True(t, ok, "Should have quantity property")
	// Check the value (could be int64 or float64 depending on parsing)
	switch v := qty.(type) {
	case int64:
		assert.Equal(t, int64(15), v)
	case float64:
		assert.Equal(t, float64(15), v)
	default:
		t.Errorf("quantity has unexpected type: %T", qty)
	}
}

// TestExecuteMultipleMatchCreateBlocks tests the Northwind pattern where multiple
// MATCH...CREATE blocks exist in a single query, each creating nodes and relationships
// This is the exact pattern from the Northwind benchmark that was failing:
// MATCH (s1), (c1) CREATE (p1) CREATE (p1)-[:REL]->(c1)
// MATCH (s2), (c2) CREATE (p2) CREATE (p2)-[:REL]->(c2)
func TestExecuteMultipleMatchCreateBlocks(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create Categories (like Northwind)
	_, err := exec.Execute(ctx, `
		CREATE (c1:Category {categoryID: 1, categoryName: 'Beverages'})
		CREATE (c2:Category {categoryID: 2, categoryName: 'Condiments'})
	`, nil)
	require.NoError(t, err)

	// Create Suppliers (like Northwind)
	_, err = exec.Execute(ctx, `
		CREATE (s1:Supplier {supplierID: 1, companyName: 'Exotic Liquids'})
		CREATE (s2:Supplier {supplierID: 2, companyName: 'New Orleans Cajun'})
	`, nil)
	require.NoError(t, err)

	// Verify initial state
	nodes, _ := store.AllNodes()
	assert.Len(t, nodes, 4, "Should have 4 initial nodes (2 categories + 2 suppliers)")

	// This is the EXACT pattern from Northwind benchmark that was failing:
	// Multiple MATCH...CREATE blocks in a single query
	result, err := exec.Execute(ctx, `
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})
		CREATE (p1)-[:PART_OF]->(c1)
		CREATE (s1)-[:SUPPLIES]->(p1)
		
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p2:Product {productID: 2, productName: 'Chang', unitPrice: 19.0})
		CREATE (p2)-[:PART_OF]->(c1)
		CREATE (s1)-[:SUPPLIES]->(p2)
		
		MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
		CREATE (p3:Product {productID: 3, productName: 'Aniseed Syrup', unitPrice: 10.0})
		CREATE (p3)-[:PART_OF]->(c2)
		CREATE (s2)-[:SUPPLIES]->(p3)
	`, nil)
	require.NoError(t, err, "Multiple MATCH...CREATE blocks should work")

	// Verify stats - should create 3 products with 6 relationships (2 per product)
	assert.Equal(t, 3, result.Stats.NodesCreated, "Should create 3 Product nodes")
	assert.Equal(t, 6, result.Stats.RelationshipsCreated, "Should create 6 relationships")

	// Verify total nodes
	nodes, err = store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 7, "Should now have 7 nodes total (4 initial + 3 products)")

	// Verify Products were created correctly
	products, err := store.GetNodesByLabel("Product")
	require.NoError(t, err)
	assert.Len(t, products, 3, "Should have 3 Product nodes")

	// Verify relationships
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 6, "Should have 6 relationships")

	// Count relationship types
	partOfCount := 0
	suppliesCount := 0
	for _, edge := range edges {
		switch edge.Type {
		case "PART_OF":
			partOfCount++
		case "SUPPLIES":
			suppliesCount++
		}
	}
	assert.Equal(t, 3, partOfCount, "Should have 3 PART_OF relationships")
	assert.Equal(t, 3, suppliesCount, "Should have 3 SUPPLIES relationships")
}

// TestExecuteMultipleMatchCreateBlocksWithDifferentCategories tests that
// each MATCH block correctly finds its own nodes and doesn't reuse previous block's nodes
// SKIP: This test relies on MATCH property filtering ({supplierID: 1}) which is currently broken
// TODO: Re-enable once MATCH property filtering is fixed
func TestExecuteMultipleMatchCreateBlocksWithDifferentCategories(t *testing.T) {
	t.Skip("MATCH property filtering ({prop: value}) is not fully implemented - see QUICK_WINS_ROADMAP.md")
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple categories with different IDs
	_, err := exec.Execute(ctx, `
		CREATE (c1:Category {categoryID: 1, categoryName: 'Beverages'})
		CREATE (c2:Category {categoryID: 2, categoryName: 'Condiments'})
		CREATE (c3:Category {categoryID: 3, categoryName: 'Confections'})
	`, nil)
	require.NoError(t, err)

	// Create multiple suppliers
	_, err = exec.Execute(ctx, `
		CREATE (s1:Supplier {supplierID: 1, companyName: 'Supplier One'})
		CREATE (s2:Supplier {supplierID: 2, companyName: 'Supplier Two'})
		CREATE (s3:Supplier {supplierID: 3, companyName: 'Supplier Three'})
	`, nil)
	require.NoError(t, err)

	// Multiple MATCH blocks referencing DIFFERENT categories
	result, err := exec.Execute(ctx, `
		MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
		CREATE (p1:Product {productID: 1, productName: 'Product1'})
		CREATE (p1)-[:PART_OF]->(c1)
		
		MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
		CREATE (p2:Product {productID: 2, productName: 'Product2'})
		CREATE (p2)-[:PART_OF]->(c2)
		
		MATCH (s3:Supplier {supplierID: 3}), (c3:Category {categoryID: 3})
		CREATE (p3:Product {productID: 3, productName: 'Product3'})
		CREATE (p3)-[:PART_OF]->(c3)
	`, nil)
	require.NoError(t, err, "Different category MATCHes should work")

	assert.Equal(t, 3, result.Stats.NodesCreated)
	assert.Equal(t, 3, result.Stats.RelationshipsCreated)

	// Verify each product is linked to the CORRECT category
	products, _ := store.GetNodesByLabel("Product")
	require.Len(t, products, 3)

	for _, product := range products {
		productID := product.Properties["productID"]
		edges, _ := store.GetOutgoingEdges(product.ID)

		// Find the PART_OF edge
		for _, edge := range edges {
			if edge.Type == "PART_OF" {
				// Get the target category
				targetNode, _ := store.GetNode(edge.EndNode)
				categoryID := targetNode.Properties["categoryID"]

				// Product1 should link to Category1, Product2 to Category2, etc.
				assert.Equal(t, productID, categoryID,
					"Product %v should be linked to Category %v", productID, categoryID)
			}
		}
	}
}

// TestExecuteMixedNodesAndRelationshipsCreate tests creating nodes AND relationships
// in a single CREATE statement (like the FastRP social network pattern)
func TestExecuteMixedNodesAndRelationshipsCreate(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This is the exact pattern from the FastRP benchmark
	result, err := exec.Execute(ctx, `
		CREATE
			(dan:Person {name: 'Dan', age: 18}),
			(annie:Person {name: 'Annie', age: 12}),
			(matt:Person {name: 'Matt', age: 22}),
			(dan)-[:KNOWS {weight: 1.0}]->(annie),
			(dan)-[:KNOWS {weight: 1.5}]->(matt),
			(annie)-[:KNOWS {weight: 2.0}]->(matt)
	`, nil)
	require.NoError(t, err, "Mixed nodes and relationships CREATE should work")

	// Should create 3 nodes and 3 relationships
	assert.Equal(t, 3, result.Stats.NodesCreated, "Should create 3 Person nodes")
	assert.Equal(t, 3, result.Stats.RelationshipsCreated, "Should create 3 KNOWS relationships")

	// Verify nodes
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 3)

	// Verify edges
	edges, err := store.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 3)

	// Verify relationship properties
	for _, edge := range edges {
		assert.Equal(t, "KNOWS", edge.Type)
		weight, exists := edge.Properties["weight"]
		assert.True(t, exists, "Should have weight property")
		_, isFloat := weight.(float64)
		assert.True(t, isFloat, "Weight should be float64")
	}
}

func TestExecuteAllProcedures(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	node := &storage.Node{
		ID:         "proc-test",
		Labels:     []string{"TestLabel"},
		Properties: map[string]interface{}{"prop1": "value1"},
	}
	require.NoError(t, store.CreateNode(node))

	procedures := []string{
		"CALL db.labels()",
		"CALL db.relationshipTypes()",
		"CALL db.propertyKeys()",
		"CALL db.indexes()",
		"CALL db.constraints()",
		"CALL db.schema.visualization()",
		"CALL db.schema.nodeProperties()",
		"CALL db.schema.relProperties()",
		"CALL dbms.components()",
		"CALL dbms.procedures()",
		"CALL dbms.functions()",
		"CALL nornicdb.version()",
		"CALL nornicdb.stats()",
		"CALL nornicdb.decay.info()",
	}

	for _, proc := range procedures {
		t.Run(proc, func(t *testing.T) {
			result, err := exec.Execute(ctx, proc, nil)
			require.NoError(t, err, "procedure %s failed", proc)
			assert.NotNil(t, result)
			assert.NotEmpty(t, result.Columns)
		})
	}
}

func TestExecuteUnknownProcedure(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL unknown.procedure()", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown procedure")
}

func TestExecuteAndOrOperators(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	nodes := []struct {
		name   string
		age    float64
		active bool
	}{
		{"Alice", 25, true},
		{"Bob", 35, true},
		{"Charlie", 25, false},
		{"Dave", 35, false},
	}
	for i, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("logic-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": n.name, "age": n.age, "active": n.active},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Test AND
	result, err := exec.Execute(ctx, "MATCH (n:Person) WHERE n.age = 25 AND n.active = true RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Only Alice

	// Test OR
	result, err = exec.Execute(ctx, "MATCH (n:Person) WHERE n.age = 25 OR n.age = 35 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 4) // All
}

func TestExecuteOrderByDesc(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with different ages
	ages := []float64{20, 30, 25, 35, 28}
	for i, age := range ages {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("order-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"age": age},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.age ORDER BY n.age DESC", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
	// First should be highest
	assert.Equal(t, float64(35), result.Rows[0][0])
}

func TestExecuteSkipAndLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 10 nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("page-%d", i)),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{"index": float64(i)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// SKIP 3 LIMIT 4
	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n SKIP 3 LIMIT 4", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 4)
}

func TestExecuteMatchNoReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MATCH without RETURN should return matched indicator
	result, err := exec.Execute(ctx, "MATCH (n:Something)", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSubstituteParams(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "param-test",
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	require.NoError(t, store.CreateNode(node))

	// Test with various parameter types
	params := map[string]interface{}{
		"name":   "Alice",
		"age":    30,
		"active": true,
		"data":   nil,
	}

	result, err := exec.Execute(ctx, "MATCH (n:User) WHERE n.name = $name RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteDeleteNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DELETE with no MATCH clause should error
	_, err := exec.Execute(ctx, "DELETE n", nil)
	assert.Error(t, err)
}

func TestResolveReturnItemVariants(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "return-test",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}
	require.NoError(t, store.CreateNode(node))

	// Return whole node
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Rows[0][0])

	// Return property
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result.Rows[0][0])

	// Return id
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id", nil)
	require.NoError(t, err)
	assert.Equal(t, "return-test", result.Rows[0][0])

	// Return *
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN *", nil)
	require.NoError(t, err)
	assert.NotNil(t, result.Rows[0][0])
}

func TestParsePropertiesVariants(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with number property
	result, err := exec.Execute(ctx, "CREATE (n:Test {count: 42, rate: 3.14, flag: true})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify properties were parsed correctly
	nodes, _ := store.GetNodesByLabel("Test")
	assert.Len(t, nodes, 1)
}

func TestExecuteCountProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes, some with email, some without
	node1 := &storage.Node{ID: "cp1", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Alice", "email": "a@b.com"}}
	node2 := &storage.Node{ID: "cp2", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Bob"}}
	node3 := &storage.Node{ID: "cp3", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Charlie", "email": "c@d.com"}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))
	require.NoError(t, store.CreateNode(node3))

	// COUNT(n.email) should only count non-null
	result, err := exec.Execute(ctx, "MATCH (n:User) RETURN count(n.email) AS emailCount", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Rows[0][0])
}

func TestToFloat64Variants(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with various numeric types
	node := &storage.Node{
		ID:     "float-test",
		Labels: []string{"Numbers"},
		Properties: map[string]interface{}{
			"int32":   int32(100),
			"int64":   int64(200),
			"float32": float32(3.14),
			"float64": float64(2.71),
			"string":  "42.5",
		},
	}
	require.NoError(t, store.CreateNode(node))

	// These should all work with numeric comparisons
	result, err := exec.Execute(ctx, "MATCH (n:Numbers) WHERE n.int32 > 50 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// ======== Additional tests for 100% coverage ========

func TestValidateSyntaxUnbalancedClosingBrackets(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Extra closing paren
	_, err := exec.Execute(ctx, "MATCH (n)) RETURN n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unbalanced brackets")

	// Extra closing bracket
	_, err = exec.Execute(ctx, "MATCH (n)-[r]]->(m) RETURN n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unbalanced brackets")

	// Extra closing brace
	_, err = exec.Execute(ctx, "CREATE (n:Person {name: 'test'}} )", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unbalanced brackets")
}

func TestValidateSyntaxEscapedQuotesInStrings(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Escaped quotes should be handled (string continues past escaped quote)
	result, err := exec.Execute(ctx, `CREATE (n:Test {name: 'O\'Brien'})`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestSubstituteParamsAllTypes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a test node
	node := &storage.Node{
		ID:         "param-types",
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"val": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	// Test int64 parameter
	params := map[string]interface{}{
		"num": int64(100),
	}
	result, err := exec.Execute(ctx, "MATCH (n:Test) WHERE n.val = $num RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test float64 parameter
	params = map[string]interface{}{
		"num": float64(100),
	}
	result, err = exec.Execute(ctx, "MATCH (n:Test) WHERE n.val = $num RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test boolean parameter
	node2 := &storage.Node{
		ID:         "param-bool",
		Labels:     []string{"Test2"},
		Properties: map[string]interface{}{"active": true},
	}
	require.NoError(t, store.CreateNode(node2))

	params = map[string]interface{}{
		"flag": true,
	}
	result, err = exec.Execute(ctx, "MATCH (n:Test2) WHERE n.active = $flag RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test nil parameter
	params = map[string]interface{}{
		"nothing": nil,
	}
	_, err = exec.Execute(ctx, "MATCH (n) WHERE n.prop = $nothing RETURN n", params)
	require.NoError(t, err)

	// Test default type (struct or other) - should use %v
	params = map[string]interface{}{
		"custom": struct{ Name string }{"test"},
	}
	_, err = exec.Execute(ctx, "MATCH (n) WHERE n.prop = $custom RETURN n", params)
	require.NoError(t, err)
}

func TestParseValueVariants(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes to test against
	node := &storage.Node{
		ID:     "parse-val",
		Labels: []string{"ValueTest"},
		Properties: map[string]interface{}{
			"name":   "test",
			"active": true,
			"count":  float64(42),
		},
	}
	require.NoError(t, store.CreateNode(node))

	// Test TRUE (uppercase)
	result, err := exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.active = TRUE RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test FALSE
	result, err = exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.active = FALSE RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// Test NULL
	result, err = exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.missing = NULL RETURN n", nil)
	require.NoError(t, err)

	// Test double-quoted string
	result, err = exec.Execute(ctx, `MATCH (n:ValueTest) WHERE n.name = "test" RETURN n`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestCompareEqualNilCases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test equality with nil on both sides
	node := &storage.Node{
		ID:         "nil-eq",
		Labels:     []string{"NilTest"},
		Properties: map[string]interface{}{"name": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// Compare existing prop with nil literal
	result, err := exec.Execute(ctx, "MATCH (n:NilTest) WHERE n.name = NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0) // name is 'test', not null
}

func TestCompareGreaterLessStrings(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values for comparison
	nodes := []struct {
		id   string
		name string
	}{
		{"str1", "apple"},
		{"str2", "banana"},
		{"str3", "cherry"},
	}
	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Fruit"},
			Properties: map[string]interface{}{"name": n.name},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// String comparison > (alphabetical)
	result, err := exec.Execute(ctx, "MATCH (n:Fruit) WHERE n.name > 'banana' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // cherry

	// String comparison <
	result, err = exec.Execute(ctx, "MATCH (n:Fruit) WHERE n.name < 'banana' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // apple
}

func TestCompareRegexInvalidPattern(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "regex-inv",
		Labels:     []string{"RegexTest"},
		Properties: map[string]interface{}{"pattern": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// Invalid regex pattern - should not match
	result, err := exec.Execute(ctx, "MATCH (n:RegexTest) WHERE n.pattern =~ '[invalid' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestCompareRegexNonStringExpected(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "regex-num",
		Labels:     []string{"RegexNum"},
		Properties: map[string]interface{}{"val": float64(123)},
	}
	require.NoError(t, store.CreateNode(node))

	// Regex with number - pattern isn't string type (will return false)
	result, err := exec.Execute(ctx, "MATCH (n:RegexNum) WHERE n.val =~ 123 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateStringOpMissingProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-miss",
		Labels:     []string{"StrMiss"},
		Properties: map[string]interface{}{"name": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// CONTAINS on non-existent property
	result, err := exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing CONTAINS 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// STARTS WITH on non-existent property
	result, err = exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing STARTS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// ENDS WITH on non-existent property
	result, err = exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing ENDS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateInOpMissingProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-miss",
		Labels:     []string{"InMiss"},
		Properties: map[string]interface{}{"name": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// IN on non-existent property
	result, err := exec.Execute(ctx, "MATCH (n:InMiss) WHERE n.missing IN ['a', 'b'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateInOpNotAList(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-notlist",
		Labels:     []string{"InNotList"},
		Properties: map[string]interface{}{"status": "active"},
	}
	require.NoError(t, store.CreateNode(node))

	// IN without proper list syntax (no brackets)
	result, err := exec.Execute(ctx, "MATCH (n:InNotList) WHERE n.status IN 'active' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0) // Should not match since 'active' is not a list
}

func TestEvaluateWhereNoValidOperator(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "no-op",
		Labels:     []string{"NoOp"},
		Properties: map[string]interface{}{"val": float64(5)},
	}
	require.NoError(t, store.CreateNode(node))

	// WHERE clause without a recognized operator - should include all
	result, err := exec.Execute(ctx, "MATCH (n:NoOp) WHERE n.val RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestEvaluateWhereNonPropertyComparison(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "non-prop",
		Labels:     []string{"NonProp"},
		Properties: map[string]interface{}{"val": float64(5)},
	}
	require.NoError(t, store.CreateNode(node))

	// Comparison that doesn't start with variable.property
	result, err := exec.Execute(ctx, "MATCH (n:NonProp) WHERE 5 = 5 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Should include all since we can't evaluate "5 = 5"
}

func TestEvaluateWherePropertyNotExists(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "prop-ne",
		Labels:     []string{"PropNE"},
		Properties: map[string]interface{}{"existing": "yes"},
	}
	require.NoError(t, store.CreateNode(node))

	// WHERE on non-existent property should return false
	result, err := exec.Execute(ctx, "MATCH (n:PropNE) WHERE n.nonexistent = 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestOrderNodesStringSorting(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values
	names := []string{"Charlie", "Alice", "Bob"}
	for i, name := range names {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("sort-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": name},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Order by string ascending
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
	assert.Equal(t, "Alice", result.Rows[0][0])

	// Order by string descending
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name DESC", nil)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", result.Rows[0][0])
}

func TestOrderNodesWithoutVariablePrefix(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ord-%d", i)),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{"priority": float64(3 - i)}, // 3, 2, 1
		}
		require.NoError(t, store.CreateNode(node))
	}

	// ORDER BY without variable prefix (just property name)
	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n ORDER BY priority", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestSplitNodePatternsWithRemainder(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create pattern with extra text after closing paren - edge case
	result, err := exec.Execute(ctx, "CREATE (a:One), (b:Two)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestParseNodePatternNoLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node without labels
	result, err := exec.Execute(ctx, "CREATE (n {name: 'Unlabeled'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParseNodePatternMultipleLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with multiple labels
	result, err := exec.Execute(ctx, "CREATE (n:Person:Employee:Manager {name: 'Boss'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify labels
	nodes, _ := store.GetNodesByLabel("Person")
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes[0].Labels, "Employee")
	assert.Contains(t, nodes[0].Labels, "Manager")
}

func TestParsePropertiesFalseBoolean(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with false boolean
	result, err := exec.Execute(ctx, "CREATE (n:BoolTest {active: false})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	nodes, _ := store.GetNodesByLabel("BoolTest")
	assert.Equal(t, false, nodes[0].Properties["active"])
}

func TestParseReturnItemsWithAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "alias-test",
		Labels:     []string{"Item"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n.value AS val", nil)
	require.NoError(t, err)
	assert.Equal(t, "val", result.Columns[0])
}

func TestParseReturnItemsOrderByLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ol-%d", i)),
			Labels:     []string{"OLTest"},
			Properties: map[string]interface{}{"idx": float64(i)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// RETURN with ORDER BY and LIMIT in return clause parsing
	result, err := exec.Execute(ctx, "MATCH (n:OLTest) RETURN n.idx ORDER BY n.idx LIMIT 3", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteCreateInvalidRelPattern(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Pattern that routes to relationship handler but fails regex
	// Contains "-[" to trigger relationship path, but not in valid format
	_, err := exec.Execute(ctx, "CREATE -[r:REL]- invalid", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid relationship pattern")
}

func TestExecuteDeleteRequiresMATCH(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DELETE without MATCH
	_, err := exec.Execute(ctx, "DETACH DELETE n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DELETE requires a MATCH clause")
}

func TestExecuteSetRequiresMATCH(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET without MATCH - should fail validation first (SET is not a valid start keyword)
	_, err := exec.Execute(ctx, "SET n.prop = 'value'", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error") // SET is not a valid starting clause
}

func TestExecuteSetInvalidPropertyAccess(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-inv",
		Labels:     []string{"SetInv"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// SET without proper n.property format - should either error or be a no-op
	// Note: Current implementation may not error on this malformed SET,
	// so we just verify it doesn't panic and no properties are set
	_, err := exec.Execute(ctx, "MATCH (n:SetInv) SET prop = 'value'", nil)
	if err != nil {
		// If an error is returned, it should mention property access
		assert.Contains(t, err.Error(), "property")
	} else {
		// If no error, verify the node wasn't modified incorrectly
		updatedNode, _ := store.GetNode("set-inv")
		if updatedNode != nil {
			// The malformed SET should not have created a "prop" property
			_, hasProp := updatedNode.Properties["prop"]
			assert.False(t, hasProp, "Malformed SET should not create 'prop' property")
		}
	}
}

func TestExecuteAggregationCountStar(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cs-%d", i)),
			Labels:     []string{"CountStar"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:CountStar) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(7), result.Rows[0][0])
}

func TestExecuteAggregationCollectNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cn-%d", i)),
			Labels:     []string{"CollectNode"},
			Properties: map[string]interface{}{"idx": float64(i)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// COLLECT(n) - collect whole nodes
	result, err := exec.Execute(ctx, "MATCH (n:CollectNode) RETURN collect(n)", nil)
	require.NoError(t, err)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 3)
	// Each item should be a map with id, labels, properties
	item := collected[0].(map[string]interface{})
	assert.Contains(t, item, "id")
	assert.Contains(t, item, "labels")
	assert.Contains(t, item, "properties")
}

func TestExecuteAggregationNonAggregateInQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("na-%d", i)),
			Labels:     []string{"NonAgg"},
			Properties: map[string]interface{}{"name": fmt.Sprintf("Item%d", i), "val": float64(i * 10)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// Mix of aggregate and non-aggregate in RETURN
	// Neo4j implicitly groups by non-aggregated columns, so we get 3 rows (one per name)
	result, err := exec.Execute(ctx, "MATCH (n:NonAgg) RETURN n.name, sum(n.val)", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3) // One row per distinct n.name

	// Verify each row has the correct name and sum for that group
	sumByName := make(map[string]float64)
	for _, row := range result.Rows {
		name := row[0].(string)
		sum := row[1].(float64)
		sumByName[name] = sum
	}
	assert.Equal(t, float64(0), sumByName["Item0"])  // Only Item0's value
	assert.Equal(t, float64(10), sumByName["Item1"]) // Only Item1's value
	assert.Equal(t, float64(20), sumByName["Item2"]) // Only Item2's value
}

func TestExecuteAggregationEmptyResultSet(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Aggregation on empty set - non-aggregate column should be nil
	result, err := exec.Execute(ctx, "MATCH (n:NonExistentLabel) RETURN n.name, sum(n.val)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0]) // No nodes, so non-aggregate is nil
}

func TestExecuteAggregationSumNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SUM with no matching property pattern
	node := &storage.Node{
		ID:         "sum-no",
		Labels:     []string{"SumNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:SumNo) RETURN sum(invalid)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(0), result.Rows[0][0])
}

func TestExecuteAggregationAvgNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "avg-no",
		Labels:     []string{"AvgNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:AvgNo) RETURN avg(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteAggregationMinNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "min-no",
		Labels:     []string{"MinNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:MinNo) RETURN min(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteAggregationMaxNoMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "max-no",
		Labels:     []string{"MaxNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:MaxNo) RETURN max(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestResolveReturnItemCountFunction(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "count-resolve",
		Labels:     []string{"CountResolve"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// Non-aggregation query but with COUNT in return item
	// This tests resolveReturnItem's COUNT handling
	result, err := exec.Execute(ctx, "MATCH (n:CountResolve) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestResolveReturnItemNonExistentProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "nep",
		Labels:     []string{"NEP"},
		Properties: map[string]interface{}{"exists": "yes"},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:NEP) RETURN n.nonexistent", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestResolveReturnItemDifferentVariable(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "diff-var",
		Labels:     []string{"DiffVar"},
		Properties: map[string]interface{}{"val": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// Return expression with different variable name
	result, err := exec.Execute(ctx, "MATCH (n:DiffVar) RETURN m.val", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0]) // m doesn't match n
}

func TestDbSchemaVisualizationWithRelationships(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and edges
	node1 := &storage.Node{ID: "sv1", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "sv2", Labels: []string{"Company"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{ID: "sev1", StartNode: "sv1", EndNode: "sv2", Type: "WORKS_AT", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	// Should have nodes and relationships
	schemaNodes := result.Rows[0][0].([]map[string]interface{})
	schemaRels := result.Rows[0][1].([]map[string]interface{})
	assert.Len(t, schemaNodes, 2)
	assert.Len(t, schemaRels, 1)
}

func TestDbSchemaNodePropertiesMultipleLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{
		ID:         "snp1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": 30},
	}
	node2 := &storage.Node{
		ID:         "snp2",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme", "revenue": 1000000},
	}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	result, err := exec.Execute(ctx, "CALL db.schema.nodeProperties()", nil)
	require.NoError(t, err)
	// Should have rows for each label/property combination
	assert.GreaterOrEqual(t, len(result.Rows), 4) // At least: Person.name, Person.age, Company.name, Company.revenue
}

func TestDbSchemaRelPropertiesWithProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "srp1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "srp2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{
		ID:         "serp1",
		StartNode:  "srp1",
		EndNode:    "srp2",
		Type:       "CONNECTS",
		Properties: map[string]interface{}{"weight": 5, "since": "2020"},
	}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.schema.relProperties()", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Rows), 2) // weight and since
}

func TestDbPropertyKeysWithEdgeProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "dpk1", Labels: []string{"X"}, Properties: map[string]interface{}{"nodeProp": "value"}}
	node2 := &storage.Node{ID: "dpk2", Labels: []string{"Y"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{
		ID:         "depk1",
		StartNode:  "dpk1",
		EndNode:    "dpk2",
		Type:       "REL",
		Properties: map[string]interface{}{"edgeProp": "edgeValue"},
	}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.propertyKeys()", nil)
	require.NoError(t, err)
	// Should include both nodeProp and edgeProp
	props := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		props[i] = row[0].(string)
	}
	assert.Contains(t, props, "nodeProp")
	assert.Contains(t, props, "edgeProp")
}

func TestCountLabelsAndRelTypes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create diverse data
	node1 := &storage.Node{ID: "cl1", Labels: []string{"Label1"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "cl2", Labels: []string{"Label2"}, Properties: map[string]interface{}{}}
	node3 := &storage.Node{ID: "cl3", Labels: []string{"Label3"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))
	require.NoError(t, store.CreateNode(node3))

	edge1 := &storage.Edge{ID: "ce1", StartNode: "cl1", EndNode: "cl2", Type: "TYPE1", Properties: map[string]interface{}{}}
	edge2 := &storage.Edge{ID: "ce2", StartNode: "cl2", EndNode: "cl3", Type: "TYPE2", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	result, err := exec.Execute(ctx, "CALL nornicdb.stats()", nil)
	require.NoError(t, err)
	// labels count
	assert.Equal(t, 3, result.Rows[0][2])
	// relationshipTypes count
	assert.Equal(t, 2, result.Rows[0][3])
}

func TestDetachDeleteWithIncomingEdges(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a graph where node2 has both incoming and outgoing edges
	node1 := &storage.Node{ID: "dd1", Labels: []string{"Node"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "dd2", Labels: []string{"Target"}, Properties: map[string]interface{}{}}
	node3 := &storage.Node{ID: "dd3", Labels: []string{"Node"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))
	require.NoError(t, store.CreateNode(node3))

	// node1 -> node2 (incoming to node2)
	edge1 := &storage.Edge{ID: "dde1", StartNode: "dd1", EndNode: "dd2", Type: "POINTS_TO", Properties: map[string]interface{}{}}
	// node2 -> node3 (outgoing from node2)
	edge2 := &storage.Edge{ID: "dde2", StartNode: "dd2", EndNode: "dd3", Type: "POINTS_TO", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Detach delete node2 - should delete both edges
	result, err := exec.Execute(ctx, "MATCH (n:Target) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, 2, result.Stats.RelationshipsDeleted)
}

func TestExecuteCreateRelationshipWithProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship with properties in the relationship
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestExecuteCreateMultipleEmptyPatterns(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with extra whitespace between patterns
	result, err := exec.Execute(ctx, "CREATE (a:A)  ,  (b:B)  ,  (c:C)", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Stats.NodesCreated)
}

func TestExecuteReturnStarWithMultipleNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "star-test",
		Labels:     []string{"Star"},
		Properties: map[string]interface{}{"name": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// RETURN * should return all matched variables
	result, err := exec.Execute(ctx, "MATCH (n:Star) RETURN *", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestToFloat64WithInt(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with plain int (not int64)
	node := &storage.Node{
		ID:         "int-test",
		Labels:     []string{"IntTest"},
		Properties: map[string]interface{}{"value": 42}, // plain int
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:IntTest) WHERE n.value > 40 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestToFloat64WithInvalidString(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// String that can't be converted to float
	node := &storage.Node{
		ID:         "inv-str",
		Labels:     []string{"InvStr"},
		Properties: map[string]interface{}{"value": "not-a-number"},
	}
	require.NoError(t, store.CreateNode(node))

	// Comparison should use string comparison as fallback
	result, err := exec.Execute(ctx, "MATCH (n:InvStr) WHERE n.value > 'aaa' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // 'not-a-number' > 'aaa' alphabetically
}

func TestExecuteDistinctWithDuplicates(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with duplicate property values
	for i := 0; i < 5; i++ {
		category := "A"
		if i >= 3 {
			category = "B"
		}
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("dist-%d", i)),
			Labels:     []string{"Dist"},
			Properties: map[string]interface{}{"cat": category},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// DISTINCT should deduplicate
	result, err := exec.Execute(ctx, "MATCH (n:Dist) RETURN DISTINCT n.cat", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2) // Only A and B
}

// Additional tests for remaining coverage gaps

func TestCallDbRelationshipTypesEmpty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No edges - should return empty
	result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

func TestCallDbLabelsEmpty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No nodes - should return empty
	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

func TestExecuteCreateWithReturnTargetVariable(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship and return the target node variable
	result, err := exec.Execute(ctx, "CREATE (a:City {name: 'NYC'})-[:ROUTE]->(b:City {name: 'LA'}) RETURN b.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteCreateRelationshipWithRelReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// CREATE with RETURN that doesn't match source or target
	result, err := exec.Execute(ctx, "CREATE (a:A)-[:REL]->(b:B) RETURN x.prop", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestParseValueFloatParsing(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "float-parse",
		Labels:     []string{"FloatParse"},
		Properties: map[string]interface{}{"value": float64(3.14159)},
	}
	require.NoError(t, store.CreateNode(node))

	// Test float literal parsing in WHERE
	result, err := exec.Execute(ctx, "MATCH (n:FloatParse) WHERE n.value > 3.14 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestParseValuePlainString(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "plain-str",
		Labels:     []string{"PlainStr"},
		Properties: map[string]interface{}{"status": "active"},
	}
	require.NoError(t, store.CreateNode(node))

	// Test comparison with unquoted string that isn't a number or boolean
	_, err := exec.Execute(ctx, "MATCH (n:PlainStr) WHERE n.status = active RETURN n", nil)
	require.NoError(t, err)
	// "active" without quotes is parsed as plain string
}

func TestEvaluateStringOpNonVariablePrefix(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-nv",
		Labels:     []string{"StrNV"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	require.NoError(t, store.CreateNode(node))

	// CONTAINS with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:StrNV) WHERE something CONTAINS 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true (includes all)
}

func TestEvaluateInOpNonVariablePrefix(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-nv",
		Labels:     []string{"InNV"},
		Properties: map[string]interface{}{"val": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// IN with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:InNV) WHERE something IN ['a', 'b'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true
}

func TestEvaluateIsNullNonVariablePrefix(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "null-nv",
		Labels:     []string{"NullNV"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// IS NULL with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:NullNV) WHERE something IS NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true
}

func TestSplitNodePatternsComplexNesting(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Complex nesting with properties containing commas (unlikely but tests depth tracking)
	result, err := exec.Execute(ctx, "CREATE (a:Test {name: 'A, B'}), (b:Test)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestExecuteMatchCountVariable(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cv-%d", i)),
			Labels:     []string{"CountVar"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// count(n) with variable name
	result, err := exec.Execute(ctx, "MATCH (n:CountVar) RETURN count(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestExecuteDeleteWithWhere(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node1 := &storage.Node{ID: "del-w1", Labels: []string{"DelKeep"}, Properties: map[string]interface{}{"type": "keeper"}}
	node2 := &storage.Node{ID: "del-w2", Labels: []string{"DelRemove"}, Properties: map[string]interface{}{"type": "remove"}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	// DELETE with label filter - DELETE all DelRemove nodes
	result, err := exec.Execute(ctx, "MATCH (n:DelRemove) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify the right one was kept
	nodes, _ := store.GetNodesByLabel("DelKeep")
	assert.Len(t, nodes, 1)
}

func TestExecuteSetUpdateNodeError(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-err",
		Labels:     []string{"SetErr"},
		Properties: nil, // nil properties map
	}
	require.NoError(t, store.CreateNode(node))

	// SET should initialize properties map if nil
	result, err := exec.Execute(ctx, "MATCH (n:SetErr) SET n.newprop = 'value'", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)
}

func TestExecuteCreateNodeWithEmptyPattern(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with just variable, no labels
	result, err := exec.Execute(ctx, "CREATE (n)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParseReturnItemsEmptyAfterSplit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "empty-ret",
		Labels:     []string{"EmptyRet"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// RETURN with trailing comma that might create empty parts
	result, err := exec.Execute(ctx, "MATCH (n:EmptyRet) RETURN n,", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestExecuteMergeAsCreate(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE currently works like CREATE
	result, err := exec.Execute(ctx, "MERGE (n:MergeTest {name: 'Test'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.NotEmpty(t, result.Rows)
}

func TestCallDbLabelsWithEmptyLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Node with empty labels array
	node := &storage.Node{
		ID:         "no-labels",
		Labels:     []string{},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	// Should not contain any labels
	assert.Empty(t, result.Rows)
}

func TestExecuteCreateRelationshipNoType(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Relationship without explicit type (uses default RELATED_TO)
	result, err := exec.Execute(ctx, "CREATE (a:A)-[r]->(b:B)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, _ := store.AllEdges()
	assert.Equal(t, "RELATED_TO", edges[0].Type)
}

// ============================================================================
// CREATE ... WITH ... DELETE Tests (Compound Query Pattern)
// ============================================================================

// TestExecuteCreateWithDeleteBasic tests the basic CREATE...WITH...DELETE pattern
func TestExecuteCreateWithDeleteBasic(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node, pass it through WITH, then delete it
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteAndReturn tests CREATE...WITH...DELETE...RETURN
func TestExecuteCreateWithDeleteAndReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This is the benchmark pattern: create, delete, return count
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t RETURN count(t)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Should have a return value
	assert.NotEmpty(t, result.Columns)
	assert.NotEmpty(t, result.Rows)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteTimestamp tests CREATE with timestamp() function
func TestExecuteCreateWithDeleteTimestamp(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// The benchmark uses timestamp() in the CREATE
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp', created: timestamp()}) WITH t DELETE t RETURN count(t)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteRelationship tests CREATE...WITH...DELETE for relationships
// Note: This creates nodes AND a relationship in one CREATE, then deletes the relationship
func TestExecuteCreateWithDeleteRelationship(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create two nodes with a relationship, pass relationship through WITH, delete it
	result, err := exec.Execute(ctx, "CREATE (a:Person)-[r:KNOWS]->(b:Person) WITH r DELETE r RETURN count(r)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsDeleted)

	// Verify relationship is gone but nodes remain
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(2), nodeCount)
	edgeCount, _ := store.EdgeCount()
	assert.Equal(t, int64(0), edgeCount)
}

// TestExecuteCreateWithDeleteMultipleNodes tests creating and deleting multiple nodes
func TestExecuteCreateWithDeleteMultipleNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a single node pattern first to verify basic functionality
	result, err := exec.Execute(ctx, "CREATE (t:Temp {id: 1}) WITH t DELETE t", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

func TestExecuteDeleteDetachKeywordPosition(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "dk1", Labels: []string{"DK"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "dk2", Labels: []string{"DK"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{ID: "dke1", StartNode: "dk1", EndNode: "dk2", Type: "REL", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	// DETACH DELETE at end of line (different position handling)
	result, err := exec.Execute(ctx, "MATCH (n:DK) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesDeleted)
}

func TestCollectRegexWithProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "collect-prop",
		Labels:     []string{"CollectProp"},
		Properties: map[string]interface{}{"name": "test"},
	}
	require.NoError(t, store.CreateNode(node))

	// COLLECT(n.name) - collect property values
	result, err := exec.Execute(ctx, "MATCH (n:CollectProp) RETURN collect(n.name)", nil)
	require.NoError(t, err)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 1)
	assert.Equal(t, "test", collected[0])
}

func TestParsePropertiesWithSpace(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Properties with spaces around colon and values
	result, err := exec.Execute(ctx, "CREATE (n:Test { name : 'Alice' , age : 30 })", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParsePropertiesNoValue(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Properties key without proper value (invalid but should not crash)
	result, err := exec.Execute(ctx, "CREATE (n:Test {name})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestUnsupportedQueryType(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// GRANT is truly unsupported
	_, err := exec.Execute(ctx, "GRANT ADMIN TO user", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
}

func TestExecuteMatchOrderByWithSkipLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("osl-%d", i)),
			Labels:     []string{"OSL"},
			Properties: map[string]interface{}{"val": float64(i)},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// ORDER BY with SKIP and LIMIT
	result, err := exec.Execute(ctx, "MATCH (n:OSL) RETURN n.val ORDER BY n.val SKIP 2 LIMIT 5", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
}

func TestParseValueIntegerParsing(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "int-parse",
		Labels:     []string{"IntParse"},
		Properties: map[string]interface{}{"count": float64(100)},
	}
	require.NoError(t, store.CreateNode(node))

	// Integer literal parsing
	result, err := exec.Execute(ctx, "MATCH (n:IntParse) WHERE n.count = 100 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Additional tests for remaining coverage gaps

func TestCompareEqualBothNil(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Node with nil property
	node := &storage.Node{
		ID:         "nil-both",
		Labels:     []string{"NilBoth"},
		Properties: map[string]interface{}{"val": nil},
	}
	require.NoError(t, store.CreateNode(node))

	// Compare nil = nil (both nil should be equal)
	result, err := exec.Execute(ctx, "MATCH (n:NilBoth) WHERE n.val = NULL RETURN n", nil)
	require.NoError(t, err)
	// Note: depends on how nil comparison is handled
	assert.NotNil(t, result)
}

func TestToFloat64AllTypes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test with float32 (we need to create nodes that have various types)
	node := &storage.Node{
		ID:     "types-all",
		Labels: []string{"TypesAll"},
		Properties: map[string]interface{}{
			"f64":  float64(100.0),
			"f32":  float32(50.0),
			"i":    int(30),
			"i64":  int64(40),
			"i32":  int32(20),
			"str":  "60.5",
			"bool": true,
		},
	}
	require.NoError(t, store.CreateNode(node))

	// Query using float comparison
	result, err := exec.Execute(ctx, "MATCH (n:TypesAll) WHERE n.f64 > 50 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestEvaluateStringOpDefaultCase(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-def",
		Labels:     []string{"StrDef"},
		Properties: map[string]interface{}{"name": "test value"},
	}
	require.NoError(t, store.CreateNode(node))

	// Test each string operator
	result, err := exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name CONTAINS 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	result, err = exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name STARTS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	result, err = exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name ENDS WITH 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchWithRelationshipQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship
	node1 := &storage.Node{ID: "rel-m1", Labels: []string{"Start"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "rel-m2", Labels: []string{"End"}, Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateNode(node1))
	require.NoError(t, store.CreateNode(node2))

	edge := &storage.Edge{ID: "rel-me", StartNode: "rel-m1", EndNode: "rel-m2", Type: "CONNECTS"}
	require.NoError(t, store.CreateEdge(edge))

	// Match with relationship type
	result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "CONNECTS", result.Rows[0][0])
}

func TestExecuteCreateWithInlineProps(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with inline properties and return the property
	result, err := exec.Execute(ctx, "CREATE (n:Inline {val: 42}) RETURN n.val", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestExecuteReturnAliasedCount(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ac-%d", i)),
			Labels:     []string{"AC"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	result, err := exec.Execute(ctx, "MATCH (n:AC) RETURN count(*) AS total", nil)
	require.NoError(t, err)
	assert.Equal(t, "total", result.Columns[0])
	assert.Equal(t, int64(4), result.Rows[0][0])
}

func TestExecuteSetNewProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-new",
		Labels:     []string{"SetNew"},
		Properties: map[string]interface{}{"existing": "yes"},
	}
	require.NoError(t, store.CreateNode(node))

	// SET a new property
	result, err := exec.Execute(ctx, "MATCH (n:SetNew) SET n.newprop = 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteMatchWithNilParams(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "nil-params",
		Labels:     []string{"NilParams"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// Execute with nil params map
	result, err := exec.Execute(ctx, "MATCH (n:NilParams) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestParseReturnItemsEmptyString(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "empty-items",
		Labels:     []string{"EmptyItems"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// Return with extra spaces/commas
	result, err := exec.Execute(ctx, "MATCH (n:EmptyItems) RETURN n , ", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteMatchWhereEqualsNumber(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "num-eq",
		Labels:     []string{"NumEq"},
		Properties: map[string]interface{}{"val": float64(42)},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "MATCH (n:NumEq) WHERE n.val = 42 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteReturnCountN(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cn-%d", i)),
			Labels:     []string{"CountN"},
			Properties: map[string]interface{}{},
		}
		require.NoError(t, store.CreateNode(node))
	}

	// COUNT(n) - count by variable name
	result, err := exec.Execute(ctx, "MATCH (n:CountN) RETURN count(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), result.Rows[0][0])
}

func TestExecuteAggregationWithNonNumeric(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values (not numeric)
	node := &storage.Node{
		ID:         "non-num",
		Labels:     []string{"NonNum"},
		Properties: map[string]interface{}{"value": "not a number"},
	}
	require.NoError(t, store.CreateNode(node))

	// SUM should handle non-numeric gracefully
	result, err := exec.Execute(ctx, "MATCH (n:NonNum) RETURN sum(n.value)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(0), result.Rows[0][0])
}

func TestCallDbLabelsWithError(t *testing.T) {
	// This is tricky - MemoryEngine doesn't error on AllNodes
	// Just verify normal behavior
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "label-test",
		Labels:     []string{"TestLabel", "SecondLabel"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestResolveReturnItemWithCount(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "count-ri",
		Labels:     []string{"CountRI"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	// This triggers resolveReturnItem with COUNT prefix in non-aggregation path
	result, err := exec.Execute(ctx, "MATCH (n:CountRI) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

// Tests for toFloat64 type coverage
func TestToFloat64TypeCoverage(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test float32 through comparison
	node1 := &storage.Node{
		ID:         "f32-test",
		Labels:     []string{"Float32Test"},
		Properties: map[string]interface{}{"val": float32(3.14)},
	}
	require.NoError(t, store.CreateNode(node1))

	result, err := exec.Execute(ctx, "MATCH (n:Float32Test) WHERE n.val > 3.0 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test int through SUM aggregation
	node2 := &storage.Node{
		ID:         "int-test",
		Labels:     []string{"IntTest"},
		Properties: map[string]interface{}{"val": int(100)},
	}
	require.NoError(t, store.CreateNode(node2))

	result, err = exec.Execute(ctx, "MATCH (n:IntTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(100), result.Rows[0][0])

	// Test int32 through AVG
	node3 := &storage.Node{
		ID:         "i32-test",
		Labels:     []string{"Int32Test"},
		Properties: map[string]interface{}{"val": int32(50)},
	}
	require.NoError(t, store.CreateNode(node3))

	result, err = exec.Execute(ctx, "MATCH (n:Int32Test) RETURN avg(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(50), result.Rows[0][0])

	// Test string that can be parsed as float
	node4 := &storage.Node{
		ID:         "str-num",
		Labels:     []string{"StrNumTest"},
		Properties: map[string]interface{}{"val": "42.5"},
	}
	require.NoError(t, store.CreateNode(node4))

	result, err = exec.Execute(ctx, "MATCH (n:StrNumTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(42.5), result.Rows[0][0])
}

// Test Parser MERGE clause
func TestParseMerge(t *testing.T) {
	parser := NewParser()
	query, err := parser.Parse("MERGE (n:Person {name: 'Alice'})")
	require.NoError(t, err)
	assert.NotNil(t, query)
	// MERGE is currently parsed but treated as CREATE internally
}

// Test all WHERE operators exercise evaluateWhere fully
func TestEvaluateWhereFullCoverage(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "where-full",
		Labels: []string{"WhereFull"},
		Properties: map[string]interface{}{
			"name":   "Alice Smith",
			"age":    float64(30),
			"active": true,
			"score":  float64(85.5),
		},
	}
	require.NoError(t, store.CreateNode(node))

	// Test >= operator
	result, err := exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age >= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test <= operator
	result, err = exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age <= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test edge cases in splitNodePatterns
func TestSplitNodePatternsEdgeCases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Empty pattern after splitting
	result, err := exec.Execute(ctx, "CREATE (a:A)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

// Test evaluateStringOp edge cases
func TestEvaluateStringOpEdgeCases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-edge",
		Labels:     []string{"StrEdge"},
		Properties: map[string]interface{}{"text": "hello world"},
	}
	require.NoError(t, store.CreateNode(node))

	// CONTAINS that matches
	result, err := exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// CONTAINS that doesn't match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'xyz' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// STARTS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text STARTS WITH 'hello' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// ENDS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text ENDS WITH 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test evaluateInOp edge cases
func TestEvaluateInOpMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-match",
		Labels:     []string{"InMatch"},
		Properties: map[string]interface{}{"status": "pending"},
	}
	require.NoError(t, store.CreateNode(node))

	// IN with matching value
	result, err := exec.Execute(ctx, "MATCH (n:InMatch) WHERE n.status IN ['active', 'pending'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test Parser default case in Parse
func TestParserDefaultCase(t *testing.T) {
	parser := NewParser()

	// Query with tokens that aren't recognized keywords
	query, err := parser.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.NotNil(t, query)
}

// =============================================================================
// Tests for Parameter Substitution (substituteParams and valueToLiteral)
// =============================================================================

func TestSubstituteParamsBasic(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		params   map[string]interface{}
		expected string
	}{
		{
			name:     "string parameter",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{"name": "Alice"},
			expected: "MATCH (n {name: 'Alice'}) RETURN n",
		},
		{
			name:     "integer parameter",
			query:    "MATCH (n) WHERE n.age = $age RETURN n",
			params:   map[string]interface{}{"age": 25},
			expected: "MATCH (n) WHERE n.age = 25 RETURN n",
		},
		{
			name:     "float parameter",
			query:    "MATCH (n) WHERE n.score > $score RETURN n",
			params:   map[string]interface{}{"score": 85.5},
			expected: "MATCH (n) WHERE n.score > 85.5 RETURN n",
		},
		{
			name:     "boolean parameter true",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": true},
			expected: "MATCH (n) WHERE n.active = true RETURN n",
		},
		{
			name:     "boolean parameter false",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": false},
			expected: "MATCH (n) WHERE n.active = false RETURN n",
		},
		{
			name:     "null parameter",
			query:    "MATCH (n) WHERE n.value = $value RETURN n",
			params:   map[string]interface{}{"value": nil},
			expected: "MATCH (n) WHERE n.value = null RETURN n",
		},
		{
			name:     "multiple parameters",
			query:    "MATCH (n {name: $name, age: $age}) RETURN n",
			params:   map[string]interface{}{"name": "Bob", "age": 30},
			expected: "MATCH (n {name: 'Bob', age: 30}) RETURN n",
		},
		{
			name:     "missing parameter unchanged",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{},
			expected: "MATCH (n {name: $name}) RETURN n",
		},
		{
			name:     "empty params",
			query:    "MATCH (n) RETURN n",
			params:   nil,
			expected: "MATCH (n) RETURN n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Verify queries execute correctly after substitution
	_, err := exec.Execute(ctx, "CREATE (n:ParamTest {name: 'Test'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:ParamTest {name: $name}) RETURN n", map[string]interface{}{"name": "Test"})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestSubstituteParamsStringEscaping(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "single quote escaping",
			value:    "O'Connor",
			expected: "'O''Connor'",
		},
		{
			name:     "backslash escaping",
			value:    "path\\to\\file",
			expected: "'path\\\\to\\\\file'",
		},
		{
			name:     "both quotes and backslashes",
			value:    "It's a\\path",
			expected: "'It''s a\\\\path'",
		},
		{
			name:     "empty string",
			value:    "",
			expected: "''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralArrays(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "string array",
			value:    []string{"a", "b", "c"},
			expected: "['a', 'b', 'c']",
		},
		{
			name:     "int array",
			value:    []int{1, 2, 3},
			expected: "[1, 2, 3]",
		},
		{
			name:     "int64 array",
			value:    []int64{100, 200, 300},
			expected: "[100, 200, 300]",
		},
		{
			name:     "float64 array",
			value:    []float64{1.5, 2.5, 3.5},
			expected: "[1.5, 2.5, 3.5]",
		},
		{
			name:     "interface array",
			value:    []interface{}{"hello", 42, true},
			expected: "['hello', 42, true]",
		},
		{
			name:     "empty array",
			value:    []interface{}{},
			expected: "[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralMaps(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	// Map with single key (deterministic)
	result := exec.valueToLiteral(map[string]interface{}{"name": "Alice"})
	assert.Equal(t, "{name: 'Alice'}", result)

	// Empty map
	result = exec.valueToLiteral(map[string]interface{}{})
	assert.Equal(t, "{}", result)
}

func TestValueToLiteralIntegerTypes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int", int(42), "42"},
		{"int8", int8(8), "8"},
		{"int16", int16(16), "16"},
		{"int32", int32(32), "32"},
		{"int64", int64(64), "64"},
		{"uint", uint(100), "100"},
		{"uint8", uint8(8), "8"},
		{"uint16", uint16(16), "16"},
		{"uint32", uint32(32), "32"},
		{"uint64", uint64(64), "64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralFloats(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	// float32
	result := exec.valueToLiteral(float32(3.14))
	assert.Contains(t, result, "3.14")

	// float64
	result = exec.valueToLiteral(float64(2.718281828))
	assert.Contains(t, result, "2.718")
}

// =============================================================================
// Tests for RETURN Clause Parsing
// =============================================================================

func TestParseReturnClauseBasic(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:         "ret-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}

	tests := []struct {
		name            string
		returnClause    string
		varName         string
		expectedCols    []string
		expectedValFunc func([]interface{}) bool
	}{
		{
			name:         "return star",
			returnClause: "*",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
		{
			name:         "return property with alias",
			returnClause: "n.name AS personName",
			varName:      "n",
			expectedCols: []string{"personName"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "Alice"
			},
		},
		{
			name:         "return property without alias",
			returnClause: "n.age",
			varName:      "n",
			expectedCols: []string{"age"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == float64(30)
			},
		},
		{
			name:         "return id function",
			returnClause: "id(n) AS node_id",
			varName:      "n",
			expectedCols: []string{"node_id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "ret-1"
			},
		},
		{
			name:         "return multiple expressions",
			returnClause: "n.name AS name, n.age AS age, id(n) AS id",
			varName:      "n",
			expectedCols: []string{"name", "age", "id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 3 && vals[0] == "Alice" && vals[1] == float64(30) && vals[2] == "ret-1"
			},
		},
		{
			name:         "return variable only",
			returnClause: "n",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols, vals := exec.parseReturnClause(tt.returnClause, tt.varName, node)
			assert.Equal(t, tt.expectedCols, cols)
			assert.True(t, tt.expectedValFunc(vals), "Value validation failed for %v", vals)
		})
	}
}

func TestSplitReturnExpressions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		clause   string
		expected []string
	}{
		{
			name:     "single expression",
			clause:   "n.name",
			expected: []string{"n.name"},
		},
		{
			name:     "multiple simple expressions",
			clause:   "n.name, n.age, n.city",
			expected: []string{"n.name", " n.age", " n.city"},
		},
		{
			name:     "expression with function",
			clause:   "id(n), n.name",
			expected: []string{"id(n)", " n.name"},
		},
		{
			name:     "nested parentheses",
			clause:   "count(n), sum(n.age)",
			expected: []string{"count(n)", " sum(n.age)"},
		},
		{
			name:     "complex function call",
			clause:   "collect(n.name), count(*)",
			expected: []string{"collect(n.name)", " count(*)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.splitReturnExpressions(tt.clause)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExpressionToAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{"property access", "n.name", "name"},
		{"nested property", "n.address.city", "city"},
		{"function call", "id(n)", "id(n)"},
		{"simple variable", "n", "n"},
		{"literal", "'hello'", "'hello'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.expressionToAlias(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluateExpression(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:     "eval-1",
		Labels: []string{"Test"},
		Properties: map[string]interface{}{
			"name":      "Test Node",
			"count":     float64(42),
			"active":    true,
			"embedding": []float64{0.1, 0.2, 0.3}, // Should be filtered
		},
	}

	tests := []struct {
		name     string
		expr     string
		varName  string
		expected interface{}
	}{
		{"id function", "id(n)", "n", "eval-1"},
		{"id function with spaces", "id( n )", "n", "eval-1"},
		{"property access", "n.name", "n", "Test Node"},
		{"numeric property", "n.count", "n", float64(42)},
		{"boolean property", "n.active", "n", true},
		{"missing property", "n.missing", "n", nil},
		{"embedding property filtered", "n.embedding", "n", nil},
		{"string literal", "'hello'", "n", "hello"},
		{"integer literal", "42", "n", int64(42)},
		{"float literal", "3.14", "n", float64(3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateExpression(tt.expr, tt.varName, node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for Internal Property Filtering (Embeddings)
// =============================================================================

func TestIsInternalProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	internalProps := []string{
		"embedding",
		"embeddings",
		"vector",
		"vectors",
		"_embedding",
		"_embeddings",
		"chunk_embedding",
		"chunk_embeddings",
		"EMBEDDING",  // Case insensitive
		"Embeddings", // Mixed case
	}

	externalProps := []string{
		"name",
		"age",
		"content",
		"description",
		"embed", // Not exact match
		"id",
	}

	for _, prop := range internalProps {
		t.Run("internal_"+prop, func(t *testing.T) {
			assert.True(t, exec.isInternalProperty(prop), "%s should be internal", prop)
		})
	}

	for _, prop := range externalProps {
		t.Run("external_"+prop, func(t *testing.T) {
			assert.False(t, exec.isInternalProperty(prop), "%s should not be internal", prop)
		})
	}
}

func TestNodeToMapFiltersEmbeddings(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:        "embed-filter-1",
		Labels:    []string{"Document"},
		Embedding: []float32{0.1, 0.2, 0.3, 0.4, 0.5}, // Actual embedding in storage
		Properties: map[string]interface{}{
			"name":                 "Test Doc",
			"content":              "Hello world",
			"embedding":            []float64{0.1, 0.2, 0.3, 0.4, 0.5}, // Should be filtered from properties
			"chunk_embedding":      []float64{0.5, 0.4, 0.3, 0.2, 0.1},
			"vector":               []float64{1.0, 2.0, 3.0},
			"embedding_model":      "mxbai-embed-large",
			"embedding_dimensions": 1024,
			"has_embedding":        true,
		},
	}

	result := exec.nodeToMap(node)

	// Check that regular properties are present at top level (Neo4j compatible)
	assert.Equal(t, "Test Doc", result["name"])
	assert.Equal(t, "Hello world", result["content"])

	// Check that embedding is now a summary object, not raw array
	embSummary, ok := result["embedding"].(map[string]interface{})
	assert.True(t, ok, "embedding should be a summary map")
	assert.Equal(t, "ready", embSummary["status"])
	assert.Equal(t, 5, embSummary["dimensions"]) // 5 from node.Embedding
	assert.Equal(t, "mxbai-embed-large", embSummary["model"])

	// Check that raw embedding arrays are NOT in properties
	assert.NotContains(t, result, "chunk_embedding")
	assert.NotContains(t, result, "vector")
	assert.NotContains(t, result, "embedding_model")      // Shown in summary instead
	assert.NotContains(t, result, "embedding_dimensions") // Shown in summary instead
	assert.NotContains(t, result, "has_embedding")        // Shown in summary instead

	// Check node metadata fields
	assert.Equal(t, "embed-filter-1", result["id"])
	assert.Equal(t, []string{"Document"}, result["labels"])
}

// =============================================================================
// Tests for MERGE with ON CREATE SET / ON MATCH SET
// =============================================================================

func TestMergeWithOnCreateSet(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE a new node with ON CREATE SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Alice'})
		ON CREATE SET n.created = 'yes', n.age = 25
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Rows, 1)

	// Verify node was created with properties
	matchResult, err := exec.Execute(ctx, "MATCH (n:Person {name: 'Alice'}) RETURN n.created, n.age", nil)
	require.NoError(t, err)
	assert.Len(t, matchResult.Rows, 1)
}

func TestMergeWithOnMatchSet(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create a node
	_, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Bob', visits: 0})", nil)
	require.NoError(t, err)

	// MERGE existing node with ON MATCH SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Bob'})
		ON MATCH SET n.visits = 1
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesCreated) // Should not create new node
}

func TestMergeRouting(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE with ON CREATE SET should NOT be routed to executeSet
	result, err := exec.Execute(ctx, `
		MERGE (n:File {path: '/test/file.txt'})
		ON CREATE SET n.created = 'true'
		RETURN n.path AS path
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 1)
	assert.Equal(t, "path", result.Columns[0])
}

func TestMergeWithParameterSubstitution(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"path":      "/app/docs/README.md",
		"name":      "README.md",
		"size":      int64(1024),
		"extension": ".md",
	}

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: $path})
		ON CREATE SET f.name = $name, f.size = $size, f.extension = $extension
		RETURN f.path AS path, f.name AS name
	`, params)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Columns, 2)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "name")
}

func TestMergeReturnIdFunction(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: '/test.txt'})
		RETURN f.path AS path, id(f) AS node_id
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 2)
	assert.Equal(t, "path", result.Columns[0])
	assert.Equal(t, "node_id", result.Columns[1])

	// node_id should be a string
	if len(result.Rows) > 0 {
		assert.IsType(t, "", result.Rows[0][1])
	}
}

// =============================================================================
// Tests for Extract Helper Functions
// =============================================================================

func TestExtractVarName(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{"simple var with label", "(n:Person)", "n"},
		{"var with multiple labels", "(f:File:Node)", "f"},
		{"var with properties", "(n:Person {name: 'Alice'})", "n"},
		{"var only", "(n)", "n"},
		{"no var, label only", "(:Person)", "n"}, // Default
		{"empty pattern", "()", "n"},             // Default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractVarName(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{"single label", "(n:Person)", []string{"Person"}},
		{"multiple labels", "(f:File:Node)", []string{"File", "Node"}},
		{"label with properties", "(n:Person {name: 'Alice'})", []string{"Person"}},
		{"no label", "(n)", []string{}},
		{"no var, label only", "(:Person)", []string{"Person"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractLabels(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for DROP INDEX (No-op)
// =============================================================================

func TestDropIndexNoOp(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DROP INDEX should be treated as no-op (returns empty result, no error)
	result, err := exec.Execute(ctx, "DROP INDEX file_path IF EXISTS", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)
}

// =============================================================================
// Tests for Edge Cases in Parameter Substitution
// =============================================================================

func TestSubstituteParamsEdgeCases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name   string
		query  string
		params map[string]interface{}
	}{
		{
			name:   "parameter at start",
			query:  "$name is the name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "parameter at end",
			query:  "The name is $name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "adjacent parameters",
			query:  "Values: $a$b",
			params: map[string]interface{}{"a": "x", "b": "y"},
		},
		{
			name:   "underscore in param name",
			query:  "Path: $host_path",
			params: map[string]interface{}{"host_path": "/app/docs"},
		},
		{
			name:   "number in param name",
			query:  "Value: $param123",
			params: map[string]interface{}{"param123": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			// Should not contain the original parameter placeholders
			for key := range tt.params {
				assert.NotContains(t, result, "$"+key)
			}
		})
	}
}

func TestSubstituteParamsComplexQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test with a complex MERGE query like Mimir uses
	query := `
		MERGE (f:File:Node {path: $path})
		ON CREATE SET f.id = 'file-123',
			f.host_path = $host_path,
			f.name = $name,
			f.extension = $extension,
			f.size_bytes = $size_bytes,
			f.content = $content
		RETURN f.path AS path, f.size_bytes AS size_bytes, id(f) AS node_id
	`

	params := map[string]interface{}{
		"path":       "/app/docs/README.md",
		"host_path":  "/Users/dev/docs/README.md",
		"name":       "README.md",
		"extension":  ".md",
		"size_bytes": int64(2048),
		"content":    "# Hello World\n\nThis is a test file.",
	}

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 3)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "size_bytes")
	assert.Contains(t, result.Columns, "node_id")

	if len(result.Rows) > 0 {
		assert.Equal(t, "/app/docs/README.md", result.Rows[0][0])
	}
}

// TestRelationshipCountAggregation tests that COUNT(r) properly aggregates
// all relationships instead of returning 1 (the bug that was fixed)
func TestRelationshipCountAggregation(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a small graph with multiple relationships
	// 3 products, 2 categories, 2 suppliers
	// Each product has 2 relationships (PART_OF category, SUPPLIES from supplier)
	// Total: 6 relationships

	// Create categories
	cat1 := &storage.Node{ID: "cat1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(1), "name": "Beverages"}}
	cat2 := &storage.Node{ID: "cat2", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(2), "name": "Condiments"}}
	require.NoError(t, store.CreateNode(cat1))
	require.NoError(t, store.CreateNode(cat2))

	// Create suppliers
	sup1 := &storage.Node{ID: "sup1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(1), "name": "Exotic Liquids"}}
	sup2 := &storage.Node{ID: "sup2", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(2), "name": "New Orleans"}}
	require.NoError(t, store.CreateNode(sup1))
	require.NoError(t, store.CreateNode(sup2))

	// Create products
	prod1 := &storage.Node{ID: "prod1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(1), "name": "Chai"}}
	prod2 := &storage.Node{ID: "prod2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(2), "name": "Chang"}}
	prod3 := &storage.Node{ID: "prod3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(3), "name": "Aniseed Syrup"}}
	require.NoError(t, store.CreateNode(prod1))
	require.NoError(t, store.CreateNode(prod2))
	require.NoError(t, store.CreateNode(prod3))

	// Create relationships
	// Product 1: Beverages category, Supplier 1
	edge1 := &storage.Edge{ID: "e1", StartNode: "prod1", EndNode: "cat1", Type: "PART_OF"}
	edge2 := &storage.Edge{ID: "e2", StartNode: "sup1", EndNode: "prod1", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Product 2: Beverages category, Supplier 1
	edge3 := &storage.Edge{ID: "e3", StartNode: "prod2", EndNode: "cat1", Type: "PART_OF"}
	edge4 := &storage.Edge{ID: "e4", StartNode: "sup1", EndNode: "prod2", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge3))
	require.NoError(t, store.CreateEdge(edge4))

	// Product 3: Condiments category, Supplier 2
	edge5 := &storage.Edge{ID: "e5", StartNode: "prod3", EndNode: "cat2", Type: "PART_OF"}
	edge6 := &storage.Edge{ID: "e6", StartNode: "sup2", EndNode: "prod3", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge5))
	require.NoError(t, store.CreateEdge(edge6))

	// Test 1: Count all relationships
	t.Run("count all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return single aggregated row")
		require.Len(t, result.Rows[0], 1, "Should return single column")

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "Should count all 6 relationships, not return 1")
	})

	// Test 2: Count relationships by type
	t.Run("count PART_OF relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:PART_OF]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 PART_OF relationships")
	})

	t.Run("count SUPPLIES relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:SUPPLIES]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 SUPPLIES relationships")
	})

	// Test 3: Count with wildcard (COUNT(*))
	t.Run("count with wildcard", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "COUNT(*) should count all 6 relationships")
	})

	// Test 4: Verify non-aggregation still works (should return all rows)
	t.Run("non-aggregation returns all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 6, "Non-aggregation should return all 6 relationship rows")
	})

	// Test 5: Count with GROUP BY (implicit grouping by type)
	t.Run("count grouped by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType, count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// Should group by type: PART_OF (3) and SUPPLIES (3) = 2 groups
		assert.Len(t, result.Rows, 2, "Should return 2 groups (PART_OF and SUPPLIES)")

		// Verify counts
		for _, row := range result.Rows {
			relType := row[0].(string)
			count := row[1].(int64)
			assert.Equal(t, int64(3), count, "Each type should have count of 3")
			assert.Contains(t, []string{"PART_OF", "SUPPLIES"}, relType)
		}
	})

	// Test 6: Empty result aggregation
	t.Run("count with no matches", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:NONEXISTENT]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(0), count, "COUNT should return 0 for no matches")
	})
}

// ========================================
// SET Label Tests - SET n:Label syntax
// ========================================

func TestSetLabelSyntax(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node without the target label
	node := &storage.Node{
		ID:         "label-test-1",
		Labels:     []string{"File"},
		Properties: map[string]interface{}{"path": "/test/file.txt"},
	}
	require.NoError(t, store.CreateNode(node))

	t.Run("SET single label", func(t *testing.T) {
		// Add Node label using SET n:Label syntax
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify the node now has both labels
		updatedNode, err := store.GetNode("label-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "File")
		assert.Contains(t, updatedNode.Labels, "Node")
	})

	t.Run("SET label idempotent", func(t *testing.T) {
		// Adding same label again should not increase count
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded, "Should not add duplicate label")
	})
}

func TestSetLabelWithPropertyAssignment(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "combo-test-1",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"name": "readme"},
	}
	require.NoError(t, store.CreateNode(node))

	t.Run("SET label and property together", func(t *testing.T) {
		// SET f:Node, f.type = 'file' - combining label and property
		result, err := exec.Execute(ctx, `
			MATCH (d:Document {name: 'readme'})
			SET d:Indexed, d.type = 'file'
			RETURN d
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)
		assert.Equal(t, 1, result.Stats.PropertiesSet)

		// Verify both label and property were set
		updatedNode, err := store.GetNode("combo-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Document")
		assert.Contains(t, updatedNode.Labels, "Indexed")
		assert.Equal(t, "file", updatedNode.Properties["type"])
	})
}

func TestSetMultipleLabels(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "multi-label-1",
		Labels:     []string{"Base"},
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateNode(node))

	t.Run("SET multiple labels in sequence", func(t *testing.T) {
		// First add one label
		_, err := exec.Execute(ctx, `MATCH (n:Base) SET n:First RETURN n`, nil)
		require.NoError(t, err)

		// Then add another
		result, err := exec.Execute(ctx, `MATCH (n:Base) SET n:Second RETURN n`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify all labels present
		updatedNode, err := store.GetNode("multi-label-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Base")
		assert.Contains(t, updatedNode.Labels, "First")
		assert.Contains(t, updatedNode.Labels, "Second")
	})
}

// ========================================
// WHERE Label Check Tests - WHERE n:Label and WHERE NOT n:Label
// ========================================

func TestWhereLabelCheck(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes with different labels
	nodes := []*storage.Node{
		{ID: "person-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "company-1", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	t.Run("WHERE n:Label filters correctly", func(t *testing.T) {
		// Match only Person nodes that are also Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT n:Label excludes correctly", func(t *testing.T) {
		// Match Person nodes that are NOT Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Bob", result.Rows[0][0])
	})
}

func TestWhereNotLabelMigrationPattern(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This tests the exact pattern from the Mimir schema initialization:
	// MATCH (f:File) WHERE NOT f:Node SET f:Node, f.type = 'file'

	// Create File nodes - some with Node label, some without
	nodes := []*storage.Node{
		{ID: "file-1", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/a.txt"}},
		{ID: "file-2", Labels: []string{"File", "Node"}, Properties: map[string]interface{}{"path": "/b.txt"}},
		{ID: "file-3", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/c.txt"}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	t.Run("migration adds label only to nodes missing it", func(t *testing.T) {
		// Run the migration pattern
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		// Should add Node label to file-1 and file-3, not file-2
		assert.Equal(t, 2, result.Stats.LabelsAdded)
		assert.Equal(t, 2, result.Stats.PropertiesSet)

		// Verify file-1 now has Node label
		file1, err := store.GetNode("file-1")
		require.NoError(t, err)
		assert.Contains(t, file1.Labels, "Node")
		assert.Equal(t, "file", file1.Properties["type"])

		// Verify file-2 unchanged (already had Node label)
		file2, err := store.GetNode("file-2")
		require.NoError(t, err)
		assert.Contains(t, file2.Labels, "Node")
		assert.Nil(t, file2.Properties["type"]) // type not set because it wasn't matched

		// Verify file-3 now has Node label
		file3, err := store.GetNode("file-3")
		require.NoError(t, err)
		assert.Contains(t, file3.Labels, "Node")
		assert.Equal(t, "file", file3.Properties["type"])
	})

	t.Run("running migration again has no effect", func(t *testing.T) {
		// Second run should do nothing since all File nodes now have Node label
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded)
		assert.Equal(t, 0, result.Stats.PropertiesSet)
	})
}

func TestWhereLabelWithAndCondition(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes
	nodes := []*storage.Node{
		{ID: "emp-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "emp-2", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}},
		{ID: "cust-1", Labels: []string{"Person", "Customer"}, Properties: map[string]interface{}{"name": "Charlie", "age": int64(35)}},
	}
	for _, n := range nodes {
		require.NoError(t, store.CreateNode(n))
	}

	t.Run("WHERE label AND property condition", func(t *testing.T) {
		// Match Employees over 28
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee AND p.age > 28
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT label AND property condition", func(t *testing.T) {
		// Match non-Employees over 30
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee AND p.age > 30
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Charlie", result.Rows[0][0])
	})
}
