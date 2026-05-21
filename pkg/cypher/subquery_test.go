package cypher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingNodeLookupEngine struct {
	storage.Engine
	allNodesErr error
	byLabelErr  error
}

func (e *failingNodeLookupEngine) AllNodes() ([]*storage.Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *failingNodeLookupEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.byLabelErr != nil {
		return nil, e.byLabelErr
	}
	return e.Engine.GetNodesByLabel(label)
}

// TestCountSubqueryComparison tests COUNT { } subquery functionality
func TestCountSubquery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Alice knows Bob, Charlie, and Dave
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery with > comparison
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } > 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryEquals tests COUNT { } = n syntax
func TestCountSubqueryEquals(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery with = comparison - find person with exactly 2 friends
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } = 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery with = failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryZero tests COUNT { } = 0 syntax (no relationships)
func TestCountSubqueryZero(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Alice knows Bob, Charlie has no relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery = 0 - find people with no KNOWS relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } = 0
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery = 0 failed: %v", err)
	}

	// Bob and Charlie should match (neither have outgoing KNOWS)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result.Rows))
	}
}

func TestCallSubquery_UseClauseInBodySwitchesNamespace(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	rootStore := storage.NewNamespacedEngine(baseStore, "nornic")
	trStore := storage.NewNamespacedEngine(baseStore, "nornic.tr")

	exec := NewStorageExecutor(rootStore)
	ctx := context.Background()

	_, err := trStore.CreateNode(&storage.Node{
		ID:         "t-1",
		Labels:     []string{"Translation"},
		Properties: map[string]interface{}{"id": "t-1", "textKey": "hello", "textKey128": "h128"},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		CALL {
			USE nornic.tr
			MATCH (t:Translation)
			RETURN t.id AS translationId
		}
		RETURN translationId
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"translationId"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "t-1", result.Rows[0][0])
}

func TestCallSubquery_ChainedUseWithImport(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	rootStore := storage.NewNamespacedEngine(baseStore, "nornic")
	trStore := storage.NewNamespacedEngine(baseStore, "nornic.tr")
	txtStore := storage.NewNamespacedEngine(baseStore, "nornic.txt")
	exec := NewStorageExecutor(rootStore)
	ctx := context.Background()

	_, err := trStore.CreateNode(&storage.Node{
		ID:         "tr-1",
		Labels:     []string{"Translation"},
		Properties: map[string]interface{}{"id": "t-1", "textKey": "k1", "textKey128": "k1-128"},
	})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{
		ID:         "tr-2",
		Labels:     []string{"Translation"},
		Properties: map[string]interface{}{"id": "t-2", "textKey": "k2", "textKey128": "k2-128"},
	})
	require.NoError(t, err)

	_, err = txtStore.CreateNode(&storage.Node{
		ID:     "txt-1",
		Labels: []string{"TranslationText"},
		Properties: map[string]interface{}{
			"translationId": "t-1",
			"text":          "value-1",
		},
	})
	require.NoError(t, err)
	_, err = txtStore.CreateNode(&storage.Node{
		ID:     "txt-2",
		Labels: []string{"TranslationText"},
		Properties: map[string]interface{}{
			"translationId": "t-2",
			"text":          "value-2",
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		USE nornic
		CALL {
			USE nornic.tr
			MATCH (t:Translation)
			RETURN t.id AS translationId, t.textKey AS textKey, t.textKey128 AS textKey128
		}
		CALL {
			USE nornic.txt
			WITH translationId
			MATCH (tt:TranslationText)
			WHERE tt.translationId = translationId
			RETURN collect(tt) AS texts
		}
		RETURN translationId, textKey, textKey128, texts
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"translationId", "textKey", "textKey128", "texts"}, result.Columns)
	require.Len(t, result.Rows, 2)

	got := make(map[string][]interface{}, len(result.Rows))
	keys := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 4)
		id, ok := row[0].(string)
		require.True(t, ok)
		texts, ok := row[3].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, texts)
		got[id] = texts
		keys = append(keys, id)
	}
	sort.Strings(keys)
	require.Equal(t, []string{"t-1", "t-2"}, keys)
}

// TestCountSubqueryGTE tests COUNT { } >= n syntax
func TestCountSubqueryGTE(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with varying relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})-[:KNOWS]->(f:Person {name: 'Frank'}),
		       (g:Person {name: 'Grace'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT { } >= 2 - find people with 2 or more friends
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery >= failed: %v", err)
	}

	// Only Alice has 3 friends (>= 2)
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryIncoming tests COUNT with incoming relationships
func TestCountSubqueryIncoming(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Bob is known by Alice and Charlie
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})-[:KNOWS]->(b)
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT with incoming relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)<-[:KNOWS]-(other) } >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT incoming subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Bob" {
		t.Errorf("Expected Bob, got %v", result.Rows[0][0])
	}
}

// ========================================
// CALL {} Subquery Tests (Neo4j 4.0+)
// ========================================

// TestCallSubqueryBasic tests basic CALL {} subquery execution
func TestCallSubqueryBasic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Basic CALL {} subquery - find oldest person
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN p.name AS name, p.age AS age
			ORDER BY p.age DESC
			LIMIT 1
		}
		RETURN name, age
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Charlie" {
		t.Errorf("Expected Charlie (oldest), got %v", result.Rows[0][0])
	}
}

// TestCallSubqueryWithOuterMatch tests CALL {} with outer MATCH context
func TestCallSubqueryWithOuterMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with outer MATCH - count friends for each person
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		CALL {
			WITH p
			MATCH (p)-[:KNOWS]->(friend)
			RETURN count(friend) AS friendCount
		}
		RETURN p.name, friendCount
		ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with outer MATCH failed: %v", err)
	}

	// Should have 5 people with their friend counts
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 results, got %d", len(result.Rows))
	}
}

// TestCallSubqueryUnion tests CALL {} with UNION inside
func TestCallSubqueryUnion(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with different node types
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', type: 'person'}),
		       (b:Company {name: 'Acme Corp', type: 'company'}),
		       (c:Person {name: 'Bob', type: 'person'}),
		       (d:Company {name: 'Tech Inc', type: 'company'})
	`, nil)
	require.NoError(t, err)

	// Verify test data was created
	personResult, err := exec.Execute(ctx, `MATCH (p:Person) RETURN p.name AS name`, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(personResult.Rows), "Should have 2 Person nodes")

	companyResult, err := exec.Execute(ctx, `MATCH (c:Company) RETURN c.name AS name`, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(companyResult.Rows), "Should have 2 Company nodes")

	t.Run("basic_union_in_call", func(t *testing.T) {
		// UNION inside CALL {} - combine Person and Company names
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				RETURN p.name AS name, p.type AS type
				UNION
				MATCH (c:Company)
				RETURN c.name AS name, c.type AS type
			}
			RETURN name, type
			ORDER BY name
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4, "Should return all Person and Company names")
		require.Equal(t, []string{"name", "type"}, result.Columns, "Should have correct column names")
	})

	t.Run("union_all_in_call", func(t *testing.T) {
		// UNION ALL inside CALL {} - includes duplicates
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				RETURN p.type AS type
				UNION ALL
				MATCH (c:Company)
				RETURN c.type AS type
			}
			RETURN type
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4, "UNION ALL should return all rows including duplicates")
		require.Equal(t, []string{"type"}, result.Columns, "Should have correct column names")
	})

	t.Run("union_with_different_aliases", func(t *testing.T) {
		// Test that UNION handles matching column names correctly
		// Both queries return 'name' but from different sources
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				RETURN p.name AS name
				UNION
				MATCH (c:Company)
				RETURN c.name AS name
			}
			RETURN name
			ORDER BY name
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4, "Should return all names")
		require.Equal(t, []string{"name"}, result.Columns, "Should have correct column name")
	})

	t.Run("union_in_call_with_outer_return", func(t *testing.T) {
		// UNION inside CALL {} with outer RETURN that renames columns
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				RETURN p.name AS name
				UNION
				MATCH (c:Company)
				RETURN c.name AS name
			}
			RETURN name AS entityName
			ORDER BY entityName
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4, "Should return all names")
		require.Equal(t, []string{"entityName"}, result.Columns, "Should have renamed column")
	})

	t.Run("nested_union_in_call", func(t *testing.T) {
		// Multiple UNIONs inside CALL {}
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				RETURN p.name AS name
				UNION
				MATCH (c:Company)
				RETURN c.name AS name
				UNION
				RETURN 'Other' AS name
			}
			RETURN name
			ORDER BY name
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 4, "Should return all names plus 'Other'")
		require.Equal(t, []string{"name"}, result.Columns, "Should have correct column name")
	})
}

// TestCallSubqueryNested tests nested CALL {} subqueries
func TestCallSubqueryNested(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', dept: 'Engineering'}),
		       (b:Person {name: 'Bob', dept: 'Engineering'}),
		       (c:Person {name: 'Charlie', dept: 'Sales'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Nested CALL {} - count per department, then find max
	result, err := exec.Execute(ctx, `
		CALL {
			CALL {
				MATCH (p:Person)
				RETURN p.dept AS dept, count(*) AS cnt
			}
			RETURN dept, cnt
			ORDER BY cnt DESC
			LIMIT 1
		}
		RETURN dept AS largestDept, cnt AS employeeCount
	`, nil)
	if err != nil {
		t.Fatalf("Nested CALL subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Engineering" {
		t.Errorf("Expected Engineering (largest dept), got %v", result.Rows[0][0])
	}
}

// TestCallSubqueryWithCreate tests CALL {} with write operations
func TestCallSubqueryWithCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with CREATE inside
	_, err = exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CALL {
			WITH p
			CREATE (p)-[:FRIEND_OF]->(:Person {name: 'Bob'})
		}
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with CREATE failed: %v", err)
	}

	// Verify Bob was created
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Bob'}) RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("Failed to query created node: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected Bob to be created, got %d results", len(result.Rows))
	}
}

// TestCallSubqueryInTransactions tests CALL {} IN TRANSACTIONS syntax with actual batching.
// This verifies that operations are processed in separate transactions per batch.
func TestCallSubqueryInTransactions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("basic batching with SET", func(t *testing.T) {
		// Create test data
		_, err := exec.Execute(ctx, `
			CREATE (a:Person {name: 'Alice'}),
			       (b:Person {name: 'Bob'}),
			       (c:Person {name: 'Charlie'})
		`, nil)
		require.NoError(t, err)

		// CALL {} IN TRANSACTIONS - batch processing with batch size of 2
		// This should process 3 nodes in 2 batches (2 in first, 1 in second)
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				SET p.processed = true
				RETURN p.name AS name
			} IN TRANSACTIONS OF 2 ROWS
			RETURN name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3, "Should return all 3 names")

		// Verify all nodes were processed
		verify, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p.processed = true
			RETURN count(p) AS count
		`, nil)
		require.NoError(t, err)
		require.Len(t, verify.Rows, 1)
		assert.Equal(t, int64(3), verify.Rows[0][0])
	})

	t.Run("large batch with default batch size", func(t *testing.T) {
		// Create 10 nodes
		_, err := exec.Execute(ctx, `
			UNWIND range(1, 10) AS i
			CREATE (n:Item {id: i, processed: false})
		`, nil)
		require.NoError(t, err)

		// Process with default batch size (1000) - should be single batch
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (n:Item)
				SET n.processed = true
				RETURN n.id AS id
			} IN TRANSACTIONS
			RETURN id ORDER BY id
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 10, "Should return all 10 items")

		// Verify all were processed
		verify, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WHERE n.processed = true
			RETURN count(n) AS count
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(10), verify.Rows[0][0])
	})

	t.Run("batch with CREATE operations", func(t *testing.T) {
		// Clean up any existing Source/Target nodes
		_, _ = exec.Execute(ctx, `MATCH (s:Source) DELETE s`, nil)
		_, _ = exec.Execute(ctx, `MATCH (t:Target) DELETE t`, nil)

		// Create source data
		_, err := exec.Execute(ctx, `
			CREATE (s:Source {value: 1}),
			       (s2:Source {value: 2}),
			       (s3:Source {value: 3})
		`, nil)
		require.NoError(t, err)

		// Process with CREATE in batches
		// Note: CREATE operations with MATCH may have different batching behavior
		// The batching applies LIMIT/SKIP to the MATCH, which should limit how many sources are processed
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (s:Source)
				CREATE (t:Target {value: s.value * 2})
				RETURN t.value AS value
			} IN TRANSACTIONS OF 2 ROWS
			RETURN value ORDER BY value
		`, nil)
		require.NoError(t, err)
		// Should return results from all batches combined
		require.GreaterOrEqual(t, len(result.Rows), 2, "Should return at least some results")

		// Verify targets were created correctly
		// The batching should process all sources across multiple batches
		verify, err := exec.Execute(ctx, `
			MATCH (t:Target)
			RETURN count(t) AS count
		`, nil)
		require.NoError(t, err)
		// Should create exactly 3 targets (one per source)
		assert.Equal(t, int64(3), verify.Rows[0][0], "Should create exactly 3 target nodes (one per source)")
	})

	t.Run("empty result set", func(t *testing.T) {
		// Process with no matching nodes
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:NonExistent)
				SET p.processed = true
				RETURN p.name AS name
			} IN TRANSACTIONS OF 2 ROWS
			RETURN name
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 0, "Should return empty result")
	})

	t.Run("read-only query (no batching needed)", func(t *testing.T) {
		// Create test data - use unique names to avoid conflicts with previous tests
		_, err := exec.Execute(ctx, `
			MATCH (p:Person) WHERE p.name IN ['ReadOnly1', 'ReadOnly2'] DELETE p
		`, nil)
		_, err = exec.Execute(ctx, `
			CREATE (a:Person {name: 'ReadOnly1'}),
			       (b:Person {name: 'ReadOnly2'})
		`, nil)
		require.NoError(t, err)

		// Read-only query should execute once (no batching)
		// However, if batching is applied, it may still work correctly
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				WHERE p.name IN ['ReadOnly1', 'ReadOnly2']
				RETURN p.name AS name
			} IN TRANSACTIONS OF 2 ROWS
			RETURN name ORDER BY name
		`, nil)
		require.NoError(t, err)
		// Read-only queries may still be batched, but should return correct results
		names := make(map[string]bool)
		for _, row := range result.Rows {
			if name, ok := row[0].(string); ok {
				names[name] = true
			}
		}
		assert.True(t, names["ReadOnly1"], "Should include ReadOnly1")
		assert.True(t, names["ReadOnly2"], "Should include ReadOnly2")
	})

	t.Run("batch error stops processing", func(t *testing.T) {
		// Create test data with unique names
		_, err := exec.Execute(ctx, `
			MATCH (p:Person) WHERE p.name IN ['ErrorTest1', 'ErrorTest2'] DELETE p
		`, nil)
		_, err = exec.Execute(ctx, `
			CREATE (a:Person {name: 'ErrorTest1'}),
			       (b:Person {name: 'ErrorTest2'})
		`, nil)
		require.NoError(t, err)

		// Deterministically invalid SET assignment inside batch execution.
		_, err = exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				WHERE p.name IN ['ErrorTest1', 'ErrorTest2']
				SET prop = 'test'
				RETURN p.name AS name
			} IN TRANSACTIONS OF 1 ROWS
			RETURN name
		`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SET assignment")
	})

	t.Run("stats accumulation across batches", func(t *testing.T) {
		// Create test data with unique names
		_, err := exec.Execute(ctx, `
			MATCH (p:Person) WHERE p.name IN ['Stats1', 'Stats2', 'Stats3', 'Stats4'] DELETE p
		`, nil)
		_, err = exec.Execute(ctx, `
			CREATE (a:Person {name: 'Stats1'}),
			       (b:Person {name: 'Stats2'}),
			       (c:Person {name: 'Stats3'}),
			       (d:Person {name: 'Stats4'})
		`, nil)
		require.NoError(t, err)

		// Process with batch size of 2 - should have 2 batches
		result, err := exec.Execute(ctx, `
			CALL {
				MATCH (p:Person)
				WHERE p.name IN ['Stats1', 'Stats2', 'Stats3', 'Stats4']
				SET p.batchProcessed = true
				RETURN p.name AS name
			} IN TRANSACTIONS OF 2 ROWS
		`, nil)
		require.NoError(t, err)
		require.NotNil(t, result.Stats, "Should have stats")
		// Stats should accumulate across batches
		assert.GreaterOrEqual(t, int64(result.Stats.PropertiesSet), int64(4), "Should set properties on all 4 nodes")
	})
}

// ========================================
// Additional EXISTS {} Subquery Tests
// ========================================

// TestExistsSubqueryMultipleRelTypes tests EXISTS with multiple relationship types
func TestExistsSubqueryMultipleRelTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})-[:WORKS_WITH]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with any outgoing relationship
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[]->() }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS subquery with any relationship failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (Alice, Charlie), got %d", len(result.Rows))
	}
}

// TestExistsSubqueryBidirectional tests EXISTS with bidirectional relationships
func TestExistsSubqueryBidirectional(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data - Alice knows Bob (directed)
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people connected to anyone (either direction)
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]-() }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS bidirectional subquery failed: %v", err)
	}

	// Both Alice and Bob are connected via KNOWS
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (Alice, Bob), got %d", len(result.Rows))
	}
}

// TestExistsSubqueryWithSpecificLabel tests EXISTS matching specific target labels
func TestExistsSubqueryWithSpecificLabel(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme'}),
		       (b:Person {name: 'Bob'})-[:KNOWS]->(a),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who work at a company
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(:Company) }
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with specific label failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// ========================================
// Additional NOT EXISTS {} Subquery Tests
// ========================================

// TestNotExistsSubqueryMultipleRelTypes tests NOT EXISTS with multiple relationship types
func TestNotExistsSubqueryMultipleRelTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})-[:WORKS_WITH]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with NO outgoing relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE NOT EXISTS { MATCH (p)-[]->() }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("NOT EXISTS subquery failed: %v", err)
	}

	// Bob, Dave, and Eve have no outgoing relationships
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 results (Bob, Dave, Eve), got %d", len(result.Rows))
	}
}

// TestNotExistsSubquerySpecificType tests NOT EXISTS for specific relationship type
func TestNotExistsSubquerySpecificType(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:MANAGES]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})-[:KNOWS]->(b),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who don't manage anyone
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE NOT EXISTS { MATCH (p)-[:MANAGES]->() }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("NOT EXISTS with specific type failed: %v", err)
	}

	// Bob, Charlie, and Dave don't manage anyone
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 results (Bob, Charlie, Dave), got %d", len(result.Rows))
	}
}

// ========================================
// Additional COUNT {} Subquery Tests
// ========================================

// TestCountSubqueryLTE tests COUNT { } <= n syntax
func TestCountSubqueryLTE(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})-[:KNOWS]->(f:Person {name: 'Frank'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with 1 or fewer outgoing KNOWS
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } <= 1
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT <= subquery failed: %v", err)
	}

	// Bob, Charlie, Dave, Eve, Frank have 0 or 1 outgoing
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 results, got %d", len(result.Rows))
	}
}

// TestCountSubqueryNotEquals tests COUNT { } != n syntax
func TestCountSubqueryNotEquals(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'}),
		       (d)-[:KNOWS]->(f:Person {name: 'Frank'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with relationship count NOT equal to 2
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } != 2
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT != subquery failed: %v", err)
	}

	// Bob, Charlie, Eve, Frank have 0 outgoing (not 2)
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 results (those with 0 outgoing), got %d", len(result.Rows))
	}
}

// TestCountSubqueryLT tests COUNT { } < n syntax
func TestCountSubqueryLT(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with less than 2 outgoing KNOWS
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } < 2
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT < subquery failed: %v", err)
	}

	// Bob, Charlie, Dave, Eve have 0 or 1 outgoing (< 2)
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 results, got %d", len(result.Rows))
	}
}

// TestCountSubqueryMultipleRelTypes tests COUNT with any relationship type
func TestCountSubqueryMultipleRelTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with mixed relationship types
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:WORKS_WITH]->(c:Person {name: 'Charlie'}),
		       (a)-[:MANAGES]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Count all outgoing relationships (any type)
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[]->() } >= 3
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT any relationship failed: %v", err)
	}

	// Only Alice has 3 outgoing relationships
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// ========================================
// Additional CALL {} Subquery Tests
// ========================================

// TestCallSubqueryWithWhere tests CALL {} with WHERE clause inside
func TestCallSubqueryWithWhere(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35}),
		       (d:Person {name: 'Dave', age: 20})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with WHERE inside
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			WHERE p.age >= 30
			RETURN p.name AS name, p.age AS age
		}
		RETURN name, age
		ORDER BY age DESC
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with WHERE failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (Alice, Charlie), got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithAggregation tests CALL {} with aggregation functions
func TestCallSubqueryWithAggregation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with aggregation
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN count(p) AS total, avg(p.age) AS avgAge
		}
		RETURN total, avgAge
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with aggregation failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithDelete tests CALL {} with DELETE operation
func TestCallSubqueryWithDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'}),
		       (b:Person {name: 'Bob'}),
		       (c:TempNode {name: 'ToDelete1'}),
		       (d:TempNode {name: 'ToDelete2'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with DELETE inside
	_, err = exec.Execute(ctx, `
		CALL {
			MATCH (t:TempNode)
			DELETE t
		}
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with DELETE failed: %v", err)
	}

	// Verify TempNodes were deleted
	result, err := exec.Execute(ctx, `
		MATCH (t:TempNode) RETURN count(t) AS cnt
	`, nil)
	if err != nil {
		t.Fatalf("Failed to count TempNodes: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result row, got %d", len(result.Rows))
	}
}

// TestCallSubqueryMultipleColumns tests CALL {} returning multiple columns
func TestCallSubqueryMultipleColumns(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30, city: 'NYC'}),
		       (b:Person {name: 'Bob', age: 25, city: 'LA'}),
		       (c:Person {name: 'Charlie', age: 35, city: 'NYC'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} returning multiple columns
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN p.name AS name, p.age AS age, p.city AS city
			ORDER BY p.age DESC
			LIMIT 2
		}
		RETURN name, age, city
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with multiple columns failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result.Rows))
	}
	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}
}

// TestCallSubqueryWithSkip tests CALL {} with SKIP
func TestCallSubqueryWithSkip(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35}),
		       (d:Person {name: 'Dave', age: 40})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with SKIP in outer query
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN p.name AS name, p.age AS age
			ORDER BY p.age DESC
		}
		RETURN name, age
		SKIP 1
		LIMIT 2
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with SKIP failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (after skip 1, limit 2), got %d", len(result.Rows))
	}
}

// TestCallSubqueryNoReturn tests CALL {} without outer RETURN (just execute)
func TestCallSubqueryNoReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} without outer RETURN - just executes the subquery
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			SET p.updated = true
			RETURN p.name AS name
		}
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery without outer RETURN failed: %v", err)
	}

	// Should return the inner result directly
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}

	// Verify the SET was applied
	verifyResult, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'}) RETURN p.updated
	`, nil)
	if err != nil {
		t.Fatalf("Failed to verify update: %v", err)
	}
	if verifyResult.Rows[0][0] != true {
		t.Errorf("Expected updated=true, got %v", verifyResult.Rows[0][0])
	}
}

// ========================================
// Combined/Complex Subquery Tests
// ========================================

// TestCombinedExistsAndCount tests combining EXISTS and COUNT in WHERE
func TestCombinedExistsAndCount(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'}),
		       (f:Person {name: 'Frank'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who know someone AND know at least 2 people
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->() }
		  AND COUNT { MATCH (p)-[:KNOWS]->() } >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("Combined EXISTS and COUNT failed: %v", err)
	}

	// Only Alice knows 2+ people
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestExistsOrNotExists tests EXISTS OR NOT EXISTS logic
func TestExistsOrNotExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:MANAGES]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who are managers OR have no connections
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:MANAGES]->() }
		   OR NOT EXISTS { MATCH (p)-[]->() }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS OR NOT EXISTS failed: %v", err)
	}

	// Alice (manager), Bob (no outgoing), Charlie (no connections), Eve (no outgoing)
	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 results, got %d", len(result.Rows))
	}
}

// TestCountSubqueryInExpression tests COUNT subquery used in expression
func TestCountSubqueryInExpression(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:KNOWS]->(e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people with exactly count = 1 or count = 2
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->() } >= 1
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery expression failed: %v", err)
	}

	// Alice (2) and Dave (1) have >= 1 outgoing KNOWS
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (Alice, Dave), got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithOrderByOnly tests CALL {} with just ORDER BY after (no RETURN)
func TestCallSubqueryWithOrderByOnly(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with ORDER BY applied to inner result
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN p.name AS name, p.age AS age
		}
		ORDER BY age ASC
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with ORDER BY only failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 results, got %d", len(result.Rows))
	}

	// First should be Bob (age 25)
	if result.Rows[0][0] != "Bob" {
		t.Errorf("Expected Bob first (youngest), got %v", result.Rows[0][0])
	}
}

// ========================================
// Whitespace Variation Tests
// ========================================

// TestExistsSubqueryWithNewlines tests EXISTS with varied whitespace/newlines
func TestExistsSubqueryWithNewlines(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// EXISTS with lots of whitespace and newlines
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS {
			MATCH (p)-[:KNOWS]->()
		}
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with newlines failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
}

// TestCountSubqueryWithNewlines tests COUNT with varied whitespace/newlines
func TestCountSubqueryWithNewlines(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// COUNT with lots of whitespace and newlines
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT {
			MATCH (p)-[:KNOWS]->(other)
		} >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT with newlines failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithNewlines tests CALL {} with varied whitespace/newlines
func TestCallSubqueryWithNewlines(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL with lots of whitespace and newlines
	result, err := exec.Execute(ctx, `
		CALL
		{
			MATCH (p:Person)
			RETURN p.name AS name
		}
		RETURN name
	`, nil)
	if err != nil {
		t.Fatalf("CALL with newlines failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestSubqueryMinimalWhitespace tests subqueries with minimal whitespace
func TestSubqueryMinimalWhitespace(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// EXISTS with minimal whitespace
	result, err := exec.Execute(ctx, `MATCH (p:Person) WHERE EXISTS{MATCH (p)-[:KNOWS]->()} RETURN p.name`, nil)
	if err != nil {
		t.Fatalf("EXISTS minimal whitespace failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestNotExistsSubqueryWithNewlines tests NOT EXISTS with varied whitespace
func TestNotExistsSubqueryWithNewlines(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// NOT EXISTS with newlines
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE NOT EXISTS
		{
			MATCH (p)-[:KNOWS]->()
		}
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("NOT EXISTS with newlines failed: %v", err)
	}

	// Bob and Charlie have no outgoing KNOWS
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result.Rows))
	}
}

// ========================================
// COLLECT Subquery Tests (Neo4j 5.0+)
// ========================================

// TestCollectSubquery tests COLLECT { } subquery for collecting values
func TestCollectSubquery(t *testing.T) {

	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// COLLECT subquery - collect friend names
	// Note: This might not be fully implemented yet
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		RETURN p.name, collect { 
			MATCH (p)-[:KNOWS]->(friend) 
			RETURN friend.name 
		} AS friends
	`, nil)

	if err != nil {
		t.Fatalf("COLLECT subquery failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 result, got %d", len(result.Rows))
	}
}

// ========================================
// Edge Cases and Special Scenarios
// ========================================

// TestExistsSubqueryEmptyResult tests EXISTS when no matches exist
func TestExistsSubqueryEmptyResult(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data - no relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'}),
		       (b:Person {name: 'Bob'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// EXISTS should return no matches when no relationships exist
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->() }
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS empty result failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 results, got %d", len(result.Rows))
	}
}

// TestCountSubqueryWithZeroMatches tests COUNT when no matches exist
func TestCountSubqueryWithZeroMatches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data - no relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'}),
		       (b:Person {name: 'Bob'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// COUNT = 0 should match all nodes without relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->() } = 0
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT zero matches failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results (Alice, Bob), got %d", len(result.Rows))
	}
}

// TestCallSubqueryEmptyResult tests CALL {} when inner query returns nothing
func TestCallSubqueryEmptyResult(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with no matching inner results
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:NonExistentLabel)
			RETURN p.name AS name
		}
		RETURN name
	`, nil)
	if err != nil {
		t.Fatalf("CALL empty result failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 results, got %d", len(result.Rows))
	}
}

// TestSubqueriesWithParameters tests subqueries with parameter substitution
func TestSubqueriesWithParameters(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// COUNT with parameter
	params := map[string]interface{}{
		"minCount": int64(2),
	}
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->() } >= $minCount
		RETURN p.name
	`, params)
	if err != nil {
		t.Fatalf("Subquery with parameter failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
}

// TestExistsSubqueryWithMultiplePatterns tests EXISTS with complex patterns
func TestExistsSubqueryWithMultiplePatterns(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data - chain: Alice -> Bob -> Charlie
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who know someone who knows someone else
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->()-[:KNOWS]->() }
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with chain pattern failed: %v", err)
	}

	// Alice knows Bob who knows Charlie
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithMerge tests CALL {} with MERGE operation
func TestCallSubqueryWithMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with MERGE inside - should create Bob if not exists
	_, err = exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CALL {
			WITH p
			MERGE (b:Person {name: 'Bob'})
			MERGE (p)-[:FRIEND_OF]->(b)
			RETURN b.name AS friendName
		}
		RETURN p.name, friendName
	`, nil)
	if err != nil {
		t.Fatalf("CALL subquery with MERGE failed: %v", err)
	}

	// Verify Bob was created
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Bob'}) RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("Failed to verify MERGE: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected Bob to be created via MERGE, got %d results", len(result.Rows))
	}

	// Verify relationship was created
	relResult, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'})-[:FRIEND_OF]->(b:Person {name: 'Bob'}) 
		RETURN a.name, b.name
	`, nil)
	if err != nil {
		t.Fatalf("Failed to verify relationship: %v", err)
	}
	if len(relResult.Rows) != 1 {
		t.Errorf("Expected FRIEND_OF relationship, got %d results", len(relResult.Rows))
	}
}

// TestMultipleSubqueriesInWhere tests multiple subqueries in same WHERE
func TestMultipleSubqueriesInWhere(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:WORKS_WITH]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})-[:KNOWS]->(f:Person {name: 'Frank'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who know 2+ people AND work with someone
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->() } >= 2
		  AND EXISTS { MATCH (p)-[:WORKS_WITH]->() }
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("Multiple subqueries failed: %v", err)
	}

	// Only Alice has 2+ KNOWS and WORKS_WITH
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
}

// TestExistsSubqueryWithWherePropertyComparison tests EXISTS subquery with WHERE property comparison
// This verifies that evaluateInnerWhere correctly handles property comparisons
func TestExistsSubqueryWithWherePropertyComparison(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS]->(b:Person {name: 'Bob', age: 25}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie', age: 35}),
		       (d:Person {name: 'Dave', age: 20})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find people who know someone older than 30
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->(other) WHERE other.age > 30 }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with WHERE property comparison failed: %v", err)
	}

	// Only Alice knows Charlie (age 35 > 30)
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}

	// Find people who know someone with name = 'Bob'
	result2, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->(other) WHERE other.name = 'Bob' }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with WHERE equality failed: %v", err)
	}

	// Only Alice knows Bob
	if len(result2.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result2.Rows))
	}

	// Test IS NOT NULL
	result3, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->(other) WHERE other.age IS NOT NULL }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with WHERE IS NOT NULL failed: %v", err)
	}

	// Alice knows Bob and Charlie (both have age), Dave has no connections
	if len(result3.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result3.Rows))
	}

	// Test AND condition
	// Bob's age is 25, Charlie's age is 35
	// Condition: other.age > 24 AND other.age < 26 matches Bob (25)
	result4, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE EXISTS { MATCH (p)-[:KNOWS]->(other) WHERE other.age > 24 AND other.age < 26 }
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS with WHERE AND condition failed: %v", err)
	}

	// Only Alice knows Bob (age 25 matches 24 < 25 < 26)
	if len(result4.Rows) != 1 {
		t.Errorf("Expected 1 result (Alice), got %d", len(result4.Rows))
	}
}

// TestNestedExistsSubquery tests nested EXISTS subqueries
func TestNestedExistsSubquery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:MANAGES]->(b:Person {name: 'Bob'}),
		       (b)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})-[:MANAGES]->(e:Person {name: 'Eve'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Find managers whose direct reports know someone
	result, err := exec.Execute(ctx, `
		MATCH (m:Person)
		WHERE EXISTS { 
			MATCH (m)-[:MANAGES]->(report)
			WHERE EXISTS { MATCH (report)-[:KNOWS]->() }
		}
		RETURN m.name
	`, nil)
	// Alice manages Bob who knows Charlie
	// Dave manages Eve but Eve doesn't know anyone, so Dave should NOT be included
	if len(result.Rows) != 1 {
		t.Errorf("Nested EXISTS returned %d results (expected 1 - only Alice): %v", len(result.Rows), result.Rows)
	} else if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCallSubqueryWithUnwind tests CALL {} with UNWIND inside
func TestCallSubqueryWithUnwind(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', skills: ['Go', 'Python', 'Rust']})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with UNWIND inside
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person {name: 'Alice'})
			UNWIND p.skills AS skill
			RETURN skill
		}
		RETURN skill
	`, nil)
	if err != nil {
		t.Fatalf("CALL with UNWIND failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 results (skills), got %d", len(result.Rows))
	}
}

// TestExistsSubqueryWithTabs tests EXISTS with tab characters
func TestExistsSubqueryWithTabs(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// EXISTS with tabs instead of spaces
	result, err := exec.Execute(ctx, "MATCH (p:Person)\tWHERE EXISTS {\tMATCH (p)-[:KNOWS]->()\t}\tRETURN p.name", nil)
	if err != nil {
		t.Fatalf("EXISTS with tabs failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestCallSubqueryOnSingleLine tests CALL {} all on one line
func TestCallSubqueryOnSingleLine(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice', age: 30})`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} all on one line
	result, err := exec.Execute(ctx, `CALL { MATCH (p:Person) RETURN p.name AS name } RETURN name`, nil)
	if err != nil {
		t.Fatalf("CALL single line failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestCountSubqueryNoSpaceBeforeBrace tests COUNT{ without space
func TestCountSubqueryNoSpaceBeforeBrace(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// COUNT{} without space before brace
	result, err := exec.Execute(ctx, `MATCH (p:Person) WHERE COUNT{ MATCH (p)-[:KNOWS]->() } >= 2 RETURN p.name`, nil)
	if err != nil {
		t.Fatalf("COUNT without space failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestExistsNoSpaceBeforeBrace tests EXISTS{ without space
func TestExistsNoSpaceBeforeBrace(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// EXISTS{ without space before brace
	result, err := exec.Execute(ctx, `MATCH (p:Person) WHERE EXISTS{ MATCH (p)-[:KNOWS]->() } RETURN p.name`, nil)
	if err != nil {
		t.Fatalf("EXISTS without space failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

// TestCallSubqueryWithOptionalMatch tests CALL {} with OPTIONAL MATCH inside
func TestCallSubqueryWithOptionalMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} with OPTIONAL MATCH inside
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			OPTIONAL MATCH (p)-[:KNOWS]->(friend)
			RETURN p.name AS name, friend.name AS friendName
		}
		RETURN name, friendName
		ORDER BY name
	`, nil)
	if err != nil {
		t.Fatalf("CALL with OPTIONAL MATCH failed: %v", err)
	}

	// Should have results for all persons
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 results, got %d", len(result.Rows))
	}
}

// TestSubqueryWithNestedBraces tests subqueries with nested braces (maps)
func TestSubqueryWithNestedBraces(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with map property
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', meta: 'data'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// CALL {} that creates nodes with map properties
	result, err := exec.Execute(ctx, `
		CALL {
			MATCH (p:Person)
			RETURN p.name AS name, {found: true} AS status
		}
		RETURN name, status
	`, nil)
	if err != nil {
		t.Fatalf("CALL with map in RETURN failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
}

func TestSubqueryHelpers_AddLimitSkipAndAfterCallProcessing(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	// addLimitSkipToSubquery branches
	s1 := exec.addLimitSkipToSubquery("MATCH (n) SET n.x = 1 RETURN n", 10, 0)
	assert.Contains(t, s1, "WITH n LIMIT 10")
	assert.Contains(t, s1, "SET n.x = 1")

	s2 := exec.addLimitSkipToSubquery("MATCH (n) CREATE (m) RETURN n", 5, 2)
	assert.Contains(t, s2, "WITH n SKIP 2 LIMIT 5")
	assert.Contains(t, s2, "CREATE (m)")

	s3 := exec.addLimitSkipToSubquery("MATCH (n) RETURN n", 3, 1)
	assert.Contains(t, s3, "SKIP 1 LIMIT 3")
	assert.Contains(t, s3, "RETURN n")

	s4 := exec.addLimitSkipToSubquery("MATCH (n)", 7, 0)
	assert.Contains(t, s4, "LIMIT 7")

	s5 := exec.addLimitSkipToSubquery("MATCH (n) RETURN n LIMIT 1", 2, 0)
	assert.Contains(t, s5, "LIMIT 1")
	assert.Contains(t, s5, "LIMIT 2")

	// convertWriteSubqueryToRead branches
	assert.Equal(t,
		"MATCH (n) RETURN n",
		exec.makeSubqueryReadOnly("MATCH (n) SET n.x = 1 RETURN n"),
	)
	assert.Equal(t,
		"MATCH (n) RETURN n",
		exec.makeSubqueryReadOnly("MATCH (n) CREATE (m) RETURN n"),
	)
	assert.Equal(t, "", exec.makeSubqueryReadOnly("CALL db.labels()"))

	inner := &ExecuteResult{
		Columns: []string{"name", "score"},
		Rows: [][]interface{}{
			{"alice", float64(0.9)},
			{"bob", float64(0.8)},
		},
	}

	// processAfterCallSubquery RETURN path
	ret, err := exec.processAfterCallSubquery(ctx, inner, "RETURN name, score")
	require.NoError(t, err)
	require.Equal(t, []string{"name", "score"}, ret.Columns)
	require.Len(t, ret.Rows, 2)

	// ORDER BY path + modifiers
	ordered, err := exec.processAfterCallSubquery(ctx, inner, "ORDER BY score DESC LIMIT 1")
	require.NoError(t, err)
	require.Len(t, ordered.Rows, 1)
	assert.Equal(t, "alice", ordered.Rows[0][0])

	// unsupported clause branch
	_, err = exec.processAfterCallSubquery(ctx, inner, "WITH name RETURN name")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported clause after CALL {}")

	// processCallSubqueryReturn: aggregation and aliases
	innerForAgg := &ExecuteResult{
		Columns: []string{"name", "score"},
		Rows: [][]interface{}{
			{"alice", float64(0.9)},
			{"bob", float64(0.8)},
		},
	}
	agg, err := exec.processCallSubqueryReturn(ctx, innerForAgg, "RETURN count(*) AS c")
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, agg.Columns)
	require.Len(t, agg.Rows, 1)
	assert.Equal(t, int64(2), agg.Rows[0][0])
}

func TestSubqueryHelpers_ExecuteMatchWithCallSubquery_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.executeMatchWithCallSubquery(ctx, "MATCH (n) RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CALL not found")

	_, err = exec.executeMatchWithCallSubquery(ctx, "CALL { RETURN 1 AS x } RETURN x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MATCH not found")

	_, err = exec.executeMatchWithCallSubquery(ctx, "MATCH (:Person) CALL { WITH seed RETURN seed } RETURN seed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not parse node pattern")

	// No seed nodes branch.
	emptyRes, err := exec.executeMatchWithCallSubquery(ctx, "MATCH (seed:Person) WHERE seed.name = 'none' CALL { WITH seed RETURN seed } RETURN seed")
	require.NoError(t, err)
	assert.Equal(t, []string{"seed"}, emptyRes.Columns)
	assert.Empty(t, emptyRes.Rows)

	_, err = eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// Empty CALL body branch.
	_, err = exec.executeMatchWithCallSubquery(ctx, "MATCH (seed:Person) CALL { } RETURN seed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty body")

	// No WITH branch routes through executeCallSubquery.
	noWithRes, err := exec.executeMatchWithCallSubquery(ctx, "MATCH (seed:Person) CALL { RETURN 1 AS x } RETURN x")
	require.NoError(t, err)
	require.NotEmpty(t, noWithRes.Rows)
	assert.Equal(t, int64(1), noWithRes.Rows[0][0])

	// Correlated WITH branch and after-CALL RETURN processing.
	withRes, err := exec.executeMatchWithCallSubquery(ctx, "MATCH (seed:Person) CALL { WITH seed RETURN seed } RETURN seed")
	require.NoError(t, err)
	require.Len(t, withRes.Rows, 2)
}

func TestSubqueryHelpers_ExecuteMatchWithCallProcedure_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n) RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CALL not found")

	_, err = exec.executeMatchWithCallProcedure(ctx, "CALL db.info()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MATCH not found")

	_, err = exec.executeMatchWithCallProcedure(ctx, "MATCH (:Person) CALL db.info()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not parse node pattern")

	// No matched nodes -> columns from YIELD.
	emptyYieldRes, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person {name: 'none'}) CALL db.info() YIELD name RETURN name")
	require.NoError(t, err)
	assert.Equal(t, []string{"name"}, emptyYieldRes.Columns)
	assert.Empty(t, emptyYieldRes.Rows)

	// No matched nodes -> vector-query defaults.
	emptyVectorRes, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person {name: 'none'}) CALL db.index.vector.queryNodes('idx', 2, n.embedding)")
	require.NoError(t, err)
	assert.Equal(t, []string{"node", "score"}, emptyVectorRes.Columns)
	assert.Empty(t, emptyVectorRes.Rows)

	_, err = eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// Matched nodes + failing call path.
	_, err = exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person) CALL db.unknownProcedure()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute CALL")

	// Matched nodes + successful call, results merged across seeds.
	okRes, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person) CALL db.info() YIELD name RETURN name")
	require.NoError(t, err)
	require.Len(t, okRes.Rows, 2)
}

func TestSubqueryHelpers_BatchingAndResultModifiers_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "a", "age": int64(10)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "b", "age": int64(20)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "c", "age": int64(30)}})
	require.NoError(t, err)

	readOnlyRes, err := exec.executeCallInTransactions(ctx, "MATCH (n:Person) RETURN n.name AS name", 0)
	require.NoError(t, err)
	require.Len(t, readOnlyRes.Rows, 3)

	writeRes, err := exec.executeCallInTransactions(ctx, "MATCH (n:Person) SET n.flag = true RETURN n.name AS name", 2)
	require.NoError(t, err)
	require.Len(t, writeRes.Rows, 3)
	for _, row := range writeRes.Rows {
		require.Len(t, row, 1)
		require.NotEmpty(t, row[0])
	}

	_, err = exec.executeCallInTransactions(ctx, "MATCH (n:Person) SET n.bad = true RETURN", 1)
	require.Error(t, err)

	withLimit := exec.addLimitSkipToSubquery("MATCH (n:Person) SET n.flag = true RETURN n.name AS name", 2, 1)
	assert.Contains(t, withLimit, "WITH n SKIP 1 LIMIT 2")
	withOnlyLimit := exec.addLimitSkipToSubquery("MATCH (n:Person) SET n.flag = true RETURN n.name AS name", 2, 0)
	assert.Contains(t, withOnlyLimit, "WITH n LIMIT 2")
	fallbackNoReturn := exec.addLimitSkipToSubquery("CREATE (n:Tmp)", 2, 1)
	assert.Equal(t, "CREATE (n:Tmp) SKIP 1 LIMIT 2", fallbackNoReturn)
	fallbackExistingLimit := exec.addLimitSkipToSubquery("MATCH (n:Person) RETURN n.name LIMIT 1", 2, 0)
	assert.Contains(t, fallbackExistingLimit, "LIMIT 2")

	inner := &ExecuteResult{
		Columns: []string{"name", "age"},
		Rows: [][]interface{}{
			{"a", int64(10)},
			{"b", int64(20)},
			{"c", int64(30)},
		},
		Stats: &QueryStats{},
	}
	retRes, err := exec.processCallSubqueryReturn(ctx, inner, "RETURN name AS n, age ORDER BY age DESC SKIP 1 LIMIT 1")
	require.NoError(t, err)
	require.Equal(t, []string{"n", "age"}, retRes.Columns)
	require.Len(t, retRes.Rows, 1)
	assert.Equal(t, "b", retRes.Rows[0][0])

	aggRes, err := exec.processCallSubqueryReturn(ctx, inner, "RETURN count(*) AS c, sum(age) AS s, avg(age) AS av, min(age) AS mn, max(age) AS mx, collect(name) AS names")
	require.NoError(t, err)
	require.Len(t, aggRes.Rows, 1)
	assert.Equal(t, int64(3), aggRes.Rows[0][0])
	assert.Equal(t, float64(60), aggRes.Rows[0][1])
	assert.Equal(t, float64(20), aggRes.Rows[0][2])

	_, err = exec.processAfterCallSubquery(ctx, inner, "SET x = 1")
	require.Error(t, err)

	modified, err := exec.applyResultModifiers(inner, "ORDER BY age DESC SKIP 1 LIMIT 1")
	require.NoError(t, err)
	require.Len(t, modified.Rows, 1)
	assert.Equal(t, "b", modified.Rows[0][0])

	// Top-k post-processing should preserve ORDER BY + LIMIT semantics.
	topKInner := &ExecuteResult{
		Columns: []string{"name", "age"},
		Rows:    make([][]interface{}, 0, 200),
		Stats:   &QueryStats{},
	}
	for i := 0; i < 200; i++ {
		age := int64((i * 37) % 97)
		topKInner.Rows = append(topKInner.Rows, []interface{}{fmt.Sprintf("p%03d", i), age})
	}

	cloneRows := func(in [][]interface{}) [][]interface{} {
		out := make([][]interface{}, len(in))
		for i := range in {
			out[i] = append([]interface{}(nil), in[i]...)
		}
		return out
	}

	gotTopK, err := exec.applyResultModifiers(&ExecuteResult{
		Columns: append([]string(nil), topKInner.Columns...),
		Rows:    cloneRows(topKInner.Rows),
		Stats:   &QueryStats{},
	}, "ORDER BY age DESC LIMIT 10")
	require.NoError(t, err)
	require.Len(t, gotTopK.Rows, 10)
	for i := 1; i < len(gotTopK.Rows); i++ {
		prev := gotTopK.Rows[i-1][1].(int64)
		cur := gotTopK.Rows[i][1].(int64)
		require.GreaterOrEqual(t, prev, cur)
	}

	gotWindow, err := exec.applyResultModifiers(&ExecuteResult{
		Columns: append([]string(nil), topKInner.Columns...),
		Rows:    cloneRows(topKInner.Rows),
		Stats:   &QueryStats{},
	}, "ORDER BY age ASC SKIP 5 LIMIT 7")
	require.NoError(t, err)
	require.Len(t, gotWindow.Rows, 7)
	for i := 1; i < len(gotWindow.Rows); i++ {
		prev := gotWindow.Rows[i-1][1].(int64)
		cur := gotWindow.Rows[i][1].(int64)
		require.LessOrEqual(t, prev, cur)
	}
}

func TestExecute_MatchWithOuterWith_CallSubqueryUnionVectorPipeline(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered', embedding: [1.0, 0.0, 0.0]})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	query := `
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.GreaterOrEqual(t, len(result.Rows), 2)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
}

func TestExecute_MatchWithOuterWith_CallSubqueryUnionVectorPipeline_StringQuery_ReturnsCandidateRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered', embedding: [1.0, 0.0, 0.0]})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	// String-query vector path requires an embedder.
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1.0, 0.0, 0.0}})

	query := `
MATCH (p:SystemPrompt)
WITH p
LIMIT 1
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, "GEt it delivered")
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.GreaterOrEqual(t, len(result.Rows), 2)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])

	foundCandidate := false
	for _, row := range result.Rows {
		if len(row) > 0 {
			if typ, ok := row[0].(string); ok && typ == "CANDIDATE" {
				foundCandidate = true
				break
			}
		}
	}
	assert.True(t, foundCandidate, "expected at least one CANDIDATE row")
}

func TestExecute_MatchIDSeed_WithUnionVectorPipeline(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("sp-fixed"),
		Labels: []string{"SystemPrompt"},
		Properties: map[string]interface{}{
			"promptId": "prompt-id",
			"text":     "prompt text",
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered', embedding: [1.0, 0.0, 0.0]})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	query := `
MATCH (p)
WHERE id(p) = 'sp-fixed'
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  WITH node, score, t
  ORDER BY score DESC, t.language
  LIMIT 5
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.GreaterOrEqual(t, len(result.Rows), 2)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
}

func TestExecute_UnionVectorTraversalShape_ReturnsSixRowsDeterministically(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})`, nil)
	require.NoError(t, err)

	// 5 candidate rows expected from vector topK + traversal.
	for i := 1; i <= 5; i++ {
		q := fmt.Sprintf(`
CREATE (o:OriginalText {id: 'o%d', originalText: 'text-%d', embedding: [1.0, 0.0, 0.0]})
CREATE (t:TranslatedText {id: 't%d', language: 'es', translatedText: 'tx-%d'})
CREATE (o)-[:TRANSLATES_TO]->(t)
`, i, i, i, i)
		_, err = exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	query := `
MATCH (p:SystemPrompt)
WITH p
LIMIT 1
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.Len(t, result.Rows, 6)

	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
	assert.Equal(t, "prompt text", result.Rows[0][1])

	candidateCount := 0
	for i := 1; i < len(result.Rows); i++ {
		row := result.Rows[i]
		require.GreaterOrEqual(t, len(row), 6)
		assert.Equal(t, "CANDIDATE", row[0])
		assert.NotNil(t, row[2], "candidate originalText must be present")
		assert.NotNil(t, row[4], "candidate language must be present")
		assert.NotNil(t, row[5], "candidate translatedText must be present")
		candidateCount++
	}
	assert.Equal(t, 5, candidateCount)
}

func TestExecute_MatchWithOuterWith_CallSubqueryUnionVectorPipeline_EmptySecondArmKeepsColumns(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered', embedding: [1.0, 0.0, 0.0]})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
	`, nil)
	require.NoError(t, err)

	// The second UNION arm has a full RETURN projection but produces no rows due to
	// missing TRANSLATES_TO relationship. It must still contribute column shape.
	query := `
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
  YIELD node, score
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
	assert.Equal(t, "prompt text", result.Rows[0][1])
}

func TestExecute_MatchWithOuterWith_CallSubqueryUnionVectorPipeline_QueryNodesNoRowsKeepsColumns(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX idx_original_text FOR (n:OriginalText) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered', embedding: [1.0, 0.0, 0.0]})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	// Regression: if queryNodes branch yields no rows, UNION must still keep second-arm
	// RETURN column shape and not fail with "got 7 and 0".
	query := `
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
  YIELD node, score
  WHERE score < 0
  MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    node.originalText AS originalText,
    score AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
	assert.Equal(t, "prompt text", result.Rows[0][1])
}

func TestExecute_CallSubqueryUnion_TraversalAggregationEmptyArmKeepsColumns(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'prompt text'})
		CREATE (o:OriginalText {id: 'o1', originalText: 'Get it delivered'})
		CREATE (t:TranslatedText {id: 't1', language: 'es', translatedText: 'Recibelo'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	// Second UNION arm includes traversal + aggregation and is filtered to empty.
	// UNION must still preserve the branch's RETURN column shape.
	query := `
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN
    0 AS sortOrder,
    'SYSTEM_PROMPT' AS rowType,
    p.text AS systemPrompt,
    null AS originalText,
    null AS score,
    null AS language,
    null AS translatedText

  UNION ALL

  WITH p
  MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
  WITH o, t, count(t) AS relCount
  WHERE relCount > 99
  RETURN
    1 AS sortOrder,
    'CANDIDATE' AS rowType,
    null AS systemPrompt,
    o.originalText AS originalText,
    toFloat(relCount) AS score,
    t.language AS language,
    t.translatedText AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6
`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rowType", "systemPrompt", "originalText", "score", "language", "translatedText"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "SYSTEM_PROMPT", result.Rows[0][0])
	assert.Equal(t, "prompt text", result.Rows[0][1])
}

func TestExecute_MatchWithCallSubquery_NoSeedRows_PreservesReturnColumns(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	query := `
MATCH (p:SystemPrompt {promptId: "missing"})
WITH p
CALL {
  WITH p
  RETURN p.text AS systemPrompt
}
RETURN systemPrompt
`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"systemPrompt"}, result.Columns)
	require.Len(t, result.Rows, 0)
}

func TestExecute_MatchWithCallSubquery_NodeImportProjectsProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (p:SystemPrompt {promptId: 'prompt-id', text: 'this is a system prompt'})
	`, nil)
	require.NoError(t, err)

	query := `
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN p.text AS systemPrompt
}
RETURN systemPrompt
`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"systemPrompt"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "this is a system prompt", result.Rows[0][0])
}

func TestExecute_MatchWithCallSubquery_OuterWithLimitIsHonored(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (p1:SystemPrompt {promptId: 'p1', text: 'one'}),
		       (p2:SystemPrompt {promptId: 'p2', text: 'two'}),
		       (p3:SystemPrompt {promptId: 'p3', text: 'three'})
	`, nil)
	require.NoError(t, err)

	query := `
MATCH (p:SystemPrompt)
WITH p
ORDER BY p.promptId
LIMIT 1
CALL {
  WITH p
  RETURN p.promptId AS promptId, p.text AS systemPrompt
}
RETURN promptId, systemPrompt
`
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"promptId", "systemPrompt"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "p1", result.Rows[0][0])
	require.Equal(t, "one", result.Rows[0][1])
}

func TestSeedNodesFromOuterMatch_WithPipelineReturnProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (p1:SystemPrompt {promptId: 'p1', text: 'one'}),
		       (p2:SystemPrompt {promptId: 'p2', text: 'two'})
	`, nil)
	require.NoError(t, err)

	nodes, err := exec.seedNodesFromOuterMatch(ctx, `
MATCH (p:SystemPrompt)
WITH p
LIMIT 1
`, "p")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotNil(t, nodes[0])
}

func TestSubqueryUnionBranchClassification_ReturnThenCall(t *testing.T) {
	body := `
WITH p
RETURN
  0 AS sortOrder,
  'SYSTEM_PROMPT' AS rowType,
  p.text AS systemPrompt,
  null AS originalText,
  null AS score,
  null AS language,
  null AS translatedText
UNION ALL
WITH p
CALL db.index.vector.queryNodes('idx_original_text', 5, "GET it delivered")
YIELD node, score
MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WITH node, score, t
ORDER BY score DESC, t.language
LIMIT 5
RETURN
  1 AS sortOrder,
  'CANDIDATE' AS rowType,
  null AS systemPrompt,
  node.originalText AS originalText,
  score AS score,
  t.language AS language,
  t.translatedText AS translatedText
`

	branches, all, ok := splitTopLevelUnionBranches(body)
	require.True(t, ok)
	require.True(t, all)
	require.Len(t, branches, 2)

	withVars0, inner0, hasWith0, err := parseLeadingWithImports(branches[0])
	require.NoError(t, err)
	require.True(t, hasWith0)
	require.Equal(t, []string{"p"}, withVars0)
	assert.True(t, isCallSubqueryPureReturn(inner0), "first UNION branch should be pure RETURN projection")

	withVars1, inner1, hasWith1, err := parseLeadingWithImports(branches[1])
	require.NoError(t, err)
	require.True(t, hasWith1)
	require.Equal(t, []string{"p"}, withVars1)
	assert.False(t, isCallSubqueryPureReturn(inner1), "second UNION branch should not be pure RETURN")
}

func TestParseLeadingWithImports_WithWherePredicate(t *testing.T) {
	withVars, inner, hasWith, err := parseLeadingWithImports(`
WITH o, t
WHERE t IS NULL
CREATE (n:Tmp)
RETURN n`)
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, []string{"o", "t"}, withVars)
	require.True(t, strings.HasPrefix(strings.TrimSpace(inner), "WITH o, t WHERE t IS NULL CREATE"), "inner body should preserve WITH ... WHERE clause semantics")
}

func TestSubqueryHelpers_IterativeCallInTransactionsBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	// No MATCH in subquery => makeSubqueryReadOnly returns empty, using iterative batching.
	// First batch then fails deterministically due invalid procedure call.
	_, err := exec.executeCallInTransactions(ctx, "CALL totally.missing.procedure()", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subquery execution failed")
}

func TestSubqueryHelpers_CallInTransactions_NonBatchableWriteExecutesOnce(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	res, err := exec.executeCallInTransactions(ctx, "CREATE (n:TmpOnce {name:'once'}) RETURN n.name AS name", 1)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "once", res.Rows[0][0])

	verify, err := exec.Execute(ctx, "MATCH (n:TmpOnce) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Equal(t, int64(1), verify.Rows[0][0])
}

func TestSubqueryHelpers_CallInTransactions_KnownBatchCountErrorBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)
	require.NoError(t, eng.GetSchema().AddConstraint(storage.Constraint{
		Name:       "uq_person_email",
		Type:       storage.ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	}))

	// Row count is known from MATCH/RETURN, but the second batch fails due unique constraint.
	_, err = exec.executeCallInTransactions(ctx, "MATCH (n:Person) SET n.email = 'dup@example.com' RETURN n.name AS name", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch 2/2 failed")
}

func TestSubqueryHelpers_CallInTransactions_IterativeBatchingWithUnwind(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	// UNWIND write query is not convertible by makeSubqueryReadOnly, so this exercises
	// iterative batching with a batchable source.
	res, err := exec.executeCallInTransactions(ctx, "UNWIND [1,2,3] AS i CREATE (n:IterTx {v:i}) RETURN i AS i", 2)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"i"}, res.Columns)
	require.Len(t, res.Rows, 3)
	assert.EqualValues(t, 3, res.Stats.NodesCreated)

	verify, err := exec.Execute(ctx, "MATCH (n:IterTx) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), verify.Rows[0][0])
}

func TestSubqueryHelpers_CallInTransactions_IterativeBatchingWithMatchMerge(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carol"}})
	require.NoError(t, err)

	// makeSubqueryReadOnly cannot rewrite MATCH...MERGE, forcing iterative batching.
	res, err := exec.executeCallInTransactions(ctx, "MATCH (n:Person) MERGE (m:Tag {name:n.name}) RETURN n.name AS name", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 3)
	require.NotNil(t, res.Stats)
	assert.GreaterOrEqual(t, res.Stats.NodesCreated, 1)

	verify, err := exec.Execute(ctx, "MATCH (m:Tag) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), verify.Rows[0][0])
}

func TestSubqueryHelpers_ParseCallSubquery_EdgeBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	body, after, inTx, batch := exec.parseCallSubquery("CALL db.info()")
	assert.Empty(t, body)
	assert.Empty(t, after)
	assert.False(t, inTx)
	assert.Equal(t, 1000, batch)

	body, after, inTx, batch = exec.parseCallSubquery("CALL { RETURN 1 ")
	assert.Empty(t, body)
	assert.Empty(t, after)
	assert.False(t, inTx)
	assert.Equal(t, 1000, batch)

	body, after, inTx, batch = exec.parseCallSubquery("CALL { RETURN 1 AS x } IN TRANSACTIONS OF nope ROWS RETURN x")
	assert.Equal(t, "RETURN 1 AS x", body)
	assert.Equal(t, "RETURN x", after)
	assert.True(t, inTx)
	// Invalid batch size falls back to default.
	assert.Equal(t, 1000, batch)

	body, after, inTx, batch = exec.parseCallSubquery("CALL { RETURN 1 AS x } IN TRANSACTIONS OF 7 ROWS RETURN x")
	assert.Equal(t, "RETURN 1 AS x", body)
	assert.Equal(t, "RETURN x", after)
	assert.True(t, inTx)
	assert.Equal(t, 7, batch)
}

func TestSubqueryHelpers_SubstituteBoundVariablesInCall_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	node := &storage.Node{
		ID: "n1",
		Properties: map[string]interface{}{
			"s":         "alice",
			"i":         int64(7),
			"f":         float64(1.5),
			"b":         true,
			"obj":       map[string]interface{}{"x": 1},
			"embedding": []interface{}{float64(0.1), int64(2), float32(0.3)},
		},
	}
	ctxMap := map[string]*storage.Node{"n": node}

	// Replacements for scalar property types and []interface{} embedding conversion.
	got := exec.substituteBoundVariablesInCall(
		"CALL x(n.s, n.i, n.f, n.b, n.embedding, n.obj)",
		ctxMap,
	)
	assert.Contains(t, got, "'alice'")
	assert.Contains(t, got, "7")
	assert.Contains(t, got, "1.5")
	assert.Contains(t, got, "true")
	assert.Contains(t, got, "[0.1, 2, 0.3]")
	assert.Contains(t, got, "map[x:1]")

	// Quoted references should not be replaced.
	quoted := exec.substituteBoundVariablesInCall("CALL x('n.s', \"n.i\")", ctxMap)
	assert.Equal(t, "CALL x('n.s', \"n.i\")", quoted)

	// embedding reads from Properties like any other property (no ChunkEmbeddings routing).
	// []float64 embedding conversion path.
	node.Properties["embedding"] = []float64{0.4, 0.5}
	got = exec.substituteBoundVariablesInCall("CALL x(n.embedding)", ctxMap)
	assert.Contains(t, got, "[0.4, 0.5]")

	// []float32 embedding conversion path.
	node.Properties["embedding"] = []float32{0.6, 0.7}
	got = exec.substituteBoundVariablesInCall("CALL x(n.embedding)", ctxMap)
	assert.Contains(t, got, "[0.6, 0.7]")
}

func TestSubqueryHelpers_ExecuteMatchWithCallProcedure_ParamAndWhereBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(30)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(20)}})
	require.NoError(t, err)

	// Parameter substitution + WHERE filtering + MATCH without label (AllNodes branch).
	ctxWithParams := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"name": "alice"})
	res, err := exec.executeMatchWithCallProcedure(
		ctxWithParams,
		"MATCH (n) WHERE n.name = $name CALL db.info() YIELD name RETURN name",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "nornicdb", res.Rows[0][0])
}

func TestSubqueryHelpers_AddLimitSkipToSubquery_WithWhereAndFallbackVariable(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// WHERE clause path should still inject WITH var SKIP/LIMIT before SET.
	withWhere := exec.addLimitSkipToSubquery(
		"MATCH (p:Person) WHERE p.age > 10 SET p.active = true RETURN p.name",
		5,
		2,
	)
	assert.Contains(t, withWhere, "WITH p SKIP 2 LIMIT 5")
	assert.Contains(t, withWhere, "SET p.active = true")

	// No variable in pattern uses fallback variable name 'n' for WITH clause.
	noVar := exec.addLimitSkipToSubquery(
		"MATCH (:Person) RETURN 1 AS one",
		3,
		0,
	)
	assert.Contains(t, noVar, "WITH n LIMIT 3")
	assert.Contains(t, noVar, "RETURN 1 AS one")
}

func TestSubqueryHelpers_AddLimitSkipToSubquery_OperationBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	tests := []struct {
		name   string
		query  string
		limit  int
		skip   int
		assert func(t *testing.T, got string)
	}{
		{
			name:  "match_delete_branch",
			query: "MATCH (n:Person) DELETE n RETURN count(*)",
			limit: 4,
			skip:  1,
			assert: func(t *testing.T, got string) {
				assert.Contains(t, got, "WITH n SKIP 1 LIMIT 4")
				assert.Contains(t, got, "DELETE n")
			},
		},
		{
			name:  "match_merge_branch",
			query: "MATCH (n:Person) MERGE (m:Person {name:'x'}) RETURN m",
			limit: 2,
			skip:  0,
			assert: func(t *testing.T, got string) {
				assert.Contains(t, got, "WITH n LIMIT 2")
				assert.Contains(t, got, "MERGE (m:Person")
			},
		},
		{
			name:  "match_return_branch",
			query: "MATCH (n:Person) RETURN n.name",
			limit: 3,
			skip:  1,
			assert: func(t *testing.T, got string) {
				assert.Contains(t, got, "WITH n SKIP 1 LIMIT 3")
				assert.Contains(t, got, "RETURN n.name")
			},
		},
		{
			name:  "fallback_before_return_without_match",
			query: "WITH 1 AS x RETURN x",
			limit: 9,
			skip:  2,
			assert: func(t *testing.T, got string) {
				assert.Contains(t, got, "SKIP 2 LIMIT 9 RETURN x")
			},
		},
		{
			name:  "fallback_existing_limit_skip",
			query: "WITH 1 AS x RETURN x LIMIT 1",
			limit: 6,
			skip:  0,
			assert: func(t *testing.T, got string) {
				assert.Contains(t, got, "RETURN x LIMIT 1 LIMIT 6")
			},
		},
		{
			name:  "fallback_no_return",
			query: "WITH 1 AS x",
			limit: 5,
			skip:  0,
			assert: func(t *testing.T, got string) {
				assert.Equal(t, "WITH 1 AS x LIMIT 5", got)
			},
		},
		{
			name:  "match_no_operation_then_no_return_fallback",
			query: "MATCH (n:Person)",
			limit: 7,
			skip:  1,
			assert: func(t *testing.T, got string) {
				assert.Equal(t, "MATCH (n:Person) SKIP 1 LIMIT 7", got)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := exec.addLimitSkipToSubquery(tc.query, tc.limit, tc.skip)
			tc.assert(t, got)
		})
	}
}

func TestExistsSubqueryHelpers_DirectBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	var alice *storage.Node
	for _, n := range nodes {
		if n.Properties["name"] == "Alice" {
			alice = n
			break
		}
	}
	require.NotNil(t, alice)

	// Empty/malformed clauses intentionally default to true to avoid false negatives.
	assert.True(t, exec.evaluateExistsSubquery(ctx, alice, "p", "name = 'x'"))
	assert.True(t, exec.evaluateNotExistsSubquery(ctx, alice, "p", "name = 'x'"))

	assert.True(t, exec.evaluateExistsSubquery(ctx, alice, "p", "EXISTS { MATCH (p)-[:KNOWS]->() }"))
	assert.False(t, exec.evaluateNotExistsSubquery(ctx, alice, "p", "NOT EXISTS { MATCH (p)-[:KNOWS]->() }"))
}

func TestExecuteMatchWithCallProcedure_ParseAndExecErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	// Pattern that cannot be parsed into a node variable.
	_, err := exec.executeMatchWithCallProcedure(ctx, "MATCH () CALL db.info() YIELD name RETURN name")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not parse node pattern")

	// Valid parsed node, but CALL execution should error for unknown procedure.
	_, err = exec.Execute(ctx, "CREATE (n:Person {name:'a'})", nil)
	require.NoError(t, err)
	_, err = exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person) CALL db.missingProcedure()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute CALL")

	// No matching rows, no YIELD, non-vector procedure => empty columns branch.
	emptyRes, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person {name:'none'}) CALL db.info()")
	require.NoError(t, err)
	require.NotNil(t, emptyRes)
	assert.Empty(t, emptyRes.Columns)
	assert.Empty(t, emptyRes.Rows)

	// No matching rows + relationship-vector procedure => default relationship columns.
	emptyRelVectorRes, err := exec.executeMatchWithCallProcedure(
		ctx,
		"MATCH (n:Person {name:'none'}) CALL db.index.vector.queryRelationships('idx', 2, n.embedding)",
	)
	require.NoError(t, err)
	require.NotNil(t, emptyRelVectorRes)
	assert.Equal(t, []string{"relationship", "score"}, emptyRelVectorRes.Columns)
	assert.Empty(t, emptyRelVectorRes.Rows)

	// No matching rows + YIELD alias should preserve alias column name.
	emptyAliasRes, err := exec.executeMatchWithCallProcedure(
		ctx,
		"MATCH (n:Person {name:'none'}) CALL db.info() YIELD name AS db_name RETURN db_name",
	)
	require.NoError(t, err)
	require.NotNil(t, emptyAliasRes)
	assert.Equal(t, []string{"db_name"}, emptyAliasRes.Columns)
	assert.Empty(t, emptyAliasRes.Rows)
}

func TestExecuteMatchWithCallProcedure_NodeLookupAndNilResultBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("id seek path avoids label/all-nodes scan", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		engine := &failingNodeLookupEngine{
			Engine:      base,
			allNodesErr: errors.New("all nodes failed"),
			byLabelErr:  errors.New("label lookup failed"),
		}
		exec := NewStorageExecutor(engine)

		_, err := engine.CreateNode(&storage.Node{
			ID:     "sp-1",
			Labels: []string{"SystemPrompt"},
			Properties: map[string]interface{}{
				"promptId": "prompt-id",
				"text":     "system text",
			},
		})
		require.NoError(t, err)

		ClearUserProcedures()
		t.Cleanup(ClearUserProcedures)
		require.NoError(t, RegisterUserProcedure(
			ProcedureSpec{Name: "custom.const", MinArgs: 0, MaxArgs: 0},
			func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{
					Columns: []string{"x"},
					Rows:    [][]interface{}{{int64(1)}},
				}, nil
			},
		))

		// This previously hit manual label/all-node scans in executeMatchWithCallProcedure.
		// It should now execute the outer MATCH via the normal executor and succeed.
		res, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (p:SystemPrompt) WHERE id(p) = 'sp-1' CALL custom.const() YIELD x RETURN x")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.Rows)
	})

	t.Run("matched rows but nil call result returns empty result", func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		exec := NewStorageExecutor(base)

		_, err := base.CreateNode(&storage.Node{
			ID:     "p1",
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"name": "alice",
			},
		})
		require.NoError(t, err)

		// Register a deterministic procedure that returns nil result without error.
		ClearUserProcedures()
		t.Cleanup(ClearUserProcedures)
		require.NoError(t, RegisterUserProcedure(
			ProcedureSpec{Name: "custom.nil", MinArgs: 0, MaxArgs: 0},
			func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return nil, nil
			},
		))

		res, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n:Person) CALL custom.nil()")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Empty(t, res.Columns)
		assert.Empty(t, res.Rows)
	})
}

func TestExecuteCorrelatedCallWithSeedRows_BatchedLookup(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "o1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId":     "s1",
			"textKey":      "k1",
			"originalText": "one",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "o2",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId":     "s2",
			"textKey128":   "k2",
			"originalText": "two",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "i1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId":       "s1",
			"language":       "en",
			"translatedText": "one-en",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "i2",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId":       "s1",
			"language":       "es",
			"translatedText": "one-es",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "i3",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId":       "s2",
			"language":       "en",
			"translatedText": "two-en",
		},
	})
	require.NoError(t, err)

	seed := &ExecuteResult{
		Columns: []string{"sourceId"},
		Rows: [][]interface{}{
			{"s1"},
			{"s2"},
		},
	}
	res, err := exec.executeCorrelatedCallWithSeedRows(
		ctx,
		seed,
		"MATCH (tt:MongoDocument) WHERE tt.sourceId = sourceId AND tt.translatedText IS NOT NULL RETURN tt.language AS language, tt.translatedText AS translatedText",
		[]string{"sourceId"},
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"sourceId", "language", "translatedText"}, res.Columns)
	require.Len(t, res.Rows, 3)

	got := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		got = append(got, fmt.Sprintf("%v|%v|%v", row[0], row[1], row[2]))
	}
	require.Contains(t, got, "s1|en|one-en")
	require.Contains(t, got, "s1|es|one-es")
	require.Contains(t, got, "s2|en|two-en")
}
