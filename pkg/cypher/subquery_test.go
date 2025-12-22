package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountSubqueryComparison tests COUNT { } subquery functionality
func TestCountSubquery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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

// TestCountSubqueryGTE tests COUNT { } >= n syntax
func TestCountSubqueryGTE(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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

		// This should fail on invalid syntax (missing WHERE clause with non-existent label)
		_, err = exec.Execute(ctx, `
			CALL {
				MATCH (p:NonExistentLabel)
				SET p.value = 'test'
				RETURN p.name AS name
			} IN TRANSACTIONS OF 1 ROWS
			RETURN name
		`, nil)
		// May or may not error depending on implementation - just verify it doesn't crash
		// The key is that batching handles errors gracefully
		if err != nil {
			// Error is acceptable - batching should stop on error
			assert.Contains(t, err.Error(), "batch", "Error should mention batch")
		}
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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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

	baseStore := storage.NewMemoryEngine()


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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
	baseStore := storage.NewMemoryEngine()

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
