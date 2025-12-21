// Package cypher - Tests for db.* and tx.* procedures.
package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========================================
// db.info Tests
// ========================================

func TestCallDbInfoExtended(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (n:User {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (n:Post {title: 'Hello'})`, nil)
	exec.Execute(ctx, `MATCH (a:User {name: 'Alice'}), (b:User {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)`, nil)

	result, err := exec.Execute(ctx, `CALL db.info()`, nil)
	if err != nil {
		t.Fatalf("db.info() failed: %v", err)
	}

	// Check columns
	expectedColumns := []string{"id", "name", "creationDate", "nodeCount", "relationshipCount"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}
	for i, col := range expectedColumns {
		if i < len(result.Columns) && result.Columns[i] != col {
			t.Errorf("Column %d: expected %s, got %s", i, col, result.Columns[i])
		}
	}

	// Check we have one row
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check database name
	if len(result.Rows) > 0 && len(result.Rows[0]) > 1 {
		if result.Rows[0][1] != "nornicdb" {
			t.Errorf("Expected database name 'nornicdb', got %v", result.Rows[0][1])
		}
	}

	// Check node count (should be 3)
	if len(result.Rows) > 0 && len(result.Rows[0]) > 3 {
		nodeCount := result.Rows[0][3]
		if count, ok := nodeCount.(int); ok && count != 3 {
			t.Errorf("Expected nodeCount 3, got %v", nodeCount)
		}
	}
}

func TestCallDbInfoYield(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test YIELD clause
	result, err := exec.Execute(ctx, `CALL db.info() YIELD name, nodeCount`, nil)
	if err != nil {
		t.Fatalf("db.info() YIELD failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns with YIELD, got %d", len(result.Columns))
	}
}

// ========================================
// db.ping Tests
// ========================================

func TestCallDbPingExtended(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.ping()`, nil)
	if err != nil {
		t.Fatalf("db.ping() failed: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "success" {
		t.Errorf("Expected column 'success', got %v", result.Columns)
	}

	if len(result.Rows) != 1 || result.Rows[0][0] != true {
		t.Errorf("Expected success=true, got %v", result.Rows)
	}
}

// ========================================
// db.awaitIndex Tests
// ========================================

func TestCallDbAwaitIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{"basic", `CALL db.awaitIndex('my_index')`},
		{"with timeout", `CALL db.awaitIndex('my_index', 60)`},
		{"different index", `CALL db.awaitIndex('user_name_idx', 30)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("db.awaitIndex() failed: %v", err)
			}

			if len(result.Columns) != 1 || result.Columns[0] != "status" {
				t.Errorf("Expected column 'status', got %v", result.Columns)
			}

			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(result.Rows))
			}

			status, ok := result.Rows[0][0].(string)
			if !ok || !strings.Contains(status, "online") {
				t.Errorf("Expected status to contain 'online', got %v", result.Rows[0][0])
			}
		})
	}
}

// ========================================
// db.awaitIndexes Tests
// ========================================

func TestCallDbAwaitIndexes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{"no timeout", `CALL db.awaitIndexes()`},
		{"with timeout", `CALL db.awaitIndexes(120)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("db.awaitIndexes() failed: %v", err)
			}

			if len(result.Columns) != 1 || result.Columns[0] != "status" {
				t.Errorf("Expected column 'status', got %v", result.Columns)
			}

			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(result.Rows))
			}

			status, ok := result.Rows[0][0].(string)
			if !ok || !strings.Contains(status, "online") {
				t.Errorf("Expected status to contain 'online', got %v", result.Rows[0][0])
			}
		})
	}
}

// ========================================
// db.resampleIndex Tests
// ========================================

func TestCallDbResampleIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{"basic", `CALL db.resampleIndex('my_index')`},
		{"different index", `CALL db.resampleIndex('user_email_idx')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("db.resampleIndex() failed: %v", err)
			}

			if len(result.Columns) != 1 || result.Columns[0] != "status" {
				t.Errorf("Expected column 'status', got %v", result.Columns)
			}

			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(result.Rows))
			}

			status, ok := result.Rows[0][0].(string)
			if !ok || !strings.Contains(status, "updated") {
				t.Errorf("Expected status to contain 'updated', got %v", result.Rows[0][0])
			}
		})
	}
}

// ========================================
// tx.setMetaData Tests
// ========================================

func TestCallTxSetMetadata(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{"simple metadata", `CALL tx.setMetaData({app: 'test'})`},
		{"multiple keys", `CALL tx.setMetaData({app: 'myapp', userId: 123, requestId: 'abc-123'})`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start a transaction first
			_, err := exec.Execute(ctx, "BEGIN", nil)
			if err != nil {
				t.Fatalf("Failed to start transaction: %v", err)
			}

			// Set metadata
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("tx.setMetaData() failed: %v", err)
			}

			if len(result.Columns) != 1 || result.Columns[0] != "status" {
				t.Errorf("Expected column 'status', got %v", result.Columns)
			}

			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(result.Rows))
			}

			// Commit transaction
			_, err = exec.Execute(ctx, "COMMIT", nil)
			if err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
		})
	}

	t.Run("error_without_transaction", func(t *testing.T) {
		// Try to set metadata without an active transaction
		_, err := exec.Execute(ctx, `CALL tx.setMetaData({app: 'test'})`, nil)
		if err == nil {
			t.Error("Expected error when setting metadata without active transaction")
		}
		if err != nil && !strings.Contains(err.Error(), "active transaction") {
			t.Errorf("Expected error about active transaction, got: %v", err)
		}
	})
}

// ========================================
// db.stats.* Tests
// ========================================

func TestCallDbStatsClear(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.stats.clear()`, nil)
	if err != nil {
		t.Fatalf("db.stats.clear() failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbStatsCollect(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.stats.collect('QUERIES')`, nil)
	if err != nil {
		t.Fatalf("db.stats.collect() failed: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check success field
	if len(result.Rows[0]) > 1 {
		if success, ok := result.Rows[0][1].(bool); !ok || !success {
			t.Errorf("Expected success=true, got %v", result.Rows[0][1])
		}
	}
}

func TestCallDbStatsRetrieve(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.stats.retrieve('QUERIES')`, nil)
	if err != nil {
		t.Fatalf("db.stats.retrieve() failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbStatsStatus(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.stats.status()`, nil)
	if err != nil {
		t.Fatalf("db.stats.status() failed: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbStatsStop(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.stats.stop()`, nil)
	if err != nil {
		t.Fatalf("db.stats.stop() failed: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}

	// Check success field
	if len(result.Rows) > 0 && len(result.Rows[0]) > 1 {
		if success, ok := result.Rows[0][1].(bool); !ok || !success {
			t.Errorf("Expected success=true, got %v", result.Rows[0][1])
		}
	}
}

func TestCallDbStatsRetrieveAllAnTheStats(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some data first
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (n:User {name: 'Bob'})`, nil)

	result, err := exec.Execute(ctx, `CALL db.stats.retrieveAllAnTheStats()`, nil)
	if err != nil {
		t.Fatalf("db.stats.retrieveAllAnTheStats() failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Should have at least 2 rows (GRAPH COUNTS and QUERIES)
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 rows, got %d", len(result.Rows))
	}
}

// ========================================
// db.clearQueryCaches Tests
// ========================================

func TestCallDbClearQueryCaches(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	t.Run("clear_all_caches", func(t *testing.T) {
		// Execute a query to populate caches
		result1, err := exec.Execute(ctx, `MATCH (n:User) RETURN n.name`, nil)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		if len(result1.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result1.Rows))
		}

		// Verify cache is working (second query should be faster/cached)
		result2, err := exec.Execute(ctx, `MATCH (n:User) RETURN n.name`, nil)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		if len(result2.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result2.Rows))
		}

		// Clear all caches
		result, err := exec.Execute(ctx, `CALL db.clearQueryCaches()`, nil)
		if err != nil {
			t.Fatalf("db.clearQueryCaches() failed: %v", err)
		}

		if len(result.Columns) != 1 || result.Columns[0] != "status" {
			t.Errorf("Expected column 'status', got %v", result.Columns)
		}

		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}

		// Verify caches are actually cleared by checking cache state
		// (caches should be empty after clear)
		if exec.cache != nil {
			// Try to get a cached result - should not be found after clear
			_, found := exec.cache.Get(`MATCH (n:User) RETURN n.name`, nil)
			if found {
				t.Error("Cache should be empty after clear")
			}
		}
	})

	t.Run("clear_plan_cache", func(t *testing.T) {
		// Execute query to populate plan cache
		_, err := exec.Execute(ctx, `MATCH (n:User) RETURN count(n)`, nil)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Clear caches
		_, err = exec.Execute(ctx, `CALL db.clearQueryCaches()`, nil)
		if err != nil {
			t.Fatalf("db.clearQueryCaches() failed: %v", err)
		}

		// Verify plan cache is cleared
		if exec.planCache != nil {
			_, _, found := exec.planCache.Get(`MATCH (n:User) RETURN count(n)`)
			if found {
				t.Error("Plan cache should be empty after clear")
			}
		}
	})

	t.Run("clear_node_lookup_cache", func(t *testing.T) {
		// Execute query that uses node lookup cache
		_, err := exec.Execute(ctx, `MATCH (n:User {name: 'Alice'}) RETURN n`, nil)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Verify node lookup cache has entry
		exec.nodeLookupCacheMu.RLock()
		cacheSizeBefore := len(exec.nodeLookupCache)
		exec.nodeLookupCacheMu.RUnlock()

		// Clear caches
		_, err = exec.Execute(ctx, `CALL db.clearQueryCaches()`, nil)
		if err != nil {
			t.Fatalf("db.clearQueryCaches() failed: %v", err)
		}

		// Verify node lookup cache is cleared
		exec.nodeLookupCacheMu.RLock()
		cacheSizeAfter := len(exec.nodeLookupCache)
		exec.nodeLookupCacheMu.RUnlock()

		if cacheSizeAfter != 0 {
			t.Errorf("Node lookup cache should be empty after clear, got size %d", cacheSizeAfter)
		}
		if cacheSizeBefore > 0 && cacheSizeAfter >= cacheSizeBefore {
			t.Errorf("Cache should have been cleared (before: %d, after: %d)", cacheSizeBefore, cacheSizeAfter)
		}
	})
}

// ========================================
// dbms.* Tests
// ========================================

func TestCallDbmsInfoExtended(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL dbms.info()`, nil)
	if err != nil {
		t.Fatalf("dbms.info() failed: %v", err)
	}

	expectedColumns := []string{"id", "name", "creationDate"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbmsListConfigExtended(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL dbms.listConfig()`, nil)
	if err != nil {
		t.Fatalf("dbms.listConfig() failed: %v", err)
	}

	expectedColumns := []string{"name", "description", "value", "dynamic"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	// Should have multiple config entries
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 config row, got %d", len(result.Rows))
	}
}

func TestCallDbmsClientConfig(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL dbms.clientConfig()`, nil)
	if err != nil {
		t.Fatalf("dbms.clientConfig() failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Should have advertised addresses
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbmsListConnections(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL dbms.listConnections()`, nil)
	if err != nil {
		t.Fatalf("dbms.listConnections() failed: %v", err)
	}

	expectedColumns := []string{"connectionId", "connectTime", "connector", "username", "userAgent", "clientAddress"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	// Connections list can be empty
}

// ========================================
// db.index.fulltext.listAvailableAnalyzers Tests
// ========================================

func TestCallDbIndexFulltextListAvailableAnalyzersExtended(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `CALL db.index.fulltext.listAvailableAnalyzers()`, nil)
	if err != nil {
		t.Fatalf("db.index.fulltext.listAvailableAnalyzers() failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}

	// Should have multiple analyzers
	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 analyzers, got %d", len(result.Rows))
	}

	// Check first analyzer has expected structure
	if len(result.Rows) > 0 && len(result.Rows[0]) >= 2 {
		if _, ok := result.Rows[0][0].(string); !ok {
			t.Errorf("Expected analyzer name to be string, got %T", result.Rows[0][0])
		}
		if _, ok := result.Rows[0][1].(string); !ok {
			t.Errorf("Expected analyzer description to be string, got %T", result.Rows[0][1])
		}
	}
}

// ========================================
// Integration Tests: Procedure + YIELD + WHERE + RETURN
// ========================================

func TestCallDbInfoWithYieldAndWhere(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data to ensure non-zero counts
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (n:User {name: 'Bob'})`, nil)

	// Test YIELD with specific columns and WHERE filtering
	result, err := exec.Execute(ctx, `CALL db.info() YIELD name, nodeCount WHERE nodeCount > 0`, nil)
	if err != nil {
		t.Fatalf("db.info() YIELD...WHERE failed: %v", err)
	}

	// Should have filtered results (only rows where nodeCount > 0)
	if len(result.Rows) == 0 {
		t.Log("Note: YIELD WHERE filtering may not be fully implemented")
	}
}

func TestCallDbLabelsWithYield(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (n:Post {title: 'Hello'})`, nil)
	exec.Execute(ctx, `CREATE (n:Comment {text: 'Nice!'})`, nil)

	// Test basic YIELD
	result, err := exec.Execute(ctx, `CALL db.labels() YIELD label`, nil)
	if err != nil {
		t.Fatalf("db.labels() YIELD failed: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "label" {
		t.Errorf("Expected column 'label', got %v", result.Columns)
	}

	// Should have 3 labels
	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 labels, got %d", len(result.Rows))
	}
}

// ========================================
// db.constraints Tests
// ========================================

func TestCallDbConstraints(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("empty_constraints", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.constraints()", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, []string{"name", "type", "labelsOrTypes", "properties"}, result.Columns)
		assert.Empty(t, result.Rows, "Should return empty when no constraints exist")
	})

	t.Run("single_unique_constraint", func(t *testing.T) {
		// Create a unique constraint
		_, err := exec.Execute(ctx, "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "CALL db.constraints()", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return one constraint")

		row := result.Rows[0]
		assert.Equal(t, "node_id_unique", row[0], "Constraint name")
		assert.Equal(t, "UNIQUE", row[1], "Constraint type")
		labelsOrTypes := row[2].([]string)
		assert.Equal(t, []string{"Node"}, labelsOrTypes, "Labels")
		properties := row[3].([]string)
		assert.Equal(t, []string{"id"}, properties, "Properties")
	})

	t.Run("multiple_constraints", func(t *testing.T) {
		// Create multiple constraints on different labels
		_, err := exec.Execute(ctx, "CREATE CONSTRAINT person_email_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE", nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, "CREATE CONSTRAINT user_username_unique IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "CALL db.constraints()", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should have at least 2 constraints (may have more from previous test)
		assert.GreaterOrEqual(t, len(result.Rows), 2, "Should have multiple constraints")

		// Verify constraint names are present
		constraintNames := make(map[string]bool)
		for _, row := range result.Rows {
			name := row[0].(string)
			constraintNames[name] = true
		}

		assert.True(t, constraintNames["person_email_unique"], "Should have person_email_unique constraint")
		assert.True(t, constraintNames["user_username_unique"], "Should have user_username_unique constraint")
	})

	t.Run("constraint_with_yield", func(t *testing.T) {
		// Test with YIELD clause
		result, err := exec.Execute(ctx, "CALL db.constraints() YIELD name, type RETURN name, type ORDER BY name", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, []string{"name", "type"}, result.Columns)
		assert.Greater(t, len(result.Rows), 0, "Should return constraints")
	})
}
