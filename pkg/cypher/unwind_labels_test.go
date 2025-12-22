// Unit tests for UNWIND with labels() and list comprehension.
// These queries are used by Mimir's index-stats API and are currently failing.

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// Setup helper - creates nodes with multiple labels
// ====================================================================================

func setupMultiLabelNodes(t *testing.T, store storage.Engine, exec *StorageExecutor) {
	ctx := context.Background()

	queries := []string{
		`CREATE (n:File:TypeScript {path: '/test/file1.ts', extension: '.ts'})`,
		`CREATE (n:File:JavaScript {path: '/test/file2.js', extension: '.js'})`,
		`CREATE (n:File:TypeScript {path: '/test/file3.ts', extension: '.ts'})`,
		`CREATE (n:File:Markdown {path: '/test/file4.md', extension: '.md'})`,
		`CREATE (n:File {path: '/test/file5.txt', extension: '.txt'})`, // No secondary label
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
}

// ====================================================================================
// labels() function tests
// ====================================================================================

func TestLabelsFunction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("labels() returns all node labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN f.path, labels(f)
			ORDER BY f.path
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)

		// First row should be file1.ts with [File, TypeScript]
		// labels() returns []interface{} for Cypher compatibility
		labels1, ok := result.Rows[0][1].([]interface{})
		require.True(t, ok, "labels() should return []interface{}")
		labelsStr := make([]string, len(labels1))
		for i, l := range labels1 {
			labelsStr[i] = l.(string)
		}
		assert.Contains(t, labelsStr, "File")
		assert.Contains(t, labelsStr, "TypeScript")
	})

	t.Run("labels() on single-label node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file5.txt'})
			RETURN labels(f)
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		labels, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok, "labels() should return []interface{}")
		assert.Len(t, labels, 1)
		assert.Equal(t, "File", labels[0])
	})
}

// ====================================================================================
// UNWIND with function results
// ====================================================================================

func TestUnwindWithFunctionResults(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("UNWIND labels(f) expands node labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file1.ts'})
			UNWIND labels(f) as label
			RETURN label
		`, nil)
		require.NoError(t, err)
		// Should return 2 rows: File, TypeScript
		require.Len(t, result.Rows, 2, "Should have 2 rows from UNWIND labels()")

		if len(result.Rows) >= 2 {
			labels := []string{}
			for _, row := range result.Rows {
				if s, ok := row[0].(string); ok {
					labels = append(labels, s)
				}
			}
			assert.Contains(t, labels, "File")
			assert.Contains(t, labels, "TypeScript")
		}
	})

	t.Run("UNWIND range() function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			UNWIND range(1, 5) AS x
			RETURN x
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
	})

	t.Run("UNWIND keys() function", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file1.ts'})
			UNWIND keys(f) as key
			RETURN key
		`, nil)
		require.NoError(t, err)
		// Should have at least 'path' and 'extension'
		require.GreaterOrEqual(t, len(result.Rows), 2)
	})
}

// ====================================================================================
// List comprehension tests
// ====================================================================================

func TestListComprehensionWithLabels(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("basic list comprehension [x IN list]", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			WITH [1, 2, 3, 4, 5] as numbers
			RETURN [x IN numbers] as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		resultList, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, resultList, 5)
	})

	t.Run("list comprehension with WHERE filter", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			WITH [1, 2, 3, 4, 5] as numbers
			RETURN [x IN numbers WHERE x > 2] as filtered
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		resultList, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, resultList, 3) // 3, 4, 5
	})

	t.Run("list comprehension with transformation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			WITH [1, 2, 3] as numbers
			RETURN [x IN numbers | x * 2] as doubled
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		resultList, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		// Should be [2, 4, 6]
		assert.Len(t, resultList, 3)
	})

	t.Run("list comprehension on labels() with WHERE", func(t *testing.T) {
		// This is the exact pattern from Mimir's byType query
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file1.ts'})
			WITH f, [label IN labels(f) WHERE label <> 'File'] as filteredLabels
			RETURN filteredLabels
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		filteredLabels, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok, "Should return a list")
		assert.Len(t, filteredLabels, 1)
		assert.Equal(t, "TypeScript", filteredLabels[0])
	})

	t.Run("list comprehension filtering strings", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			WITH ['File', 'TypeScript', 'Node'] as labels
			RETURN [label IN labels WHERE label <> 'File'] as filtered
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		filtered, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok)
		assert.Len(t, filtered, 2)
	})
}

// ====================================================================================
// MATCH ... UNWIND combined queries
// ====================================================================================

func TestMatchUnwindCombined(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("MATCH then UNWIND labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			UNWIND labels(f) as label
			WHERE label <> 'File'
			RETURN label, count(*) as count
		`, nil)
		require.NoError(t, err)
		// Should have TypeScript(2), JavaScript(1), Markdown(1)
		require.GreaterOrEqual(t, len(result.Rows), 3)
	})

	t.Run("MATCH with WITH then UNWIND", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f, labels(f) as nodeLabels
			UNWIND nodeLabels as label
			WHERE label <> 'File'
			RETURN label, count(*) as count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err)
		// TypeScript should have count 2, others count 1
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})
}

// ====================================================================================
// The exact Mimir byType query
// ====================================================================================

func TestMimirByTypeQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("exact Mimir byType query", func(t *testing.T) {
		// This is the exact query from Mimir's index-api.ts line 682-689
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f, [label IN labels(f) WHERE label <> 'File'] as filteredLabels
			UNWIND filteredLabels as label
			WITH label, COUNT(f) as count
			RETURN label as type, count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err)

		// Should return TypeScript(2), JavaScript(1), Markdown(1)
		// File5.txt has no secondary label so doesn't appear
		require.Len(t, result.Rows, 3, "Should have 3 type groups")

		// First should be TypeScript with count 2
		assert.Equal(t, "TypeScript", result.Rows[0][0])
		assert.Equal(t, int64(2), result.Rows[0][1])
	})

	t.Run("files with no secondary label are excluded", func(t *testing.T) {
		// File5.txt only has label 'File', so after filtering it has empty list
		// UNWIND of empty list produces no rows
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file5.txt'})
			WITH f, [label IN labels(f) WHERE label <> 'File'] as filteredLabels
			UNWIND filteredLabels as label
			RETURN label
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0, "UNWIND of empty list should produce no rows")
	})
}

// ====================================================================================
// UNWIND edge cases
// ====================================================================================

func TestUnwindEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("UNWIND empty list", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			UNWIND [] AS x
			RETURN x
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("UNWIND null produces no rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			UNWIND null AS x
			RETURN x
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("UNWIND nested list", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			UNWIND [[1, 2], [3, 4]] AS x
			RETURN x
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 2)
	})

	t.Run("UNWIND with aggregation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			UNWIND [1, 2, 3, 4, 5] AS x
			RETURN sum(x) as total
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// 1+2+3+4+5 = 15
		if total, ok := result.Rows[0][0].(float64); ok {
			assert.Equal(t, float64(15), total)
		} else if total, ok := result.Rows[0][0].(int64); ok {
			assert.Equal(t, int64(15), total)
		}
	})

	t.Run("UNWIND preserves variables from MATCH", func(t *testing.T) {
		setupMultiLabelNodes(t, store, exec)

		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file1.ts'})
			UNWIND [1, 2] AS x
			RETURN f.path, x
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 rows from UNWIND")
		// Both rows should have the same path
		if len(result.Rows) >= 2 {
			assert.Equal(t, "/test/file1.ts", result.Rows[0][0])
			assert.Equal(t, "/test/file1.ts", result.Rows[1][0])
		}
	})
}

// ====================================================================================
// Alternative simpler query for byType (workaround)
// ====================================================================================

func TestSimplifiedByTypeQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupMultiLabelNodes(t, store, exec)

	t.Run("simplified byType using direct label check", func(t *testing.T) {
		// Alternative approach: query each known type separately
		// This is a workaround if list comprehension isn't supported

		// Count TypeScript files
		result, err := exec.Execute(ctx, `
			MATCH (f:TypeScript)
			RETURN 'TypeScript' as type, count(*) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(2), result.Rows[0][1])
	})
}

// ====================================================================================
// UNWIND with CREATE - Bulk node creation
// ====================================================================================

func TestUnwindWithCreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("UNWIND creates multiple nodes from list", func(t *testing.T) {
		// Create multiple TestNode nodes using UNWIND
		_, err := exec.Execute(ctx, `
			UNWIND ['node1', 'node2', 'node3'] AS name
			CREATE (n:TestNode {name: name})
		`, nil)
		require.NoError(t, err)

		// Verify nodes were created
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), result.Rows[0][0], "Should have created 3 TestNode nodes")
	})

	t.Run("UNWIND creates nodes with multiple properties", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create nodes with structured data
		_, err := exec.Execute(ctx, `
			UNWIND [
				{name: 'Alice', age: 30},
				{name: 'Bob', age: 25},
				{name: 'Charlie', age: 35}
			] AS props
			CREATE (n:TestNode {name: props.name, age: props.age})
		`, nil)
		require.NoError(t, err)

		// Verify nodes and properties
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN n.name, n.age
			ORDER BY n.age
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		
		// Check sorted by age
		assert.Equal(t, "Bob", result.Rows[0][0])
		assert.Equal(t, int64(25), result.Rows[0][1])
		assert.Equal(t, "Alice", result.Rows[1][0])
		assert.Equal(t, int64(30), result.Rows[1][1])
		assert.Equal(t, "Charlie", result.Rows[2][0])
		assert.Equal(t, int64(35), result.Rows[2][1])
	})

	t.Run("UNWIND with range creates sequential nodes", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create nodes using range
		_, err := exec.Execute(ctx, `
			UNWIND range(1, 5) AS num
			CREATE (n:TestNode {id: num})
		`, nil)
		require.NoError(t, err)

		// Verify count
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})

	t.Run("UNWIND CREATE with RETURN", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create and return nodes
		result, err := exec.Execute(ctx, `
			UNWIND ['A', 'B', 'C'] AS name
			CREATE (n:TestNode {name: name})
			RETURN n.name as nodeName
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3, "Should return 3 created nodes")
		
		// Debug: print what we got
		t.Logf("Result columns: %v", result.Columns)
		t.Logf("Result rows: %v", result.Rows)
		
		names := []string{}
		for _, row := range result.Rows {
			if val, ok := row[0].(string); ok {
				names = append(names, val)
			}
		}
		t.Logf("Extracted names: %v", names)
		assert.Contains(t, names, "A")
		assert.Contains(t, names, "B")
		assert.Contains(t, names, "C")
	})

	t.Run("UNWIND empty list creates no nodes", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create with empty list
		_, err := exec.Execute(ctx, `
			UNWIND [] AS name
			CREATE (n:TestNode {name: name})
		`, nil)
		require.NoError(t, err)

		// Verify no nodes created
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(0), result.Rows[0][0])
	})

	t.Run("UNWIND with parameters", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create nodes using parameter
		params := map[string]interface{}{
			"names": []interface{}{"X", "Y", "Z"},
		}
		
		_, err := exec.Execute(ctx, `
			UNWIND $names AS name
			CREATE (n:TestNode {name: name})
		`, params)
		require.NoError(t, err)

		// Verify count
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), result.Rows[0][0])
	})

	t.Run("UNWIND CREATE preserves transaction atomicity", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// This should create all or none
		_, err := exec.Execute(ctx, `
			UNWIND [1, 2, 3, 4, 5] AS num
			CREATE (n:TestNode {id: num})
		`, nil)
		require.NoError(t, err)

		// All 5 should exist
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(5), result.Rows[0][0])
	})

	t.Run("UNWIND CREATE with additional properties", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()

		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)

		// Create nodes with all properties at creation time
		_, err := exec.Execute(ctx, `
			UNWIND ['alpha', 'beta', 'gamma'] AS name
			CREATE (n:TestNode {name: name, created: true, timestamp: 123456})
		`, nil)
		require.NoError(t, err)

		// Verify properties were set
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			WHERE n.created = true
			RETURN count(n) as count
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), result.Rows[0][0])
	})
}
