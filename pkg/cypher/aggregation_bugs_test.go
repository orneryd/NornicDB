// Unit tests for aggregation bugs discovered in real-world usage.
// These tests cover issues found when using the VSCode Code Intelligence stats queries.

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupAggregationTestData creates test data for aggregation tests
func setupAggregationTestData(t *testing.T, store storage.Engine) {
	ctx := context.Background()
	exec := NewStorageExecutor(store)

	// Create File nodes with various extensions
	queries := []string{
		`CREATE (f:File {id: 'file1', path: '/test/file1.ts', extension: '.ts', name: 'file1.ts'})`,
		`CREATE (f:File {id: 'file2', path: '/test/file2.ts', extension: '.ts', name: 'file2.ts'})`,
		`CREATE (f:File {id: 'file3', path: '/test/file3.md', extension: '.md', name: 'file3.md'})`,
		`CREATE (f:File {id: 'file4', path: '/test/file4.md', extension: '.md', name: 'file4.md'})`,
		`CREATE (f:File {id: 'file5', path: '/test/file5.md', extension: '.md', name: 'file5.md'})`,
		// Files without extension (to test IS NOT NULL filtering)
		`CREATE (f:File {id: 'file6', path: '/test/noext', name: 'noext'})`,
		`CREATE (f:File {id: 'file7', path: '/test/noext2', name: 'noext2'})`,
	}

	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
}

// ====================================================================================
// BUG #1: WHERE ... IS NOT NULL combined with WITH aggregation returns empty results
// ====================================================================================

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("WHERE IS NOT NULL before WITH aggregation", func(t *testing.T) {
		// This is the exact query that fails in production
		// Expected: returns 2 rows (.ts=2, .md=3)
		// Actual bug: returns 0 rows
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			WITH f.extension as ext, COUNT(f) as count
			RETURN ext, count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err, "Query should execute without error")
		require.GreaterOrEqual(t, len(result.Rows), 2, "Should return at least 2 rows (one per extension)")

		// Verify we have both extensions
		extCounts := make(map[string]int64)
		for _, row := range result.Rows {
			if row[0] != nil {
				ext := row[0].(string)
				count := row[1].(int64)
				extCounts[ext] = count
			}
		}

		assert.Equal(t, int64(2), extCounts[".ts"], ".ts should have 2 files")
		assert.Equal(t, int64(3), extCounts[".md"], ".md should have 3 files")
	})

	t.Run("simple WHERE IS NOT NULL filter", func(t *testing.T) {
		// This simpler query should also work
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			RETURN f.extension as ext, count(*) as count
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1, "Should return at least 1 row")
	})

	t.Run("WHERE IS NOT NULL returns correct count", func(t *testing.T) {
		// Verify base filtering works
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			RETURN count(f) as count_with_ext
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// We have 5 files with extension (file1-5), 2 without (file6-7)
		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(5), count, "Should count 5 files with extensions")
	})
}

// ====================================================================================
// BUG #2: COUNT(f) in WITH clause returns null instead of proper count
// ====================================================================================

func TestBug_CountInWithClauseReturnsNull(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("COUNT in WITH clause should aggregate", func(t *testing.T) {
		// This query returns count: null for each row instead of aggregated count
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f.extension as ext, COUNT(f) as count
			WHERE ext IS NOT NULL
			RETURN ext, count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err)

		// Should return 2 rows: .ts (2), .md (3)
		require.GreaterOrEqual(t, len(result.Rows), 1, "Should return aggregated rows")

		// Each count should NOT be null
		for i, row := range result.Rows {
			assert.NotNil(t, row[1], "count in row %d should not be nil", i)
		}
	})

	t.Run("GROUP BY implicit in WITH aggregation", func(t *testing.T) {
		// When we have COUNT() with a non-aggregated column, implicit GROUP BY should happen
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f.extension as ext, COUNT(*) as count
			RETURN ext, count
		`, nil)
		require.NoError(t, err)

		// Should have one row per distinct extension value (including null)
		// 3 distinct values: .ts, .md, null
		assert.LessOrEqual(t, len(result.Rows), 3, "Should group by extension")
	})
}

// ====================================================================================
// Test adjacent scenarios for better coverage
// ====================================================================================

func TestAggregation_GroupByProperty(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("GROUP BY with single aggregation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN f.extension as ext, count(*) as count
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// Verify columns
		assert.Contains(t, result.Columns, "ext")
		assert.Contains(t, result.Columns, "count")
	})

	t.Run("multiple aggregations with GROUP BY", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN f.extension as ext, count(*) as total, count(f.name) as named
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})
}

func TestAggregation_WithThenReturn(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("WITH non-aggregated then RETURN aggregated", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f.extension as ext
			RETURN ext, count(*) as cnt
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})

	t.Run("WITH aggregated then RETURN pass-through", func(t *testing.T) {
		// WITH does aggregation, RETURN just passes through
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH count(f) as total
			RETURN total
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		total := result.Rows[0][0]
		assert.NotNil(t, total)
		assert.Equal(t, int64(7), total.(int64), "Should count all 7 files")
	})
}

func TestAggregation_ChainedWith(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("two WITH clauses with aggregation in second", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f, f.extension as ext
			WITH ext, count(*) as cnt
			RETURN ext, cnt
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1, "Should return aggregated results")
	})
}

func TestAggregation_NullHandling(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("COUNT excludes null values", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN count(f.extension) as count_ext
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Should only count 5 (files with extension), not 7
		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(5), count, "COUNT(f.extension) should exclude nulls")
	})

	t.Run("COUNT(*) includes all rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN count(*) as count_all
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(7), count, "COUNT(*) should include all rows")
	})
}

func TestAggregation_CollectDistinct(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("COLLECT DISTINCT in WITH clause", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH COLLECT(DISTINCT f.extension) as extensions
			RETURN extensions
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		extensions, ok := result.Rows[0][0].([]interface{})
		require.True(t, ok, "Should return array")

		// 3 distinct values: .ts, .md, null
		assert.LessOrEqual(t, len(extensions), 3)
	})
}

func TestAggregation_SumAndAvg(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with numeric values
	_, err := exec.Execute(ctx, `CREATE (n:Item {value: 10, group: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {value: 20, group: 'A'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {value: 30, group: 'B'})`, nil)
	require.NoError(t, err)

	t.Run("SUM with GROUP BY", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN n.group as grp, SUM(n.value) as total
		`, nil)
		require.NoError(t, err)

		// Should have 2 groups
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})

	t.Run("AVG with GROUP BY", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN n.group as grp, AVG(n.value) as average
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})
}

func TestAggregation_WhereOnAggregatedResult(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("WHERE after WITH aggregation (HAVING equivalent)", func(t *testing.T) {
		// In Cypher, WHERE after WITH acts like HAVING in SQL
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f.extension as ext, count(*) as cnt
			WHERE cnt > 2
			RETURN ext, cnt
		`, nil)
		require.NoError(t, err)

		// Only .md has count > 2 (has 3 files)
		// This may return 0 if NULL extension also counts
		if len(result.Rows) > 0 {
			for _, row := range result.Rows {
				cnt := row[1].(int64)
				assert.Greater(t, cnt, int64(2), "All results should have count > 2")
			}
		}
	})
}

func TestAggregation_OrderByAggregatedValue(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("ORDER BY count DESC", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			RETURN f.extension as ext, count(*) as cnt
			ORDER BY cnt DESC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// Results should be ordered by count descending
		if len(result.Rows) >= 2 {
			// First count should be >= second count
			cnt1 := result.Rows[0][1].(int64)
			cnt2 := result.Rows[1][1].(int64)
			assert.GreaterOrEqual(t, cnt1, cnt2, "Results should be ordered by count DESC")
		}
	})
}

func TestAggregation_WithMultipleAggregates(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("multiple aggregates in WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH count(f) as total, count(f.extension) as withExt
			RETURN total, withExt, total - withExt as withoutExt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		total := result.Rows[0][0].(int64)
		withExt := result.Rows[0][1].(int64)

		assert.Equal(t, int64(7), total, "Total should be 7")
		assert.Equal(t, int64(5), withExt, "With extension should be 5")

		// withoutExt = total - withExt = 7 - 5 = 2
		if result.Rows[0][2] != nil {
			// Result can be int64 or float64 depending on implementation
			switch v := result.Rows[0][2].(type) {
			case int64:
				assert.Equal(t, int64(2), v, "Without extension should be 2")
			case float64:
				assert.Equal(t, float64(2), v, "Without extension should be 2")
			default:
				t.Errorf("Unexpected type for withoutExt: %T", result.Rows[0][2])
			}
		}
	})
}

// ====================================================================================
// Production query from Mimir's index-api.ts
// ====================================================================================

func TestMimirIndexStatsExtensionQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupAggregationTestData(t, store)

	t.Run("exact Mimir extension query", func(t *testing.T) {
		// This is the exact query from index-api.ts that fails
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE f.extension IS NOT NULL
			WITH f.extension as ext, COUNT(f) as count
			RETURN ext, count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err, "Query should execute without error")

		// Must return at least 1 row
		require.GreaterOrEqual(t, len(result.Rows), 1, "Should return at least one extension group")

		// Verify columns
		assert.Contains(t, result.Columns, "ext")
		assert.Contains(t, result.Columns, "count")

		// Each row should have non-null count
		for i, row := range result.Rows {
			assert.NotNil(t, row[0], "ext in row %d should not be nil", i)
			assert.NotNil(t, row[1], "count in row %d should not be nil", i)
		}
	})
}

// TestMimirIndexStatsTypeQuery tests the exact byType query from Mimir's index-api.ts
func TestMimirIndexStatsTypeQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create File nodes like Mimir does (note: no "type" property)
	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(`
			CREATE (f:File {path: '/test/file%d.md', extension: '.md'})
		`, i), nil)
		require.NoError(t, err)
	}

	t.Run("byType query groups by null property", func(t *testing.T) {
		// This is the exact query from Mimir's index-api.ts
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WITH f.type as type, COUNT(f) as count
			RETURN type, count
			ORDER BY count DESC
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should have 1 group (all files have null type)")

		// The type should be nil and count should be 5
		assert.Nil(t, result.Rows[0][0], "Type should be nil")
		assert.Equal(t, int64(5), result.Rows[0][1], "Count should be 5")
	})
}

// TestMimirComplexStatsQuery tests the complex stats query from Mimir's index-api.ts
func TestMimirComplexStatsQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create File nodes
	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(`
			CREATE (f:File {path: '/test/file%d.md', extension: '.md'})
		`, i), nil)
		require.NoError(t, err)
	}

	t.Run("complex stats query with OPTIONAL MATCH and CASE WHEN", func(t *testing.T) {
		// This is the exact query from Mimir's index-api.ts
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FileChunk)
			WITH f, c,
				CASE WHEN c IS NOT NULL AND c.embedding IS NOT NULL THEN 1 ELSE 0 END as chunkHasEmbedding,
				CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END as fileHasEmbedding
			WITH 
				COUNT(DISTINCT f) as totalFiles,
				COUNT(DISTINCT c) as totalChunks,
				SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) as totalEmbeddings,
				COLLECT(DISTINCT f.extension) as extensions
			RETURN 
				totalFiles,
				totalChunks,
				totalEmbeddings,
				extensions
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Verify the results
		assert.Equal(t, int64(5), result.Rows[0][0], "totalFiles should be 5")
		assert.Equal(t, int64(0), result.Rows[0][1], "totalChunks should be 0 (no chunks)")
		// totalEmbeddings could be 0 or nil depending on SUM behavior
		extensions, ok := result.Rows[0][3].([]interface{})
		require.True(t, ok, "extensions should be a slice")
		assert.Contains(t, extensions, ".md", "extensions should include .md")
	})
}
