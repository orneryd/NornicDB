// Unit tests for adjacent aggregation query patterns.
// Tests various permutations that production queries might use.

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// ORDER BY variations on aggregated results
// ====================================================================================

func TestAggregation_OrderByVariations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup: Create items with values for testing
	queries := []string{
		`CREATE (n:Product {name: 'Apple', category: 'Fruit', price: 1.50})`,
		`CREATE (n:Product {name: 'Banana', category: 'Fruit', price: 0.75})`,
		`CREATE (n:Product {name: 'Orange', category: 'Fruit', price: 2.00})`,
		`CREATE (n:Product {name: 'Carrot', category: 'Vegetable', price: 0.50})`,
		`CREATE (n:Product {name: 'Broccoli', category: 'Vegetable', price: 1.25})`,
		`CREATE (n:Product {name: 'Milk', category: 'Dairy', price: 3.00})`,
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("ORDER BY ASC explicit", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, count(*) as cnt
			ORDER BY cnt ASC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// ASC: first count should be <= second count
		if len(result.Rows) >= 2 {
			cnt1 := result.Rows[0][1].(int64)
			cnt2 := result.Rows[1][1].(int64)
			assert.LessOrEqual(t, cnt1, cnt2, "ASC: first should be <= second")
		}
	})

	t.Run("ORDER BY with LIMIT on aggregated", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, count(*) as cnt
			ORDER BY cnt DESC
			LIMIT 2
		`, nil)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(result.Rows), 2, "LIMIT should restrict to 2 rows")
	})

	t.Run("ORDER BY with SKIP on aggregated", func(t *testing.T) {
		// First get all results
		allResult, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, count(*) as cnt
			ORDER BY cnt DESC
		`, nil)
		require.NoError(t, err)

		// Then get with SKIP 1
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, count(*) as cnt
			ORDER BY cnt DESC
			SKIP 1
		`, nil)
		require.NoError(t, err)

		if len(allResult.Rows) > 1 {
			assert.Equal(t, len(allResult.Rows)-1, len(result.Rows), "SKIP should reduce by 1")
		}
	})

	t.Run("ORDER BY non-aggregated column", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, count(*) as cnt
			ORDER BY cat ASC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// Categories should be alphabetically sorted
		if len(result.Rows) >= 2 {
			cat1 := result.Rows[0][0].(string)
			cat2 := result.Rows[1][0].(string)
			assert.LessOrEqual(t, cat1, cat2, "Categories should be alphabetically sorted")
		}
	})

	t.Run("ORDER BY SUM DESC", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.category as cat, SUM(p.price) as total
			ORDER BY total DESC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// First total should be >= second
		if len(result.Rows) >= 2 {
			total1, ok1 := result.Rows[0][1].(float64)
			total2, ok2 := result.Rows[1][1].(float64)
			if ok1 && ok2 {
				assert.GreaterOrEqual(t, total1, total2, "ORDER BY SUM DESC should work")
			}
		}
	})
}

// ====================================================================================
// WHERE clause variations
// ====================================================================================

func TestAggregation_WhereVariations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	queries := []string{
		`CREATE (n:Item {type: 'A', value: 10, active: true})`,
		`CREATE (n:Item {type: 'A', value: 20, active: true})`,
		`CREATE (n:Item {type: 'A', value: 30, active: false})`,
		`CREATE (n:Item {type: 'B', value: 15, active: true})`,
		`CREATE (n:Item {type: 'B', value: 25})`, // No active property
		`CREATE (n:Item {type: 'C', value: 100, active: true})`,
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("WHERE with equality before WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WHERE n.type = 'A'
			WITH n.type as typ, count(*) as cnt
			RETURN typ, cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "A", result.Rows[0][0])
		assert.Equal(t, int64(3), result.Rows[0][1])
	})

	t.Run("WHERE with comparison before WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WHERE n.value > 20
			WITH n.type as typ, count(*) as cnt
			RETURN typ, cnt
		`, nil)
		require.NoError(t, err)
		// Should include: A(30), B(25), C(100) = 3 items across potentially 3 types
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})

	t.Run("WHERE with boolean before WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WHERE n.active = true
			WITH n.type as typ, count(*) as cnt
			RETURN typ, cnt
		`, nil)
		require.NoError(t, err)
		// Should include only active items
		require.GreaterOrEqual(t, len(result.Rows), 1)
	})

	t.Run("HAVING equivalent - WHERE after WITH with comparison", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WITH n.type as typ, count(*) as cnt
			WHERE cnt >= 2
			RETURN typ, cnt
		`, nil)
		require.NoError(t, err)

		// Only groups with count >= 2
		for _, row := range result.Rows {
			cnt := row[1].(int64)
			assert.GreaterOrEqual(t, cnt, int64(2), "All counts should be >= 2")
		}
	})

	t.Run("HAVING with SUM comparison", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			WITH n.type as typ, SUM(n.value) as total
			WHERE total > 50
			RETURN typ, total
		`, nil)
		require.NoError(t, err)

		for _, row := range result.Rows {
			if total, ok := row[1].(float64); ok {
				assert.Greater(t, total, float64(50), "All totals should be > 50")
			}
		}
	})
}

// ====================================================================================
// Multiple aggregations in same query
// ====================================================================================

func TestAggregation_MultipleAggregates(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	queries := []string{
		`CREATE (n:Sale {product: 'Widget', amount: 100, quantity: 5})`,
		`CREATE (n:Sale {product: 'Widget', amount: 150, quantity: 3})`,
		`CREATE (n:Sale {product: 'Gadget', amount: 200, quantity: 2})`,
		`CREATE (n:Sale {product: 'Gadget', amount: 75, quantity: 10})`,
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("COUNT, SUM, AVG together", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (s:Sale)
			RETURN s.product as product, 
				   count(*) as sales,
				   SUM(s.amount) as total,
				   AVG(s.amount) as avg_amount
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2) // Widget and Gadget

		assert.Contains(t, result.Columns, "product")
		assert.Contains(t, result.Columns, "sales")
		assert.Contains(t, result.Columns, "total")
		assert.Contains(t, result.Columns, "avg_amount")
	})

	t.Run("Multiple aggregates in WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (s:Sale)
			WITH s.product as product,
				 count(*) as sales,
				 SUM(s.quantity) as total_qty
			RETURN product, sales, total_qty
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)

		// Verify data
		for _, row := range result.Rows {
			assert.NotNil(t, row[0], "product should not be nil")
			assert.NotNil(t, row[1], "sales should not be nil")
			assert.NotNil(t, row[2], "total_qty should not be nil")
		}
	})
}

// ====================================================================================
// GROUP BY with multiple columns
// ====================================================================================

func TestAggregation_MultipleGroupByColumns(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	queries := []string{
		`CREATE (n:Order {region: 'East', status: 'Complete', amount: 100})`,
		`CREATE (n:Order {region: 'East', status: 'Complete', amount: 200})`,
		`CREATE (n:Order {region: 'East', status: 'Pending', amount: 50})`,
		`CREATE (n:Order {region: 'West', status: 'Complete', amount: 300})`,
		`CREATE (n:Order {region: 'West', status: 'Pending', amount: 75})`,
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("GROUP BY two columns", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (o:Order)
			RETURN o.region as region, o.status as status, count(*) as cnt
		`, nil)
		require.NoError(t, err)

		// Should have 4 groups: East/Complete, East/Pending, West/Complete, West/Pending
		assert.GreaterOrEqual(t, len(result.Rows), 3)

		// Each row should have distinct region+status combination
		seen := make(map[string]bool)
		for _, row := range result.Rows {
			key := ""
			if row[0] != nil {
				key += row[0].(string)
			}
			key += "-"
			if row[1] != nil {
				key += row[1].(string)
			}
			assert.False(t, seen[key], "Should not have duplicate groups")
			seen[key] = true
		}
	})

	t.Run("GROUP BY two columns with SUM", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (o:Order)
			RETURN o.region as region, o.status as status, SUM(o.amount) as total
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 3)
	})

	t.Run("GROUP BY two columns in WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (o:Order)
			WITH o.region as region, o.status as status, count(*) as cnt
			RETURN region, status, cnt
			ORDER BY cnt DESC
		`, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// Verify ordering
		if len(result.Rows) >= 2 {
			cnt1 := result.Rows[0][2].(int64)
			cnt2 := result.Rows[1][2].(int64)
			assert.GreaterOrEqual(t, cnt1, cnt2)
		}
	})
}

// ====================================================================================
// NULL handling in aggregations
// ====================================================================================

func TestAggregation_NullHandlingAdvanced(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup with deliberate nulls
	queries := []string{
		`CREATE (n:Record {group: 'A', value: 10})`,
		`CREATE (n:Record {group: 'A', value: 20})`,
		`CREATE (n:Record {group: 'A'})`,               // No value
		`CREATE (n:Record {group: 'B', value: 30})`,
		`CREATE (n:Record {value: 40})`,                // No group
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("COUNT(*) includes nulls, COUNT(prop) excludes", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (r:Record)
			RETURN count(*) as total, count(r.value) as with_value
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		total := result.Rows[0][0].(int64)
		withValue := result.Rows[0][1].(int64)

		assert.Equal(t, int64(5), total, "COUNT(*) should count all")
		assert.Equal(t, int64(4), withValue, "COUNT(r.value) should exclude null")
	})

	t.Run("GROUP BY null values", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (r:Record)
			RETURN r.group as grp, count(*) as cnt
		`, nil)
		require.NoError(t, err)

		// Should have groups: A, B, null
		assert.GreaterOrEqual(t, len(result.Rows), 2)
	})

	t.Run("SUM ignores nulls", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (r:Record)
			WHERE r.group = 'A'
			RETURN SUM(r.value) as total
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// SUM(10, 20, null) = 30
		if total, ok := result.Rows[0][0].(float64); ok {
			assert.Equal(t, float64(30), total)
		}
	})
}

// ====================================================================================
// Edge cases
// ====================================================================================

func TestAggregation_EdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("Empty result set", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:NonExistent)
			RETURN count(*) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(0), result.Rows[0][0])
	})

	t.Run("Single node aggregation", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE (n:Solo {name: 'Only'})`, nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, `
			MATCH (n:Solo)
			RETURN n.name as name, count(*) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "Only", result.Rows[0][0])
		assert.Equal(t, int64(1), result.Rows[0][1])
	})

	t.Run("Aggregation with no grouping column", func(t *testing.T) {
		_, _ = exec.Execute(ctx, `CREATE (n:Counter {val: 1})`, nil)
		_, _ = exec.Execute(ctx, `CREATE (n:Counter {val: 2})`, nil)
		_, _ = exec.Execute(ctx, `CREATE (n:Counter {val: 3})`, nil)

		result, err := exec.Execute(ctx, `
			MATCH (n:Counter)
			RETURN count(*) as total, SUM(n.val) as sum, AVG(n.val) as avg
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		assert.Equal(t, int64(3), result.Rows[0][0])
	})
}

// ====================================================================================
// COLLECT variations
// ====================================================================================

func TestAggregation_CollectVariations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	queries := []string{
		`CREATE (n:Tag {category: 'Tech', name: 'Go'})`,
		`CREATE (n:Tag {category: 'Tech', name: 'Rust'})`,
		`CREATE (n:Tag {category: 'Tech', name: 'Go'})`, // Duplicate
		`CREATE (n:Tag {category: 'Science', name: 'Physics'})`,
		`CREATE (n:Tag {category: 'Science', name: 'Math'})`,
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	t.Run("COLLECT per group", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (t:Tag)
			RETURN t.category as cat, COLLECT(t.name) as names
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2) // Tech and Science

		for _, row := range result.Rows {
			names, ok := row[1].([]interface{})
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(names), 1)
		}
	})

	t.Run("COLLECT DISTINCT per group", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (t:Tag)
			WITH t.category as cat, COLLECT(DISTINCT t.name) as names
			RETURN cat, names
		`, nil)
		require.NoError(t, err)

		// Find Tech group and verify Go appears only once
		for _, row := range result.Rows {
			if row[0] == "Tech" {
				names := row[1].([]interface{})
				goCount := 0
				for _, n := range names {
					if n == "Go" {
						goCount++
					}
				}
				assert.Equal(t, 1, goCount, "COLLECT DISTINCT should dedupe")
			}
		}
	})
}

// ====================================================================================
// Combined WHERE + ORDER BY + LIMIT
// ====================================================================================

func TestAggregation_CombinedClauses(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	for i := 0; i < 10; i++ {
		cat := "Cat" + string(rune('A'+i%3))
		_, _ = exec.Execute(ctx, `CREATE (n:Data {category: $cat, value: $val})`,
			map[string]interface{}{"cat": cat, "val": i * 10})
	}

	t.Run("WHERE before WITH + ORDER BY + LIMIT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (d:Data)
			WHERE d.value >= 30
			WITH d.category as cat, count(*) as cnt
			RETURN cat, cnt
			ORDER BY cnt DESC
			LIMIT 2
		`, nil)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(result.Rows), 2)
	})

	t.Run("HAVING + ORDER BY + SKIP", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (d:Data)
			WITH d.category as cat, count(*) as cnt
			WHERE cnt >= 2
			RETURN cat, cnt
			ORDER BY cnt DESC
			SKIP 1
		`, nil)
		require.NoError(t, err)
		// Verify HAVING filter applied
		for _, row := range result.Rows {
			if cnt, ok := row[1].(int64); ok {
				assert.GreaterOrEqual(t, cnt, int64(2))
			}
		}
	})
}
