package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestOrderByLimitSkip(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	for i := 1; i <= 10; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:Person {name: $name, age: $age})", map[string]interface{}{
			"name": string(rune('A' + i - 1)), // A, B, C, etc.
			"age":  int64(20 + i),              // 21, 22, 23, etc.
		})
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}
	}

	t.Run("OrderByASC", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 10 {
			t.Errorf("Expected 10 rows, got %d", len(result.Rows))
		}

		// Verify ascending order
		for i := 0; i < len(result.Rows)-1; i++ {
			age1 := result.Rows[i][1].(int64)
			age2 := result.Rows[i+1][1].(int64)
			if age1 > age2 {
				t.Errorf("Not in ascending order: row %d (%d) > row %d (%d)", i, age1, i+1, age2)
			}
		}
	})

	t.Run("OrderByDESC", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 10 {
			t.Errorf("Expected 10 rows, got %d", len(result.Rows))
		}

		// Verify descending order
		for i := 0; i < len(result.Rows)-1; i++ {
			age1 := result.Rows[i][1].(int64)
			age2 := result.Rows[i+1][1].(int64)
			if age1 < age2 {
				t.Errorf("Not in descending order: row %d (%d) < row %d (%d)", i, age1, i+1, age2)
			}
		}
	})

	t.Run("Limit", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age LIMIT 5", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows with LIMIT 5, got %d", len(result.Rows))
		}
	})

	t.Run("Skip", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age SKIP 3", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 7 {
			t.Errorf("Expected 7 rows with SKIP 3 (10-3), got %d", len(result.Rows))
		}
	})

	t.Run("OrderByWithLimit", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC LIMIT 3", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}

		// Verify we got the top 3 (ages 30, 29, 28)
		if result.Rows[0][1].(int64) != 30 {
			t.Errorf("Expected first row age 30, got %d", result.Rows[0][1])
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		// Page 1: SKIP 0 LIMIT 3
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC SKIP 0 LIMIT 3", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Page 1: Expected 3 rows, got %d", len(result.Rows))
		}
		if result.Rows[0][1].(int64) != 21 {
			t.Errorf("Page 1: Expected first age 21, got %d", result.Rows[0][1])
		}

		// Page 2: SKIP 3 LIMIT 3
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC SKIP 3 LIMIT 3", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Page 2: Expected 3 rows, got %d", len(result.Rows))
		}
		if result.Rows[0][1].(int64) != 24 {
			t.Errorf("Page 2: Expected first age 24, got %d", result.Rows[0][1])
		}
	})

	t.Run("Distinct", func(t *testing.T) {
		// Create duplicate ages
		exec.Execute(ctx, "CREATE (n:Person {name: 'Duplicate1', age: 25})", nil)
		exec.Execute(ctx, "CREATE (n:Person {name: 'Duplicate2', age: 25})", nil)

		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN DISTINCT n.age", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Count unique ages
		uniqueAges := make(map[int64]bool)
		for _, row := range result.Rows {
			age := row[0].(int64)
			uniqueAges[age] = true
		}

		// Should have 10 unique ages (21-30)
		if len(uniqueAges) != 10 {
			t.Errorf("Expected 10 unique ages, got %d", len(uniqueAges))
		}

		// But result should only have 10 rows (duplicates removed)
		if len(result.Rows) != 10 {
			t.Errorf("DISTINCT should return 10 rows, got %d", len(result.Rows))
		}
	})
}

func TestOrderByStringValues(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with names
	names := []string{"Zoe", "Alice", "Bob", "Charlie", "Diana"}
	for _, name := range names {
		_, err := exec.Execute(ctx, "CREATE (n:Person {name: $name})", map[string]interface{}{
			"name": name,
		})
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	t.Run("OrderByStringASC", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Verify alphabetical order
		expected := []string{"Alice", "Bob", "Charlie", "Diana", "Zoe"}
		for i, row := range result.Rows {
			name := row[0].(string)
			if name != expected[i] {
				t.Errorf("Row %d: expected %s, got %s", i, expected[i], name)
			}
		}
	})

	t.Run("OrderByStringDESC", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name DESC", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Verify reverse alphabetical order
		expected := []string{"Zoe", "Diana", "Charlie", "Bob", "Alice"}
		for i, row := range result.Rows {
			name := row[0].(string)
			if name != expected[i] {
				t.Errorf("Row %d: expected %s, got %s", i, expected[i], name)
			}
		}
	})
}

func TestLimitSkipEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 5 nodes
	for i := 1; i <= 5; i++ {
		exec.Execute(ctx, "CREATE (n:Test {value: $val})", map[string]interface{}{
			"val": int64(i),
		})
	}

	t.Run("LimitZero", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value LIMIT 0", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("LIMIT 0 should return 0 rows, got %d", len(result.Rows))
		}
	})

	t.Run("LimitGreaterThanTotal", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value LIMIT 100", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 5 {
			t.Errorf("LIMIT 100 should return all 5 rows, got %d", len(result.Rows))
		}
	})

	t.Run("SkipAll", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value SKIP 5", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("SKIP 5 (all rows) should return 0 rows, got %d", len(result.Rows))
		}
	})

	t.Run("SkipMoreThanTotal", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Test) RETURN n.value SKIP 100", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("SKIP 100 should return 0 rows, got %d", len(result.Rows))
		}
	})
}
