package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCaseExpressionSearched(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	for _, data := range []struct {
		name string
		age  int64
	}{
		{"Alice", 15},
		{"Bob", 30},
		{"Charlie", 70},
		{"Diana", 25},
	} {
		_, err := exec.Execute(ctx, "CREATE (p:Person {name: $name, age: $age})", map[string]interface{}{
			"name": data.name,
			"age":  data.age,
		})
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	t.Run("BasicSearchedCase", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			RETURN p.name,
				CASE
					WHEN p.age < 18 THEN 'minor'
					WHEN p.age < 65 THEN 'adult'
					ELSE 'senior'
				END AS ageGroup
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 4 {
			t.Errorf("Expected 4 rows, got %d", len(result.Rows))
		}

		// Check specific cases
		expected := map[string]string{
			"Alice":   "minor",
			"Bob":     "adult",
			"Charlie": "senior",
			"Diana":   "adult",
		}

		for _, row := range result.Rows {
			name := row[0].(string)
			ageGroup := row[1]
			if ageGroup == nil {
				t.Errorf("Got NULL for %s, expected %s", name, expected[name])
				continue
			}
			if expected[name] != ageGroup.(string) {
				t.Errorf("For %s: expected %s, got %v", name, expected[name], ageGroup)
			}
		}
	})

	t.Run("CaseWithoutElse", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			RETURN p.name,
				CASE
					WHEN p.age < 18 THEN 'minor'
					WHEN p.age >= 65 THEN 'senior'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Check that adults get NULL (no matching WHEN, no ELSE)
		for _, row := range result.Rows {
			name := row[0].(string)
			category := row[1]

			if name == "Bob" || name == "Diana" {
				// Adults should get NULL
				if category != nil {
					t.Errorf("Expected NULL for %s (adult), got %v", name, category)
				}
			} else {
				// Minor and senior should have values
				if category == nil {
					t.Errorf("Expected non-NULL for %s", name)
				}
			}
		}
	})

	t.Run("CaseWithComparison", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p.name = 'Bob'
			RETURN 
				CASE
					WHEN p.age > 50 THEN 'old'
					WHEN p.age > 25 THEN 'middle'
					WHEN p.age > 0 THEN 'young'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		if result.Rows[0][0] != "middle" {
			t.Errorf("Expected 'middle', got %v", result.Rows[0][0])
		}
	})

	t.Run("CaseWithLessEqual", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p.name = 'Diana'
			RETURN 
				CASE
					WHEN p.age <= 20 THEN 'young'
					WHEN p.age <= 40 THEN 'adult'
					ELSE 'old'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Rows[0][0] != "adult" {
			t.Errorf("Expected 'adult', got %v", result.Rows[0][0])
		}
	})
}

func TestCaseExpressionSimple(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with status
	for _, data := range []struct {
		name   string
		status string
	}{
		{"Task1", "pending"},
		{"Task2", "active"},
		{"Task3", "done"},
		{"Task4", "cancelled"},
	} {
		_, err := exec.Execute(ctx, "CREATE (t:Task {name: $name, status: $status})", map[string]interface{}{
			"name":   data.name,
			"status": data.status,
		})
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	t.Run("SimpleCaseExpression", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (t:Task)
			RETURN t.name,
				CASE t.status
					WHEN 'pending' THEN 'Not Started'
					WHEN 'active' THEN 'In Progress'
					WHEN 'done' THEN 'Completed'
					ELSE 'Unknown'
				END AS statusLabel
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 4 {
			t.Errorf("Expected 4 rows, got %d", len(result.Rows))
		}

		expected := map[string]string{
			"Task1": "Not Started",
			"Task2": "In Progress",
			"Task3": "Completed",
			"Task4": "Unknown",
		}

		for _, row := range result.Rows {
			name := row[0].(string)
			label := row[1].(string)
			if expected[name] != label {
				t.Errorf("For %s: expected %s, got %s", name, expected[name], label)
			}
		}
	})

	t.Run("SimpleCaseWithNumbers", func(t *testing.T) {
		// Create numeric test data
		exec.Execute(ctx, "CREATE (n:Number {value: 1})", nil)
		exec.Execute(ctx, "CREATE (n:Number {value: 2})", nil)
		exec.Execute(ctx, "CREATE (n:Number {value: 3})", nil)

		result, err := exec.Execute(ctx, `
			MATCH (n:Number)
			RETURN n.value,
				CASE n.value
					WHEN 1 THEN 'one'
					WHEN 2 THEN 'two'
					WHEN 3 THEN 'three'
				END AS word
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}

		for _, row := range result.Rows {
			value := row[0].(int64)
			word := row[1].(string)
			
			expected := map[int64]string{1: "one", 2: "two", 3: "three"}
			if expected[value] != word {
				t.Errorf("For value %d: expected %s, got %s", value, expected[value], word)
			}
		}
	})
}

func TestCaseExpressionNullHandling(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create data with NULL values
	exec.Execute(ctx, "CREATE (p:Person {name: 'Alice', age: 30})", nil)
	exec.Execute(ctx, "CREATE (p:Person {name: 'Bob'})", nil) // No age property

	t.Run("CaseWithNullValue", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			RETURN p.name,
				CASE
					WHEN p.age IS NULL THEN 'unknown'
					WHEN p.age < 18 THEN 'minor'
					ELSE 'adult'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		for _, row := range result.Rows {
			name := row[0].(string)
			category := row[1].(string)

			if name == "Bob" && category != "unknown" {
				t.Errorf("Expected 'unknown' for Bob (NULL age), got %s", category)
			}
			if name == "Alice" && category != "adult" {
				t.Errorf("Expected 'adult' for Alice, got %s", category)
			}
		}
	})

	t.Run("CaseReturningNull", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p.name = 'Bob'
			RETURN 
				CASE
					WHEN p.age > 100 THEN 'very old'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should return NULL (no matching WHEN, no ELSE)
		if result.Rows[0][0] != nil {
			t.Errorf("Expected NULL, got %v", result.Rows[0][0])
		}
	})
}

func TestCaseExpressionComplexConditions(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	exec.Execute(ctx, "CREATE (p:Product {name: 'Widget', price: 100, stock: 5})", nil)
	exec.Execute(ctx, "CREATE (p:Product {name: 'Gadget', price: 50, stock: 0})", nil)
	exec.Execute(ctx, "CREATE (p:Product {name: 'Tool', price: 200, stock: 10})", nil)

	t.Run("CaseWithMultipleConditions", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			RETURN p.name,
				CASE
					WHEN p.stock = 0 THEN 'Out of Stock'
					WHEN p.price >= 200 THEN 'Premium'
					WHEN p.price >= 100 THEN 'Standard'
					ELSE 'Budget'
				END AS category
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		expected := map[string]string{
			"Widget": "Standard",
			"Gadget": "Out of Stock", // stock = 0 comes first
			"Tool":   "Premium",
		}

		for _, row := range result.Rows {
			name := row[0].(string)
			category := row[1].(string)
			if expected[name] != category {
				t.Errorf("For %s: expected %s, got %s", name, expected[name], category)
			}
		}
	})

	t.Run("CaseWithNotEquals", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Product)
			WHERE p.name = 'Widget'
			RETURN 
				CASE
					WHEN p.stock <> 0 THEN 'available'
					ELSE 'unavailable'
				END AS availability
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Rows[0][0] != "available" {
			t.Errorf("Expected 'available', got %v", result.Rows[0][0])
		}
	})
}

func TestCaseExpressionEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	exec.Execute(ctx, "CREATE (n:Test {value: 0})", nil)
	exec.Execute(ctx, "CREATE (n:Test {value: -1})", nil)
	exec.Execute(ctx, "CREATE (n:Test {name: ''})", nil)

	t.Run("CaseWithZeroValue", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Test)
			WHERE n.value = 0
			RETURN 
				CASE n.value
					WHEN 0 THEN 'zero'
					WHEN 1 THEN 'one'
					ELSE 'other'
				END AS label
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Rows[0][0] != "zero" {
			t.Errorf("Expected 'zero', got %v", result.Rows[0][0])
		}
	})

	t.Run("CaseWithNegativeNumber", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Test)
			WHERE n.value = -1
			RETURN 
				CASE
					WHEN n.value < 0 THEN 'negative'
					WHEN n.value > 0 THEN 'positive'
					ELSE 'zero'
				END AS sign
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Rows[0][0] != "negative" {
			t.Errorf("Expected 'negative', got %v", result.Rows[0][0])
		}
	})

	t.Run("CaseWithEmptyString", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Test)
			WHERE n.name IS NOT NULL
			RETURN 
				CASE n.name
					WHEN '' THEN 'empty'
					ELSE 'not empty'
				END AS status
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Rows[0][0] != "empty" {
			t.Errorf("Expected 'empty', got %v", result.Rows[0][0])
		}
	})
}
