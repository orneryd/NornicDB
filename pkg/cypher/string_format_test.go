// Test for format() function only
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestFormatStringFunction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a simple test node to ensure executor works
	_, err := exec.Execute(ctx, `CREATE (n:Test {name: 'Alice', age: 30})`, nil)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "format with string",
			query:    `RETURN format('Hello %s', 'World') AS result`,
			expected: "Hello World",
		},
		{
			name:     "format with integer",
			query:    `RETURN format('Number: %d', 42) AS result`,
			expected: "Number: 42",
		},
		{
			name:     "format with float",
			query:    `RETURN format('Pi: %.2f', 3.14159) AS result`,
			expected: "Pi: 3.14",
		},
		{
			name:     "format with multiple args",
			query:    `RETURN format('%s is %d years old', 'Alice', 30) AS result`,
			expected: "Alice is 30 years old",
		},
		{
			name:     "format no args",
			query:    `RETURN format('No placeholders') AS result`,
			expected: "No placeholders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v\nQuery: %s", err, tt.query)
			}

			if len(result.Rows) == 0 {
				t.Fatal("No rows returned")
			}

			if str, ok := result.Rows[0][0].(string); ok {
				if str != tt.expected {
					t.Errorf("format() = %q, want %q", str, tt.expected)
				}
			} else {
				t.Errorf("format() returned %T (%v), want string", result.Rows[0][0], result.Rows[0][0])
			}
		})
	}
}

func TestExistingStringFunctionsStillWork(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "reverse still works",
			query:    `RETURN reverse('hello') AS result`,
			expected: "olleh",
		},
		{
			name:     "lpad still works",
			query:    `RETURN lpad('5', 3, '0') AS result`,
			expected: "005",
		},
		{
			name:     "rpad still works",
			query:    `RETURN rpad('5', 3, '0') AS result`,
			expected: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("No rows returned")
			}

			if result.Rows[0][0] != tt.expected {
				t.Errorf("Got %v (%T), want %v", result.Rows[0][0], result.Rows[0][0], tt.expected)
			}
		})
	}
}
