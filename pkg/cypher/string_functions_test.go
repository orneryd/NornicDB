// Tests for string functions in NornicDB Cypher implementation.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestReverseFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "reverse simple string",
			query:    "RETURN reverse('hello') AS result",
			expected: "olleh",
		},
		{
			name:     "reverse with unicode",
			query:    "RETURN reverse('hello 世界') AS result",
			expected: "界世 olleh",
		},
		{
			name:     "reverse empty string",
			query:    "RETURN reverse('') AS result",
			expected: "",
		},
		{
			name:     "reverse single char",
			query:    "RETURN reverse('a') AS result",
			expected: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}
			got := result.Rows[0][0]
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestLpadFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "lpad with default space",
			query:    "RETURN lpad('hi', 5) AS result",
			expected: "   hi",
		},
		{
			name:     "lpad with custom char",
			query:    "RETURN lpad('42', 5, '0') AS result",
			expected: "00042",
		},
		{
			name:     "lpad string longer than length",
			query:    "RETURN lpad('hello world', 5, '*') AS result",
			expected: "hello",
		},
		{
			name:     "lpad exact length",
			query:    "RETURN lpad('hello', 5, '*') AS result",
			expected: "hello",
		},
		{
			name:     "lpad with multi-char pad",
			query:    "RETURN lpad('x', 5, 'ab') AS result",
			expected: "ababx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}
			got := result.Rows[0][0]
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestRpadFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "rpad with default space",
			query:    "RETURN rpad('hi', 5) AS result",
			expected: "hi   ",
		},
		{
			name:     "rpad with custom char",
			query:    "RETURN rpad('42', 5, '0') AS result",
			expected: "42000",
		},
		{
			name:     "rpad string longer than length",
			query:    "RETURN rpad('hello world', 5, '*') AS result",
			expected: "hello",
		},
		{
			name:     "rpad exact length",
			query:    "RETURN rpad('hello', 5, '*') AS result",
			expected: "hello",
		},
		{
			name:     "rpad with multi-char pad",
			query:    "RETURN rpad('x', 5, 'ab') AS result",
			expected: "xabab",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}
			got := result.Rows[0][0]
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}
