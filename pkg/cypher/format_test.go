// Package cypher - Tests for format() string function
package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestFormatFunction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name:     "format with string placeholder",
			expr:     `format('Hello %s', 'World')`,
			expected: "Hello World",
		},
		{
			name:     "format with integer placeholder",
			expr:     `format('Number: %d', 42)`,
			expected: "Number: 42",
		},
		{
			name:     "format with float placeholder",
			expr:     `format('Pi: %.2f', 3.14159)`,
			expected: "Pi: 3.14",
		},
		{
			name:     "format with multiple placeholders",
			expr:     `format('%s is %d years old', 'Alice', 30)`,
			expected: "Alice is 30 years old",
		},
		{
			name:     "format with %v placeholder",
			expr:     `format('Value: %v', true)`,
			expected: "Value: true",
		},
		{
			name:     "format with multiple types",
			expr:     `format('User %s: age %d, balance $%.2f', 'Bob', 25, 100.50)`,
			expected: "User Bob: age 25, balance $100.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateExpressionWithContext(tt.expr, nil, nil)
			if str, ok := result.(string); ok {
				if str != tt.expected {
					t.Errorf("format() = %q, want %q", str, tt.expected)
				}
			} else {
				t.Errorf("format() returned %T, want string", result)
			}
		})
	}
}

func TestFormatFunctionEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	t.Run("format with no args", func(t *testing.T) {
		result := exec.evaluateExpressionWithContext(`format('No placeholders')`, nil, nil)
		if str, ok := result.(string); ok {
			if str != "No placeholders" {
				t.Errorf("format() = %q, want %q", str, "No placeholders")
			}
		} else {
			t.Errorf("format() returned %T, want string", result)
		}
	})

	t.Run("format with quoted template", func(t *testing.T) {
		result := exec.evaluateExpressionWithContext(`format("User: %s", "Alice")`, nil, nil)
		if str, ok := result.(string); ok {
			if str != "User: Alice" {
				t.Errorf("format() = %q, want %q", str, "User: Alice")
			}
		} else {
			t.Errorf("format() returned %T, want string", result)
		}
	})

	t.Run("format with percent literal", func(t *testing.T) {
		result := exec.evaluateExpressionWithContext(`format('100%% complete')`, nil, nil)
		if str, ok := result.(string); ok {
			if str != "100% complete" {
				t.Errorf("format() = %q, want %q", str, "100% complete")
			}
		} else {
			t.Errorf("format() returned %T, want string", result)
		}
	})
}
