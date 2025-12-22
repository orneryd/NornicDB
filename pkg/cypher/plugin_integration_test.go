package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginFunctionLookupIntegration(t *testing.T) {
	// Create executor
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Save original lookup and restore after test
	originalLookup := PluginFunctionLookup
	defer func() { PluginFunctionLookup = originalLookup }()

	t.Run("plugin lookup not configured - falls back to built-in", func(t *testing.T) {
		PluginFunctionLookup = nil

		// This should fall back to built-in implementation
		result, err := exec.Execute(ctx, "RETURN apoc.coll.sum([1, 2, 3]) AS total", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Built-in should still work
		assert.NotNil(t, result.Rows[0][0])
	})

	t.Run("plugin lookup configured - calls plugin function", func(t *testing.T) {
		// Configure a mock plugin lookup
		mockCalled := false
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			if name == "myplugin.double" {
				mockCalled = true
				return func(x float64) float64 { return x * 2 }, true
			}
			return nil, false
		}

		// This should try to call the plugin function
		result, err := exec.Execute(ctx, "RETURN myplugin.double(21) AS value", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// The mock was called
		assert.True(t, mockCalled, "Plugin lookup should have been called")

		// Result should be 42
		assert.Equal(t, 42.0, result.Rows[0][0])
	})

	t.Run("plugin returns sum function", func(t *testing.T) {
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			if name == "test.sum" {
				return func(vals []interface{}) float64 {
					sum := 0.0
					for _, v := range vals {
						switch n := v.(type) {
						case int:
							sum += float64(n)
						case int64:
							sum += float64(n)
						case float64:
							sum += n
						}
					}
					return sum
				}, true
			}
			return nil, false
		}

		result, err := exec.Execute(ctx, "RETURN test.sum([10, 20, 30]) AS total", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, 60.0, result.Rows[0][0])
	})

	t.Run("plugin not found falls back to built-in", func(t *testing.T) {
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			// Plugin doesn't have this function
			return nil, false
		}

		// Built-in apoc.coll.sum should still work
		result, err := exec.Execute(ctx, "RETURN apoc.coll.sum([1, 2, 3, 4]) AS total", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Should get result from built-in
		assert.NotNil(t, result.Rows[0][0])
	})

	t.Run("plugin string function", func(t *testing.T) {
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			if name == "text.shout" {
				return func(s string) string { return s + "!!!" }, true
			}
			return nil, false
		}

		result, err := exec.Execute(ctx, "RETURN text.shout('hello') AS greeting", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "hello!!!", result.Rows[0][0])
	})
}

func TestCallPluginHandler(t *testing.T) {
	t.Run("nil handler returns error", func(t *testing.T) {
		_, err := callPluginHandler(nil, nil)
		assert.Error(t, err)
	})

	t.Run("func() float64", func(t *testing.T) {
		handler := func() float64 { return 3.14 }
		result, err := callPluginHandler(handler, nil)
		require.NoError(t, err)
		assert.Equal(t, 3.14, result)
	})

	t.Run("func() string", func(t *testing.T) {
		handler := func() string { return "hello" }
		result, err := callPluginHandler(handler, nil)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("func(float64) float64", func(t *testing.T) {
		handler := func(x float64) float64 { return x * 2 }
		result, err := callPluginHandler(handler, []interface{}{21.0})
		require.NoError(t, err)
		assert.Equal(t, 42.0, result)
	})

	t.Run("func(string) string", func(t *testing.T) {
		handler := func(s string) string { return s + "!" }
		result, err := callPluginHandler(handler, []interface{}{"hello"})
		require.NoError(t, err)
		assert.Equal(t, "hello!", result)
	})

	t.Run("func([]interface{}) float64", func(t *testing.T) {
		handler := func(vals []interface{}) float64 {
			return float64(len(vals))
		}
		result, err := callPluginHandler(handler, []interface{}{[]interface{}{1, 2, 3}})
		require.NoError(t, err)
		assert.Equal(t, 3.0, result)
	})

	t.Run("func(string, string) string", func(t *testing.T) {
		handler := func(a, b string) string { return a + " " + b }
		result, err := callPluginHandler(handler, []interface{}{"hello", "world"})
		require.NoError(t, err)
		assert.Equal(t, "hello world", result)
	})

	t.Run("func([]float64, []float64) float64 - dot product", func(t *testing.T) {
		handler := func(a, b []float64) float64 {
			sum := 0.0
			for i := 0; i < len(a) && i < len(b); i++ {
				sum += a[i] * b[i]
			}
			return sum
		}
		result, err := callPluginHandler(handler, []interface{}{
			[]float64{1, 2, 3},
			[]float64{4, 5, 6},
		})
		require.NoError(t, err)
		// 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
		assert.Equal(t, 32.0, result)
	})

	t.Run("unsupported signature returns error", func(t *testing.T) {
		handler := func(a, b, c, d int) int { return a + b + c + d }
		_, err := callPluginHandler(handler, []interface{}{1, 2, 3, 4})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported")
	})
}

func TestNamespacedFunctionDetection(t *testing.T) {
	// Test that namespaced functions are properly detected
	testCases := []struct {
		expr     string
		hasNS    bool
		looksLikeFunc bool
	}{
		{"apoc.coll.sum([1,2,3])", true, true},
		{"myplugin.func()", true, true},
		{"a.b.c.d(x)", true, true},
		{"simple()", false, true},
		{"noparens", false, false},
		{"UPPER.CASE.FUNC()", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			hasNS := containsDot(tc.expr)
			assert.Equal(t, tc.hasNS, hasNS, "namespace detection for %s", tc.expr)

			isFunc := looksLikeFunctionCall(tc.expr)
			assert.Equal(t, tc.looksLikeFunc, isFunc, "function detection for %s", tc.expr)
		})
	}
}

// Helper to check if string contains a dot
func containsDot(s string) bool {
	for _, c := range s {
		if c == '.' {
			return true
		}
	}
	return false
}
