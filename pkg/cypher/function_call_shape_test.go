package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStandaloneFunctionCallPurposes(t *testing.T) {
	for _, tc := range []struct {
		source, expected, name, inner string
		qualified, named              bool
	}{
		{"count(n)", "count", "count", "n", true, true},
		{"COUNT (n)", "count", "COUNT", "n", true, true},
		{"  count(n)", "count", "count", "n", true, true},
		{"count(n) ", "count", "count", "n", true, false},
		{" count ( n ) \n", "count", "count", " n ", true, false},
		{"custom . range(n)", "custom . range", "custom.range", "n", true, true},
		{"custom . /*c*/ range(n)", "custom . /*c*/ range", "custom.range", "n", true, true},
		{"count('x(y)')", "count", "count", "'x(y)'", true, true},
		{"count(n /* ) */)", "count", "count", "n /* ) */", true, true},
		{"count(n) + 1", "count", "", "", false, false},
		{"toLower(count (n))", "count", "toLower", "count (n)", true, false},
		{"count(n", "count", "", "", false, false},
		{"count(n))", "count", "", "", false, false},
		{"count/*c*/(n)", "count", "count", "n", true, false},
		{"bad-name(n)", "bad-name", "", "", false, true},
		{"(())", "(", "", "", false, true},
	} {
		t.Run(tc.source, func(t *testing.T) {
			var facts functionCallFacts
			matched := inspectFunctionCall(tc.source, qualifiedFunctionCall, "", &facts)
			require.Equal(t, tc.qualified, matched)
			require.Equal(t, tc.name, facts.name)
			require.Equal(t, tc.inner, facts.inner)
			require.Equal(t, matched, inspectFunctionCall(tc.source, qualifiedFunctionCall, "", nil), "capture must not alter admission")
			require.Equal(t, tc.named, inspectFunctionCall(tc.source, namedFunctionCall, tc.expected, nil))
		})
	}
}

func TestStandaloneFunctionCallPublicConsumers(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "function-shape"))
	for _, tc := range []struct {
		query string
		value interface{}
	}{
		{"RETURN toLower ( 'ABC' ) AS value", "abc"},
		{"RETURN substring ( toLower ('ABCD'), 1, 2 ) AS value", "bc"},
		{"RETURN coalesce ( null, 'kept' ) AS value", "kept"},
		{"RETURN size ( range (1, 3) ) AS value", int64(3)},
		{"UNWIND [1,2] AS x RETURN count (x) AS value", int64(2)},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := executor.Execute(t.Context(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, result.Columns)
			require.Equal(t, [][]interface{}{{tc.value}}, result.Rows)
		})
	}
}

func BenchmarkNamedFunctionCallShape(b *testing.B) {
	for _, tc := range []struct{ source, name string }{
		{"count(n)", "count"},
		{"custom.range(n)", "custom.range"},
		{"count(n) ", "count"},
		{"bad-name(n)", "bad-name"},
	} {
		b.Run(tc.source, func(b *testing.B) {
			want := inspectFunctionCall(tc.source, namedFunctionCall, tc.name, nil)
			b.ReportAllocs()
			for b.Loop() {
				if inspectFunctionCall(tc.source, namedFunctionCall, tc.name, nil) != want {
					b.Fatal("unstable function shape")
				}
			}
		})
	}
}
