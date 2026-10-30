package cypher

// Author: Codex NOR-184 lane. Ordinary signatures across both public evaluators.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCOrdinaryInvocationNOR184(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "forwarded")
	withPluginFunctions(t, map[string]interface{}{
		"nor184.ordinary_zero":     func() string { return "zero" },
		"nor184.ordinary_text":     func(v string) string { return v + "!" },
		"nor184.ordinary_number":   func(v float64) float64 { return v * 2 },
		"nor184.ordinary_list":     func(v []float64) []float64 { return v },
		"nor184.ordinary_pair":     func(a, b []float64) float64 { return a[0] + b[0] },
		"nor184.ordinary_any":      func(v interface{}) interface{} { return v },
		"nor184.ordinary_variadic": func(v ...int64) int64 { return int64(len(v)) },
		"nor184.ordinary_context":  func(got context.Context, v string) string { require.Equal(t, "forwarded", got.Value(key{})); return v },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184_ordinary_invocation"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, tc := range []struct {
			expression string
			want       interface{}
		}{
			{"nor184.ordinary_zero()", "zero"},
			{"nor184.ordinary_text('a')", "a!"},
			{"nor184.ordinary_number('2.5')", float64(5)},
			{"size(nor184.ordinary_list(['2',3]))", int64(2)},
			{"size(nor184.ordinary_list(null))", int64(0)},
			{"nor184.ordinary_pair(['2'],[3])", float64(5)},
			{"nor184.ordinary_any(null)", nil},
			{"nor184.ordinary_variadic(1,2,3)", int64(3)},
			{"nor184.ordinary_context('context')", "context"},
		} {
			for _, prefix := range []string{"", "UNWIND [1] AS row WITH row "} {
				query := prefix + "RETURN " + tc.expression + " AS v"
				t.Run(query, func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
				})
			}
		}
	}
}
