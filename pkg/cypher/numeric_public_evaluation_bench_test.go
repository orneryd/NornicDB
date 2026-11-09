package cypher

import (
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkNumericExpressionPublicEvaluation enters the complete Execute path
// with result caching disabled. Row expressions enter prefix admission; constant
// comparison/folding and nonconstant zero-division enter arithmetic admission.
func BenchmarkNumericExpressionPublicEvaluation(b *testing.B) {
	for _, tc := range []struct {
		name, query string
		wantError   bool
		want        any
	}{
		{"row_numeric_prefix", "UNWIND [123] AS n RETURN n+1 AS value", false, int64(124)},
		{"row_signed_prefix", "UNWIND [-123] AS n RETURN n+1 AS value", false, int64(-122)},
		{"constant_comparison", "RETURN 9007199254740993 = 9007199254740992.0 AS value", false, true},
		{"folded_zero", "RETURN 1.0 / (1 - 1) AS value", false, math.Inf(1)},
		{"nonconstant_zero", "RETURN toFloat(1) / 0 AS value", true, nil},
	} {
		b.Run(tc.name, func(b *testing.B) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "numeric_public_eval"), QueryCachePolicy{MaxEntries: 0})
			ctx := b.Context()
			result, err := executor.Execute(ctx, tc.query, nil)
			if tc.wantError {
				if err == nil {
					b.Fatal("missing division error")
				}
			} else {
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != tc.want {
					b.Fatalf("unexpected public result: %#v", result)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := executor.Execute(ctx, tc.query, nil)
				if (err != nil) != tc.wantError {
					b.Fatalf("unexpected public error: %v", err)
				}
			}
		})
	}
}
