package fn

import (
	"context"
	"testing"
	"time"
)

// BenchmarkReadObservationPolicy evaluates one registered function with the
// same pure implementation under both policies. The policy is part of the
// finite registry entry, so a lookup and an ordinary (unprofiled) call add no
// per-call allocation under either policy; only an opaque registration reaches
// the opaque-callback hook. Run with -benchmem to compare.
func BenchmarkReadObservationPolicy(b *testing.B) {
	ctx := Context{Eval: func(string) (interface{}, error) { return nil, nil }, Now: time.Now}
	implementation := func(_ context.Context, _ Context, _ []string) (interface{}, error) { return "value", nil }
	for _, tc := range []struct {
		name  string
		reads ReadObservation
	}{
		{name: "opaque", reads: OpaqueReads},
		{name: "canonical", reads: CanonicalReads},
	} {
		b.Run(tc.name, func(b *testing.B) {
			Register("policy_benchmark_value", implementation, tc.reads)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, found, err := EvaluateFunction(context.Background(), "policy_benchmark_value", nil, ctx)
				if err != nil || !found || value != "value" {
					b.Fatalf("evaluation failed: value=%v found=%v err=%v", value, found, err)
				}
			}
		})
	}
}
