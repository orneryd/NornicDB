package resultstream

import (
	"context"
	"testing"
	"time"
)

func BenchmarkRegistryReadToEndAndDiscard(b *testing.B) {
	r, err := NewRegistry(Config{TTL: time.Hour, MaxStreams: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(r.Close)
	scope := Scope{Owner: "benchmark", Database: "nornic"}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := NewProgressive([][]any{{0}, {1}}, true, 2, nil)
		if err != nil {
			b.Fatal(err)
		}
		first, err := r.Start(ctx, scope, s, 1)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := r.Pull(ctx, scope, first.QID, 1); err != nil {
			b.Fatal(err)
		}
		// The baseline needs DISCARD to avoid exhausting admission. The same
		// request is an idempotent no-op after terminal consumption on the fix.
		if err := r.Discard(scope, first.QID); err != nil {
			b.Fatal(err)
		}
	}
}
