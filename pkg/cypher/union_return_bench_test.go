package cypher

import (
	"context"
	"testing"
)

// BenchmarkExecuteReturn_NoBindings pins the common RETURN path (no value
// scope) after the §6.2 binding-seeding change.
func BenchmarkExecuteReturn_NoBindings(b *testing.B) {
	exec, _ := newTestExecutor(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.executeReturn(ctx, "RETURN 1 AS one, 'x' AS two"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExecuteReturn_BoundValues pins the bound-child RETURN path (value
// scope present).
func BenchmarkExecuteReturn_BoundValues(b *testing.B) {
	exec, _ := newTestExecutor(b)
	ctx := withValueBindings(context.Background(), map[string]interface{}{"x": int64(7), "row": map[string]interface{}{"name": "ann"}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.executeReturn(ctx, "RETURN x + 1 AS one, row.name AS two"); err != nil {
			b.Fatal(err)
		}
	}
}
