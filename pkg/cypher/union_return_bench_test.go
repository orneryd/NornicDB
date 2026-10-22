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
	fixtureAdmission1847Context, fixtureAdmission1847 := statementTestContext(ctx, "RETURN 1 AS one, 'x' AS two")
	b.ResetTimer()

	_ = fixtureAdmission1847Context
	for i := 0; i < b.N; i++ {
		if _, err := exec.executeReturn(fixtureAdmission1847, fixtureAdmission1847Context, "RETURN 1 AS one, 'x' AS two"); err != nil {
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
	fixtureAdmission1848Context, fixtureAdmission1848 := statementTestContext(ctx, "RETURN x + 1 AS one, row.name AS two")
	b.ResetTimer()

	_ = fixtureAdmission1848Context
	for i := 0; i < b.N; i++ {
		if _, err := exec.executeReturn(fixtureAdmission1848, fixtureAdmission1848Context, "RETURN x + 1 AS one, row.name AS two"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteInternal_ScalarParameter(b *testing.B) {
	exec, _ := newTestExecutor(b)
	ctx := context.Background()
	params := map[string]interface{}{"value": int64(7)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.executeInternal(ctx, "RETURN $value AS value", params); err != nil {
			b.Fatal(err)
		}
	}
}
