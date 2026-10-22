package cypher

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkSemanticValidationDistinctText exercises misses in text-derived
// caches as well as semantic validation: every statement has a different ID.
func BenchmarkSemanticValidationDistinctText(b *testing.B) {
	executor := &StorageExecutor{}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		query := "MATCH (n:Person {id: " + strconv.Itoa(i) + "}) RETURN n.name"
		// Each text is new; prepare its source outside validation measurement.
		b.StopTimer()
		fixtureContext, statement := statementTestContext(ctx, query)
		b.StartTimer()
		if err := executor.validateSemanticScopes(statement, fixtureContext, query); err != nil {
			b.Fatal(err)
		}
	}
}
