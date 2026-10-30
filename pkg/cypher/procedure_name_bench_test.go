package cypher

import (
	"regexp"
	"testing"
)

var procedureNameBenchmarkResult string

// The baseline is the literal reader from release88358ffa; only canonical
// unquoted calls are compared because it cannot parse quoted/spaced names.
func BenchmarkProcedureNameCanonicalReader(b *testing.B) {
	legacy := regexp.MustCompile(`(?i)CALL\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`)
	for _, query := range []string{"CALL db.awaitIndexes()", "CALL apoc.custom.installProcedure()"} {
		b.Run(query, func(b *testing.B) {
			b.Run("release88358ffa", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					procedureNameBenchmarkResult = legacy.FindStringSubmatch(query)[1]
				}
			})
			b.Run("sharedComponentReader", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					procedureNameBenchmarkResult = extractProcedureName(b.Context(), query)
				}
			})
		})
	}
}
