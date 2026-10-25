package cypher

import "testing"

var patternBindingBenchmarkNames []string

// This isolates uncached binding extraction from statement and result caches.
// The public routing controls separately measure ordinary MATCH execution.
func BenchmarkPatternBindingDeclarations(b *testing.B) {
	exec := new(StorageExecutor)
	patterns := []string{
		"(a:Person)-[:KNOWS]->(b:Person)",
		"(n:$any(ls))",
		"p = shortestPath((a)-[*]-(b))",
		"(a)((x)-[r:T]->(y)){1,3}(b)",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, pattern := range patterns {
			patternBindingBenchmarkNames = exec.extractVariableNamesFromPattern(b.Context(), pattern)
		}
	}
}
