// Package cypher provides integrated executor-flow comparisons between the
// Nornic and ANTLR parser paths.
//
// These tests intentionally measure full executor entry through execution, not
// parser-only validation or AST construction. For parser-only reporting, use
// parser_benchmark_report_test.go.
//
// Run with: go test -v -run TestParserComparison ./pkg/cypher/
//
// A/B comparison: NORNICDB_PARSER=antlr go test -v -run TestParserComparison ./pkg/cypher/
package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	antlrparser "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restoreParserTypeAfter restores the parser set when tb started (for
// example by NORNICDB_PARSER=antlr) once tb ends. The parser is process-wide,
// so a test that switches it and leaves it switched would run every later
// test in the package under that parser (#956).
func restoreParserTypeAfter(tb testing.TB) {
	tb.Helper()
	previous := config.GetParserType()
	tb.Cleanup(func() { config.SetParserType(previous) })
}

func setupParserComparisonExecutor(tb testing.TB) (*StorageExecutor, context.Context) {
	tb.Helper()

	baseStore := newTestMemoryEngine(tb)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	return exec, ctx
}

func setupParserComparisonData(tb testing.TB, exec *StorageExecutor, ctx context.Context) {
	tb.Helper()

	// Seed a small but representative dataset. This keeps comparisons stable and
	// avoids "empty DB" artifacts (and avoids cross-run mutation effects).
	queries := []string{
		"CREATE (a:Person {name: 'Alice', age: 30, city: 'NYC'})",
		"CREATE (b:Person {name: 'Bob', age: 25, city: 'LA'})",
		"CREATE (c:Person {name: 'Charlie', age: 35, city: 'NYC'})",
		"CREATE (d:Person {name: 'Diana', age: 28, city: 'Chicago'})",
		"CREATE (e:Company {name: 'Acme', size: 100})",
		"CREATE (f:Company {name: 'TechCorp', size: 500})",
	}
	for _, q := range queries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(tb, err, "Setup query failed: %s", q)
	}

	// Add a relationship for relationship/optional-match queries.
	_, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
		CREATE (a)-[:KNOWS {since: 2020}]->(b)
	`, nil)
	require.NoError(tb, err)
}

// testQueries contains queries for A/B testing between parsers
var testQueries = []struct {
	name  string
	query string
}{
	// Basic MATCH
	{"simple_match", "MATCH (n) RETURN n"},
	{"match_with_label", "MATCH (n:Person) RETURN n"},
	{"match_with_properties", "MATCH (n:Person {name: 'Alice'}) RETURN n"},
	{"match_with_variable", "MATCH (p:Person) RETURN p"},

	// WHERE clauses
	{"match_where_equals", "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n"},
	{"match_where_gt", "MATCH (n:Person) WHERE n.age > 25 RETURN n"},
	{"match_where_and", "MATCH (n:Person) WHERE n.age > 25 AND n.name = 'Alice' RETURN n"},
	{"match_where_or", "MATCH (n:Person) WHERE n.age > 25 OR n.name = 'Alice' RETURN n"},
	{"match_where_is_null", "MATCH (n:Person) WHERE n.email IS NULL RETURN n"},
	{"match_where_is_not_null", "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n"},
	{"match_where_in", "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n"},
	{"match_where_starts_with", "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n"},
	{"match_where_contains", "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n"},

	// Relationships
	{"match_relationship", "MATCH (a)-[r]->(b) RETURN a, r, b"},
	{"match_typed_relationship", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b"},
	{"match_variable_length", "MATCH (a)-[*1..3]->(b) RETURN a, b"},
	{"match_reverse_relationship", "MATCH (a)<-[r]-(b) RETURN a, b"},

	// CREATE
	{"create_node", "CREATE (n:Person {name: 'Alice'})"},
	{"create_with_return", "CREATE (n:Person {name: 'Alice'}) RETURN n"},

	// MERGE
	{"merge_node", "MERGE (n:Person {name: 'Alice'})"},

	// SET
	{"set_property", "MATCH (n:Person {name: 'Alice'}) SET n.age = 30"},

	// DELETE
	{"delete_node", "MATCH (n:Person {name: 'Alice'}) DELETE n"},
	{"detach_delete", "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n"},

	// RETURN variations
	{"return_alias", "MATCH (n:Person) RETURN n.name AS name"},
	{"return_distinct", "MATCH (n:Person) RETURN DISTINCT n.city"},
	{"return_limit", "MATCH (n:Person) RETURN n LIMIT 10"},
	{"return_skip", "MATCH (n:Person) RETURN n SKIP 5"},
	{"return_order_by", "MATCH (n:Person) RETURN n ORDER BY n.name"},
	{"return_order_desc", "MATCH (n:Person) RETURN n ORDER BY n.age DESC"},

	// WITH
	{"with_simple", "MATCH (n:Person) WITH n RETURN n"},
	{"with_where", "MATCH (n:Person) WITH n WHERE n.age > 25 RETURN n"},

	// Aggregations
	{"count_all", "MATCH (n:Person) RETURN count(*)"},
	{"count_nodes", "MATCH (n:Person) RETURN count(n)"},
	{"sum", "MATCH (n:Person) RETURN sum(n.age)"},
	{"avg", "MATCH (n:Person) RETURN avg(n.age)"},

	// UNWIND
	{"unwind_list", "UNWIND [1, 2, 3] AS x RETURN x"},

	// OPTIONAL MATCH
	{"optional_match", "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m"},

	// CALL procedures
	{"call_procedure", "CALL db.labels()"},
}

func benchmarkParserValidationIsolation(b *testing.B, parserName string, validate func(string) error) {
	b.Helper()
	exec := &StorageExecutor{}

	for _, tc := range testQueries {
		b.Run(parserName+"/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.query)))
			if parserName == "Nornic" {
				if err := exec.validateSyntaxNornic(tc.query); err != nil {
					b.Fatalf("preflight validateSyntaxNornic failed for %s: %v", tc.name, err)
				}
			} else {
				if err := antlrparser.Validate(tc.query); err != nil {
					b.Fatalf("preflight antlr.Validate failed for %s: %v", tc.name, err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := validate(tc.query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkParserValidationIsolation measures the parser/validator cost in isolation,
// excluding executor routing, store setup, and query execution work.
func BenchmarkParserValidationIsolation(b *testing.B) {
	exec := &StorageExecutor{}
	benchmarkParserValidationIsolation(b, "Nornic", exec.validateSyntaxNornic)
	benchmarkParserValidationIsolation(b, "ANTLR", antlrparser.Validate)
}

// TestParserComparison runs A/B tests between the Nornic and ANTLR executor
// paths using config switching.
// Prints an integrated-flow timing comparison report at the end.
func TestParserComparison(t *testing.T) {
	restoreParserTypeAfter(t)
	type result struct {
		name       string
		nornicTime time.Duration
		antlrTime  time.Duration
		nornicErr  error
		antlrErr   error
	}
	var results []result

	measureMedian := func(t *testing.T, parserType string, query string, samples int) (time.Duration, error) {
		t.Helper()
		if samples < 1 {
			samples = 1
		}

		durations := make([]time.Duration, 0, samples)
		for i := 0; i < samples; i++ {
			config.SetParserType(parserType)
			exec, ctx := setupParserComparisonExecutor(t)
			setupParserComparisonData(t, exec, ctx)

			start := time.Now()
			_, err := exec.Execute(ctx, query, nil)
			duration := time.Since(start)
			if err != nil {
				return duration, err
			}
			durations = append(durations, duration)
		}

		sort.Slice(durations, func(i, j int) bool {
			return durations[i] < durations[j]
		})
		return durations[len(durations)/2], nil
	}

	const samplesPerParser = 5

	for idx, tc := range testQueries {
		t.Run(tc.name, func(t *testing.T) {
			var r result
			r.name = tc.name

			// IMPORTANT: Use fresh executors per parser sample.
			// Reusing the same executor biases results due to warmed caches
			// (query cache / plan cache / analyzer cache) and mutated store state.
			// Also alternate parser order per query so the first measured parser
			// does not systematically absorb one-time process jitter.
			if idx%2 == 0 {
				r.nornicTime, r.nornicErr = measureMedian(t, config.ParserTypeNornic, tc.query, samplesPerParser)
				r.antlrTime, r.antlrErr = measureMedian(t, config.ParserTypeANTLR, tc.query, samplesPerParser)
			} else {
				r.antlrTime, r.antlrErr = measureMedian(t, config.ParserTypeANTLR, tc.query, samplesPerParser)
				r.nornicTime, r.nornicErr = measureMedian(t, config.ParserTypeNornic, tc.query, samplesPerParser)
			}

			results = append(results, r)

			// Log any discrepancies
			if r.nornicErr != nil && r.antlrErr == nil {
				t.Logf("Nornic failed but ANTLR succeeded: %v", r.nornicErr)
			}
			if r.antlrErr != nil && r.nornicErr == nil {
				t.Logf("ANTLR failed but Nornic succeeded: %v", r.antlrErr)
			}
		})
	}

	// Print timing comparison report for the integrated executor path.
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("INTEGRATED EXECUTOR COMPARISON REPORT")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("\n%-30s | %-12s | %-12s | %-8s | %s\n",
		"Query", "Nornic", "ANTLR", "Ratio", "Status")
	fmt.Println(strings.Repeat("-", 80))

	var totalNornic, totalANTLR time.Duration
	for _, r := range results {
		totalNornic += r.nornicTime
		totalANTLR += r.antlrTime

		ratio := float64(r.antlrTime) / float64(r.nornicTime)
		status := "✓"
		if r.nornicErr != nil || r.antlrErr != nil {
			if r.nornicErr != nil && r.antlrErr != nil {
				status = "both failed"
			} else if r.nornicErr != nil {
				status = "nornic failed"
			} else {
				status = "antlr failed"
			}
		}

		name := r.name
		if len(name) > 30 {
			name = name[:27] + "..."
		}
		fmt.Printf("%-30s | %12s | %12s | %7.1fx | %s\n",
			name, r.nornicTime, r.antlrTime, ratio, status)
	}

	fmt.Println(strings.Repeat("-", 80))
	avgRatio := float64(totalANTLR) / float64(totalNornic)
	fmt.Printf("%-30s | %12s | %12s | %7.1fx |\n",
		"TOTAL", totalNornic, totalANTLR, avgRatio)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("\nSummary: Nornic integrated executor path is %.1fx faster than ANTLR on average\n\n", avgRatio)
}

// BenchmarkParserComparison benchmarks both executor paths using the integrated
// flow. It is not a parser-only benchmark.
func BenchmarkParserComparison(b *testing.B) {
	restoreParserTypeAfter(b)
	queries := []struct {
		name  string
		query string
	}{
		{"simple", "MATCH (n:Person) RETURN n"},
		{"medium", "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 25 RETURN n.name, m.name"},
	}

	for _, tc := range queries {
		// Benchmark with Nornic parser
		b.Run("Nornic/"+tc.name, func(b *testing.B) {
			config.SetParserType(config.ParserTypeNornic)
			exec, benchCtx := setupParserComparisonExecutor(b)
			setupParserComparisonData(b, exec, benchCtx)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = exec.Execute(benchCtx, tc.query, nil)
			}
		})

		// Benchmark with ANTLR parser
		b.Run("ANTLR/"+tc.name, func(b *testing.B) {
			config.SetParserType(config.ParserTypeANTLR)
			exec, benchCtx := setupParserComparisonExecutor(b)
			setupParserComparisonData(b, exec, benchCtx)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = exec.Execute(benchCtx, tc.query, nil)
			}
		})
	}
}

// TestParserPerformanceComparison runs a detailed integrated executor-flow
// comparison with config switching. It is not a parser-only benchmark.
func TestParserPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance comparison in short mode")
	}
	restoreParserTypeAfter(t)

	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []struct {
		name  string
		query string
	}{
		{"simple", "MATCH (n:Person) RETURN n"},
		{"medium", "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 25 RETURN n.name, m.name"},
	}

	iterations := 1000

	fmt.Println("\n=== Integrated Executor Performance Comparison ===")
	fmt.Printf("Iterations: %d\n\n", iterations)
	fmt.Printf("%-15s | %-15s | %-15s | %-10s\n", "Query Type", "Nornic (avg)", "ANTLR (avg)", "Ratio")
	fmt.Println(strings.Repeat("-", 60))

	for _, tc := range queries {
		// Time with Nornic parser
		config.SetParserType(config.ParserTypeNornic)
		nornicStart := time.Now()
		for i := 0; i < iterations; i++ {
			_, _ = exec.Execute(ctx, tc.query, nil)
		}
		nornicDuration := time.Since(nornicStart)
		nornicAvg := nornicDuration / time.Duration(iterations)

		// Time with ANTLR parser
		config.SetParserType(config.ParserTypeANTLR)
		antlrStart := time.Now()
		for i := 0; i < iterations; i++ {
			_, _ = exec.Execute(ctx, tc.query, nil)
		}
		antlrDuration := time.Since(antlrStart)
		antlrAvg := antlrDuration / time.Duration(iterations)

		ratio := float64(antlrAvg) / float64(nornicAvg)
		fmt.Printf("%-15s | %-15s | %-15s | %.2fx\n", tc.name, nornicAvg, antlrAvg, ratio)
	}
	fmt.Println()
}

// TestParserTypeSwitch verifies the config-based parser switching.
func TestParserTypeSwitch(t *testing.T) {
	restoreParserTypeAfter(t)
	// Start from Nornic, the default, whatever parser the run selected.
	config.SetParserType(config.ParserTypeNornic)
	assert.Equal(t, config.ParserTypeNornic, config.GetParserType())
	assert.True(t, config.IsNornicParser())
	assert.False(t, config.IsANTLRParser())

	// Test switching to ANTLR
	cleanup := config.WithANTLRParser()
	assert.Equal(t, config.ParserTypeANTLR, config.GetParserType())
	assert.True(t, config.IsANTLRParser())
	assert.False(t, config.IsNornicParser())

	// Test cleanup restores the previous parser
	cleanup()
	assert.Equal(t, config.ParserTypeNornic, config.GetParserType())
	assert.True(t, config.IsNornicParser())

	// Test direct set
	config.SetParserType("antlr")
	assert.Equal(t, config.ParserTypeANTLR, config.GetParserType())

	config.SetParserType("nornic")
	assert.Equal(t, config.ParserTypeNornic, config.GetParserType())

	// Test invalid value defaults to nornic
	config.SetParserType("invalid")
	assert.Equal(t, config.ParserTypeNornic, config.GetParserType())
}
