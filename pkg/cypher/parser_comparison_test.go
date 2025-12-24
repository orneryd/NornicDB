// Package cypher provides parser comparison tests between Nornic and ANTLR parsers.
//
// Run with: go test -v -run TestParserComparison ./pkg/cypher/
//
// A/B comparison: NORNICDB_PARSER=antlr go test -v -run TestParserComparison ./pkg/cypher/
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupParserComparisonExecutor(tb testing.TB) (*StorageExecutor, context.Context) {
	tb.Helper()

	baseStore := storage.NewMemoryEngine()
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

// TestParserComparison runs A/B tests between Nornic and ANTLR parsers
// using the integrated executor flow with config switching.
// Prints a timing comparison report at the end.
func TestParserComparison(t *testing.T) {
	type result struct {
		name       string
		nornicTime time.Duration
		antlrTime  time.Duration
		nornicErr  error
		antlrErr   error
	}
	var results []result

	runOnce := func(t *testing.T, parserType string, query string) (time.Duration, error) {
		t.Helper()

		config.SetParserType(parserType)
		exec, ctx := setupParserComparisonExecutor(t)
		setupParserComparisonData(t, exec, ctx)

		start := time.Now()
		_, err := exec.Execute(ctx, query, nil)
		return time.Since(start), err
	}

	for _, tc := range testQueries {
		t.Run(tc.name, func(t *testing.T) {
			var r result
			r.name = tc.name

			// IMPORTANT: Use fresh executors per parser run.
			// Reusing the same executor biases results due to warmed caches
			// (query cache / plan cache / analyzer cache) and mutated store state.
			r.nornicTime, r.nornicErr = runOnce(t, config.ParserTypeNornic, tc.query)
			r.antlrTime, r.antlrErr = runOnce(t, config.ParserTypeANTLR, tc.query)

			// Reset to default
			config.SetParserType(config.ParserTypeNornic)

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

	// Print timing comparison report
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("PARSER COMPARISON REPORT")
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
	fmt.Printf("\nSummary: Nornic parser is %.1fx faster than ANTLR on average\n\n", avgRatio)
}

// BenchmarkParserComparison benchmarks both parsers using the integrated executor flow.
func BenchmarkParserComparison(b *testing.B) {
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

	// Reset to default
	config.SetParserType(config.ParserTypeNornic)
}

// TestParserPerformanceComparison runs a detailed performance comparison
// using the integrated executor flow with config switching.
func TestParserPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance comparison in short mode")
	}

	baseStore := storage.NewMemoryEngine()

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

	fmt.Println("\n=== Parser Performance Comparison (Integrated Flow) ===")
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

	// Reset to default
	config.SetParserType(config.ParserTypeNornic)
}

// TestParserTypeSwitch verifies the config-based parser switching.
func TestParserTypeSwitch(t *testing.T) {
	// Test default is nornic
	assert.Equal(t, config.ParserTypeNornic, config.GetParserType())
	assert.True(t, config.IsNornicParser())
	assert.False(t, config.IsANTLRParser())

	// Test switching to ANTLR
	cleanup := config.WithANTLRParser()
	assert.Equal(t, config.ParserTypeANTLR, config.GetParserType())
	assert.True(t, config.IsANTLRParser())
	assert.False(t, config.IsNornicParser())

	// Test cleanup restores default
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
