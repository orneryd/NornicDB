package cypher

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkRelationshipOwnerRows measures relationship syntax on the paths
// that run per row or per invalidation, through public APIs that compile
// unchanged on the unmodified bases. The result cache is off, so each
// iteration executes the statement.
func BenchmarkRelationshipOwnerRows(b *testing.B) {
	ctx := context.Background()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, time.Minute)
	const people = 64
	for i := 0; i < people; i++ {
		statement := fmt.Sprintf("CREATE (:P {id: %d})-[:A]->(:Q {id: %d}), (:P2 {id: %d})-[:C]->(:Q2 {id: %d})", i, i, i, i)
		if _, err := exec.Execute(ctx, statement, nil); err != nil {
			b.Fatal(err)
		}
	}
	run := func(name, query string, want int64) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != strconv.FormatInt(want, 10) {
					b.Fatalf("%s: rows %v", name, result.Rows)
				}
			}
		})
	}
	run("match_type_alternatives", "MATCH (a:P)-[:A|B|C]->(b) RETURN count(b) AS n", people)
	run("bound_count_per_row", "MATCH (a:P) WITH a, COUNT { (a)-[:A|B]->(:Q) } AS c RETURN sum(c) AS n", people)
	run("bound_pattern_count_per_row", "MATCH (a:P) WITH a, COUNT { (a)-[:A]->(q:Q {id: a.id}) } AS c RETURN sum(c) AS n", people)
	run("exists_per_row", "MATCH (a:P2) WHERE EXISTS { (a)-[:C]->(:Q2) } RETURN count(a) AS n", people)
	run("exists_unanchored_per_row", "MATCH (a:P) WHERE EXISTS { MATCH (s:Q {id: a.id}) WHERE any(i IN range(0, 1) WHERE s.id >= i) } RETURN count(a) AS n", people)

	query := "MATCH ()-[:A|`B|C`]->() RETURN count(*) AS n"
	result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(1)}}}
	b.Run("publication_then_invalidation_64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cache := NewSmartQueryCache(128)
			prefix := query + "_" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_"
			for entry := 0; entry < 64; entry++ {
				cache.Put(prefix+strconv.Itoa(entry), nil, result, time.Minute)
			}
			cache.InvalidateLabels([]string{"Unrelated"})
		}
	})
}

// BenchmarkQuotedRewriteControls are first rewrites of distinct statements.
// quoted_node_uncached rewrites a quoted node variable, which the bases
// already rewrite correctly: base and head do the same work for it.
func BenchmarkQuotedRewriteControls(b *testing.B) {
	b.Run("quoted_node_uncached", func(b *testing.B) {
		prefix := "MATCH (`n n`)-->() RETURN `n n`.x AS c_" + strconv.Itoa(b.N) + "_"
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalizeQuotedVariables(prefix + strconv.Itoa(i))
		}
	})
}
