package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Quantified path patterns whose group predicates read the step's node and
// relationship (#1227), on a chain of 200 nodes (199 paths from the first),
// with the result cache off so each iteration runs the statement.

const quantifiedGroupBenchChain = 200

func newQuantifiedGroupBenchExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "qpp_bench"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(0, $last) AS i CREATE (:N {id: i, k: i % 5})", map[string]interface{}{"last": int64(quantifiedGroupBenchChain - 1)}); err != nil {
		b.Fatal(err)
	}
	if _, err := exec.Execute(ctx, "UNWIND range(0, $last) AS i MATCH (a:N {id: i}), (c:N {id: i + 1}) CREATE (a)-[:R {w: 1, id: 'r' + toString(i)}]->(c)", map[string]interface{}{"last": int64(quantifiedGroupBenchChain - 2)}); err != nil {
		b.Fatal(err)
	}
	return exec
}

// benchmarkStatementCount runs query b.N times and requires its single
// count to be want, checked once before the timer starts. A build that
// rejects the statement skips it: before #1211, m[r.id] in a group
// predicate was a SyntaxError.
func benchmarkStatementCount(b *testing.B, exec *StorageExecutor, query string, params map[string]interface{}, want int64) {
	b.Helper()
	ctx := context.Background()
	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		b.Skipf("this build rejects %s: %v", query, err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != want {
		b.Fatalf("%s: got %v, want [[%d]]", query, result.Rows, want)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuantifiedGroupPredicate_PropertyFilter(b *testing.B) {
	exec := newQuantifiedGroupBenchExecutor(b)
	benchmarkStatementCount(b, exec,
		"MATCH p = (a:N {id: 0})((x)-[r:R]->(y) WHERE r.w > 0 AND x.k IN $ks)+(z) RETURN count(p) AS c",
		map[string]interface{}{"ks": []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4)}},
		quantifiedGroupBenchChain-1)
}

func BenchmarkQuantifiedGroupPredicate_StepSubscript(b *testing.B) {
	exec := newQuantifiedGroupBenchExecutor(b)
	benchmarkStatementCount(b, exec,
		"WITH $m AS m MATCH p = (a:N {id: 0})((x)-[r:R]->(y) WHERE m[r.id] IS NULL)+(z) RETURN count(p) AS c",
		map[string]interface{}{"m": map[string]interface{}{"none": true}},
		quantifiedGroupBenchChain-1)
}

func BenchmarkQuantifiedGroupPredicate_LocalScopes(b *testing.B) {
	exec := newQuantifiedGroupBenchExecutor(b)
	benchmarkStatementCount(b, exec,
		"MATCH p = (a:N {id: 0})((x)-[r:R]->(y) WHERE all(k IN $ks WHERE k >= 0) AND reduce(acc = 0, v IN [r.w, x.k] | acc + v) > 0)+(z) RETURN count(p) AS c",
		map[string]interface{}{"ks": []interface{}{int64(0), int64(1), int64(2)}},
		quantifiedGroupBenchChain-1)
}

// BenchmarkLocalScopeExpression: a WHERE whose quantifier binds a local
// read by subscript, over 10k rows.
func BenchmarkLocalScopeExpression(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "local_scope_bench"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	benchmarkStatementCount(b, exec,
		"UNWIND range(1, 10000) AS i WITH i, [[i > 0], [true]] AS list WHERE all(r IN list WHERE r[0]) RETURN count(*) AS c",
		nil, 10000)
}
