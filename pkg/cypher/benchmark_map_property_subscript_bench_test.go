package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A subscript on a map variable's property (v.h[k]) against the same map
// bound whole (h[k]), in a MATCH's own WHERE and in a per-row CALL
// subquery's MATCH WHERE: 200 nodes, 600 relationships, a 50-key map, the
// result cache off (#1227). The map_variable form is the cost to match.

func newMapPropertySubscriptBenchExecutor(b *testing.B) (*StorageExecutor, map[string]interface{}) {
	b.Helper()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "map_property_bench"), 0, 0)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(1, 200) AS i CREATE (:N {id: 'n' + i, g: i % 10})", nil); err != nil {
		b.Fatal(err)
	}
	rels := make([]interface{}, 0, 600)
	for i := 1; i <= 200; i++ {
		for j := 1; j <= 3; j++ {
			rels = append(rels, map[string]interface{}{"a": fmt.Sprintf("n%d", i), "b": fmt.Sprintf("n%d", (i*7+j*13)%200+1), "id": fmt.Sprintf("r%d_%d", i, j)})
		}
	}
	if _, err := exec.Execute(ctx, "UNWIND $rels AS x MATCH (a:N {id: x.a}), (c:N {id: x.b}) CREATE (a)-[:R {id: x.id}]->(c)", map[string]interface{}{"rels": rels}); err != nil {
		b.Fatal(err)
	}
	keys := make([]interface{}, 0, 50)
	for i := 1; i <= 50; i++ {
		keys = append(keys, fmt.Sprintf("r%d_1", i))
	}
	return exec, map[string]interface{}{"keys": keys}
}

func benchmarkMapPropertySubscript(b *testing.B, variableTail, propertyTail string) {
	exec, params := newMapPropertySubscriptBenchExecutor(b)
	forms := map[string]string{
		"map_variable": "WITH apoc.map.fromPairs([x IN $keys | [x, true]]) AS h MATCH (s:N) WHERE s.g = 1 " + variableTail,
		"map_property": "WITH {h: apoc.map.fromPairs([x IN $keys | [x, true]])} AS v MATCH (s:N) WHERE s.g = 1 " + propertyTail,
	}
	result, err := exec.Execute(context.Background(), forms["map_variable"], params)
	if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		b.Fatalf("map_variable: %v %v", result, err)
	}
	want, _ := result.Rows[0][0].(int64)
	for _, name := range []string{"map_variable", "map_property"} {
		b.Run(name, func(b *testing.B) {
			// A build that answers the map_property form wrongly (before
			// #1227 a variable-length or 2-hop any() over v.h[k]) skips it.
			if got, err := exec.Execute(context.Background(), forms[name], params); err == nil && len(got.Rows) == 1 && len(got.Rows[0]) == 1 && got.Rows[0][0] != want {
				b.Skipf("this build answers %v, want %d", got.Rows, want)
			}
			benchmarkStatementCount(b, exec, forms[name], params, want)
		})
	}
}

func BenchmarkMapPropertySubscript_MatchWhere(b *testing.B) {
	benchmarkMapPropertySubscript(b,
		"MATCH (s)-[r:R]->(e)-[r2:R]->(f) WHERE any(k IN [r.id, r2.id] WHERE h[k] IS NOT NULL) RETURN count(*) AS c",
		"MATCH (s)-[r:R]->(e)-[r2:R]->(f) WHERE any(k IN [r.id, r2.id] WHERE v.h[k] IS NOT NULL) RETURN count(*) AS c")
}

func BenchmarkMapPropertySubscript_CallSubquery(b *testing.B) {
	benchmarkMapPropertySubscript(b,
		"MATCH (s)-[r:R]->(e) CALL (r, h) { MATCH (e2)-[r2:R]->() WHERE e2.id = endNode(r).id AND h[r2.id] IS NULL RETURN count(*) AS n } RETURN sum(n) AS c",
		"MATCH (s)-[r:R]->(e) CALL (r, v) { MATCH (e2)-[r2:R]->() WHERE e2.id = endNode(r).id AND v.h[r2.id] IS NULL RETURN count(*) AS n } RETURN sum(n) AS c")
}
