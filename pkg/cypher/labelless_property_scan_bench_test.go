package cypher

// Benchmarks for #824: a property match without a label (graphify's edge push,
// MATCH (a {id: $src})) scans every node; only the cost per scanned node is
// under the engine's control.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupLabellessPropertyScanBench(b *testing.B, nodes int) (*StorageExecutor, context.Context) {
	b.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench824"))
	ctx := context.Background()
	run := func(query string, params map[string]interface{}) {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(query, err)
		}
	}
	for _, label := range []string{"Code", "Document", "Entity"} {
		run("CREATE INDEX FOR (n:"+label+") ON (n.id)", nil)
	}
	for start := 0; start < nodes; start += 2000 {
		end := min(start+2000, nodes)
		run(`UNWIND range($a, $b) AS i
			CREATE (n {id: 'n' + toString(i), name: 'node ' + toString(i), body: 'some body text for node ' + toString(i)})
			WITH n, i SET n:Code`, map[string]interface{}{"a": int64(start), "b": int64(end - 1)})
	}
	// One node under a label without an index on id: the per-label indexes
	// can't answer a label-less lookup, so it scans.
	run("CREATE (:Other {id: 'other'})", nil)
	return exec, ctx
}

func BenchmarkLabellessPropertyScan(b *testing.B) {
	for _, nodes := range []int{5000, 20000} {
		exec, ctx := setupLabellessPropertyScanBench(b, nodes)
		for _, tc := range []struct{ name, query string }{
			{"labelless_hit", "MATCH (a {id: $id}) RETURN a.id"},
			{"labelless_miss", "MATCH (a {id: $missing}) RETURN a.id"},
			{"labelled_unindexed", "MATCH (a:Code {name: $name}) RETURN a.id"},
		} {
			b.Run(fmt.Sprintf("%s/n=%d", tc.name, nodes), func(b *testing.B) {
				params := map[string]interface{}{"id": "n7", "missing": "none", "name": "node 7"}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					result, err := exec.Execute(ctx, tc.query, params)
					if err != nil {
						b.Fatal(err)
					}
					if tc.name != "labelless_miss" && len(result.Rows) != 1 {
						b.Fatalf("rows = %v", result.Rows)
					}
				}
			})
		}
	}
}
