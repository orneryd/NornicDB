package cypher

// Benchmarks for NornicDB #562: seeding a bound-start (or bound-end)
// MATCH traversal from its already-bound node instead of expanding the
// pattern over the whole store and joining afterward.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BoundHop isolates #562 with a label-anchored first clause; ThreeMatch
// preserves the downstream exposure path, whose label-less first scan also
// measures #561. Each shape retains its own graph and decoy relationship type.
func BenchmarkIssue562SinkProbe(b *testing.B) {
	for _, shape := range []string{"BoundHop", "ThreeMatch"} {
		for _, decoyCount := range []int{0, 2000, 20000} {
			b.Run(fmt.Sprintf("%s/%dDecoys", shape, decoyCount), func(b *testing.B) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
				ctx := context.Background()
				var q string
				var params map[string]any
				if shape == "BoundHop" {
					seedSinkProbeGraph(b, exec, decoyCount)
					q = `MATCH (reached:Function {uid:'fn'}) MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN ['QUERIES_TABLE'] RETURN type(sinkRel) AS t`
				} else {
					run := func(q string, p map[string]any) {
						_, err := exec.Execute(ctx, q, p)
						require.NoError(b, err, q)
					}
					run(`CREATE (:Function {uid: 'src'})`, nil)
					run(`CREATE (:Function {uid: 'mid'})`, nil)
					run(`CREATE (:Function {uid: 'leaf'})`, nil)
					run(`CREATE (:SqlTable {name: 'orders'})`, nil)
					run(`MATCH (a:Function {uid:'src'}), (b:Function {uid:'mid'}) CREATE (a)-[:CALLS]->(b)`, nil)
					run(`MATCH (a:Function {uid:'mid'}), (b:Function {uid:'leaf'}) CREATE (a)-[:CALLS]->(b)`, nil)
					run(`MATCH (a:Function {uid:'leaf'}), (t:SqlTable {name:'orders'}) CREATE (a)-[:QUERIES_TABLE]->(t)`, nil)
					for start := 0; start < decoyCount; start += decoySeedBatchSize {
						end := start + decoySeedBatchSize
						if end > decoyCount {
							end = decoyCount
						}
						run(`UNWIND range($start, $end - 1) AS k CREATE (:X {uid: 'x' + toString(k)})-[:QUERIES_TABLE]->(:Y {uid: 'y' + toString(k)})`,
							map[string]any{"start": int64(start), "end": int64(end)})
					}

					q = `MATCH (src) WHERE coalesce(src.id, src.uid) = $source_entity_id
MATCH path = (src)-[:CALLS*0..4]->(reached)
MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN $sink_rels
RETURN nodes(path) AS chain, type(sinkRel) AS sink_rel, sinkNode AS sink_node, labels(sinkNode) AS sink_labels, length(path) AS depth
LIMIT $limit`
					params = map[string]any{"source_entity_id": "src", "sink_rels": []interface{}{"QUERIES_TABLE"}, "limit": int64(25)}

				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res, err := exec.Execute(ctx, q, params)
					if err != nil {
						b.Fatal(err)
					}
					if len(res.Rows) != 1 {
						b.Fatalf("expected 1 row, got %d", len(res.Rows))
					}
				}
			})
		}
	}
}
