package cypher

// Benchmarks for NornicDB #562: seeding a bound-start (or bound-end)
// MATCH traversal from its already-bound node instead of expanding the
// pattern over the whole store and joining afterward.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func benchmarkSinkProbeBoundHop(b *testing.B, decoyCount int) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
	// Result cache off, so each iteration measures execution, not a cache hit.
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	seedSinkProbeGraph(b, exec, decoyCount)
	ctx := context.Background()
	q := `MATCH (reached:Function {uid:'fn'}) MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN ['QUERIES_TABLE'] RETURN type(sinkRel) AS t`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, q, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != 1 {
			b.Fatalf("expected 1 row, got %d", len(res.Rows))
		}
	}
}

func BenchmarkIssue562SinkProbeBoundHop_0Decoys(b *testing.B)    { benchmarkSinkProbeBoundHop(b, 0) }
func BenchmarkIssue562SinkProbeBoundHop_2000Decoys(b *testing.B) { benchmarkSinkProbeBoundHop(b, 2000) }
func BenchmarkIssue562SinkProbeBoundHop_20000Decoys(b *testing.B) {
	benchmarkSinkProbeBoundHop(b, 20000)
}

// benchmarkSinkProbeThreeMatch mirrors a real downstream exposure-path sink
// probe verbatim: MATCH (src) WHERE ... MATCH path=(src)-[:CALLS*0..4]->(reached)
// MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN [...] LIMIT.
// Its first clause, "MATCH (src) WHERE coalesce(...)", is unlabeled and
// therefore also pays the separate label-less scan cost tracked in #561 (a
// label-less MATCH streams the whole store); this benchmark's growth with
// decoyCount is not attributable to #562. BenchmarkIssue562SinkProbeBoundHop
// above isolates #562, since its first clause is label-anchored.
func benchmarkSinkProbeThreeMatch(b *testing.B, decoyCount int) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
	// Result cache off, so each iteration measures execution, not a cache hit.
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
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
	q := `MATCH (src) WHERE coalesce(src.id, src.uid) = $source_entity_id
MATCH path = (src)-[:CALLS*0..4]->(reached)
MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN $sink_rels
RETURN nodes(path) AS chain, type(sinkRel) AS sink_rel, sinkNode AS sink_node, labels(sinkNode) AS sink_labels, length(path) AS depth
LIMIT $limit`
	params := map[string]any{"source_entity_id": "src", "sink_rels": []interface{}{"QUERIES_TABLE"}, "limit": int64(25)}

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
}

func BenchmarkIssue562SinkProbeThreeMatch_0Decoys(b *testing.B) { benchmarkSinkProbeThreeMatch(b, 0) }
func BenchmarkIssue562SinkProbeThreeMatch_2000Decoys(b *testing.B) {
	benchmarkSinkProbeThreeMatch(b, 2000)
}
func BenchmarkIssue562SinkProbeThreeMatch_20000Decoys(b *testing.B) {
	benchmarkSinkProbeThreeMatch(b, 20000)
}
