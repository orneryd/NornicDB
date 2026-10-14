package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkUnwindMergeBatch_OriginalTextUpsert(b *testing.B) {
	base := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	rows := make([]map[string]interface{}, 0, 12)
	for i := 0; i < 12; i++ {
		rows = append(rows, map[string]interface{}{
			"textKey":      fmt.Sprintf("benchhop-%03d", i),
			"textKey128":   fmt.Sprintf("benchhop-%03d-128", i),
			"originalText": fmt.Sprintf("benchmark text %d", i),
			"page":         "https://bench.local/deep-hop",
			"pagePath":     "/deep-hop",
			"trackingId":   fmt.Sprintf("bench-track-%03d", i),
			"runID":        "bench-deep-hop-v1",
			"now":          "2026-03-31T20:00:00Z",
		})
	}

	query := `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
ON CREATE SET o.originalText = row.originalText,
              o.page = row.page,
              o.pagePath = row.pagePath,
              o.trackingId = row.trackingId,
              o.textKey128 = row.textKey128,
              o.benchmarkRun = row.runID,
              o.createdAt = row.now,
              o.updatedAt = row.now
ON MATCH SET  o.originalText = row.originalText,
              o.page = row.page,
              o.pagePath = row.pagePath,
              o.trackingId = row.trackingId,
              o.textKey128 = row.textKey128,
              o.updatedAt = row.now
RETURN count(o) AS prepared`

	params := map[string]interface{}{"rows": rows}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}

func BenchmarkUnwindBenchHopLinkBatch(b *testing.B) {
	base := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	originalRows := make([]map[string]interface{}, 0, 12)
	for i := 0; i < 12; i++ {
		originalRows = append(originalRows, map[string]interface{}{
			"textKey": fmt.Sprintf("benchhop-%03d", i),
		})
	}
	if _, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
RETURN count(o) AS prepared
`, map[string]interface{}{"rows": originalRows}); err != nil {
		b.Fatalf("seed originals failed: %v", err)
	}

	hops := make([]map[string]interface{}, 0, 12*6)
	for i := 0; i < 12; i++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", i, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}
	if _, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops}); err != nil {
		b.Fatalf("seed hops failed: %v", err)
	}

	query := `
UNWIND $rows AS row
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (h1:BenchmarkHop {hopId: row.textKey + ':1'})
MATCH (h2:BenchmarkHop {hopId: row.textKey + ':2'})
MATCH (h3:BenchmarkHop {hopId: row.textKey + ':3'})
MATCH (h4:BenchmarkHop {hopId: row.textKey + ':4'})
MATCH (h5:BenchmarkHop {hopId: row.textKey + ':5'})
MATCH (h6:BenchmarkHop {hopId: row.textKey + ':6'})
MERGE (o)-[:BENCH_HOP]->(h1)
MERGE (h1)-[:BENCH_HOP]->(h2)
MERGE (h2)-[:BENCH_HOP]->(h3)
MERGE (h3)-[:BENCH_HOP]->(h4)
MERGE (h4)-[:BENCH_HOP]->(h5)
MERGE (h5)-[:BENCH_HOP]->(h6)
RETURN count(o) AS prepared`
	params := map[string]interface{}{"rows": originalRows}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}
