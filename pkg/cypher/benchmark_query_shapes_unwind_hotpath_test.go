package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMergeBatch_OriginalTextUpsert_OnCreateOnMatch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

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

	firstRows := []map[string]interface{}{
		{
			"textKey":      "benchhop-000",
			"originalText": "get it delivered",
			"page":         "https://bench.local/a",
			"pagePath":     "/a",
			"trackingId":   "trk-a",
			"textKey128":   "benchhop-000-128",
			"runID":        "run-v1",
			"now":          "2026-03-31T20:00:00Z",
		},
		{
			"textKey":      "benchhop-001",
			"originalText": "ready now",
			"page":         "https://bench.local/b",
			"pagePath":     "/b",
			"trackingId":   "trk-b",
			"textKey128":   "benchhop-001-128",
			"runID":        "run-v1",
			"now":          "2026-03-31T20:00:00Z",
		},
	}
	res, err := exec.Execute(ctx, query, map[string]interface{}{"rows": firstRows})
	require.NoError(t, err)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	secondRows := []map[string]interface{}{
		{
			"textKey":      "benchhop-000",
			"originalText": "get it delivered now",
			"page":         "https://bench.local/a2",
			"pagePath":     "/a2",
			"trackingId":   "trk-a2",
			"textKey128":   "benchhop-000-128-new",
			"runID":        "run-v2",
			"now":          "2026-03-31T21:00:00Z",
		},
	}
	res, err = exec.Execute(ctx, query, map[string]interface{}{"rows": secondRows})
	require.NoError(t, err)
	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("OriginalText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	byKey := map[string]*storage.Node{}
	for _, n := range nodes {
		byKey[n.Properties["textKey"].(string)] = n
	}
	n0 := byKey["benchhop-000"]
	require.NotNil(t, n0)
	require.Equal(t, "get it delivered now", n0.Properties["originalText"])
	require.Equal(t, "https://bench.local/a2", n0.Properties["page"])
	require.Equal(t, "/a2", n0.Properties["pagePath"])
	require.Equal(t, "trk-a2", n0.Properties["trackingId"])
	require.Equal(t, "benchhop-000-128-new", n0.Properties["textKey128"])
	require.Equal(t, "run-v1", n0.Properties["benchmarkRun"], "ON MATCH must not overwrite benchmarkRun")
	require.Equal(t, "2026-03-31T20:00:00Z", n0.Properties["createdAt"], "ON MATCH must not overwrite createdAt")
	require.Equal(t, "2026-03-31T21:00:00Z", n0.Properties["updatedAt"])
}

func TestUnwindBenchHopLinkBatch_QueryShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
RETURN count(o) AS prepared
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "benchhop-000"},
			{"textKey": "benchhop-001"},
		},
	})
	require.NoError(t, err)

	hops := make([]map[string]interface{}, 0, 12)
	for i := 0; i < 2; i++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", i, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}
	_, err = exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)

	linkQuery := `
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

	rows := []map[string]interface{}{
		{"textKey": "benchhop-000"},
		{"textKey": "benchhop-001"},
	}
	res, err := exec.Execute(ctx, linkQuery, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 12, res.Stats.RelationshipsCreated)

	res, err = exec.Execute(ctx, linkQuery, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 0, res.Stats.RelationshipsCreated, "re-running should be idempotent")
}

func TestUnwindBenchHopLinkBatch_QueryShape_RootIDRootKey(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
RETURN count(o) AS prepared
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "benchhop-000"},
			{"textKey": "benchhop-001"},
		},
	})
	require.NoError(t, err)

	originals, err := store.GetNodesByLabel("OriginalText")
	require.NoError(t, err)
	require.Len(t, originals, 2)
	idByTextKey := map[string]string{}
	for _, n := range originals {
		idByTextKey[fmt.Sprintf("%v", n.Properties["textKey"])] = string(n.ID)
	}

	hops := make([]map[string]interface{}, 0, 12)
	for i := 0; i < 2; i++ {
		rootKey := fmt.Sprintf("benchhop-%03d", i)
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("%s:%d", rootKey, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}
	_, err = exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)

	linkQuery := `
UNWIND $rows AS row
MATCH (o:OriginalText)
WHERE elementId(o) = row.rootID
MATCH (h1:BenchmarkHop {hopId: row.rootKey + ':1'})
MATCH (h2:BenchmarkHop {hopId: row.rootKey + ':2'})
MATCH (h3:BenchmarkHop {hopId: row.rootKey + ':3'})
MATCH (h4:BenchmarkHop {hopId: row.rootKey + ':4'})
MATCH (h5:BenchmarkHop {hopId: row.rootKey + ':5'})
MATCH (h6:BenchmarkHop {hopId: row.rootKey + ':6'})
MERGE (o)-[:BENCH_HOP]->(h1)
MERGE (h1)-[:BENCH_HOP]->(h2)
MERGE (h2)-[:BENCH_HOP]->(h3)
MERGE (h3)-[:BENCH_HOP]->(h4)
MERGE (h4)-[:BENCH_HOP]->(h5)
MERGE (h5)-[:BENCH_HOP]->(h6)
RETURN count(o) AS prepared`

	rows := []map[string]interface{}{
		{"rootID": "4:nornicdb:" + idByTextKey["benchhop-000"], "rootKey": "benchhop-000"},
		{"rootID": "4:nornicdb:" + idByTextKey["benchhop-001"], "rootKey": "benchhop-001"},
	}
	res, err := exec.Execute(ctx, linkQuery, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 12, res.Stats.RelationshipsCreated)

	res, err = exec.Execute(ctx, linkQuery, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 0, res.Stats.RelationshipsCreated, "re-running should be idempotent")
}

func TestUnwindFixedChainLinkBatch_QueryShape_VariableDepth(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (src:OriginalText {textKey: row.textKey})
RETURN count(src) AS prepared
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "chain-000"},
		},
	})
	require.NoError(t, err)

	hops := []map[string]interface{}{
		{"hopId": "chain-000:1", "runID": "run-1"},
		{"hopId": "chain-000:2", "runID": "run-1"},
		{"hopId": "chain-000:3", "runID": "run-1"},
	}
	_, err = exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (node:BenchmarkHop {hopId: hop.hopId})
SET node.benchmarkRun = hop.runID
RETURN count(node) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)

	query := `
UNWIND $rows AS row
MATCH (o:OriginalText {textKey: row.textKey})
MATCH (x1:BenchmarkHop {hopId: row.textKey + ':1'})
MATCH (x2:BenchmarkHop {hopId: row.textKey + ':2'})
MATCH (x3:BenchmarkHop {hopId: row.textKey + ':3'})
MERGE (o)-[:BENCH_HOP]->(x1)
MERGE (x1)-[:BENCH_HOP]->(x2)
MERGE (x2)-[:BENCH_HOP]->(x3)
RETURN count(o) AS prepared`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "chain-000"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")
	require.NotNil(t, res.Stats)
	require.EqualValues(t, 3, res.Stats.RelationshipsCreated)
}
