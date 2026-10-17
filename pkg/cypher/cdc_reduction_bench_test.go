package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkCDCReduction measures cdcReductionStatement over one page of
// 5,000 changes (the page a reader takes): creates, updates and deletes of
// records, topics, people and articles, and creates, updates and
// deletes of the links between records. The stages are the statement cut
// short, so each one's share is the difference to the stage before:
// "query" reads the page, "lookups" adds the per-change element reads,
// "collect" the change maps, and "full" the per-kind id sets.
func BenchmarkCDCReduction(b *testing.B) {
	exec, start := newCDCReductionBenchmark(b)
	params := map[string]interface{}{"cursor": start, "selectors": cdcReductionSelectors, "limit": 5000}
	require.Equal(b, int64(5000), cdcReduction(b, exec, start, 5000)["read"])
	lookups := cdcReductionStatement[:strings.Index(cdcReductionStatement, "WITH collect(")]
	collect := cdcReductionStatement[:strings.Index(cdcReductionStatement, "RETURN size(rows)")]
	stages := []struct{ name, query string }{
		{"query", "CALL db.cdc.query($cursor, $selectors, $limit) YIELD id, metadata, event RETURN count(*)"},
		{"lookups", lookups + "RETURN count(*)"},
		{"collect", collect + "RETURN size(rows)"},
		{"full", cdcReductionStatement},
	}
	ctx := context.Background()
	for _, stage := range stages {
		b.Run(stage.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, stage.query, params)
				if err != nil {
					b.Fatal(err)
				}
				gdsBenchmarkResult = result
			}
		})
	}
}

// newCDCReductionBenchmark writes more than 5,000 selected changes to a
// capturing store and returns the cursor before them.
func newCDCReductionBenchmark(b testing.TB) (*StorageExecutor, string) {
	b.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(b, ok)
	capture.SetChangeCapture("test", storage.ChangeCaptureDiff)
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	current, err := exec.Execute(ctx, "CALL db.cdc.current() YIELD id RETURN id", nil)
	require.NoError(b, err)
	start := current.Rows[0][0].(string)
	run := func(statement string, params map[string]interface{}) {
		_, err := exec.Execute(ctx, statement, params)
		require.NoError(b, err)
	}
	// Batches of 100 changes, as a writer's transactions would be.
	for batch := 0; batch < 12; batch++ {
		ids := make([]interface{}, 100)
		for index := range ids {
			ids[index] = fmt.Sprintf("b%d-%d", batch, index)
		}
		params := map[string]interface{}{"ids": ids, "batch": batch}
		run("UNWIND $ids AS id CREATE (:Record {id: id, origin: 'generated', updated_at: $batch, note: 'n'})", params)
		run("UNWIND $ids AS id CREATE (:Person {id: 'u' + id, origin: CASE WHEN $batch % 2 = 0 THEN 'manual' ELSE 'generated' END, match_key: id})", params)
		run("UNWIND range(0, 98) AS i MATCH (a:Record {id: $ids[i]}), (b:Record {id: $ids[i + 1]}) CREATE (a)-[:LINK {id: 'l' + $ids[i], origin: 'generated', updated_at: $batch}]->(b)", params)
		run("UNWIND $ids AS id MATCH (r:Record {id: id}) SET r.updated_at = $batch + 100 REMOVE r.note", params)
		run("UNWIND range(0, 49) AS i MATCH (u:Person {id: 'u' + $ids[i]}) SET u.match_key = 'k' + $ids[i], u.updated_at = $batch", params)
		run("UNWIND range(0, 9) AS i MERGE (c:Topic {id: 'c' + toString(i)}) SET c.updated_at = $batch", params)
		run("UNWIND range(0, 9) AS i WITH $ids[i * 10] AS id MATCH (:Record {id: id})-[l:LINK]->() DELETE l", params)
		run("UNWIND range(0, 4) AS i WITH $ids[i * 20 + 1] AS id MATCH (r:Record {id: id}) DETACH DELETE r", params)
	}
	return exec, start
}
