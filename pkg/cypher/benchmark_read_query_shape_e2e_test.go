package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fixedBenchmarkQueryEmbedder struct {
	vec []float32
}

func (f fixedBenchmarkQueryEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	out := make([]float32, len(f.vec))
	copy(out, f.vec)
	return out, nil
}

func (f fixedBenchmarkQueryEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

func TestBenchmarkReadShape_VectorDeepHop_E2E(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(fixedBenchmarkQueryEmbedder{vec: []float32{1.0, 0.0, 0.0}})
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx_original_text', 'OriginalText', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)

	upsertOriginalQuery := `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
ON CREATE SET o.originalText = row.originalText,
              o.embedding = row.embedding,
              o.benchmarkRun = row.runID
ON MATCH SET  o.originalText = row.originalText,
              o.embedding = row.embedding
RETURN count(o) AS prepared`

	_, err = exec.Execute(ctx, upsertOriginalQuery, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"textKey":      "benchhop-000",
				"originalText": "get it delivered",
				"embedding":    []interface{}{1.0, 0.0, 0.0},
				"runID":        "bench-e2e-v1",
			},
			{
				"textKey":      "benchhop-nohop",
				"originalText": "noise candidate",
				"embedding":    []interface{}{0.9, 0.1, 0.0},
				"runID":        "bench-e2e-v1",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	hops := make([]map[string]interface{}, 0, 6)
	for depth := 1; depth <= 6; depth++ {
		hops = append(hops, map[string]interface{}{
			"hopId": fmt.Sprintf("benchhop-000:%d", depth),
			"runID": "bench-e2e-v1",
		})
	}
	upsertHopsQuery := `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared`
	_, err = exec.Execute(ctx, upsertHopsQuery, map[string]interface{}{"hops": hops})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind merge batch hot path")

	linkHopsQuery := `
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
	_, err = exec.Execute(ctx, linkHopsQuery, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "benchhop-000"},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindFixedChainLinkBatch, "expected unwind fixed-chain link hot path")

	for depth := 1; depth <= 6; depth++ {
		query := fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $topK, $text)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP*1..%d]->(:BenchmarkHop)
WITH node, score, max(length(p)) AS maxDepth
RETURN node.textKey AS textKey, maxDepth, score
ORDER BY score DESC
LIMIT $topK
`, depth)

		res, qErr := exec.Execute(ctx, query, map[string]interface{}{
			"topK": int64(5),
			"text": "get it delivered",
		})
		require.NoError(t, qErr, "depth=%d query failed", depth)
		require.True(t, exec.LastHotPathTrace().CallTailPipelineUsed, "depth=%d should use the pipeline tail", depth)
		require.NotNil(t, res, "depth=%d result nil", depth)
		require.NotEmpty(t, res.Rows, "depth=%d should return traversal rows", depth)

		var sawBenchhop bool
		for _, row := range res.Rows {
			require.Len(t, row, 3)
			textKey, _ := row[0].(string)
			if textKey != "benchhop-000" {
				continue
			}
			sawBenchhop = true
			require.EqualValues(t, depth, toInt64ForTest(t, row[1]), "depth=%d maxDepth mismatch", depth)
		}
		require.True(t, sawBenchhop, "depth=%d should include benchhop-000, rows=%v", depth, res.Rows)
	}
}
