package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallTailSetBasedSupportsVariableLengthMaxLengthAggregation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx_original_text', 'OriginalText', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
SET o.originalText = row.originalText,
    o.embedding = row.embedding
RETURN count(o) AS prepared
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"textKey":      "benchhop-000",
				"originalText": "get it delivered",
				"embedding":    []interface{}{1.0, 0.0, 0.0},
			},
			{
				"textKey":      "benchhop-001",
				"originalText": "ship this order",
				"embedding":    []interface{}{0.85, 0.15, 0.0},
			},
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
RETURN count(h) AS prepared
`, map[string]interface{}{
		"hops": []map[string]interface{}{
			{"hopId": "benchhop-000:1"},
			{"hopId": "benchhop-000:2"},
			{"hopId": "benchhop-000:3"},
			{"hopId": "benchhop-000:4"},
			{"hopId": "benchhop-000:5"},
			{"hopId": "benchhop-000:6"},
			{"hopId": "benchhop-001:1"},
			{"hopId": "benchhop-001:2"},
			{"hopId": "benchhop-001:3"},
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (o:OriginalText {textKey: 'benchhop-000'})
MATCH (h1:BenchmarkHop {hopId: 'benchhop-000:1'})
MATCH (h2:BenchmarkHop {hopId: 'benchhop-000:2'})
MATCH (h3:BenchmarkHop {hopId: 'benchhop-000:3'})
MATCH (h4:BenchmarkHop {hopId: 'benchhop-000:4'})
MATCH (h5:BenchmarkHop {hopId: 'benchhop-000:5'})
MATCH (h6:BenchmarkHop {hopId: 'benchhop-000:6'})
MERGE (o)-[:BENCH_HOP]->(h1)
MERGE (h1)-[:BENCH_HOP]->(h2)
MERGE (h2)-[:BENCH_HOP]->(h3)
MERGE (h3)-[:BENCH_HOP]->(h4)
MERGE (h4)-[:BENCH_HOP]->(h5)
MERGE (h5)-[:BENCH_HOP]->(h6)
RETURN count(o) AS prepared
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (o:OriginalText {textKey: 'benchhop-001'})
MATCH (h1:BenchmarkHop {hopId: 'benchhop-001:1'})
MATCH (h2:BenchmarkHop {hopId: 'benchhop-001:2'})
MATCH (h3:BenchmarkHop {hopId: 'benchhop-001:3'})
MERGE (o)-[:BENCH_HOP]->(h1)
MERGE (h1)-[:BENCH_HOP]->(h2)
MERGE (h2)-[:BENCH_HOP]->(h3)
RETURN count(o) AS prepared
`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
CALL db.index.vector.queryNodes('idx_original_text', 5, [1.0, 0.0, 0.0])
YIELD node, score
MATCH p = (node)-[:BENCH_HOP*1..6]->(:BenchmarkHop)
WITH node, score, max(length(p)) AS maxDepth
RETURN node.textKey AS textKey, maxDepth, score
ORDER BY score DESC
LIMIT 5
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "maxDepth", "score"}, res.Columns)
	require.Len(t, res.Rows, 2)

	firstKey, ok := res.Rows[0][0].(string)
	require.True(t, ok)
	assert.Equal(t, "benchhop-000", firstKey)
	assert.EqualValues(t, 6, toInt64ForTest(t, res.Rows[0][1]))

	secondKey, ok := res.Rows[1][0].(string)
	require.True(t, ok)
	assert.Equal(t, "benchhop-001", secondKey)
	assert.EqualValues(t, 3, toInt64ForTest(t, res.Rows[1][1]))

	firstScore, ok := res.Rows[0][2].(float64)
	require.True(t, ok)
	secondScore, ok := res.Rows[1][2].(float64)
	require.True(t, ok)
	assert.Greater(t, firstScore, secondScore)
}
