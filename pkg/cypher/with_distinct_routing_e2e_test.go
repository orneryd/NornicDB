package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithDistinctRelationshipRoute_EvaluatesProjectedExpressions(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (o1:OriginalText {originalText:'alpha'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (o2:OriginalText {originalText:'beta'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (h1:BenchmarkHop {hopId:'h1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (h2:BenchmarkHop {hopId:'h2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (h3:BenchmarkHop {hopId:'h3'})", nil)
	require.NoError(t, err)

	// Create duplicate-producing traversal rows for o1 so WITH DISTINCT o must collapse rows.
	_, err = exec.Execute(ctx, "MATCH (o:OriginalText {originalText:'alpha'}), (h:BenchmarkHop {hopId:'h1'}) CREATE (o)-[:BENCH_HOP]->(h)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (o:OriginalText {originalText:'alpha'}), (h:BenchmarkHop {hopId:'h2'}) CREATE (o)-[:BENCH_HOP]->(h)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (o:OriginalText {originalText:'beta'}), (h:BenchmarkHop {hopId:'h3'}) CREATE (o)-[:BENCH_HOP]->(h)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:BENCH_HOP]->(:BenchmarkHop)
WHERE o.originalText IS NOT NULL AND trim(o.originalText) <> ''
WITH DISTINCT o
RETURN elementId(o) AS id, o.originalText AS text
ORDER BY text
LIMIT 3
`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"id", "text"}, result.Columns)
	require.Len(t, result.Rows, 2)

	assert.Equal(t, "alpha", result.Rows[0][1])
	assert.Equal(t, "beta", result.Rows[1][1])
	for _, row := range result.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		assert.NotEmpty(t, id)
		assert.NotEqual(t, "elementId(o)", id)

		text, ok := row[1].(string)
		require.True(t, ok)
		assert.NotEmpty(t, text)
		assert.NotEqual(t, "o.originalText", text)
	}
}

func TestWithDistinctNodeRoute_EvaluatesProjectedExpressions(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (o1:OriginalText {originalText:'gamma'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (o2:OriginalText {originalText:'delta'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE o.originalText IS NOT NULL AND trim(o.originalText) <> ''
WITH DISTINCT o
RETURN elementId(o) AS id, o.originalText AS text
ORDER BY text
`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"id", "text"}, result.Columns)
	require.Len(t, result.Rows, 2)

	assert.Equal(t, "delta", result.Rows[0][1])
	assert.Equal(t, "gamma", result.Rows[1][1])
	for _, row := range result.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		assert.NotEmpty(t, id)
		assert.NotEqual(t, "elementId(o)", id)

		text, ok := row[1].(string)
		require.True(t, ok)
		assert.NotEmpty(t, text)
		assert.NotEqual(t, "o.originalText", text)
	}
}

func TestOptionalMatchVariableLengthByElementIDParam_EvaluatesAndCountsPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (o:OriginalText {originalText:'probe-root'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (h1:BenchmarkHop {hopId:'probe:1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (h2:BenchmarkHop {hopId:'probe:2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (o:OriginalText {originalText:'probe-root'}), (h1:BenchmarkHop {hopId:'probe:1'}) CREATE (o)-[:BENCH_HOP]->(h1)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (h1:BenchmarkHop {hopId:'probe:1'}), (h2:BenchmarkHop {hopId:'probe:2'}) CREATE (h1)-[:BENCH_HOP]->(h2)", nil)
	require.NoError(t, err)

	rootRes, err := exec.Execute(ctx, "MATCH (o:OriginalText {originalText:'probe-root'}) RETURN elementId(o) AS id", nil)
	require.NoError(t, err)
	require.Len(t, rootRes.Rows, 1)
	rootID, ok := rootRes.Rows[0][0].(string)
	require.True(t, ok)
	require.NotEmpty(t, rootID)

	result, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE elementId(o) = $rootID
OPTIONAL MATCH (o)-[:BENCH_HOP*1..6]->(:BenchmarkHop)
RETURN o.originalText AS text, count(*) AS paths
`, map[string]interface{}{"rootID": rootID})
	require.NoError(t, err)
	require.Equal(t, []string{"text", "paths"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "probe-root", result.Rows[0][0])

	pathsCount, ok := result.Rows[0][1].(int64)
	require.True(t, ok)
	assert.GreaterOrEqual(t, pathsCount, int64(1))
}
