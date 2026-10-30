package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestE2E_VectorCosine_QueryShapes_StayOnIndexedPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX chunk_emb_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 16, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	seedVec := func(i int) string {
		vals := make([]float64, 16)
		vals[i%16] = 1.0
		return formatInlineFloat64Vector(vals)
	}

	for i := 0; i < 80; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'c-%d', group_id:'g', emb:%s})", i, seedVec(i)), nil)
		require.NoError(t, err)
	}

	q := make([]float64, 16)
	q[0] = 1.0
	params := map[string]interface{}{"q": q, "g": "g", "groups": []string{"g"}, "lim": 5, "min": 0.1}

	shapes := []struct {
		name            string
		query           string
		requireFastPath bool
	}{
		{
			name:            "V1 direct return cosine",
			query:           "MATCH (c:Chunk) RETURN vector.similarity.cosine(c.emb,$q) AS s ORDER BY s DESC LIMIT 5",
			requireFastPath: true,
		},
		{
			name:            "V2 direct return cosine with where",
			query:           "MATCH (c:Chunk) WHERE c.group_id=$g RETURN vector.similarity.cosine(c.emb,$q) AS s ORDER BY s DESC LIMIT 5",
			requireFastPath: false,
		},
		{
			name:            "V3 with projection cosine",
			query:           "MATCH (c:Chunk) WITH c, vector.similarity.cosine(c.emb,$q) AS s RETURN c.uuid, s ORDER BY s DESC LIMIT 5",
			requireFastPath: true,
		},
		{
			name:            "V4 direct return cosine with parameterized limit",
			query:           "MATCH (c:Chunk) RETURN vector.similarity.cosine(c.emb,$q) AS s ORDER BY s DESC LIMIT $lim",
			requireFastPath: true,
		},
		{
			name: "V5 graphiti projection with pre and post where",
			query: `MATCH (c:Chunk) WHERE c.group_id IN $groups
WITH c, vector.similarity.cosine(c.emb,$q) AS score
WHERE score > $min
RETURN c.uuid, score
ORDER BY score DESC
LIMIT $lim`,
			requireFastPath: true,
		},
	}

	for i, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'invalidate-%d', group_id:'g', emb:%s})", i, seedVec(i+81)), nil)
			require.NoError(t, err)

			counting.allNodesCalls = 0
			counting.labelCalls = 0
			counting.streamNodesCalls = 0

			res, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.NotEmpty(t, res.Rows)

			trace := exec.LastHotPathTrace()
			if tc.requireFastPath {
				require.True(t, trace.CosineVectorIndexFastPath, "shape should route through cosine vector-index fast path")
			}
		})
	}
}

func TestE2E_VectorCosine_QueryShape_ExplicitTransactionUsesIndexedPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX tx_emb_idx FOR (n:Tx) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 16, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	for i := 0; i < 60; i++ {
		vec := make([]float64, 16)
		vec[i%16] = 1.0
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Tx {uuid:'tx-%d', name_embedding:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}

	query := "MATCH (n:Tx) WITH n, vector.similarity.cosine(n.name_embedding,$q) AS s RETURN n.uuid, s ORDER BY s DESC LIMIT 5"
	q := make([]float64, 16)
	q[0] = 1.0

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": q})
	require.NoError(t, err)
	require.Len(t, res.Rows, 5)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "explicit transaction should route through cosine vector-index fast path")

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func formatInlineFloat64Vector(vec []float64) string {
	if len(vec) == 0 {
		return "[]"
	}
	out := "["
	for i, v := range vec {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%.1f", v)
	}
	out += "]"
	return out
}

const tinyChunkPropertyQuery = `
MATCH (c:Chunk {group_id:'kg'}) WHERE c.emb IS NOT NULL
WITH c, vector.similarity.cosine(c.emb, $q) AS sim
RETURN c.uuid AS uuid, sim
ORDER BY sim DESC
LIMIT 3
`

// newTinyChunkPropertyFixture owns the shared 21-node, 1024-dimensional
// population and index warmup. The caller selects instrumentation and caching
// before any query runs; benchmark setup stays outside its measured loop.
func newTinyChunkPropertyFixture(t testing.TB, executor func(storage.Engine) *StorageExecutor) (*StorageExecutor, map[string]interface{}) {
	t.Helper()
	ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := executor(ns)
	ctx := context.Background()
	const dim = 1024
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX tiny_chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	service := search.NewService(ns, dim, nil)
	exec.SetSearchService(service)
	for i := 0; i < 21; i++ {
		vec := make([]float64, dim)
		vec[i%dim] = 1.0
		if i == 1 {
			vec[0], vec[1] = 0.8, 0.6
		}
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'c-%02d', group_id:'kg', emb:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}
	require.Equal(t, 21, service.CountPropertyVectorEntries("emb"))
	require.NoError(t, service.BuildIndexes(ctx))
	require.True(t, service.IsReady())
	queryVector := make([]float64, dim)
	queryVector[0] = 1.0
	params := map[string]interface{}{"q": queryVector}
	_, err = exec.Execute(ctx, "CALL db.index.vector.queryNodes('tiny_chunk_idx', 1, $q) YIELD node, score RETURN node.uuid AS uuid, score", params)
	require.NoError(t, err)
	return exec, params
}

func TestE2E_VectorCosine_TinyChunkPropertyPatternPreservesExactResults(t *testing.T) {
	var counting *countingStreamingEngine
	exec, params := newTinyChunkPropertyFixture(t, func(engine storage.Engine) *StorageExecutor {
		counting = &countingStreamingEngine{Engine: engine}
		return NewStorageExecutor(counting)
	})
	counting.allNodesCalls, counting.labelCalls, counting.streamNodesCalls = 0, 0, 0
	res := runTinyChunkPropertyQueries(t, exec, params, 1)
	require.Equal(t, "c-00", res.Rows[0][0])
	require.Equal(t, "c-01", res.Rows[1][0])
}

func BenchmarkE2E_VectorCosine_TinyChunkPropertyPattern(b *testing.B) {
	exec, params := newTinyChunkPropertyFixture(b, func(engine storage.Engine) *StorageExecutor {
		exec := NewStorageExecutor(engine)
		exec.cache = nil
		return exec
	})
	b.ReportAllocs()
	b.ResetTimer()
	runTinyChunkPropertyQueries(b, exec, params, b.N)
}

// runTinyChunkPropertyQueries owns the query and cardinality invariant shared
// by correctness and timing runs. Fixture creation and benchmark timer control
// stay with their callers; the correctness test also checks the returned IDs.
func runTinyChunkPropertyQueries(t testing.TB, exec *StorageExecutor, params map[string]interface{}, count int) *ExecuteResult {
	t.Helper()
	ctx := context.Background()
	var result *ExecuteResult
	for i := 0; i < count; i++ {
		var err error
		result, err = exec.Execute(ctx, tinyChunkPropertyQuery, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	}
	return result
}
