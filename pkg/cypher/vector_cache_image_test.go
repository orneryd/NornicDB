package cypher

import (
	"runtime/pprof"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const cachedVectorSourceIndex = "CREATE VECTOR INDEX cached_source_chunks FOR (n:Source) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`:2048, `vector.similarity_function`:'cosine'}}"
const cachedVectorNodeQuery = "CALL db.index.vector.queryNodes('cached_source_chunks',25,$q) YIELD node,score RETURN node,node.id AS id,node.title AS title"

// Give the existing managed-chunk fixture distinct scores without adding
// inline named-vector payloads. The first chunk wins; all other chunks remain
// orthogonal to the query. The ranking is therefore exact and deterministic.
func cachedVectorGraph(tb testing.TB, workload chunkHydrationWorkload) (*nor413Graph, *StorageExecutor) {
	tb.Helper()
	tb.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	tb.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	graph, exec := cachedHydrationGraph(tb, workload, cachedVectorSourceIndex, nor413Dims)
	for rank, row := range chunkHydrationFulltextRows(graph, workload.documents) {
		id := storage.NodeID("test:" + row[0].(string))
		stored, err := graph.engine.GetNode(tb.Context(), id)
		require.NoError(tb, err)
		require.Len(tb, stored.ChunkEmbeddings, workload.chunksPerSource)
		vector := make([]float32, nor413Dims)
		vector[0], vector[1] = 1, float32(rank+1)/float32(workload.documents)
		stored.ChunkEmbeddings[0] = vector
		require.NoError(tb, graph.engine.UpdateNodeEmbeddingSidecar(tb.Context(), stored))
		check, err := graph.engine.GetNode(tb.Context(), id)
		require.NoError(tb, err)
		require.Equal(tb, vector, check.ChunkEmbeddings[0])
	}
	require.NoError(tb, exec.search().BuildIndexes(tb.Context()))
	return graph, exec
}

func TestPublicVectorCacheHitsPreserveMetadataImages(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		workload := chunkHydrationWorkload{documents: 32, chunksPerSource: 8}
		graph, exec := cachedVectorGraph(t, workload)
		spy := storagefault.NewQueryNodeReadSpy(graph.store)
		exec.storage = spy
		query := make([]float64, nor413Dims)
		query[0] = 1
		params := map[string]interface{}{"q": query}
		want := chunkHydrationFulltextRows(graph, 25)
		require.True(t, isCacheableReadQuery(cachedVectorNodeQuery))
		hits, misses, _, _, _ := exec.cache.Stats()
		received, err := exec.Execute(t.Context(), cachedVectorNodeQuery, params)
		require.NoError(t, err)
		requireCachedSourceImages(t, received, want, workload.chunksPerSource, true)
		h, m, _, _, _ := exec.cache.Stats()
		require.Equal(t, hits, h)
		require.Equal(t, misses+1, m)
		require.Positive(t, spy.Query.Load(), "the cold query establishes that the probe sees admitted metadata reads")
		for i := 0; i < 2; i++ {
			editReceivedNodeImages(received)
			spy.Full.Store(0)
			spy.Query.Store(0)
			hits, misses, _, _, _ = exec.cache.Stats()
			received, err = exec.Execute(t.Context(), cachedVectorNodeQuery, params)
			require.NoError(t, err)
			requireCachedSourceImages(t, received, want, workload.chunksPerSource, true)
			h, m, _, _, _ = exec.cache.Stats()
			require.Equal(t, hits+1, h, "prove actual public query-cache serving")
			require.Equal(t, misses, m)
			require.Zero(t, spy.Full.Load(), "cache serving does not hydrate stored chunk vectors")
			require.Zero(t, spy.Query.Load(), "a cache hit needs no node rehydration")
		}
		for _, row := range want {
			stored, err := graph.store.GetNode(t.Context(), storage.NodeID(row[0].(string)))
			require.NoError(t, err)
			require.Len(t, stored.ChunkEmbeddings, workload.chunksPerSource)
			require.Empty(t, stored.NamedEmbeddings)
		}
	})
}

// The public cache path is measured against the same source payload scales as
// the existing fulltext qualification. Legitimate initial vector index build
// and the initial query occur outside the cache-hit timer.
func BenchmarkCachedVectorQueryNodeImages(b *testing.B) {
	for _, workload := range chunkHydrationWorkloads() {
		if workload.documents != 512 {
			continue
		}
		b.Run(workload.label(), func(b *testing.B) {
			graph, exec := cachedVectorGraph(b, workload)
			query := make([]float64, nor413Dims)
			query[0] = 1
			params := []map[string]interface{}{{"q": query}}
			want := chunkHydrationFulltextRows(graph, 25)
			first, err := exec.Execute(b.Context(), cachedVectorNodeQuery, params[0])
			require.NoError(b, err)
			requireCachedSourceImages(b, first, want, workload.chunksPerSource, true)
			hits, misses, _, _, _ := exec.cache.Stats()
			benchmarkStatementRead(b, exec, cachedVectorNodeQuery, params, false,
				pprof.Labels("phase", "cached-vector-query", "workload", workload.label()),
				func(result *ExecuteResult) {
					requireCachedSourceImages(b, result, want, workload.chunksPerSource, true)
					h, m, _, _, _ := exec.cache.Stats()
					require.Equal(b, hits+1, h)
					require.Equal(b, misses, m)
					hits = h
				})
		})
	}
}
