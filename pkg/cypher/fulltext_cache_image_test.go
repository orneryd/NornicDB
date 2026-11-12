package cypher

import (
	"runtime/pprof"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const cachedFulltextNodeQuery = "CALL db.index.fulltext.queryNodes('chunk_hydration_source_title', $query, {limit: 25}) YIELD node, score RETURN node, node.id AS id, node.title AS title"

// One fixture composition owns the declared index, enabled cache and service
// for public fulltext/vector tests and measurements. The graph, sidecar writes and payload checks stay shared with
// the existing uncached qualification.
func cachedHydrationGraph(tb testing.TB, workload chunkHydrationWorkload, declaration string, dimensions int) (*nor413Graph, *StorageExecutor) {
	tb.Helper()
	graph, indexing := chunkHydrationGraph(tb, workload)
	_, err := indexing.Execute(tb.Context(), declaration, nil)
	require.NoError(tb, err)
	service := search.NewService(graph.store, dimensions, nil)
	tb.Cleanup(func() { require.NoError(tb, service.Close()) })
	exec := NewStorageExecutor(graph.store, QueryCachePolicy{MaxEntries: 8, TTL: time.Hour})
	exec.SetSearchService(service)
	return graph, exec
}

func requireCachedSourceImages(tb testing.TB, result *ExecuteResult, want [][]interface{}, chunks int, nodes bool) {
	tb.Helper()
	require.NotNil(tb, result)
	require.Len(tb, result.Rows, len(want))
	if !nodes {
		require.Equal(tb, []string{"id", "title"}, result.Columns)
		require.Equal(tb, want, result.Rows)
		return
	}
	require.Equal(tb, []string{"node", "id", "title"}, result.Columns)
	for i, expected := range want {
		require.Len(tb, result.Rows[i], 3)
		node, ok := result.Rows[i][0].(*storage.Node)
		require.True(tb, ok, "node row %d has type %T", i, result.Rows[i][0])
		require.Equal(tb, storage.NodeID(expected[0].(string)), node.ID)
		require.Equal(tb, []string{"Source"}, node.Labels)
		require.Equal(tb, map[string]interface{}{"id": expected[0], "title": expected[1]}, node.Properties)
		require.Equal(tb, expected, result.Rows[i][1:])
		require.Equal(tb, chunks > 0, nodeHasEmbeddingValue(node))
		require.Empty(tb, node.ChunkEmbeddings, "ordinary results do not retain stored chunks")
		require.Empty(tb, node.NamedEmbeddings)
	}
}

func editReceivedNodeImages(result *ExecuteResult) {
	for _, row := range result.Rows {
		for _, value := range row {
			if node, ok := value.(*storage.Node); ok {
				node.Properties["title"] = "caller edit"
				node.Labels[0] = "CallerLabel"
				node.ChunkEmbeddings = [][]float32{{1}}
			}
		}
		for i := range row {
			row[i] = "caller edit"
		}
	}
	for i := range result.Columns {
		result.Columns[i] = "caller edit"
	}
	result.Rows = append(result.Rows, []interface{}{"caller extra row"})
}

func TestPublicFulltextCacheHitsPreserveMetadataImages(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, chunks := range []int{0, 8} {
			for _, nodes := range []bool{false, true} {
				name := chunkHydrationWorkload{documents: 32, chunksPerSource: chunks}.label()
				if nodes {
					name += "/nodes"
				} else {
					name += "/metadata"
				}
				t.Run(name, func(t *testing.T) {
					graph, exec := cachedHydrationGraph(t, chunkHydrationWorkload{documents: 32, chunksPerSource: chunks}, chunkHydrationFulltextIndex, search.DefaultVectorDimensions)
					query := chunkHydrationFulltextQuery
					if nodes {
						query = cachedFulltextNodeQuery
					}
					require.True(t, isCacheableReadQuery(query))
					want := chunkHydrationFulltextRows(graph, chunkHydrationFulltextLimit)
					params := map[string]interface{}{"query": "source"}
					expectedChunks := chunks
					read := func() *ExecuteResult {
						got, err := exec.Execute(t.Context(), query, params)
						require.NoError(t, err)
						requireCachedSourceImages(t, got, want, expectedChunks, nodes)
						return got
					}
					h0, m0, _, _, _ := exec.cache.Stats()
					received := read()
					h1, m1, _, _, _ := exec.cache.Stats()
					require.Equal(t, h0, h1)
					require.Equal(t, m0+1, m1, "first read must execute")
					for i := 0; i < 2; i++ {
						editReceivedNodeImages(received)
						h0, m0, _, _, _ = exec.cache.Stats()
						received = read()
						h1, m1, _, _, _ = exec.cache.Stats()
						require.Equal(t, h0+1, h1, "prove actual cache serving")
						require.Equal(t, m0, m1)
					}
					// The same full read that native callers use still sees all stored data.
					for _, expected := range want {
						stored, err := graph.store.GetNode(t.Context(), storage.NodeID(expected[0].(string)))
						require.NoError(t, err)
						require.Equal(t, []string{"Source"}, stored.Labels)
						require.Equal(t, map[string]interface{}{"id": expected[0], "title": expected[1]}, stored.Properties)
						require.Len(t, stored.ChunkEmbeddings, chunks)
					}
					// Change the actual returned source values, so stale serving is observable.
					_, err := exec.Execute(t.Context(), "MATCH (s:Source) SET s.title = 'updated source'", nil)
					require.NoError(t, err)
					for _, row := range want {
						row[1] = "updated source"
					}
					// title is embedding text under this fixture's default
					// policy. The legitimate write invalidates its old managed
					// vectors; a fresh result must report that state as well.
					expectedChunks = 0
					stored, nativeErr := graph.store.GetNode(t.Context(), storage.NodeID(want[0][0].(string)))
					require.NoError(t, nativeErr)
					require.Empty(t, stored.ChunkEmbeddings)

					read()
				})
			}
		}
	})
}

// This compares identical 25-row cache hits against 32/512 MiB source payloads.
// It is not the private application replay. Each calibration invalidates its
// owned result cache before cold setup; retained search/admission state may be warm.
func BenchmarkCachedFulltextQueryNodeImages(b *testing.B) {
	selected := 0
	for _, workload := range chunkHydrationWorkloads() {
		if workload.documents != 512 {
			continue
		}
		selected++
		b.Run(workload.label(), func(b *testing.B) {
			for _, parser := range relationshipEqualityParsers {
				b.Run(parser.name, func(b *testing.B) {
					b.Cleanup(parser.setup())
					graph, exec := cachedHydrationGraph(b, workload, chunkHydrationFulltextIndex, search.DefaultVectorDimensions)
					want := chunkHydrationFulltextRows(graph, chunkHydrationFulltextLimit)
					params := []map[string]interface{}{{"query": "source"}, {"query": "src"}}
					for _, nodes := range []bool{false, true} {
						name, query := "metadata", chunkHydrationFulltextQuery
						if nodes {
							name, query = "nodes", cachedFulltextNodeQuery
						}
						b.Run(name, func(b *testing.B) {
							exec.cache.Invalidate()
							for _, set := range params {
								h0, m0, _, _, _ := exec.cache.Stats()
								got, err := exec.Execute(b.Context(), query, set)
								require.NoError(b, err)
								requireCachedSourceImages(b, got, want, workload.chunksPerSource, nodes)
								h1, m1, _, _, _ := exec.cache.Stats()
								require.Equal(b, h0, h1)
								require.Equal(b, m0+1, m1)
							}
							hits, misses, _, _, _ := exec.cache.Stats()
							first := hits
							benchmarkStatementRead(b, exec, query, params, false,
								pprof.Labels("phase", "cached-fulltext-query", "statement", name, "workload", workload.label()),
								func(got *ExecuteResult) {
									requireCachedSourceImages(b, got, want, workload.chunksPerSource, nodes)
									h1, m1, _, _, _ := exec.cache.Stats()
									require.Equal(b, hits+1, h1)
									require.Equal(b, misses, m1)
									hits = h1
								})
							require.Equal(b, first+int64(b.N), hits)
						})
					}
				})
			}
		})
	}
	require.Equal(b, 2, selected)
}
