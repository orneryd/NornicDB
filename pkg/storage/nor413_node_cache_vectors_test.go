package storage

import (
	"context"
	"runtime"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A warmed full cache must not make metadata reads copy the vector payload;
// the lightweight query image may coexist with the bounded full image.
func TestNOR413NodeCachesSeparateHydratedVectorsFromQueryReads(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:cache-vectors")
			chunks := tc.create(t, b, id)
			for i := 0; i < 3; i++ {
				full, err := b.GetNode(t.Context(), id)
				require.NoError(t, err)
				require.Equal(t, chunks, full.ChunkEmbeddings)
				seen := 0
				require.NoError(t, b.StreamNodesWithOptions(t.Context(), StreamNodesOptions{Label: "Doc", Prefix: "test:", QueryNodes: true}, func(node *Node) error {
					seen++
					require.Empty(t, node.ChunkEmbeddings, "warm label scans must use the same lightweight cache selection")
					return nil
				}))
				require.Equal(t, 1, seen)
				query, err := b.GetQueryNode(t.Context(), id)
				require.NoError(t, err)
				require.Empty(t, query.ChunkEmbeddings, "a preceding full read must not make a query read copy stored vectors")
				require.True(t, query.HasChunkEmbeddings())
				b.nodeCacheMu.RLock()
				queryChunks := len(b.queryNodeCache[id].ChunkEmbeddings)
				fullChunks := len(b.nodeCache[id].ChunkEmbeddings)
				b.nodeCacheMu.RUnlock()
				require.Zero(t, queryChunks)
				require.Len(t, chunks, fullChunks, "full reads retain their accelerated payload")
				full.ChunkEmbeddings[0][0] = -100
			}
		})
	}
}

func TestNOR413WarmQueryReadAllocationDoesNotScaleWithChunks(t *testing.T) {
	b := newSidecarTestBadger(t)
	id := NodeID("test:warm-allocation")
	chunks := make([][]float32, 128)
	for i := range chunks {
		chunks[i] = make([]float32, 2048)
		chunks[i][0] = 1
	}
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "warm"}, ChunkEmbeddings: chunks})
	require.NoError(t, err)
	_, err = b.GetNode(t.Context(), id)
	require.NoError(t, err)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 8; i++ {
		_, err = b.GetQueryNode(t.Context(), id)
		require.NoError(t, err)
	}
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(512*1024), "eight metadata reads should not copy the 1 MiB chunk payload each time")
}

func BenchmarkNOR413WarmQueryRead(b *testing.B) {
	engine := newTestMemoryEngine(b).BadgerEngine
	var err error
	chunks := make([][]float32, 128)
	for i := range chunks {
		chunks[i] = make([]float32, 2048)
		chunks[i][0] = 1
	}
	_, err = engine.CreateNode(b.Context(), &Node{ID: "bench:node", Labels: []string{"Doc"}, Properties: map[string]any{"title": "warm"}, ChunkEmbeddings: chunks})
	if err != nil {
		b.Fatal(err)
	}
	_, err = engine.GetNode(b.Context(), "bench:node")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err = engine.GetQueryNode(context.Background(), "bench:node"); err != nil {
			b.Fatal(err)
		}
	}
}

// A vector-bearing replacement must revoke every previous cache image;
// a late fill from the preceding generation cannot overwrite the replacement.
func TestNOR413VectorReplacementRevokesOldCachedBody(t *testing.T) {
	b := newSidecarTestBadger(t)
	id := NodeID("test:replace")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = b.GetQueryNode(t.Context(), id)
	require.NoError(t, err)
	oldGeneration := b.nodeCacheGen.current()
	before, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	updated := CopyNode(before)
	updated.Properties["v"] = int64(2)
	updated.NamedEmbeddings = map[string][]float32{"title": {1, 2, 3}}
	require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{Before: before, After: updated}))
	b.cacheFillNode(oldGeneration, before, nodeReadCacheFull, b.db.oracle.published.Load())
	for i := 0; i < 2; i++ {
		full, err := b.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, int64(2), full.Properties["v"])
		require.Equal(t, []float32{1, 2, 3}, full.NamedEmbeddings["title"])
		full.NamedEmbeddings["title"][0] = 99
	}
	b.nodeCacheMu.RLock()
	_, fullCached := b.nodeCache[id]
	_, queryCached := b.queryNodeCache[id]
	b.nodeCacheMu.RUnlock()
	require.True(t, fullCached)
	require.False(t, queryCached)
}

func TestNOR413UncachedVectorPointAndLabelReadsShareSource(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.nodeCacheMaxBytes = 1024 // this full image cannot be retained
	id := NodeID("test:same-source")
	lazyEmbeddingCases()[0].create(t, b, id)
	point, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	labelled, err := b.GetNodesByLabel(t.Context(), "Doc")
	require.NoError(t, err)
	require.Len(t, labelled, 1)
	require.Equal(t, point, labelled[0], "native readers must observe the same node image and embedding provenance")
}

// Full-vector reads must retain their performance within the byte budget.
func BenchmarkNOR413FullVectorRead(b *testing.B) {
	engine := newTestMemoryEngine(b).BadgerEngine
	chunks := make([][]float32, 128)
	for i := range chunks {
		chunks[i] = make([]float32, 2048)
		chunks[i][0] = 1
	}
	_, err := engine.CreateNode(b.Context(), &Node{ID: "bench:full", Labels: []string{"Doc"}, Properties: map[string]any{"title": "full"}, ChunkEmbeddings: chunks})
	if err != nil {
		b.Fatal(err)
	}
	_, err = engine.GetNode(b.Context(), "bench:full")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		node, readErr := engine.GetNode(b.Context(), "bench:full")
		if readErr != nil {
			b.Fatal(readErr)
		}
		if len(node.ChunkEmbeddings) != len(chunks) {
			b.Fatal("full read lost chunks")
		}
	}
}

func TestNOR413NodeReadCachesShareByteBudgetAndReleaseIt(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, NodeCacheMaxBytes: 96 << 10}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for i := 0; i < 24; i++ {
		id := NodeID("budget:" + itoaBench(i))
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{make([]float32, 8192)}, NamedEmbeddings: map[string][]float32{"title": {1, 2}}})
		require.NoError(t, err)
		_, err = engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		_, err = engine.GetQueryNode(t.Context(), id)
		require.NoError(t, err)
		engine.nodeCacheMu.RLock()
		var retainedPayload int64
		for _, cache := range []map[NodeID]*Node{engine.nodeCache, engine.queryNodeCache} {
			for _, node := range cache {
				for _, v := range node.ChunkEmbeddings {
					retainedPayload += int64(len(v)) * 4
				}
				for _, v := range node.NamedEmbeddings {
					retainedPayload += int64(len(v)) * 4
				}
			}
		}
		accounted := engine.nodeCacheBytes
		engine.nodeCacheMu.RUnlock()
		require.LessOrEqual(t, retainedPayload, int64(96<<10), "retained vectors cannot grow with the entry-count limit")
		require.GreaterOrEqual(t, accounted, retainedPayload)
		require.LessOrEqual(t, accounted, int64(96<<10))
	}
	engine.invalidateCachesAfterRestore()
	require.Zero(t, engine.nodeCacheBytes)
}
