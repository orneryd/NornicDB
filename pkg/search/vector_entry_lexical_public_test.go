package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The review's four-document public BuildIndexes reproduction. These bytes are
// independent graph IDs; no constructor is used to manufacture the collision.
// Check public construction, graph readers, and both snapshot iterators against
// authoritative BM25 document hints, rather than one branch's cache layout.
func TestVectorEntryPublicLexicalOwnership(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
	for attempt := 0; attempt < 16; attempt++ {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			engine := newNamespacedEngine(t)
			alias := "\x00n\x05otherv"
			for _, node := range []*storage.Node{
				{ID: "other", Properties: map[string]any{"text": "victimtoken victimtoken"}, NamedEmbeddings: map[string][]float32{"v": {1, 0}}},
				{ID: storage.NodeID(alias), Properties: map[string]any{"text": "markertoken markertoken"}, ChunkEmbeddings: [][]float32{{0, 1}}},
				{ID: "victim-peer", Properties: map[string]any{"text": "victimtoken victimtoken"}},
				{ID: "marker-peer", Properties: map[string]any{"text": "markertoken markertoken"}},
			} {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			require.NotNil(t, svc.hnswIndex, "the public build must construct ANN")
			docs := svc.hnswLexicalSeedHints(svc.fulltext(), svc.hnswIndex.Config().M)
			markerHint, ok := nodeHintForID(docs, alias)
			require.True(t, ok)
			otherHint, ok := nodeHintForID(docs, "other")
			require.True(t, ok)
			require.NotEqual(t, markerHint.Signature, otherHint.Signature)
			require.Less(t, markerHint.Rank, otherHint.Rank)
			markerKey := svc.nodeChunkVectors[alias][0]
			otherKey := svc.nodeNamedVector["other"]["v"]
			require.Equal(t, []string{markerKey, otherKey}, svc.hnswIndex.internalToID, "each document owns its ranked construction slot")
			want := map[string]nodeLexicalHint{markerKey: markerHint, otherKey: otherHint}
			for internalID, key := range svc.hnswIndex.internalToID {
				hint, found := svc.hnswIndex.buildLexicalHintLocked(uint32(internalID))
				require.True(t, found)
				require.Equal(t, want[key], hint, "graph lookup must keep the entry's own owner/signature/rank")
			}
			pairs := hnswOrderedVectorIndexPairs(svc.vectorIndex, docs)
			checkPairs := func(pairs []hnswBuildPair) {
				t.Helper()
				require.Len(t, pairs, 2)
				require.Equal(t, markerKey, pairs[0].id)
				for _, pair := range pairs {
					require.True(t, pair.hasHint)
					require.Equal(t, want[pair.id], pair.hint)
				}
			}
			checkPairs(pairs)
			vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
			require.NoError(t, err)
			require.NoError(t, vfs.Load())
			t.Cleanup(func() { require.NoError(t, vfs.Close()) })
			for _, pair := range pairs {
				require.NoError(t, vfs.Add(pair.id, pair.vec))
			}
			for name, iterator := range map[string]hnswBuildIterator{
				"file": hnswVectorFileStoreIterator(vfs, docs),
				"chunked": hnswVectorChunkIterator(func(_ int, fn func([]string, [][]float32) error) error {
					return fn([]string{otherKey, markerKey}, [][]float32{{1, 0}, {0, 1}})
				}, docs),
			} {
				var read []hnswBuildPair
				require.NoError(t, iterator(1, func(batch []hnswBuildPair) error { read = append(read, batch...); return nil }), name)
				checkPairs(read)
			}
			// The accelerated orchestration consumes the same pairs. CPU scoring
			// exercises that path without claiming execution on GPU hardware.
			config := DefaultHNSWConfig()
			config.UseGPUBuild = true
			config.GPUBuildBatchSize = 1
			built, _, err := buildHNSWWithOptionalGPU(context.Background(), 2, config, svc.vectorIndex.getVectorRef, len(pairs), hnswPairSliceIterator(pairs), NewCPUHNSWBuildAccelerator())
			require.NoError(t, err)
			for internalID, key := range built.internalToID {
				hint, found := built.buildLexicalHintLocked(uint32(internalID))
				require.True(t, found)
				require.Equal(t, want[key], hint)
			}
		})
	}
}
