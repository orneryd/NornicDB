package search

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorCapacityReplacementIndependentOfFulltext(t *testing.T) {
	for _, bm25 := range []bool{false, true} {
		for _, transferred := range []bool{false, true} {
			t.Run(fmt.Sprintf("bm25=%t/transferred=%t", bm25, transferred), func(t *testing.T) {
				svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 24})
				t.Cleanup(func() { _ = svc.Close() })
				svc.SetIndexFlags(bm25, true)
				if transferred {
					svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
					svc.SetPersistenceEnabled(true)
				}
				for i := 0; i < 3; i++ {
					node := &storage.Node{ID: "owner", NamedEmbeddings: map[string][]float32{fmt.Sprintf("name%d", i): {1, 0, 0}}}
					require.NoError(t, svc.IndexNode(node))
					if i == 0 && transferred {
						svc.SetPersistenceEnabled(false)
						require.Nil(t, svc.vectorFileStore)
					}
					assert.Equal(t, 1, svc.vectorIndex.Count(), "replacement must remove old vector IDs even when fulltext is skipped")
				}
				assert.Equal(t, int64(12), svc.vectorResidentBytes)
				assert.LessOrEqual(t, svc.vectorResidentBytes, svc.vectorMemoryMaxBytes, "admission assumed replacement but retained prior payload")
				require.NoError(t, svc.RemoveNode("owner"))
				require.Zero(t, svc.vectorResidentBytes)
				require.Zero(t, svc.vectorIndex.Count())
			})
		}
	}
}

func TestVectorCapacityPropertyRemovalReleasesAdmission(t *testing.T) {
	for _, transferred := range []bool{false, true} {
		t.Run(fmt.Sprint(transferred), func(t *testing.T) {
			svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 12})
			t.Cleanup(func() { _ = svc.Close() })
			if transferred {
				svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
				svc.SetPersistenceEnabled(true)
			}
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "owner", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}))
			if transferred {
				svc.SetPersistenceEnabled(false)
				require.Nil(t, svc.vectorFileStore)
			}
			metadata := svc.vectorMetadataBytes
			svc.RemovePropertyVectorIndex("embedding")
			assert.Zero(t, svc.vectorResidentBytes)
			assert.Less(t, svc.vectorMetadataBytes, metadata, "remove the entry's metadata charge too")
			require.Zero(t, svc.vectorIndex.Count())
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "next", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
			svc.RemovePropertyVectorIndex("embedding")
			require.Equal(t, int64(12), svc.vectorResidentBytes)
			require.NoError(t, svc.RemoveNode("owner"))
			require.Equal(t, int64(12), svc.vectorResidentBytes)
			require.NoError(t, svc.RemoveNode("next"))
			require.Zero(t, svc.vectorResidentBytes)
		})
	}
}

func TestVectorCapacityReplacementRetiresEveryEntryKind(t *testing.T) {
	for _, bm25 := range []bool{false, true} {
		t.Run(fmt.Sprint(bm25), func(t *testing.T) {
			svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 48})
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetIndexFlags(bm25, true)
			node := &storage.Node{ID: "owner", NamedEmbeddings: map[string][]float32{"old": {1, 0, 0}}, ChunkEmbeddings: [][]float32{{1, 0, 0}, {0, 1, 0}}, Properties: map[string]any{"embedding": []float32{0, 0, 1}, "text": "old document"}}
			require.NoError(t, svc.IndexNode(node))
			require.Equal(t, 4, svc.vectorIndex.Count())
			require.Equal(t, int64(48), svc.vectorResidentBytes)
			replacement := &storage.Node{ID: "owner", ChunkEmbeddings: [][]float32{{1, 0, 0}}, Properties: map[string]any{"text": "replacement document"}}
			require.NoError(t, svc.IndexNode(replacement))
			require.Equal(t, 1, svc.vectorIndex.Count())
			require.Equal(t, int64(12), svc.vectorResidentBytes)
			require.Empty(t, svc.nodeNamedVector["owner"])
			require.Empty(t, svc.nodePropVector["owner"])
			require.Equal(t, []string{"owner"}, svc.nodeChunkVectors["owner"])
			if bm25 {
				text, ok := svc.fulltext().GetDocument("owner")
				require.True(t, ok)
				require.Contains(t, text, "replacement document")
			}
		})
	}
}

func TestVectorCapacityBaseBatchPreservesFulltext(t *testing.T) {
	engine := newNamespacedEngine(t)
	node := &storage.Node{ID: "owner", ChunkEmbeddings: [][]float32{{1, 0, 0}}, Properties: map[string]any{"text": "batch document"}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	// No limits selects the real batch route: IndexBatch writes fulltext and
	// indexNodeLocked(skipFulltext=true) then populates the vector entries.
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(t.Context()))
	text, ok := svc.fulltext().GetDocument("owner")
	require.True(t, ok)
	require.Contains(t, text, "batch document")
	require.Equal(t, 1, svc.vectorIndex.Count())
}

func TestVectorCapacityPropertyRemovalPreservesSiblingCharges(t *testing.T) {
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 36})
	t.Cleanup(func() { _ = svc.Close() })
	node := &storage.Node{ID: "owner", NamedEmbeddings: map[string][]float32{"name": {1, 0, 0}}, Properties: map[string]any{"embedding": []float32{0, 1, 0}, "other": []float32{0, 0, 1}, "text": "preserved fulltext"}}
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, int64(36), svc.vectorResidentBytes)
	fulltextResident, fulltextMetadata := svc.bm25ResidentBytes, svc.bm25MetadataBytes
	metadata := svc.vectorMetadataBytes
	svc.metadataMemoryMaxBytes = metadata
	svc.RemovePropertyVectorIndex("embedding")
	require.Equal(t, int64(24), svc.vectorResidentBytes)
	require.Equal(t, 2, svc.vectorIndex.Count())
	require.Equal(t, metadata-int64(len("owner-prop-embedding")+16), svc.vectorMetadataBytes)
	require.Equal(t, fulltextResident, svc.bm25ResidentBytes)
	require.Equal(t, fulltextMetadata, svc.bm25MetadataBytes)
	text, ok := svc.fulltext().GetDocument("owner")
	require.True(t, ok)
	require.Contains(t, text, "preserved fulltext")
	// A short new node fits the reclaimed payload and metadata budgets.
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "n", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	require.Equal(t, int64(36), svc.vectorResidentBytes)
	require.NoError(t, svc.RemoveNode("owner"))
	require.Equal(t, int64(12), svc.vectorResidentBytes)
	require.NoError(t, svc.RemoveNode("n"))
	require.Zero(t, svc.vectorResidentBytes)
	require.Zero(t, svc.vectorMetadataBytes)
}
