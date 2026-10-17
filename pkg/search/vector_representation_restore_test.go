package search

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorScaleReplacementPreservesRawCoordinates(t *testing.T) {
	for _, fileBacked := range []bool{false, true} {
		for _, bm25 := range []bool{false, true} {
			for _, shape := range []string{"named", "property", "main", "chunk"} {
				t.Run(fmt.Sprintf("file=%t/bm25=%t/%s", fileBacked, bm25, shape), func(t *testing.T) {
					payloadLimit := int64(24)
					if shape == "chunk" {
						payloadLimit += 12
					}
					svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: payloadLimit})
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					svc.SetIndexFlags(bm25, true)
					if fileBacked {
						svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
						svc.SetPersistenceEnabled(true)
					}
					makeNode := func(scale float32) *storage.Node {
						n := &storage.Node{ID: "a", Properties: map[string]any{"text": "x"}}
						v := []float32{scale, 0, 0}
						switch shape {
						case "named":
							n.NamedEmbeddings = map[string][]float32{"v": v}
						case "property":
							n.Properties["v"] = v
						case "main":
							n.ChunkEmbeddings = [][]float32{v}
						case "chunk":
							n.ChunkEmbeddings = [][]float32{{0, 1, 0}, v}
						}
						return n
					}
					for _, scale := range []float32{2, 3, 1, 2} {
						require.NoError(t, svc.IndexNode(makeNode(scale)))
						for _, similarity := range []string{"dot", "euclidean"} {
							hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0}, VectorQuerySpec{Property: "v", Similarity: similarity, Limit: 1})
							require.NoError(t, err)
							require.Len(t, hits, 1)
							want := float64(scale)
							if similarity == "euclidean" {
								delta := float64(scale - 1)
								want = 1 / (1 + delta*delta)
								if shape == "chunk" && want < 1.0/3 {
									want = 1.0 / 3
								}
							}
							if fileBacked {
								want = 1
							} // The file representation intentionally retains normalized coordinates only.
							assert.InDelta(t, want, hits[0].Score, 1e-6, "scale=%v %s", scale, similarity)
						}
						wantBytes := int64(24)
						if scale == 1 {
							wantBytes = 12
						}
						if shape == "chunk" {
							wantBytes += 12
						}
						if fileBacked {
							wantBytes = 0
						}
						assert.Equal(t, wantBytes, svc.vectorResidentBytes, "scale=%v", scale)
						svc.indexMu.RLock()
						assert.True(t, svc.nodeVectorStateUnchangedLocked(makeNode(scale)), "identical replacement must retain the fast path")
						svc.indexMu.RUnlock()
					}
				})
			}
		}
	}
}

func TestVectorRestoreReconstructsMetadataCapacity(t *testing.T) {
	for _, bm25Engine := range []string{"v1", "v2"} {
		for _, limit := range []int64{42, 41} {
			t.Run(fmt.Sprintf("%s/limit=%d", bm25Engine, limit), func(t *testing.T) {
				engine := newNamespacedEngine(t)
				node := &storage.Node{ID: "a", NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}, Properties: map[string]any{"text": "x"}}
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
				dir := t.TempDir()
				options := &ServiceOptions{MetadataMemoryMaxBytes: 42}
				configure := func(s *Service) {
					s.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
					s.SetVectorIndexPath(filepath.Join(dir, "vectors"))
					s.SetPersistenceEnabled(true)
				}
				first := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, bm25Engine, options)
				configure(first)
				require.NoError(t, first.IndexNode(node))
				require.Equal(t, int64(42), first.vectorMetadataBytes)
				fulltextResident, fulltextMetadata := first.bm25ResidentBytes, first.bm25MetadataBytes
				first.PersistIndexesToDisk()
				require.NoError(t, first.Close())
				restored := NewServiceWithDimensionsAndBM25EngineAndOptions(&iteratorEngine{Engine: engine, iterateErr: errors.New("restored source must not iterate storage")}, 3, bm25Engine, &ServiceOptions{MetadataMemoryMaxBytes: limit})
				t.Cleanup(func() { require.NoError(t, restored.Close()) })
				configure(restored)
				err = restored.BuildIndexes(context.Background())
				if limit < 42 {
					require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
					require.False(t, restored.IsReady())
					require.Equal(t, int64(42), restored.vectorMetadataBytes, "an over-budget source must still be charged")
					require.ErrorIs(t, restored.IndexNode(&storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}}), ErrIndexMemoryBudgetExceeded)
					require.Equal(t, 1, restored.EmbeddingCount())
					return
				}
				require.NoError(t, err)
				require.True(t, restored.IsReady())
				require.NotNil(t, restored.vectorFileStore)
				require.Equal(t, 1, restored.fulltext().Count())
				require.Equal(t, nodeNamedVectorKey("a", "v"), restored.nodeNamedVector["a"]["v"])
				assert.Equal(t, int64(42), restored.vectorMetadataBytes, "restored metadata must retain its admission charges")
				require.Equal(t, fulltextResident, restored.bm25ResidentBytes)
				require.Equal(t, fulltextMetadata, restored.bm25MetadataBytes)
				err = restored.IndexNode(&storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}})
				require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
				require.Equal(t, 1, restored.EmbeddingCount(), "rejection must precede mutation")
				node.NamedEmbeddings["v"] = []float32{0, 0, 1}
				require.NoError(t, restored.IndexNode(node))
				require.Equal(t, int64(42), restored.vectorMetadataBytes)
				restored.SetPersistenceEnabled(false)
				require.Nil(t, restored.vectorFileStore)
				require.Equal(t, int64(12), restored.vectorResidentBytes)
				require.Equal(t, int64(42), restored.vectorMetadataBytes)
				restored.SetPersistenceEnabled(true)
				require.NoError(t, restored.IndexNode(node))
				require.NotNil(t, restored.vectorFileStore)
				require.Zero(t, restored.vectorResidentBytes)
				require.Equal(t, int64(42), restored.vectorMetadataBytes)
				restored.ClearVectorIndex()
				restored.ClearVectorIndex()
				require.Zero(t, restored.vectorMetadataBytes)
				require.Equal(t, fulltextResident, restored.bm25ResidentBytes)
				require.Equal(t, fulltextMetadata, restored.bm25MetadataBytes)
				require.NoError(t, restored.IndexNode(&storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}}))
				require.NoError(t, restored.RemoveNode("a"))
				require.NoError(t, restored.RemoveNode("a"))
				require.Equal(t, int64(42), restored.vectorMetadataBytes)
				require.Zero(t, restored.bm25MetadataBytes)
				require.NoError(t, restored.RemoveNode("b"))
				require.Zero(t, restored.vectorMetadataBytes)
			})
		}
	}
}
