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

func TestVectorRestorePreservesFulltextOutsideVectorMetadata(t *testing.T) {
	for _, engineName := range []string{"v1", "v2"} {
		for _, fresh := range []bool{false, true} {
			for _, metadataOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/fresh=%t/metadataOnly=%t", engineName, fresh, metadataOnly), func(t *testing.T) {
					engine := newNamespacedEngine(t)
					indexedEngine := &iteratorEngine{Engine: engine}
					dir := t.TempDir()
					configure := func(s *Service) {
						s.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
						s.SetVectorIndexPath(filepath.Join(dir, "vectors"))
						s.SetPersistenceEnabled(true)
					}
					options := &ServiceOptions{BM25MemoryMaxBytes: 1}
					cText := "y"
					if metadataOnly {
						options = &ServiceOptions{MetadataMemoryMaxBytes: 60}
						cText = "y z"
					}
					newService := func() *Service {
						s := NewServiceWithDimensionsAndBM25EngineAndOptions(indexedEngine, 3, engineName, options)
						configure(s)
						t.Cleanup(func() { require.NoError(t, s.Close()) })
						return s
					}
					svc := newService()
					a := &storage.Node{ID: "a", Properties: map[string]any{"text": "x"}}
					_, err := engine.CreateNode(a)
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(a))
					require.NoError(t, waitNodeAdmission(t, svc))
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					require.Equal(t, int64(26), svc.bm25MetadataBytes)
					svc.ClearVectorIndex()
					require.NotContains(t, svc.nodeLabels, "a")
					b := &storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}}
					_, err = engine.CreateNode(b)
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(b))
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					svc.PersistIndexesToDisk()
					indexedEngine.iterateErr = errors.New("persisted indexes must not iterate storage")
					if fresh {
						require.NoError(t, svc.Close())
						svc = newService()
					}
					require.NoError(t, svc.BuildIndexes(context.Background()))
					require.True(t, svc.IsReady())
					text, ok := svc.fulltext().GetDocument("a")
					require.True(t, ok)
					require.Equal(t, "x", text)
					require.NotContains(t, svc.nodeLabels, "a")
					require.Equal(t, "b-named-v", svc.nodeNamedVector["b"]["v"])
					assert.Equal(t, int64(1), svc.bm25ResidentBytes, "restored text-only document must remain charged")
					assert.Equal(t, int64(26), svc.bm25MetadataBytes)
					require.Zero(t, svc.indexCapacityByNode["a"].vectorMetadata, "restore must not invent cleared vector metadata for the surviving text")
					c := &storage.Node{ID: "c", Properties: map[string]any{"text": cText}}
					err = svc.IndexNode(c)
					require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
					require.Equal(t, 1, svc.fulltext().Count(), "admission must reject before mutation")
					svc.SetPersistenceEnabled(false)
					require.Equal(t, int64(12), svc.vectorResidentBytes)
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					require.Zero(t, svc.indexCapacityByNode["a"].vectorMetadata)
					svc.SetPersistenceEnabled(true)
					require.NoError(t, svc.IndexNode(b))
					require.Zero(t, svc.vectorResidentBytes)
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					a.Properties["text"] = "z"
					require.NoError(t, svc.IndexNode(a))
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					svc.ClearVectorIndex()
					svc.ClearVectorIndex()
					require.Equal(t, int64(1), svc.bm25ResidentBytes)
					require.Equal(t, int64(26), svc.bm25MetadataBytes)
					require.Zero(t, svc.vectorMetadataBytes)
					require.ErrorIs(t, svc.IndexNode(c), ErrIndexMemoryBudgetExceeded)
					require.NoError(t, svc.RemoveNode("a"))
					require.NoError(t, svc.RemoveNode("a"))
					require.NoError(t, svc.IndexNode(c))
					require.NoError(t, svc.RemoveNode("b"))
					require.NoError(t, svc.RemoveNode("c"))
					require.Zero(t, svc.bm25ResidentBytes)
					require.Zero(t, svc.bm25MetadataBytes)
				})
			}
		}
	}
}

func TestBaseRestoreCapacityWithoutFileVectors(t *testing.T) {
	for _, engineName := range []string{"v1", "v2"} {
		for _, memoryVector := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/memoryVector=%t", engineName, memoryVector), func(t *testing.T) {
				engine := newNamespacedEngine(t)
				dir := t.TempDir()
				newService := func() *Service {
					s := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, engineName, &ServiceOptions{BM25MemoryMaxBytes: 1, VectorStorageMode: "memory"})
					s.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
					s.SetVectorIndexPath(filepath.Join(dir, "vectors"))
					s.SetPersistenceEnabled(true)
					t.Cleanup(func() { require.NoError(t, s.Close()) })
					return s
				}
				svc := newService()
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "a", Properties: map[string]any{"text": "x"}}))
				require.NoError(t, waitNodeAdmission(t, svc))
				svc.ClearVectorIndex()
				if memoryVector {
					require.NoError(t, svc.IndexNode(&storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {2, 0, 0}}}))
					// Legacy memory entries can predate query metadata. Their actual payload
					// still belongs to the existing source-capacity reconciliation owner.
					require.NoError(t, svc.vectorIndex.Add("legacy", []float32{1, 0, 0}))
				}
				svc.PersistIndexesToDisk()
				if !memoryVector {
					require.NoError(t, svc.Close())
					svc = newService()
				}
				require.NoError(t, svc.BuildIndexes(context.Background()))
				require.True(t, svc.IsReady())
				require.Nil(t, svc.vectorFileStore)
				require.Equal(t, int64(1), svc.bm25ResidentBytes)
				require.Equal(t, int64(26), svc.bm25MetadataBytes)
				require.Zero(t, svc.indexCapacityByNode["a"].vectorMetadata)
				if memoryVector {
					require.Equal(t, int64(36), svc.vectorResidentBytes)
					require.Equal(t, int64(12), svc.indexCapacityByNode["legacy"].vectorResident)
					require.Zero(t, svc.indexCapacityByNode["legacy"].vectorMetadata)
				} else {
					require.Zero(t, svc.vectorResidentBytes)
				}
				require.ErrorIs(t, svc.IndexNode(&storage.Node{ID: "c", Properties: map[string]any{"text": "y"}}), ErrIndexMemoryBudgetExceeded)
				require.Equal(t, 1, svc.fulltext().Count())
			})
		}
	}
}
