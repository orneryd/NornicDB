package search

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorClearReleasesNodeMetadataBeforeDifferentIDInsertion(t *testing.T) {
	for _, fileBacked := range []bool{false, true} {
		name := "memory"
		if fileBacked {
			name = "file"
		}
		for _, bm25 := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bm25=%t", name, bm25), func(t *testing.T) {
				svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{MetadataMemoryMaxBytes: 42})
				t.Cleanup(func() { _ = svc.Close() })
				svc.SetIndexFlags(bm25, true)
				if fileBacked {
					svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
					svc.SetPersistenceEnabled(true)
				}
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "a", NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}, Properties: map[string]any{"text": "needle"}}))
				require.Equal(t, int64(42), svc.vectorMetadataBytes)
				fulltextResident, fulltextMetadata := svc.bm25ResidentBytes, svc.bm25MetadataBytes
				svc.ClearVectorIndex()
				assert.Zero(t, svc.vectorMetadataBytes, "Clear left removed node metadata charged")
				assert.Zero(t, svc.vectorResidentBytes)
				require.Equal(t, fulltextResident, svc.bm25ResidentBytes)
				require.Equal(t, fulltextMetadata, svc.bm25MetadataBytes)
				if bm25 {
					text, ok := svc.fulltext().GetDocument("a")
					require.True(t, ok)
					require.Equal(t, "needle", text)
				}
				svc.ClearVectorIndex() // repeated clearing preserves fulltext too
				require.Equal(t, fulltextResident, svc.bm25ResidentBytes)
				require.Equal(t, fulltextMetadata, svc.bm25MetadataBytes)
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "b", NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}}))
				require.NoError(t, svc.RemoveNode("a"))
				require.NoError(t, svc.RemoveNode("a"))
				require.Zero(t, svc.bm25ResidentBytes)
				require.Zero(t, svc.bm25MetadataBytes)
				require.Equal(t, int64(42), svc.vectorMetadataBytes)
				require.NoError(t, svc.RemoveNode("b"))
				require.Zero(t, svc.vectorMetadataBytes)
			})
		}
	}
}
