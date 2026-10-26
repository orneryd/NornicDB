package storage_test

import (
	"context"
	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Author: Lane #1644 (Codex). Query-derived public input classes, not a named
// test prescription: the existing namespace point owner accepts both forms.
func TestFoundationNamespaceBatchPointAcceptedKeys(t *testing.T) {
	for _, word := range []string{"", "T", "W", "TW", "WT"} {
		t.Run("wrappers="+word, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			tenant := storage.NewNamespacedEngine(base, "tenant")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"BatchKeys"}, Properties: map[string]interface{}{"id": string(id)}, ChunkEmbeddings: [][]float32{{1, 2}}})
				require.NoError(t, err)
			}
			var engine storage.Engine = tenant
			for _, token := range word {
				if token == 'T' {
					engine = storage.NewTracedEngine(engine)
				} else {
					wal, err := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, wal.Close()) })
					engine = storage.NewWALEngine(engine, wal)
				}
			}
			ids := []storage.NodeID{"a", "tenant:a", "b", "tenant:b", "missing", "tenant:missing"}
			// Independent point admission controls establish these are valid caller IDs.
			plain, err := engine.GetNode("a")
			require.NoError(t, err)
			prefixed, err := engine.GetNode("tenant:a")
			require.NoError(t, err)
			require.Equal(t, plain, prefixed)
			for _, mode := range []string{"full", "contextual", "light", "light-contextual"} {
				t.Run(mode, func(t *testing.T) {
					point := engine.GetNode
					batch := engine.BatchGetNodes
					if mode == "contextual" {
						batch = func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
							return storage.BatchGetNodesWithContext(context.Background(), engine, ids)
						}
					}
					if mode == "light" || mode == "light-contextual" {
						require.True(t, storage.HasReadCapability[storage.BatchNodeWithoutEmbeddingsReader](engine))
						point = func(id storage.NodeID) (*storage.Node, error) {
							return storage.ReadWithCapability(engine, func(reader storage.NodeWithoutEmbeddingsReader) (*storage.Node, error) {
								return reader.GetNodeWithoutEmbeddings(id)
							}, nil)
						}
						batch = func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
							return storage.ReadWithCapability(engine, func(reader storage.BatchNodeWithoutEmbeddingsReader) (map[storage.NodeID]*storage.Node, error) {
								if mode == "light-contextual" {
									contextual, ok := storage.OwnedCapability[interface {
										BatchGetNodesWithoutEmbeddingsWithContext(context.Context, []storage.NodeID) (map[storage.NodeID]*storage.Node, error)
									}](reader)
									require.True(t, ok)
									return contextual.BatchGetNodesWithoutEmbeddingsWithContext(context.Background(), ids)
								}
								return reader.BatchGetNodesWithoutEmbeddings(ids)
							}, nil)
						}
					}
					enginecontract.NodeBatchPoint(t, ids, batch, point)
				})
			}
		})
	}
}
