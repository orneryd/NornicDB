// Author: NOR-288/NOR-292 lane (Codex), 2026-10-29.
package storage_test

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAcceptedEnvelopeMetadataNilEmptyImages(t *testing.T) {
	// Node/Edge declarations supply the collection population. Compare accepted
	// native images, including the merged codec's nil-to-empty property contract.
	for _, field := range []string{"Labels", "EmbedMeta", "NamedEmbeddings", "ChunkEmbeddings", "NodeProperties", "EdgeProperties"} {
		for _, shape := range []string{"nil", "empty", "populated"} {
			t.Run(field+"/"+shape, func(t *testing.T) {
				t.Cleanup(config.WithWALEnabled())
				base, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				dir := t.TempDir()
				wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: "immediate"})
				require.NoError(t, err)
				owner := storage.NewWALEngine(base, wal)
				manager, err := multidb.NewDatabaseManager(t.Context(), owner, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				name := manager.DefaultDatabaseName()
				_, err = manager.GetStorage(name)
				require.NoError(t, err)
				at := time.Unix(1700000000, 123)
				node := &storage.Node{ID: storage.NodeID(name + ":n"), CreatedAt: at, UpdatedAt: at}
				edge := &storage.Edge{ID: storage.EdgeID(name + ":e"), StartNode: node.ID, EndNode: node.ID, Type: "SELF", CreatedAt: at, UpdatedAt: at}
				var metadata map[string]any
				var vectors map[string][]float32
				var chunks [][]float32
				var labels []string
				if shape == "empty" {
					metadata, vectors, chunks, labels = map[string]any{}, map[string][]float32{}, [][]float32{}, []string{}
				} else if shape == "populated" {
					metadata, vectors = map[string]any{"model": "kept", "nested": map[string]any{}}, map[string][]float32{"default": {1, 2}}
					chunks, labels = [][]float32{{1, 2}}, []string{"Kept"}
				}
				switch field {
				case "Labels":
					node.Labels = labels
				case "EmbedMeta":
					node.EmbedMeta = metadata
				case "NamedEmbeddings":
					node.NamedEmbeddings = vectors
				case "ChunkEmbeddings":
					node.ChunkEmbeddings = chunks
				case "NodeProperties":
					node.Properties = metadata
				case "EdgeProperties":
					edge.Properties = metadata
				}
				tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
				require.NoError(t, err)
				_, err = tx.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, tx.CreateEdge(edge))
				prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "metadata"})
				require.NoError(t, err)
				t.Cleanup(prepared.Abort)
				receipt, err := owner.SubmitEntityCommit(t.Context(), prepared, nil)
				require.NoError(t, err)
				require.NoError(t, receipt.Wait(t.Context()))
				image := func(engine storage.Engine) []byte {
					t.Helper()
					n, err := storage.GetStoredNode(engine, node.ID)
					require.NoError(t, err)
					e, err := storage.GetStoredEdge(engine, edge.ID)
					require.NoError(t, err)
					encoded, err := json.Marshal([]any{n.Labels, n.Properties, n.EmbedMeta, n.NamedEmbeddings, n.ChunkEmbeddings, e.Properties})
					require.NoError(t, err)
					return encoded
				}
				live := image(base)
				liveBytes, liveNodes, liveEdges := manager.GetStorageSize(name)
				require.NoError(t, manager.Close())
				cold, replay, err := storage.RecoverFromWALWithResult(dir, "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = cold.Close() })
				require.Empty(t, replay.Errors)
				require.Equal(t, live, image(cold), "same-engine accepted metadata image bytes survive cold replay")
				recount, err := multidb.NewDatabaseManager(t.Context(), cold, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = recount.Close() })
				coldBytes, coldNodes, coldEdges := recount.GetStorageSize(name)
				require.Equal(t, liveBytes, coldBytes)
				require.Equal(t, liveNodes, coldNodes)
				require.Equal(t, liveEdges, coldEdges)
			})
		}
	}
}
