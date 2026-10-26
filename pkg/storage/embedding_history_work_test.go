package storage

import (
	"expvar"
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Replacing one current embedding must follow its live keys, not the number of
// accepted outcomes still retained for an older reader. The work probe counts
// calls to Badger's actual parseItem, rather than a storage-owned statistic.
func TestEmbeddingSidecarHistoryBoundedWork(t *testing.T) {
	metric, instrumented := expvar.Get("embed_pending_parse_work").(*expvar.Int)
	if os.Getenv("NORNICDB_EMBED_PENDING_WORK_COUNTER") == "required" {
		require.True(t, instrumented, "Badger's physical-work counter must be installed")
	}
	for _, operation := range []string{"sidecar", "update", "transaction", "delete"} {
		for _, history := range []int{0, 256} {
			t.Run(fmt.Sprintf("%s/history_%d", operation, history), func(t *testing.T) {
				engine := newTestMemoryEngine(t).BadgerEngine
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
				body := &Node{ID: "test:history-owner", Labels: []string{"Doc"}, Properties: map[string]any{"text": "original"}}
				_, err := engine.CreateNode(body)
				require.NoError(t, err)
				initial := [][]float32{{10, 20}, {30, 40}}
				seed := CopyNode(body)
				seed.ChunkEmbeddings = initial
				seed.EmbedMeta = map[string]any{"chunk_count": 2}
				require.NoError(t, engine.UpdateNodeEmbeddingSidecar(seed))

				// Keep the public transaction open: the old keys/versions must stay
				// readable, independently of compaction or cache timing.
				old, err := engine.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, old.Rollback()) })
				oldNode, err := old.GetNode(body.ID)
				require.NoError(t, err)
				require.Equal(t, initial, oldNode.ChunkEmbeddings)
				for i := 0; i < history; i++ {
					payload := CopyNode(body)
					payload.ChunkEmbeddings = [][]float32{{float32(i), 2}}
					payload.EmbedMeta = map[string]any{"chunk_count": 1}
					require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
				}
				current, err := engine.GetNode(body.ID)
				require.NoError(t, err)
				current.Properties = map[string]any{"text": "changed"}
				var before int64
				if instrumented {
					before = metric.Value()
				}
				switch operation {
				case "sidecar":
					payload := CopyNode(body)
					payload.ChunkEmbeddings = [][]float32{{-1, -2}}
					payload.EmbedMeta = map[string]any{"chunk_count": 1}
					require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
				case "update":
					require.NoError(t, engine.UpdateNode(current))
				case "transaction":
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.UpdateGraphNode(current, ClientNodeEdits))
					require.NoError(t, tx.Commit())
				case "delete":
					require.NoError(t, engine.DeleteNode(body.ID))
				}
				if instrumented {
					work := metric.Value() - before
					t.Logf("EMBEDDING_HISTORY_WORK operation=%s history=%d parse_items=%d", operation, history, work)
					require.LessOrEqual(t, work, int64(64), "embedding replacement walked retained versions")
				}
				if operation == "delete" {
					_, err := engine.GetNode(body.ID)
					require.ErrorIs(t, err, ErrNotFound)
				} else {
					got, err := engine.GetNode(body.ID)
					require.NoError(t, err)
					if operation == "sidecar" {
						require.Equal(t, [][]float32{{-1, -2}}, got.ChunkEmbeddings)
					} else {
						require.Empty(t, got.ChunkEmbeddings)
						require.Equal(t, "changed", got.Properties["text"])
					}
				}
				// A removed chunk must stay absent even while its historical
				// versions are retained, and the old reader must keep its vectors.
				require.NoError(t, engine.withView(func(txn *badger.Txn) error {
					_, err := txn.Get(embeddingKey(body.ID, 1))
					require.ErrorIs(t, err, badger.ErrKeyNotFound)
					return nil
				}))
				oldNode, err = old.GetNode(body.ID)
				require.NoError(t, err)
				require.Equal(t, initial, oldNode.ChunkEmbeddings)
			})
		}
	}
}
