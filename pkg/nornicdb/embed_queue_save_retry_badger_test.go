package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Badger validates sidecar freshness against the body's UpdatedAt. Memory
// storage does not, so a provider-count-only test cannot detect stale stamps.
func TestEmbedWorkerSaveRetryAfterSameSourceUpdate(t *testing.T) {
	for _, mode := range []string{"whole", "document-batch"} {
		for _, update := range []string{"idempotent", "bookkeeping"} {
			t.Run(mode+"/"+update, func(t *testing.T) {
				worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1)
				original, err := queue.GetNode("node-000")
				require.NoError(t, err)
				dir := t.TempDir()
				disk, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				require.NoError(t, err)
				t.Cleanup(func() { _ = disk.Close() })
				store := storage.NewNamespacedEngine(disk, "retry-freshness")
				_, err = store.CreateNode(original)
				require.NoError(t, err)
				queue.orderedEmbeddingQueue.Engine = store

				var published *storage.Node
				worker.SetOnEmbedded(func(node *storage.Node) { published = copyNodeForEmbedding(node) })
				require.True(t, worker.processNextBatch())
				require.Nil(t, published, "rejected writes cannot publish")
				before, err := store.GetNode(original.ID)
				require.NoError(t, err)
				current := copyNodeForEmbedding(before)
				current.UpdatedAt = before.UpdatedAt.Add(time.Second)
				if update == "bookkeeping" {
					current.Properties["updatedAt"] = "new bookkeeping value"
				}
				require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: current}))
				current, err = store.GetNode(original.ID)
				require.NoError(t, err)
				require.False(t, before.UpdatedAt.Equal(current.UpdatedAt))
				require.True(t, (storage.EmbeddingTextPolicy{IncludeLabels: true}).SameSource(before, current))

				makeNodeSaveRetriesReady(worker)
				require.True(t, worker.processNextBatch())
				require.NotNil(t, published)
				require.Equal(t, int64(1), worker.processed.Load())
				require.Equal(t, 2, provider.GetEmbedCount(), "request a fresh provider result even after a source-preserving update")
				require.True(t, current.UpdatedAt.Equal(published.UpdatedAt), "publication must carry the fresh body")
				require.Equal(t, current.Properties, published.Properties)
				saved, err := store.GetNode(original.ID)
				require.NoError(t, err)
				require.NotEmpty(t, saved.ChunkEmbeddings, "successful retry must be readable from Badger")
				require.Equal(t, published.ChunkEmbeddings, saved.ChunkEmbeddings)
				require.Equal(t, provider.Model(), saved.EmbedMeta["embedding_model"])
				if mode == "document-batch" {
					require.Equal(t, "retry-test", saved.EmbedMeta["chunker_version"])
				}

				worker.Close()
				require.NoError(t, disk.Close())
				reopened, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				require.NoError(t, err)
				defer reopened.Close()
				restored, err := storage.NewNamespacedEngine(reopened, "retry-freshness").GetNode(original.ID)
				require.NoError(t, err)
				require.Equal(t, saved.ChunkEmbeddings, restored.ChunkEmbeddings, "the persisted payload must survive reopen")
				require.Equal(t, 2, provider.GetEmbedCount())
			})
		}
	}
}
