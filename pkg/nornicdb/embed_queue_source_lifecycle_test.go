package nornicdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerConsumedSourceLifecycle(t *testing.T) {
	for _, route := range []string{"direct-single", "resolver-single", "resolver-batch"} {
		for _, wrapper := range []string{"plain", "cached", "traced", "cached-traced", "traced-cached"} {
			for _, chunked := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/chunked=%t", route, wrapper, chunked), func(t *testing.T) {
					count := 1
					if route == "resolver-batch" {
						count = 2
					}
					worker, queue, base := nodeSaveRetryFixture(t, "whole", count, 1)
					worker.config.PropertiesInclude = []string{"text"}
					ids := append([]storage.NodeID(nil), queue.pending...)
					dir := t.TempDir()
					disk, err := storage.NewBadgerEngine(dir)
					require.NoError(t, err)
					defer func() { _ = disk.Close() }()
					disk.SetEmbeddingsEnabled(true)
					store := storage.NewNamespacedEngine(disk, "contract")
					for _, id := range ids {
						n, e := queue.GetNode(id)
						require.NoError(t, e)
						n.Properties["id"] = "old"
						n.Properties["updatedAt"] = "old"
						_, e = store.CreateNode(n)
						require.NoError(t, e)
					}
					queue.orderedEmbeddingQueue.Engine = store
					structured := &retryMetadataPropertyEmbedder{mockEmbedder: base, key: "id", declared: true, chunked: chunked}
					var provider embed.Embedder = structured
					worker.config.EmbedBatchSize = 1
					if route == "resolver-batch" {
						provider = &retryMetadataPropertyBatchEmbedder{structured}
						worker.config.EmbedBatchSize = 4
					}
					switch wrapper {
					case "cached":
						provider = embed.NewCachedEmbedder(provider, 8)
					case "traced":
						provider = embed.NewTracedEmbedder(provider)
					case "cached-traced":
						provider = embed.NewCachedEmbedder(embed.NewTracedEmbedder(provider), 8)
					case "traced-cached":
						provider = embed.NewTracedEmbedder(embed.NewCachedEmbedder(provider, 8))
					}
					worker.SetEmbedder(provider)
					if route != "direct-single" {
						worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
					}
					mutate := func(key, value string) {
						for _, id := range ids {
							tx, e := disk.BeginTransaction()
							require.NoError(t, e)
							require.NoError(t, tx.SetNamespace("contract"))
							n, e := tx.GetNode(storage.NodeID("contract:" + string(id)))
							require.NoError(t, e)
							n.Properties[key] = value
							n.UpdatedAt = n.UpdatedAt.Add(time.Second)
							require.NoError(t, tx.UpdateNode(n))
							require.NoError(t, tx.Commit())
							if worker.ctx.Err() == nil {
								worker.Enqueue(string(id))
							}
						}
					}
					require.True(t, worker.processNextBatch())
					require.Zero(t, worker.processed.Load())
					mutate("updatedAt", "between-passes") // Not consumed by this provider or its fallback text.
					makeNodeSaveRetriesReady(worker)
					require.True(t, worker.processNextBatch())
					require.Equal(t, count, base.GetEmbedCount(), "unconsumed retry input must reuse")
					mutate("updatedAt", "after-save")
					mutate("ignored", "after-save")
					for _, id := range ids {
						n, e := store.GetNode(id)
						require.NoError(t, e)
						require.NotEmpty(t, n.ChunkEmbeddings)
						_, present, valid := embeddingsource.Read(n.EmbedMeta)
						require.True(t, present && valid)
					}
					mutate("id", "new")
					for _, id := range ids {
						n, e := store.GetNode(id)
						require.NoError(t, e)
						require.Empty(t, n.ChunkEmbeddings, "consumed metadata must invalidate after a successful retained save")
						queue.AddToPendingEmbeddings(id)
					}
					require.True(t, worker.processNextBatch())
					require.Equal(t, 2*count, base.GetEmbedCount())
					payloads := make([]*storage.Node, 0, count)
					for _, id := range ids {
						n, e := store.GetNode(id)
						require.NoError(t, e)
						require.Equal(t, float32(2), n.ChunkEmbeddings[0][0])
						if chunked {
							require.Len(t, n.ChunkEmbeddings, 2)
						}
						payloads = append(payloads, storage.CopyNode(n))
					}
					mutate("updatedAt", "in-flight-unconsumed")
					for _, payload := range payloads {
						require.NoError(t, store.UpdateNodeEmbeddingSidecar(payload))
						n, e := store.GetNode(payload.ID)
						require.NoError(t, e)
						require.NotEmpty(t, n.ChunkEmbeddings, "accepted unconsumed update must remain readable")
					}
					mutate("id", "old")
					for _, payload := range payloads {
						require.ErrorIs(t, store.UpdateNodeEmbeddingSidecar(payload), storage.ErrEmbeddingSourceChanged, "final storage guard must reject consumed metadata changes")
					}
					// Reopen exercises the serialized descriptor, not only the in-memory map.
					for _, id := range ids {
						queue.AddToPendingEmbeddings(id)
					}
					require.True(t, worker.processNextBatch())
					worker.Close()
					require.NoError(t, disk.Close())
					disk, err = storage.NewBadgerEngine(dir)
					require.NoError(t, err)
					disk.SetEmbeddingsEnabled(true)
					store = storage.NewNamespacedEngine(disk, "contract")
					mutate("updatedAt", "after-reopen")
					for _, id := range ids {
						n, e := store.GetNode(id)
						require.NoError(t, e)
						require.NotEmpty(t, n.ChunkEmbeddings)
					}
					mutate("id", "new")
					for _, id := range ids {
						n, e := store.GetNode(id)
						require.NoError(t, e)
						require.Empty(t, n.ChunkEmbeddings)
					}
				})
			}
		}
	}
}
