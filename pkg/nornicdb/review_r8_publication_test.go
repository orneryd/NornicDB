package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewSourcePreservingAcceptancePublication(t *testing.T) {
	ctx := context.Background()
	disk, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer disk.Close()
	store := storage.NewNamespacedEngine(disk, "review")
	_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "same", "text": "antiquecobalt"}})
	require.NoError(t, err)
	payload, err := store.GetNode("doc")
	require.NoError(t, err)
	payload.ChunkEmbeddings = [][]float32{{1, 0}}
	payload.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1, embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
	service := search.NewServiceWithDimensions(store, 2)
	defer service.Close()
	require.NoError(t, service.BuildIndexes(ctx))
	var observed *storage.Node
	store.OnNodeUpdated(func(node *storage.Node) {
		observed = storage.CopyNode(node)
		require.NoError(t, service.IndexNode(node))
	})
	// An ordinary body update lands after the provider snapshot but before
	// final acceptance; the provider consumes only id, so the source-preserving sidecar is valid.
	current, err := store.GetNode("doc")
	require.NoError(t, err)
	current.Properties["text"] = "novelquartz"
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	require.NoError(t, store.UpdateNode(current))
	opts := search.DefaultSearchOptions()
	opts.MinRRFScore = 0
	before, err := service.Search(ctx, "novelquartz", nil, opts)
	require.NoError(t, err)
	require.Len(t, before.Results, 1, "new body must be indexed before embedding write")
	require.NoError(t, store.UpdateNodeEmbeddingSidecar(payload))
	saved, err := store.GetNode("doc")
	require.NoError(t, err)
	require.Equal(t, "novelquartz", saved.Properties["text"])
	require.NotEmpty(t, saved.ChunkEmbeddings, "source-preserving result should be accepted")
	require.NotNil(t, observed)
	assert.Equal(t, "novelquartz", observed.Properties["text"], "embedding-only notification must publish the current body with the accepted vectors")
	after, err := service.Search(ctx, "novelquartz", nil, opts)
	require.NoError(t, err)
	assert.Len(t, after.Results, 1, "accepted embedding must not roll back a newer fulltext index entry")
	t.Logf("stored text=%v published text=%v search hits before=%d after=%d", saved.Properties["text"], observed.Properties["text"], len(before.Results), len(after.Results))
}

func TestReviewConsumedSourceCypherPaths(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 1)
			node, err := queue.GetNode("node-000")
			require.NoError(t, err)
			node.Properties["id"] = "old"
			require.NoError(t, queue.UpdateNode(node))
			worker.config.PropertiesInclude = []string{"text"}
			worker.SetEmbedder(&retryMetadataPropertyEmbedder{mockEmbedder: provider, key: "id", declared: true})
			require.True(t, worker.processNextBatch())
			executor := cypher.NewStorageExecutor(queue.Engine)
			ctx := context.Background()
			query := func(statement string) {
				if explicit {
					_, err := executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := executor.Execute(ctx, statement, nil)
				require.NoError(t, err)
				if explicit {
					_, err := executor.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			}
			query("MATCH (n:Doc) SET n.updatedAt = 'between-passes', n.ignored = 'one' RETURN n.id")
			makeNodeSaveRetriesReady(worker)
			require.True(t, worker.processNextBatch())
			require.Equal(t, 2, provider.GetEmbedCount())
			query("MATCH (n:Doc) SET n.ignored = 'two' RETURN n.id")
			node, err = queue.GetNode("node-000")
			require.NoError(t, err)
			require.NotEmpty(t, node.ChunkEmbeddings)
			query("MATCH (n:Doc) SET n.id = 'new' RETURN n.id")
			node, err = queue.GetNode("node-000")
			require.NoError(t, err)
			require.Empty(t, node.ChunkEmbeddings, "both query transaction paths must invalidate consumed metadata")
		})
	}
}
