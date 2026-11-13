package nornicdb_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// clearPublicationConfig is the store and worker shape of the report's
// reproduction: a journaled store whose reset is journaled per node, and no
// embedding worker of its own.
func clearPublicationConfig() *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Database.WALSnapshotInterval = 50 * time.Millisecond
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = 16
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	cfg.EmbeddingWorker.IncludeLabels = false
	cfg.EmbeddingWorker.NumWorkers = 1
	cfg.EmbeddingWorker.MaxRetries = 0
	cfg.EmbeddingWorker.BatchDelay = 0
	cfg.EmbeddingWorker.TriggerDebounceDelay = 0
	return cfg
}

// clearPublicationVector is a unit vector of the configured dimensions.
func clearPublicationVector() []float32 {
	vector := make([]float32, 16)
	vector[0] = 1
	return vector
}

// clearPublicationNode stores one node that carries managed embeddings, as the
// report's reproduction does.
func clearPublicationNode(t *testing.T, db *nornicdb.DB) {
	t.Helper()
	_, err := db.GetStorage().CreateNode(context.Background(), &storage.Node{
		ID:              "v",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"key": "v", "text": "t"},
		ChunkEmbeddings: [][]float32{clearPublicationVector()},
		EmbedMeta:       map[string]any{"has_embedding": true},
	})
	require.NoError(t, err)
}

// The serving vector index is a publication of the stored vectors, so it
// follows the stored reset instead of preceding it: a clear the journal refuses
// resets no node, and the index keeps serving the vectors storage still holds.
// Report 47 (group B16).
func TestRefusedEmbeddingClearKeepsServingIndex(t *testing.T) {
	ctx := context.Background()
	db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), clearPublicationConfig())
	require.NoError(t, err)
	// The test closes the journal itself, so Close may find it already stopped.
	t.Cleanup(func() { _ = db.Close() })

	vector := clearPublicationVector()
	clearPublicationNode(t, db)

	svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.NoError(t, svc.EnsureWarm(ctx))

	options := search.DefaultSearchOptions()
	options.Limit = 5
	before, err := svc.Search(ctx, "", vector, options)
	require.NoError(t, err)
	require.Len(t, before.Results, 1, "the node is served before the reset")

	// A closed journal refuses the reset of the node's embeddings.
	journaled, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(t, ok, "the default store is journaled")
	require.NoError(t, journaled.GetWAL().Close())

	count, err := db.ClearAllEmbeddings()
	require.Error(t, err, "the refused reset reports the journal's refusal")
	require.Zero(t, count, "no node was reset")

	// Storage still holds the vector, so the index must still serve it.
	stored, err := db.GetStorage().GetNode(ctx, "v")
	require.NoError(t, err)
	require.Len(t, stored.ChunkEmbeddings, 1, "the refused reset leaves the vectors in storage")
	require.Equal(t, 1, svc.EmbeddingCount(), "the refused reset leaves the index entries")

	after, err := svc.Search(ctx, "", vector, options)
	require.NoError(t, err)
	require.Len(t, after.Results, 1, "vector search still finds a node that keeps its vector")
}

// The cleared vectors are published once the reset commits: a completed reset
// leaves neither stored vectors nor served ones. The publication is the storage
// commit's own per-node refresh (indexNodeFromEvent -> RefreshNodeWithMutation),
// so it applies on the existing debounced flush rather than synchronously with
// the reset. Report 47 (group B16).
func TestCompletedEmbeddingClearPublishesAnEmptyIndex(t *testing.T) {
	ctx := context.Background()
	db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), clearPublicationConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	clearPublicationNode(t, db)

	svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.NoError(t, svc.EnsureWarm(ctx))
	require.Equal(t, 1, svc.EmbeddingCount(), "the stored vector is served before the reset")

	count, err := db.ClearAllEmbeddings()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	stored, err := db.GetStorage().GetNode(ctx, "v")
	require.NoError(t, err)
	require.Empty(t, stored.ChunkEmbeddings)
	require.Eventually(t, func() bool { return svc.EmbeddingCount() == 0 }, 10*time.Second, 5*time.Millisecond,
		"the reset's own node publication must leave the vector index empty")
}
