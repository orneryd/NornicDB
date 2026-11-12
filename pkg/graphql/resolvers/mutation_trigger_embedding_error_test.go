package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newTriggerEmbeddingMutationResolver builds the mutation resolver against db
// and proves the fixture has no auto-embed worker, so every failure asserted
// below comes from a deterministic worker/storage state rather than timing.
func newTriggerEmbeddingMutationResolver(t *testing.T, db *nornicdb.DB) *mutationResolver {
	t.Helper()
	require.Nil(t, db.EmbedQueueStats(), "fixture requires a disabled auto-embed worker")
	return &mutationResolver{NewResolver(db, testutil.DatabaseManager(t, db))}
}

// A trigger is not a best-effort hint: when the worker is unavailable the
// caller must see the failure instead of an EmbeddingStatus that implies work
// was queued.
func TestMutationTriggerEmbeddingReportsUnavailableWorker(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	mr := newTriggerEmbeddingMutationResolver(t, db)

	_, triggerErr := db.EmbedExisting(ctx)
	require.Error(t, triggerErr)
	require.Contains(t, triggerErr.Error(), "auto-embed not enabled")

	status, err := mr.mutationTriggerEmbedding(ctx, nil)

	require.Nil(t, status, "a failed trigger must not return an embedding status")
	require.ErrorContains(t, err, "failed to trigger embedding: ")
	require.ErrorContains(t, err, triggerErr.Error())

	wrapped := errors.Unwrap(err)
	require.Error(t, wrapped, "the stage error must stay in the wrapped chain")
	require.Contains(t, wrapped.Error(), "auto-embed not enabled")
}

// Regeneration is staged (clear, reset, trigger). A badger-backed database with
// no worker clears successfully and then fails to reset, which is exactly the
// state the previous implementation reported as success.
func TestMutationTriggerEmbeddingRegenerateFailsWhenWorkerResetFails(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	mr := newTriggerEmbeddingMutationResolver(t, db)

	_, err := db.GetStorage().CreateNode(ctx, &storage.Node{
		ID:              "regen-seed",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)
	resetErr := db.ResetEmbedWorker()
	require.Error(t, resetErr)
	require.Contains(t, resetErr.Error(), "auto-embed not enabled")

	regenerate := true
	status, err := mr.mutationTriggerEmbedding(ctx, &regenerate)

	require.Nil(t, status, "a failed regeneration must not return an embedding status")
	require.ErrorContains(t, err, "failed to reset embedding worker: ")
	require.ErrorContains(t, err, resetErr.Error())
	require.NotContains(t, err.Error(), "failed to clear existing embeddings")
	require.NotContains(t, err.Error(), "failed to trigger embedding")

	wrapped := errors.Unwrap(err)
	require.Error(t, wrapped, "the stage error must stay in the wrapped chain")
	require.Contains(t, wrapped.Error(), "auto-embed not enabled")

	// Stage order proof: the clear stage ran and succeeded before the reset
	// stage failed, and the trigger stage never ran.
	node, err := db.GetStorage().GetNode(ctx, "regen-seed")
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
}

// The in-memory engine is the public fixture whose storage does not support
// ClearAllEmbeddings, so it isolates the first regeneration stage: the mutation
// must report that failure and never start the reset or trigger stages.
func TestMutationTriggerEmbeddingRegenerateFailsWhenClearFails(t *testing.T) {
	ctx := context.Background()
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mr := newTriggerEmbeddingMutationResolver(t, db)

	_, clearErr := db.ClearAllEmbeddings()
	require.Error(t, clearErr)
	require.Contains(t, clearErr.Error(), "does not support ClearAllEmbeddings")

	regenerate := true
	status, err := mr.mutationTriggerEmbedding(ctx, &regenerate)

	require.Nil(t, status, "a failed regeneration must not return an embedding status")
	require.ErrorContains(t, err, "failed to clear existing embeddings: ")
	require.ErrorContains(t, err, clearErr.Error())
	require.NotContains(t, err.Error(), "failed to reset embedding worker")
	require.NotContains(t, err.Error(), "failed to trigger embedding")

	wrapped := errors.Unwrap(err)
	require.Error(t, wrapped, "the stage error must stay in the wrapped chain")
	require.Contains(t, wrapped.Error(), "does not support ClearAllEmbeddings")
}
