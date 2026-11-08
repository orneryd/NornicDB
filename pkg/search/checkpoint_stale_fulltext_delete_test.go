package search

import (
	"context"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The saved fulltext is only this database's rows under the saved population's
// source certificate. A source delete that never reached the search service
// leaves a saved row the source no longer holds; restart must rebuild from the
// source instead of replaying that row.
func TestCheckpointDoesNotRestoreFulltextRowDeletedWithoutDelivery(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	svc := compatStoreService(engine, dir)
	t.Cleanup(func() { _ = svc.Close(); _ = engine.Close() })
	for _, n := range compatStoreNodes {
		_, err = engine.CreateNode(t.Context(), storage.CopyNode(n))
		require.NoError(t, err)
	}
	require.NoError(t, svc.BuildIndexes(context.Background()))
	// Intentionally no search removal: the source loses a row the saved
	// fulltext still holds.
	require.NoError(t, engine.DeleteNode(t.Context(), storage.NodeID("nornic:c")))
	svc.PersistIndexesToDisk()
	require.NoError(t, svc.Close())
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	restored := compatStoreService(reopened, dir)
	defer func() { require.NoError(t, restored.Close()); require.NoError(t, reopened.Close()) }()
	require.NoError(t, restored.BuildIndexes(context.Background()))
	require.Equal(t, 2, restored.fulltext().Count(), "the rebuilt fulltext holds exactly the source's rows")
	_, stale := restored.fulltext().GetDocument("nornic:c")
	require.False(t, stale, "a deleted source row is not restored from the stale checkpoint")
	resp, err := restored.Search(context.Background(), "charlie", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Empty(t, resp.Results, "charlie")
	require.Equal(t, 2, restored.EmbeddingCount(), "a and b; the deleted node's vector is gone")
}
