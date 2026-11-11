package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A closed journal refuses a node's reset record. The triggerEmbedding
// resolver, called directly with regenerate, then returns the journal's error
// and no status, and the node keeps its vectors.
func TestTriggerEmbeddingRegenerateReportsARefusedJournalRecord(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	mr := &mutationResolver{NewResolver(db, testutil.DatabaseManager(t, db))}
	_, err := db.GetStorage().CreateNode(ctx, &storage.Node{ID: "v", Labels: []string{"Doc"}, Properties: map[string]any{"content": "v"},
		ChunkEmbeddings: [][]float32{{1, 2, 3}}})
	require.NoError(t, err)
	walEngine, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(t, ok, "the database's storage is journaled")
	require.NoError(t, walEngine.GetWAL().Close())

	regenerate := true
	status, err := mr.mutationTriggerEmbedding(ctx, &regenerate)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	require.Nil(t, status)
	node, err := db.GetStorage().GetNode(ctx, "v")
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1, "the node keeps its vectors")
}
