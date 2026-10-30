package nornicdb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestSol1482R4RestoreRebindsCypherConsumer(t *testing.T) {
	ctx := context.Background()
	c := DefaultConfig()
	c.Memory.EmbeddingEnabled = false
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingDimensions = 2
	db, err := Open(t.TempDir(), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.EnsureSearchIndexes(ctx, db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	waitDBWorkIdle(t, db)
	_, err = db.ExecuteCypher(ctx, "CREATE VECTOR INDEX docvec FOR (n:Doc) ON (n.vec) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = db.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"vec": []float32{1, 0}}})
	require.NoError(t, err)
	settleQueuedRefreshes()
	query := "CALL db.index.vector.queryNodes('docvec', 1, [1.0,0.0]) YIELD node, score RETURN score"
	first, err := db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.InDelta(t, 1, first.Rows[0][0], 1e-6)
	p := filepath.Join(t.TempDir(), "backup")
	require.NoError(t, db.Backup(ctx, p))
	require.NoError(t, db.storage.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"vec": []float32{0, 1}}}))
	settleQueuedRefreshes()
	later, err := db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, later.Rows, 1)
	require.InDelta(t, 0.5, later.Rows[0][0], 1e-6)
	require.NoError(t, db.Restore(ctx, p))
	_, err = db.EnsureSearchIndexes(ctx, db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	restored, err := db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, restored.Rows, 1)
	require.InDelta(t, 1, restored.Rows[0][0], 1e-6, "Cypher retained the replaced search service after restore")
}
