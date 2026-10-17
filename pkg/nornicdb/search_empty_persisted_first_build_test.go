package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A new database with persisted search indexes starts over empty storage. Its
// first build must leave search ready, so the first document written is found
// by fulltext search without a restart, and again after one.
func TestFreshPersistedDatabaseServesFulltextWithoutRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *DB {
		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		cfg.Database.DataDir = dir
		cfg.Database.PersistSearchIndexes = true
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		_, err = db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		return db
	}
	requireReady := func(db *DB) {
		t.Helper()
		var status DatabaseSearchStatus
		require.Eventually(t, func() bool {
			status = db.GetDatabaseSearchStatus(db.defaultDatabaseName())
			return status.Ready || status.Error != ""
		}, 10*time.Second, 20*time.Millisecond)
		require.True(t, status.Ready, "search is not ready: phase=%q error=%q", status.Phase, status.Error)
		require.Empty(t, status.Error)
	}
	requireFound := func(db *DB, want int) {
		t.Helper()
		results, err := db.Search(ctx, "needle", nil, 5)
		require.NoError(t, err)
		require.Len(t, results, want)
		rows, err := db.ExecuteCypher(ctx, "CALL db.index.vector.queryNodes('doc_emb', 5, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.content AS content, score", nil)
		require.NoError(t, err)
		require.Len(t, rows.Rows, want)
	}

	db := open()
	requireReady(db)
	_, err := db.ExecuteCypher(ctx, "CREATE VECTOR INDEX doc_emb FOR (n:Doc) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "CREATE (:Doc {content: 'needle in the hay', embedding: [1.0, 0.0, 0.0]})", nil)
	require.NoError(t, err)
	requireReady(db)
	requireFound(db, 1)
	_, err = db.ExecuteCypher(ctx, "CREATE (:Doc {content: 'second needle', embedding: [0.0, 1.0, 0.0]})", nil)
	require.NoError(t, err)
	requireFound(db, 2)
	require.NoError(t, db.Close())

	db = open()
	t.Cleanup(func() { _ = db.Close() })
	requireReady(db)
	requireFound(db, 2)
}
