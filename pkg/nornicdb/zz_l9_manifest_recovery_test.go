package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL9EmbeddedMutationSurvivesDamagedStoreOpen(t *testing.T) {
	ctx := t.Context()
	root := filepath.Join(t.TempDir(), "data")
	cfg := DefaultConfig()
	cfg.Database.StrictDurability = true
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	open := func() *DB {
		t.Helper()
		db, err := Open(root, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	verify := func(db *DB, stage string) {
		t.Helper()
		p, err := db.ExecuteCypher(ctx, "MATCH (n:P {key:'p'}) RETURN n.hidden", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{true}}, p.Rows, stage)
		q, err := db.ExecuteCypher(ctx, "MATCH (n:Q) RETURN n.key", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"q"}}, q.Rows, stage)
	}

	db := open()
	for _, query := range []string{"CREATE (:P {key:'p'})", "MATCH (n:P {key:'p'}) SET n.hidden = true"} {
		_, err := db.ExecuteCypher(ctx, query, nil)
		require.NoError(t, err)
	}
	_, err := db.GetStorage().CreateNode(ctx, &storage.Node{ID: "q-control", Labels: []string{"Q"}, Properties: map[string]any{"key": "q"}})
	require.NoError(t, err)
	require.NoError(t, db.GetBaseStorageForManager().(*storage.WALEngine).GetWAL().Sync())
	verify(db, "before close")
	require.NoError(t, db.Close())

	db = open()
	verify(db, "normal reopen")
	require.NoError(t, db.Close())

	require.NoError(t, os.WriteFile(filepath.Join(root, "MANIFEST"), []byte("damaged"), 0o600))
	db = open()
	preserved, err := filepath.Glob(root + ".corrupted-*")
	require.NoError(t, err)
	require.Len(t, preserved, 1, "the prefix proves the real startup recovery ran")
	verify(db, "damaged store rebuilt from journal")
}
