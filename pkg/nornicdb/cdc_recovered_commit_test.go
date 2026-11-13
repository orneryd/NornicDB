package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// openChangeCapturingStore opens dir with durable synchronous writes and no
// automatic snapshot, the mode the report's writer used, and gives the default
// database its txLogEnrichment option as the server does: through the database
// manager, which owns the persisted option and the engine's capture mode.
func openChangeCapturingStore(t *testing.T, dir string) (*DB, *cypher.StorageExecutor) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	manager, err := multidb.NewDatabaseManager(t.Context(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.SetDatabaseOption(t.Context(), db.defaultDatabaseName(), multidb.OptionTxLogEnrichment, "DIFF"))
	engine, err := manager.GetStorage(db.defaultDatabaseName())
	require.NoError(t, err)
	// Use the managed statement owner the server uses, so this reproduction
	// does not depend on the separate embedded-journal correction.
	return db, cypher.NewStorageExecutor(engine)
}

// cdcCurrent is db.cdc.current(): the feed position just after the last
// committed change the statement sees.
func cdcCurrent(t *testing.T, db *DB) string {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), "CALL db.cdc.current() YIELD id RETURN id", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	cursor, ok := result.Rows[0][0].(string)
	require.True(t, ok, "cursor %#v", result.Rows[0][0])
	return cursor
}

// cdcChangedNodeNames reads the feed after cursor and returns the name of every
// changed node, in feed order.
func cdcChangedNodeNames(t *testing.T, db *DB, cursor string) []string {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(),
		"CALL db.cdc.query($from) YIELD id, event RETURN event.state.after.properties.name",
		map[string]any{"from": cursor})
	require.NoError(t, err)
	names := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 1)
		name, ok := row[0].(string)
		require.True(t, ok, "changed node name %#v", row[0])
		names = append(names, name)
	}
	return names
}

// countNodes is query's single count.
func countNodes(t *testing.T, db *DB, query string) int64 {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "count %#v", result.Rows[0][0])
	return count
}

// report 13: a journaled commit that start-up applies gets no change-data-
// capture event. The writer acknowledged the commit under txLogEnrichment DIFF
// and its store never applied it (a crash between journal and store write). The
// next open replays the commit, so the node is there; the change feed after the
// cursor the writer recorded must hold that commit's event, exactly once, and a
// second restart must not duplicate it.
func TestRecoveredCommitPublishesItsChangeEvent(t *testing.T) {
	root := t.TempDir()
	db, writer := openChangeCapturingStore(t, root)
	_, err := writer.Execute(t.Context(), "CREATE (:K {name: 'baseline'})", nil)
	require.NoError(t, err)
	cursor := cdcCurrent(t, db)
	require.NoError(t, db.Close())

	// The state a crash between journal and store write leaves: a store
	// without the later commit, and the journal that holds it.
	crashCopy := t.TempDir()
	require.NoError(t, os.CopyFS(crashCopy, os.DirFS(root)))

	db, writer = openChangeCapturingStore(t, root)
	_, err = writer.Execute(t.Context(), "CREATE (:K {name: 'recovered'})", nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	wal := filepath.Join(root, "wal")
	require.NoError(t, os.RemoveAll(filepath.Join(crashCopy, "wal")))
	require.NoError(t, os.CopyFS(filepath.Join(crashCopy, "wal"), os.DirFS(wal)))

	db, writer = openChangeCapturingStore(t, crashCopy)
	require.Equal(t, int64(2), countNodes(t, db, "MATCH (n:K) RETURN count(n)"), "the recovered commit's node is stored")
	require.Equal(t, []string{"recovered"}, cdcChangedNodeNames(t, db, cursor), "the recovered commit's change is in the feed")
	require.NoError(t, db.Close())

	db, writer = openChangeCapturingStore(t, crashCopy)
	t.Cleanup(func() { _ = db.Close() })
	require.Equal(t, []string{"recovered"}, cdcChangedNodeNames(t, db, cursor), "a second restart does not duplicate the recovered change")
}
