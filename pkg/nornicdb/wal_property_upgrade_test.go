package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The public constructor must gate V4 migration, recover the acknowledged
// historical WAL, and keep a byte-identical copy of the original log.
func TestOpenMigratesGenuinePreIdentityWAL(t *testing.T) {
	dir := fixtureowner.ExtractZip(t, filepath.Join("..", "storage", "testdata", "wal-pre-identity", "release.zip"), "data/")
	readPersistedStore := func() map[string][]byte {
		t.Helper()
		engine, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
		require.NoError(t, err)
		entries := make(map[string][]byte)
		require.NoError(t, engine.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				entries[string(item.KeyCopy(nil))] = value
			}
			return nil
		}))
		require.NoError(t, engine.Close())
		return entries
	}
	before := readPersistedStore()
	require.NotEmpty(t, before)
	walPath := filepath.Join(dir, "wal", "wal.log")
	walBytes, err := os.ReadFile(walPath)
	require.NoError(t, err)
	cfg := config.LoadDefaults()
	cfg.Database.AllowStorageUpgrade = false
	cfg.Database.StrictDurability = false
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.WALAutoCompactionEnabled = false
	for reopen := 0; reopen < 2; reopen++ {
		db, err := Open(dir, cfg)
		require.ErrorIs(t, err, storage.ErrWALUnsupportedVersion)
		require.Nil(t, db)
		require.Equal(t, before, readPersistedStore())
		after, err := os.ReadFile(walPath)
		require.NoError(t, err)
		require.Equal(t, walBytes, after)
	}
	cfg.Database.AllowStorageUpgrade = true
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	first := db
	t.Cleanup(func() { _ = first.Close() })
	node, err := db.GetBaseStorageForManager().GetNode("upgrade:a")
	require.NoError(t, err)
	require.Equal(t, int64(20), node.Properties["value"])
	archives, err := filepath.Glob(filepath.Join(dir, "wal.pre-v4-*"))
	require.NoError(t, err)
	require.Len(t, archives, 1)
	backup, err := os.ReadFile(filepath.Join(archives[0], "wal.log"))
	require.NoError(t, err)
	require.Equal(t, walBytes, backup)
	require.NoError(t, db.Close())
	cfg.Database.AllowStorageUpgrade = false
	db, err = Open(dir, cfg)
	require.NoError(t, err)
	second := db
	t.Cleanup(func() { _ = second.Close() })
	node, err = db.GetBaseStorageForManager().GetNode("upgrade:a")
	require.NoError(t, err)
	require.Equal(t, int64(20), node.Properties["value"])
	require.NoError(t, db.Close())
}
