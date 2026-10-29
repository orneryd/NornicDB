package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Standalone readers and foreign import reject unconverted historical WALs.
// The startup migration separately converts the database's own WAL directory.
func TestWALBindingGenuinePreIdentityUpgrade(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, fixture := range []string{"release", "develop"} {
		t.Run(fixture, func(t *testing.T) {
			root := fixtureowner.ExtractZip(t, filepath.Join("testdata", "wal-pre-identity", fixture+".zip"), "")
			dataDir, walDir := filepath.Join(root, "data"), filepath.Join(root, "wal")
			old, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dataDir).WithReadOnly(true).WithLogger(nil)))
			require.NoError(t, err)
			require.NoError(t, old.View(func(tx *badger.Txn) error {
				_, err := tx.Get(walPropertyIdentityKey)
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
				return nil
			}))
			require.NoError(t, old.Close())
			path := filepath.Join(walDir, "wal.log")
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			entries, err := ReadWALEntriesFromDir(walDir)
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			require.Empty(t, entries)
			w, err := NewWAL(walDir, DefaultWALConfig())
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			require.Nil(t, w)
			b := reviewBindingEngine(t, dataDir)
			before, err := b.GetNode("upgrade:a")
			require.NoError(t, err)
			require.Contains(t, before.Properties, "value")
			stamp, ok := before.Properties["stamp"].(time.Time)
			require.True(t, ok)
			require.True(t, time.Date(2026, 10, 23, 1, 2, 3, 4000, time.UTC).Equal(stamp))
			_, _, err = RecoverIntoEngine(b, walDir, "")
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			after, err := b.GetNode("upgrade:a")
			require.NoError(t, err)
			require.Equal(t, before.Properties, after.Properties)
			assertPropertyDictionaryBijection(t, b)
			require.NoError(t, b.Close())
			reopened := reviewBindingEngine(t, dataDir)
			defer reopened.Close()
			persisted, err := reopened.GetNode("upgrade:a")
			require.NoError(t, err)
			require.Equal(t, before.Properties, persisted.Properties)
			afterWAL, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, afterWAL, "refusal must not repair or rewrite the prior WAL")
		})
	}
}
