package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These are actual closed Badger files and pending WAL records emitted by the
// unchanged release/develop bases. No current writer or field-stripping shim
// creates this upgrade population. See testdata/wal-pre-identity/README.md.
func TestWALBindingGenuinePreIdentityUpgrade(t *testing.T) {
	// Run with the production WAL switch; earlier package tests may leave it off.
	t.Cleanup(config.WithWALEnabled())
	for _, fixture := range []string{"release", "develop"} {
		t.Run(fixture, func(t *testing.T) {
			root := fixtureowner.ExtractZip(t, filepath.Join("testdata", "wal-pre-identity", fixture+".zip"), "")
			dataDir, walDir := filepath.Join(root, "data"), filepath.Join(root, "wal")
			old, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dataDir).WithReadOnly(true).WithLogger(nil)))
			require.NoError(t, err)
			require.NoError(t, old.View(func(tx *badger.Txn) error {
				_, err := tx.Get(walPropertyIdentityKey)
				require.ErrorIs(t, err, badger.ErrKeyNotFound, "the base could not stamp a future identity")
				return nil
			}))
			require.NoError(t, old.Close())
			entries, err := ReadWALEntriesFromDir(walDir)
			require.NoError(t, err)
			require.Len(t, entries, 20)
			for _, entry := range entries {
				data, err := decodeBufferedWALRecord(entry)
				require.NoError(t, err)
				require.Empty(t, data.PropertyDictionaryID)
				require.Empty(t, data.PropertyBindings)
				require.Empty(t, data.PropKeys)
			}
			b := reviewBindingEngine(t, dataDir)
			identity := b.walPropertySource.identity
			require.NotEmpty(t, identity)
			w, err := NewWAL(walDir, DefaultWALConfig())
			require.NoError(t, err)
			defer w.Close()
			n, err := RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			require.Equal(t, 20, n)
			check := func() {
				node, err := b.GetNode("upgrade:a")
				require.NoError(t, err)
				require.Equal(t, int64(20), node.Properties["value"])
				stamp, ok := node.Properties["stamp"].(time.Time)
				require.True(t, ok, "the stored temporal type must survive the upgrade")
				require.True(t, time.Date(2026, 10, 23, 1, 2, 3, 4000, time.UTC).Equal(stamp))
				assertPropertyDictionaryBijection(t, b)
				require.False(t, b.walPropertySource.foreign)
				require.Equal(t, identity, b.walPropertySource.identity)
			}
			check()
			require.NoError(t, b.Close())
			b = reviewBindingEngine(t, dataDir)
			n, err = RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			require.Zero(t, n)
			check()
		})
	}
}
