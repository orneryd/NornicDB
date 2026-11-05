package multidb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// markClosedASCIICatalogFixtureV3 pins only the released V3 marker, after the
// test's store has closed. These disposable ASCII catalog fixtures use the
// unchanged V3 body codec; this is a bounded constructed fixture, not an old
// binary qualification. Key 10/02 and big-endian uint64(3) are pinned to release
// d663ce8f1320e409e2eaa29560b987ffe6526207 (badger_helpers.go/badger.go).
// It preserves every other native key/value and records the baseline hash.
func markClosedASCIICatalogFixtureV3(t *testing.T, dir string) {
	t.Helper()
	db, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil).WithMemTableSize(8 << 20).WithBlockCacheSize(8 << 20)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	snapshot := func() map[string][]byte {
		result := make(map[string][]byte)
		require.NoError(t, db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().KeyCopy(nil)
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				result[string(key)] = value
			}
			return nil
		}))
		return result
	}
	before := snapshot()
	key := []byte{0x10, 0x02}
	marker := make([]byte, 8)
	binary.BigEndian.PutUint64(marker, 3)
	require.NoError(t, db.Update(func(txn *badger.Txn) error { return txn.Set(key, marker) }))
	require.NoError(t, db.Sync())
	after := snapshot()
	require.Equal(t, len(before), len(after))
	var keys []string
	for k, v := range before {
		keys = append(keys, k)
		if k != string(key) {
			require.True(t, bytes.Equal(v, after[k]), "V3 fixture changed another native value")
		}
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, k := range keys {
		hash.Write([]byte(k))
		hash.Write(after[k])
	}
	t.Logf("constructed release-V3 ASCII fixture keys=%d sha256=%x", len(after), hash.Sum(nil))
}

func openCatalogUpgradeFixture(t *testing.T, dir string, cfg *Config) (*storage.BadgerEngine, error) {
	t.Helper()
	plan, err := NewCatalogUpgradeV4(cfg.SystemDatabase, cfg.RemoteCredentialEncryptionKey)
	if err != nil {
		return nil, err
	}
	return fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, AllowStorageUpgrade: true, CatalogUpgradeV4: plan}))
}

func closedCatalogFixtureDigest(t *testing.T, dir string) [32]byte {
	t.Helper()
	db, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil).WithReadOnly(true).WithMemTableSize(8 << 20).WithBlockCacheSize(8 << 20)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	hash := sha256.New()
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			hash.Write(key)
			hash.Write(value)
		}
		return nil
	}))
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
