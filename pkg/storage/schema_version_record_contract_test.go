package storage

import (
	"encoding/hex"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The literal key/payload witnesses are persisted-format contracts, independent
// of the accessor used by production. Refactoring access must preserve them.
func TestSchemaVersionRecordBytes(t *testing.T) {
	engine := newTestEngine(t)
	db := engine.db
	for _, test := range []struct {
		name    string
		version int
		wire    string
	}{
		{"zero", 0, "0000000000000000"},
		{"one", 1, "0000000000000001"},
		{"catalog V4", 4, "0000000000000004"},
		{"ordinary", 42, "000000000000002a"},
		{"low byte", 255, "00000000000000ff"},
		{"next byte", 256, "0000000000000100"},
		{"signed conversion", -1, "ffffffffffffffff"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, engine.writeSchemaVersion(test.version))
			want, err := hex.DecodeString(test.wire)
			require.NoError(t, err)
			require.NoError(t, db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte{0x10, 0x02})
				if err != nil {
					return err
				}
				got, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				require.Equal(t, want, got)
				return nil
			}))
			version, err := engine.readSchemaVersion()
			require.NoError(t, err)
			require.Equal(t, test.version, version)
		})
	}
}

func TestSchemaVersionTxnAdmission(t *testing.T) {
	db := openTestBadgerDB(t)
	read := func(txn *badger.Txn) (int, error) { return accessSchemaVersionInTxn(txn, loadSchemaVersion, 0) }
	write := func(txn *badger.Txn, version int) error {
		_, err := accessSchemaVersionInTxn(txn, storeSchemaVersion, version)
		return err
	}
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		version, err := read(txn)
		require.NoError(t, err)
		require.Zero(t, version)
		require.ErrorIs(t, write(txn, 4), badger.ErrReadOnlyTxn)
		return nil
	}))
	txn := db.NewTransaction(false)
	txn.Discard()
	_, err := read(txn)
	require.ErrorIs(t, err, badger.ErrDiscardedTxn)
	for _, test := range []struct {
		name      string
		wire      []byte
		version   int
		errorText string
	}{
		{"legacy ready marker", []byte{1}, 0, ""},
		{"different one byte", []byte{2}, 0, "invalid schema version length: 1"},
		{"empty record", []byte{}, 0, "invalid schema version length: 0"},
		{"short record", []byte{0, 0, 0}, 0, "invalid schema version length: 3"},
		{"eight byte version", []byte{0, 0, 0, 0, 0, 0, 0, 4}, 4, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, db.Update(func(txn *badger.Txn) error { return txn.Set([]byte{0x10, 0x02}, test.wire) }))
			require.NoError(t, db.View(func(txn *badger.Txn) error {
				version, err := read(txn)
				if test.errorText != "" {
					require.EqualError(t, err, test.errorText)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, test.version, version)
				return nil
			}))
		})
	}
}
