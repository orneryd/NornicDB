package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func openTestBadgerDB(t *testing.T) *badger.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrateBadgerToMsgpackWithDB_NilDBAndNoData(t *testing.T) {
	stats, err := MigrateBadgerToMsgpackWithDB(nil, "x", SerializerMigrationOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil badger db")
	require.Equal(t, "x", stats.DataDir)

	db := openTestBadgerDB(t)
	stats, err = MigrateBadgerToMsgpackWithDB(db, "empty", SerializerMigrationOptions{BatchSize: 0})
	require.NoError(t, err)
	require.Equal(t, "empty", stats.DataDir)
	require.False(t, stats.HasLegacyData)
	require.Equal(t, 0, stats.TotalScanned)
}

func TestMigratePrefixToMsgpack_ErrorAndSkipBranches(t *testing.T) {
	db := openTestBadgerDB(t)

	// Invalid legacy payload for node decode triggers decode error branch.
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte{prefixNode, 'n', '1'}, []byte("not-a-valid-node"))
	}))

	_, _, _, err := migratePrefixToMsgpack(
		db,
		func() migrationBatch { return db.NewWriteBatch() },
		prefixNode,
		"node",
		func(data []byte) (any, error) { return decodeNodeV1(data) },
		func(v any) ([]byte, error) { return encodeValue(v) },
		SerializerMigrationOptions{DryRun: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode node value")

	// Compact edge format is explicitly skipped.
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte{prefixEdge, 'e', '1'}, []byte{edgeFormatCompactV1, 0x00})
	}))

	converted, skipped, scanned, err := migratePrefixToMsgpack(
		db,
		func() migrationBatch { return db.NewWriteBatch() },
		prefixEdge,
		"edge",
		func(data []byte) (any, error) { return decodeEdge(data) },
		func(v any) ([]byte, error) { return encodeValue(v) },
		SerializerMigrationOptions{DryRun: true},
	)
	require.NoError(t, err)
	require.Equal(t, 0, converted)
	require.GreaterOrEqual(t, skipped, 1)
	require.GreaterOrEqual(t, scanned, 1)
}
