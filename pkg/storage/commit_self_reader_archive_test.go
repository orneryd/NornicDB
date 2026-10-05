package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// countNodeVersionRecords counts the archived MVCC node version records.
func countNodeVersionRecords(t *testing.T, eng *BadgerEngine) int {
	t.Helper()
	count := 0
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixMVCCNode}))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

// A committing transaction stops counting as a snapshot reader once its
// conflicts are validated, so with no other reader its update archives no
// superseded version. With another transaction reading, the version is
// archived and that reader keeps its snapshot.
func TestCommitArchivesSupersededVersionOnlyForOtherReaders(t *testing.T) {
	eng := newTestEngine(t)
	_, err := eng.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	base := countNodeVersionRecords(t, eng)

	update := func(v int64) {
		t.Helper()
		tx, err := eng.BeginTransaction()
		require.NoError(t, err)
		node, err := tx.GetNode("test:a")
		require.NoError(t, err)
		node.Properties["v"] = v
		require.NoError(t, tx.UpdateNode(node))
		require.NoError(t, tx.Commit())
	}

	update(2)
	require.Equal(t, base, countNodeVersionRecords(t, eng), "no other reader: nothing archived")
	require.Zero(t, eng.activeMVCCSnapshotReaders.Load())

	reader, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	before, err := reader.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), before.Properties["v"])

	update(3)
	require.Greater(t, countNodeVersionRecords(t, eng), base, "another reader: superseded version archived")
	seen, err := reader.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), seen.Properties["v"], "the reader keeps its snapshot")
	require.Equal(t, int64(1), eng.activeMVCCSnapshotReaders.Load())

	latest, err := eng.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(3), latest.Properties["v"])
}
