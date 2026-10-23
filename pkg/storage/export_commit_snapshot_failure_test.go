package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Stop only the background scheduler so tests can choose the flush boundary.
func (b *BadgerEngine) CommitConflictStopWriteBehindForTest() { b.writeBehind.Close() }

// CommitSnapshotFailureFixture installs a malformed primary node record while
// retaining its readable MVCC version. A newer head makes commit validate that
// primary baseline. Only test builds expose this fixture; all public query,
// transaction and protocol code runs unchanged.
func (b *BadgerEngine) CommitSnapshotFailureFixture(t *testing.T, id NodeID) string {
	t.Helper()
	node, err := b.GetNode(id)
	require.NoError(t, err)
	head, err := b.GetNodeCurrentHead(id)
	require.NoError(t, err)
	newer := head.Version
	newer.CommitSequence++
	newer.CommitTimestamp = newer.CommitTimestamp.Add(time.Hour)
	corrupt := []byte{0xff}
	_, decodeErr := b.decodeNode(namespaceForNodeID(id), corrupt)
	require.Error(t, decodeErr)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := b.writeNodeMVCCVersionInTxn(txn, node, head.Version); err != nil {
			return err
		}
		if err := b.writeNodeMVCCHeadInTxn(txn, id, newer, false); err != nil {
			return err
		}
		return txn.Set(nodeKey(id), corrupt)
	}))
	// The logical snapshot remains the readable prior version; the physical
	// snapshot contains the bad primary body that the conflict owner must read.
	readable, err := b.GetNodeVisibleAt(id, head.Version)
	require.NoError(t, err)
	require.Equal(t, node.Properties, readable.Properties)
	t.Cleanup(func() {
		require.NoError(t, b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(nodeKey(id))
			if err != nil {
				return err
			}
			return item.Value(func(value []byte) error {
				require.Equal(t, corrupt, value, "the failed commit must not publish its pending write")
				return nil
			})
		}))
	})
	return decodeErr.Error()
}
