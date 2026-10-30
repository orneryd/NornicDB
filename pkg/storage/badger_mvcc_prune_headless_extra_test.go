package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PruneMVCCKeyspace_HeadlessLogicalKeyFallback(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	// Seed one headless logical node-key history directly in MVCC keyspace.
	v1 := MVCCVersion{CommitTimestamp: time.Now().Add(-2 * time.Hour).UTC(), CommitSequence: 1}
	v2 := MVCCVersion{CommitTimestamp: time.Now().Add(-1 * time.Hour).UTC(), CommitSequence: 2}
	numID := uint64(4242)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		rec1, err := encodeMVCCNodeRecord(&Node{ID: "test:headless", Labels: []string{"L"}}, false)
		require.NoError(t, err)
		rec2, err := encodeMVCCNodeRecord(nil, true)
		require.NoError(t, err)
		require.NoError(t, txn.Set(mvccNodeVersionKey(numID, v1), rec1))
		require.NoError(t, txn.Set(mvccNodeVersionKey(numID, v2), rec2))
		return nil
	}))

	// No head exists, so prune must synthesize one from latest record (v2 tombstone)
	// and compact away older versions for this logical key.
	var deleted int64
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		var err error
		deleted, err = engine.pruneMVCCKeyspaceInTxn(
			context.Background(),
			txn,
			[]byte{prefixMVCCNode},
			MVCCPruneOptions{MaxVersionsPerKey: 1},
			false,
		)
		return err
	}))
	require.GreaterOrEqual(t, deleted, int64(1))

	// Latest tombstone marker should remain.
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(mvccNodeVersionKey(numID, v2))
		require.NoError(t, err)
		_, err = txn.Get(mvccNodeVersionKey(numID, v1))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}
