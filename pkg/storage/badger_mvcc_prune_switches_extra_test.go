package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerMVCC_WithViewNodeMVCCVersionsFromKey_UnknownDecodeAndYieldError(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	unknownKey := mvccNodeVersionKey(8_888_888, v)
	unknownRec, err := encodeMVCCNodeRecord(&Node{ID: "ghost:n", Labels: []string{"N"}}, false)
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error { return txn.Set(unknownKey, unknownRec) }))

	yieldCount := 0
	_, reachedEnd, err := engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 1, func(NodeID, MVCCVersion, bool) error {
		yieldCount++
		return nil
	})
	require.NoError(t, err)
	require.False(t, reachedEnd)
	require.Equal(t, 0, yieldCount)

	_, err = engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		vk, keyErr := engine.mvccNodeVersionKeyString(txn, "test:n1", v)
		if keyErr != nil {
			return keyErr
		}
		return txn.Set(vk, []byte("corrupt-node-mvcc-record"))
	}))

	_, _, err = engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 64, func(NodeID, MVCCVersion, bool) error {
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "msgpack")

	// Restore a valid record so yield-error branch can be exercised.
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		vk, keyErr := engine.mvccNodeVersionKeyString(txn, "test:n1", v)
		if keyErr != nil {
			return keyErr
		}
		rec, recErr := encodeMVCCNodeRecord(&Node{ID: "test:n1", Labels: []string{"N"}}, false)
		if recErr != nil {
			return recErr
		}
		return txn.Set(vk, rec)
	}))

	wantErr := errors.New("stop-yield")
	_, _, err = engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 64, func(NodeID, MVCCVersion, bool) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestBadgerMVCC_BootstrapInTxn_HeadDecodeErrorBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:bn1", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:bn2", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:be1", StartNode: "test:bn1", EndNode: "test:bn2", Type: "R"}))

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		// prefix-only garbage keys exercise len(key)<=1 skip branches.
		if err := txn.Set([]byte{prefixNode}, []byte("x")); err != nil {
			return err
		}
		if err := txn.Set([]byte{prefixEdge}, []byte("x")); err != nil {
			return err
		}
		nh, err := engine.mvccNodeHeadKeyString(txn, "test:bn1")
		if err != nil {
			return err
		}
		eh, err := engine.mvccEdgeHeadKeyString(txn, "test:be1")
		if err != nil {
			return err
		}
		if err := txn.Set(nh, []byte{0x01}); err != nil {
			return err
		}
		return txn.Set(eh, []byte{0x01})
	}))

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		err := engine.bootstrapNodeMVCCFromCurrentStateInTxn(txn)
		require.Error(t, err)
		return nil
	}))

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		err := engine.bootstrapEdgeMVCCFromCurrentStateInTxn(txn)
		require.Error(t, err)
		return nil
	}))
}

func TestBadgerMVCC_PruneMVCCKeyspaceInTxn_TombstoneReclaim_NodeAndEdge(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	v1 := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-2 * time.Minute), CommitSequence: 10}
	v2 := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-time.Minute), CommitSequence: 11}

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		if err := engine.writeNodeMVCCVersionInTxn(txn, &Node{ID: "test:n-reclaim", Labels: []string{"N"}}, v1); err != nil {
			return err
		}
		if err := engine.writeNodeMVCCTombstoneInTxn(txn, "test:n-reclaim", v2); err != nil {
			return err
		}
		if err := engine.writeNodeMVCCHeadWithFloorInTxn(txn, "test:n-reclaim", v2, true, v1); err != nil {
			return err
		}

		if err := engine.writeEdgeMVCCVersionInTxn(txn, &Edge{ID: "test:e-reclaim", StartNode: "test:n-reclaim", EndNode: "test:n-reclaim", Type: "R"}, v1); err != nil {
			return err
		}
		if err := engine.writeEdgeMVCCTombstoneInTxn(txn, "test:e-reclaim", v2); err != nil {
			return err
		}
		if err := engine.writeEdgeMVCCHeadWithFloorInTxn(txn, "test:e-reclaim", v2, true, v1); err != nil {
			return err
		}
		return nil
	}))

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		nDeleted, err := engine.pruneMVCCKeyspaceInTxn(context.Background(), txn, []byte{prefixMVCCNode}, MVCCPruneOptions{MaxVersionsPerKey: 0}, false)
		require.NoError(t, err)
		require.GreaterOrEqual(t, nDeleted, int64(2))

		eDeleted, err := engine.pruneMVCCKeyspaceInTxn(context.Background(), txn, []byte{prefixMVCCEdge}, MVCCPruneOptions{MaxVersionsPerKey: 0}, false)
		require.NoError(t, err)
		require.GreaterOrEqual(t, eDeleted, int64(2))
		return nil
	}))

	_, err := engine.GetNodeCurrentHead("test:n-reclaim")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeCurrentHead("test:e-reclaim")
	require.ErrorIs(t, err, ErrNotFound)
}
