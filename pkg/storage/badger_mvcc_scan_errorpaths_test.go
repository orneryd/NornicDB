package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_WithViewEdgeMVCCVersionsFromKey_UnknownNumAndDecodeError(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	unknownKey := mvccEdgeVersionKey(9_999_999, v)
	unknownRec, err := encodeMVCCEdgeRecord(&Edge{ID: "ghost:e", StartNode: "ghost:a", EndNode: "ghost:b", Type: "REL"}, false)
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		return txn.Set(unknownKey, unknownRec)
	}))

	yieldCount := 0
	_, reachedEnd, err := engine.withViewEdgeMVCCVersionsFromKey([]byte{prefixMVCCEdge}, 1, func(EdgeID, MVCCVersion, bool) error {
		yieldCount++
		return nil
	})
	require.NoError(t, err)
	require.False(t, reachedEnd)
	require.Equal(t, 0, yieldCount)

	_, err = engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		vk, keyErr := engine.mvccEdgeVersionKeyString(txn, "test:e1", v)
		if keyErr != nil {
			return keyErr
		}
		return txn.Set(vk, []byte("corrupt-edge-mvcc-record"))
	}))

	_, _, err = engine.withViewEdgeMVCCVersionsFromKey([]byte{prefixMVCCEdge}, 64, func(EdgeID, MVCCVersion, bool) error {
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "msgpack")
}

func TestBadgerEngine_RebuildMVCCHeads_ContextCancelledAndClosedEngine(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	id := NodeID(prefixTestID("mvcc-cancel-node"))
	_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"v": 2}}))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = engine.rebuildNodeMVCCHeadsFromVersions(cancelled)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, engine.Close())
	err = engine.RebuildMVCCHeads(context.Background())
	require.Error(t, err)
}

func TestBadgerEngine_PruneMVCCKeyspaceInTxn_UnsupportedPrefixAndCanceled(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID("test:prune-node")

	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 2}}))

	err = engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 11}
		customKey := make([]byte, 0, 1+8+16)
		customKey = append(customKey, 0x7f)
		customKey = append(customKey, make([]byte, 8)...)
		customKey = append(customKey, encodeMVCCSortVersion(v)...)
		if setErr := txn.Set(customKey, []byte{0x01}); setErr != nil {
			return setErr
		}
		_, pruneErr := engine.pruneMVCCKeyspaceInTxn(context.Background(), txn, []byte{0x7f}, MVCCPruneOptions{MaxVersionsPerKey: 1}, false)
		return pruneErr
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported mvcc prune prefix")

	err = engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 77}
		vk, keyErr := engine.mvccNodeVersionKeyString(txn, nodeID, v)
		if keyErr != nil {
			return keyErr
		}
		rec, recErr := encodeMVCCNodeRecord(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 77}}, false)
		if recErr != nil {
			return recErr
		}
		if setErr := txn.Set(vk, rec); setErr != nil {
			return setErr
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, pruneErr := engine.pruneMVCCKeyspaceInTxn(ctx, txn, []byte{prefixMVCCNode}, MVCCPruneOptions{MaxVersionsPerKey: 1}, false)
		return pruneErr
	})
	require.ErrorIs(t, err, context.Canceled)
}
