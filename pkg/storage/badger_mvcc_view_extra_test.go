package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_WithViewNodeMVCCVersionsFromKey_UnknownNumDecodeAndYield(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 101}
	unknownKey := mvccNodeVersionKey(8_888_888, version)
	unknownRec, err := encodeMVCCNodeRecord(&Node{ID: "ghost:n", Labels: []string{"Ghost"}}, false)
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		return txn.Set(unknownKey, unknownRec)
	}))

	// Unknown numeric ID should be skipped but still counted for pagination.
	yieldCalls := 0
	_, reachedEnd, err := engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 1, func(NodeID, MVCCVersion, bool) error {
		yieldCalls++
		return nil
	})
	require.NoError(t, err)
	require.False(t, reachedEnd)
	require.Equal(t, 0, yieldCalls)

	// Prepare a known node and force decode failure on one MVCC record.
	_, err = engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		key, keyErr := engine.mvccNodeVersionKeyString(txn, "test:n1", version)
		if keyErr != nil {
			return keyErr
		}
		return txn.Set(key, []byte("corrupt-node-mvcc-record"))
	}))

	_, _, err = engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 64, func(NodeID, MVCCVersion, bool) error {
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "msgpack")
}

func TestBadgerEngine_WithViewEdgeMVCCVersionsFromKey_YieldErrorAndLimit(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 202}
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		edgeNum, numErr := engine.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, "test:e1")
		if numErr != nil {
			return numErr
		}
		key := mvccEdgeVersionKey(edgeNum, version)
		record, recErr := encodeMVCCEdgeRecord(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}, false)
		if recErr != nil {
			return recErr
		}
		return txn.Set(key, record)
	}))

	// Limit branch in edge iterator.
	seen := 0
	_, reachedEnd, err := engine.withViewEdgeMVCCVersionsFromKey([]byte{prefixMVCCEdge}, 1, func(EdgeID, MVCCVersion, bool) error {
		seen++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, seen)
	require.False(t, reachedEnd)

	// Yield error propagation branch.
	wantErr := errors.New("stop-yield")
	_, _, err = engine.withViewEdgeMVCCVersionsFromKey([]byte{prefixMVCCEdge}, 64, func(EdgeID, MVCCVersion, bool) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestBadgerEngine_RebuildMVCCHeads_NilContext(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:nx", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: "test:nx", Labels: []string{"Doc"}, Properties: map[string]any{"v": 2}}))

	// nil context branch should default to Background and succeed.
	require.NoError(t, engine.RebuildMVCCHeads(nil))
	_, err = engine.GetNodeCurrentHead("test:nx")
	require.NoError(t, err)

	// Also keep a direct context path active for the same test flow.
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
}
