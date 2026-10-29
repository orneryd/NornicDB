package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

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

	err = engine.withUpdate(func(txn *badger.Txn) error {
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

	err = engine.withUpdate(func(txn *badger.Txn) error {
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
