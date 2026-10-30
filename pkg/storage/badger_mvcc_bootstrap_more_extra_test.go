package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_MVCCBootstrapHelpersAndTxnVariants(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:bn1", Labels: []string{"N"}, Properties: map[string]any{"i": int64(1)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:bn2", Labels: []string{"N"}, Properties: map[string]any{"i": int64(2)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:bn3", Labels: []string{"N"}, Properties: map[string]any{"i": int64(3)}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:be1", StartNode: "test:bn1", EndNode: "test:bn2", Type: "R"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:be2", StartNode: "test:bn2", EndNode: "test:bn3", Type: "R"}))

	// Remove current heads so bootstrap paths have work to do.
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCNodeHead))
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCEdgeHead))

	nodeBatch1, nodeLast, nodeReachedEnd, err := engine.collectNodeBootstrapBatch(context.Background(), []byte{prefixNode}, 1)
	require.NoError(t, err)
	require.Len(t, nodeBatch1, 1)
	require.False(t, nodeReachedEnd)
	require.NotEmpty(t, nodeLast)
	require.NoError(t, engine.applyNodeBootstrapBatch(nodeBatch1))

	nodeBatch2, _, nodeReachedEnd2, err := engine.collectNodeBootstrapBatch(context.Background(), nextScanStart(nodeLast), 10)
	require.NoError(t, err)
	require.NotEmpty(t, nodeBatch2)
	require.True(t, nodeReachedEnd2)
	require.NoError(t, engine.applyNodeBootstrapBatch(nodeBatch2))

	edgeBatch1, edgeLast, edgeReachedEnd, err := engine.collectEdgeBootstrapBatch(context.Background(), []byte{prefixEdge}, 1)
	require.NoError(t, err)
	require.Len(t, edgeBatch1, 1)
	require.False(t, edgeReachedEnd)
	require.NotEmpty(t, edgeLast)
	require.NoError(t, engine.applyEdgeBootstrapBatch(edgeBatch1))

	edgeBatch2, _, edgeReachedEnd2, err := engine.collectEdgeBootstrapBatch(context.Background(), nextScanStart(edgeLast), 10)
	require.NoError(t, err)
	require.NotEmpty(t, edgeBatch2)
	require.True(t, edgeReachedEnd2)
	require.NoError(t, engine.applyEdgeBootstrapBatch(edgeBatch2))

	// Full bootstrap orchestration.
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCNodeHead))
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCEdgeHead))
	require.NoError(t, engine.bootstrapMVCCHeadsFromCurrentState(context.Background()))

	_, err = engine.GetNodeCurrentHead("test:bn1")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:be1")
	require.NoError(t, err)

	// In-txn bootstrap variants.
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCNodeHead))
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixMVCCEdgeHead))
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := engine.bootstrapNodeMVCCFromCurrentStateInTxn(txn); err != nil {
			return err
		}
		return engine.bootstrapEdgeMVCCFromCurrentStateInTxn(txn)
	}))

	_, err = engine.GetNodeCurrentHead("test:bn2")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:be2")
	require.NoError(t, err)

	// Cancellation path in collectors.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = engine.collectNodeBootstrapBatch(cancelled, []byte{prefixNode}, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, _, _, err = engine.collectEdgeBootstrapBatch(cancelled, []byte{prefixEdge}, 1)
	require.ErrorIs(t, err, context.Canceled)
}
