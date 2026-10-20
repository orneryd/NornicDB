package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEdgeBetweenBackfill_AdditionalBranches(t *testing.T) {
	t.Run("ensure marks ready on empty store", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.ensureEdgeBetweenIndex())
		ready, err := engine.edgeBetweenIndexReady()
		require.NoError(t, err)
		require.True(t, ready)
	})

	t.Run("start called twice and stop when running", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		engine.startEdgeBetweenIndexBackfill()
		engine.startEdgeBetweenIndexBackfill() // no-op branch when already running
		engine.stopEdgeBetweenIndexBackfill()
		engine.stopEdgeBetweenIndexBackfill() // no-op branch when not running
	})

	t.Run("rebuild respects canceled context before work", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := engine.rebuildEdgeBetweenIndex(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("rebuild decode error branch", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey(EdgeID("test:bad-edge")), []byte{0xFF, 0x00})
		}))

		_, err := engine.rebuildEdgeBetweenIndex(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode edge for edge-between index")
	})

	t.Run("rebuild rejects a closed engine before clearing the index", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		require.NoError(t, engine.Close())

		_, err = engine.rebuildEdgeBetweenIndex(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestLabelBackfill_AdditionalBranches(t *testing.T) {
	t.Run("ensure marks ready on empty store", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.ensureLabelIndex())
		ready, err := engine.labelIndexReady()
		require.NoError(t, err)
		require.True(t, ready)
	})

	t.Run("start called twice and stop when running", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		engine.startLabelIndexBackfill()
		engine.startLabelIndexBackfill() // no-op branch when already running
		engine.stopLabelIndexBackfill()
		engine.stopLabelIndexBackfill() // no-op branch when not running
	})

	t.Run("rebuild respects canceled context before work", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := engine.rebuildLabelIndex(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("rebuild decode error branch", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(nodeKey(NodeID("test:bad-node")), []byte{0xFF, 0x00})
		}))

		_, err := engine.rebuildLabelIndex(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode node")
	})

	t.Run("rebuild rejects a closed engine before clearing the index", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		require.NoError(t, engine.Close())

		_, err = engine.rebuildLabelIndex(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}
