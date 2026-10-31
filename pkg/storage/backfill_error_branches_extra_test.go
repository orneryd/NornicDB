package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestIndexBackfill_AdditionalBranches(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		kind                     startupIndexKind
		rebuild                  func(*BadgerEngine, context.Context) (int, error)
		badKey                   []byte
		decodeError, closedError string
	}{
		{"edge-between", startupEdgeBetweenIndex,
			func(b *BadgerEngine, ctx context.Context) (int, error) {
				return b.rebuildRelationshipIndexes(ctx, rebuildEdgeBetweenOnly)
			}, edgeKey(EdgeID("test:bad-edge")), "decode edge for edge-between index", "clear edge-between set index before rebuild"},
		{"label",
			startupLabelIndex,
			(*BadgerEngine).rebuildLabelIndex, nodeKey(NodeID("test:bad-node")), "decode node", "clear label index before rebuild"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("ensure marks ready on empty store", func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				require.NoError(t, engine.startIndexBackfill(tc.kind, initializeIndexBackfill))
				ready, err := engine.indexBackfillReady(tc.kind)
				require.NoError(t, err)
				require.True(t, ready)
			})
			t.Run("start called twice and stop when running", func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				// A ready marker must short-circuit initialization without
				// registering a job; explicit force mode must still register one.
				require.NoError(t, engine.startIndexBackfill(tc.kind, initializeIndexBackfill))
				mu, doneSlot := &engine.labelIndexBackfillMu, &engine.labelIndexBackfill
				if tc.kind == startupEdgeBetweenIndex {
					mu, doneSlot = &engine.edgeBetweenIndexBackfillMu, &engine.edgeBetweenIndexBackfill
				}
				mu.Lock()
				dormant := *doneSlot == nil
				mu.Unlock()
				require.True(t, dormant)
				require.NoError(t, engine.startIndexBackfill(tc.kind, forceIndexBackfill))
				require.NoError(t, engine.startIndexBackfill(tc.kind, forceIndexBackfill))
				mu.Lock()
				registered := *doneSlot != nil
				mu.Unlock()
				require.True(t, registered)
				engine.stopIndexBackfill(tc.kind)
				engine.stopIndexBackfill(tc.kind)
			})
			t.Run("rebuild respects canceled context before work", func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := tc.rebuild(engine, ctx)
				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled)
			})
			t.Run("rebuild decode error branch", func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Set(tc.badKey, []byte{0xFF, 0x00}) }))
				_, err := tc.rebuild(engine, context.Background())
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.decodeError)
			})
			t.Run("rebuild with closed db hits drop-prefix error", func(t *testing.T) {
				engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				if engine != nil {
					fixtureowner.Own(t, engine)
				}
				require.NoError(t, err)
				require.NoError(t, engine.Close())
				require.NoError(t, engine.restartIndexBackfills(), "supported closed restart remains a no-op")
				_, err = tc.rebuild(engine, context.Background())
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.closedError)
			})
		})
	}
}
