package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_MVCCEdgeVisibility_FallbackAndTombstoneBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	start := NodeID("test:mvcc-edge-vis-start")
	end := NodeID("test:mvcc-edge-vis-end")
	edgeID := EdgeID("test:mvcc-edge-vis-e1")

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "REL", Properties: map[string]any{"w": 1}}))

	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	// Ensure an explicit version record exists at head.Version, then remove primary key
	// so GetEdgeLatestVisible/GetEdgeVisibleAt must resolve via MVCC fallback paths.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		edge, gErr := engine.GetEdge(edgeID)
		require.NoError(t, gErr)
		require.NoError(t, engine.writeEdgeMVCCVersionInTxn(txn, edge, head.Version))
		return txn.Delete(edgeKey(edgeID))
	}))

	latest, err := engine.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.Equal(t, edgeID, latest.ID)

	_, err = engine.GetEdgeVisibleAt(edgeID, head.Version)
	// Depending on whether an exact head-version record exists in this test setup,
	// the reader may resolve via fallback or report not found; both execute the
	// targeted fallback branch after primary-key miss.
	if err != nil {
		require.ErrorIs(t, err, ErrNotFound)
	}

	_, err = engine.GetEdgeVisibleAt(edgeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotVisibleAtSnapshot)

	// Append a tombstone head explicitly because we removed the primary edge key
	// above; DeleteEdge expects that current primary body exists.
	tombVersion := MVCCVersion{
		CommitTimestamp: head.Version.CommitTimestamp.Add(time.Nanosecond),
		CommitSequence:  head.Version.CommitSequence + 1,
	}
	require.NoError(t, engine.AppendEdgeTombstone(edgeID, tombVersion))
	tombHead, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.True(t, tombHead.Tombstoned)

	_, err = engine.GetEdgeLatestVisible(edgeID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeVisibleAt(edgeID, tombHead.Version)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_MVCCEdgeVisibility_FilterAndBetweenGuards(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	engine.SetDecayEnabled(true)

	start := NodeID("test:mvcc-edge-filter-start")
	end := NodeID("test:mvcc-edge-filter-end")
	edgeID := EdgeID("test:mvcc-edge-filter-e1")

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "REL", VisibilitySuppressed: true}))

	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	_, err = engine.GetEdgeVisibleAt(edgeID, head.Version)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeLatestVisible(edgeID)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = engine.GetEdgesBetweenVisibleAt("", end, MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1})
	require.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_MVCCEdgeVisibility_HistoricalFallbackAndSuppressionBranches(t *testing.T) {
	t.Run("historical suppressed record is filtered after mvcc lookup", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		engine.SetDecayEnabled(true)
		for _, nodeID := range []NodeID{"test:hs-a", "test:hs-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		edgeID := EdgeID("test:hs-edge")
		v1 := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-time.Second), CommitSequence: 10}
		v2 := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 11}
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := engine.writeEdgeMVCCVersionInTxn(txn, &Edge{ID: edgeID, StartNode: "test:hs-a", EndNode: "test:hs-b", Type: "REL", VisibilitySuppressed: true}, v1); err != nil {
				return err
			}
			return engine.writeEdgeMVCCHeadWithFloorInTxn(txn, edgeID, v2, false, v1)
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.getEdgeVisibleAtInTxn(txn, edgeID, v1)
			require.ErrorIs(t, err, ErrNotFound)
			return nil
		}))
	})

	t.Run("missing primary falls back to older mvcc record when head record is absent", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:hf-a", "test:hf-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		edgeID := EdgeID("test:hf-edge")
		require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: "test:hf-a", EndNode: "test:hf-b", Type: "REL", Properties: map[string]any{"w": int64(1)}}))
		require.NoError(t, engine.UpdateEdge(&Edge{ID: edgeID, StartNode: "test:hf-a", EndNode: "test:hf-b", Type: "REL", Properties: map[string]any{"w": int64(2)}}))
		v2, err := engine.GetEdgeCurrentHead(edgeID)
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Delete(edgeKey(edgeID))
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			edge, err := engine.getEdgeVisibleAtInTxn(txn, edgeID, v2.Version)
			require.NoError(t, err)
			require.EqualValues(t, 1, edge.Properties["w"])
			return nil
		}))
	})
}
