package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newLoggedMVCCDelegationWAL(t *testing.T, engine Engine) *WALEngine {
	t.Helper()
	log, err := NewWAL(filepath.Join(t.TempDir(), "wal"), nil)
	require.NoError(t, err)
	wal := NewWALEngine(engine, log)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	return wal
}

func TestWALEngine_MVCCDelegationAndFallback(t *testing.T) {
	t.Run("delegates head lookups and maintenance to MVCC-capable engine", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		wal := newLoggedMVCCDelegationWAL(t, engine)

		nodeID := NodeID(prefixTestID("wal-mvcc-node"))
		_, err := wal.CreateNode(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 1}})
		require.NoError(t, err)
		require.NoError(t, wal.UpdateNode(NodeUpdateRequest{After: &Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 2}}}))
		require.NoError(t, wal.UpdateNode(NodeUpdateRequest{After: &Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"version": 3}}}))

		head, err := wal.GetNodeCurrentHead(nodeID)
		require.NoError(t, err)
		require.False(t, head.Version.IsZero())

		nodeAt, err := wal.GetNodeVisibleAt(nodeID, head.Version)
		require.NoError(t, err)
		require.EqualValues(t, 3, nodeAt.Properties["version"])

		deleted, err := wal.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
		require.NoError(t, err)
		require.GreaterOrEqual(t, deleted, int64(1))
		require.NoError(t, wal.RebuildMVCCHeads(context.Background()))

		start := NodeID(prefixTestID("wal-mvcc-start"))
		end := NodeID(prefixTestID("wal-mvcc-end"))
		_, err = wal.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
		require.NoError(t, err)
		_, err = wal.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
		require.NoError(t, err)
		edgeID := EdgeID(prefixTestID("wal-mvcc-edge"))
		require.NoError(t, wal.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "LINKS"}))

		edgeHead, err := wal.GetEdgeCurrentHead(edgeID)
		require.NoError(t, err)
		require.False(t, edgeHead.Version.IsZero())

		edgeAt, err := wal.GetEdgeVisibleAt(edgeID, edgeHead.Version)
		require.NoError(t, err)
		require.Equal(t, edgeID, edgeAt.ID)

		edgesByType, err := wal.GetEdgesByTypeVisibleAt("LINKS", edgeHead.Version)
		require.NoError(t, err)
		require.Len(t, edgesByType, 1)
		require.Equal(t, edgeID, edgesByType[0].ID)

		edgesBetween, err := wal.GetEdgesBetweenVisibleAt(start, end, edgeHead.Version)
		require.NoError(t, err)
		require.Len(t, edgesBetween, 1)
		require.Equal(t, edgeID, edgesBetween[0].ID)
	})

	t.Run("returns ErrNotImplemented when wrapped engine is not MVCC-capable", func(t *testing.T) {
		baseInner := newTestMemoryEngine(t)
		t.Cleanup(func() { _ = baseInner.Close() })
		wal := newLoggedMVCCDelegationWAL(t, &nonMVCCEngine{Engine: baseInner})

		version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
		_, err := wal.GetNodeCurrentHead(NodeID("nornic:wal-head-node"))
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = wal.GetEdgeCurrentHead(EdgeID("nornic:wal-head-edge"))
		require.ErrorIs(t, err, ErrNotImplemented)
		require.ErrorIs(t, wal.RebuildMVCCHeads(context.Background()), ErrNotImplemented)
		_, err = wal.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = wal.GetNodeVisibleAt(NodeID("nornic:wal-head-node"), version)
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = wal.GetEdgeVisibleAt(EdgeID("nornic:wal-head-edge"), version)
		require.ErrorIs(t, err, ErrNotImplemented)
	})
}
