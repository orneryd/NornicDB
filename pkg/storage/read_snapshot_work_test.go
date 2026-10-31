// Author: Codex implementation lane, 2026-10-27 (NOR-148 capture work boundary).
package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadSnapshotRequestedEntityWork(t *testing.T) {
	base, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, err = base.CreateNode(&Node{ID: "capture:a"})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "capture:b"})
	require.NoError(t, err)
	require.NoError(t, base.FlushWriteBehind(context.Background()))
	// Direct engine writes are synchronous even with write-behind enabled.
	// Seed both pending kinds through the canonical implicit commit owner.
	// Author: Codex implementation lane, 29 October 2026 (fixture precondition).
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("capture"))
	_, err = tx.CreateNode(&Node{ID: "capture:pending", Properties: map[string]any{"text": "hotel"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "capture:pending-edge", StartNode: "capture:a", EndNode: "capture:b", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	// Buffer operation counts measure physical mutations, not entity images.
	// Validate the semantic precondition through the canonical overlay reader.
	pendingNodes, pendingEdges := func() ([]*Node, []*Edge) {
		base.writeBehind.mu.RLock()
		defer base.writeBehind.mu.RUnlock()
		nodes := base.readBufferedEntities(base.writeBehind, storedRead, bufferedEntityRead{kind: bufferedNodes, prefix: "capture:"})
		edges := base.readBufferedEntities(base.writeBehind, storedRead, bufferedEntityRead{kind: bufferedEdges, prefix: "capture:"})
		return nodes.nodes, edges.edges
	}()
	require.Len(t, pendingNodes, 1, "the fixture must contain the actual pending node image")
	require.Equal(t, NodeID("capture:pending"), pendingNodes[0].ID)
	require.Len(t, pendingEdges, 1, "the fixture must contain the actual pending edge image")
	require.Equal(t, EdgeID("capture:pending-edge"), pendingEdges[0].ID)
	for _, reader := range []SnapshotReader{SnapshotStored, SnapshotAllNodes, SnapshotAllEdges} {
		ctx := WithSnapshotReader(context.Background(), reader)
		view, err := OpenReadSnapshot(ctx, base)
		require.NoError(t, err)
		snapshot := view.(*badgerReadSnapshot)
		if reader == SnapshotAllNodes {
			require.Empty(t, snapshot.overlayEdges)
		} else {
			require.NotEmpty(t, snapshot.overlayEdges)
		}
		if reader == SnapshotAllEdges {
			require.Empty(t, snapshot.overlayNodes)
		} else {
			require.NotEmpty(t, snapshot.overlayNodes)
		}
		view.Close()
	}
}
