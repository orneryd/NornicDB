// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The visible adapter must retain the committed point and namespace of its
// canonical snapshot, while letting cancellation stop between visited rows.
func TestReadSnapshotVisibleNodesContracts(t *testing.T) {
	base := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := NewNamespacedEngine(base, "visible_snapshot")
	other := NewNamespacedEngine(base, "other_snapshot")
	for _, id := range []NodeID{"one", "two", "three"} {
		_, err := store.CreateNode(&Node{ID: id, Properties: map[string]any{"text": "captured"}})
		require.NoError(t, err)
	}
	_, err := other.CreateNode(&Node{ID: "other", Properties: map[string]any{"text": "outside"}})
	require.NoError(t, err)
	snapshot, err := OpenReadSnapshot(context.Background(), store)
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	visible, ok := snapshot.(VisibleReadSnapshot)
	require.True(t, ok)
	require.NoError(t, store.UpdateNode(NodeUpdateRequest{After: &Node{ID: "one", Properties: map[string]any{"text": "later"}}}))
	var ids []NodeID
	require.NoError(t, visible.IterateVisibleNodes(context.Background(), func(node *Node) bool {
		ids = append(ids, node.ID)
		require.Equal(t, "captured", node.Properties["text"], "a later write must not enter the admitted view")
		return true
	}))
	require.ElementsMatch(t, []NodeID{"one", "two", "three"}, ids)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err = visible.IterateVisibleNodes(ctx, func(*Node) bool {
		visited++
		cancel()
		return true // The storage owner, not the visitor, must stop the scan.
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, visited)
	snapshot.Close()
	err = visible.IterateVisibleNodes(context.Background(), func(*Node) bool { t.Error("visited a closed view"); return true })
	require.ErrorIs(t, err, ErrStorageClosed)
}
