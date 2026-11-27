package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingBetweenIndexTracksStagedEdgeChanges(t *testing.T) {
	ctx := context.Background()
	tx := &BadgerTransaction{pendingEdges: make(map[EdgeID]*Edge)}
	first := &Edge{ID: "first", StartNode: "a", EndNode: "b", Type: "REL"}
	tx.setPendingEdgeLocked(first)

	between := func(start, end NodeID, edgeType string) map[EdgeID]*Edge {
		t.Helper()
		got, err := tx.pendingBetweenLocked(ctx, start, end, edgeType, tx.pendingEdges)
		require.NoError(t, err)
		return got
	}
	require.Equal(t, map[EdgeID]*Edge{"first": first}, between("a", "b", "REL"))
	require.Empty(t, between("b", "a", "REL"))
	require.Empty(t, between("a", "b", "OTHER"))

	second := &Edge{ID: "second", StartNode: "a", EndNode: "b", Type: "REL"}
	tx.setPendingEdgeLocked(second)
	require.Len(t, between("a", "b", "REL"), 2)

	moved := &Edge{ID: "first", StartNode: "a", EndNode: "c", Type: "REL"}
	tx.setPendingEdgeLocked(moved)
	require.Equal(t, map[EdgeID]*Edge{"second": second}, between("a", "b", "REL"))
	require.Equal(t, map[EdgeID]*Edge{"first": moved}, between("a", "c", "REL"))
	retyped := &Edge{ID: "first", StartNode: "a", EndNode: "c", Type: "OTHER"}
	tx.setPendingEdgeLocked(retyped)
	require.Empty(t, between("a", "c", "REL"))
	require.Equal(t, map[EdgeID]*Edge{"first": retyped}, between("a", "c", "OTHER"))

	tx.removePendingEdgeLocked("second")
	require.Empty(t, between("a", "b", "REL"))

	// A lock-refresh read image is visible for this read, but never staged.
	readOnly := &Edge{ID: "read", StartNode: "a", EndNode: "b", Type: "REL"}
	images := map[EdgeID]*Edge{"first": retyped, "read": readOnly}
	got, err := tx.pendingBetweenLocked(ctx, "a", "b", "REL", images)
	require.NoError(t, err)
	require.Equal(t, map[EdgeID]*Edge{"read": readOnly}, got)
	require.Empty(t, between("a", "b", "REL"))
}
