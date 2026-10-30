package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPendingNodeIndexReplaceAddsUnlistedPrev covers replace's contract
// (#1043 follow-up): when prev was never listed (the committed version the
// write-behind buffer hands in for a node it doesn't hold yet), an update
// that keeps the tracked values must still list the new version. When prev
// is listed, such an update keeps the existing entries.
func TestPendingNodeIndexReplaceAddsUnlistedPrev(t *testing.T) {
	index := newPendingNodeIndex()
	index.track("User", "id", func(NodeID) *Node { return nil })
	committed := &Node{ID: "u1", Labels: []string{"User"}, Properties: map[string]any{"id": int64(1), "v": int64(0)}}
	updated := &Node{ID: "u1", Labels: []string{"User"}, Properties: map[string]any{"id": int64(1), "v": int64(1)}}

	require.False(t, index.listed(committed))
	index.replace(committed, updated)
	require.True(t, index.listed(updated), "an unlisted prev must not skip the add")
	key := pendingValueKey{label: "User", property: "id", value: mustIndexValueKey(t, int64(1))}
	require.Equal(t, []NodeID{"u1"}, index.byValue[key])

	again := &Node{ID: "u1", Labels: []string{"User"}, Properties: map[string]any{"id": int64(1), "v": int64(2)}}
	index.replace(updated, again)
	require.Equal(t, []NodeID{"u1"}, index.byValue[key], "a listed prev with the same tracked values keeps its entries")
	require.True(t, index.listed(again))

	unlabelled := &Node{ID: "n1", Properties: map[string]any{"v": int64(0)}}
	require.True(t, index.listed(unlabelled), "a node without labels has no entries to miss")
}

func mustIndexValueKey(t *testing.T, value any) interface{} {
	t.Helper()
	key, ok := indexValueKey(value)
	require.True(t, ok)
	return key
}
