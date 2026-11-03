package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompositeBackfillCancellationKeepsIndexUnfilledAndRetries(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddConstraint(Constraint{Name: "backfill_cancel", Type: ConstraintNodeKey, Label: "Item", Properties: []string{"p", "q"}}, false))
	idx, found := sm.ConstraintPropertyIndex("backfill_cancel")
	require.True(t, found)
	require.True(t, idx.unfilled.Load())
	entries := map[NodeID]map[string]interface{}{"item:1": {"p": normalizationFlatList(4096), "q": int64(1)}}
	err := sm.BackfillCompositeIndex(newIndexPollCancelContext(t, 16), idx.Name, entries)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, idx.unfilled.Load(), "a rejected fill must not publish seek availability")
	require.Empty(t, idx.fullIndex, "canceled node preparation files no partial keys")
	require.Empty(t, idx.prefixIndex)
	require.NoError(t, sm.BackfillCompositeIndex(t.Context(), idx.Name, entries))
	require.False(t, idx.unfilled.Load())
	ids, err := idx.LookupFull(t.Context(), entries["item:1"]["p"], int64(1))
	require.NoError(t, err)
	require.Equal(t, []NodeID{"item:1"}, ids)
}
