package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexBufferedPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"appendInNamespaceWithContext", "propertyEntriesWithContext", "lookupUniqueValueWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			sm := b.GetSchemaForNamespace("test")
			if owner == "lookupUniqueValueWithContext" {
				require.NoError(t, sm.AddUniqueConstraint("p_unique", "P", "k"))
			} else {
				require.NoError(t, sm.AddPropertyIndex("p_k", "P", []string{"k"}))
			}
			for i := 0; i < 12; i++ {
				bufferedCreate(t, b, &Node{ID: NodeID(fmt.Sprintf("test:p%d", i)), Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(i)}})
			}
			ctx := preparationContext(t, owner, 2)
			var err error
			switch owner {
			case "appendInNamespaceWithContext":
				_, err = sm.PropertyIndexLookupWithContext(ctx, "P", "k", int64(1))
			case "propertyEntriesWithContext":
				_, _, err = sm.PropertyIndexAllNonNilWithContext(ctx, "P", "k", false)
			case "lookupUniqueValueWithContext":
				_, _, _, _, err = sm.LookupUniqueConstraintValueForPlanningWithContext(ctx, "P", "k", int64(1))
			}
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			if owner == "lookupUniqueValueWithContext" {
				id, found, exists, _, err := sm.LookupUniqueConstraintValueForPlanningWithContext(context.Background(), "P", "k", int64(1))
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, exists)
				require.Equal(t, NodeID("test:p1"), id)
				return
			}
			ids, err := sm.PropertyIndexLookupWithContext(context.Background(), "P", "k", int64(1))
			require.NoError(t, err)
			require.Equal(t, []NodeID{"test:p1"}, ids)
			all, exists, err := sm.PropertyIndexAllNonNilWithContext(context.Background(), "P", "k", false)
			require.NoError(t, err)
			require.True(t, exists)
			require.Len(t, all, 12)
		})
	}
}

func TestIndexPrivateSortCancellation(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("p_k", "P", []string{"k"}))
	idx, _ := sm.GetPropertyIndex("P", "k")
	for i := 0; i < 20; i++ {
		require.NoError(t, idx.IndexNode(NodeID(fmt.Sprintf("p%d", i)), map[string]interface{}{"k": int64(20 - i)}))
	}
	ctx := preparationContext(t, "sortIndexCandidatesWithContext", 3)
	_, _, err := sm.PropertyIndexAllNonNilWithContext(ctx, "P", "k", false)
	require.GreaterOrEqual(t, ctx.hits, 3)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, idx.keysDirty, "interrupted private keys must not publish a cache")
	ids, exists, err := sm.PropertyIndexAllNonNilWithContext(context.Background(), "P", "k", false)
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, ids, 20)
	require.Equal(t, NodeID("p19"), ids[0])
}
