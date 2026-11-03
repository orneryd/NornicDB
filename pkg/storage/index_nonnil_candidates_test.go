package storage

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertyIndexNonNilMembershipTransitions(t *testing.T) {
	for _, owner := range []string{"schema", "composite", "pending"} {
		t.Run(owner, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, sm.AddPropertyIndex("nonnull", "Item", []string{"value"}))
			idx, exists, err := sm.GetPropertyIndex(t.Context(), "Item", "value")
			require.NoError(t, err)
			require.True(t, exists)
			var w *WriteBehindBuffer
			previous := &Node{ID: "test:n", Labels: []string{"Item"}, Properties: map[string]interface{}{}}
			if owner == "pending" {
				previous.Properties["value"] = []byte{9}
				require.NoError(t, idx.IndexNode(t.Context(), previous.ID, previous.Properties))
				w = &WriteBehindBuffer{active: newCommitBuffer()}
				sm.attachPendingWrites(w, "test")
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: &Node{ID: "other:foreign", Labels: []string{"Item"}, Properties: map[string]interface{}{"value": []byte{8}}}}}})
			}
			for _, step := range []struct {
				value           interface{}
				member, ordered bool
			}{
				{nil, false, true}, {[]byte{1, 2}, true, false}, {int64(2), true, true}, {[]interface{}{int64(1), int64(2)}, true, false}, {[]byte{3}, true, false}, {nil, false, true},
			} {
				next := &Node{ID: previous.ID, Labels: previous.Labels, Properties: map[string]interface{}{"value": step.value}}
				switch owner {
				case "schema":
					require.NoError(t, sm.PropertyIndexDelete("Item", "value", previous.ID, previous.Properties["value"]))
					require.NoError(t, sm.PropertyIndexInsert("Item", "value", next.ID, step.value))
					require.NoError(t, sm.PropertyIndexInsert("Item", "value", next.ID, step.value))
				case "composite":
					idx.RemoveNode(previous.ID, previous.Properties)
					require.NoError(t, idx.IndexNode(t.Context(), next.ID, next.Properties))
					require.NoError(t, idx.IndexNode(t.Context(), next.ID, next.Properties))
				case "pending":
					w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpUpdateNode, OldNode: previous, Node: next}}})
				}
				ids, found, err := sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
				require.NoError(t, err)
				require.True(t, found)
				if step.member {
					require.Equal(t, []NodeID{"test:n"}, ids)
					ids[0] = "owned-result"
					again, _, err := sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
					require.NoError(t, err)
					require.Equal(t, []NodeID{"test:n"}, again)
				} else {
					require.Empty(t, ids)
				}
				ordered, err := sm.PropertyIndexAllNonNil(t.Context(), "Item", "value", false)
				if step.ordered {
					require.NoError(t, err)
					if step.member {
						require.Equal(t, []NodeID{"test:n"}, ordered)
					} else {
						require.Empty(t, ordered)
					}
				} else {
					require.ErrorIs(t, err, ErrPropertyIndexValueOrderUnavailable)
					require.Nil(t, ordered)
					calls := 0
					_, err = sm.VisitPropertyIndexGroups(t.Context(), "Item", "value", false, func([]NodeID) bool { calls++; return true })
					require.ErrorIs(t, err, ErrPropertyIndexValueOrderUnavailable)
					require.Zero(t, calls)
					top, err := sm.PropertyIndexTopK(t.Context(), "Item", "value", 1, false)
					require.ErrorIs(t, err, ErrPropertyIndexValueOrderUnavailable)
					require.Nil(t, top)
				}
				ranged, found, err := sm.PropertyIndexRange(t.Context(), "Item", "value", PropertyIndexBounds{HasLower: true, Lower: int64(0), LowerInclusive: true, HasUpper: true, Upper: int64(5), UpperInclusive: true})
				require.NoError(t, err)
				require.True(t, found)
				if _, numeric := step.value.(int64); numeric {
					require.Equal(t, []NodeID{"test:n"}, ranged)
				} else {
					require.Empty(t, ranged)
				}
				previous = next
			}
			if w != nil {
				moved := &Node{ID: previous.ID, Labels: []string{"Other"}, Properties: map[string]interface{}{"value": []byte{4}}}
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpUpdateNode, OldNode: previous, Node: moved}}})
				ids, _, err := sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
				require.NoError(t, err)
				require.Empty(t, ids)
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: moved.ID, OldNode: moved}}})
				ids, _, err = sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
				require.NoError(t, err)
				require.Empty(t, ids)
			}
		})
	}
}

func TestPropertyIndexNonNilCandidatesCancellationAvailabilityAndRetry(t *testing.T) {
	sm := NewSchemaManager()
	ids, found, err := sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, ids)
	require.NoError(t, sm.AddPropertyIndex("nonnull", "Item", []string{"value"}))
	ids, found, err = sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, ids)
	want := make([]NodeID, 1024)
	for i := range want {
		want[i] = NodeID(fmt.Sprintf("test:n%04d", i))
		require.NoError(t, sm.PropertyIndexInsert("Item", "value", want[i], []byte{1, 2}))
	}
	for _, polls := range []int64{1, 11} {
		ctx := newIndexPollCancelContext(t, polls)
		ids, _, err = sm.PropertyIndexNonNilCandidates(ctx, "Item", "value")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
		require.ErrorIs(t, ctx.Context.Err(), context.Canceled)
		ids, found, err = sm.PropertyIndexNonNilCandidates(t.Context(), "Item", "value")
		require.NoError(t, err)
		require.True(t, found)
		slices.Sort(ids)
		require.Equal(t, want, ids)
	}
	idx, _, err := sm.GetPropertyIndex(t.Context(), "Item", "value")
	require.NoError(t, err)
	require.Equal(t, 0, idx.Stats()["valueEntries"], "membership marker is not one equality value")
	stats := sm.GetIndexStats()
	require.Len(t, stats, 1)
	require.Zero(t, stats[0].UniqueValues)
	require.Zero(t, stats[0].TotalEntries)
}
