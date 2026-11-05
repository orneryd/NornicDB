package storage

// Read-only audit reproduction for NOR-376 on release f0074ec55: at buffered
// ACK the ordered group visitor (the population behind Cypher's indexed ORDER
// BY window) must publish the same membership as the scalar range reader.
// Both callers now share readPropertyIndexSnapshot, which merges the attached
// pending writes, so this test records whether the stale-group gap is closed
// on the current release.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelatedAckedOrderedGroupPopulationMatchesScalarRange(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddCompositeIndex("doc_rank", "Doc", []string{"rank"}))
	require.NoError(t, sm.BackfillCompositeIndex(t.Context(), "doc_rank", nil))

	// A durable higher-ranked peer keeps an empty index from standing in for
	// the missing population.
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:peer", Labels: []string{"Doc"}, Properties: map[string]any{
		"rank": int64(20),
	}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
		"rank": int64(0),
	}})
	require.NoError(t, err)

	bounds := PropertyIndexBounds{HasLower: true, Lower: int64(10), LowerInclusive: true}
	before, ok, err := sm.PropertyIndexRange(t.Context(), "Doc", "rank", bounds)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []NodeID{"test:peer"}, before, "committed range control before the acknowledged update")

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
		After:  &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"rank": int64(10)}},
		Policy: ClientNodeEdits,
	}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the reproduction must observe a buffered ACK, not a drained apply")
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "nothing may drain before the reads")

	final, err := engine.GetNode(t.Context(), "test:doc")
	require.NoError(t, err)
	require.Equal(t, int64(10), final.Properties["rank"], "the acknowledged final body is already visible")

	ranged, ok, err := sm.PropertyIndexRange(t.Context(), "Doc", "rank", bounds)
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, ranged, NodeID("test:doc"), "the scalar range reader is the pending-aware control")

	var groups [][]NodeID
	found, err := sm.VisitPropertyIndexGroupsInRange(t.Context(), "Doc", "rank", false, bounds, func(ids []NodeID) bool {
		groups = append(groups, append([]NodeID(nil), ids...))
		return true
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, flattenRelatedGroups(groups), NodeID("test:doc"),
		"the ordered group visitor must publish the same acknowledged membership as the range reader")
	t.Logf("groups=%v ranged=%v", groups, ranged)
}

func flattenRelatedGroups(groups [][]NodeID) []NodeID {
	var ids []NodeID
	for _, group := range groups {
		ids = append(ids, group...)
	}
	return ids
}
