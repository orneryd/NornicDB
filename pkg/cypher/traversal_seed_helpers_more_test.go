package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTraversalSeedHelpers_WhereExtractionAndReverse(t *testing.T) {
	where := "d.filed_at IS NOT NULL AND r.name = 'root' AND id(d) = 'x' AND elementId(d) = 'x2'"
	seedOnly := (&StorageExecutor{}).extractTraversalSeedWhereClause(originalBraceView(where), "d", "r")
	require.Equal(t, "d.filed_at IS NOT NULL AND id(d) = 'x' AND elementId(d) = 'x2'", seedOnly)
	require.Equal(t, "", (&StorageExecutor{}).extractTraversalSeedWhereClause(originalBraceView(""), "d"))

	orig := &TraversalMatch{
		StartNode:    nodePatternInfo{variable: "a", labels: []string{"A"}},
		EndNode:      nodePatternInfo{variable: "b", labels: []string{"B"}},
		Relationship: RelationshipPattern{Direction: "outgoing", MinHops: 1, MaxHops: 3, Types: []string{"R"}},
	}
	reversed := reverseTraversalMatch(orig)
	require.NotNil(t, reversed)
	require.Equal(t, "incoming", reversed.Relationship.Direction)
	require.Equal(t, "b", reversed.StartNode.variable)
	require.Equal(t, "a", reversed.EndNode.variable)
	require.Nil(t, reverseTraversalMatch(&TraversalMatch{IsChained: true}))
	require.Nil(t, reverseTraversalMatch(nil))

	require.Equal(t, 200, topKSeedLimit(1))
	require.Equal(t, 800, topKSeedLimit(200))
}

func TestTraversalStartSeedOrderLimit_Deterministic(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_start_seed_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "wing-1", Labels: []string{"Wing"}, Properties: map[string]interface{}{"name": "Wing"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "room-1", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "Room"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "room-wing", Type: "IN_WING", StartNode: "room-1", EndNode: "wing-1"}))

	for i, drawerID := range []string{"d-3", "d-1", "d-2"} {
		_, err = store.CreateNode(&storage.Node{
			ID:     storage.NodeID(drawerID),
			Labels: []string{"Drawer"},
			Properties: map[string]interface{}{
				"drawer_id": drawerID,
				"filed_at":  int64(100 - i*10),
			},
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("e-" + drawerID), Type: "IN_ROOM", StartNode: storage.NodeID(drawerID), EndNode: "room-1"}))
	}

	_, err = exec.Execute(ctx, "CREATE INDEX idx_drawer_filed_at FOR (d:Drawer) ON (d.filed_at)", nil)
	require.NoError(t, err)

	returnItems := []returnItem{
		{expr: "d.drawer_id"},
		{expr: "d.filed_at"},
	}
	res, used, err := exec.tryExecuteTraversalSeedOrderLimit(
		ctx,
		originalBraceView("(d:Drawer)-[:IN_ROOM]->(r:Room)"),
		expressionBraceView("d.filed_at IS NOT NULL"),
		returnItems,
		"",
		expressionBraceView("d.filed_at DESC"),
		2,
		traversalStartSeed,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "d-3", res.Rows[0][0])
	require.EqualValues(t, int64(100), res.Rows[0][1])
	require.Equal(t, "d-1", res.Rows[1][0])

	_, used, err = exec.tryExecuteTraversalSeedOrderLimit(ctx, originalBraceView("(d)-[:IN_ROOM]->(r)"), originalBraceView(""), returnItems, "", originalBraceView(""), 2, traversalStartSeed)
	require.NoError(t, err)
	require.False(t, used)
}

func TestTraversalEndSeedOrderLimit_Deterministic(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_end_seed_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "room-1", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "room-2", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "B"}})
	require.NoError(t, err)
	for i, drawerID := range []string{"d-1", "d-2", "d-3"} {
		_, err = store.CreateNode(&storage.Node{
			ID:     storage.NodeID(drawerID),
			Labels: []string{"Drawer"},
			Properties: map[string]interface{}{
				"drawer_id": drawerID,
				"rank":      int64(i + 1),
			},
		})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-1", Type: "IN_ROOM", StartNode: "d-1", EndNode: "room-1"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-2", Type: "IN_ROOM", StartNode: "d-2", EndNode: "room-1"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-3", Type: "IN_ROOM", StartNode: "d-3", EndNode: "room-2"}))

	_, err = exec.Execute(ctx, "CREATE INDEX idx_room_name FOR (r:Room) ON (r.name)", nil)
	require.NoError(t, err)

	returnItems := []returnItem{{expr: "d.drawer_id"}, {expr: "r.name"}}
	res, used, err := exec.tryExecuteTraversalSeedOrderLimit(
		ctx,
		originalBraceView("(d:Drawer)-[:IN_ROOM]->(r:Room)"),
		expressionBraceView("r.name IS NOT NULL"),
		returnItems,
		"",
		expressionBraceView("r.name ASC"),
		2,
		traversalEndSeed,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, res.Rows, 2)
	// room-1 (A) paths should come before room-2 (B) due end-seed ordering.
	require.Equal(t, "A", res.Rows[0][1])
	require.Equal(t, "A", res.Rows[1][1])
}
