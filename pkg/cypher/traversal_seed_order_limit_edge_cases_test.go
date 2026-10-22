package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTraversalSeedOrderLimit_EarlyExitGuards(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_end_seed_guard_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	returnItems := []returnItem{{expr: "d.id"}}

	fixtureAdmission909Context, fixtureAdmission909 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems)
	_ = fixtureAdmission909Context
	res, used, err := exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission909, fixtureAdmission909Context, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "r.name ASC", 0, traversalEndSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission910Context, fixtureAdmission910 := traversalTestContext(ctx, "(d)-[:IN_ROOM]->(r)", "", returnItems)
	_ = fixtureAdmission910Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission910, fixtureAdmission910Context, "(d)-[:IN_ROOM]->(r)", "", returnItems, "", "r.name ASC", 2, traversalEndSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission911Context, fixtureAdmission911 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "d.id = 'x'", returnItems)
	_ = fixtureAdmission911Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission911, fixtureAdmission911Context, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "d.id = 'x'", returnItems, "", "r.name ASC", 2, traversalEndSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission912Context, fixtureAdmission912 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "r.name IS NOT NULL", returnItems)
	_ = fixtureAdmission912Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission912, fixtureAdmission912Context, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "r.name IS NOT NULL", returnItems, "", "x.name ASC", 2, traversalEndSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission913Context, fixtureAdmission913 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems)
	_ = fixtureAdmission913Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission913, fixtureAdmission913Context, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "d.rank DESC", 0, traversalStartSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission914Context, fixtureAdmission914 := traversalTestContext(ctx, "(:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems)
	_ = fixtureAdmission914Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission914, fixtureAdmission914Context, "(:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "d.rank DESC", 2, traversalStartSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	fixtureAdmission915Context, fixtureAdmission915 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems)
	_ = fixtureAdmission915Context
	res, used, err = exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission915, fixtureAdmission915Context, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "", 2, traversalStartSeed)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)
}

func TestTraversalEndSeedOrderLimit_IndexWindowDeclines(t *testing.T) {
	t.Run("no_matching_seed", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "trav_end_seed_empty_cov")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := store.CreateNode(&storage.Node{ID: "room-1", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "A"}})
		require.NoError(t, err)

		_, err = exec.Execute(ctx, "CREATE INDEX idx_room_name_empty FOR (r:Room) ON (r.name)", nil)
		require.NoError(t, err)

		fixtureAdmission916Context, fixtureAdmission916 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "r.name = 'missing'", []returnItem{{expr: "d.id", alias: "did"}})
		_ = fixtureAdmission916Context
		res, used, err := exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission916,
			fixtureAdmission916Context,
			"(d:Drawer)-[:IN_ROOM]->(r:Room)",
			"r.name = 'missing'",
			[]returnItem{{expr: "d.id", alias: "did"}},
			"",
			"r.name ASC",
			2,
			traversalEndSeed,
		)
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, res)
	})

	t.Run("seed_window_insufficient", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "trav_end_seed_fallback_cov")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		for i := 0; i < 200; i++ {
			id := storage.NodeID(fmt.Sprintf("room-%03d", i))
			_, err := store.CreateNode(&storage.Node{
				ID:     id,
				Labels: []string{"Room"},
				Properties: map[string]interface{}{
					"name": fmt.Sprintf("R%03d", i),
				},
			})
			require.NoError(t, err)
		}

		_, err := exec.Execute(ctx, "CREATE INDEX idx_room_name_fallback FOR (r:Room) ON (r.name)", nil)
		require.NoError(t, err)

		// No Drawer->Room edges exist, so traversal from end-seeded rooms yields no paths.
		// With limit=1, seedLimit=200; when seedNodes >= seedLimit and produced paths < limit,
		// helper intentionally returns (nil,false,nil) to fall back to full execution.
		fixtureAdmission981Context, fixtureAdmission981 := traversalTestContext(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "r.name IS NOT NULL", []returnItem{{expr: "d.id"}, {expr: "r.name"}})
		_ = fixtureAdmission981Context
		res, used, err := exec.tryExecuteTraversalSeedOrderLimit(fixtureAdmission981,
			fixtureAdmission981Context,
			"(d:Drawer)-[:IN_ROOM]->(r:Room)",
			"r.name IS NOT NULL",
			[]returnItem{{expr: "d.id"}, {expr: "r.name"}},
			"",
			"r.name ASC",
			1,
			traversalEndSeed,
		)
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, res)
	})
}

func TestTraversalCollectStartSeedNodes_InputAndTruncateBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_collect_seed_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1515Context, fixtureAdmission1515 := expressionTestContext(ctx, "")
	_ = fixtureAdmission1515Context
	nodes, used, err := exec.tryCollectTraversalStartSeedOrderNodes(fixtureAdmission1515, fixtureAdmission1515Context, nodePatternInfo{}, "", "d.rank DESC", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	for i := 0; i < 8; i++ {
		id := storage.NodeID(fmt.Sprintf("d-%02d", i))
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Drawer"}, Properties: map[string]interface{}{"rank": int64(i)}})
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, "CREATE INDEX idx_drawer_rank_cov FOR (d:Drawer) ON (d.rank)", nil)
	require.NoError(t, err)

	fixtureAdmission1516Context, fixtureAdmission1516 := expressionTestContext(ctx, "d.rank IS NOT NULL")
	_ = fixtureAdmission1516Context
	nodes, used, err = exec.tryCollectTraversalStartSeedOrderNodes(fixtureAdmission1516,
		fixtureAdmission1516Context,
		nodePatternInfo{variable: "d", labels: []string{"Drawer"}},
		"d.rank IS NOT NULL",
		"d.rank DESC",
		3,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 3)
	require.Equal(t, storage.NodeID("d-07"), nodes[0].ID)
	require.Equal(t, storage.NodeID("d-06"), nodes[1].ID)
	require.Equal(t, storage.NodeID("d-05"), nodes[2].ID)
}

func TestTraversalStartPropertyScan_EmptyCandidateBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_scan_empty_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1517Context, fixtureAdmission1517 := expressionTestContext(ctx, "n.name = 'x'")
	_ = fixtureAdmission1517Context
	nodes, used, err := exec.tryCollectNodesFromStartPropertyScan(fixtureAdmission1517, fixtureAdmission1517Context, nodePatternInfo{variable: "n", labels: []string{"Missing"}}, "n.name = 'x'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1518Context, fixtureAdmission1518 := expressionTestContext(ctx, "n.name IS NOT NULL")
	_ = fixtureAdmission1518Context
	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(fixtureAdmission1518, fixtureAdmission1518Context, nodePatternInfo{variable: "n", labels: []string{"Missing"}}, "n.name IS NOT NULL")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)
}
