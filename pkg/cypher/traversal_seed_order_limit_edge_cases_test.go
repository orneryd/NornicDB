package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTraversalEndSeedOrderLimit_EarlyExitGuards(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_end_seed_guard_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	returnItems := []returnItem{{expr: "d.id"}}

	res, used, err := exec.tryExecuteTraversalEndSeedOrderLimit(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "r.name ASC", 0)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	res, used, err = exec.tryExecuteTraversalEndSeedOrderLimit(ctx, "(d)-[:IN_ROOM]->(r)", "", returnItems, "", "r.name ASC", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	res, used, err = exec.tryExecuteTraversalEndSeedOrderLimit(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "d.id = 'x'", returnItems, "", "r.name ASC", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	res, used, err = exec.tryExecuteTraversalEndSeedOrderLimit(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "r.name IS NOT NULL", returnItems, "", "x.name ASC", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)
}

func TestTraversalEndSeedOrderLimit_PartialGeneralIndexDeclines(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_end_seed_empty_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "room-1", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE INDEX idx_room_name_empty FOR (r:Room) ON (r.name)", nil)
	require.NoError(t, err)

	res, used, err := exec.tryExecuteTraversalEndSeedOrderLimit(
		ctx,
		"(d:Drawer)-[:IN_ROOM]->(r:Room)",
		"r.name = 'missing'",
		[]returnItem{{expr: "d.id", alias: "did"}},
		"",
		"r.name ASC",
		2,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)
}

func TestTraversalEndSeedOrderLimit_FallbackWhenSeedWindowInsufficient(t *testing.T) {
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
	res, used, err := exec.tryExecuteTraversalEndSeedOrderLimit(
		ctx,
		"(d:Drawer)-[:IN_ROOM]->(r:Room)",
		"r.name IS NOT NULL",
		[]returnItem{{expr: "d.id"}, {expr: "r.name"}},
		"",
		"r.name ASC",
		1,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)
}

func TestTraversalStartSeedOrderLimit_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_start_seed_more_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	returnItems := []returnItem{{expr: "d.id"}}

	res, used, err := exec.tryExecuteTraversalStartSeedOrderLimit(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "d.rank DESC", 0)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	res, used, err = exec.tryExecuteTraversalStartSeedOrderLimit(ctx, "(:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "d.rank DESC", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	res, used, err = exec.tryExecuteTraversalStartSeedOrderLimit(ctx, "(d:Drawer)-[:IN_ROOM]->(r:Room)", "", returnItems, "", "", 2)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)
}

func TestTraversalCollectStartSeedNodes_InputAndTruncateBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "trav_collect_seed_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	nodes, used, err := exec.tryCollectTraversalStartSeedOrderNodes(ctx, nil, nodePatternInfo{}, "", "d.rank DESC", 2)
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

	nodes, used, err = exec.tryCollectTraversalStartSeedOrderNodes(
		ctx, nil,
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

	nodes, used, err := exec.tryCollectNodesFromStartPropertyScan(ctx, nil, nodePatternInfo{variable: "n", labels: []string{"Missing"}}, "n.name = 'x'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromStartPropertyScan(ctx, nil, nodePatternInfo{variable: "n", labels: []string{"Missing"}}, "n.name IS NOT NULL")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)
}
