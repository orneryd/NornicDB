package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeOrderComparatorOwner(t *testing.T) {
	exec := &StorageExecutor{}
	nodes := []*storage.Node{
		{ID: "a", Properties: map[string]interface{}{"group": int64(2), "rank": int64(1)}},
		{ID: "b", Properties: map[string]interface{}{"group": int64(1), "rank": int64(1)}},
		{ID: "c", Properties: map[string]interface{}{"group": int64(1), "rank": int64(3)}},
		{ID: "d", Properties: map[string]interface{}{"group": int64(2), "rank": int64(4)}},
		{ID: "e", Properties: map[string]interface{}{"rank": int64(9)}},
	}
	original := append([]*storage.Node(nil), nodes...)
	ordered := exec.orderNodes(nodes, "n", "n.group ASC, n.rank DESC")
	require.Equal(t, []*storage.Node{nodes[2], nodes[1], nodes[3], nodes[0], nodes[4]}, ordered)
	top, handled := exec.selectTopKNodesByOrder(nodes, "n", "n.group ASC, n.rank DESC", 3)
	require.True(t, handled)
	require.Equal(t, ordered[:3], top)
	require.Equal(t, original, nodes, "both readers preserve the source slice")
}
