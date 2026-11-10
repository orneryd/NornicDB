package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A transaction's composite pending read (MergePendingCompositeMatches) lists
// exactly the pending nodes a pass over all of them with compositeNodeMatches
// lists, below and above the pending-index threshold, for full and prefix
// values, numbers equal across types, other labels, missing properties, and
// nodes the transaction rewrites or deletes after the index tracks them.
// PendingCompositeMatchCount bounds the read from above.
func TestTransactionPendingCompositeReadMatchesFullPass(t *testing.T) {
	ctx := t.Context()
	properties := []string{"kind", "shard"}
	probes := [][]interface{}{
		{"a", int64(1)}, {"b", int64(0)}, {"a", float64(2)}, {"a"}, {"b"}, {"c"}, {"a", int64(9)},
	}
	for _, pending := range []int{pendingIndexMinNodes - 2, 60} {
		t.Run(fmt.Sprintf("pending=%d", pending), func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			for i := 0; i < pending; i++ {
				props := map[string]interface{}{"kind": []string{"a", "b"}[i%2], "shard": int64(i % 3)}
				if i%7 == 0 {
					props["shard"] = float64(i % 3)
				}
				if i%11 == 0 {
					delete(props, "shard")
				}
				labels := []string{"P"}
				if i%5 == 0 {
					labels = []string{"Q"}
				}
				_, err := tx.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:p%d", i)), Labels: labels, Properties: props})
				require.NoError(t, err)
			}
			check := func(stage string) {
				for _, values := range probes {
					var expected []NodeID
					for id, node := range tx.pendingNodes {
						matched, err := compositeNodeMatches(ctx, node, "P", properties, values)
						require.NoError(t, err)
						if matched {
							expected = append(expected, id)
						}
					}
					got, err := tx.MergePendingCompositeMatches(ctx, nil, "P", properties, values)
					require.NoError(t, err)
					require.ElementsMatch(t, expected, got, "%s: values %v", stage, values)
					count, err := tx.PendingCompositeMatchCount(ctx, "P", properties, values)
					require.NoError(t, err)
					require.GreaterOrEqual(t, count, len(expected), "%s: values %v", stage, values)
				}
			}
			check("first read")
			require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:p1", Labels: []string{"P"}, Properties: map[string]interface{}{"kind": "b", "shard": int64(0)}}, Policy: ClientNodeEdits}))
			require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:p2", Labels: []string{"P"}, Properties: map[string]interface{}{"kind": "a", "shard": float64(1)}}, Policy: ClientNodeEdits}))
			require.NoError(t, tx.DeleteGraphNode(ctx, "test:p4", CascadeEdges))
			check("after rewrites")
		})
	}
}
