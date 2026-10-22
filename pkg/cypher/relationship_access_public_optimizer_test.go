package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Inspection must preserve the node names the optimized routes bind, without
// materializing inline expressions. Public dispatch must retain their rows.
func TestRelationshipAccessPublicOptimizerStructure(t *testing.T) {
	var calls atomic.Int64
	withPluginFunctions(t, map[string]interface{}{
		"inspection.routingkey": func() string { calls.Add(1); return "a" },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "routing_owner"))
	exec.cache = nil
	_, err := exec.Execute(context.Background(), "CREATE (a:RoutingOwner {name:'a'}), (b:RoutingOwner {name:'b'}), (c:RoutingOwner {name:'c'}), (a)-[:RoutingLink]->(b), (b)-[:RoutingLink]->(a), (b)-[:RoutingLink]->(c)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		pattern    QueryPattern
		query      string
		inspection string
		want       [][]interface{}
	}{
		{"mutual", PatternMutualRelationship,
			"MATCH (a:RoutingOwner)-[:RoutingLink]->(b:RoutingOwner)-[:RoutingLink]->(a) RETURN a.name, b.name ORDER BY a.name",
			"MATCH (a:RoutingOwner {name:inspection.routingkey()})-[:RoutingLink]->(b:RoutingOwner)-[:RoutingLink]->(a) RETURN a.name, b.name",
			[][]interface{}{{"a", "b"}, {"b", "a"}}},
		{"incoming", PatternIncomingCountAgg,
			"MATCH (a:RoutingOwner)<-[:RoutingLink]-(b:RoutingOwner) RETURN a.name, count(b) ORDER BY a.name",
			"MATCH (a:RoutingOwner {name:inspection.routingkey()})<-[:RoutingLink]-(b:RoutingOwner) RETURN a.name, count(b)",
			[][]interface{}{{"a", int64(1)}, {"b", int64(1)}, {"c", int64(1)}}},
		{"outgoing", PatternOutgoingCountAgg,
			"MATCH (a:RoutingOwner)-[:RoutingLink]->(b:RoutingOwner) RETURN a.name, count(b) ORDER BY a.name",
			"MATCH (a:RoutingOwner {name:inspection.routingkey()})-[:RoutingLink]->(b:RoutingOwner) RETURN a.name, count(b)",
			[][]interface{}{{"a", int64(1)}, {"b", int64(2)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, query := range []string{tc.query, tc.inspection} {
				ctx := withExpressionFailureSlot(context.Background())
				info := DetectQueryPattern(ctx, query)
				require.Equal(t, tc.pattern, info.Pattern)
				require.Equal(t, "a", info.StartVar)
				require.Equal(t, "b", info.EndVar)
				require.Equal(t, "RoutingLink", info.RelType)
				require.NoError(t, getExpressionFailure(ctx))
				require.Zero(t, calls.Load(), "inspection cannot evaluate property operands")
			}
			_, err := exec.Execute(context.Background(), "EXPLAIN "+tc.query, nil)
			require.NoError(t, err)
			require.Zero(t, calls.Load())
			for _, mode := range []string{"", "PROFILE "} {
				result, err := exec.Execute(context.Background(), mode+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			}
		})
	}
}
