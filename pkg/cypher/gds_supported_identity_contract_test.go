package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSSupportedNativeIdentityContracts(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "u5_native_gds")
	for _, id := range []string{"alpha", "beta", "shared", "1", "true"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"name": id, "value": int64(7)}})
		require.NoError(t, err)
	}
	for _, id := range []string{"alpha", "beta"} {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(id), EndNode: "shared", Type: "K"}))
	}
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('u5_native_contract', 'P', 'K', {nodeProperties: ['value']})", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "CALL gds.graph.drop('u5_native_contract')", nil) })
	t.Run("fastRP typed output", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL gds.fastRP.stream('u5_native_contract', {embeddingDimension:4, randomSeed:7}) YIELD nodeId RETURN nodeId + ':ok' ORDER BY nodeId", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
		for _, row := range result.Rows {
			require.True(t, strings.HasSuffix(row[0].(string), ":ok"))
		}
	})
	for _, proc := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
		t.Run(proc+" typed output", func(t *testing.T) {
			result, err := exec.Execute(ctx, "CALL gds.linkPrediction."+proc+".stream('u5_native_contract', {sourceNode:'alpha', targetNodes:['beta']}) YIELD node1,node2 RETURN node1 + ':ok', node2 + ':ok'", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"alpha:ok", "beta:ok"}}, result.Rows)
		})
	}
	t.Run("metadata matches stored IDs", func(t *testing.T) {
		result, err := exec.Execute(ctx, "SHOW PROCEDURES YIELD name, signature WHERE name = 'gds.fastRP.stream' OR name STARTS WITH 'gds.linkPrediction.' RETURN name, signature", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 7)
		for _, row := range result.Rows {
			signature := row[1].(string)
			if row[0] == "gds.fastRP.stream" {
				require.Contains(t, signature, "nodeId :: STRING")
			} else {
				require.Contains(t, signature, "node1 :: STRING")
				require.Contains(t, signature, "node2 :: STRING")
			}
		}
	})
	t.Run("native string ID utility round trip", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:P {name:'alpha'}) RETURN gds.util.asNode(id(n)).name, [x IN gds.util.asNodes([id(n), '1', 1]) | x.name], gds.util.nodeProperty('u5_native_contract', id(n), 'value')", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"alpha", []interface{}{"alpha", "1", "1"}, float64(7)}}, result.Rows)
	})
	t.Run("legacy string coercion stays supported", func(t *testing.T) {
		result, err := exec.Execute(ctx, "RETURN gds.util.asNode(true).name, gds.util.asNode(1.0).name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"true", "1"}}, result.Rows)
	})
}
