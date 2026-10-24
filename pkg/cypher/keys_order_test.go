package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keys() returns every property key, in one sorted order, on every route and
// every run (#602): a node or relationship created in the same statement, a
// matched one, a map in RETURN or in a SET value. Keys named like internal
// fields ("labels", "type", "_nodeId") are ordinary keys.
func TestKeysReturnsEveryKeyInOneOrder(t *testing.T) {
	strs := func(keys ...string) []interface{} {
		out := make([]interface{}, len(keys))
		for i, key := range keys {
			out[i] = key
		}
		return out
	}
	cases := []struct {
		stmt string
		want []interface{}
	}{
		{"CREATE (n:K {z: 1, a: 2, m: 3, c: 4}) RETURN keys(n) AS k", strs("a", "c", "m", "z")},
		{"MATCH (n:T) RETURN keys(n) AS k", strs("a", "c", "m", "z")},
		{"MATCH (n:T) WITH n RETURN keys(n) AS k", strs("a", "c", "m", "z")},
		{"WITH {z: 1, a: 2, m: 3, c: 4} AS m MATCH (n:T) SET n.k = keys(m) RETURN n.k AS k", strs("a", "c", "m", "z")},
		{"UNWIND [{z: 1, a: 2, m: 3, c: 4}] AS m MATCH (n:T) SET n.k = keys(m) RETURN n.k AS k", strs("a", "c", "m", "z")},
		{"CREATE (:K)-[r:R {y: 1, b: 2, x: 3}]->(:K) RETURN keys(r) AS k", strs("b", "x", "y")},
		{"MATCH ()-[r:RR]->() RETURN keys(r) AS k", strs("b", "x", "y")},
		{"WITH {labels: 1, type: 2, a: 3, _nodeId: 4} AS m RETURN keys(m) AS k", strs("_nodeId", "a", "labels", "type")},
		{"RETURN keys({labels: 1, type: 2, a: 3}) AS k", strs("a", "labels", "type")},
		{"MATCH (n:T) SET n.labels = 1, n.type = 2 RETURN keys(n) AS k", strs("a", "c", "labels", "m", "type", "z")},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					for run := 0; run < 10; run++ {
						exec := build(t)
						ctx := context.Background()
						_, err := exec.Execute(ctx, "CREATE (:T {z: 1, a: 2, m: 3, c: 4})-[:RR {y: 1, b: 2, x: 3}]->(:U)", nil)
						require.NoError(t, err)
						if mode == "explicit transaction" {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						res, err := exec.Execute(ctx, tc.stmt, nil)
						require.NoError(t, err)
						if mode == "explicit transaction" {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						require.Equal(t, [][]interface{}{{tc.want}}, res.Rows, "run %d", run)
					}
				})
			}
		}
	}
}

// PropertyKeys reads all keys of ordinary maps, including graph-shaped maps.
// Only actual entity values expose their nested properties.
func TestPropertyKeysOfGraphShapedOrdinaryMaps(t *testing.T) {
	keys, ok, _ := cypherfn.PropertyKeys(context.Background(), map[string]interface{}{
		"_nodeId": "n1", "labels": []string{"L"}, "id": "n1", "b": 1, "a": 2,
		"properties": map[string]interface{}{"b": 1, "a": 2},
	})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"_nodeId", "a", "b", "id", "labels", "properties"}, keys)
	keys, ok, _ = cypherfn.PropertyKeys(context.Background(), map[string]interface{}{
		"_edgeId": "e1", "type": "R", "properties": map[string]interface{}{"w": 1},
	})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"_edgeId", "properties", "type"}, keys)
	keys, ok, _ = cypherfn.PropertyKeys(context.Background(), map[string]interface{}{"properties": 1, "x": 2})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"properties", "x"}, keys)
	keys, ok, _ = cypherfn.PropertyKeys(context.Background(), &storage.Node{Properties: map[string]interface{}{"b": 1, "a": 2}})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b"}, keys)
	_, ok, _ = cypherfn.PropertyKeys(context.Background(), (*storage.Node)(nil))
	assert.False(t, ok)
	_, ok, _ = cypherfn.PropertyKeys(context.Background(), 42)
	assert.False(t, ok)
}
