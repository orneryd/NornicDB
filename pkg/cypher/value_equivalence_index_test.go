package cypher

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCypherValueIndexCanonicalEquivalence(t *testing.T) {
	values := []interface{}{
		map[string]interface{}{}, map[string]interface{}{"a": int64(1)},
		nil, (*storage.Node)(nil), (*storage.Edge)(nil), "", "N", true, false,
		int64(0), math.Copysign(0, -1), int64(1), 1, int32(1), uint8(1), float32(1), 1.0,
		float32(2.5), 2.5, int64(-7), -7.0, int64(math.MinInt64), float64(math.MinInt64),
		int64(9007199254740993), 9007199254740992.0, uint64(1) << 63, math.Ldexp(1, 63),
		uint64(math.MaxUint64), math.Ldexp(1, 64), math.MaxFloat64, math.Inf(1), math.Inf(-1), math.NaN(),
		[]byte("abc"), "Xabc", []int64{1, 2}, []interface{}{1.0, 2.0}, []interface{}{nil, 1}, []interface{}{nil, 1.0},
		[]interface{}{math.NaN()}, map[string]interface{}{"a": 1, "b": nil}, map[string]interface{}{"b": nil, "a": 1.0},
		map[interface{}]interface{}{"a": 1, "b": nil}, &storage.Node{ID: "x"}, &storage.Edge{ID: "x"},
		time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
		storage.RemoteEntityValue(nil), storage.RemotePathValue(nil),
		storage.RemoteEntityValue{"elementId": "remote-a", "name": "before"},
		storage.RemoteEntityValue{"elementId": "remote-a", "name": "after"},
		storage.RemoteEntityValue{"elementId": "remote-b"},
		storage.RemotePathValue{storage.RemoteEntityValue{"elementId": "remote-a"}},
		storage.RemotePathValue{storage.RemoteEntityValue{"elementId": "remote-b"}},
	}
	scopedNodes := []*storage.Node{
		{ID: "shared"}, {ID: "shared", Properties: map[string]interface{}{"changed": true}}, {ID: "shared"},
		{ID: "shared"}, {ID: "shared"}, {ID: "shared"}, {ID: "shared"},
	}
	for i, database := range []string{"one", "one", "two", "one", "two", "one", "two"} {
		scopedNodes[i].SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: database})
	}
	values = append(values,
		scopedNodes[0],
		scopedNodes[1],
		scopedNodes[2],
		&storage.Edge{ID: "shared", Database: "one"},
		&storage.Edge{ID: "shared", Database: "two"},
		[]interface{}{scopedNodes[3]},
		[]interface{}{scopedNodes[4]},
		map[string]interface{}{"node": scopedNodes[5]},
		map[string]interface{}{"node": scopedNodes[6]},
	)
	values = append(values, pathCarriers()...)
	for _, path := range pathCarriers() {
		values = append(values, []interface{}{path}, map[string]interface{}{"p": path})
	}
	for _, left := range values {
		for _, right := range values {
			var index cypherValueIndex
			index.addByContract(left, cypherEquivalenceContract)
			_, duplicate := index.addByContract(right, cypherEquivalenceContract)
			require.Equal(t, cypherValueKey(left, cypherEquivalenceContract) == cypherValueKey(right, cypherEquivalenceContract), duplicate, "%#v vs %#v", left, right)
			var collisions cypherValueIndex
			collisions.addTupleValuesByContract(42, []interface{}{left}, cypherEquivalenceContract)
			_, duplicate = collisions.addTupleValuesByContract(42, []interface{}{right}, cypherEquivalenceContract)
			require.Equal(t, cypherValueKey(left, cypherEquivalenceContract) == cypherValueKey(right, cypherEquivalenceContract), duplicate, "forced collision: %#v vs %#v", left, right)
			if cypherValueKey(left, cypherEquivalenceContract) == cypherValueKey(right, cypherEquivalenceContract) {
				require.Equal(t, cypherValueHash(left, cypherEquivalenceContract), cypherValueHash(right, cypherEquivalenceContract), "equivalent values hash alike: %#v vs %#v", left, right)
			}
		}
	}
}

func TestCypherValueIndexHashCollisions(t *testing.T) {
	var index cypherValueIndex
	position, exists := index.addTupleValuesByContract(42, []interface{}{int64(1), "a"}, cypherEquivalenceContract)
	require.False(t, exists)
	require.Equal(t, 0, position)
	position, exists = index.addTupleValuesByContract(42, []interface{}{int64(2), "a"}, cypherEquivalenceContract)
	require.False(t, exists)
	require.Equal(t, 1, position)
	position, exists = index.addTupleValuesByContract(42, []interface{}{int64(1)}, cypherEquivalenceContract)
	require.False(t, exists)
	require.Equal(t, 2, position)
	position, exists = index.addTupleValuesByContract(42, []interface{}{int64(1), "a"}, cypherEquivalenceContract)
	require.True(t, exists)
	require.Equal(t, 0, position)
	var empty cypherValueIndex
	_, exists = empty.addTupleByContract(nil, cypherEquivalenceContract)
	require.False(t, exists)
	_, exists = empty.addTupleByContract(nil, cypherEquivalenceContract)
	require.True(t, exists)
	var structural cypherValueIndex
	_, exists = structural.addTupleValuesByContract(42, []interface{}{map[string]interface{}{"a": []interface{}{int64(1), nil}}}, cypherEquivalenceContract)
	require.False(t, exists)
	_, exists = structural.addTupleValuesByContract(42, []interface{}{map[string]interface{}{"a": []interface{}{1.0, nil}}}, cypherEquivalenceContract)
	require.True(t, exists)
	_, exists = structural.addTupleValuesByContract(42, []interface{}{map[string]interface{}{"b": []interface{}{int64(1), nil}}}, cypherEquivalenceContract)
	require.False(t, exists)
}

// pathCarriers holds every engine representation of the one-node path (a):
// native pointer and value, both map wrapper shapes and an entity snapshot,
// then a different path and an ordinary map with a forged _pathResult key.
func pathCarriers() []interface{} {
	a := &storage.Node{ID: "a", Properties: map[string]interface{}{"version": int64(1)}}
	snapshot := &storage.Node{ID: "a", Properties: map[string]interface{}{"version": int64(2)}}
	b := &storage.Node{ID: "b"}
	native := &PathResult{Nodes: []*storage.Node{a}}
	return []interface{}{
		native,
		PathResult{Nodes: []*storage.Node{snapshot}},
		map[string]interface{}{"_pathResult": native, "length": 0, "nodes": native.Nodes, "rels": native.Relationships},
		map[string]interface{}{"_pathResult": PathResult{Nodes: []*storage.Node{snapshot}}, "nodes": []interface{}{map[string]interface{}{"version": int64(2)}}, "relationships": []interface{}{}, "length": int64(0)},
		&PathResult{Nodes: []*storage.Node{a, b}, Relationships: []*storage.Edge{{ID: "r", StartNode: "a", EndNode: "b", Type: "R"}}, Length: 1},
		map[string]interface{}{"_pathResult": "ordinary user property"},
	}
}

func TestDistinctPathCarriers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "distinct_paths"))
	paths := pathCarriers()
	run := func(query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err)
		return result.Rows
	}
	// Four carriers of path (a), the path (a)-[r]->(b) and the forged map.
	values := map[string]interface{}{"paths": paths}
	require.Equal(t, [][]interface{}{{int64(3)}}, run("WITH $paths AS paths CALL (paths) { UNWIND paths AS p WITH DISTINCT p RETURN count(*) AS n } RETURN n", values))
	require.Equal(t, [][]interface{}{{int64(3)}}, run("UNWIND $paths AS p RETURN count(DISTINCT p) AS n", values))
	require.Len(t, run("UNWIND $paths AS p RETURN DISTINCT p", values), 3)
	require.ElementsMatch(t, [][]interface{}{{int64(4)}, {int64(1)}, {int64(1)}}, run("UNWIND $paths AS p WITH p, count(*) AS n RETURN n", values))
	require.Len(t, run("UNWIND $a AS p RETURN p UNION UNWIND $b AS p RETURN p", map[string]interface{}{"a": paths[:2], "b": paths[2:4]}), 1)
	nested := []interface{}{[]interface{}{paths[0]}, []interface{}{paths[3]}, map[string]interface{}{"p": paths[2]}, map[string]interface{}{"p": paths[1]}}
	require.Equal(t, [][]interface{}{{int64(2)}}, run("UNWIND $values AS v RETURN count(DISTINCT v) AS n", map[string]interface{}{"values": nested}))
}
