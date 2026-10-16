package cypher

import (
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
	for _, left := range values {
		for _, right := range values {
			var index cypherValueIndex
			index.add(left)
			_, duplicate := index.add(right)
			require.Equal(t, cypherEquivalenceKey(left) == cypherEquivalenceKey(right), duplicate, "%#v vs %#v", left, right)
			var collisions cypherValueIndex
			collisions.addTupleValues(42, []interface{}{left})
			_, duplicate = collisions.addTupleValues(42, []interface{}{right})
			require.Equal(t, cypherEquivalenceKey(left) == cypherEquivalenceKey(right), duplicate, "forced collision: %#v vs %#v", left, right)
			if cypherEquivalenceKey(left) == cypherEquivalenceKey(right) {
				require.Equal(t, cypherEquivalenceHash(left), cypherEquivalenceHash(right), "equivalent values hash alike: %#v vs %#v", left, right)
			}
		}
	}
}

func TestCypherValueIndexHashCollisions(t *testing.T) {
	var index cypherValueIndex
	position, exists := index.addTupleValues(42, []interface{}{int64(1), "a"})
	require.False(t, exists)
	require.Equal(t, 0, position)
	position, exists = index.addTupleValues(42, []interface{}{int64(2), "a"})
	require.False(t, exists)
	require.Equal(t, 1, position)
	position, exists = index.addTupleValues(42, []interface{}{int64(1)})
	require.False(t, exists)
	require.Equal(t, 2, position)
	position, exists = index.addTupleValues(42, []interface{}{int64(1), "a"})
	require.True(t, exists)
	require.Equal(t, 0, position)
	var empty cypherValueIndex
	_, exists = empty.addTuple(nil)
	require.False(t, exists)
	_, exists = empty.addTuple(nil)
	require.True(t, exists)
	var structural cypherValueIndex
	_, exists = structural.addTupleValues(42, []interface{}{map[string]interface{}{"a": []interface{}{int64(1), nil}}})
	require.False(t, exists)
	_, exists = structural.addTupleValues(42, []interface{}{map[string]interface{}{"a": []interface{}{1.0, nil}}})
	require.True(t, exists)
	_, exists = structural.addTupleValues(42, []interface{}{map[string]interface{}{"b": []interface{}{int64(1), nil}}})
	require.False(t, exists)
}
