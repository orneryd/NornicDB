package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// newRangeIndexEngine returns a schema manager over an engine whose Item(v)
// index holds every written value; writes apply synchronously.
func newRangeIndexEngine(t *testing.T, flushed, pending []interface{}) *SchemaManager {
	t.Helper()
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	schema := badger.GetSchemaForNamespace("nornic")
	require.NoError(t, schema.AddPropertyIndex("item_v", "Item", []string{"v"}))
	create := func(prefix string, values []interface{}) {
		for i, v := range values {
			_, err := badger.CreateNode(&Node{ID: NodeID(fmt.Sprintf("nornic:%s-%d", prefix, i)), Labels: []string{"Item"}, Properties: map[string]any{"v": v}})
			require.NoError(t, err)
		}
	}
	create("f", flushed)
	create("p", pending)
	return schema
}

func rangeValues(t *testing.T, schema *SchemaManager, flushed, pending []interface{}, bounds PropertyIndexBounds) []interface{} {
	t.Helper()
	ids, ok := schema.PropertyIndexRange("Item", "v", bounds)
	require.True(t, ok)
	values := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		var prefix string
		var i int
		_, err := fmt.Sscanf(string(id), "nornic:%1s-%d", &prefix, &i)
		require.NoError(t, err)
		if prefix == "f" {
			values = append(values, flushed[i])
		} else {
			values = append(values, pending[i])
		}
	}
	return values
}

// PropertyIndexRange lists the values within the bounds, in value order,
// flushed and pending writes alike, and compares numbers only with numbers
// and strings only with strings, as Cypher does.
func TestPropertyIndexRange_SelectsValuesWithinBounds(t *testing.T) {
	flushed := []interface{}{int64(1), 2.5, int64(4), "apple", "cherry", true, int64(9)}
	pending := []interface{}{int64(3), "banana", 0.5}
	schema := newRangeIndexEngine(t, flushed, pending)

	for _, tc := range []struct {
		name   string
		bounds PropertyIndexBounds
		want   []interface{}
	}{
		{"lower inclusive", PropertyIndexBounds{Lower: int64(3), HasLower: true, LowerInclusive: true}, []interface{}{int64(3), int64(4), int64(9)}},
		{"lower exclusive", PropertyIndexBounds{Lower: int64(4), HasLower: true}, []interface{}{int64(9)}},
		{"upper inclusive", PropertyIndexBounds{Upper: 2.5, HasUpper: true, UpperInclusive: true}, []interface{}{0.5, int64(1), 2.5}},
		{"upper exclusive", PropertyIndexBounds{Upper: int64(1), HasUpper: true}, []interface{}{0.5}},
		{"both bounds", PropertyIndexBounds{Lower: int64(1), HasLower: true, Upper: int64(4), HasUpper: true}, []interface{}{2.5, int64(3)}},
		{"strings", PropertyIndexBounds{Lower: "b", HasLower: true, LowerInclusive: true}, []interface{}{"banana", "cherry"}},
		{"string upper", PropertyIndexBounds{Upper: "banana", HasUpper: true, UpperInclusive: true}, []interface{}{"apple", "banana"}},
		{"empty range", PropertyIndexBounds{Lower: int64(5), HasLower: true, Upper: int64(6), HasUpper: true}, []interface{}{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rangeValues(t, schema, flushed, pending, tc.bounds))
		})
	}
}

func TestPropertyIndexRange_DeclinesWithoutIndexOrComparableBound(t *testing.T) {
	schema := newRangeIndexEngine(t, []interface{}{int64(1)}, nil)
	_, ok := schema.PropertyIndexRange("Item", "w", PropertyIndexBounds{Lower: int64(1), HasLower: true})
	require.False(t, ok, "no index on the property")
	_, ok = schema.PropertyIndexRange("Other", "v", PropertyIndexBounds{Lower: int64(1), HasLower: true})
	require.False(t, ok, "no index on the label")
	for _, bound := range []interface{}{true, nil, []interface{}{int64(1)}} {
		_, ok = schema.PropertyIndexRange("Item", "v", PropertyIndexBounds{Upper: bound, HasUpper: true})
		require.False(t, ok, "bound %v is neither a number nor a string", bound)
		_, ok = schema.PropertyIndexRange("Item", "v", PropertyIndexBounds{Lower: bound, HasLower: true})
		require.False(t, ok, "bound %v is neither a number nor a string", bound)
	}
}

func TestCompareIndexRangeValues(t *testing.T) {
	for _, tc := range []struct {
		value, bound interface{}
		order        int
		comparable   bool
	}{
		{int64(1), 2.0, -1, true},
		{3.5, int64(3), 1, true},
		{"b", "b", 0, true},
		{"a", int64(1), 0, false},
		{int64(1), "a", 0, false},
		{true, int64(1), 0, false},
		{[]interface{}{int64(1)}, int64(1), 0, false},
	} {
		order, comparable := compareIndexRangeValues(tc.value, tc.bound)
		require.Equal(t, tc.comparable, comparable, "%v vs %v", tc.value, tc.bound)
		if comparable {
			require.Equal(t, tc.order, order, "%v vs %v", tc.value, tc.bound)
		}
	}
}
