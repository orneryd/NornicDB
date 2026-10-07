package storage

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPropertyIndexKeyRangeNarrowsOnlyOneKind(t *testing.T) {
	index := func(keys ...interface{}) *PropertyIndex {
		idx := &PropertyIndex{values: map[interface{}][]NodeID{}}
		for i, key := range keys {
			idx.values[key] = []NodeID{NodeID(string(rune('a' + i)))}
		}
		idx.keysDirty = true
		return idx
	}
	lower := func(value interface{}, inclusive bool) PropertyIndexBounds {
		return PropertyIndexBounds{Lower: value, HasLower: true, LowerInclusive: inclusive}
	}

	for _, test := range []struct {
		name     string
		idx      *PropertyIndex
		bounds   PropertyIndexBounds
		keys     []interface{}
		narrowed bool
	}{
		{"strings", index("a", "b", "c", "d"), lower("b", false), []interface{}{"c", "d"}, true},
		{"strings inclusive", index("a", "b", "c"), lower("b", true), []interface{}{"b", "c"}, true},
		{"strings with upper", index("a", "b", "c", "d"), PropertyIndexBounds{Lower: "a", HasLower: true, Upper: "c", HasUpper: true}, []interface{}{"b"}, true},
		{"integers", index(int64(1), int64(5), int64(9)), lower(int64(4), true), []interface{}{int64(5), int64(9)}, true},
		{"integers and floats", index(int64(1), 2.5, int64(3)), lower(2.0, false), []interface{}{2.5, int64(3)}, true},
		{"large integers, integer bound", index(int64(math.MaxInt64-2), int64(math.MaxInt64-1)), lower(int64(math.MaxInt64-2), false), []interface{}{int64(math.MaxInt64 - 1)}, true},
		{"large integers, float bound", index(int64(1<<60), int64(1<<60+1)), lower(float64(1<<60), false), []interface{}{int64(1 << 60), int64(1<<60 + 1)}, false},
		{"mixed strings and numbers", index("a", int64(1)), lower("a", true), nil, false},
		{"NaN", index(1.0, math.NaN()), lower(0.5, true), nil, false},
		{"booleans", index(true, false), lower(int64(0), true), nil, false},
		{"string bound on numbers", index(int64(1), int64(2)), lower("a", true), []interface{}{int64(1), int64(2)}, false},
		{"no bounds", index("a", "b"), PropertyIndexBounds{}, []interface{}{"a", "b"}, true},
		{"empty index", index(), lower("a", true), []interface{}{}, false},
		{"float32 keys", index(float32(1.5), float32(2.5)), lower(2.0, false), []interface{}{float32(2.5)}, true},
		{"unsigned keys", index(uint64(1), uint64(5)), lower(int64(2), true), []interface{}{uint64(5)}, true},
		{"other kinds", index(time.Unix(1, 0), time.Unix(2, 0)), lower(int64(0), true), nil, false},
		{"crossed bounds", index(int64(1), int64(2), int64(3)), PropertyIndexBounds{Lower: int64(3), HasLower: true, LowerInclusive: true, Upper: int64(1), HasUpper: true, UpperInclusive: true}, []interface{}{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, narrowed := test.idx.sortedKeyRangeLocked(test.bounds)
			require.Equal(t, test.narrowed, narrowed)
			if test.keys != nil {
				require.Equal(t, test.keys, append([]interface{}{}, keys...))
			}
		})
	}
}

func TestVisitPropertyIndexGroupsInRange(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	schema := engine.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("idx_item_t", "Item", []string{"t"}))
	for i := 0; i < 10; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID("nornic:" + string(rune('a'+i))), Labels: []string{"Item"}, Properties: map[string]interface{}{"t": int64(i / 2)}})
		require.NoError(t, err)
	}
	bare := func(ids []NodeID) []NodeID {
		out := make([]NodeID, len(ids))
		for i, id := range ids {
			out[i] = NodeID(strings.TrimPrefix(string(id), "nornic:"))
		}
		return out
	}
	groups := func(descending bool, bounds PropertyIndexBounds) [][]NodeID {
		var out [][]NodeID
		require.True(t, schema.VisitPropertyIndexGroupsInRange("Item", "t", descending, bounds, func(ids []NodeID) bool {
			out = append(out, bare(ids))
			return true
		}))
		return out
	}
	all := groups(false, PropertyIndexBounds{})
	require.Len(t, all, 5)
	require.Equal(t, [][]NodeID{{"e", "f"}, {"g", "h"}, {"i", "j"}}, groups(false, PropertyIndexBounds{Lower: int64(2), HasLower: true, LowerInclusive: true}))
	require.Equal(t, [][]NodeID{{"g", "h"}, {"e", "f"}}, groups(true, PropertyIndexBounds{Lower: int64(1), HasLower: true, Upper: int64(3), HasUpper: true, UpperInclusive: true}))
	require.Equal(t, [][]NodeID{{"i", "j"}, {"g", "h"}, {"e", "f"}, {"c", "d"}, {"a", "b"}}, groups(true, PropertyIndexBounds{}))
	var visited [][]NodeID
	schema.VisitPropertyIndexGroups("Item", "t", false, func(ids []NodeID) bool {
		visited = append(visited, bare(ids))
		return len(visited) < 2
	})
	require.Equal(t, all[:2], visited)
	require.False(t, schema.VisitPropertyIndexGroupsInRange("Missing", "t", false, PropertyIndexBounds{}, func([]NodeID) bool { return true }))

	ids, ok := schema.PropertyIndexRange("Item", "t", PropertyIndexBounds{Lower: int64(3), HasLower: true, LowerInclusive: true})
	require.True(t, ok)
	require.Equal(t, []NodeID{"g", "h", "i", "j"}, bare(ids))
}

// TestPropertyIndexRangeConcurrentReaders runs readers of a property index
// while a writer keeps its sorted-key cache stale: under the race detector,
// no reader may rebuild the cache under the shared read lock (#942).
func TestPropertyIndexRangeConcurrentReaders(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	schema := engine.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("idx_item_t", "Item", []string{"t"}))
	create := func(i int) error {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"t": int64(i)}})
		return err
	}
	for i := 0; i < 200; i++ {
		require.NoError(t, create(i))
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for i := 200; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if create(i) != nil {
				return
			}
		}
	}()
	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 300; i++ {
				ids, ok := schema.PropertyIndexRange("Item", "t", PropertyIndexBounds{Lower: int64(10), HasLower: true})
				if !ok || len(ids) < 189 {
					t.Errorf("range read %d ids, ok %v", len(ids), ok)
					return
				}
				schema.PropertyIndexTopK("Item", "t", 5, true)
			}
		}()
	}
	readers.Wait()
	close(stop)
	<-done
}

// TestVisitPropertyIndexGroupsInRangeEqualKeysOfTwoTypes: a number and a
// string that compare equal (2 and "2"; the index files every number as a
// float64) are separate index keys but one group, visited together in either
// direction.
func TestVisitPropertyIndexGroupsInRangeEqualKeysOfTwoTypes(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	schema := engine.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("idx_item_t", "Item", []string{"t"}))
	for id, value := range map[string]interface{}{"nornic:a": int64(1), "nornic:b": int64(2), "nornic:c": "2", "nornic:d": int64(3)} {
		_, err := engine.CreateNode(&Node{ID: NodeID(id), Labels: []string{"Item"}, Properties: map[string]interface{}{"t": value}})
		require.NoError(t, err)
	}
	for _, descending := range []bool{false, true} {
		var groups [][]NodeID
		schema.VisitPropertyIndexGroupsInRange("Item", "t", descending, PropertyIndexBounds{}, func(ids []NodeID) bool {
			groups = append(groups, append([]NodeID(nil), ids...))
			return true
		})
		want := [][]NodeID{{"nornic:a"}, {"nornic:b", "nornic:c"}, {"nornic:d"}}
		if descending {
			want = [][]NodeID{{"nornic:d"}, {"nornic:b", "nornic:c"}, {"nornic:a"}}
		}
		require.Equal(t, want, groups, "descending %v", descending)
	}
}
