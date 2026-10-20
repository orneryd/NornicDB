package storage

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// One exact numeric key: a property index answers equality, range and
// ordered visits exactly for integers beyond 2^53 and 2^63 next to floats,
// with binary-search narrowing kept for an all-number index.
func TestPropertyIndexNumericKeysExact(t *testing.T) {
	values := []interface{}{
		int64(9007199254740991), int64(9007199254740992), int64(9007199254740993),
		int64(9007199254740994), uint64(9007199254740995), int64(-9007199254740993),
		int64(math.MaxInt64), uint64(math.MaxUint64), float64(9007199254740996), 0x1p63, -0x1p53 - 2, 1.5,
	}
	engine := NewMemoryEngine()
	defer engine.Close()
	schema := engine.GetSchema()
	require.NoError(t, schema.AddPropertyIndex("idx_precise", "P", []string{"id"}))
	ids := make([]NodeID, len(values))
	for i, value := range values {
		ids[i] = NodeID(fmt.Sprintf("nornic:%02d", i))
		_, err := engine.CreateNode(&Node{ID: ids[i], Labels: []string{"P"}, Properties: map[string]interface{}{"id": value}})
		require.NoError(t, err)
	}
	exact := func(v interface{}) *big.Float {
		f := new(big.Float).SetPrec(128)
		switch n := v.(type) {
		case int64:
			return f.SetInt64(n)
		case uint64:
			return f.SetUint64(n)
		case float64:
			return f.SetFloat64(n)
		}
		panic(fmt.Sprintf("%T", v))
	}
	sorted := func(in []NodeID) []NodeID {
		out := append([]NodeID(nil), in...)
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}

	// Keys: a float64 key only where a float64 holds the value exactly.
	key, _ := indexValueKey(int64(9007199254740992))
	require.Equal(t, float64(9007199254740992), key)
	key, _ = indexValueKey(int64(9007199254740993))
	require.Equal(t, uint64(9007199254740993), key)
	key, _ = indexValueKey(int64(-9007199254740993))
	require.Equal(t, int64(-9007199254740993), key)
	key, _ = indexValueKey(uint(9007199254740993))
	require.Equal(t, uint64(9007199254740993), key)

	require.Equal(t, []NodeID{ids[2]}, schema.PropertyIndexLookup("P", "id", int64(9007199254740993)))
	require.Equal(t, []NodeID{ids[1]}, schema.PropertyIndexLookup("P", "id", float64(9007199254740992)))
	require.Equal(t, []NodeID{ids[4]}, schema.PropertyIndexLookup("P", "id", int64(9007199254740995)))

	for _, bound := range []interface{}{int64(9007199254740992), float64(9007199254740992), uint64(9007199254740993), int64(math.MaxInt64), 0x1p63} {
		for _, inclusive := range []bool{false, true} {
			for _, upper := range []bool{false, true} {
				bounds := PropertyIndexBounds{Lower: bound, HasLower: true, LowerInclusive: inclusive}
				if upper {
					bounds = PropertyIndexBounds{Upper: bound, HasUpper: true, UpperInclusive: inclusive}
				}
				var want []NodeID
				for i, value := range values {
					c := exact(value).Cmp(exact(bound))
					if (!upper && (c > 0 || inclusive && c == 0)) || (upper && (c < 0 || inclusive && c == 0)) {
						want = append(want, ids[i])
					}
				}
				got, ok := schema.PropertyIndexRange("P", "id", bounds)
				require.True(t, ok)
				require.Equal(t, sorted(want), sorted(got), "bound %v (%T) upper=%v inclusive=%v", bound, bound, upper, inclusive)
			}
		}
	}

	positions := make([]int, len(values))
	for i := range positions {
		positions[i] = i
	}
	sort.Slice(positions, func(i, j int) bool { return exact(values[positions[i]]).Cmp(exact(values[positions[j]])) < 0 })
	order := make([]NodeID, len(positions))
	for i, position := range positions {
		order[i] = ids[position]
	}
	for _, descending := range []bool{false, true} {
		var visited []NodeID
		require.True(t, schema.VisitPropertyIndexGroupsInRange("P", "id", descending, PropertyIndexBounds{}, func(group []NodeID) bool {
			require.Len(t, group, 1, "every value is its own group")
			visited = append(visited, group...)
			return true
		}))
		want := append([]NodeID(nil), order...)
		if descending {
			for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
				want[i], want[j] = want[j], want[i]
			}
		}
		require.Equal(t, want, visited, "descending=%v", descending)
	}

	// An all-number index still narrows by binary search across the forms.
	idx := &PropertyIndex{values: map[interface{}][]NodeID{}, keysDirty: true}
	for _, value := range values {
		k, ok := indexValueKey(value)
		require.True(t, ok)
		idx.values[k] = []NodeID{"x"}
	}
	keys, narrowed := idx.sortedKeyRangeLocked(PropertyIndexBounds{Lower: int64(9007199254740992), HasLower: true})
	require.True(t, narrowed)
	require.Len(t, keys, 7) // 2^53+1 .. 2^53+4, MaxInt64, MaxUint64, 2^63
	for _, k := range keys {
		require.Positive(t, exact(k).Cmp(exact(int64(9007199254740992))))
	}
}
