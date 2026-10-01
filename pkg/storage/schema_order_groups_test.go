package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaOrderExactIntegers(t *testing.T) {
	const smaller int64 = 9007199254740992
	const larger int64 = 9007199254740993
	for _, test := range []struct {
		name        string
		left, right interface{}
		want        int
	}{
		{"adjacent ascending", smaller, larger, -1},
		{"adjacent descending", larger, smaller, 1},
		{"equal integers", larger, larger, 0},
		{"mixed signed widths", int(smaller), larger, -1},
		{"unsigned integers", uint64(smaller), uint64(larger), -1},
		{"signed unsigned", larger, uint64(smaller), 1},
		{"unsigned signed", uint64(smaller), larger, -1},
		{"negative adjacent", -larger, -smaller, -1},
		{"unsigned above signed range", ^uint64(0), int64(9223372036854775807), 1},
		{"signed below unsigned range", int64(9223372036854775807), ^uint64(0), -1},
		{"negative unsigned", int64(-1), uint64(0), -1},
		{"unsigned negative", uint64(0), int64(-1), 1},
		{"mixed float widening", larger, float64(smaller), 0},
		{"mixed float widening reverse", float64(smaller), larger, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, compareSchemaIndexValues(test.left, test.right))
		})
	}

	t.Run("raw integer keys", func(t *testing.T) {
		schema := NewSchemaManager()
		require.NoError(t, schema.AddPropertyIndex("rank", "Item", []string{"rank"}))
		index := schema.propertyIndexes["Item:rank"]
		index.values[larger] = []NodeID{"a-larger"}
		index.values[smaller] = []NodeID{"z-smaller"}
		index.keysDirty = true
		for _, descending := range []bool{false, true} {
			var groups [][]NodeID
			require.True(t, schema.VisitPropertyIndexGroups("Item", "rank", descending, func(ids []NodeID) bool {
				groups = append(groups, ids)
				return true
			}))
			want := [][]NodeID{{"z-smaller"}, {"a-larger"}}
			if descending {
				want = [][]NodeID{{"a-larger"}, {"z-smaller"}}
			}
			require.Equal(t, want, groups)
			require.Equal(t, want[0], schema.PropertyIndexTopK("Item", "rank", 1, descending))
		}
	})
}

func TestVisitPropertyIndexGroups(t *testing.T) {
	sm := NewSchemaManager()
	called := false
	require.False(t, sm.VisitPropertyIndexGroups("Item", "rank", false, func([]NodeID) bool {
		called = true
		return true
	}))
	require.False(t, called)
	require.NoError(t, sm.AddPropertyIndex("rank", "Item", []string{"rank"}))
	require.True(t, sm.VisitPropertyIndexGroups("Item", "rank", false, func([]NodeID) bool {
		called = true
		return true
	}))
	require.False(t, called)
	for _, entry := range []struct {
		id    NodeID
		value interface{}
	}{
		{"b", int64(1)}, {"a", float64(1)}, {"c", int64(2)}, {"null", nil},
	} {
		require.NoError(t, sm.PropertyIndexInsert("Item", "rank", entry.id, entry.value))
	}
	for _, descending := range []bool{false, true} {
		var groups [][]NodeID
		require.True(t, sm.VisitPropertyIndexGroups("Item", "rank", descending, func(ids []NodeID) bool {
			groups = append(groups, ids)
			// The callback can safely use the index API; no index lock is held.
			require.Equal(t, []NodeID{"c"}, sm.PropertyIndexLookup("Item", "rank", int64(2)))
			return true
		}))
		expected := [][]NodeID{{"a", "b"}, {"c"}}
		if descending {
			expected = [][]NodeID{{"c"}, {"a", "b"}}
		}
		require.Equal(t, expected, groups)
	}
	visits := 0
	require.True(t, sm.VisitPropertyIndexGroups("Item", "rank", false, func(ids []NodeID) bool {
		visits++
		require.Equal(t, []NodeID{"a", "b"}, ids)
		return false
	}))
	require.Equal(t, 1, visits)
}

func TestVisitPropertyIndexGroupsSnapshotsMembership(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("rank", "Item", []string{"rank"}))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "x", int64(1)))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "y", int64(2)))

	var visited []NodeID
	group := 0
	require.True(t, sm.VisitPropertyIndexGroups("Item", "rank", false, func(ids []NodeID) bool {
		visited = append(visited, ids...)
		if group == 0 {
			require.NoError(t, sm.PropertyIndexDelete("Item", "rank", "x", int64(1)))
			require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "x", int64(2)))
		}
		group++
		return true
	}))
	require.Equal(t, []NodeID{"x", "y"}, visited)
}
