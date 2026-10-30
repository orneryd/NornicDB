// Package coll_test provides unit tests for APOC collection functions.
package coll

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected float64
	}{
		{"integers", []interface{}{1, 2, 3, 4, 5}, 15.0},
		{"floats", []interface{}{1.5, 2.5, 3.0}, 7.0},
		{"mixed", []interface{}{1, 2.5, 3}, 6.5},
		{"empty", []interface{}{}, 0.0},
		{"with non-numeric", []interface{}{1, "skip", 2, nil, 3}, 6.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Sum(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAvg(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected float64
	}{
		{"integers", []interface{}{1, 2, 3, 4, 5}, 3.0},
		{"floats", []interface{}{10.0, 20.0, 30.0}, 20.0},
		{"empty", []interface{}{}, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Avg(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected interface{}
	}{
		{"integers", []interface{}{5, 2, 8, 1, 9}, int(1)},
		{"floats", []interface{}{5.5, 2.2, 8.8}, 2.2},
		{"strings", []interface{}{"zebra", "apple", "banana"}, "apple"},
		{"empty", []interface{}{}, nil},
		{"single", []interface{}{42}, 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Min(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected interface{}
	}{
		{"integers", []interface{}{5, 2, 8, 1, 9}, int(9)},
		{"floats", []interface{}{5.5, 2.2, 8.8}, 8.8},
		{"strings", []interface{}{"zebra", "apple", "banana"}, "zebra"},
		{"empty", []interface{}{}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Max(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSort(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected []interface{}
	}{
		{"integers", []interface{}{3, 1, 4, 1, 5}, []interface{}{1, 1, 3, 4, 5}},
		{"strings", []interface{}{"c", "a", "b"}, []interface{}{"a", "b", "c"}},
		{"empty", []interface{}{}, []interface{}{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Sort(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReverse(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected []interface{}
	}{
		{"integers", []interface{}{1, 2, 3, 4, 5}, []interface{}{5, 4, 3, 2, 1}},
		{"strings", []interface{}{"a", "b", "c"}, []interface{}{"c", "b", "a"}},
		{"empty", []interface{}{}, []interface{}{}},
		{"single", []interface{}{42}, []interface{}{42}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Reverse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		list     []interface{}
		value    interface{}
		expected bool
	}{
		{"found int", []interface{}{1, 2, 3, 4, 5}, 3, true},
		{"not found int", []interface{}{1, 2, 3, 4, 5}, 6, false},
		{"found string", []interface{}{"a", "b", "c"}, "b", true},
		{"empty list", []interface{}{}, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Contains(tt.list, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsAll(t *testing.T) {
	tests := []struct {
		name     string
		list     []interface{}
		values   []interface{}
		expected bool
	}{
		{"all present", []interface{}{1, 2, 3, 4, 5}, []interface{}{2, 4}, true},
		{"not all present", []interface{}{1, 2, 3}, []interface{}{2, 6}, false},
		{"empty values", []interface{}{1, 2, 3}, []interface{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsAll(tt.list, tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		name     string
		list     []interface{}
		values   []interface{}
		expected bool
	}{
		{"some present", []interface{}{1, 2, 3}, []interface{}{3, 4, 5}, true},
		{"none present", []interface{}{1, 2, 3}, []interface{}{4, 5, 6}, false},
		{"empty values", []interface{}{1, 2, 3}, []interface{}{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsAny(tt.list, tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToSet(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected int // expected length (order preserved)
	}{
		{"with duplicates", []interface{}{1, 2, 1, 3, 2, 4}, 4},
		{"no duplicates", []interface{}{1, 2, 3}, 3},
		{"empty", []interface{}{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToSet(tt.input)
			assert.Len(t, result, tt.expected)
		})
	}
}

func TestUnion(t *testing.T) {
	list1 := []interface{}{1, 2, 3}
	list2 := []interface{}{3, 4, 5}

	result := Union(list1, list2)
	assert.Len(t, result, 5) // [1, 2, 3, 4, 5]
}

func TestUnionAll(t *testing.T) {
	list1 := []interface{}{1, 2}
	list2 := []interface{}{2, 3}

	result := UnionAll(list1, list2)
	assert.Len(t, result, 4) // [1, 2, 2, 3]
}

func TestIntersection(t *testing.T) {
	list1 := []interface{}{1, 2, 3, 4}
	list2 := []interface{}{2, 3, 4, 5}

	result := Intersection(list1, list2)
	assert.Len(t, result, 3) // [2, 3, 4]
}

func TestSubtract(t *testing.T) {
	list1 := []interface{}{1, 2, 3, 4, 5}
	list2 := []interface{}{2, 4}

	result := Subtract(list1, list2)
	assert.Len(t, result, 3) // [1, 3, 5]
}

func TestIndexOf(t *testing.T) {
	tests := []struct {
		name     string
		list     []interface{}
		value    interface{}
		expected int
	}{
		{"found", []interface{}{1, 2, 3, 4, 5}, 3, 2},
		{"not found", []interface{}{1, 2, 3}, 5, -1},
		{"first occurrence", []interface{}{1, 2, 3, 2}, 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IndexOf(tt.list, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplit(t *testing.T) {
	list := []interface{}{1, 2, 3, 4, 5, 6, 7}

	result := Split(list, 3)
	assert.Len(t, result, 3) // [[1,2,3], [4,5,6], [7]]
	assert.Len(t, result[0], 3)
	assert.Len(t, result[1], 3)
	assert.Len(t, result[2], 1)
}

func TestPairs(t *testing.T) {
	list := []interface{}{1, 2, 3, 4}

	result := Pairs(list)
	assert.Len(t, result, 3) // [[1,2], [2,3], [3,4]]
	assert.Equal(t, []interface{}{1, 2}, result[0])
	assert.Equal(t, []interface{}{2, 3}, result[1])
}

func TestZip(t *testing.T) {
	list1 := []interface{}{1, 2, 3}
	list2 := []interface{}{"a", "b", "c"}

	result := Zip(list1, list2)
	assert.Len(t, result, 3)
	assert.Equal(t, []interface{}{1, "a"}, result[0])
	assert.Equal(t, []interface{}{2, "b"}, result[1])
}

func TestFrequencies(t *testing.T) {
	list := []interface{}{1, 2, 2, 3, 3, 3}

	// As in APOC: {item, count} in first-seen order, and the same counts
	// keyed by item text; 1 and 1.0 count together.
	assert.Equal(t, []map[string]interface{}{
		{"item": 1, "count": int64(1)},
		{"item": 2, "count": int64(2)},
		{"item": 3, "count": int64(3)},
	}, Frequencies(list))
	assert.Equal(t, map[string]interface{}{"1": int64(2), "a": int64(1)}, FrequenciesAsMap([]interface{}{1, 1.0, "a"}))
	assert.Equal(t, map[string]interface{}{"3": int64(1)}, FrequenciesAsMap([]interface{}{int64(3)}))
	assert.Empty(t, Frequencies(nil))
}

func TestOccurrences(t *testing.T) {
	list := []interface{}{1, 2, 2, 3, 2, 4}

	result := Occurrences(list, 2)
	assert.Equal(t, 3, result)
}

func TestFlatten(t *testing.T) {
	tests := []struct {
		name      string
		input     []interface{}
		recursive bool
		expected  int // expected length
	}{
		{
			"simple nested",
			[]interface{}{[]interface{}{1, 2}, []interface{}{3, 4}},
			false,
			4,
		},
		{
			"deep nested recursive",
			[]interface{}{[]interface{}{1, []interface{}{2, 3}}, 4},
			true,
			4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Flatten(tt.input, tt.recursive)
			assert.Len(t, result, tt.expected)
		})
	}
}

func TestSlice(t *testing.T) {
	list := []interface{}{1, 2, 3, 4, 5}

	result := Slice(list, 1, 4)
	assert.Equal(t, []interface{}{2, 3, 4}, result)
}

func TestInsert(t *testing.T) {
	list := []interface{}{1, 2, 4, 5}

	result := Insert(list, 2, 3)
	assert.Equal(t, []interface{}{1, 2, 3, 4, 5}, result)
}

func TestRemove(t *testing.T) {
	list := []interface{}{1, 2, 3, 4, 5}

	result := Remove(list, 2)
	assert.Equal(t, []interface{}{1, 2, 4, 5}, result)
}

func TestSet(t *testing.T) {
	list := []interface{}{1, 2, 3, 4, 5}

	result := Set(list, 2, 99)
	assert.Equal(t, []interface{}{1, 2, 99, 4, 5}, result)
}

func TestIsEmpty(t *testing.T) {
	assert.True(t, IsEmpty([]interface{}{}))
	assert.False(t, IsEmpty([]interface{}{1}))
}

func TestIsNotEmpty(t *testing.T) {
	assert.False(t, IsNotEmpty([]interface{}{}))
	assert.True(t, IsNotEmpty([]interface{}{1}))
}

func TestContainsDuplicates(t *testing.T) {
	assert.True(t, ContainsDuplicates([]interface{}{1, 2, 3, 2}))
	assert.False(t, ContainsDuplicates([]interface{}{1, 2, 3, 4}))
}

func TestDuplicates(t *testing.T) {
	list := []interface{}{1, 2, 3, 2, 3, 3}

	result := Duplicates(list)
	assert.Len(t, result, 2) // [2, 3]
}

func TestDropDuplicateNeighbors(t *testing.T) {
	list := []interface{}{1, 1, 2, 2, 3, 3, 2, 2}

	result := DropDuplicateNeighbors(list)
	assert.Equal(t, []interface{}{1, 2, 3, 2}, result)
}

func TestFill(t *testing.T) {
	result := Fill("x", 5)
	assert.Len(t, result, 5)
	for _, v := range result {
		assert.Equal(t, "x", v)
	}
}

func TestSumLongs(t *testing.T) {
	list := []interface{}{1, 2, 3, 4, 5}

	result := SumLongs(list)
	assert.Equal(t, int64(15), result)
}
