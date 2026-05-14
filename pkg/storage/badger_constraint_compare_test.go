package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericConstraintValue(t *testing.T) {
	cases := []struct {
		val  interface{}
		want float64
		ok   bool
	}{
		{int(7), 7.0, true},
		{int8(7), 7.0, true},
		{int16(7), 7.0, true},
		{int32(7), 7.0, true},
		{int64(7), 7.0, true},
		{uint(7), 7.0, true},
		{uint8(7), 7.0, true},
		{uint16(7), 7.0, true},
		{uint32(7), 7.0, true},
		{uint64(7), 7.0, true},
		{float32(1.5), 1.5, true},
		{float64(1.5), 1.5, true},
		{"hello", 0.0, false},
		{nil, 0.0, false},
		{true, 0.0, false},
		{[]int{1, 2, 3}, 0.0, false},
	}
	for _, tc := range cases {
		got, ok := numericConstraintValue(tc.val)
		require.Equal(t, tc.ok, ok, "value=%v", tc.val)
		if ok {
			require.InDelta(t, tc.want, got, 0.0001, "value=%v", tc.val)
		}
	}
}

func TestCompareValues_NumericCrossType(t *testing.T) {
	// Numeric types compare by value, not by Go type.
	require.True(t, compareValues(int(5), int64(5)))
	require.True(t, compareValues(int(5), float64(5)))
	require.True(t, compareValues(int64(5), float64(5.0)))
	require.True(t, compareValues(float32(2.5), float64(2.5)))
	require.True(t, compareValues(uint64(1), int64(1)))

	require.False(t, compareValues(int(5), int(6)))
	require.False(t, compareValues(float64(5.5), int(5)))
}

func TestCompareValues_PrimitiveTypes(t *testing.T) {
	require.True(t, compareValues("a", "a"))
	require.False(t, compareValues("a", "b"))

	require.True(t, compareValues(true, true))
	require.False(t, compareValues(true, false))

	// Mismatched types stay as-is, no coercion.
	require.False(t, compareValues("5", 5))
	require.False(t, compareValues(true, "true"))
}

func TestCompareValues_NilAndDeepEqual(t *testing.T) {
	// Both nil → true.
	require.True(t, compareValues(nil, nil))
	// One nil → false.
	require.False(t, compareValues(nil, "x"))
	require.False(t, compareValues("x", nil))

	// Slices fall through to reflect.DeepEqual.
	require.True(t, compareValues([]int{1, 2, 3}, []int{1, 2, 3}))
	require.False(t, compareValues([]int{1, 2, 3}, []int{1, 2, 4}))

	// Maps too.
	require.True(t, compareValues(map[string]int{"a": 1}, map[string]int{"a": 1}))
}
