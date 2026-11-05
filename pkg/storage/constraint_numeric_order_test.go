package storage

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// One exact numeric owner orders and equates constraint numbers: integers
// above 2^53 never collapse through float64, against each other or floats.
func TestConstraintNumbersOrderExactly(t *testing.T) {
	even, odd := int64(9007199254740992), int64(9007199254740993)
	for _, tc := range []struct {
		name    string
		a, b    interface{}
		order   int
		ordered bool
	}{
		{"adjacent int64", odd, even, 1, true},
		{"adjacent int64 reversed", even, odd, -1, true},
		{"int64 above float", odd, float64(even), 1, true},
		{"float below int64", float64(even), odd, -1, true},
		{"int64 equals float", even, float64(even), 0, true},
		{"negative adjacent", -odd, -even, -1, true},
		{"negative int64 below float", -odd, float64(-even), -1, true},
		{"fraction above integer", int64(2), 2.5, -1, true},
		{"negative fraction", int64(-2), -2.5, 1, true},
		{"uint64 max above int64 max", uint64(math.MaxUint64), int64(math.MaxInt64), 1, true},
		{"uint64 max below 2^64 float", uint64(math.MaxUint64), float64(math.MaxUint64), -1, true},
		{"int64 max below 2^63 float", int64(math.MaxInt64), float64(math.MaxInt64), -1, true},
		{"int64 min equals -2^63 float", int64(math.MinInt64), -0x1p63, 0, true},
		{"negative signed below unsigned", int64(-1), uint64(0), -1, true},
		{"infinity above integers", math.Inf(1), int64(math.MaxInt64), 1, true},
		{"negative infinity below unsigned", math.Inf(-1), uint64(0), -1, true},
		{"zero signs equal", math.Copysign(0, -1), 0.0, 0, true},
		{"small mixed", int32(7), float32(7), 0, true},
		{"NaN unordered", math.NaN(), int64(1), 0, false},
		{"NaN self unordered", math.NaN(), math.NaN(), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			order, numeric, ordered := compareConstraintNumbers(reflect.ValueOf(tc.a), reflect.ValueOf(tc.b))
			require.True(t, numeric)
			require.Equal(t, tc.ordered, ordered)
			if tc.ordered {
				require.Equal(t, tc.order, order)
			}
			require.Equal(t, tc.ordered && tc.order == 0, compareValues(tc.a, tc.b))
			require.Equal(t, tc.ordered && tc.order > 0, compareGreaterConstraint(tc.a, tc.b))
			require.Equal(t, tc.ordered && tc.order < 0, compareLessConstraint(tc.a, tc.b))
			require.Equal(t, tc.ordered && tc.order >= 0, compareConstraintExpressionValue(tc.a, ">=", tc.b))
			require.Equal(t, tc.ordered && tc.order <= 0, compareConstraintExpressionValue(tc.a, "<=", tc.b))
		})
	}
	_, numeric, _ := compareConstraintNumbers(reflect.ValueOf("1"), reflect.ValueOf(int64(1)))
	require.False(t, numeric)
	// Non-numeric operands keep their existing parsed / printed ordering.
	require.True(t, compareGreaterConstraint("10", "9"))
	require.True(t, compareConstraintExpressionValue("abc", ">=", "abc"))
}
