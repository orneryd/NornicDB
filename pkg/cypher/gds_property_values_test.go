package cypher

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGDSPropertyValueConversion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   interface{}
		target  string
		widen   bool
		want    interface{}
		kind    string
		ok      bool
		message string
	}{
		{name: "null"},
		{name: "text", value: "1"},
		{name: "boolean", value: true},
		{name: "unsupported list element", value: []interface{}{nil}},
		{name: "integer", value: int64(9007199254740993), want: int64(9007199254740993), kind: "Integer", ok: true},
		{name: "double", value: 1.5, want: 1.5, kind: "Float", ok: true},
		{name: "integer list", value: []interface{}{int8(1), int64(9007199254740993)}, want: []int64{1, 9007199254740993}, kind: "List of Integer", ok: true},
		{name: "mixed numeric list", value: []interface{}{int64(2), 3.5}, want: []float64{2, 3.5}, kind: "List of Float", ok: true},
		{name: "numeric list", value: []interface{}{2.5, 3.5}, want: []float64{2.5, 3.5}, kind: "List of Float", ok: true},
		{name: "empty integer array", value: []int64{}, want: []int64{}, kind: "List of Integer", ok: true},
		{name: "empty double array", value: []float64{}, want: []float64{}, kind: "List of Float", ok: true},
		{name: "untyped empty extension", value: []interface{}{}, want: []int64{}, kind: "List of Integer", ok: true},
		{name: "string array", value: []interface{}{"a"}, message: "Loading of values of type StringArray is currently not supported"},
		{name: "aggregation relationship exact long", value: int64(9007199254740992), target: "Relationship", want: float64(9007199254740992), kind: "Float", ok: true},
		{name: "aggregation relationship rounded long", value: int64(9007199254740993), target: "Relationship", message: "Cannot safely convert 9007199254740993 into an double value"},
		{name: "native relationship rounded long", value: int64(9007199254740993), target: "Relationship", widen: true, message: "Cannot safely convert 9007199254740993 into an double value"},
		{name: "long to double", value: int64(5), target: "Float", want: 5.0, kind: "Float", ok: true},
		{name: "lossless double to long", value: 5.0, target: "Integer", want: int64(5), kind: "Integer", ok: true},
		{name: "native scalar widening", value: 5.5, target: "Integer", widen: true, want: 5.5, kind: "Float", ok: true},
		{name: "fraction scalar", value: 5.5, target: "Integer", message: "Cannot safely convert 5.50 into an long value"},
		{name: "lossless double array", value: []float64{3, 4}, target: "List of Integer", want: []int64{3, 4}, kind: "List of Integer", ok: true},
		{name: "long array to double", value: []int64{2}, target: "List of Float", want: []float64{2}, kind: "List of Float", ok: true},
		{name: "fraction array", value: []float64{3, 4.5}, target: "List of Integer", message: "Cannot safely convert DoubleArray[3.0, 4.5] into a Long Array. Cannot safely convert 4.50 into an long value"},
		{name: "scalar to array", value: int64(2), target: "List of Float", message: "Cannot safely convert 2 into a Double Array."},
		{name: "array to scalar", value: []int64{2}, target: "Float", message: "Cannot safely convert LongArray[2] into a Double."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exact, kind, ok, err := gdsConvertPropertyValue(tc.value, tc.target, tc.widen)
			if tc.message != "" {
				require.EqualError(t, err, tc.message)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.want, exact)
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0x1p63, -0x1p63 - 2048, 1.5} {
		_, _, _, err := gdsConvertPropertyValue([]float64{value}, "List of Integer", false)
		require.ErrorContains(t, err, "Cannot safely convert")
	}
	for _, value := range []float64{-0x1p63, 0x1p63 - 1024, 0, 42} {
		result, _, _, err := gdsConvertPropertyValue([]float64{value}, "List of Integer", false)
		require.NoError(t, err)
		require.Equal(t, []int64{int64(value)}, result)
	}
}

func TestGDSPropertyValueMissingArrays(t *testing.T) {
	proj := &GraphProjection{NodePropertyTypes: map[string]string{"longs": "List of Integer", "doubles": "List of Float"}}
	value, err := proj.nodePropertyValue("missing", "longs", "")
	require.NoError(t, err)
	require.Equal(t, []int64{}, value)
	value, err = proj.nodePropertyValue("missing", "doubles", "")
	require.NoError(t, err)
	require.Equal(t, []float64{}, value)
}
