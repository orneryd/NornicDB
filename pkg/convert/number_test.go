package convert

import (
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"testing"
)

func TestCheckedNumericBoundaries(t *testing.T) {
	for _, input := range listinputs.NumericInputs() {
		t.Run(input.Name, func(t *testing.T) {
			integer, valid := ToInt64(input.Scalar)
			require.Equal(t, input.Valid, valid)
			require.Equal(t, input.Integer, integer)
			core, valid := Int64(input.Scalar, CypherIntegerConversion)
			if input.Core == nil {
				require.False(t, valid)
			} else {
				require.True(t, valid)
				require.Equal(t, input.Core, core)
			}
			for _, source := range []interface{}{input.List, []interface{}{input.Scalar}, input.Property} {
				list, ok := ReadList(source)
				require.True(t, ok)
				require.Equal(t, 1, list.Len())
				integer, valid = ToInt64(list.At(0))
				require.Equal(t, input.Valid, valid)
				require.Equal(t, input.Integer, integer)
			}
		})
	}
	for _, width := range []int{8, 16, 32, 64} {
		maximum := int64((uint64(1) << (width - 1)) - 1)
		minimum := -maximum - 1
		for _, integer := range []int64{minimum, maximum} {
			got, ok := Int64Bits(integer, width, IntegerOnly)
			require.True(t, ok)
			require.Equal(t, integer, got)
		}
		_, ok := Int64Bits(uint64(maximum)+1, width, IntegerOnly)
		require.False(t, ok)
		_, ok = Int64Bits(float64(uint64(1)<<(width-1)), width, IntegerFloat)
		require.False(t, ok)
		maximumUnsigned := uint64(math.MaxUint64)
		if width < 64 {
			maximumUnsigned = (uint64(1) << width) - 1
		}
		number, _ := ReadNumber(maximumUnsigned)
		got, ok := number.Uint64Bits(width, IntegerOnly)
		require.True(t, ok)
		require.Equal(t, maximumUnsigned, got)
		if width < 64 {
			number, _ = ReadNumber(maximumUnsigned + 1)
			_, ok = number.Uint64Bits(width, IntegerOnly)
			require.False(t, ok)
		}
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0x1p64, -1} {
		_, ok := ToUint64(value, IntegerFloat)
		require.False(t, ok)
	}
	zero, ok := ToUint64(-0.5, IntegerFloat)
	require.True(t, ok)
	require.Zero(t, zero)
	_, ok = ToUint64(-0.5, IntegerFloat|IntegerWholeFloat)
	require.False(t, ok)
	_, ok = Int64(1.5, IntegerFloat|IntegerWholeFloat)
	require.False(t, ok)
	_, ok = Int64("1/2", IntegerStrings|IntegerDecimalStrings)
	require.False(t, ok)
	_, ok = Int64("3", IntegerOnly)
	require.False(t, ok)
	_, ok = Int64(true, IntegerOnly)
	require.False(t, ok)
	require.Equal(t, int64(3), func() int64 { value, ok := Int64(" 3.9 ", CypherIntegerConversion); require.True(t, ok); return value }())
}

func TestCheckedNumericListReaders(t *testing.T) {
	for _, input := range listinputs.Inputs() {
		list, ok := ReadList(input.Value)
		require.True(t, ok)
		require.Equal(t, 3, list.Len(), input.Name)
	}
	for _, value := range []interface{}{[]float64{1e100}, []interface{}{1e100}} {
		list, ok := ReadList(value)
		require.True(t, ok)
		_, issue := list.Numbers32()
		require.NotNil(t, issue)
		require.True(t, issue.OutOfRange)
	}
	list, ok := ReadList([]string{"3"})
	require.True(t, ok)
	_, valid := list.Numbers()
	require.False(t, valid)
	_, issue := list.Numbers32()
	require.NotNil(t, issue)
	require.False(t, issue.OutOfRange)
	numbers, valid := ToFloat64Slice([]string{"3"})
	require.True(t, valid)
	require.Equal(t, []float64{3}, numbers)
	require.Equal(t, []float32{3}, ToFloat32Slice([]interface{}{"3", false}))
	for _, value := range []interface{}{nil, 3, "3"} {
		_, ok := ReadList(value)
		require.False(t, ok)
	}
	var boxed interface{} = []interface{}{int64(1)}
	var result []interface{}
	require.Zero(t, testing.AllocsPerRun(100, func() { result, _ = ListValue(boxed) }))
	require.Equal(t, reflect.ValueOf(boxed).Pointer(), reflect.ValueOf(result).Pointer())
}
