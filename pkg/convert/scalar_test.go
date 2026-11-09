package convert

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestNativeScalarKeepsExactGoTypePopulation(t *testing.T) {
	type namedInt int
	type namedString string
	for _, value := range []interface{}{nil, "", true, false, int(0), int8(0), int16(0), int32(0), int64(0), uint(0), uint8(0), uint16(0), uint32(0), uint64(0), float32(0), float64(0), math.NaN(), math.Inf(1)} {
		require.True(t, NativeScalar(value), "%T must retain its primitive classification", value)
	}
	var pointer *int
	var function func()
	for _, value := range []interface{}{namedInt(1), namedString("x"), pointer, function, uintptr(1), complex64(1), complex128(1), []byte{}, []interface{}{}, map[string]interface{}{}, struct{}{}} {
		require.False(t, NativeScalar(value), "%T must retain its non-native classification", value)
	}
}
