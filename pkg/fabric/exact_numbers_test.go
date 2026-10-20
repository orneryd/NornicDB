package fabric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFabricExactNumericIdentity(t *testing.T) {
	for _, pair := range [][2]interface{}{
		{int64(9007199254740992), float64(9007199254740992)},
		{int64(0), math.Copysign(0, -1)},
		{float64(0), math.Copysign(0, -1)},
		{int32(7), uint64(7)},
	} {
		require.Equal(t, applyLookupKeyString(pair[0]), applyLookupKeyString(pair[1]))
		require.Equal(t, hashRow([]interface{}{pair[0]}), hashRow([]interface{}{pair[1]}))
		require.Equal(t, hashRow([]interface{}{[]interface{}{pair[0]}, map[string]interface{}{"v": pair[0]}}), hashRow([]interface{}{[]interface{}{pair[1]}, map[string]interface{}{"v": pair[1]}}))
		require.Len(t, deduplicateRows([][]interface{}{{pair[0]}, {pair[1]}}), 1)
	}
	require.NotEqual(t, applyLookupKeyString(int64(9007199254740992)), applyLookupKeyString(int64(9007199254740993)))
	require.Len(t, deduplicateRows([][]interface{}{{int64(9007199254740992)}, {int64(9007199254740993)}}), 2)
}

func TestFabricExactNumericOrder(t *testing.T) {
	for _, pair := range [][2]interface{}{
		{int64(9007199254740992), int64(9007199254740993)},
		{int64(-9007199254740993), float64(-9007199254740992)},
		{int64(math.MaxInt64), float64(9223372036854775808)},
		{uint64(math.MaxUint64 - 1), uint64(math.MaxUint64)},
	} {
		require.Equal(t, -1, compareSimpleOrderValues(pair[0], pair[1]))
		require.Equal(t, 1, compareSimpleOrderValues(pair[1], pair[0]))
	}
}
