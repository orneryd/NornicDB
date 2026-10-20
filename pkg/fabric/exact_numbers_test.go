package fabric

import (
	"fmt"
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
		t.Run(fmt.Sprintf("%T_%v_%T_%v", pair[0], pair[0], pair[1], pair[1]), func(t *testing.T) {
			require.Equal(t, applyLookupKeyString(pair[0]), applyLookupKeyString(pair[1]))
			require.Equal(t, hashRow([]interface{}{pair[0]}), hashRow([]interface{}{pair[1]}))
			require.Equal(t, hashRow([]interface{}{[]interface{}{pair[0]}, map[string]interface{}{"v": pair[0]}}), hashRow([]interface{}{[]interface{}{pair[1]}, map[string]interface{}{"v": pair[1]}}))
			require.Len(t, deduplicateRows([][]interface{}{{pair[0]}, {pair[1]}}), 1)
			it := NewDistinctRowIterator(NewResultRowIterator(&ResultStream{Rows: [][]interface{}{{pair[0]}, {pair[1]}}}))
			defer it.Close()
			require.True(t, it.Next())
			require.False(t, it.Next())
			require.NoError(t, it.Err())
		})
	}
	require.NotEqual(t, applyLookupKeyString(int64(9007199254740992)), applyLookupKeyString(int64(9007199254740993)))
	require.Len(t, deduplicateRows([][]interface{}{{int64(9007199254740992)}, {int64(9007199254740993)}}), 2)
}

func TestFabricExactNumericPropertyLists(t *testing.T) {
	left := []int64{0, 9007199254740992}
	right := []interface{}{math.Copysign(0, -1), float64(9007199254740992)}
	require.Equal(t, applyLookupKeyString(left), applyLookupKeyString(right))
	require.Equal(t, hashRow([]interface{}{left}), hashRow([]interface{}{right}))
	require.NotEqual(t, hashRow([]interface{}{left}), hashRow([]interface{}{[]int64{0, 9007199254740993}}))
}

func TestFabricExactNumericGroupedProjection(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{{
			[]interface{}{map[string]interface{}{"textKey": "key", "textKey128": int64(9007199254740992)}},
			float64(9007199254740992), []interface{}{"matched"},
		}},
	}
	result := executeRowsGroupedJoinProjection(input)
	require.Equal(t, [][]interface{}{{"key", int64(9007199254740992), []interface{}{"matched"}}}, result.Rows)
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
