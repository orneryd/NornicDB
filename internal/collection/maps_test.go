package collection

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapViewsRetainOrderingAndIndependentOwnership(t *testing.T) {
	require.NotNil(t, SortedKeys[int](nil))
	require.Equal(t, []string{"a", "z"}, SortedKeys(map[string]int{"z": 2, "a": 1}))
	original := map[string][]int{"values": {1, 2}, "nil": nil, "empty": {}}
	snapshot := CloneSliceValues(original)
	require.NotNil(t, CloneSliceValues[string, int](nil))
	require.Nil(t, snapshot["nil"])
	require.Nil(t, snapshot["empty"])
	snapshot["values"][0] = 9
	snapshot["new"] = []int{3}
	require.Equal(t, []int{1, 2}, original["values"])
	require.NotContains(t, original, "new")
	original["values"][1] = 7
	require.Equal(t, []int{9, 2}, snapshot["values"])
}
