package storage

import (
	"github.com/orneryd/nornicdb/internal/testfixture"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueCopyPreservesNilAndEmptyListCells(t *testing.T) {
	source := []any{nil, []any(nil), []any{}, [][]any(nil), [][]any{}}
	copied := CopyValue(source, nil).([]any)
	require.Len(t, copied, len(source))
	require.Nil(t, copied[0])
	require.True(t, copied[1] != nil, "a typed nil list must not become a nil interface")
	require.IsType(t, []any(nil), copied[1])
	require.Nil(t, copied[1].([]any))
	require.IsType(t, []any{}, copied[2])
	require.NotNil(t, copied[2].([]any))
	require.Empty(t, copied[2].([]any))
	require.True(t, copied[3] != nil, "a typed nil matrix must not become a nil interface")
	require.IsType(t, [][]any(nil), copied[3])
	require.Nil(t, copied[3].([][]any))
	require.IsType(t, [][]any{}, copied[4])
	require.NotNil(t, copied[4].([][]any))
	require.Empty(t, copied[4].([][]any))
}

func TestValueCopyRebindsMapIdentityWithoutSharingValues(t *testing.T) {
	nested := testfixture.NestedGraphProperties(1)
	source := map[string]any{"first": nested, "second": nested}
	source["self"] = source
	pointer := func(m map[string]any) uintptr { return uintptr(reflect.ValueOf(m).UnsafePointer()) }
	bindings := make(map[uintptr]uintptr)
	copied := CopyValue(source, func(from, to map[string]any) { bindings[pointer(from)] = pointer(to) }).(map[string]any)
	first := copied["first"].(map[string]any)
	second := copied["second"].(map[string]any)
	require.NotEqual(t, pointer(source), pointer(copied))
	require.Equal(t, pointer(first), pointer(second), "one copy preserves alias identity within its owned graph")
	require.Equal(t, pointer(copied), pointer(copied["self"].(map[string]any)))
	require.Equal(t, map[uintptr]uintptr{pointer(source): pointer(copied), pointer(nested): pointer(first), pointer(nested["profile"].(map[string]any)): pointer(first["profile"].(map[string]any))}, bindings)
	first["tags"].([]any)[0] = "changed"
	require.Equal(t, "alpha", nested["tags"].([]any)[0])
	var absent map[string]any
	require.Nil(t, CopyValue(absent, nil))
}

func TestSuccessiveValueCopiesKeepIndependentOverflowGraphs(t *testing.T) {
	source := make([]any, 12)
	for i := range source {
		source[i] = map[string]any{"values": []string{"original"}}
	}
	first := CopyValue(source, nil).([]any)
	second := CopyValue(source, nil).([]any)
	for _, value := range first {
		value.(map[string]any)["values"].([]string)[0] = "changed"
	}
	for _, value := range second {
		require.Equal(t, "original", value.(map[string]any)["values"].([]string)[0])
	}
	for _, value := range source {
		require.Equal(t, "original", value.(map[string]any)["values"].([]string)[0])
	}
}
