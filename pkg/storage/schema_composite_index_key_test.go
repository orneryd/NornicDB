package storage

// Lists and maps are filed in property indexes and unique constraints under a
// canonical key, so an indexed equality on them finds the node (#844).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexValueKeyListsAndMaps(t *testing.T) {
	key := func(value interface{}) interface{} {
		t.Helper()
		k, ok, keyErr := indexValueKey(t.Context(), value)
		require.NoError(t, keyErr)
		require.True(t, ok, "%#v", value)
		return k
	}
	require.Equal(t, key([]interface{}{int64(1), int64(2)}), key([]interface{}{1.0, 2.0}))
	require.Equal(t, key([]interface{}{int64(1), int64(2)}), key([]int64{1, 2}))
	require.NotEqual(t, key([]interface{}{int64(1), int64(2)}), key([]interface{}{int64(2), int64(1)}))
	require.NotEqual(t, key([]interface{}{"1"}), key([]interface{}{int64(1)}))
	require.NotEqual(t, key([]interface{}{true}), key([]interface{}{"true"}))
	require.NotEqual(t, key([]interface{}{false}), key([]interface{}{true}))
	require.NotEqual(t, key([]interface{}{"a,b"}), key([]interface{}{"a", "b"}))
	require.NotEqual(t, key([]interface{}{nil}), key([]interface{}{}))
	require.Equal(t, key([]interface{}{[]interface{}{"a"}, nil}), key([]interface{}{[]string{"a"}, nil}))
	require.Equal(t,
		key(map[string]interface{}{"a": int64(1), "b": []interface{}{"x"}}),
		key(map[string]interface{}{"b": []interface{}{"x"}, "a": 1.0}))
	require.NotEqual(t, key(map[string]interface{}{"a": int64(1)}), key(map[string]interface{}{"b": int64(1)}))
	// A list key never equals a string key with the same text.
	listKey := key([]interface{}{"x"})
	require.NotEqual(t, listKey, key(string(listKey.(compositeIndexKey))))

	for _, value := range []interface{}{nil, []byte("ab"), []interface{}{struct{ s []int }{}}, map[string]interface{}{"a": []byte("x")}} {
		_, ok, keyErr := indexValueKey(t.Context(), value)
		require.NoError(t, keyErr)
		require.False(t, ok, "%#v", value)
	}
}

func TestPropertyIndexFilesListsForEqualityOnly(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("idx", "P", []string{"tags"}))
	require.NoError(t, sm.PropertyIndexInsert("P", "tags", "n1", []interface{}{int64(1), int64(2)}))
	require.NoError(t, sm.PropertyIndexInsert("P", "tags", "n2", int64(5)))
	require.NoError(t, sm.PropertyIndexInsert("P", "tags", "n3", []interface{}{"a"}))

	require.Equal(t, []NodeID{"n1"}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "P", "tags", []interface{}{1.0, 2.0})))
	require.Equal(t, []NodeID{"n3"}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "P", "tags", []string{"a"})))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "P", "tags", []interface{}{int64(2), int64(1)})))
	require.Equal(t, []NodeID{"n1"}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookupAnyLabel(t.Context(), "tags", []interface{}{int64(1), int64(2)})))
	// Ordered reads cannot silently omit list memberships. Unordered nonnull
	// candidates retain every member; the query owner sorts their actual values.
	ids, err := sm.PropertyIndexAllNonNil(t.Context(), "P", "tags", false)
	require.ErrorIs(t, err, ErrPropertyIndexValueOrderUnavailable)
	require.Nil(t, ids)
	ids, found, err := sm.PropertyIndexNonNilCandidates(t.Context(), "P", "tags")
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, []NodeID{"n1", "n2", "n3"}, ids)

	require.NoError(t, sm.PropertyIndexDelete("P", "tags", "n1", []interface{}{int64(1), int64(2)}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "P", "tags", []interface{}{int64(1), int64(2)})))
}
