package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCacheKeysSeparateParameterTypes: result-cache keys differ whenever
// the parameters are different Cypher values, including values whose text
// is the same (1, 1.0, "1"; [1, 2] and "[1 2]"; true and "true"; a map and
// its text), and agree when they are the same value in different Go types
// (#729).
func TestCacheKeysSeparateParameterTypes(t *testing.T) {
	const query = "RETURN $v AS v"
	different := []interface{}{
		nil, "<nil>",
		int64(1), 1.0, "1",
		true, "true",
		[]interface{}{int64(1), int64(2)}, "[1 2]", []interface{}{"1", "2"}, []interface{}{[]interface{}{int64(1)}, int64(2)},
		map[string]interface{}{"a": int64(1)}, "map[a:1]", map[string]interface{}{"a": "1"},
		map[string]interface{}{"a": map[string]interface{}{"b": int64(1)}}, map[string]interface{}{"a": map[string]interface{}{"b": 1.0}},
		[]byte("1"), time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), "2020-01-02 03:04:05 +0000 UTC",
		uint64(1 << 63), int64(-1 << 63),
		"a\x00b", "a",
	}
	seen := make(map[string]interface{}, len(different))
	for _, value := range different {
		key := resultCacheEntryKey(query, map[string]interface{}{"v": value})
		if previous, collides := seen[key]; collides {
			t.Fatalf("%#v and %#v share the cache key %s", previous, value, key)
		}
		seen[key] = value
	}
	queryCache := NewQueryCache(10)
	require.NotEqual(t,
		queryCache.cacheKey(query, map[string]interface{}{"v": int64(1)}),
		queryCache.cacheKey(query, map[string]interface{}{"v": "1"}))

	// Parameter names are part of the key, and a value can't run into the
	// next name.
	require.NotEqual(t,
		resultCacheEntryKey(query, map[string]interface{}{"a": "b", "c": "d"}),
		resultCacheEntryKey(query, map[string]interface{}{"a": "b\x00c", "": "d"}))

	same := [][2]interface{}{
		{int64(1), int32(1)},
		{int64(1), 1},
		{int64(1), uint8(1)},
		{[]interface{}{"a", "b"}, []string{"a", "b"}},
		{[]interface{}{int64(1)}, []int64{1}},
		{map[string]interface{}{"a": "x"}, map[string]string{"a": "x"}},
		{map[string]interface{}{"a": int64(1), "b": int64(2)}, map[string]interface{}{"b": int64(2), "a": int64(1)}},
	}
	for _, pair := range same {
		require.Equal(t,
			resultCacheEntryKey(query, map[string]interface{}{"v": pair[0]}),
			resultCacheEntryKey(query, map[string]interface{}{"v": pair[1]}), "%#v and %#v", pair[0], pair[1])
	}
}

// TestNormalizeQueryKeepsQuotedText: whitespace is collapsed outside string
// literals and quoted names only (#729).
func TestNormalizeQueryKeepsQuotedText(t *testing.T) {
	for input, want := range map[string]string{
		"RETURN   'a  b'  AS  s":           "RETURN 'a  b' AS s",
		"RETURN \"a \n b\" AS s":           "RETURN \"a \n b\" AS s",
		"RETURN 1 AS `my  col`":            "RETURN 1 AS `my  col`",
		"RETURN 'it\\'s  x'   AS s":        "RETURN 'it\\'s  x' AS s",
		"MATCH (n)\n\tWHERE n.x = 'a\tb' ": "MATCH (n) WHERE n.x = 'a\tb'",
	} {
		require.Equal(t, want, normalizeQuery(input), input)
	}
	require.NotEqual(t, resultCacheEntryKey("RETURN 'a  b' AS s", nil), resultCacheEntryKey("RETURN 'a b' AS s", nil))
	require.Equal(t, resultCacheEntryKey("RETURN  'a b'  AS s", nil), resultCacheEntryKey("RETURN 'a b' AS s", nil))
}

// TestResultCacheServesEachParameterValueItsOwnResult: the statements that
// shared a cached result on main return their own results (#729).
func TestResultCacheServesEachParameterValueItsOwnResult(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:KC {id: 1, name: 'integer'}), (:KC {id: '1', name: 'string'}), (:KC {id: [1, 2], name: 'list'})", nil)
	require.NoError(t, err)

	lookup := "MATCH (n:KC) WHERE n.id = $id RETURN n.name AS name"
	for _, tc := range []struct {
		id   interface{}
		want [][]interface{}
	}{
		{int64(1), [][]interface{}{{"integer"}}},
		{"1", [][]interface{}{{"string"}}},
		{[]interface{}{int64(1), int64(2)}, [][]interface{}{{"list"}}},
		{"[1 2]", [][]interface{}{}},
	} {
		result, err := exec.Execute(ctx, lookup, map[string]interface{}{"id": tc.id})
		require.NoError(t, err)
		require.Equal(t, tc.want, result.Rows, "id %#v", tc.id)
	}

	for _, value := range []interface{}{1.0, "1", int64(1), true, "true"} {
		result, err := exec.Execute(ctx, "RETURN $v AS v", map[string]interface{}{"v": value})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{value}}, result.Rows, "v %#v", value)
	}

	for _, text := range []string{"a  b", "a b"} {
		result, err := exec.Execute(ctx, "RETURN '"+text+"' AS s", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{text}}, result.Rows)
	}
}

// TestFabricResultCacheServesOnlyAuthorizedCallers: a composite read's
// cached result isn't served to a caller the statement isn't authorized
// for; the caller gets the permission error it gets uncached (#729).
func TestFabricResultCacheServesOnlyAuthorizedCallers(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_a", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))
	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "t-1", Labels: []string{"Translation"}, Properties: map[string]interface{}{"id": "t-1"}})
	require.NoError(t, err)
	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := "USE nornic_cmp_a CALL { USE nornic_cmp_a.tr MATCH (t:Translation) RETURN t.id AS id } RETURN id"
	authorized := WithPermissionChecker(context.Background(), func(string) bool { return true })
	result, err := exec.Execute(authorized, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"t-1"}}, result.Rows)

	denied := WithPermissionChecker(context.Background(), func(string) bool { return false })
	_, err = exec.Execute(denied, query, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read permission")

	deniedOnDatabase := WithDatabasePermissionResolver(context.Background(), mgr.DefaultDatabaseName(),
		func(database, permission string) bool { return database != "nornic_tr" })
	_, err = exec.Execute(deniedOnDatabase, query, nil)
	require.Error(t, err)

	// The authorized caller still gets the cached result.
	result, err = exec.Execute(authorized, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"t-1"}}, result.Rows)
}
