package cypher

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResultCacheDetachedMapOrdersFollowReturnedMaps(t *testing.T) {
	for _, kind := range []string{"ordinary", "smart"} {
		t.Run(kind, func(t *testing.T) {
			var cache interface {
				Put(string, map[string]interface{}, *ExecuteResult, time.Duration)
				Get(string, map[string]interface{}) (*ExecuteResult, bool)
			}
			if kind == "ordinary" {
				cache = NewQueryCache(4)
			} else {
				cache = NewSmartQueryCache(4)
			}
			nested := map[string]interface{}{"a": int64(1), "b": int64(2)}
			outer := map[string]interface{}{"outer": []interface{}{nested}}
			pointer := func(v map[string]interface{}) uintptr { return uintptr(reflect.ValueOf(v).UnsafePointer()) }
			original := &ExecuteResult{
				Columns: []string{"value"}, Rows: [][]interface{}{{outer}},
				MapKeyOrders: map[uintptr][]string{pointer(outer): {"outer"}, pointer(nested): {"b", "a"}},
			}
			cache.Put("RETURN $value", nil, original, time.Hour)
			first, ok := cache.Get("RETURN $value", nil)
			require.True(t, ok)
			firstOuter := first.Rows[0][0].(map[string]interface{})
			firstNested := firstOuter["outer"].([]interface{})[0].(map[string]interface{})
			require.Equal(t, []string{"outer"}, first.MapKeyOrders[pointer(firstOuter)])
			require.Equal(t, []string{"b", "a"}, first.MapKeyOrders[pointer(firstNested)])
			first.MapKeyOrders[pointer(firstNested)][0] = "changed"
			firstNested["b"] = int64(99)
			second, ok := cache.Get("RETURN $value", nil)
			require.True(t, ok)
			secondOuter := second.Rows[0][0].(map[string]interface{})
			secondNested := secondOuter["outer"].([]interface{})[0].(map[string]interface{})
			require.Equal(t, int64(2), secondNested["b"])
			require.Equal(t, []string{"b", "a"}, second.MapKeyOrders[pointer(secondNested)], "map-order metadata must be detached and refer to the returned copy")
			require.Equal(t, []string{"b", "a"}, original.MapKeyOrders[pointer(nested)], "delivery must not expose the caller's original metadata either")
		})
	}
}

// Exercise the real metadata producer and cache consumer together: literal maps
// are materialized with traversal order, then every cache delivery must rebind it.
func TestExecuteCachedLiteralMapMetadataFollowsOwnedRows(t *testing.T) {
	exec := nor307PublicExecutor(t)
	query := "MATCH (n:Nor307Ownership) RETURN {node:n, nested:{b:2,a:1}} AS value"
	var previous *ExecuteResult
	for generation := range 3 {
		hits, _, _, _, _ := exec.cache.Stats()
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		if generation > 0 {
			after, _, _, _, _ := exec.cache.Stats()
			require.Equal(t, hits+1, after, "this must exercise a real cache hit")
			require.NotSame(t, previous, result)
		}
		outer := result.Rows[0][0].(map[string]interface{})
		nested := outer["nested"].(map[string]interface{})
		outerID := uintptr(reflect.ValueOf(outer).UnsafePointer())
		nestedID := uintptr(reflect.ValueOf(nested).UnsafePointer())
		require.Equal(t, []string{"node", "nested"}, result.MapKeyOrders[outerID])
		require.Equal(t, []string{"b", "a"}, result.MapKeyOrders[nestedID])
		require.Equal(t, int64(2), nested["b"])
		nested["b"] = int64(99)
		result.MapKeyOrders[nestedID][0] = "caller changed"
		previous = result
	}
}
