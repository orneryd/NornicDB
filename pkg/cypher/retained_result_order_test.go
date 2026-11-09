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
