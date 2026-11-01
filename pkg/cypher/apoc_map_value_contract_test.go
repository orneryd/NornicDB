package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

// TestApocMapRemovalArgumentContracts pins what the shared map-argument owner
// and the removal copy must keep from the source they converged: the null key's
// copy runs before the config is read, a key the top level lacks copies without
// recursion, a list member that isn't a string matches no key, and the
// argument-count, type, null and config errors keep their order and text.
func TestApocMapRemovalArgumentContracts(t *testing.T) {
	entries := map[string]interface{}{
		"keep":   int64(1),
		"drop":   int64(2),
		"nested": map[string]interface{}{"drop": int64(3), "keep": int64(4)},
	}
	withoutTopLevelDrop := map[string]interface{}{
		"keep":   int64(1),
		"nested": map[string]interface{}{"drop": int64(3), "keep": int64(4)},
	}
	values := map[string]interface{}{
		"m":             entries,
		"keys":          []interface{}{"drop"},
		"mixed":         []interface{}{"drop", int64(1), nil},
		"key":           "drop",
		"absent":        "absent",
		"recursive":     map[string]interface{}{"recursive": true},
		"null":          nil,
		"not-a-map":     int64(5),
		"not-a-list":    "drop",
		"not-a-string":  int64(1),
		"unparsed-conf": "not a map",
	}
	call := func(function string, args ...string) (interface{}, error) {
		value, found, err := cypherfn.EvaluateFunction(context.Background(), function, args, neo5FunctionContext(values))
		require.True(t, found, function)
		return value, err
	}

	// A null key copies the map before the config is parsed: the config is not
	// a map, and the call still returns the map.
	value, err := call("apoc.map.removeKey", "m", "null", "unparsed-conf")
	require.NoError(t, err)
	require.Equal(t, entries, value)
	value, err = call("apoc.map.removeKey", "m", "null")
	require.NoError(t, err)
	require.Equal(t, entries, value)

	// A key the top level lacks copies without recursion, nested maps included.
	value, err = call("apoc.map.removeKey", "m", "absent", "recursive")
	require.NoError(t, err)
	require.Equal(t, entries, value)

	// A list member that isn't a string matches no map key; without recursive
	// the nested map keeps the key.
	value, err = call("apoc.map.removeKeys", "m", "mixed")
	require.NoError(t, err)
	require.Equal(t, withoutTopLevelDrop, value)
	value, err = call("apoc.map.removeKeys", "m", "keys", "recursive")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"keep": int64(1), "nested": map[string]interface{}{"keep": int64(4)}}, value)

	// Type errors name the signature's argument type.
	_, err = call("apoc.map.removeKeys", "m", "not-a-list")
	require.ErrorContains(t, err, "a List<String>")
	_, err = call("apoc.map.removeKey", "m", "not-a-string", "recursive")
	require.ErrorContains(t, err, "a String")
	_, err = call("apoc.map.removeKeys", "m", "keys", "not-a-map")
	require.ErrorContains(t, err, "a Map")
	_, err = call("apoc.map.removeKeys", "not-a-map", "keys")
	require.ErrorContains(t, err, "a Map")

	// The null boundary's failures keep their argument names.
	_, err = call("apoc.map.removeKeys", "null", "keys")
	require.ErrorContains(t, err, "`map` argument is null")
	_, err = call("apoc.map.removeKeys", "m", "null")
	require.ErrorContains(t, err, "`keys` argument is null")

	// The argument count is checked before the arguments are read.
	_, err = call("apoc.map.removeKeys", "m")
	require.ErrorContains(t, err, "Insufficient parameters")
	_, err = call("apoc.map.removeKeys", "m", "keys", "recursive", "recursive")
	require.ErrorContains(t, err, "Too many parameters")
	_, err = call("apoc.map.removeKey", "m")
	require.ErrorContains(t, err, "Insufficient parameters")
}
