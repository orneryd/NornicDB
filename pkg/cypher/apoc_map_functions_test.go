package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.map.removeKeys and apoc.map.removeKey return what APOC 5.26's
// apoc.map.Maps returns, from literals, variables, parameters and node
// properties.
func TestApocMapRemoveKeysValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_map_remove_keys"))
	ctx := context.Background()
	nested := "{a: 1, b: {a: 2, c: 3}, d: [{a: 4}, {a: 5, e: 6}, 7], f: {a: 9}, g: [], h: [{a: 1}]}"
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		// Plain removal; keys the map doesn't have change nothing.
		{"RETURN apoc.map.removeKeys({a: 1, b: 2, c: 3}, ['b', 'c'])", map[string]interface{}{"a": int64(1)}},
		{"RETURN apoc.map.removeKeys({a: 1, b: 2}, ['x', 'y'])", map[string]interface{}{"a": int64(1), "b": int64(2)}},
		{"RETURN apoc.map.removeKeys({a: 1}, [])", map[string]interface{}{"a": int64(1)}},
		{"RETURN apoc.map.removeKeys({a: 1}, ['a'])", map[string]interface{}{}},
		{"RETURN apoc.map.removeKeys({}, ['a'])", map[string]interface{}{}},
		// A key that isn't a string matches no key.
		{"RETURN apoc.map.removeKeys({a: 1, b: 2}, ['b', 1, null])", map[string]interface{}{"a": int64(1)}},
		// Without recursive, nested maps keep the key.
		{"RETURN apoc.map.removeKeys(" + nested + ", ['a'])", map[string]interface{}{
			"b": map[string]interface{}{"a": int64(2), "c": int64(3)},
			"d": []interface{}{map[string]interface{}{"a": int64(4)}, map[string]interface{}{"a": int64(5), "e": int64(6)}, int64(7)},
			"f": map[string]interface{}{"a": int64(9)}, "g": []interface{}{}, "h": []interface{}{map[string]interface{}{"a": int64(1)}},
		}},
		{"RETURN apoc.map.removeKeys(" + nested + ", ['a'], {})", map[string]interface{}{
			"b": map[string]interface{}{"a": int64(2), "c": int64(3)},
			"d": []interface{}{map[string]interface{}{"a": int64(4)}, map[string]interface{}{"a": int64(5), "e": int64(6)}, int64(7)},
			"f": map[string]interface{}{"a": int64(9)}, "g": []interface{}{}, "h": []interface{}{map[string]interface{}{"a": int64(1)}},
		}},
		// Recursive: nested maps and maps in lists lose the key, and a nested
		// map or list left empty is dropped, an empty list included.
		{"RETURN apoc.map.removeKeys(" + nested + ", ['a'], {recursive: true})", map[string]interface{}{
			"b": map[string]interface{}{"c": int64(3)},
			"d": []interface{}{map[string]interface{}{"e": int64(6)}, int64(7)},
		}},
		{"RETURN apoc.map.removeKeys({x: {y: {a: 1, z: 2}}}, ['a'], {recursive: true})", map[string]interface{}{"x": map[string]interface{}{"y": map[string]interface{}{"z": int64(2)}}}},
		{"RETURN apoc.map.removeKeys({x: [[{a: 1}]]}, ['a'], {recursive: true})", map[string]interface{}{"x": []interface{}{[]interface{}{map[string]interface{}{"a": int64(1)}}}}},
		// recursive is read as APOC's Util.toBoolean reads it.
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], {recursive: 'true'})", map[string]interface{}{"b": map[string]interface{}{"c": int64(2)}}},
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], {recursive: 1})", map[string]interface{}{"b": map[string]interface{}{"c": int64(2)}}},
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], {recursive: 'No'})", map[string]interface{}{"b": map[string]interface{}{"a": int64(1), "c": int64(2)}}},
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], {recursive: 0.5})", map[string]interface{}{"b": map[string]interface{}{"a": int64(1), "c": int64(2)}}},
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], {recursive: null})", map[string]interface{}{"b": map[string]interface{}{"a": int64(1), "c": int64(2)}}},
		{"RETURN apoc.map.removeKeys({b: {a: 1, c: 2}}, ['a'], null)", map[string]interface{}{"b": map[string]interface{}{"a": int64(1), "c": int64(2)}}},
		// Variables and the function name in any letter case.
		{"WITH {a: 1, b: 2} AS m, ['a'] AS ks RETURN apoc.map.removeKeys(m, ks)", map[string]interface{}{"b": int64(2)}},
		{"UNWIND [{a: 1, b: 2}] AS m RETURN APOC.MAP.REMOVEKEYS(m, ['b'])", map[string]interface{}{"a": int64(1)}},
		// removeKey: removeKeys with one key, but a key the top level lacks
		// returns the map unchanged, nested maps included.
		{"RETURN apoc.map.removeKey({a: 1, b: 2}, 'b')", map[string]interface{}{"a": int64(1)}},
		{"RETURN apoc.map.removeKey({a: 1, b: {a: 2, c: 3}}, 'a', {recursive: true})", map[string]interface{}{"b": map[string]interface{}{"c": int64(3)}}},
		{"RETURN apoc.map.removeKey({b: {a: 2, c: 3}}, 'a', {recursive: true})", map[string]interface{}{"b": map[string]interface{}{"a": int64(2), "c": int64(3)}}},
		{"RETURN apoc.map.removeKey({a: 1}, null)", map[string]interface{}{"a": int64(1)}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, tc.want, result.Rows[0][0])
		})
	}

	result, err := exec.Execute(ctx, "RETURN apoc.map.removeKeys($m, $keys, $config) AS m", map[string]interface{}{
		"m":      map[string]interface{}{"a": int64(1), "b": map[string]interface{}{"a": int64(2), "c": int64(3)}},
		"keys":   []interface{}{"a"},
		"config": map[string]interface{}{"recursive": true},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"b": map[string]interface{}{"c": int64(3)}}, result.Rows[0][0])
}

// ON CREATE SET n = apoc.map.removeKeys($map, [...]) stores the map without
// the keys, and node properties are a map removeKeys reads.
func TestApocMapRemoveKeysInSet(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_map_remove_keys_set"))
	ctx := context.Background()
	params := map[string]interface{}{
		"id":       "k1",
		"identity": map[string]interface{}{"id": "k1", "name": "n", "scratch": "x", "raw": "y"},
	}
	query := "MERGE (i:RemoveKeysIdentity {id: $id}) ON CREATE SET i = apoc.map.removeKeys($identity, ['scratch', 'raw']) RETURN i.id, i.name, i.scratch, i.raw"
	for range 2 {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"k1", "n", nil, nil}}, result.Rows)
	}

	result, err := exec.Execute(ctx, "MATCH (i:RemoveKeysIdentity) RETURN count(i), apoc.map.removeKeys(properties(i), ['id'])", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), map[string]interface{}{"name": "n"}}}, result.Rows)
}

// A null map or key list fails the call, as APOC's NullPointerException
// does; an argument of the wrong type is a type error, and a wrong argument
// count Neo4j's parameter-count error.
func TestApocMapRemoveKeysErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_map_remove_keys_errors"))
	ctx := context.Background()
	for _, tc := range []struct {
		query string
		code  string
	}{
		{"RETURN apoc.map.removeKeys(null, ['a'])", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.map.removeKeys({a: 1}, null)", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.map.removeKeys(null, null, {recursive: true})", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.map.removeKey(null, 'a')", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"WITH null AS m RETURN apoc.map.removeKeys(m, ['a'])", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.map.removeKeys({a: 1}, 'a')", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKeys({a: 1}, {a: 1})", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKeys('a', ['a'])", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKeys([1], ['a'])", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKeys({a: 1}, ['a'], 'recursive')", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKey({a: 1}, 1)", "Neo.ClientError.Statement.TypeError"},
		{"RETURN apoc.map.removeKeys({a: 1})", "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN apoc.map.removeKeys({a: 1}, ['a'], {}, {})", "Neo.ClientError.Statement.SyntaxError"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			_, err := exec.Execute(ctx, tc.query, nil)
			requireStatusCode(t, err, tc.code)
		})
	}

	_, err := exec.Execute(ctx, "RETURN apoc.map.removeKeys(null, ['a'])", nil)
	require.ErrorContains(t, err, "apoc.map.removeKeys")
	require.ErrorContains(t, err, "`map` argument is null")
	_, err = exec.Execute(ctx, "RETURN apoc.map.removeKeys({}, null)", nil)
	require.ErrorContains(t, err, "`keys` argument is null")
}

// SHOW FUNCTIONS lists apoc.map.removeKeys with APOC 5.26's row.
func TestShowFunctionsListsApocMapRemoveKeys(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_map_remove_keys_show"))
	result, err := exec.Execute(context.Background(), "SHOW FUNCTIONS YIELD name, description, signature, isBuiltIn, argumentDescription WHERE name = 'apoc.map.removeKeys' RETURN name, description, signature, isBuiltIn, [a IN argumentDescription | a.description]", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		"apoc.map.removeKeys",
		"Removes the given keys from the `MAP` (recursively if recursive is true).",
		"apoc.map.removeKeys(map :: MAP, keys :: LIST<STRING>, config = {} :: MAP) :: MAP",
		false,
		[]interface{}{"The map to be updated.", "The keys to remove from the map.", "{ recursive = false :: BOOLEAN }"},
	}}, result.Rows)
}
