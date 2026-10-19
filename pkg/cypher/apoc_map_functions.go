package cypher

import (
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// apoc.map.removeKey and apoc.map.removeKeys, as APOC 5.26 computes them
// (apoc.map.Maps): a copy of the map without the given keys, and with
// config {recursive: true} also without them in every nested map, including
// maps inside lists. A nested map or list that the recursive removal leaves
// empty is dropped. Like APOC, a null map or a null key list fails the call
// (ProcedureCallFailed). An argument of the wrong type is a type error.
func init() {
	cypherfn.Register("apoc.map.removekey", fnApocMapRemoveKey)
	cypherfn.Register("apoc.map.removekeys", fnApocMapRemoveKeys)
}

// fnApocMapRemoveKey is apoc.map.removeKey(map, key, config = {}). APOC
// returns the map unchanged when its top level has no such key, without
// looking into nested maps even with recursive set; otherwise it is
// apoc.map.removeKeys(map, [key], config). A null key is in no map.
func fnApocMapRemoveKey(ctx cypherfn.Context, args []string) (interface{}, error) {
	const function = "apoc.map.removeKey"
	entries, values, err := apocMapArguments(ctx, function, args)
	if err != nil {
		return nil, err
	}
	if values[1] == nil {
		return apocMapWithoutKeysForFunction(ctx, entries, nil, false)
	}
	key, isString := values[1].(string)
	if !isString {
		return nil, apocMapArgumentTypeError(function, "String", values[1])
	}
	recursive, err := apocMapRecursive(function, values)
	if err != nil {
		return nil, err
	}
	if _, present := entries[key]; !present {
		return apocMapWithoutKeysForFunction(ctx, entries, nil, false)
	}
	return apocMapWithoutKeysForFunction(ctx, entries, map[string]struct{}{key: {}}, recursive)
}

// fnApocMapRemoveKeys is apoc.map.removeKeys(map, keys, config = {}). A key
// in the list that isn't a string matches no map key.
func fnApocMapRemoveKeys(ctx cypherfn.Context, args []string) (interface{}, error) {
	const function = "apoc.map.removeKeys"
	entries, values, err := apocMapArguments(ctx, function, args)
	if err != nil {
		return nil, err
	}
	if values[1] == nil {
		return nil, apocMapNullArgumentError(function, "keys")
	}
	items, isList := cypherListValue(values[1])
	if !isList {
		return nil, apocMapArgumentTypeError(function, "List<String>", values[1])
	}
	recursive, err := apocMapRecursive(function, values)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, len(items))
	for _, item := range items {
		if ctx.Cancellation != nil && !ctx.Cancellation.Continue() {
			return nil, ctx.Cancellation.Err()
		}
		if key, isString := item.(string); isString {
			keys[key] = struct{}{}
		}
	}
	return apocMapWithoutKeysForFunction(ctx, entries, keys, recursive)
}

func apocMapWithoutKeysForFunction(ctx cypherfn.Context, entries map[string]interface{}, keys map[string]struct{}, recursive bool) (interface{}, error) {
	result := apocMapWithoutKeysWithCancellation(entries, keys, recursive, ctx.Cancellation)
	if ctx.Cancellation != nil && ctx.Cancellation.Err() != nil {
		return nil, ctx.Cancellation.Err()
	}
	return result, nil
}

// apocMapArguments evaluates the (map, key or keys, config = {}) arguments
// and reads the map.
func apocMapArguments(ctx cypherfn.Context, function string, args []string) (map[string]interface{}, []interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, nil, functionParameterCountError(function, len(args) > 3)
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, nil, err
	}
	if values[0] == nil {
		return nil, nil, apocMapNullArgumentError(function, "map")
	}
	entries, isMap := apocMapValue(values[0])
	if !isMap {
		return nil, nil, apocMapArgumentTypeError(function, "Map", values[0])
	}
	return entries, values, nil
}

// apocMapRecursive reads config.recursive as APOC's Util.toBoolean does; a
// missing or null config is {}.
func apocMapRecursive(function string, values []interface{}) (bool, error) {
	if len(values) < 3 || values[2] == nil {
		return false, nil
	}
	config, isMap := apocMapValue(values[2])
	if !isMap {
		return false, apocMapArgumentTypeError(function, "Map", values[2])
	}
	return apocToBoolean(config["recursive"]), nil
}

// apocToBoolean is APOC's Util.toBoolean: false for null, false, a number
// whose long value is 0 (so 0.5 too), and the strings "", "false", "no" and
// "0" in any letter case; true for everything else.
func apocToBoolean(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return !(v == "" || strings.EqualFold(v, "false") || strings.EqualFold(v, "no") || v == "0")
	case float64:
		return !math.IsNaN(v) && (v <= -1 || v >= 1)
	case float32:
		return !math.IsNaN(float64(v)) && (v <= -1 || v >= 1)
	}
	if cypherValueKindOf(value) == valueKindInteger {
		// An unsigned value past int64 isn't read, and isn't 0.
		number, read := cypherIntegerValue(value)
		return !read || number != 0
	}
	return true
}

// apocMapValue is value as a Cypher map, not a node or relationship.
func apocMapValue(value interface{}) (map[string]interface{}, bool) {
	entries, isMap := value.(map[string]interface{})
	if !isMap || cypherValueKindOf(value) != valueKindMap {
		return nil, false
	}
	return entries, true
}

// apocMapWithoutKeys copies entries without keys. With recursive set, it
// also removes keys from nested maps and from the maps in nested lists, and
// drops a nested map or list left empty (an empty list too), as APOC does.
// The top-level map is returned even when empty.
func apocMapWithoutKeys(entries map[string]interface{}, keys map[string]struct{}, recursive bool) map[string]interface{} {
	return apocMapWithoutKeysWithCancellation(entries, keys, recursive, nil)
}

func apocMapWithoutKeysWithCancellation(entries map[string]interface{}, keys map[string]struct{}, recursive bool, probe cypherfn.Cancellation) map[string]interface{} {
	out := make(map[string]interface{}, len(entries))
	for key, value := range entries {
		if probe != nil && !probe.Continue() {
			return nil
		}
		if _, removed := keys[key]; removed {
			continue
		}
		if recursive {
			if nested, isMap := apocMapValue(value); isMap {
				nested = apocMapWithoutKeysWithCancellation(nested, keys, true, probe)
				if len(nested) == 0 {
					continue
				}
				value = nested
			} else if items, isList := cypherListValue(value); isList {
				kept := make([]interface{}, 0, len(items))
				for _, item := range items {
					if probe != nil && !probe.Continue() {
						return nil
					}
					if nested, isMap := apocMapValue(item); isMap {
						nested = apocMapWithoutKeysWithCancellation(nested, keys, true, probe)
						if len(nested) == 0 {
							continue
						}
						item = nested
					}
					kept = append(kept, item)
				}
				if len(kept) == 0 {
					continue
				}
				value = kept
			}
		}
		out[key] = value
	}
	return out
}

// apocMapNullArgumentError is APOC's failure for a null map or key list
// (a NullPointerException in Neo4j): ProcedureCallFailed.
func apocMapNullArgumentError(function, argument string) error {
	return localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed",
		localization.CypherCoreFunctionArgumentNull(function, argument))
}

func apocMapArgumentTypeError(function, expected string, value interface{}) error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCoreFunctionArgumentInvalid(function, "a "+expected, neo4jValueRepr(value)))
}
