package cypher

import (
	"context"
	"math"
	"strings"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
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
	cypherfn.Register("apoc.map.removekey", apocMapRemovalFunction(false), cypherfn.CanonicalReads)
	cypherfn.Register("apoc.map.removekeys", apocMapRemovalFunction(true), cypherfn.CanonicalReads)
}

// apocMapRemovalFunction binds the key shape to the one removal owner. A
// scalar null key copies before reading config; a null list is an error.
// A missing scalar key never descends into nested maps, while a list's keys
// are removed throughout the map when recursive is set. Non-string list
// members match no map key. Key admission and recursive copying share one cancellation progress.
func apocMapRemovalFunction(multiple bool) cypherfn.Func {
	function := "apoc.map.removeKey"
	if multiple {
		function = "apoc.map.removeKeys"
	}
	return func(workCtx context.Context, ctx cypherfn.Context, args []string) (interface{}, error) {
		entries, values, err := apocMapArguments(ctx, function, args)
		if err != nil {
			return nil, err
		}
		progress, err := apocBeginCopy(workCtx)
		if err != nil {
			return nil, err
		}
		if values[1] == nil {
			if multiple {
				return nil, apocMapNullArgumentError(function, "keys")
			}
			return apocMapWithoutKeys(&progress, entries, nil, false)
		}
		var items []interface{}
		var key string
		if multiple {
			items, err = apocMapArgumentList(function, values, 1, "List<String>")
			if err != nil {
				return nil, err
			}
		} else {
			var isString bool
			key, isString = values[1].(string)
			if !isString {
				return nil, apocMapArgumentTypeError(function, "String", values[1])
			}
		}
		recursive, err := apocMapRecursive(function, values)
		if err != nil {
			return nil, err
		}
		var keys map[string]struct{}
		if multiple {
			keys = make(map[string]struct{}, len(items))
			for _, item := range items {
				if err := progress.entry(); err != nil {
					return nil, err
				}
				if key, isString := item.(string); isString {
					keys[key] = struct{}{}
				}
			}
		} else if _, present := entries[key]; present {
			keys = map[string]struct{}{key: {}}
		} else {
			recursive = false
		}
		return apocMapWithoutKeys(&progress, entries, keys, recursive)
	}
}

// apocMapArguments is the removers' reader of the (map, key or keys,
// config = {}) arguments: the shared constructor argument owner plus the map.
func apocMapArguments(ctx cypherfn.Context, function string, args []string) (map[string]interface{}, []interface{}, error) {
	values, err := apocMapFunctionArguments(function, apocMapRemovalArity, ctx, args)
	if err != nil {
		return nil, nil, err
	}
	entries, err := apocMapArgumentMap(function, values, 0)
	if err != nil {
		return nil, nil, err
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
	}
	if number, numeric := valueconvert.ReadNumber(value); numeric {
		floating := number.Float64()
		return !math.IsNaN(floating) && (floating <= -1 || floating >= 1)
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

// apocCopyCancellationStride is how many copied entries a map copy admits
// between observations of the caller's statement context. A copy of any size
// therefore ends at the caller's cancellation, while a context lookup per
// entry, which would cost more than the copy it guards, does not.
const apocCopyCancellationStride = 128

// apocCopyObserve reports the caller's statement error once a copy has
// admitted apocCopyCancellationStride entries, counting the entry it just
// made, and nil before that.
func apocCopyObserve(workCtx context.Context, copied int) error {
	if copied%apocCopyCancellationStride != 0 {
		return nil
	}
	return workCtx.Err()
}

// apocCopyProgress is one recursive copy's entry count and the caller's
// statement context it observes. Nested maps and lists share one progress, so
// the whole copy observes cancellation every apocCopyCancellationStride
// entries rather than once per nested container.
type apocCopyProgress struct {
	workCtx context.Context
	copied  int
}

// apocBeginCopy observes the caller's statement context before a copy starts.
func apocBeginCopy(workCtx context.Context) (apocCopyProgress, error) {
	if err := workCtx.Err(); err != nil {
		return apocCopyProgress{}, err
	}
	return apocCopyProgress{workCtx: workCtx}, nil
}

// entry records one copied entry and returns the caller's cancellation at
// every apocCopyCancellationStride-th entry. A copy ends with that error and
// no value instead of returning a partially built map.
func (p *apocCopyProgress) entry() error {
	p.copied++
	return apocCopyObserve(p.workCtx, p.copied)
}

// apocCopyInto copies source into out, ending at the caller's cancellation.
func apocCopyInto(workCtx context.Context, out, source map[string]interface{}) error {
	if err := workCtx.Err(); err != nil {
		return err
	}
	copied := 0
	for key, value := range source {
		copied++
		if err := apocCopyObserve(workCtx, copied); err != nil {
			return err
		}
		out[key] = value
	}
	return nil
}

// apocMapWithoutKeys copies entries without keys. With recursive set, it
// also removes keys from nested maps and from the maps in nested lists, and
// drops a nested map or list left empty (an empty list too), as APOC does.
// The top-level map is returned even when empty.
func apocMapWithoutKeys(progress *apocCopyProgress, entries map[string]interface{}, keys map[string]struct{}, recursive bool) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(entries))
	for key, value := range entries {
		if err := progress.entry(); err != nil {
			return nil, err
		}
		if _, removed := keys[key]; removed {
			continue
		}
		if recursive {
			if nested, isMap := apocMapValue(value); isMap {
				nested, err := apocMapWithoutKeys(progress, nested, keys, true)
				if err != nil {
					return nil, err
				}
				if len(nested) == 0 {
					continue
				}
				value = nested
			} else if items, isList := cypherListValue(value); isList {
				kept := make([]interface{}, 0, len(items))
				for _, item := range items {
					if err := progress.entry(); err != nil {
						return nil, err
					}
					if nested, isMap := apocMapValue(item); isMap {
						nested, err := apocMapWithoutKeys(progress, nested, keys, true)
						if err != nil {
							return nil, err
						}
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
	return out, nil
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
