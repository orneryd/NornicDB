package cypher

// Author: Codex NOR-184 lane: immutable callable metadata preserves identity checks.

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

func apocMapNullResult(name string, args []interface{}) (interface{}, bool, error) {
	// Coll.frequenciesAsMap and Date.fields explicitly construct {} for a
	// null primary input in APOC 5.26.30. These are intentional exceptions.
	if (name == "apoc.coll.frequenciesAsMap" || name == "apoc.date.fields") && len(args) > 0 && apocNullValue(args[0]) {
		return map[string]interface{}{}, true, nil
	}
	if null, err := apocMapNullInput(name, args); null || err != nil {
		return nil, true, err
	}
	return nil, false, nil
}

// apocMapNullInput owns the null boundary for map-returning APOC functions.
// APOC Core 5.26.30 Maps.java / Util.java distinguish failures, null results,
// and merge/setter constructors accepting a null base map. A null result
// must be an untyped nil: a typed nil Go map is otherwise a Cypher empty map.
// NornicDB-only map functions propagate a null primary input.
func apocMapNullInput(name string, args []interface{}) (bool, error) {
	function := name
	name = strings.ToLower(name)
	required := []int(nil)
	switch name {
	case "apoc.map.merge", "apoc.map.setkey", "apoc.map.setentry":
		return false, nil
	case "apoc.map.setpairs", "apoc.map.setvalues":
		required = []int{1}
	case "apoc.map.setlists":
		required = []int{1, 2}
	case "apoc.map.fromlists":
		required = []int{0, 1}
	case "apoc.map.clean":
		required = []int{0, 1, 2}
	case "apoc.map.removekeys":
		required = []int{0, 1}
	case "apoc.map.updatetree":
		required = []int{0, 2}
	case "apoc.map.frompairs", "apoc.map.fromvalues", "apoc.map.mergelist", "apoc.map.get",
		"apoc.map.removekey", "apoc.map.flatten", "apoc.map.unflatten",
		"apoc.map.groupby", "apoc.map.groupbymulti":
		required = []int{0}
	case "apoc.map.submap":
		if len(args) >= 2 && (apocNullValue(args[0]) || apocNullValue(args[1])) {
			return true, nil
		}
		return false, nil
	default:
		return len(args) > 0 && apocNullValue(args[0]), nil
	}
	for _, i := range required {
		if i < len(args) && apocNullValue(args[i]) {
			argument := fmt.Sprintf("argument %d", i+1)
			if name == "apoc.map.removekey" || name == "apoc.map.removekeys" || name == "apoc.map.get" {
				argument = "map"
				if i == 1 {
					argument = "keys"
				}
			}
			return false, apocMapNullArgumentError(function, argument)
		}
	}
	// Java iterates/dereferences each pair or map; a null member is not an
	// empty entry. SetPairs and FromPairs share exactly this decision.
	listAt := -1
	switch name {
	case "apoc.map.frompairs", "apoc.map.mergelist":
		listAt = 0
	case "apoc.map.setpairs":
		listAt = 1
	case "apoc.map.updatetree":
		listAt = 2
	}
	if listAt >= 0 && listAt < len(args) {
		if items, ok := cypherListValue(args[listAt]); ok {
			for _, item := range items {
				if apocNullValue(item) {
					return false, apocMapNullArgumentError(function, "list entry")
				}
			}
		}
	}
	return false, nil
}

func apocNullValue(value interface{}) bool {
	return cypherfn.RuntimeValue(value) == nil
}

// bundledAPOCMapHandler identifies the actual bundled implementation, so
// third-party handlers retain their own contract and override precedence.
func bundledAPOCMapHandler(name string, handler *PluginFunctionHandler, argumentCount int) bool {
	typ := handler.Signature()
	if typ == nil {
		return false
	}
	if typ.NumOut() == 0 || (typ.Out(0).Kind() != reflect.Map && !(name == "apoc.map.get" && typ.Out(0).Kind() == reflect.Interface)) {
		return false
	}
	count := typ.NumIn()
	if count > 0 && typ.In(0) == pluginContextType {
		count--
	}
	if (!typ.IsVariadic() && argumentCount != count) || (typ.IsVariadic() && argumentCount < count-1) {
		return false
	}
	// Required Go scalar arguments already reject null. Preserve that error;
	// the date.fields exception above deliberately accepts a null primary.
	if count > 0 && name != "apoc.date.fields" && name != "apoc.convert.fromJsonMap" {
		at := typ.NumIn() - count
		switch typ.In(at).Kind() {
		case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface:
		default:
			return false
		}
	}
	return strings.HasPrefix(handler.Symbol(), "github.com/orneryd/nornicdb/apoc/")
}

func init() {
	for _, name := range []string{"apoc.map.frompairs", "apoc.map.fromlists", "apoc.map.merge", "apoc.map.setkey", "apoc.map.clean"} {
		cypherfn.Register(name, apocMapConstructor(name))
	}
}

// apocMapArity is a map constructor's or remover's accepted argument count.
type apocMapArity struct{ min, max int }

// contains reports whether count arguments fit the arity.
func (a apocMapArity) contains(count int) bool { return count >= a.min && count <= a.max }

// apocMapArityOf is the one owner of the map constructors' and removers'
// argument counts. clean's one- and two-argument forms are the release's
// shorthand, which reads its missing lists as [].
func apocMapArityOf(function string) apocMapArity {
	switch {
	case strings.EqualFold(function, "apoc.map.fromlists"), strings.EqualFold(function, "apoc.map.merge"):
		return apocMapArity{min: 2, max: 2}
	case strings.EqualFold(function, "apoc.map.setkey"):
		return apocMapArity{min: 3, max: 3}
	case strings.EqualFold(function, "apoc.map.clean"):
		return apocMapArity{min: 1, max: 3}
	case strings.EqualFold(function, "apoc.map.removekey"), strings.EqualFold(function, "apoc.map.removekeys"):
		return apocMapArity{min: 2, max: 3}
	default:
		return apocMapArity{min: 1, max: 1}
	}
}

// apocMapRemovalArity is apoc.map.removeKey's and apoc.map.removeKeys'
// argument count, read from the shared table once.
var apocMapRemovalArity = apocMapArityOf("apoc.map.removekey")

// apocMapFunctionArguments owns the argument work the map constructors and
// removers share: the function's argument count, one evaluation of the
// arguments, clean's omitted-list defaults and the null boundary. Its order
// preserves the functions' existing error precedence, and its caller supplies
// the arity the shared table holds, so a statement does not repeat it.
func apocMapFunctionArguments(function string, arity apocMapArity, ctx cypherfn.Context, args []string) ([]interface{}, error) {
	if !arity.contains(len(args)) {
		return nil, functionParameterCountError(function, len(args) > arity.max)
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	// clean's omitted lists default to [], whereas an explicit null fails.
	if strings.EqualFold(function, "apoc.map.clean") {
		for len(values) < 3 {
			values = append(values, []interface{}{})
		}
	}
	if null, err := apocMapNullInput(function, values); null || err != nil {
		return nil, err
	}
	return values, nil
}

// apocMapArgumentMap reads argument index as a Cypher map, not a node or
// relationship. A null argument is nil, not a type error: apoc.map.merge and
// apoc.map.setKey accept a null base map, and the null boundary has already
// rejected a null where the function fails.
func apocMapArgumentMap(function string, values []interface{}, index int) (map[string]interface{}, error) {
	if apocNullValue(values[index]) {
		return nil, nil
	}
	entries, isMap := apocMapValue(values[index])
	if !isMap {
		return nil, apocMapArgumentTypeError(function, "Map", values[index])
	}
	return entries, nil
}

// apocMapArgumentList reads argument index as a Cypher list.
func apocMapArgumentList(function string, values []interface{}, index int, expected string) ([]interface{}, error) {
	items, isList := cypherListValue(values[index])
	if !isList {
		return nil, apocMapArgumentTypeError(function, expected, values[index])
	}
	return items, nil
}

// apocMapConstructor owns the map-returning constructors' results. Their
// argument count, evaluation and null boundary are apocMapFunctionArguments,
// and their copies observe the caller's statement cancellation.
func apocMapConstructor(name string) cypherfn.Func {
	arity := apocMapArityOf(name)
	return func(workCtx context.Context, ctx cypherfn.Context, args []string) (interface{}, error) {
		// Preserve the native merge shorthand's existing result for a missing
		// or extra operand. Null operands are the null boundary's.
		if name == "apoc.map.merge" && !arity.contains(len(args)) {
			return map[string]interface{}{}, nil
		}
		values, err := apocMapFunctionArguments(name, arity, ctx, args)
		if err != nil {
			return nil, err
		}
		out := map[string]interface{}{}
		switch name {
		case "apoc.map.frompairs":
			// A value whose type becomes known only in the row (including a
			// WITH-bound parameter) retains the signature's SyntaxError.
			if isAllStringList(values[0]) {
				return nil, typeNameMismatchError("List<List<T>>", "List<String>")
			}
			pairs, err := apocMapArgumentList(name, values, 0, "List")
			if err != nil {
				return out, nil // retain the native non-list shorthand
			}
			if err := workCtx.Err(); err != nil {
				return nil, err
			}
			for index, pair := range pairs {
				if err := apocCopyObserve(workCtx, index+1); err != nil {
					return nil, err
				}
				items, ok := cypherListValue(pair)
				if !ok {
					// Neo4j's ANY-to-list argument coercion wraps a map as a
					// singleton pair. Scalar values cannot be coerced to a list.
					if value, mapped := apocMapValue(pair); mapped {
						items = []interface{}{value}
					} else {
						return nil, apocMapArgumentTypeError(name, "List", pair)
					}
				}
				if len(items) == 0 || items[0] == nil {
					continue
				}
				var value interface{}
				if len(items) > 1 {
					value = items[1]
				}
				out[apocObjectString(items[0])] = value
			}
		case "apoc.map.fromlists":
			keys, err := apocMapArgumentList(name, values, 0, "List")
			if err != nil {
				return nil, err
			}
			items, err := apocMapArgumentList(name, values, 1, "List")
			if err != nil {
				return nil, err
			}
			if len(keys) != len(items) {
				return nil, nativeAPOCCallFailure(name, values)
			}
			if err := workCtx.Err(); err != nil {
				return nil, err
			}
			for index, key := range keys {
				if err := apocCopyObserve(workCtx, index+1); err != nil {
					return nil, err
				}
				out[fmt.Sprint(key)] = items[index]
			}
		case "apoc.map.merge":
			for index := range values {
				m, err := apocMapArgumentMap(name, values, index)
				if err != nil {
					return nil, err
				}
				if err := apocCopyInto(workCtx, out, m); err != nil {
					return nil, err
				}
			}
		case "apoc.map.setkey":
			m, err := apocMapArgumentMap(name, values, 0)
			if err != nil {
				return nil, err
			}
			if err := apocCopyInto(workCtx, out, m); err != nil {
				return nil, err
			}
			if values[1] != nil {
				key, ok := values[1].(string)
				if !ok {
					return nil, apocMapArgumentTypeError(name, "String", values[1])
				}
				out[key] = values[2]
			}
		case "apoc.map.clean":
			m, err := apocMapArgumentMap(name, values, 0)
			if err != nil {
				return out, nil // retain the native non-map shorthand
			}
			keys, err := apocMapArgumentList(name, values, 1, "List")
			if err != nil {
				return nil, err
			}
			items, err := apocMapArgumentList(name, values, 2, "List")
			if err != nil {
				return nil, err
			}
			if err := workCtx.Err(); err != nil {
				return nil, err
			}
			copied := 0
			for key, value := range m {
				copied++
				if err := apocCopyObserve(workCtx, copied); err != nil {
					return nil, err
				}
				if value == nil {
					continue
				}
				skip := false
				for _, removed := range keys {
					if removed == key {
						skip = true
						break
					}
				}
				for _, removed := range items {
					if apocSameValue(value, removed) || apocSameValue(fmt.Sprint(value), removed) {
						skip = true
						break
					}
				}
				if !skip {
					out[key] = value
				}
			}
		}
		return out, nil
	}
}

// APOC map keys use Java Object.toString, including Map's {key=value}
// spelling. Keep nested map/list formatting at this value boundary.
func apocObjectString(value interface{}) string {
	if value == nil {
		return "null"
	}
	if m, ok := apocMapValue(value); ok {
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = key + "=" + apocObjectString(m[key])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	if list, ok := cypherListValue(value); ok {
		parts := make([]string, len(list))
		for i, item := range list {
			parts[i] = apocObjectString(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(value)
}
