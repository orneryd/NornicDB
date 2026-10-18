package cypher

import (
	"fmt"
	"reflect"
	"runtime"
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
func bundledAPOCMapHandler(name string, handler interface{}, argumentCount int) bool {
	v := reflect.ValueOf(handler)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() {
		return false
	}
	typ := v.Type()
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
	fn := runtime.FuncForPC(v.Pointer())
	return fn != nil && strings.HasPrefix(fn.Name(), "github.com/orneryd/nornicdb/apoc/")
}

func init() {
	for _, name := range []string{"apoc.map.frompairs", "apoc.map.fromlists", "apoc.map.merge", "apoc.map.setkey", "apoc.map.clean"} {
		cypherfn.Register(name, apocMapConstructor(name))
	}
}

func apocMapConstructor(name string) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		min, max := 1, 1
		switch name {
		case "apoc.map.fromlists", "apoc.map.merge":
			min, max = 2, 2
		case "apoc.map.setkey":
			min, max = 3, 3
		case "apoc.map.clean":
			min, max = 1, 3 // retain the release shorthand
		}
		if len(args) < min || len(args) > max {
			// Preserve the native merge shorthand's existing result for a
			// missing operand. Null operands are handled separately below.
			if name == "apoc.map.merge" {
				return map[string]interface{}{}, nil
			}
			return nil, functionParameterCountError(name, len(args) > max)
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		// Omitted clean lists default to [], whereas explicit null fails.
		if name == "apoc.map.clean" {
			for len(values) < 3 {
				values = append(values, []interface{}{})
			}
		}
		if null, err := apocMapNullInput(name, values); null || err != nil {
			return nil, err
		}
		out := map[string]interface{}{}
		readMap := func(value interface{}) (map[string]interface{}, error) {
			if apocNullValue(value) {
				return nil, nil
			}
			m, ok := apocMapValue(value)
			if !ok {
				return nil, apocMapArgumentTypeError(name, "Map", value)
			}
			return m, nil
		}
		readList := func(value interface{}) ([]interface{}, error) {
			list, ok := cypherListValue(value)
			if !ok {
				return nil, apocMapArgumentTypeError(name, "List", value)
			}
			return list, nil
		}
		switch name {
		case "apoc.map.frompairs":
			pairs, err := readList(values[0])
			if err != nil {
				return out, nil // retain the native non-list shorthand
			}
			for _, pair := range pairs {
				items, err := readList(pair)
				if err != nil {
					return nil, err
				}
				if len(items) == 0 || items[0] == nil {
					continue
				}
				var value interface{}
				if len(items) > 1 {
					value = items[1]
				}
				out[fmt.Sprint(items[0])] = value
			}
		case "apoc.map.fromlists":
			keys, err := readList(values[0])
			if err != nil {
				return nil, err
			}
			items, err := readList(values[1])
			if err != nil {
				return nil, err
			}
			if len(keys) != len(items) {
				return nil, nativeAPOCCallFailure(name, values)
			}
			for i, key := range keys {
				out[fmt.Sprint(key)] = items[i]
			}
		case "apoc.map.merge":
			for _, value := range values {
				m, err := readMap(value)
				if err != nil {
					return nil, err
				}
				for key, value := range m {
					out[key] = value
				}
			}
		case "apoc.map.setkey":
			m, err := readMap(values[0])
			if err != nil {
				return nil, err
			}
			for key, value := range m {
				out[key] = value
			}
			if values[1] != nil {
				key, ok := values[1].(string)
				if !ok {
					return nil, apocMapArgumentTypeError(name, "String", values[1])
				}
				out[key] = values[2]
			}
		case "apoc.map.clean":
			m, err := readMap(values[0])
			if err != nil {
				return out, nil // retain the native non-map shorthand
			}
			keys, err := readList(values[1])
			if err != nil {
				return nil, err
			}
			items, err := readList(values[2])
			if err != nil {
				return nil, err
			}
			for key, value := range m {
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
