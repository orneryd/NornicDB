package fn

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RuntimeValue canonicalizes the null representations crossing the expression
// boundary. A boxed nil map is NULL, while an allocated empty map is a map.
// Entity property bags must be read with EntityProperties before this boundary:
// an existing entity with no properties still has a valid empty property map.
// Lists retain their existing semantics, including nil slices. FunctionResult
// applies this same classification recursively when Go handlers produce values.
func RuntimeValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case map[string]interface{}:
		if v == nil {
			return nil
		}
	case map[interface{}]interface{}:
		if v == nil {
			return nil
		}
	case storage.RemoteEntityValue:
		if v == nil {
			return nil
		}
	case storage.RemotePathValue:
		if v == nil {
			return nil
		}
	case *storage.Node:
		if v == nil {
			return nil
		}
	case *storage.Edge:
		if v == nil {
			return nil
		}
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, []interface{}, []string, []byte, []int, []int64, []float32, []float64, []bool:
	default:
		// Named map types and typed nil pointers from public Go parameters or
		// reflected plugin handlers obey the same contract as the common types.
		reflected := reflect.ValueOf(value)
		if (reflected.Kind() == reflect.Map || reflected.Kind() == reflect.Ptr) && reflected.IsNil() {
			return nil
		}
	}
	return value
}

// FunctionResult admits a Go function or procedure result into Cypher. Unlike
// the scalar expression boundary, it visits container members once, before any
// consumer can observe a boxed nil. For example, {a: map[string]interface{}(nil)}
// becomes {a: nil}, while allocated empty maps and nil lists remain containers.
// Containers are copied only when conversion is needed; handler-owned values
// are never modified. Entities retain their identity and property-bag semantics.
func FunctionResult(value interface{}) interface{} {
	normalized, _ := functionResult(value)
	return normalized
}

func functionResult(value interface{}) (interface{}, bool) {
	// Primitive scalars cannot contain NULL members or be boxed nils.
	// Keep their producer path as cheap as the scalar expression boundary.
	switch value.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return value, false
	}
	if RuntimeValue(value) == nil {
		return nil, value != nil
	}
	switch v := value.(type) {
	case map[string]interface{}:
		var out map[string]interface{}
		for key, item := range v {
			normalized, changed := functionResult(item)
			if !changed {
				continue
			}
			if out == nil {
				out = make(map[string]interface{}, len(v))
				for k, item := range v {
					out[k] = item
				}
			}
			out[key] = normalized
		}
		if out != nil {
			return out, true
		}
	case map[interface{}]interface{}:
		var out map[interface{}]interface{}
		for key, item := range v {
			normalized, changed := functionResult(item)
			if !changed {
				continue
			}
			if out == nil {
				out = make(map[interface{}]interface{}, len(v))
				for k, item := range v {
					out[k] = item
				}
			}
			out[key] = normalized
		}
		if out != nil {
			return out, true
		}
	case []interface{}:
		var out []interface{}
		for index, item := range v {
			normalized, changed := functionResult(item)
			if !changed {
				continue
			}
			if out == nil {
				out = append([]interface{}(nil), v...)
			}
			out[index] = normalized
		}
		if out != nil {
			return out, true
		}
	case storage.RemoteEntityValue, storage.RemotePathValue:
		// These maps are graph values, not user maps. In particular an empty
		// entity property bag is still a map, not a NULL entity.
	case []string, []byte, []int, []int64, []float32, []float64, []bool:
		// Primitive-list results need no traversal or allocation.
	default:
		reflected := reflect.ValueOf(value)
		switch reflected.Kind() {
		case reflect.Map:
			if reflected.Type().Key().Kind() == reflect.String {
				out := make(map[string]interface{}, reflected.Len())
				iterator := reflected.MapRange()
				for iterator.Next() {
					out[iterator.Key().String()] = FunctionResult(iterator.Value().Interface())
				}
				return out, true
			}
			out := make(map[interface{}]interface{}, reflected.Len())
			iterator := reflected.MapRange()
			for iterator.Next() {
				out[iterator.Key().Interface()] = FunctionResult(iterator.Value().Interface())
			}
			return out, true
		case reflect.Slice, reflect.Array:
			out := make([]interface{}, reflected.Len())
			for index := range out {
				out[index] = FunctionResult(reflected.Index(index).Interface())
			}
			return out, true
		}
	}
	return value, false
}
