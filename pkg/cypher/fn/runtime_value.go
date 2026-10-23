package fn

import (
	"context"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
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
//
// The walk observes the statement (statement): a canceled one stops it
// (bulk.Checkpoint.Check) and gets its error, and no value.
func FunctionResult(statement context.Context, value interface{}) (normalized interface{}, err error) {
	defer bulk.RecoverStop(&err)
	walk := bulk.NewCheckpoint(statement)
	normalized, _ = TransformValue(&walk, value, functionResultStep)
	return normalized, nil
}

// functionResultStep is FunctionResult's decision for one value (the value
// walker copies the containers whose members change).
func functionResultStep(value interface{}) (interface{}, bool, bool) {
	// Primitive scalars cannot contain NULL members or be boxed nils.
	// Keep their producer path as cheap as the scalar expression boundary.
	switch value.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return value, false, false
	}
	if RuntimeValue(value) == nil {
		return nil, value != nil, false
	}
	switch value.(type) {
	case map[string]interface{}, map[interface{}]interface{}, []interface{}:
		return value, false, true
	case storage.RemoteEntityValue, storage.RemotePathValue:
		// These maps are graph values, not user maps. In particular an empty
		// entity property bag is still a map, not a NULL entity.
		return value, false, false
	case []string, []byte, []int, []int64, []float32, []float64, []bool:
		// Primitive-list results need no traversal or allocation.
		return value, false, false
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		// Typed lists and maps are converted to Cypher lists and maps.
		return value, false, true
	}
	return value, false, false
}
