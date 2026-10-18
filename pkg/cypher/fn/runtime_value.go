package fn

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RuntimeValue canonicalizes the null representations crossing the expression
// boundary. A boxed nil map is NULL, while an allocated empty map is a map.
// Entity property bags must be read with EntityProperties before this boundary:
// an existing entity with no properties still has a valid empty property map.
// Lists retain their existing semantics, including nil slices.
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
