package multidb

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

// Typed Cypher scalars were not registered with the legacy quota gob encoder.
// Charge their durable scalar representation inside the existing logical gob
// model. Encoding their concrete Go structs directly is not a stable size:
// gob emits interface type definitions in map iteration order, and their
// enclosing message lengths can vary. Primitive properties retain their
// existing quota representation. This projection never changes stored data.
func quotaTypedProperty(value any) (any, bool, error) {
	if _, typed := value.(storage.TypedPropertyValue); typed {
		encoded, err := msgpack.Marshal(value)
		return encoded, true, err
	}
	if properties, ok := value.(map[string]any); ok {
		return quotaTypedProperties(properties)
	}
	list := reflect.ValueOf(value)
	if !list.IsValid() || (list.Kind() != reflect.Array && list.Kind() != reflect.Slice) {
		return value, false, nil
	}
	switch list.Type().Elem().Kind() {
	case reflect.Interface, reflect.Struct, reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
	default:
		return value, false, nil
	}
	for i := 0; i < list.Len(); i++ {
		_, changed, err := quotaTypedProperty(list.Index(i).Interface())
		if err != nil {
			return nil, false, err
		}
		if changed {
			encoded, err := msgpack.Marshal(value)
			return encoded, true, err
		}
	}
	return value, false, nil
}

func quotaTypedProperties(properties map[string]any) (map[string]any, bool, error) {
	var projected map[string]any
	for key, value := range properties {
		item, changed, err := quotaTypedProperty(value)
		if err != nil {
			return nil, false, err
		}
		if changed && projected == nil {
			projected = make(map[string]any, len(properties))
			for name, original := range properties {
				projected[name] = original
			}
		}
		if changed {
			projected[key] = item
		}
	}
	if projected != nil {
		return projected, true, nil
	}
	return properties, false, nil
}
