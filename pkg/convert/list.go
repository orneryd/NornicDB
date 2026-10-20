package convert

import "reflect"

type listKind uint8

const (
	reflectList listKind = iota
	boxedList
	stringList
	intList
	int64List
	float64List
	float32List
	boolList
	mapList
)

// List is a borrowed runtime list view. Numeric consumers can visit coordinates
// without first allocating a boxed copy; collection consumers use Values.
type List struct {
	value     interface{}
	reflected reflect.Value
	length    int
	kind      listKind
}

// ReadList is the single runtime slice/array reader. Graph scalars using a named
// slice explicitly retain scalar identity. No consumer enumerates slice widths.
func ReadList(value interface{}) (List, bool) {
	switch values := value.(type) {
	case []interface{}:
		return List{value: value, length: len(values), kind: boxedList}, true
	case []string:
		return List{value: value, length: len(values), kind: stringList}, true
	case []int:
		return List{value: value, length: len(values), kind: intList}, true
	case []int64:
		return List{value: value, length: len(values), kind: int64List}, true
	case []float64:
		return List{value: value, length: len(values), kind: float64List}, true
	case []float32:
		return List{value: value, length: len(values), kind: float32List}, true
	case []bool:
		return List{value: value, length: len(values), kind: boolList}, true
	case []map[string]interface{}:
		return List{value: value, length: len(values), kind: mapList}, true
	}
	if _, opaque := value.(interface{ CypherOpaqueSlice() }); opaque {
		return List{}, false
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return List{}, false
	}
	return List{reflected: reflected, length: reflected.Len()}, true
}

func (list List) Len() int { return list.length }

// ElementNumberKind retains a typed empty list's numeric element kind. Boxed
// lists have no declared numeric kind; their elements are read individually.
func (list List) ElementNumberKind() NumberKind {
	var element reflect.Type
	if list.reflected.IsValid() {
		element = list.reflected.Type().Elem()
	} else {
		element = reflect.TypeOf(list.value).Elem()
	}
	number, numeric := ReadReflectNumber(reflect.Zero(element))
	if !numeric {
		return NotNumber
	}
	return number.Kind()
}

// At retains the engine's established []int/[]float32 element coercions. Other
// carriers, including nested lists and named scalars, retain their values.
func (list List) At(index int) interface{} {
	switch list.kind {
	case boxedList:
		return list.value.([]interface{})[index]
	case stringList:
		return list.value.([]string)[index]
	case intList:
		return int64(list.value.([]int)[index])
	case int64List:
		return list.value.([]int64)[index]
	case float64List:
		return list.value.([]float64)[index]
	case float32List:
		return float64(list.value.([]float32)[index])
	case boolList:
		return list.value.([]bool)[index]
	case mapList:
		return list.value.([]map[string]interface{})[index]
	}
	return list.reflected.Index(index).Interface()
}

// numberAt avoids boxing floating coordinates between the two shared readers.
func (list List) numberAt(index int) (Number, bool) {
	switch list.kind {
	case boxedList:
		return ReadNumber(list.value.([]interface{})[index])
	case intList:
		return ReadNumber(list.value.([]int)[index])
	case int64List:
		return ReadNumber(list.value.([]int64)[index])
	case float64List:
		return ReadNumber(list.value.([]float64)[index])
	case float32List:
		return ReadNumber(list.value.([]float32)[index])
	case reflectList:
		coordinate := list.reflected.Index(index)
		if coordinate.Kind() == reflect.Interface {
			return ReadNumber(coordinate.Interface())
		}
		return ReadReflectNumber(coordinate)
	}
	return Number{}, false
}

// Values borrows boxed lists. A caller that mutates the result must own a copy.
func (list List) Values() []interface{} {
	if list.kind == boxedList {
		return list.value.([]interface{})
	}
	values := make([]interface{}, list.length)
	for index := range values {
		values[index] = list.At(index)
	}
	return values
}

// ListValue is the boxed-list adapter of ReadList, shared by all collection and
// nested-list algorithms. It does not add another representation classifier.
func ListValue(value interface{}) ([]interface{}, bool) {
	list, ok := ReadList(value)
	if !ok {
		return nil, false
	}
	return list.Values(), true
}
