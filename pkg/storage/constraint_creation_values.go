package storage

import "reflect"

// constraintCreationValues is the shared duplicate owner for node and
// relationship constraint backfills. Canonical keys select candidates; actual
// equality decides duplicates. Go maps cannot directly key list properties,
// and string concatenation cannot distinguish types or embedded delimiters.
type constraintCreationValues map[compositeIndexKey][]constraintCreationValue

type constraintCreationValue struct {
	id     string
	values []interface{}
}

func (seen constraintCreationValues) add(id string, values []interface{}) (string, bool) {
	bucketValues, _ := constraintCreationBucketList(values)
	key, ok := compositeIndexKeyOf(bucketValues)
	if !ok {
		// Keep a bounded candidate bucket for other comparable property values
		// (e.g. timestamps), rather than turning a large temporal backfill into
		// an all-pairs scan. Unindexable forms conservatively share one bucket.
		canonical := make([]interface{}, len(values))
		for i, value := range bucketValues.([]interface{}) {
			if part, valid := compositeIndexKeyOf(value); valid {
				canonical[i] = part
				continue
			}
			part, valid := indexValueKey(value)
			if !valid {
				canonical = nil
				break
			}
			canonical[i] = part
		}
		key = ""
		if canonical != nil {
			key = compositeIndexKey(constraintCompositeKey(canonical))
		}
	}
	for _, prior := range seen[key] {
		if len(prior.values) != len(values) {
			continue
		}
		equal := true
		for i, value := range values {
			if !compareValues(prior.values[i], value) {
				equal = false
				break
			}
		}
		if equal {
			return prior.id, true
		}
	}
	seen[key] = append(seen[key], constraintCreationValue{id: id, values: values})
	return "", false
}

// Property indexes deliberately do not index byte arrays. DDL uniqueness,
// however, uses the existing constraint equality owner, which compares byte
// slices with numeric slices element by element. Normalize only the private
// candidate bucket, recursively, without changing the original values used
// for equality or the durable/property index encodings. Array buckets may be
// coarser than equality (array versus slice stays distinct), which is safe
// because add always verifies the original candidate values.
func constraintCreationBucketList(value interface{}) (interface{}, bool) {
	list := reflect.ValueOf(value)
	if !list.IsValid() || (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) {
		return value, false
	}
	if list.Type().Elem().Kind() == reflect.Uint8 {
		numbers := make([]int64, list.Len())
		for i := range numbers {
			numbers[i] = int64(list.Index(i).Uint())
		}
		return numbers, true
	}
	kind := list.Type().Elem().Kind()
	if kind != reflect.Interface && kind != reflect.Slice && kind != reflect.Array {
		return value, false
	}
	var normalized []interface{}
	for i := 0; i < list.Len(); i++ {
		child, changed := constraintCreationBucketList(list.Index(i).Interface())
		if changed && normalized == nil {
			normalized = make([]interface{}, list.Len())
			for j := 0; j < i; j++ {
				normalized[j] = list.Index(j).Interface()
			}
		}
		if normalized != nil {
			normalized[i] = child
		}
	}
	if normalized != nil {
		return normalized, true
	}
	return value, false
}
