package storage

import (
	"context"
	"reflect"
)

// constraintCreationValues is the shared duplicate owner for node and
// relationship constraint backfills. Canonical keys select candidates; actual
// equality decides duplicates. Go maps cannot directly key list properties,
// and string concatenation cannot distinguish types or embedded delimiters.
type constraintCreationValues map[compositeIndexKey][]constraintCreationValue

type constraintCreationValue struct {
	id     string
	values []interface{}
}

func (seen constraintCreationValues) add(ctx context.Context, id string, values []interface{}) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	bucketValues, _ := constraintCreationBucketList(values)
	key, ok, err := compositeIndexKeyOf(ctx, bucketValues)
	if err != nil {
		return "", false, err
	}
	if !ok {
		// Keep a bounded candidate bucket for other comparable property values
		// (e.g. timestamps), rather than turning a large temporal backfill into
		// an all-pairs scan. Unindexable forms conservatively share one bucket.
		canonical := make([]interface{}, len(values))
		for i, value := range bucketValues.([]interface{}) {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			if part, valid, err := compositeIndexKeyOf(ctx, value); err != nil {
				return "", false, err
			} else if valid {
				canonical[i] = part
				continue
			}
			part, valid, err := indexValueKey(ctx, value)
			if err != nil {
				return "", false, err
			}
			if !valid {
				canonical = nil
				break
			}
			canonical[i] = part
		}
		key = ""
		if canonical != nil {
			composite, err := NewCompositeKey(ctx, canonical...)
			if err != nil {
				return "", false, err
			}
			key = compositeIndexKey(composite.Hash)
		}
	}
	priorValues := seen[key]
	if len(priorValues) > 0 {
		// A tuple with a value unequal to itself cannot match any candidate
		// under the same constraint equality. Keep at most the first such
		// tuple in its bucket instead of growing a quadratic collision scan.
		// Distinct ordinary buckets need no extra value traversal.
		for _, value := range values {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			if !compareValues(value, value) {
				return "", false, ctx.Err()
			}
		}
	}
	for _, prior := range priorValues {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if len(prior.values) != len(values) {
			continue
		}
		equal := true
		for i, value := range values {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			if !compareValues(prior.values[i], value) {
				equal = false
				break
			}
		}
		if equal {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			return prior.id, true, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	seen[key] = append(priorValues, constraintCreationValue{id: id, values: values})
	return "", false, nil
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
