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
	key, ok := compositeIndexKeyOf(values)
	if !ok {
		// Keep a bounded candidate bucket for other comparable property values
		// (e.g. timestamps), rather than turning a large temporal backfill into
		// an all-pairs scan. Unindexable forms conservatively share one bucket.
		canonical := make([]interface{}, len(values))
		for i, value := range values {
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
			if !constraintCreationValueEqual(prior.values[i], value) {
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

func constraintCreationValueEqual(a, b interface{}) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.IsValid() && bv.IsValid() {
		// Reuse the exact numeric owner already used for constraint list elements:
		// widening adjacent int64 values through float64 would create duplicates.
		if equal, numeric := equalConstraintListNumbers(av, bv); numeric {
			return equal
		}
	}
	return compareValues(a, b)
}
