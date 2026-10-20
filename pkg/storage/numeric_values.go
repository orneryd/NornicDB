package storage

import "reflect"

// NumericValueKey returns the canonical numeric identity used by property
// indexes, constraints and federated execution. Equal numbers have equal keys:
// int64(1) and float64(1) share a float64 key, while int64(9007199254740993)
// retains its exact integer value. Both zero signs have the positive-zero key.
// Non-numbers report false. NaN remains non-reflexive, as it is in equality.
func NumericValueKey(value interface{}) (interface{}, bool) {
	return numericIndexKey(value)
}

// PropertyValueKey exposes the index owner's key, including recursive numeric
// normalization in list/map property values. Unkeyable values report false.
// NumericValueKey documents the scalar number representations.
func PropertyValueKey(value interface{}) (interface{}, bool) {
	return indexValueKey(value)
}

// CompareNumericValues compares numbers through the property/constraint owner.
// numeric is false unless both operands are numeric; ordered is false for NaN.
// For example, int64(9007199254740993) orders after float64(9007199254740992),
// and negative zero compares equal to integer zero.
func CompareNumericValues(left, right interface{}) (order int, numeric, ordered bool) {
	return compareConstraintNumbers(reflect.ValueOf(left), reflect.ValueOf(right))
}
