package storage

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// PropertyIndexBounds selects the values of a property index within a lower
// and/or an upper bound. Bounds are numbers or strings; a value of another
// type than a bound never falls within it, as Cypher compares numbers only
// with numbers and strings only with strings (anything else is null).
type PropertyIndexBounds struct {
	Lower, Upper                   interface{}
	HasLower, HasUpper             bool
	LowerInclusive, UpperInclusive bool
}

// PropertyIndexRange returns the node IDs whose indexed value lies within
// bounds, in value order, merged with the pending writes. ok is false when
// the label and property have no index or a bound is neither a number nor a
// string; the caller must then not rely on the index.
func (sm *SchemaManager) PropertyIndexRange(label, property string, bounds PropertyIndexBounds) (ids []NodeID, ok bool) {
	if (bounds.HasLower && !indexRangeBoundType(bounds.Lower)) || (bounds.HasUpper && !indexRangeBoundType(bounds.Upper)) {
		return nil, false
	}
	return sm.orderedPropertyIndexIDs(label, property, false, -1, bounds)
}

// bounded reports whether bounds limits anything.
func (bounds PropertyIndexBounds) bounded() bool {
	return bounds.HasLower || bounds.HasUpper
}

// contains reports whether an index value lies within bounds. A value of
// another type than a bound never does: Cypher compares numbers only with
// numbers and strings only with strings.
func (bounds PropertyIndexBounds) contains(key interface{}) bool {
	if bounds.HasLower {
		order, comparable := compareIndexRangeValues(key, bounds.Lower)
		if !comparable || order < 0 || (order == 0 && !bounds.LowerInclusive) {
			return false
		}
	}
	if bounds.HasUpper {
		order, comparable := compareIndexRangeValues(key, bounds.Upper)
		if !comparable || order > 0 || (order == 0 && !bounds.UpperInclusive) {
			return false
		}
	}
	return true
}

func indexRangeBoundType(value interface{}) bool {
	if _, isString := value.(string); isString {
		return true
	}
	_, isNumber := convert.ToFloat64(value)
	return isNumber && !isBoolValue(value)
}

func isBoolValue(value interface{}) bool {
	_, isBool := value.(bool)
	return isBool
}

// compareIndexRangeValues orders an index value against a range bound;
// comparable is false when one is a string and the other a number, or the
// value is of any other type.
func compareIndexRangeValues(value, bound interface{}) (order int, comparable bool) {
	if boundString, isString := bound.(string); isString {
		valueString, ok := value.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(valueString, boundString), true
	}
	if !indexRangeBoundType(value) {
		return 0, false
	}
	if _, isString := value.(string); isString {
		return 0, false
	}
	return compareSchemaIndexValues(value, bound), true
}
