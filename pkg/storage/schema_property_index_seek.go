package storage

import (
	"math"
	"reflect"
	"sort"
)

// propertyIndexKeyKinds records which kinds of key a property index's sorted
// keys hold, and so whether binary search finds a bound in the order
// compareSchemaIndexValues sorted them in (sortedKeyRangeLocked). That order
// agrees with Cypher's comparison only within one kind: strings by code point,
// integers exactly, and numbers that a float64 holds exactly; a string and a
// number, a boolean, NaN or an integer beyond 2^53 against a float don't
// compare consistently, and an index holding them is never narrowed.
type propertyIndexKeyKinds struct {
	strings       bool // every key is a string
	integers      bool // every key is an integer
	exactNumbers  bool // every key is a float (not NaN) or an integer within ±2^53
	hasKeys       bool
}

// maxExactFloatInteger is 2^53, the largest magnitude below which every
// integer is a float64.
const maxExactFloatInteger = 1 << 53

func newPropertyIndexKeyKinds(keys []interface{}) propertyIndexKeyKinds {
	kinds := propertyIndexKeyKinds{strings: true, integers: true, exactNumbers: true, hasKeys: len(keys) > 0}
	for _, key := range keys {
		isString, isInteger, isExactNumber := indexValueKinds(key)
		kinds.strings = kinds.strings && isString
		kinds.integers = kinds.integers && isInteger
		kinds.exactNumbers = kinds.exactNumbers && isExactNumber
	}
	return kinds
}

// indexValueKinds classifies one index key or range bound.
func indexValueKinds(value interface{}) (isString, isInteger, isExactNumber bool) {
	switch typed := value.(type) {
	case string:
		return true, false, false
	case bool:
		return false, false, false
	case float32:
		return false, false, !math.IsNaN(float64(typed))
	case float64:
		return false, false, !math.IsNaN(typed)
	}
	reflected := reflect.ValueOf(value)
	switch {
	case reflected.CanInt():
		number := reflected.Int()
		return false, true, number >= -maxExactFloatInteger && number <= maxExactFloatInteger
	case reflected.CanUint():
		return false, true, reflected.Uint() <= maxExactFloatInteger
	}
	return false, false, false
}

// seekable reports whether a bound can be found by binary search in keys of
// these kinds.
func (kinds propertyIndexKeyKinds) seekable(bound interface{}) bool {
	if !kinds.hasKeys {
		return false
	}
	isString, isInteger, isExactNumber := indexValueKinds(bound)
	return kinds.strings && isString || kinds.integers && isInteger || kinds.exactNumbers && isExactNumber
}

// sortedKeyRangeLocked returns the sorted non-null keys of idx that bounds
// allows, as a view of the shared cache the caller must not modify. A bound
// is found by binary search when the keys and the bound are of one kind
// (propertyIndexKeyKinds); otherwise it isn't applied, and narrowed is false
// for it. Keys outside the returned range fail the bound; the caller still
// tests the bound on the keys returned when narrowed is false.
// Caller holds idx.mu as sortedKeysViewLocked requires.
func (idx *PropertyIndex) sortedKeyRangeLocked(bounds PropertyIndexBounds) (keys []interface{}, narrowed bool) {
	keys, kinds := idx.sortedKeysViewLocked()
	low, high := 0, len(keys)
	lowerApplied, upperApplied := !bounds.HasLower, !bounds.HasUpper
	if bounds.HasLower && kinds.seekable(bounds.Lower) {
		low = sort.Search(len(keys), func(i int) bool {
			order := compareSchemaIndexValues(keys[i], bounds.Lower)
			return order > 0 || order == 0 && bounds.LowerInclusive
		})
		lowerApplied = true
	}
	if bounds.HasUpper && kinds.seekable(bounds.Upper) {
		high = sort.Search(len(keys), func(i int) bool {
			order := compareSchemaIndexValues(keys[i], bounds.Upper)
			return order > 0 || order == 0 && !bounds.UpperInclusive
		})
		upperApplied = true
	}
	if high < low {
		high = low
	}
	return keys[low:high], lowerApplied && upperApplied
}
