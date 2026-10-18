package storage

import (
	"math"
	"reflect"
	"strconv"
	"strings"
)

// A distinct token type keeps list keys separate from ordinary string keys
// without changing the persisted digest of any scalar key.
type temporalListKeyToken string
type temporalListFallbackToken struct{}

// listBearing selects history readers that remain correct when an upgrade
// merges formerly distinct list representations with overlapping periods.
func (s temporalKeySpec) listBearing(key interface{}) bool {
	values := []interface{}{key}
	if s.composite() {
		values, _ = key.([]interface{})
	}
	for _, value := range values {
		v := reflect.ValueOf(value)
		if v.IsValid() && v.Kind() == reflect.Slice {
			return true
		}
	}
	return false
}

// temporalListHashValue uses the constraint owner's exact numeric writer, not
// DDL's deliberately coarse candidate buckets. Complex leaves keep the existing
// equality semantics in one shared fallback history; readers must verify keys.
func temporalListHashValue(value interface{}) (interface{}, bool) {
	list := reflect.ValueOf(value)
	if !list.IsValid() || list.Kind() != reflect.Slice {
		return nil, false
	}
	var out strings.Builder
	if !writeTemporalListToken(&out, list, make(map[uintptr]bool)) {
		return temporalListFallbackToken{}, true
	}
	return temporalListKeyToken(out.String()), true
}

func writeTemporalListToken(out *strings.Builder, value reflect.Value, path map[uintptr]bool) bool {
	for value.IsValid() && value.Kind() == reflect.Interface {
		value = value.Elem()
	}
	if !value.IsValid() {
		out.WriteByte('z')
		return true
	}
	if value.Kind() == reflect.Slice {
		if value.IsNil() {
			out.WriteByte('N')
			return true
		}
		if value.Len() != 0 {
			pointer := value.Pointer()
			if path[pointer] {
				return false
			}
			path[pointer] = true
			defer delete(path, pointer)
		}
		out.WriteByte('[')
		for i := 0; i < value.Len(); i++ {
			if !writeTemporalListToken(out, value.Index(i), path) {
				return false
			}
			out.WriteByte(';')
		}
		out.WriteByte(']')
		return true
	}
	// NaN is not equal even to itself, so its history needs the same explicit
	// equality filtering as other values without an exact canonical token.
	if constraintFloat(value.Kind()) && math.IsNaN(value.Float()) {
		return false
	}
	if writeConstraintListNumericKey(out, value.Interface()) {
		return true
	}
	switch value.Kind() {
	case reflect.String:
		out.WriteByte('s')
		out.WriteString(strconv.Quote(value.String()))
		return true
	case reflect.Bool:
		out.WriteByte('b')
		out.WriteString(strconv.FormatBool(value.Bool()))
		return true
	default:
		return false
	}
}
