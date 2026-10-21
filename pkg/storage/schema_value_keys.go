package storage

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// indexValueKey is the one key every in-memory index, UNIQUE cache, key lock
// and pending view files a value under: numbers by exact value (numericIndexKey),
// so 1 and 1.0 match but adjacent integers above 2^53 do not, lists and maps
// under their compositeIndexKey, and other comparable values as they are. It
// reports false for null and for values it can't key (byte arrays, lists
// holding values without a key), which no index holds. An index lookup is
// complete only because every keyed value is filed: a list that was not filed
// made an indexed equality on it return no rows (#844).
func indexValueKey(value interface{}) (interface{}, bool) {
	if key, ok := numericIndexKey(value); ok {
		return key, true
	}
	if value == nil {
		return nil, false
	}
	if key, ok := temporalIndexKeyOf(value); ok {
		return key, true
	}
	if reflect.TypeOf(value).Comparable() {
		return value, true
	}
	if key, ok := compositeIndexKeyOf(value); ok {
		return key, true
	}
	return nil, false
}

// numericIndexKey keys a number by its exact value: as float64 when a float64
// holds it exactly (every float, and every integer within ±2^53 or otherwise
// representable), else as the integer itself, uint64 when non-negative and
// int64 when negative. Equal keys are exactly the numbers compareValues
// equates, and compareSchemaIndexValues orders all three forms exactly. Both
// zero signs are one key, +0: composite and lock hashes serialise keys as
// text, where -0 and 0 would otherwise differ.
func numericIndexKey(value interface{}) (interface{}, bool) {
	switch n := value.(type) {
	case float64:
		if n == 0 {
			return float64(0), true
		}
		return n, true
	case int64:
		return exactIntegerIndexKey(n), true
	case int:
		return exactIntegerIndexKey(int64(n)), true
	case uint64:
		return exactUnsignedIndexKey(n), true
	case uint:
		return exactUnsignedIndexKey(uint64(n)), true
	}
	// The narrower integer types and float32 always fit a float64 exactly.
	numeric, ok := numericConstraintValue(value)
	if numeric == 0 {
		numeric = 0 // float32 -0
	}
	return numeric, ok
}

func exactIntegerIndexKey(n int64) interface{} {
	if f := float64(n); constraintIntegerFloatEqual(n, f) {
		return f
	}
	if n < 0 {
		return n
	}
	return uint64(n)
}

func exactUnsignedIndexKey(n uint64) interface{} {
	if f := float64(n); constraintUnsignedFloatEqual(n, f) {
		return f
	}
	return n
}

// compositeIndexKey is the index key of a list or map value: a canonical text
// of its elements, numbers in their numeric form, so [1, 2] and [1.0, 2.0]
// share a key, and map entries in key order. Its own type keeps it apart from
// string keys. Ordered index scans skip it (sortedKeysViewLocked).
type compositeIndexKey string

func compositeIndexKeyOf(value interface{}) (compositeIndexKey, bool) {
	var b strings.Builder
	if !writeCompositeIndexKey(&b, value) {
		return "", false
	}
	return compositeIndexKey(b.String()), true
}

func writeCompositeIndexKey(b *strings.Builder, value interface{}) bool {
	if writeConstraintListNumericKey(b, value) {
		return true
	}
	switch v := value.(type) {
	case nil:
		b.WriteByte('z')
		return true
	case string:
		b.WriteByte('s')
		b.WriteString(strconv.Quote(v))
		return true
	case bool:
		if v {
			b.WriteString("bt")
		} else {
			b.WriteString("bf")
		}
		return true
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(key))
			b.WriteByte(':')
			if !writeCompositeIndexKey(b, v[key]) {
				return false
			}
		}
		b.WriteByte('}')
		return true
	}
	list := reflect.ValueOf(value)
	if (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || list.Type().Elem().Kind() == reflect.Uint8 {
		return false
	}
	b.WriteByte('[')
	for i := 0; i < list.Len(); i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		if !writeCompositeIndexKey(b, list.Index(i).Interface()) {
			return false
		}
	}
	b.WriteByte(']')
	return true
}

// temporalIndexKey is the index key of a temporal value: its kind, instant,
// UTC offset and text (a zone ID among it). A temporal value read from
// storage carries a zone of its own, so as a map key it never equals the
// same value written or given as a parameter. Ordered scans compare keys by
// their text (compareSchemaIndexValues), as they compared the values.
type temporalIndexKey struct {
	kind   string
	unix   int64
	nanos  int
	offset int
	text   string
}

func temporalIndexKeyOf(value interface{}) (temporalIndexKey, bool) {
	var at time.Time
	kind := "time"
	switch v := value.(type) {
	case time.Time:
		at = v
	case interface{ TemporalTime() time.Time }:
		at = v.TemporalTime()
		kind = reflect.TypeOf(value).String()
	default:
		return temporalIndexKey{}, false
	}
	_, offset := at.Zone()
	key := temporalIndexKey{kind: kind, unix: at.Unix(), nanos: at.Nanosecond(), offset: offset}
	if text, ok := value.(interface{ String() string }); ok && kind != "time" {
		key.text = text.String()
	} else {
		key.text = at.Format("2006-01-02 15:04:05.999999999 -0700")
	}
	return key, true
}

func (k temporalIndexKey) String() string { return k.text }
