package storage

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// indexValueKey is the key a property index and the generic pending view
// (pendingNodeIndex) file a value under. UNIQUE caches preserve precision
// with uniqueConstraintValueKey when this legacy numeric key is lossy:
// numbers in one numeric form, so 1 and 1.0 match, lists and maps under their
// compositeIndexKey, and other comparable values as they are. It reports false
// for null and for values it can't key (byte arrays, lists holding values
// without a key), which no index holds. An index lookup is complete only
// because every keyed value is filed: a list that was not filed made an
// indexed equality on it return no rows (#844).
func indexValueKey(value interface{}) (interface{}, bool) {
	if numeric, ok := numericConstraintValue(value); ok {
		return numeric, true
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
