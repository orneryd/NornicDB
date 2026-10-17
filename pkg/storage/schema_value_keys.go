package storage

import (
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
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
	if reflect.TypeOf(value).Comparable() {
		return value, true
	}
	if key, ok := compositeIndexKeyOf(value); ok {
		return key, true
	}
	return nil, false
}

// listElementKey is the key an arity-1 property index files a node under for
// each element of its list value, so a membership test (x IN n.prop) seeks
// the nodes whose list holds x (PropertyIndexMemberLookup). Its own type
// keeps it apart from the whole-value keys, which equality seeks, ordered
// scans and index statistics read.
type listElementKey struct {
	element interface{}
}

// forEachIndexEntryKey visits every key a property index files value under:
// its indexValueKey, when it has one, and for a list a listElementKey for
// each distinct element that has one. A list is filed by element even when
// the whole list has no key (it holds a byte array): a membership seek must
// still find it by its other elements. Every arity-1 index and pending view
// files and removes a value through this one reader, so the element entries
// never drift from the whole-value entry. A scalar costs no allocation.
func forEachIndexEntryKey(value interface{}, visit func(key interface{})) {
	if whole, ok := indexValueKey(value); ok {
		visit(whole)
	}
	elements, isList := listElementKeys(value)
	if !isList {
		return
	}
	for _, key := range elements {
		visit(key)
	}
}

// listElementKeys returns the distinct listElementKeys of a list value's
// elements; isList is false for a value that is not a list (byte arrays are
// not lists).
func listElementKeys(value interface{}) (keys []interface{}, isList bool) {
	if value == nil {
		return nil, false
	}
	list := reflect.ValueOf(value)
	if (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || list.Type().Elem().Kind() == reflect.Uint8 {
		return nil, false
	}
	for i := 0; i < list.Len(); i++ {
		elementKey, ok := indexValueKey(list.Index(i).Interface())
		if !ok {
			continue
		}
		key := listElementKey{element: elementKey}
		if !slices.Contains(keys, interface{}(key)) {
			keys = append(keys, key)
		}
	}
	return keys, true
}

// hasIndexEntryKey reports whether a property index files value under key
// (forEachIndexEntryKey).
func hasIndexEntryKey(value, key interface{}) bool {
	if _, isElement := key.(listElementKey); isElement {
		elements, _ := listElementKeys(value)
		return slices.Contains(elements, key)
	}
	whole, ok := indexValueKey(value)
	return ok && whole == key
}

// sameIndexEntryKeys reports whether two values are filed under the same keys.
func sameIndexEntryKeys(a, b interface{}) bool {
	aKey, aOK := indexValueKey(a)
	bKey, bOK := indexValueKey(b)
	if aOK != bOK || aKey != bKey {
		return false
	}
	if aOK {
		// Equal whole keys are equal canonical values: the same elements.
		return true
	}
	aElements, _ := listElementKeys(a)
	bElements, _ := listElementKeys(b)
	return slices.Equal(aElements, bElements)
}

// indexMemberKeys returns the keys under which a property index lists the
// nodes for which value IN n.prop can hold: a list property holding an
// element equal to value, and a property equal to value itself. ok is false
// when value has no key, which no index entry can equal.
func indexMemberKeys(value interface{}) (keys []interface{}, ok bool) {
	whole, ok := indexValueKey(value)
	if !ok {
		return nil, false
	}
	return []interface{}{whole, listElementKey{element: whole}}, true
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
