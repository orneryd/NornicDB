package storage

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/ctxsort"
)

// indexValueKey is the one key every in-memory index, UNIQUE cache, key lock
// and pending view files a value under: numbers by exact value (numericIndexKey),
// so 1 and 1.0 match but adjacent integers above 2^53 do not, lists and maps
// under their compositeIndexKey, and other comparable values as they are. It
// reports false for null and for values it can't key (byte arrays, lists
// holding values without a key), which no index holds. An index lookup is
// complete only because every keyed value is filed: a list that was not filed
// made an indexed equality on it return no rows (#844).
//
// ctx is the lifetime the key is prepared for. Keying a list or map writes its
// canonical text element by element, which is linear in the value's element
// count, so that traversal observes ctx every bulkReadCancellationInterval
// units (and the map-key sort observes it too). A cancelled lifetime yields no
// key at all — never a partially prepared one — and the error the caller
// returns is ctx's. Preparation of a healthy lifetime is unchanged, byte for
// byte: the poll is an observation, not a change to what is written.
func indexValueKey(ctx context.Context, value interface{}) (interface{}, bool, error) {
	if key, ok := numericIndexKey(value); ok {
		return key, true, nil
	}
	if value == nil {
		return nil, false, nil
	}
	if key, ok := temporalIndexKeyOf(value); ok {
		return key, true, nil
	}
	if reflect.TypeOf(value).Comparable() {
		return value, true, nil
	}
	key, ok, err := compositeIndexKeyOf(ctx, value)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return key, true, nil
}

// nonNilUnorderedIndexKey is membership only, never an equality key. It keeps
// accepted nonnull values without an indexValueKey (for example []byte) in the
// same arity-one/pending property population used by complete candidate reads.
type nonNilUnorderedIndexKey struct{}

func indexNonNilMembershipKey(ctx context.Context, value interface{}) (interface{}, bool, error) {
	key, keyed, err := indexValueKey(ctx, value)
	if err != nil || keyed || value == nil {
		return key, keyed, err
	}
	return nonNilUnorderedIndexKey{}, true, nil
}

func unorderedIndexMembership(key interface{}) bool {
	switch key.(type) {
	case compositeIndexKey, nonNilUnorderedIndexKey:
		return true
	}
	return false
}

// numericIndexKey keys a number by its exact value: as float64 when a float64
// holds it exactly (every float, and every integer within ±2^53 or otherwise
// representable), else as the integer itself, uint64 when non-negative and
// int64 when negative. Equal keys are exactly the numbers compareValues
// equates, and compareSchemaIndexValues orders all three forms exactly. Both
// zero signs are one key, +0: composite and lock hashes serialise keys as
// text, where -0 and 0 would otherwise differ.
//
// A numeric key has constant size, so it needs no cancellation of its own.
func numericIndexKey(value interface{}) (interface{}, bool) {
	switch n := value.(type) {
	case float64:
		if n == 0 {
			return float64(0), true
		}
		return value, true // already its own key: no new box
	case int64:
		// A negative integer no float64 holds is its own key: return it as
		// given rather than in a new box (IndexLookupValue's keys come back).
		if f := float64(n); !constraintIntegerFloatEqual(n, f) && n < 0 {
			return value, true
		}
		return exactIntegerIndexKey(n), true
	case int:
		return exactIntegerIndexKey(int64(n)), true
	case uint64:
		if f := float64(n); !constraintUnsignedFloatEqual(n, f) {
			return value, true
		}
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

// IndexLookupValue returns value in the form the indexes key it by when it is
// a number, so a caller that counts and then reads the same number converts
// it once; any other value is returned unchanged. Lookups accept either form.
func IndexLookupValue(value interface{}) interface{} {
	if key, ok := numericIndexKey(value); ok {
		return key
	}
	return value
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
// string keys. It contributes to complete nonnull candidates, but is not a
// property value that an ordered index read can sort.
type compositeIndexKey string

func compositeIndexKeyOf(ctx context.Context, value interface{}) (compositeIndexKey, bool, error) {
	var b strings.Builder
	// One cancellation budget covers the whole traversal, nesting and map sort
	// included, so a cancelled lifetime stops the next block of 256 units
	// wherever in the value it lands.
	var cancellation bulkReadCancellation
	ok, err := writeCompositeIndexKey(ctx, &cancellation, &b, value)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return compositeIndexKey(b.String()), true, nil
}

// writeCompositeIndexKey appends value's canonical key text to b and reports
// whether value has one. It polls cancellation before each element it writes,
// so a cancellation that arrives during a long list or map stops the traversal
// within one block instead of after the whole value.
func writeCompositeIndexKey(ctx context.Context, cancellation *bulkReadCancellation, b *strings.Builder, value interface{}) (bool, error) {
	if writeConstraintListNumericKey(b, value) {
		return true, nil
	}
	switch v := value.(type) {
	case nil:
		b.WriteByte('z')
		return true, nil
	case string:
		b.WriteByte('s')
		b.WriteString(strconv.Quote(v))
		return true, nil
	case bool:
		if v {
			b.WriteString("bt")
		} else {
			b.WriteString("bf")
		}
		return true, nil
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			if err := cancellation.check(ctx); err != nil {
				return false, err
			}
			keys = append(keys, key)
		}
		// The shared cancellable sort owner keeps the byte order sort.Strings
		// gave these keys and observes ctx while it compares them.
		if err := ctxsort.Sort(ctx, keys, strings.Compare, false); err != nil {
			return false, err
		}
		b.WriteByte('{')
		for i, key := range keys {
			if err := cancellation.check(ctx); err != nil {
				return false, err
			}
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(key))
			b.WriteByte(':')
			ok, err := writeCompositeIndexKey(ctx, cancellation, b, v[key])
			if err != nil || !ok {
				return false, err
			}
		}
		b.WriteByte('}')
		return true, nil
	}
	list := reflect.ValueOf(value)
	if (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || list.Type().Elem().Kind() == reflect.Uint8 {
		return false, nil
	}
	b.WriteByte('[')
	for i := 0; i < list.Len(); i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		if err := cancellation.check(ctx); err != nil {
			return false, err
		}
		ok, err := writeCompositeIndexKey(ctx, cancellation, b, list.Index(i).Interface())
		if err != nil || !ok {
			return false, err
		}
	}
	b.WriteByte(']')
	return true, nil
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
