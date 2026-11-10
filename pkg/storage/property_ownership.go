package storage

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// OwnedPropertyValue is implemented by a registered scalar's codec owner.
// StoragePropertyValue returns an independent value in the representation that
// the scalar's durable decoder returns. Exact is false when the durable decoder
// must validate the image instead. Containers remain storage's responsibility.
type OwnedPropertyValue interface {
	StoragePropertyValue() (value any, exact bool)
}

// ownedEntries copies src entries into dst, taking ownership of every value
// through CopyValue (property_copy.go, the generic copy owner). A nil dst
// is allocated with room for src, so callers that return an independent image
// always get a non-nil map, while callers that must keep an absent map absent
// check the nil before the call.
func ownedEntries(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = make(map[string]any, len(src))
	}
	for key, value := range src {
		dst[key] = CopyValue(value, nil)
	}
	return dst
}

// ownRetainedValue returns value in the representation the durable decoder
// returns, and whether that image is exact (see OwnedPropertyValue).
func ownRetainedValue(value any) (any, bool) {
	// These immutable scalar images already match the durable decoder. Keep
	// their existing interface boxes instead of allocating a replacement on
	// every cache copy. Other numeric types still require codec normalization.
	switch value.(type) {
	case nil, string, bool, int64, float32, float64:
		return value, true
	}
	if number, ok := convert.ReadNumber(value); ok {
		switch number.Kind() {
		case convert.SignedNumber:
			v, _ := number.Signed()
			return v, true
		case convert.UnsignedNumber:
			v, _ := number.Unsigned()
			if v <= math.MaxInt64 {
				return int64(v), true
			}
			return v, true
		case convert.FloatNumber:
			if reflect.TypeOf(value).Kind() == reflect.Float32 {
				return float32(number.Float64()), true
			}
			return number.Float64(), true
		}
	}
	switch v := value.(type) {
	case OwnedPropertyValue:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, true
		}
		return v.StoragePropertyValue()
	case time.Time:
		location := v.Location()
		if strings.Contains(location.String(), "/") {
			var err error
			location, err = time.LoadLocation(location.String())
			if err != nil {
				return v, false
			}
		} else {
			_, offset := v.Zone()
			location = time.FixedZone("", int(int32(offset)))
		}
		return time.Unix(v.Unix(), int64(v.Nanosecond())).In(location), true
	case map[string]any:
		if v == nil {
			return nil, true
		}
		return ownRetainedMap(v)
	case []byte:
		if v == nil {
			return nil, true
		}
		return slices.Clone(v), true
	case []float32:
		return retainedTypedPropertyList(v), true
	case []float64:
		return retainedTypedPropertyList(v), true
	case []int64:
		return retainedTypedPropertyList(v), true
	case []string:
		return retainedTypedPropertyList(v), true
	case []bool:
		return retainedTypedPropertyList(v), true
	}
	if list, ok := convert.ReadList(value); ok {
		if rv := reflect.ValueOf(value); rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil, true
		}
		exact := true
		result, _ := canonicalPropertyList(list.Len(), func(i int) (any, error) {
			owned, ok := ownRetainedValue(list.At(i))
			exact = exact && ok
			return owned, nil
		})
		return result, exact
	}
	return value, false
}

func ownRetainedMap(properties map[string]any) (map[string]any, bool) {
	result := make(map[string]any, len(properties))
	exact := true
	for key, value := range properties {
		owned, ok := ownRetainedValue(value)
		result[key] = owned
		exact = exact && ok
	}
	return result, exact
}

func retainedTypedPropertyList[T any](values []T) any {
	if values == nil {
		return nil
	}
	if len(values) == 0 {
		return []any{}
	}
	return slices.Clone(values)
}

// canonicalPropertyList is the shared homogeneous/mixed representation owner.
// A decoder supplies its wire reader; ingress supplies recursively owned values.
func canonicalPropertyList(length int, next func(int) (any, error)) (any, error) {
	if length == 0 {
		return []any{}, nil
	}
	first, err := next(0)
	if err != nil {
		return nil, err
	}
	switch v := first.(type) {
	case int64:
		return collectPropertyList(length, v, next)
	case float64:
		return collectPropertyList(length, v, next)
	case float32:
		return collectPropertyList(length, v, next)
	case string:
		return collectPropertyList(length, v, next)
	case bool:
		return collectPropertyList(length, v, next)
	default:
		result := make([]any, length)
		result[0] = first
		for i := 1; i < length; i++ {
			result[i], err = next(i)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
}

func collectPropertyList[T any](length int, first T, next func(int) (any, error)) (any, error) {
	result := make([]T, length)
	result[0] = first
	for i := 1; i < length; i++ {
		value, err := next(i)
		if err != nil {
			return nil, err
		}
		if typed, ok := value.(T); ok {
			result[i] = typed
			continue
		}
		mixed := make([]any, length)
		for j := 0; j < i; j++ {
			mixed[j] = result[j]
		}
		mixed[i] = value
		for j := i + 1; j < length; j++ {
			mixed[j], err = next(j)
			if err != nil {
				return nil, err
			}
		}
		return mixed, nil
	}
	return result, nil
}
