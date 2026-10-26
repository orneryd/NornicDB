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

// canonicalPropertyValue owns retained property data at ingress and decode.
// Numeric interpretation belongs to convert; typed scalars retain their codec
// owner's representation, including pointer and timezone semantics.
func canonicalPropertyValue(value any) any {
	owned, _ := ownPropertyValue(value)
	return owned
}

func ownPropertyValue(value any) (any, bool) {
	return ownRetainedValue(value, true)
}

// Metadata shares recursive ownership with properties, while retaining its
// codec's scalar and list types (DecodeNamedMetadata deliberately differs).
func ownedMetadataValue(value any) any {
	owned, exact := ownRetainedValue(value, false)
	if exact {
		return owned
	}
	// Uncommon metadata containers use the existing named-value codec's
	// representation. Invalid images retain their existing serializer error
	// point; no new metadata value format or admission policy is introduced.
	data, err := EncodeNamedProperties(map[string]any{"value": value})
	if err != nil {
		return owned
	}
	decoded, err := DecodeNamedMetadata(data)
	if err != nil {
		return owned
	}
	return decoded["value"]
}

func ownRetainedValue(value any, propertyImage bool) (any, bool) {
	if number, ok := convert.ReadNumber(value); ok {
		if !propertyImage {
			return value, true
		}
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
	case nil, string, bool:
		return v, true
	case OwnedPropertyValue:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, true
		}
		owned, exact := v.StoragePropertyValue()
		if !propertyImage && rv.Kind() == reflect.Pointer && owned != nil && reflect.TypeOf(owned) == rv.Type().Elem() {
			pointer := reflect.New(rv.Type().Elem())
			pointer.Elem().Set(reflect.ValueOf(owned))
			return pointer.Interface(), exact
		}
		return owned, exact
	case time.Time:
		if !propertyImage {
			return v, true
		}
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
		return ownRetainedMap(v, propertyImage)
	case []byte:
		if v == nil {
			return nil, true
		}
		return slices.Clone(v), true
	case []float32:
		return retainedTypedPropertyList(v, propertyImage), true
	case []float64:
		return retainedTypedPropertyList(v, propertyImage), true
	case []int64:
		return retainedTypedPropertyList(v, propertyImage), true
	case []string:
		return retainedTypedPropertyList(v, propertyImage), true
	case []bool:
		return retainedTypedPropertyList(v, propertyImage), true
	}
	if list, ok := convert.ReadList(value); ok {
		if rv := reflect.ValueOf(value); rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil, true
		}
		if !propertyImage {
			original := reflect.ValueOf(value)
			var owned reflect.Value
			if original.Kind() == reflect.Array {
				owned = reflect.New(original.Type()).Elem()
			} else {
				owned = reflect.MakeSlice(original.Type(), list.Len(), list.Len())
			}
			exact := true
			for i := 0; i < list.Len(); i++ {
				item, ok := ownRetainedValue(list.At(i), false)
				exact = exact && ok
				if item != nil {
					owned.Index(i).Set(reflect.ValueOf(item))
				}
			}
			return owned.Interface(), exact
		}
		exact := true
		result, _ := canonicalPropertyList(list.Len(), func(i int) (any, error) {
			owned, ok := ownPropertyValue(list.At(i))
			exact = exact && ok
			return owned, nil
		})
		return result, exact
	}
	return value, false
}

func ownedProperties(properties map[string]any) map[string]any {
	owned, _ := ownPropertyMap(properties)
	return owned
}

func ownPropertyMap(properties map[string]any) (map[string]any, bool) {
	return ownRetainedMap(properties, true)
}

func ownRetainedMap(properties map[string]any, propertyImage bool) (map[string]any, bool) {
	result := make(map[string]any, len(properties))
	exact := true
	for key, value := range properties {
		owned, ok := ownRetainedValue(value, propertyImage)
		result[key] = owned
		exact = exact && ok
	}
	return result, exact
}

func retainedTypedPropertyList[T any](values []T, propertyImage bool) any {
	if !propertyImage {
		return slices.Clone(values)
	}
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
