package storage

import (
	"math"
	"reflect"
	"slices"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// canonicalPropertyValue is the property representation owner. Write entries
// give it caller data and retain only its owned result, for both encoding and
// publication. Decoders feed it decoded values, including older narrow integer
// encodings. Neither wire tags nor cache residency decide the property type.
// Every container is copied; scalar integers in the signed range become int64.
func canonicalPropertyValue(value any) any {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return canonicalPropertyValue(uint64(v))
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v)
		}
		return v
	case map[string]any:
		if v == nil {
			return nil
		}
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = canonicalPropertyValue(item)
		}
		return out
	case []byte:
		if v == nil {
			return nil
		}
		return slices.Clone(v)
	case []int64:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		return slices.Clone(v)
	case []int:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		out := make([]int64, len(v))
		for i, item := range v {
			out[i] = int64(item)
		}
		return out
	case []float64:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		return slices.Clone(v)
	case []float32:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		return slices.Clone(v)
	case []string:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		return slices.Clone(v)
	case []bool:
		if v == nil {
			return nil
		}
		if len(v) == 0 {
			return []any{}
		}
		return slices.Clone(v)
	case []any:
		if v == nil {
			return nil
		}
		out, _ := canonicalPropertyList(len(v), func(i int) (any, error) {
			return canonicalPropertyValue(v[i]), nil
		}, nil)
		return out
	default:
		// Typed input lists enter the same recursive policy as decoded lists.
		// Opaque property scalars (temporal values and points) stay opaque.
		rv := reflect.ValueOf(value)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			if rv.IsNil() {
				return nil
			}
			items := make([]any, rv.Len())
			for i := range items {
				items[i] = rv.Index(i).Interface()
			}
			return canonicalPropertyValue(items)
		}
		return value
	}
}

// canonicalPropertyList owns the representation of every list. A wire reader
// supplies typed numeric reads to avoid boxing each element; a caller-owned
// list supplies recursive copies. Both use the same homogeneous/mixed builder.
func canonicalPropertyList(n int, next func(int) (any, error), wire *msgpack.Decoder) (any, error) {
	if n == 0 {
		return []any{}, nil
	}
	var first any
	var err error
	var unsignedFirst uint64
	unsignedConsumed := false
	if wire != nil {
		code, peekErr := wire.PeekCode()
		if peekErr != nil {
			return nil, peekErr
		}
		// These are transport tags, mapped to the same canonical scalar kinds
		// selected below. Integer widths never choose a different list type.
		switch {
		case code == msgpcode.Uint64:
			unsignedFirst, err = wire.DecodeUint64()
			unsignedConsumed = true
			first = int64(0)
			if unsignedFirst > math.MaxInt64 {
				first = uint64(0)
			}
		case code == msgpcode.Double:
			first = float64(0)
		case code == msgpcode.Float:
			first = float32(0)
		case propertyIntegerCode(code):
			first = int64(0)
		default:
			first, err = next(0)
		}
	} else {
		first, err = next(0)
	}
	if err != nil {
		return nil, err
	}
	// Numeric wire candidates have not consumed their first element.
	consumed := wire == nil
	if wire != nil {
		switch first.(type) {
		case int64, float32, float64:
		default:
			consumed = true
		}
	}
	read := func(i int) (any, error) {
		if i == 0 && consumed {
			return first, nil
		}
		return next(i)
	}
	switch first.(type) {
	case int64:
		return buildCanonicalList[int64](n, read, wire, consumed, propertyIntegerCode, func() (int64, bool, any, error) {
			code, err := wire.PeekCode()
			if err != nil {
				return 0, false, nil, err
			}
			if code >= msgpcode.Uint8 && code <= msgpcode.Uint64 {
				u, err := wire.DecodeUint64()
				if u > math.MaxInt64 {
					return 0, false, u, err
				}
				return int64(u), true, nil, err
			}
			v, err := wire.DecodeInt64()
			return v, true, nil, err
		}, int64(unsignedFirst), unsignedConsumed)
	case uint64:
		if wire == nil {
			return buildCanonicalList[uint64](n, read, nil, true, nil, nil, 0, false)
		}
		return buildCanonicalList[uint64](n, read, wire, false, func(c byte) bool { return c == msgpcode.Uint64 }, func() (uint64, bool, any, error) {
			v, err := wire.DecodeUint64()
			if v <= math.MaxInt64 {
				return 0, false, int64(v), err
			}
			return v, true, nil, err
		}, unsignedFirst, unsignedConsumed)
	case float64:
		return buildCanonicalList[float64](n, read, wire, consumed, func(c byte) bool { return c == msgpcode.Double }, func() (float64, bool, any, error) { v, err := wire.DecodeFloat64(); return v, true, nil, err }, 0, false)
	case float32:
		return buildCanonicalList[float32](n, read, wire, consumed, func(c byte) bool { return c == msgpcode.Float }, func() (float32, bool, any, error) { v, err := wire.DecodeFloat32(); return v, true, nil, err }, 0, false)
	case string:
		return buildCanonicalList[string](n, read, nil, true, nil, nil, "", false)
	case bool:
		return buildCanonicalList[bool](n, read, nil, true, nil, nil, false, false)
	default:
		out := make([]any, n)
		for i := range out {
			out[i], err = read(i)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
}

func propertyIntegerCode(c byte) bool {
	return c <= 0x7f || c >= 0xe0 || (c >= msgpcode.Uint8 && c <= msgpcode.Int64)
}

// The common builder makes exactly one backing allocation on homogeneous
// lists. Only a mixed list needs an interface slice; nested values arrive
// already canonical from the recursive reader.
func buildCanonicalList[T any](n int, next func(int) (any, error), wire *msgpack.Decoder, consumed bool, matches func(byte) bool, number func() (T, bool, any, error), first T, hasFirst bool) (any, error) {
	out := make([]T, n)
	for i := range out {
		if i == 0 && hasFirst {
			out[i] = first
			continue
		}
		var value T
		var ok bool
		var err error
		var mixed any
		if wire != nil && !(i == 0 && consumed) {
			code, peekErr := wire.PeekCode()
			if peekErr != nil {
				return nil, peekErr
			}
			if matches(code) {
				value, ok, mixed, err = number()
			} else {
				mixed, err = next(i)
			}
		} else {
			mixed, err = next(i)
			value, ok = mixed.(T)
		}
		if err != nil {
			return nil, err
		}
		if !ok {
			items := make([]any, n)
			for j := 0; j < i; j++ {
				items[j] = out[j]
			}
			items[i] = mixed
			for j := i + 1; j < n; j++ {
				items[j], err = next(j)
				if err != nil {
					return nil, err
				}
			}
			return items, nil
		}
		out[i] = value
	}
	return out, nil
}

func nodeForStorageWrite(node *Node) *Node {
	if node == nil {
		return nil
	}
	owned := *node
	if node.Properties != nil {
		owned.Properties = canonicalPropertyValue(node.Properties).(map[string]any)
	} else {
		owned.Properties = map[string]any{}
	}
	return &owned
}

func edgeForStorageWrite(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	owned := *edge
	if edge.Properties != nil {
		owned.Properties = canonicalPropertyValue(edge.Properties).(map[string]any)
	} else {
		owned.Properties = map[string]any{}
	}
	return &owned
}

// General storage envelopes (change logs, legacy bodies and recovery records)
// contain property maps too. Route those fields to the same value owner without
// changing envelope counters, transaction metadata or other non-property data.
func canonicalizeDecodedPropertyContainers(value any) {
	transformRecoveryEntities(value, func(node *Node) *Node {
		if node != nil {
			normalizePropertyMapShapes(node.Properties)
		}
		return node
	}, func(edge *Edge) *Edge {
		if edge != nil {
			normalizePropertyMapShapes(edge.Properties)
		}
		return edge
	})
	if event, ok := value.(*ChangeEvent); ok {
		if event.Before != nil {
			normalizePropertyMapShapes(event.Before.Properties)
		}
		if event.After != nil {
			normalizePropertyMapShapes(event.After.Properties)
		}
	}
}
