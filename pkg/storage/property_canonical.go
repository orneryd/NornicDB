package storage

import (
	"math"
	"reflect"
	"slices"
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
		out := make([]any, len(v))
		var kind reflect.Type
		homogeneous := len(v) > 0
		for i, item := range v {
			out[i] = canonicalPropertyValue(item)
			if i == 0 {
				switch out[i].(type) {
				case int64, uint64, float32, float64, string, bool:
					kind = reflect.TypeOf(out[i])
				default:
					homogeneous = false
				}
			} else if reflect.TypeOf(out[i]) != kind {
				homogeneous = false
			}
		}
		if !homogeneous {
			return out
		}
		// This is the sole list-shape decision, after recursively applying
		// the scalar contract to every element, irrespective of input width.
		typed := reflect.MakeSlice(reflect.SliceOf(kind), len(out), len(out))
		for i, item := range out {
			typed.Index(i).Set(reflect.ValueOf(item))
		}
		return typed.Interface()
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
