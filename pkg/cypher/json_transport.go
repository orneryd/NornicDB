package cypher

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// JSONScalar converts native scalar values to their native HTTP JSON forms.
// Entity identities and protocol-specific graph metadata belong to the caller.
func JSONScalar(value interface{}) (interface{}, bool) {
	switch v := value.(type) {
	case []byte:
		out := make([]int8, len(v))
		for i, b := range v {
			out[i] = int8(b)
		}
		return out, true
	case float32:
		return JSONFloat(float64(v), 32), true
	case float64:
		return JSONFloat(v, 64), true
	case CypherDate, CypherLocalTime, CypherTime, CypherLocalDateTime, CypherDateTime:
		return v.(fmt.Stringer).String(), true
	case *CypherDuration:
		if v == nil {
			return nil, true
		}
		return v.String(), true
	case CypherDuration:
		return v.String(), true
	case CypherUUID:
		return v.String(), true
	}
	return nil, false
}

// JSONFloat is the native JSON spelling of a floating point number, including
// the string representation of non-finite numbers accepted by native Cypher.
func JSONFloat(value float64, bits int) interface{} {
	text := FormatFloat(value, bits)
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return text
	}
	return json.Number(text)
}

// ErrJSONValueTooLarge means a value cannot fit the caller's JSON budget.
var ErrJSONValueTooLarge = errors.New("native JSON value exceeds result byte limit")

// JSONTransportValue recursively applies the native scalar codec, preserving
// selected entity IDs/properties, maps and lists. The budget is charged before
// allocating converted containers or serializing large strings/byte arrays.
// It bounds conversion itself as well as the returned JSON representation.
func JSONTransportValue(value interface{}, maxBytes int) (interface{}, error) {
	remaining := maxBytes
	return jsonTransportValue(value, &remaining)
}

func jsonTransportValue(value interface{}, remaining *int) (interface{}, error) {
	charge := func(n int) error {
		if n > *remaining {
			return ErrJSONValueTooLarge
		}
		*remaining -= n
		return nil
	}
	if *remaining <= 0 {
		return nil, ErrJSONValueTooLarge
	}
	if text, ok := value.(string); ok && len(text) > *remaining {
		return nil, ErrJSONValueTooLarge
	}
	if bytes, ok := value.([]byte); ok && len(bytes) > *remaining/2 {
		return nil, ErrJSONValueTooLarge
	}
	if converted, scalar := JSONScalar(value); scalar {
		value = converted
	}
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return jsonTransportValue(nil, remaining)
		}
		return jsonTransportValue(map[string]interface{}{"id": v.ID, "labels": v.Labels, "properties": v.Properties}, remaining)
	case *storage.Edge:
		if v == nil {
			return jsonTransportValue(nil, remaining)
		}
		return jsonTransportValue(map[string]interface{}{"id": v.ID, "startNode": v.StartNode, "endNode": v.EndNode, "type": v.Type, "properties": v.Properties}, remaining)
	case map[string]interface{}:
		v = storage.PublicRemoteMap(v)
		if err := charge(2); err != nil {
			return nil, err
		}
		out := make(map[string]interface{})
		for key, item := range v {
			if len(key) > *remaining {
				return nil, ErrJSONValueTooLarge
			}
			encoded, _ := json.Marshal(key)
			if err := charge(len(encoded) + 2); err != nil {
				return nil, err
			}
			converted, err := jsonTransportValue(item, remaining)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	}
	if value != nil {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return jsonTransportValue(nil, remaining)
			}
			return jsonTransportValue(v.Elem().Interface(), remaining)
		case reflect.Map:
			if v.Type().Key().Kind() == reflect.String {
				if err := charge(2); err != nil {
					return nil, err
				}
				out := make(map[string]interface{})
				iter := v.MapRange()
				for iter.Next() {
					key := iter.Key().String()
					if len(key) > *remaining {
						return nil, ErrJSONValueTooLarge
					}
					encoded, _ := json.Marshal(key)
					if err := charge(len(encoded) + 2); err != nil {
						return nil, err
					}
					item, err := jsonTransportValue(iter.Value().Interface(), remaining)
					if err != nil {
						return nil, err
					}
					out[key] = item
				}
				return out, nil
			}
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice && v.IsNil() {
				return jsonTransportValue(nil, remaining)
			}
			if err := charge(2); err != nil {
				return nil, err
			}
			out := make([]interface{}, 0)
			for i := 0; i < v.Len(); i++ {
				if err := charge(1); err != nil {
					return nil, err
				}
				item, err := jsonTransportValue(v.Index(i).Interface(), remaining)
				if err != nil {
					return nil, err
				}
				out = append(out, item)
			}
			return out, nil
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := charge(len(encoded)); err != nil {
		return nil, err
	}
	return value, nil
}
