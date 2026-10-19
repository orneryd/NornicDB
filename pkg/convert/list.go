package convert

import "reflect"

// ListValue reads every runtime slice or array as a Cypher list. Boxed lists
// are borrowed; typed elements use the engine's established int/float coercions.
// The caller owns any result it mutates and recursively reads nested lists here.
func ListValue(v interface{}) ([]interface{}, bool) {
	switch s := v.(type) {
	case []interface{}:
		return s, true
	case []map[string]interface{}:
		out := make([]interface{}, len(s))
		for i, m := range s {
			out[i] = m
		}
		return out, true
	case []string:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	case []int:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = int64(s[i])
		}
		return out, true
	case []int64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	case []float64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	case []float32:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = float64(s[i])
		}
		return out, true
	case []bool:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	}
	// Some graph scalars use a named slice as their transport carrier. They
	// explicitly retain scalar identity at this shared conversion boundary.
	if _, opaque := v.(interface{ CypherOpaqueSlice() }); opaque {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil, false
	}
	out := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}
