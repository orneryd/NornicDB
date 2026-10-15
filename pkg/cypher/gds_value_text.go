package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// gdsJavaValueText owns GDS error value rendering. Procedure values use Java
// collection text; property values retain Neo4j's String/Boolean/array wrappers.
// Context supplies map-literal order, including keys sharing a hash bucket.
func gdsJavaValueText(value interface{}, propertyValue bool, contexts ...context.Context) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		if propertyValue {
			return "String(" + strconv.Quote(typed) + ")"
		}
		return typed
	case bool:
		text := strconv.FormatBool(typed)
		if propertyValue {
			return "Boolean('" + text + "')"
		}
		return text
	case float64:
		return gdsDefaultText(typed, "")
	case float32:
		return gdsDefaultText(float64(typed), "")
	case map[string]interface{}:
		keys, literalOrder := gdsMapLiteralKeys(typed, contexts...)
		if !literalOrder {
			keys = appendSortedMapKeys(nil, typed)
		}
		keys = gdsNativeProjectionMapOrder(keys)
		parts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = key + "=" + gdsJavaValueText(typed[key], false, contexts...)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	if isIntegerProcedureValue(value) {
		return strconv.FormatInt(gdsInteger(value), 10)
	}
	if items, list := cypherListValue(value); list {
		prefix := ""
		if propertyValue {
			switch value.(type) {
			case []int64:
				prefix = "LongArray"
			case []float64:
				prefix = "DoubleArray"
			case []string:
				prefix = "StringArray"
			}
		}
		parts := make([]string, len(items))
		for i, item := range items {
			parts[i] = gdsJavaValueText(item, false, contexts...)
		}
		return prefix + "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(value)
}

// gdsMapLiteralKeys shares expression provenance with native projection.
// Callers use sorted keys when a map has no recorded literal order.
func gdsMapLiteralKeys(value map[string]interface{}, contexts ...context.Context) ([]string, bool) {
	if len(contexts) > 0 {
		keys := snapshotMapKeyOrders(contexts[0])[uintptr(reflect.ValueOf(value).UnsafePointer())]
		if len(keys) == len(value) && len(keys) > 0 {
			return keys, true
		}
	}
	return nil, false
}
