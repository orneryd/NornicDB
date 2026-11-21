package convert

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ValueEquivalenceKey encodes ordinary collection values by type and exact
// numeric value. Nested members use the same rules and each NaN has its own key.
func ValueEquivalenceKey(value any) string {
	switch typed := value.(type) {
	case nil:
		return "N"
	case string:
		return "S" + typed
	case int64:
		return IntegerEquivalenceKey(typed)
	}
	var key strings.Builder
	if !AppendScalarEquivalenceKey(&key, value, false) {
		if list, ok := ListValue(value); ok {
			AppendListEquivalenceKey(&key, list, ValueEquivalenceKey)
		} else if fields, ok := StringKeyedMap(value); ok {
			AppendMapEquivalenceKey(&key, fields, ValueEquivalenceKey)
		} else {
			AppendTypedEquivalenceKey(&key, value)
		}
	}
	return key.String()
}

// AppendScalarEquivalenceKey handles booleans, bytes, and numbers. Pass
// distinguishFloat64 for a removal operation that preserves numeric types.
func AppendScalarEquivalenceKey(key *strings.Builder, value any, distinguishFloat64 bool) bool {
	switch typed := value.(type) {
	case bool:
		if typed {
			key.WriteString("Bt")
		} else {
			key.WriteString("Bf")
		}
		return true
	case []byte:
		key.WriteByte('X')
		key.Write(typed)
		return true
	}
	number, ok := ReadNumber(value)
	if !ok {
		return false
	}
	var buf [32]byte
	switch number.Kind() {
	case SignedNumber:
		signed, _ := number.Signed()
		key.Write(AppendIntegerEquivalenceKey(buf[:0], signed))
	case UnsignedNumber:
		unsigned, _ := number.Unsigned()
		key.Write(AppendUnsignedEquivalenceKey(buf[:0], unsigned))
	case FloatNumber:
		if _, typed := value.(float64); typed && distinguishFloat64 {
			key.WriteString("F64:")
		}
		floating, _ := number.Float()
		key.Write(AppendFloatEquivalenceKey(buf[:0], floating))
	}
	return true
}

// AppendListEquivalenceKey frames an ordered list. keyOf encodes each
// member using the caller's value contract, including any graph-specific types.
func AppendListEquivalenceKey(key *strings.Builder, list []any, keyOf func(any) string) {
	key.WriteByte('L')
	key.WriteString(strconv.Itoa(len(list)))
	for _, item := range list {
		AppendFramedEquivalenceKey(key, keyOf(item))
	}
}

// AppendMapEquivalenceKey frames string-keyed fields in key order.
func AppendMapEquivalenceKey(key *strings.Builder, fields map[string]any, keyOf func(any) string) {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	key.WriteByte('M')
	key.WriteString(strconv.Itoa(len(fields)))
	for _, name := range names {
		AppendFramedEquivalenceKey(key, name)
		AppendFramedEquivalenceKey(key, keyOf(fields[name]))
	}
}

// AppendFramedEquivalenceKey prefixes a nested key with its byte length.
func AppendFramedEquivalenceKey(key *strings.Builder, nested string) {
	key.WriteByte('|')
	key.WriteString(strconv.Itoa(len(nested)))
	key.WriteByte(':')
	key.WriteString(nested)
}

// AppendTypedEquivalenceKey formats an opaque value with its type.
func AppendTypedEquivalenceKey(key *strings.Builder, value any) {
	key.WriteByte('T')
	key.WriteString(fmt.Sprintf("%T:%#v", value, value))
}

// StringKeyedMap reads boxed maps whose keys are all strings.
func StringKeyedMap(value any) (map[string]any, bool) {
	switch fields := value.(type) {
	case map[string]any:
		return fields, true
	case map[any]any:
		out := make(map[string]any, len(fields))
		for k, v := range fields {
			name, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[name] = v
		}
		return out, true
	default:
		return nil, false
	}
}
