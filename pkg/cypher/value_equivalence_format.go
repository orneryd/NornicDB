package cypher

import (
	"cmp"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Uncommon values keep their existing Go-syntax fallback. Walking containers
// explicitly makes even a single PathResult/vector interruptible; fmt alone
// buffers the entire value before calling its writer.
func appendEquivalenceFallback(out *strings.Builder, value interface{}, probe *pipelineCancellation) {
	out.WriteString(fmt.Sprintf("%T:", value))
	appendEquivalenceGoValue(out, reflect.ValueOf(value), probe, false)
}

func appendEquivalenceGoValue(out *strings.Builder, value reflect.Value, probe *pipelineCancellation, nested bool) {
	if probe != nil && !probe.check() {
		return
	}
	if !value.IsValid() {
		out.WriteString("<nil>")
		return
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			out.WriteString("interface {}(nil)")
			return
		}
		appendEquivalenceGoValue(out, value.Elem(), probe, nested)
		return
	}
	if value.CanInterface() {
		if _, custom := value.Interface().(fmt.GoStringer); custom {
			out.WriteString(fmt.Sprintf("%#v", value.Interface()))
			return
		}
	}
	switch value.Kind() {
	case reflect.String:
		out.WriteByte('"')
		text := value.String()
		for len(text) > 0 {
			if probe != nil && !probe.check() {
				return
			}
			_, size := utf8.DecodeRuneInString(text)
			quoted := strconv.Quote(text[:size])
			out.WriteString(quoted[1 : len(quoted)-1])
			text = text[size:]
		}
		out.WriteByte('"')
	case reflect.Struct:
		out.WriteString(value.Type().String())
		out.WriteByte('{')
		for index := 0; index < value.NumField(); index++ {
			if probe != nil && !probe.check() {
				return
			}
			if index > 0 {
				out.WriteString(", ")
			}
			out.WriteString(value.Type().Field(index).Name)
			out.WriteByte(':')
			appendEquivalenceGoValue(out, value.Field(index), probe, true)
			if probe != nil && probe.err != nil {
				return
			}
		}
		out.WriteByte('}')
	case reflect.Slice, reflect.Array:
		out.WriteString(value.Type().String())
		if value.Kind() == reflect.Slice && value.IsNil() {
			out.WriteString("(nil)")
			return
		}
		out.WriteByte('{')
		for index := 0; index < value.Len(); index++ {
			if index > 0 {
				out.WriteString(", ")
			}
			appendEquivalenceGoValue(out, value.Index(index), probe, true)
			if probe != nil && probe.err != nil {
				return
			}
		}
		out.WriteByte('}')
	case reflect.Map:
		out.WriteString(value.Type().String())
		if value.IsNil() {
			out.WriteString("(nil)")
			return
		}
		type entry struct {
			key      string
			original reflect.Value
			value    reflect.Value
		}
		entries := make([]entry, 0, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			var key strings.Builder
			appendEquivalenceGoValue(&key, iterator.Key(), probe, true)
			if probe != nil && probe.err != nil {
				return
			}
			entries = append(entries, entry{key.String(), iterator.Key(), iterator.Value()})
		}
		less := func(i, j int) bool {
			return compareEquivalenceGoMapKeys(entries[i].original, entries[j].original, probe) < 0
		}
		if probe == nil {
			sort.Slice(entries, less)
		} else if !probe.sort(entries, less, false) {
			return
		}
		out.WriteByte('{')
		for index, entry := range entries {
			if index > 0 {
				out.WriteString(", ")
			}
			if !appendPipelineString(out, entry.key, probe) {
				return
			}
			out.WriteByte(':')
			appendEquivalenceGoValue(out, entry.value, probe, true)
			if probe != nil && probe.err != nil {
				return
			}
		}
		out.WriteByte('}')
	case reflect.Pointer:
		if !nested && !value.IsNil() {
			switch value.Elem().Kind() {
			case reflect.Struct, reflect.Array, reflect.Slice, reflect.Map:
				out.WriteByte('&')
				appendEquivalenceGoValue(out, value.Elem(), probe, true)
				return
			}
		}
		out.WriteByte('(')
		out.WriteString(value.Type().String())
		out.WriteByte(')')
		if value.IsNil() {
			out.WriteString("(nil)")
		} else {
			out.WriteString("(0x")
			out.WriteString(strconv.FormatUint(uint64(value.Pointer()), 16))
			out.WriteByte(')')
		}
	default:
		if value.CanInterface() {
			out.WriteString(fmt.Sprintf("%#v", value.Interface()))
			return
		}
		// Unexported fields cannot be passed to fmt through Interface.
		switch value.Kind() {
		case reflect.Bool:
			out.WriteString(strconv.FormatBool(value.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			out.WriteString(strconv.FormatInt(value.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			out.WriteString("0x")
			out.WriteString(strconv.FormatUint(value.Uint(), 16))
		case reflect.Float32, reflect.Float64:
			out.WriteString(strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits()))
		case reflect.Complex64, reflect.Complex128:
			out.WriteString(fmt.Sprintf("%#v", value.Complex()))
		case reflect.Chan, reflect.Func, reflect.UnsafePointer:
			out.WriteString(fmt.Sprintf("(%s)(0x%x)", value.Type(), value.Pointer()))
		}
	}
}

func compareEquivalenceGoMapKeys(left, right reflect.Value, probe *pipelineCancellation) int {
	if probe != nil && !probe.check() {
		return 0
	}
	if left.Type() != right.Type() {
		return strings.Compare(left.Type().String(), right.Type().String())
	}
	switch left.Kind() {
	case reflect.Interface:
		if left.IsNil() {
			if right.IsNil() {
				return 0
			}
			return -1
		}
		if right.IsNil() {
			return 1
		}
		return compareEquivalenceGoMapKeys(left.Elem(), right.Elem(), probe)
	case reflect.String:
		return comparePipelineStrings(left.String(), right.String(), probe)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(left.Int(), right.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return cmp.Compare(left.Uint(), right.Uint())
	case reflect.Float32, reflect.Float64:
		return cmp.Compare(left.Float(), right.Float())
	case reflect.Complex64, reflect.Complex128:
		a, b := left.Complex(), right.Complex()
		return cmp.Or(cmp.Compare(real(a), real(b)), cmp.Compare(imag(a), imag(b)))
	case reflect.Bool:
		if left.Bool() == right.Bool() {
			return 0
		}
		if !left.Bool() {
			return -1
		}
		return 1
	case reflect.Pointer, reflect.Chan, reflect.UnsafePointer:
		return cmp.Compare(left.Pointer(), right.Pointer())
	case reflect.Array:
		for index := 0; index < left.Len(); index++ {
			order := compareEquivalenceGoMapKeys(left.Index(index), right.Index(index), probe)
			if order != 0 || probe != nil && probe.err != nil {
				return order
			}
		}
	case reflect.Struct:
		for index := 0; index < left.NumField(); index++ {
			order := compareEquivalenceGoMapKeys(left.Field(index), right.Field(index), probe)
			if order != 0 || probe != nil && probe.err != nil {
				return order
			}
		}
	}
	return 0
}
