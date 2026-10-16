package cypher

import (
	"cmp"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Uncommon values keep their existing Go-syntax fallback. Walking containers
// explicitly makes even a single PathResult/vector interruptible; fmt alone
// buffers the entire value before calling its writer.
func appendEquivalenceFallback(out *strings.Builder, value interface{}, probe *pipelineCancellation) {
	if !appendPipelineString(out, fmt.Sprintf("%T:", value), probe) {
		return
	}
	appendEquivalenceGoValue(out, reflect.ValueOf(value), probe, false)
}

func appendEquivalenceGoValue(out *strings.Builder, value reflect.Value, probe *pipelineCancellation, nested bool) {
	appendPipelineFormatValue(out, value, probe, nested, true)
}

func appendPipelineDisplayValue(out *strings.Builder, value interface{}, probe *pipelineCancellation) {
	if probe == nil && !isNilPipelineFormatValue(reflect.ValueOf(value)) {
		out.WriteString(fmt.Sprint(value))
		return
	}
	appendPipelineFormatValue(out, reflect.ValueOf(value), probe, false, false)
}

func appendPipelineFormatValue(out *strings.Builder, value reflect.Value, probe *pipelineCancellation, nested, goSyntax bool) {
	if probe != nil && !probe.check() {
		return
	}
	if !value.IsValid() {
		if !appendPipelineString(out, "<nil>", probe) {
			return
		}
		return
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			if goSyntax {
				if !appendPipelineString(out, "interface {}(nil)", probe) {
					return
				}
			} else {
				if !appendPipelineString(out, "<nil>", probe) {
					return
				}
			}
			return
		}
		appendPipelineFormatValue(out, value.Elem(), probe, nested, goSyntax)
		return
	}
	nilValue := isNilPipelineFormatValue(value)
	if value.CanInterface() && !nilValue {
		if vector, ok := value.Interface().(CypherVector); ok && !goSyntax {
			appendPipelineString(out, vector.stringWithCancellation(probe), probe)
			return
		}
		if vector, ok := value.Interface().(*CypherVector); ok && !goSyntax && vector != nil {
			appendPipelineString(out, vector.stringWithCancellation(probe), probe)
			return
		}
		// User callbacks have no work-budget contract. Reject them before fmt
		// can invoke them; copying their result in chunks is already too late.
		if probe != nil && probe.ctx.Done() != nil && hasUnboundedPipelineFormatter(value.Interface(), goSyntax) {
			probe.err = localizedStatusError("Neo.ClientError.Statement.TypeError", "", localization.CypherCustomFormattingUnsupported(value.Type().String()))
			probe.remaining = 0
			recordExpressionFailure(probe.ctx, probe.err)
			return
		}
		if _, custom := value.Interface().(fmt.Formatter); custom {
			if goSyntax {
				appendPipelineString(out, fmt.Sprintf("%#v", value.Interface()), probe)
			} else {
				appendPipelineString(out, fmt.Sprint(value.Interface()), probe)
			}
			return
		}
		if custom, ok := value.Interface().(fmt.GoStringer); ok && goSyntax && !(value.Kind() == reflect.Pointer && value.IsNil()) {
			appendPipelineString(out, custom.GoString(), probe)
			return
		}
	}
	if !goSyntax && value.Kind() == reflect.Pointer && value.IsNil() {
		if !appendPipelineString(out, "<nil>", probe) {
			return
		}
		return
	}
	if !goSyntax && value.CanInterface() && !nilValue {
		if custom, ok := value.Interface().(fmt.Stringer); ok {
			appendPipelineString(out, custom.String(), probe)
			return
		}
		if custom, ok := value.Interface().(error); ok {
			appendPipelineString(out, custom.Error(), probe)
			return
		}
	}
	switch value.Kind() {
	case reflect.String:
		if !goSyntax {
			appendPipelineString(out, value.String(), probe)
			return
		}
		if !appendPipelineByte(out, '"', probe) {
			return
		}
		text := value.String()
		for len(text) > 0 {
			if probe != nil && !probe.check() {
				return
			}
			_, size := utf8.DecodeRuneInString(text)
			quoted := strconv.Quote(text[:size])
			if !appendPipelineString(out, quoted[1:len(quoted)-1], probe) {
				return
			}
			text = text[size:]
		}
		if !appendPipelineByte(out, '"', probe) {
			return
		}
	case reflect.Struct:
		if goSyntax {
			if !appendPipelineString(out, value.Type().String(), probe) {
				return
			}
		}
		if !appendPipelineByte(out, '{', probe) {
			return
		}
		for index := 0; index < value.NumField(); index++ {
			if probe != nil && !probe.check() {
				return
			}
			if index > 0 {
				if goSyntax {
					if !appendPipelineString(out, ", ", probe) {
						return
					}
				} else {
					if !appendPipelineByte(out, ' ', probe) {
						return
					}
				}
			}
			if goSyntax {
				if !appendPipelineString(out, value.Type().Field(index).Name, probe) {
					return
				}
				if !appendPipelineByte(out, ':', probe) {
					return
				}
			}
			appendPipelineFormatValue(out, value.Field(index), probe, true, goSyntax)
			if probe != nil && probe.err != nil {
				return
			}
		}
		if !appendPipelineByte(out, '}', probe) {
			return
		}
	case reflect.Slice, reflect.Array:
		if goSyntax {
			if !appendPipelineString(out, value.Type().String(), probe) {
				return
			}
		}
		if goSyntax && value.Kind() == reflect.Slice && value.IsNil() {
			if !appendPipelineString(out, "(nil)", probe) {
				return
			}
			return
		}
		if goSyntax {
			if !appendPipelineByte(out, '{', probe) {
				return
			}
		} else {
			if !appendPipelineByte(out, '[', probe) {
				return
			}
		}
		for index := 0; index < value.Len(); index++ {
			if index > 0 {
				if goSyntax {
					if !appendPipelineString(out, ", ", probe) {
						return
					}
				} else {
					if !appendPipelineByte(out, ' ', probe) {
						return
					}
				}
			}
			appendPipelineFormatValue(out, value.Index(index), probe, true, goSyntax)
			if probe != nil && probe.err != nil {
				return
			}
		}
		if goSyntax {
			if !appendPipelineByte(out, '}', probe) {
				return
			}
		} else {
			if !appendPipelineByte(out, ']', probe) {
				return
			}
		}
	case reflect.Map:
		if goSyntax {
			if !appendPipelineString(out, value.Type().String(), probe) {
				return
			}
		}
		if goSyntax && value.IsNil() {
			if !appendPipelineString(out, "(nil)", probe) {
				return
			}
			return
		}
		type entry struct {
			key      string
			original reflect.Value
			value    reflect.Value
		}
		entries := make([]entry, 0, pipelineInitialCapacity(value.Len(), probe))
		iterator := value.MapRange()
		for iterator.Next() {
			var key strings.Builder
			appendPipelineFormatValue(&key, iterator.Key(), probe, true, goSyntax)
			if probe != nil && probe.err != nil {
				return
			}
			entries = appendPipelineItem(entries, entry{key.String(), iterator.Key(), iterator.Value()}, probe)
			if probe != nil && probe.err != nil {
				return
			}

		}
		less := func(i, j int) bool {
			return compareEquivalenceGoMapKeys(entries[i].original, entries[j].original, probe) < 0
		}
		if probe == nil {
			sort.Slice(entries, less)
		} else if !probe.sort(entries, less, false) {
			return
		}
		if goSyntax {
			if !appendPipelineByte(out, '{', probe) {
				return
			}
		} else {
			if !appendPipelineString(out, "map[", probe) {
				return
			}
		}
		for index, entry := range entries {
			if index > 0 {
				if goSyntax {
					if !appendPipelineString(out, ", ", probe) {
						return
					}
				} else {
					if !appendPipelineByte(out, ' ', probe) {
						return
					}
				}
			}
			if !appendPipelineString(out, entry.key, probe) {
				return
			}
			if !appendPipelineByte(out, ':', probe) {
				return
			}
			appendPipelineFormatValue(out, entry.value, probe, true, goSyntax)
			if probe != nil && probe.err != nil {
				return
			}
		}
		if goSyntax {
			if !appendPipelineByte(out, '}', probe) {
				return
			}
		} else {
			if !appendPipelineByte(out, ']', probe) {
				return
			}
		}
	case reflect.Pointer:
		if !nested && !value.IsNil() {
			switch value.Elem().Kind() {
			case reflect.Struct, reflect.Array, reflect.Slice, reflect.Map:
				if !appendPipelineByte(out, '&', probe) {
					return
				}
				appendPipelineFormatValue(out, value.Elem(), probe, true, goSyntax)
				return
			}
		}
		if !goSyntax {
			// Nil display pointers returned before custom-method handling.
			if !appendPipelineString(out, "0x", probe) {
				return
			}
			if !appendPipelineString(out, strconv.FormatUint(uint64(value.Pointer()), 16), probe) {
				return
			}
			return
		}
		if !appendPipelineByte(out, '(', probe) {
			return
		}
		if !appendPipelineString(out, value.Type().String(), probe) {
			return
		}
		if !appendPipelineByte(out, ')', probe) {
			return
		}
		if value.IsNil() {
			if !appendPipelineString(out, "(nil)", probe) {
				return
			}
		} else {
			if !appendPipelineString(out, "(0x", probe) {
				return
			}
			if !appendPipelineString(out, strconv.FormatUint(uint64(value.Pointer()), 16), probe) {
				return
			}
			if !appendPipelineByte(out, ')', probe) {
				return
			}
		}
	default:
		if nilValue {
			if goSyntax {
				if !appendPipelineString(out, "("+value.Type().String()+")(nil)", probe) {
					return
				}
			} else {
				if !appendPipelineString(out, "<nil>", probe) {
					return
				}
			}
			return
		}
		if value.CanInterface() {
			if goSyntax {
				if !appendPipelineString(out, fmt.Sprintf("%#v", value.Interface()), probe) {
					return
				}
			} else {
				if !appendPipelineString(out, fmt.Sprint(value.Interface()), probe) {
					return
				}
			}
			return
		}
		// Unexported fields cannot be passed to fmt through Interface.
		switch value.Kind() {
		case reflect.Bool:
			if !appendPipelineString(out, strconv.FormatBool(value.Bool()), probe) {
				return
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !appendPipelineString(out, strconv.FormatInt(value.Int(), 10), probe) {
				return
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if goSyntax {
				if !appendPipelineString(out, "0x", probe) {
					return
				}
				if !appendPipelineString(out, strconv.FormatUint(value.Uint(), 16), probe) {
					return
				}
			} else {
				if !appendPipelineString(out, strconv.FormatUint(value.Uint(), 10), probe) {
					return
				}
			}
		case reflect.Float32, reflect.Float64:
			if !appendPipelineString(out, strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits()), probe) {
				return
			}
		case reflect.Complex64, reflect.Complex128:
			if !appendPipelineString(out, fmt.Sprintf("%#v", value.Complex()), probe) {
				return
			}
		case reflect.Chan, reflect.Func, reflect.UnsafePointer:
			if !appendPipelineString(out, fmt.Sprintf("(%s)(0x%x)", value.Type(), value.Pointer()), probe) {
				return
			}
		}
	}
}

func isNilPipelineFormatValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func hasUnboundedPipelineFormatter(value interface{}, goSyntax bool) bool {
	if isNilPipelineFormatValue(reflect.ValueOf(value)) {
		return false
	}
	// These concrete built-in values have bounded representations. A named
	// user type cannot enter this list merely by implementing the same methods.
	switch typed := value.(type) {
	case time.Time:
		zone, _ := typed.Zone()
		return len(zone) > 4096 || len(typed.Location().String()) > 4096
	case CypherDate, CypherTime, CypherLocalTime, CypherLocalDateTime,
		CypherPoint, CypherUUID, VectorCoordinateType, *CypherDuration,
		*CypherDate, *CypherTime, *CypherLocalTime, *CypherLocalDateTime, *CypherPoint, *CypherUUID:
		return false
	case CypherDateTime:
		return len(typed.ZoneID) > 4096
	case *time.Time:
		return typed != nil && hasUnboundedPipelineFormatter(*typed, goSyntax)
	case *CypherDateTime:
		return typed != nil && len(typed.ZoneID) > 4096
	case *CypherVector:
		return false // non-nil display values use the bounded vector reader above
	}
	if _, custom := value.(fmt.Formatter); custom {
		return true
	}
	if goSyntax {
		_, custom := value.(fmt.GoStringer)
		return custom
	}
	if _, custom := value.(fmt.Stringer); custom {
		return true
	}
	_, custom := value.(error)
	return custom
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
