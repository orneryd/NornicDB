package cypher

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
)

var pluginContextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var pluginErrorType = reflect.TypeOf((*error)(nil)).Elem()

// invokeTypedPlugin is the checked fallback for Go signatures outside the
// allocation-free common cases. Conversion follows declared Go argument types;
// it never truncates fractional integers or wraps an out-of-range integer.
func invokeTypedPlugin(ctx context.Context, handler interface{}, args []interface{}) (interface{}, error) {
	fn := reflect.ValueOf(handler)
	if !fn.IsValid() || fn.Kind() != reflect.Func || fn.IsNil() {
		return nil, errUnsupportedPluginSignature
	}
	typ := fn.Type()
	outputs := typ.NumOut()
	if outputs > 2 || outputs == 2 && !typ.Out(1).Implements(pluginErrorType) {
		return nil, errUnsupportedPluginSignature
	}
	for i := 0; i < outputs; i++ {
		if kind := typ.Out(i).Kind(); kind == reflect.Func || kind == reflect.Chan || kind == reflect.UnsafePointer {
			return nil, errUnsupportedPluginSignature
		}
	}
	offset := 0
	if typ.NumIn() > 0 && typ.In(0) == pluginContextType {
		offset = 1
	}
	count := typ.NumIn() - offset
	if !typ.IsVariadic() && len(args) != count || typ.IsVariadic() && len(args) < count-1 {
		return nil, fmt.Errorf("%w: expected %d arguments, got %d", errUnsupportedPluginSignature, count, len(args))
	}
	values := make([]reflect.Value, len(args)+offset)
	if offset == 1 {
		values[0] = reflect.ValueOf(ctx)
	}
	for i, arg := range args {
		at := i + offset
		var target reflect.Type
		if typ.IsVariadic() && at >= typ.NumIn()-1 {
			target = typ.In(typ.NumIn() - 1).Elem()
		} else {
			target = typ.In(at)
		}
		converted, ok := convertPluginArgument(reflect.ValueOf(arg), target)
		if !ok {
			return nil, fmt.Errorf("%w: argument %d cannot be converted to %s", errUnsupportedPluginSignature, i+1, target)
		}
		values[at] = converted
	}
	result := fn.Call(values)
	if outputs == 0 {
		return nil, nil
	}
	last := result[outputs-1]
	if typ.Out(outputs - 1).Implements(pluginErrorType) {
		nilError := false
		switch last.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			nilError = last.IsNil()
		}
		if !nilError {
			return nil, last.Interface().(error)
		}
		if outputs == 1 {
			return nil, nil
		}
	}
	return result[0].Interface(), nil
}

func convertPluginArgument(value reflect.Value, target reflect.Type) (reflect.Value, bool) {
	if !value.IsValid() {
		switch target.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
			return reflect.Zero(target), true
		}
		return reflect.Value{}, false
	}
	if value.Type().AssignableTo(target) {
		return value, true
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return convertPluginArgument(reflect.Value{}, target)
		}
		return convertPluginArgument(value.Elem(), target)
	}
	out := reflect.New(target).Elem()
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var n int64
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n = value.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if value.Uint() > math.MaxInt64 {
				return reflect.Value{}, false
			}
			n = int64(value.Uint())
		case reflect.Float32, reflect.Float64:
			f := value.Float()
			if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < -math.Exp2(63) || f >= math.Exp2(63) {
				return reflect.Value{}, false
			}
			n = int64(f)
		default:
			return reflect.Value{}, false
		}
		if out.OverflowInt(n) {
			return reflect.Value{}, false
		}
		out.SetInt(n)
		return out, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var n uint64
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if value.Int() < 0 {
				return reflect.Value{}, false
			}
			n = uint64(value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			n = value.Uint()
		case reflect.Float32, reflect.Float64:
			f := value.Float()
			if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < 0 || f >= math.Exp2(64) {
				return reflect.Value{}, false
			}
			n = uint64(f)
		default:
			return reflect.Value{}, false
		}
		if out.OverflowUint(n) {
			return reflect.Value{}, false
		}
		out.SetUint(n)
		return out, true
	case reflect.Float32, reflect.Float64:
		var n float64
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n = float64(value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			n = float64(value.Uint())
		case reflect.Float32, reflect.Float64:
			n = value.Float()
		default:
			return reflect.Value{}, false
		}
		if out.OverflowFloat(n) {
			return reflect.Value{}, false
		}
		out.SetFloat(n)
		return out, true
	case reflect.String:
		if value.Kind() == reflect.String {
			out.SetString(value.String())
			return out, true
		}
	case reflect.Bool:
		if value.Kind() == reflect.Bool {
			out.SetBool(value.Bool())
			return out, true
		}
	case reflect.Slice:
		if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
			return reflect.Value{}, false
		}
		if value.Kind() == reflect.Slice && value.IsNil() {
			return reflect.Zero(target), true
		}
		list := reflect.MakeSlice(target, value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			item, ok := convertPluginArgument(value.Index(i), target.Elem())
			if !ok {
				return reflect.Value{}, false
			}
			list.Index(i).Set(item)
		}
		return list, true
	case reflect.Map:
		if value.Kind() != reflect.Map {
			return reflect.Value{}, false
		}
		if value.IsNil() {
			return reflect.Zero(target), true
		}
		result := reflect.MakeMapWithSize(target, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			k, ok := convertPluginArgument(iter.Key(), target.Key())
			if !ok {
				return reflect.Value{}, false
			}
			v, ok := convertPluginArgument(iter.Value(), target.Elem())
			if !ok {
				return reflect.Value{}, false
			}
			result.SetMapIndex(k, v)
		}
		return result, true
	}
	return reflect.Value{}, false
}

// Context-taking plugin calls need the row evaluator to retain the statement's
// context, just as clocks and subqueries do. This is checked while preparing
// an expression; ordinary handlers do not force a context-bearing row copy.
func containsContextPluginCall(expression string) bool {
	if PluginFunctionLookup == nil || !strings.Contains(expression, ".") || !strings.Contains(expression, "(") {
		return false
	}
	found := false
	_ = forEachFunctionCall(expression, func(name, _ string) error {
		if handler, ok := pluginFunctionHandler(name); ok && handler != nil {
			if contract, declared := handler.(PluginFunctionContract); declared {
				handler = contract.Handler
			}
			typ := reflect.TypeOf(handler)
			if typ.Kind() == reflect.Func && typ.NumIn() > 0 && typ.In(0) == pluginContextType {
				found = true
			}
		}
		return nil
	})
	return found
}
