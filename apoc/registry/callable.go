package registry

// Author: Codex NOR-184 lane. Admission and result decoding for registered Go functions.

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// ErrUnsupportedSignature identifies a handler that cannot answer a scalar call.
var ErrUnsupportedSignature = errors.New("unsupported function signature")

// ErrNonScalarResult identifies multiple results without an error result.
// Such declarations may be described as metadata, but cannot be invoked.
var ErrNonScalarResult = fmt.Errorf("%w: multiple non-error results", ErrUnsupportedSignature)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// Callable is an admitted Go function with a checked result protocol. Its raw
// function and reflection results are private: all invocations converge on
// InvokeValues, which always returns both the value and the error.
// Construct one with Compile; a zero value is not callable.
type Callable struct {
	function  reflect.Value
	signature reflect.Type
	symbol    string
	outputs   int
	hasError  bool
}

// Compile admits a non-nil function returning no result, one value, one error,
// or (value, error). It rejects an error in any other position before a function
// can be published. An already admitted Callable retains its identity.
func Compile(function interface{}) (*Callable, error) {
	if handler, ok := function.(*Callable); ok {
		if handler == nil || handler.signature == nil {
			return nil, fmt.Errorf("%w: nil handler", ErrUnsupportedSignature)
		}
		return handler, nil
	}
	v := reflect.ValueOf(function)
	if !v.IsValid() {
		return nil, fmt.Errorf("%w: nil handler", ErrUnsupportedSignature)
	}
	if v.Kind() != reflect.Func || v.IsNil() {
		return nil, fmt.Errorf("%w: not a non-nil function", ErrUnsupportedSignature)
	}
	t := v.Type()
	n := t.NumOut()
	for i := 0; i < n; i++ {
		if t.Out(i).Implements(errorType) && (i != n-1 || n > 2) {
			return nil, fmt.Errorf("%w: error must be the only result or follow one value", ErrUnsupportedSignature)
		}
	}
	if n > 2 || n == 2 && !t.Out(1).Implements(errorType) {
		return nil, ErrNonScalarResult
	}
	// Admission owns the Go result/error protocol. A Go value can be a
	// lifecycle capability; query representability belongs to its consumer.
	h := &Callable{function: v, signature: t, outputs: n, hasError: n > 0 && t.Out(n-1).Implements(errorType)}
	if symbol := runtime.FuncForPC(v.Pointer()); symbol != nil {
		h.symbol = symbol.Name()
	}
	return h, nil
}

// DescribeNonScalar retains an existing non-scalar declaration's metadata,
// without retaining its invocable function. No error result is permitted.
// Call and InvokeValues always reject this metadata-only descriptor.
func DescribeNonScalar(function interface{}) (*Callable, error) {
	v := reflect.ValueOf(function)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() || v.Type().NumOut() < 2 {
		return nil, ErrUnsupportedSignature
	}
	t := v.Type()
	for i := 0; i < t.NumOut(); i++ {
		if t.Out(i).Implements(errorType) {
			return nil, fmt.Errorf("%w: an error result cannot be described without its checked invoker", ErrUnsupportedSignature)
		}
	}
	h := &Callable{signature: t}
	if symbol := runtime.FuncForPC(v.Pointer()); symbol != nil {
		h.symbol = symbol.Name()
	}
	return h, nil
}

// Signature returns immutable type metadata, never the invocable function.
func (h *Callable) Signature() reflect.Type {
	if h == nil {
		return nil
	}
	return h.signature
}

// Symbol returns the registered function's Go symbol for implementation identity.
func (h *Callable) Symbol() string {
	if h == nil {
		return ""
	}
	return h.symbol
}

// Call binds supplied arguments by their declared Go types, then uses the sole
// checked result decoder. It does not inject or change a caller's context.
//
//	handler, err := registry.Compile(math.Lcm)
//	if err != nil { return err }
//	value, err := handler.Call([]interface{}{int64(12), int64(18)})
func (h *Callable) Call(args []interface{}) (interface{}, error) {
	return h.CallWithPolicy(args, nil, GoArguments)
}

// CallWithPolicy binds a consumer's arguments, including any prefix already
// bound by that consumer, and invokes through the same checked result owner.
// Prefix binding does not inject a context or manufacture a caller lifetime.
func (h *Callable) CallWithPolicy(args []interface{}, prefix []reflect.Value, policy ArgumentPolicy) (interface{}, error) {
	if h == nil || !h.function.IsValid() {
		return nil, ErrUnsupportedSignature
	}
	plan, admitted := h.argumentPlan(policy)
	if !admitted {
		return nil, ErrUnsupportedSignature
	}
	count := len(args) + len(prefix)
	if !h.acceptsArgumentCount(count) {
		return nil, fmt.Errorf("%w: expected %d arguments, got %d", ErrUnsupportedSignature, h.signature.NumIn(), count)
	}
	values := make([]reflect.Value, count)
	copy(values, prefix)
	for i, arg := range args {
		at := i + len(prefix)
		target := h.argumentType(at)
		binding := declaredArgument
		if at < len(plan) {
			binding = plan[at]
		}
		value, ok := bindPolicyArgument(arg, target, binding)
		if !ok {
			return nil, fmt.Errorf("%w: argument %d cannot be converted to %s", ErrUnsupportedSignature, i+1, target)
		}
		values[at] = value
	}
	return h.InvokeValues(values)
}

// InvokeValues consumes already bound arguments, including any context bound by
// the existing caller. It is the only registered-function reflect.Call owner.
// It never exposes raw reflection results that a consumer could truncate.
func (h *Callable) InvokeValues(values []reflect.Value) (interface{}, error) {
	if h == nil || !h.function.IsValid() {
		return nil, ErrUnsupportedSignature
	}
	if !h.acceptsArgumentCount(len(values)) {
		return nil, ErrUnsupportedSignature
	}
	for i, value := range values {
		target := h.argumentType(i)
		if !value.IsValid() || !value.Type().AssignableTo(target) {
			return nil, fmt.Errorf("%w: argument %d is not bound to %s", ErrUnsupportedSignature, i+1, target)
		}
	}
	results := h.function.Call(values)
	if h.outputs == 0 {
		return nil, nil
	}
	if h.hasError {
		last := results[h.outputs-1]
		nilError := false
		switch last.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			nilError = last.IsNil()
		}
		if !nilError {
			return nil, last.Interface().(error)
		}
		if h.outputs == 1 {
			return nil, nil
		}
	}
	return results[0].Interface(), nil
}

func (h *Callable) acceptsArgumentCount(count int) bool {
	t := h.signature
	return !t.IsVariadic() && count == t.NumIn() || t.IsVariadic() && count >= t.NumIn()-1
}

func (h *Callable) argumentType(index int) reflect.Type {
	t := h.signature
	if t.IsVariadic() && index >= t.NumIn()-1 {
		return t.In(t.NumIn() - 1).Elem()
	}
	return t.In(index)
}

// BindArgument uses the existing typed-plugin checked conversion policy.
func BindArgument(value reflect.Value, target reflect.Type) (reflect.Value, bool) {
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
			return BindArgument(reflect.Value{}, target)
		}
		return BindArgument(value.Elem(), target)
	}
	out := reflect.New(target).Elem()
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number, numeric := valueconvert.ReadReflectNumber(value)
		if !numeric {
			return reflect.Value{}, false
		}
		n, ok := number.Int64Bits(target.Bits(), valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat)
		if !ok {
			return reflect.Value{}, false
		}
		out.SetInt(n)
		return out, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number, numeric := valueconvert.ReadReflectNumber(value)
		if !numeric {
			return reflect.Value{}, false
		}
		n, ok := number.Uint64Bits(target.Bits(), valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat)
		if !ok {
			return reflect.Value{}, false
		}
		out.SetUint(n)
		return out, true
	case reflect.Float32, reflect.Float64:
		number, numeric := valueconvert.ReadReflectNumber(value)
		if !numeric {
			return reflect.Value{}, false
		}
		n := number.Float64()
		if target.Kind() == reflect.Float32 {
			narrowed, ok := number.Float32()
			if !ok {
				return reflect.Value{}, false
			}
			n = float64(narrowed)
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
		items, isList := valueconvert.ListValue(value.Interface())
		if !isList {
			return reflect.Value{}, false
		}
		if value.Kind() == reflect.Slice && value.IsNil() {
			return reflect.Zero(target), true
		}
		list := reflect.MakeSlice(target, len(items), len(items))
		for i := range items {
			item, ok := BindArgument(reflect.ValueOf(items[i]), target.Elem())
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
			k, ok := BindArgument(iter.Key(), target.Key())
			if !ok {
				return reflect.Value{}, false
			}
			v, ok := BindArgument(iter.Value(), target.Elem())
			if !ok {
				return reflect.Value{}, false
			}
			result.SetMapIndex(k, v)
		}
		return result, true
	}
	return reflect.Value{}, false
}
