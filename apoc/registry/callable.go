package registry

// Author: Codex NOR-184 lane. Admission and result decoding for registered Go functions.

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// ErrUnsupportedSignature identifies a handler that cannot answer a scalar call.
var ErrUnsupportedSignature = errors.New("unsupported function signature")

// ErrNonScalarResult identifies multiple results without an error result.
// Such declarations may be described as metadata, but cannot be invoked.
var ErrNonScalarResult = fmt.Errorf("%w: multiple non-error results", ErrUnsupportedSignature)

// ErrNotInvoked identifies a call the admitted owner refused before entering the
// implementation: a metadata-only declaration, an argument count the signature
// does not accept, an argument the consumer's policy cannot bind, or a policy
// that cannot express the signature. It is an ErrUnsupportedSignature, and it
// is the one rejection a dispatch owner may not report as an opaque callback:
// the implementation never ran.
//
// It is a cause, not a phase: an implementation can return the rejection of a
// nested call it made, so errors.Is(err, ErrNotInvoked) classifies an error
// chain and never proves that this call was refused. Ask InvocationStarted for
// this call's phase.
var ErrNotInvoked = fmt.Errorf("%w: the implementation was not invoked", ErrUnsupportedSignature)

// ErrInvalidReadObservation identifies an admission whose declared read
// observation is not a declared policy. Requests are validated through the same
// owner the function registry uses (queryprofile.ReadObservation.Valid).
var ErrInvalidReadObservation = errors.New("invalid read observation")

// ErrConflictingReadObservation identifies an already admitted Callable
// recompiled under a different read observation. An admission keeps the policy
// it was admitted with, so a caller that means to declare another policy must
// re-admit the implementation instead of retagging the admitted one.
var ErrConflictingReadObservation = errors.New("conflicting read observation")

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// invocationError records the phase of this call independently of its error
// cause. Only the invocation boundary can create this sealed receipt. A nested
// call's rejection can remain in the cause chain without describing this call.
type invocationError struct {
	cause   error
	started bool
}

func (e *invocationError) Error() string { return e.cause.Error() }
func (e *invocationError) Unwrap() error { return e.cause }

// notInvoked returns an outermost receipt for a pre-entry rejection.
func notInvoked(cause error) error { return &invocationError{cause: cause} }

// enteredResult prevents an implementation from returning a nested rejection
// receipt as this call's phase. Ordinary errors retain their identity and add
// no allocation.
func enteredResult(cause error) error {
	if receipt, ok := cause.(*invocationError); ok && !receipt.started {
		return &invocationError{cause: cause, started: true}
	}
	return cause
}

// InvocationStarted reports whether one call entered its implementation. Pass
// the error returned directly by that call: nil and ordinary implementation
// errors mean it ran; the boundary's pre-entry rejection means it did not.
// errors.Is(err, ErrNotInvoked) classifies a cause and may instead describe a
// nested call. It cannot establish this call's phase.
func InvocationStarted(err error) bool {
	if receipt, ok := err.(*invocationError); ok {
		return receipt.started
	}
	return true
}

// ReadObservation is the read-observation policy an admission declares for the
// database access its implementation performs. It is the same canonical policy
// the Cypher function registry names, so one policy value means the same thing
// at every registration owner.
type ReadObservation = queryprofile.ReadObservation

const (
	// OpaqueReads declares a callback whose complete database access the
	// caller's reader observations cannot see. A custom implementation, an
	// unreviewed bundled callback and a storage-backed adapter all stay opaque
	// until they report their own reads.
	OpaqueReads = queryprofile.OpaqueReads
	// CanonicalReads declares a reviewed implementation whose reads are
	// complete without the opaque-callback hook: it reads no database state, or
	// it reports every read through the canonical observation owners.
	CanonicalReads = queryprofile.CanonicalReads
)

// Callable is an admitted Go function with a checked result protocol and the
// read-observation policy it was admitted with. Its raw function, reflection
// results and policy are private: all invocations converge on InvokeValues,
// which always returns both the value and the error.
// Construct one with Compile; a zero value is not callable.
type Callable struct {
	function  reflect.Value
	signature reflect.Type
	symbol    string
	outputs   int
	hasError  bool
	reads     ReadObservation
}

// Compile admits a non-nil function returning no result, one value, one error,
// or (value, error) under the caller's read-observation policy. It rejects an
// error in any other position before a function can be published, and it
// rejects a policy that isn't a declared one.
//
// An already admitted Callable retains its identity when the requested policy
// is the one it was admitted with. Redeclaring it under another policy is a
// conflict, not a retag: the policy and the implementation were reviewed
// together.
func Compile(function interface{}, observation ReadObservation) (*Callable, error) {
	if !observation.Valid() {
		return nil, fmt.Errorf("%w: %d is not a declared policy", ErrInvalidReadObservation, observation)
	}
	if handler, ok := function.(*Callable); ok {
		if handler == nil || handler.signature == nil {
			return nil, fmt.Errorf("%w: nil handler", ErrUnsupportedSignature)
		}
		if handler.reads != observation {
			return nil, fmt.Errorf("%w: admitted as %d, redeclared as %d", ErrConflictingReadObservation, handler.reads, observation)
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
	h := &Callable{function: v, signature: t, outputs: n, hasError: n > 0 && t.Out(n-1).Implements(errorType), reads: observation}
	if symbol := runtime.FuncForPC(v.Pointer()); symbol != nil {
		h.symbol = symbol.Name()
	}
	return h, nil
}

// AdmissionObservation returns the read observation a registrar must declare
// for function. An already admitted Callable keeps the policy it was admitted
// with; any other value is a custom or unreviewed implementation whose database
// access this owner cannot establish, so it is opaque.
func AdmissionObservation(function interface{}) ReadObservation {
	if admitted, ok := function.(*Callable); ok && admitted != nil {
		return admitted.reads
	}
	return OpaqueReads
}

// DescribeNonScalar retains an existing non-scalar declaration's metadata,
// without retaining its invocable function. No error result is permitted.
// The descriptor is opaque metadata: it declares no reviewed read observation
// and Call, InvokeValues and CallWithPolicy always reject it.
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
	h := &Callable{signature: t, reads: OpaqueReads}
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

// ReadObservation returns the immutable read-observation policy this function
// was admitted with. It is the only policy a consumer may act on: a dispatch
// owner that reported a different one would be retagging an admitted handler.
// A metadata-only descriptor reports OpaqueReads and stays noninvocable.
func (h *Callable) ReadObservation() ReadObservation {
	if h == nil {
		return OpaqueReads
	}
	return h.reads
}

// Call binds supplied arguments by their declared Go types, then uses the sole
// checked result decoder. It does not inject or change a caller's context.
//
//	handler, err := registry.Compile(math.Lcm, registry.CanonicalReads)
//	if err != nil { return err }
//	value, err := handler.Call([]interface{}{int64(12), int64(18)})
func (h *Callable) Call(args []interface{}) (interface{}, error) {
	return h.CallWithPolicy(args, nil, GoArguments)
}

// CallWithPolicy binds a consumer's arguments, including any prefix already
// bound by that consumer, and invokes through the same checked result owner.
// Prefix binding does not inject a context or manufacture a caller lifetime.
// A call it refuses before invoking returns its rejection receipt outermost,
// which InvocationStarted reads as this call's phase.
func (h *Callable) CallWithPolicy(args []interface{}, prefix []reflect.Value, policy ArgumentPolicy) (interface{}, error) {
	if h == nil || !h.function.IsValid() {
		return nil, notInvoked(ErrNotInvoked)
	}
	plan, admitted := h.argumentPlan(policy)
	if !admitted {
		return nil, notInvoked(fmt.Errorf("%w: the %d argument policy cannot express %s", ErrNotInvoked, policy, h.signature))
	}
	count := len(args) + len(prefix)
	if !h.acceptsArgumentCount(count) {
		return nil, notInvoked(fmt.Errorf("%w: expected %d arguments, got %d", ErrNotInvoked, h.signature.NumIn(), count))
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
			return nil, notInvoked(fmt.Errorf("%w: argument %d cannot be converted to %s", ErrNotInvoked, i+1, target))
		}
		values[at] = value
	}
	return h.InvokeValues(values)
}

// InvokeValues consumes already bound arguments, including any context bound by
// the existing caller. It is the only registered-function reflect.Call owner.
// It never exposes raw reflection results that a consumer could truncate.
// An implementation's own error is returned as this call's entered result, so a
// rejection it returned cannot be read as this call's phase.
func (h *Callable) InvokeValues(values []reflect.Value) (interface{}, error) {
	if h == nil || !h.function.IsValid() {
		return nil, notInvoked(ErrNotInvoked)
	}
	if !h.acceptsArgumentCount(len(values)) {
		return nil, notInvoked(ErrNotInvoked)
	}
	for i, value := range values {
		target := h.argumentType(i)
		if !value.IsValid() || !value.Type().AssignableTo(target) {
			return nil, notInvoked(fmt.Errorf("%w: argument %d is not bound to %s", ErrNotInvoked, i+1, target))
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
			return nil, enteredResult(last.Interface().(error))
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
