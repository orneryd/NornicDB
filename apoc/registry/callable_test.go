// Author: Codex NOR-184 lane. Opaque protocol and invocation boundary controls.
package registry_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	apocmath "github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/number"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

func TestCallableCheckedResultsNOR184(t *testing.T) {
	for _, function := range []interface{}{apocmath.Gcd, apocmath.Lcm, number.GCD, number.LCM} {
		handler, err := registry.Compile(function, registry.CanonicalReads)
		require.NoError(t, err)
		require.Equal(t, reflect.TypeOf(function), handler.Signature())
		require.NotEmpty(t, handler.Symbol())
		same, err := registry.Compile(handler, registry.CanonicalReads)
		require.NoError(t, err)
		require.Same(t, handler, same)
		for _, values := range [][]reflect.Value{nil, {reflect.ValueOf(int64(1))}, {reflect.ValueOf("bad"), reflect.ValueOf(int64(1))}, {{}, reflect.ValueOf(int64(1))}} {
			var err error
			require.NotPanics(t, func() { _, err = handler.InvokeValues(values) })
			require.Error(t, err)
		}
	}
	handler, err := registry.Compile(apocmath.Lcm, registry.CanonicalReads)
	require.NoError(t, err)
	value, err := handler.InvokeValues([]reflect.Value{reflect.ValueOf(int64(math.MaxInt64)), reflect.ValueOf(int64(2))})
	require.Error(t, err)
	require.Nil(t, value)
	value, err = handler.InvokeValues([]reflect.Value{reflect.ValueOf(int64(12)), reflect.ValueOf(int64(18))})
	require.NoError(t, err)
	require.Equal(t, int64(36), value)
}

// A Go registration owns its result/error protocol, while each consumer owns
// the value types it can represent. Capabilities must survive Go invocation.
func TestCallableGoValueDomainNOR184(t *testing.T) {
	sentinel := errors.New("capability producer failed")
	called := false
	capability := func() { called = true }
	channel := make(chan int)
	defer close(channel)
	number := 1
	for _, entry := range []struct {
		name     string
		function interface{}
		kind     reflect.Kind
	}{
		{"function", func() func() { return capability }, reflect.Func},
		{"channel", func() chan int { return channel }, reflect.Chan},
		{"pointer", func() unsafe.Pointer { return unsafe.Pointer(&number) }, reflect.UnsafePointer},
	} {
		t.Run(entry.name, func(t *testing.T) {
			r := registry.NewFunctionRegistry()
			require.NoError(t, r.RegisterFunction(entry.name, "values", entry.function, registry.OpaqueReads, "", nil))
			value, err := r.CallFunction(entry.name, nil)
			require.NoError(t, err)
			require.Equal(t, entry.kind, reflect.TypeOf(value).Kind())
			if entry.kind == reflect.Func {
				value.(func())()
				require.True(t, called)
			}
		})
	}
	for _, function := range []interface{}{
		func() (func(), error) { return capability, sentinel },
		func() (chan int, error) { return channel, sentinel },
		func() (unsafe.Pointer, error) { return unsafe.Pointer(&number), sentinel },
	} {
		handler, err := registry.Compile(function, registry.OpaqueReads)
		require.NoError(t, err)
		value, err := handler.Call(nil)
		require.ErrorIs(t, err, sentinel)
		require.Nil(t, value)
	}
}

func TestCallableProtocolNOR184(t *testing.T) {
	var nilHandler *registry.Callable
	for _, handler := range []*registry.Callable{nilHandler, new(registry.Callable)} {
		require.Nil(t, handler.Signature())
		require.Empty(t, handler.Symbol())
		require.Equal(t, registry.OpaqueReads, handler.ReadObservation())
		_, err := handler.Call(nil)
		require.Error(t, err)
		_, err = handler.InvokeValues(nil)
		require.Error(t, err)
		_, err = registry.Compile(handler, registry.OpaqueReads)
		require.Error(t, err)
	}
	sentinel := errors.New("fixture error")
	for _, function := range []interface{}{func() error { return sentinel }, func() (int, error) { return 0, sentinel }} {
		handler, err := registry.Compile(function, registry.OpaqueReads)
		require.NoError(t, err)
		value, err := handler.Call(nil)
		require.ErrorIs(t, err, sentinel)
		require.Nil(t, value)
	}
	for _, function := range []interface{}{func() {}, func() error { return nil }, func() (interface{}, error) { return nil, nil }} {
		handler, err := registry.Compile(function, registry.OpaqueReads)
		require.NoError(t, err)
		value, err := handler.Call(nil)
		require.NoError(t, err)
		require.Nil(t, value)
	}
	scalar, err := registry.Compile(func() string { return "single" }, registry.CanonicalReads)
	require.NoError(t, err)
	value, err := scalar.Call(nil)
	require.NoError(t, err)
	require.Equal(t, "single", value)
	variadic, err := registry.Compile(func(values ...int64) int { return len(values) }, registry.CanonicalReads)
	require.NoError(t, err)
	value, err = variadic.Call([]interface{}{int64(1), int8(2)})
	require.NoError(t, err)
	require.Equal(t, 2, value)
	_, err = variadic.Call([]interface{}{nil})
	require.Error(t, err)
	value, err = variadic.InvokeValues([]reflect.Value{reflect.ValueOf(int64(1)), reflect.ValueOf(int64(2))})
	require.NoError(t, err)
	require.Equal(t, 2, value)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "forwarded")
	contextual, err := registry.Compile(func(ctx context.Context, value int64) (int64, error) {
		require.Equal(t, "forwarded", ctx.Value(key{}))
		return value, nil
	}, registry.OpaqueReads)
	require.NoError(t, err)
	_, err = contextual.Call([]interface{}{int64(1)})
	require.Error(t, err, "this owner must not inject a context")
	value, err = contextual.Call([]interface{}{ctx, int64(1)})
	require.NoError(t, err)
	require.Equal(t, int64(1), value)
	_, err = registry.Compile(func() (int, int) { return 0, 0 }, registry.CanonicalReads)
	require.ErrorIs(t, err, registry.ErrNonScalarResult)
	metadata, err := registry.DescribeNonScalar(func() (int, int) { panic("metadata cannot execute") })
	require.NoError(t, err)
	require.NotNil(t, metadata.Signature())
	require.NotEmpty(t, metadata.Symbol())
	require.Equal(t, registry.OpaqueReads, metadata.ReadObservation())
	_, err = metadata.Call(nil)
	require.Error(t, err)
	_, err = metadata.InvokeValues(nil)
	require.Error(t, err)
	for _, function := range []interface{}{nil, 3, (func() (int, int))(nil), func() int { return 0 }, func() (int, error) { return 0, sentinel }, func() (error, int) { return sentinel, 0 }} {
		_, err := registry.DescribeNonScalar(function)
		require.Error(t, err)
	}
}

// The read observation is part of the admission: a declared policy is stored
// with the implementation, a policy the owners don't declare is rejected, and
// an admitted callable keeps the policy it was admitted with.
func TestCallableReadObservationAdmission(t *testing.T) {
	implementation := func() int { return 1 }
	for _, policy := range []registry.ReadObservation{registry.ReadObservation(2), registry.ReadObservation(255)} {
		_, err := registry.Compile(implementation, policy)
		require.ErrorIs(t, err, registry.ErrInvalidReadObservation)
	}
	for _, policy := range []registry.ReadObservation{registry.CanonicalReads, registry.OpaqueReads} {
		admitted, err := registry.Compile(implementation, policy)
		require.NoError(t, err)
		require.Equal(t, policy, admitted.ReadObservation())
		same, err := registry.Compile(admitted, policy)
		require.NoError(t, err)
		require.Same(t, admitted, same, "matching identity is preserved")
		require.Equal(t, policy, registry.AdmissionObservation(admitted))
		other := registry.CanonicalReads
		if policy == registry.CanonicalReads {
			other = registry.OpaqueReads
		}
		_, err = registry.Compile(admitted, other)
		require.ErrorIs(t, err, registry.ErrConflictingReadObservation, "an admitted handler is never retagged")
	}
	require.Equal(t, registry.OpaqueReads, registry.AdmissionObservation(implementation))
	require.Equal(t, registry.OpaqueReads, registry.AdmissionObservation(nil))

	r := registry.NewFunctionRegistry()
	require.ErrorIs(t, r.RegisterFunction("bad", "values", implementation, registry.ReadObservation(9), "", nil), registry.ErrInvalidReadObservation)
	_, published := r.Get("bad")
	require.False(t, published)
	require.NoError(t, r.RegisterFunction("good", "values", implementation, registry.CanonicalReads, "", nil))
	descriptor, ok := r.Get("good")
	require.True(t, ok)
	require.Equal(t, registry.CanonicalReads, descriptor.Handler.ReadObservation())
}

// A call the admitted owner refuses is not an invocation: the implementation
// never runs, and the rejection stays an unsupported-signature error so query
// dispatch can fall back without reporting an opaque callback. A metadata
// descriptor is refused the same way.
func TestCallableRefusedCallIsNotInvocation(t *testing.T) {
	invocations := 0
	handler, err := registry.Compile(func(values []int64) int { invocations++; return len(values) }, registry.OpaqueReads)
	require.NoError(t, err)
	_, err = handler.CallWithPolicy([]interface{}{"text"}, nil, registry.CypherArguments)
	require.ErrorIs(t, err, registry.ErrNotInvoked)
	require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
	require.Zero(t, invocations, "a refused call never enters the implementation")
	_, err = handler.Call(nil)
	require.ErrorIs(t, err, registry.ErrNotInvoked)
	require.Zero(t, invocations)
	value, err := handler.Call([]interface{}{[]interface{}{int64(1), int64(2)}})
	require.NoError(t, err)
	require.Equal(t, 2, value)
	require.Equal(t, 1, invocations)

	metadata, err := registry.DescribeNonScalar(func() (int, int) { invocations++; return 0, 0 })
	require.NoError(t, err)
	_, err = metadata.CallWithPolicy(nil, nil, registry.CypherArguments)
	require.ErrorIs(t, err, registry.ErrNotInvoked)
	require.Equal(t, 1, invocations, "the metadata descriptor never enters its function")
	same, err := registry.Compile(metadata, registry.OpaqueReads)
	require.NoError(t, err)
	require.Same(t, metadata, same)
	_, err = registry.Compile(metadata, registry.CanonicalReads)
	require.ErrorIs(t, err, registry.ErrConflictingReadObservation)
}

// This call's phase is a receipt the invocation boundary owns, not a
// classification of the error chain: an implementation can return the rejection
// of a nested call it made with the same owner, and that rejection stays an
// unsupported-signature cause without claiming the outer call never ran.
func TestCallableInvocationPhaseIsAReceiptNotACause(t *testing.T) {
	require.True(t, registry.InvocationStarted(nil), "a call that returned states its own phase")

	rejector, err := registry.Compile(func(int64) {}, registry.OpaqueReads)
	require.NoError(t, err)
	for name, refuse := range map[string]func() error{
		"policy cannot express the signature": func() error {
			_, err := rejector.CallWithPolicy(nil, nil, registry.ArgumentPolicy(255))
			return err
		},
		"argument count": func() error {
			_, err := rejector.CallWithPolicy(nil, nil, registry.GoArguments)
			return err
		},
		"copied argument binding": func() error {
			_, err := rejector.CallWithPolicy([]interface{}{"text"}, nil, registry.CypherArguments)
			return err
		},
		"bound argument count": func() error {
			_, err := rejector.InvokeValues(nil)
			return err
		},
		"bound argument type": func() error {
			_, err := rejector.InvokeValues([]reflect.Value{reflect.ValueOf("text")})
			return err
		},
	} {
		rejection := refuse()
		require.ErrorIs(t, rejection, registry.ErrNotInvoked, name)
		require.ErrorIs(t, rejection, registry.ErrUnsupportedSignature, name)
		require.False(t, registry.InvocationStarted(rejection), "a pre-entry rejection is this call's phase: %s", name)
	}
	metadata, err := registry.DescribeNonScalar(func() (int, int) { return 0, 0 })
	require.NoError(t, err)
	_, descriptor := metadata.Call(nil)
	require.ErrorIs(t, descriptor, registry.ErrNotInvoked)
	require.False(t, registry.InvocationStarted(descriptor), "a metadata descriptor never runs")

	_, rejection := rejector.CallWithPolicy(nil, nil, registry.GoArguments)
	invocations := 0
	nested, err := registry.Compile(func() (int, error) { invocations++; return 0, rejection }, registry.OpaqueReads)
	require.NoError(t, err)
	value, err := nested.Call(nil)
	require.ErrorIs(t, err, registry.ErrNotInvoked, "the returned rejection keeps its cause classification")
	require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
	require.Equal(t, rejection.Error(), err.Error(), "the rejection's message is unchanged")
	require.True(t, registry.InvocationStarted(err), "an implementation's returned rejection cannot deny that it ran")
	require.Nil(t, value)
	require.Equal(t, 1, invocations)

	propagated, err := registry.Compile(func() error { return fmt.Errorf("nested call refused: %w", rejection) }, registry.OpaqueReads)
	require.NoError(t, err)
	_, err = propagated.Call(nil)
	require.ErrorIs(t, err, registry.ErrNotInvoked)
	require.True(t, registry.InvocationStarted(err), "a rejection nested under the implementation's error is not this call's phase")

	ordinary := errors.New("implementation failed")
	failing, err := registry.Compile(func() (int, error) { return 0, ordinary }, registry.OpaqueReads)
	require.NoError(t, err)
	_, err = failing.Call(nil)
	require.Same(t, ordinary, err, "an entered call returns its own error unchanged")
	require.True(t, registry.InvocationStarted(err))

	admitted, err := registry.Compile(func() int { return 1 }, registry.CanonicalReads)
	require.NoError(t, err)
	value, err = admitted.Call(nil)
	require.NoError(t, err)
	require.Equal(t, 1, value)
	require.True(t, registry.InvocationStarted(err), "a call that returned is this call's phase")
}
