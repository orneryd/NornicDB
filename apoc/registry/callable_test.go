// Author: Codex NOR-184 lane. Opaque protocol and invocation boundary controls.
package registry_test

import (
	"context"
	"errors"
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
		handler, err := registry.Compile(function)
		require.NoError(t, err)
		require.Equal(t, reflect.TypeOf(function), handler.Signature())
		require.NotEmpty(t, handler.Symbol())
		same, err := registry.Compile(handler)
		require.NoError(t, err)
		require.Same(t, handler, same)
		for _, values := range [][]reflect.Value{nil, {reflect.ValueOf(int64(1))}, {reflect.ValueOf("bad"), reflect.ValueOf(int64(1))}, {{}, reflect.ValueOf(int64(1))}} {
			var err error
			require.NotPanics(t, func() { _, err = handler.InvokeValues(values) })
			require.Error(t, err)
		}
	}
	handler, err := registry.Compile(apocmath.Lcm)
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
			require.NoError(t, r.RegisterFunction(entry.name, "values", entry.function, "", nil))
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
		handler, err := registry.Compile(function)
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
		_, err := handler.Call(nil)
		require.Error(t, err)
		_, err = handler.InvokeValues(nil)
		require.Error(t, err)
		_, err = registry.Compile(handler)
		require.Error(t, err)
	}
	sentinel := errors.New("fixture error")
	for _, function := range []interface{}{func() error { return sentinel }, func() (int, error) { return 0, sentinel }} {
		handler, err := registry.Compile(function)
		require.NoError(t, err)
		value, err := handler.Call(nil)
		require.ErrorIs(t, err, sentinel)
		require.Nil(t, value)
	}
	for _, function := range []interface{}{func() {}, func() error { return nil }, func() (interface{}, error) { return nil, nil }} {
		handler, err := registry.Compile(function)
		require.NoError(t, err)
		value, err := handler.Call(nil)
		require.NoError(t, err)
		require.Nil(t, value)
	}
	scalar, err := registry.Compile(func() string { return "single" })
	require.NoError(t, err)
	value, err := scalar.Call(nil)
	require.NoError(t, err)
	require.Equal(t, "single", value)
	variadic, err := registry.Compile(func(values ...int64) int { return len(values) })
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
	})
	require.NoError(t, err)
	_, err = contextual.Call([]interface{}{int64(1)})
	require.Error(t, err, "this owner must not inject a context")
	value, err = contextual.Call([]interface{}{ctx, int64(1)})
	require.NoError(t, err)
	require.Equal(t, int64(1), value)
	_, err = registry.Compile(func() (int, int) { return 0, 0 })
	require.ErrorIs(t, err, registry.ErrNonScalarResult)
	metadata, err := registry.DescribeNonScalar(func() (int, int) { panic("metadata cannot execute") })
	require.NoError(t, err)
	require.NotNil(t, metadata.Signature())
	require.NotEmpty(t, metadata.Symbol())
	_, err = metadata.Call(nil)
	require.Error(t, err)
	_, err = metadata.InvokeValues(nil)
	require.Error(t, err)
	for _, function := range []interface{}{nil, 3, (func() (int, int))(nil), func() int { return 0 }, func() (int, error) { return 0, sentinel }, func() (error, int) { return sentinel, 0 }} {
		_, err := registry.DescribeNonScalar(function)
		require.Error(t, err)
	}
}
