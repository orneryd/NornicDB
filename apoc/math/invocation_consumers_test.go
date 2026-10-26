// Author: Codex NOR-184 lane. Same public fixtures compile on release and head.
package math_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/testing/apocintegers"
	"github.com/stretchr/testify/require"
)

func consumerCallNOR184(handler interface{}, args []interface{}) (interface{}, error) {
	switch h := handler.(type) {
	case interface {
		Call([]interface{}) (interface{}, error)
	}:
		return h.Call(args)
	case func(interface{}, interface{}) (interface{}, error):
		return h(args[0], args[1])
	case func(int64, int64) (int64, error):
		value, err := h(args[0].(int64), args[1].(int64))
		if err != nil {
			return nil, err
		}
		return value, nil
	case func(int64, int64) int64:
		return h(args[0].(int64), args[1].(int64)), nil
	default:
		return nil, fmt.Errorf("unexpected public handler %T", handler)
	}
}

func TestInvocationConsumersNOR184(t *testing.T) {
	consumers, cleanup, err := apocintegers.Consumers("api")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup()) })
	for _, consumer := range consumers {
		for _, a := range apocintegers.Values {
			for _, b := range apocintegers.Values {
				t.Run(fmt.Sprintf("%s/%s/%d/%d", consumer.Class, consumer.Name, a, b), func(t *testing.T) {
					want, fits := apocintegers.Expected(consumer.Operation, a, b)
					var got interface{}
					var err error
					require.NotPanics(t, func() { got, err = consumerCallNOR184(consumer.Handler, []interface{}{a, b}) })
					if !fits {
						require.Error(t, err)
						require.Nil(t, got)
						return
					}
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		}
		// Typed Go exports accept int64 by construction. The remaining classes
		// are dynamic public APIs and must also bind/reject raw values safely.
		if consumer.Class == "export" || consumer.Class == "root_descriptor" {
			continue
		}
		for _, input := range apocintegers.Inputs {
			for _, swap := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/%s/swap=%t", consumer.Class, consumer.Name, input.Name, swap), func(t *testing.T) {
					args := []interface{}{input.Value, int64(6)}
					if swap {
						args[0], args[1] = args[1], args[0]
					}
					var got interface{}
					var err error
					require.NotPanics(t, func() { got, err = consumerCallNOR184(consumer.Handler, args) })
					want, fits := apocintegers.Expected(consumer.Operation, input.Integer, 6)
					if !input.Valid || !fits {
						require.Error(t, err)
						require.Nil(t, got)
						return
					}
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		}
	}
}

func TestInvocationAdmissionNOR184(t *testing.T) {
	sentinel := errors.New("checked function failed")
	for _, entry := range []struct {
		name     string
		function interface{}
		reject   bool
	}{
		{"nil", nil, true}, {"not_function", int64(1), true}, {"nil_function", (func() int64)(nil), true},
		{"error_first", func() (error, int64) { return sentinel, 0 }, true},
		{"error_among_three", func() (int64, error, int64) { return 0, sentinel, 0 }, true},
		{"three_with_error", func() (int64, int64, error) { return 0, 0, sentinel }, true},
		{"two_values", func() (int64, int64) { return 1, 2 }, true},
		{"error_only", func() error { return sentinel }, false},
		{"value_and_error", func() (int64, error) { return 0, sentinel }, false},
	} {
		t.Run(entry.name, func(t *testing.T) {
			r := registry.NewFunctionRegistry()
			var err error
			require.NotPanics(t, func() { err = r.RegisterFunction("probe", "probe", entry.function, "", nil) })
			if entry.reject {
				require.Error(t, err)
				_, published := r.Get("probe")
				require.False(t, published)
				return
			}
			require.NoError(t, err)
			var got interface{}
			require.NotPanics(t, func() { got, err = r.CallFunction("probe", nil) })
			require.ErrorIs(t, err, sentinel)
			require.Nil(t, got)
		})
	}
}
