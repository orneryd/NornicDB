package registry_test

// Author: Codex NOR-184 lane. Consumer argument contracts through public calls.

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

func TestCallableInvocationPoliciesNOR184(t *testing.T) {
	type namedString string
	for _, tc := range []struct {
		name     string
		function interface{}
		args     []interface{}
		policy   registry.ArgumentPolicy
		want     interface{}
		fails    bool
	}{
		{"go_float_text", func(v float64) float64 { return v }, []interface{}{"2.5"}, registry.GoArguments, nil, true},
		{"root_float_text", func(v float64) float64 { return v }, []interface{}{"2.5"}, registry.APOCArguments, nil, true},
		{"query_float_text", func(v float64) float64 { return v }, []interface{}{"2.5"}, registry.CypherArguments, 2.5, false},
		{"root_float_number", func(v float64) float64 { return v }, []interface{}{2.5}, registry.APOCArguments, 2.5, false},
		{"query_numeric_list_text", func(v []float64) []float64 { return v }, []interface{}{[]interface{}{"2", 3}}, registry.CypherArguments, []float64{2, 3}, false},
		{"root_numeric_list_text", func(v []float64) []float64 { return v }, []interface{}{[]interface{}{"2", 3}}, registry.APOCArguments, nil, true},
		{"query_numeric_list_null", func(v []float64) []float64 { return v }, []interface{}{nil}, registry.CypherArguments, []float64(nil), false},
		{"root_numeric_list_null", func(v []float64) []float64 { return v }, []interface{}{nil}, registry.APOCArguments, nil, true},
		{"root_named_string", func(v string) string { return v }, []interface{}{namedString("x")}, registry.APOCArguments, nil, true},
		{"query_named_string", func(v string) string { return v }, []interface{}{namedString("x")}, registry.CypherArguments, "x", false},
		{"root_string_null", func(v string) string { return v }, []interface{}{nil}, registry.APOCArguments, nil, true},
		{"root_unlisted_signature", func(v int64) int64 { return v }, []interface{}{int64(2)}, registry.APOCArguments, nil, true},
		{"query_integer", func(v int64) int64 { return v }, []interface{}{float64(2)}, registry.CypherArguments, int64(2), false},
		{"query_fractional_integer", func(v int64) int64 { return v }, []interface{}{2.5}, registry.CypherArguments, nil, true},
		{"root_string_list", func(v []string, d string) string { return v[0] + d + v[1] }, []interface{}{[]interface{}{"a", "b"}, ":"}, registry.APOCArguments, "a:b", false},
		{"root_typed_list", func(v []interface{}) float64 { return float64(len(v)) }, []interface{}{[2]int{1, 2}}, registry.APOCArguments, float64(2), false},
		{"query_variadic", func(v ...int64) int { return len(v) }, []interface{}{1, 2, 3}, registry.CypherArguments, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := registry.Compile(tc.function)
			require.NoError(t, err)
			value, err := handler.CallWithPolicy(tc.args, nil, tc.policy)
			if tc.fails {
				require.Error(t, err)
				require.Nil(t, value)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, value)
		})
	}
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "caller")
	handler, err := registry.Compile(func(got context.Context, v string) string { require.Same(t, ctx, got); return v })
	require.NoError(t, err)
	value, err := handler.CallWithPolicy([]interface{}{"value"}, []reflect.Value{reflect.ValueOf(ctx)}, registry.CypherArguments)
	require.NoError(t, err)
	require.Equal(t, "value", value)
	_, err = handler.CallWithPolicy([]interface{}{"value"}, []reflect.Value{reflect.ValueOf("wrong-prefix")}, registry.CypherArguments)
	require.Error(t, err)
}
