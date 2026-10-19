package fn

import (
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFunctionResultRecursiveNulls(t *testing.T) {
	type namedMap map[string]interface{}
	type namedList []interface{}
	type otherMap map[int]interface{}
	node := &storage.Node{ID: "present"}
	for _, null := range []interface{}{map[string]interface{}(nil), map[interface{}]interface{}(nil), namedMap(nil), (*storage.Node)(nil), (*storage.Edge)(nil), (*time.Time)(nil), storage.RemoteEntityValue(nil), storage.RemotePathValue(nil)} {
		t.Run(reflect.TypeOf(null).String(), func(t *testing.T) {
			input := map[string]interface{}{"map": map[string]interface{}{"a": null}, "list": []interface{}{namedList{null}, namedMap{"a": null}}, "node": node}
			want := map[string]interface{}{"map": map[string]interface{}{"a": nil}, "list": []interface{}{[]interface{}{nil}, map[string]interface{}{"a": nil}}, "node": node}
			actual := FunctionResult(input)
			require.Equal(t, want, actual)
			// A plugin may return the same owned object again on another call.
			require.Equal(t, reflect.TypeOf(null), reflect.TypeOf(input["map"].(map[string]interface{})["a"]))
			require.Equal(t, want, FunctionResult(input))
		})
	}
	for _, tc := range []struct{ input, want interface{} }{
		{[]*storage.Node{nil, node}, []interface{}{nil, node}},
		{[2]interface{}{(*storage.Node)(nil), map[string]interface{}(nil)}, []interface{}{nil, nil}},
		{map[interface{}]interface{}{"a": (*storage.Node)(nil)}, map[interface{}]interface{}{"a": nil}},
		{otherMap{1: (*storage.Edge)(nil)}, map[interface{}]interface{}{1: nil}},
		{namedList(nil), []interface{}{}}, {namedMap{}, map[string]interface{}{}},
	} {
		require.Equal(t, tc.want, FunctionResult(tc.input))
	}
}

func TestFunctionResultRetainsCanonicalValues(t *testing.T) {
	for _, value := range []interface{}{nil, int64(1), "text", []byte{0, 128, 255}, []int64{1, 2}, []interface{}(nil), []string(nil), map[string]interface{}{}, &storage.Node{}, storage.RemoteEntityValue{"elementId": "node"}, storage.RemotePathValue{"nodes": []interface{}{}}} {
		require.Equal(t, value, FunctionResult(value), "%T", value)
	}
	unchanged := map[string]interface{}{"a": []interface{}{nil, "text", int64(1)}}
	require.Equal(t, reflect.ValueOf(unchanged).Pointer(), reflect.ValueOf(FunctionResult(unchanged)).Pointer())
	require.Equal(t, float64(0), testing.AllocsPerRun(10, func() { FunctionResult(unchanged) }))
	require.Equal(t, map[string]interface{}{}, FunctionResult(EntityProperties(nil)))
}

func BenchmarkFunctionResult(b *testing.B) {
	for _, tc := range []struct{ name string; value interface{} }{
		{"scalar", int64(42)},
		{"canonical", map[string]interface{}{"a": int64(1), "list": []interface{}{nil, "value"}}},
		{"nested_null", map[string]interface{}{"a": map[string]interface{}(nil), "list": []interface{}{(*storage.Node)(nil)}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FunctionResult(tc.value)
			}
		})
	}
}
