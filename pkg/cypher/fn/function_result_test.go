package fn

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
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
			actual := functionResultOf(t, input)
			require.Equal(t, want, actual)
			// A plugin may return the same owned object again on another call.
			require.Equal(t, reflect.TypeOf(null), reflect.TypeOf(input["map"].(map[string]interface{})["a"]))
			require.Equal(t, want, functionResultOf(t, input))
		})
	}
	for _, tc := range []struct{ input, want interface{} }{
		{[]*storage.Node{nil, node}, []interface{}{nil, node}},
		{[2]interface{}{(*storage.Node)(nil), map[string]interface{}(nil)}, []interface{}{nil, nil}},
		{map[interface{}]interface{}{"a": (*storage.Node)(nil)}, map[interface{}]interface{}{"a": nil}},
		{otherMap{1: (*storage.Edge)(nil)}, map[interface{}]interface{}{1: nil}},
		{namedList(nil), []interface{}{}}, {namedMap{}, map[string]interface{}{}},
	} {
		require.Equal(t, tc.want, functionResultOf(t, tc.input))
	}
}

func TestFunctionResultRetainsCanonicalValues(t *testing.T) {
	for _, value := range []interface{}{nil, int64(1), "text", []byte{0, 128, 255}, []int64{1, 2}, []interface{}(nil), []string(nil), map[string]interface{}{}, &storage.Node{}, storage.RemoteEntityValue{"elementId": "node"}, storage.RemotePathValue{{"elementId": "node"}}} {
		require.Equal(t, value, functionResultOf(t, value), "%T", value)
	}
	unchanged := map[string]interface{}{"a": []interface{}{nil, "text", int64(1)}}
	require.Equal(t, float64(0), testing.AllocsPerRun(10, func() { _, _ = FunctionResult(context.Background(), unchanged) }))
	require.Equal(t, map[string]interface{}{}, functionResultOf(t, EntityProperties(nil)))
}

func functionResultOf(t *testing.T, value interface{}) interface{} {
	t.Helper()
	normalized, err := FunctionResult(context.Background(), value)
	require.NoError(t, err)
	return normalized
}

// A canceled statement stops the walk of a large result: its error, and no
// value.
func TestFunctionResultStopsOnCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	large := make([]interface{}, 4*bulk.Stride)
	for i := range large {
		large[i] = map[string]interface{}{"a": map[string]interface{}(nil)}
	}
	normalized, err := FunctionResult(canceled, large)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, normalized)
}
