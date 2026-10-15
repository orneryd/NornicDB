package cypher

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTypedPluginHandlerShapes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler interface{}
		args    []interface{}
		want    interface{}
	}{
		{"float_slice", func(xs []float64) float64 { return xs[0] + xs[1] }, []interface{}{[]interface{}{int64(2), int64(3)}}, float64(5)},
		{"any_string", func(x interface{}) string { return "typed" }, []interface{}{int64(1)}, "typed"},
		{"two_ints", func(a, b int64) int64 { return a & b }, []interface{}{int64(5), int64(3)}, int64(1)},
		{"map_result", func() map[string]interface{} { return map[string]interface{}{"v": int64(1)} }, nil, map[string]interface{}{"v": int64(1)}},
		{"four_args", func(a, b, c, d int64) int64 { return a + b + c + d }, []interface{}{int64(1), int64(2), int64(3), int64(4)}, int64(10)},
		{"variadic", func(xs ...int64) int64 {
			var n int64
			for _, x := range xs {
				n += x
			}
			return n
		}, []interface{}{int64(1), int64(2)}, int64(3)},
		{"error_result", func(x int64) (int64, error) { return x, nil }, []interface{}{int64(2)}, int64(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := callPluginHandler(tc.handler, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
func TestTypedPluginPublicQueries(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{"zz.bitwise.and": func(a, b int64) int64 { return a & b }, "zz.stats.sum": func(xs []float64) float64 {
		var n float64
		for _, x := range xs {
			n += x
		}
		return n
	}, "zz.map.value": func() map[string]interface{} { return map[string]interface{}{"v": int64(9)} }})
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typed_plugins"))
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"RETURN zz.bitwise.and(5,3) AS v", [][]interface{}{{int64(1)}}},
		{"UNWIND [[1,2],[3,4]] AS xs RETURN zz.stats.sum(xs) AS v", [][]interface{}{{float64(3)}, {float64(7)}}},
		{"RETURN zz.map.value().v AS v", [][]interface{}{{int64(9)}}},
	} {
		r, err := e.Execute(context.Background(), tc.query, nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, r.Rows)
	}
}

var pluginIdentityBenchmarkResult interface{}

func BenchmarkPluginHandlerIdentity(b *testing.B) {
	handler := func(x interface{}) interface{} { return x }
	args := []interface{}{"value"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, err := callPluginHandler(handler, args)
		if err != nil {
			b.Fatal(err)
		}
		pluginIdentityBenchmarkResult = v
	}
}

func TestTypedPluginCheckedFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   interface{}
		args []interface{}
	}{
		{"fraction", func(int64) int64 { return 0 }, []interface{}{1.5}},
		{"overflow", func(int8) int8 { return 0 }, []interface{}{int64(128)}},
		{"negative_unsigned", func(uint) uint { return 0 }, []interface{}{int64(-1)}},
		{"nan_integer", func(int64) int64 { return 0 }, []interface{}{math.NaN()}},
		{"int64_boundary", func(int64) int64 { return 0 }, []interface{}{math.Exp2(63)}},
		{"nil_scalar", func(int64) int64 { return 0 }, []interface{}{nil}},
		{"wrong_slice", func([]float64) float64 { return 0 }, []interface{}{[]interface{}{int64(1), "bad"}}},
		{"extra_fast_argument", func(string) string { panic("must not invoke") }, []interface{}{"a", "b"}},
		{"extra_zero_argument", func() string { panic("must not invoke") }, []interface{}{"a"}},
		{"bad_return", func() (int, string) { panic("must not invoke") }, nil},
		{"not_function", 3, nil},
		{"nil_function", (func(int) int)(nil), []interface{}{1}},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := callPluginHandler(tc.fn, tc.args); require.Error(t, err) })
	}
	_, err := callPluginHandler(func(string) string { panic("fixture") }, []interface{}{"ok"})
	require.ErrorContains(t, err, "plugin handler panic: fixture")
	_, err = callPluginHandler(func(int64) int64 { panic("typed fixture") }, []interface{}{int64(1)})
	require.ErrorContains(t, err, "typed fixture")
	sentinel := errors.New("fixture error")
	_, err = callPluginHandler(func() (string, error) { return "", sentinel }, nil)
	require.ErrorIs(t, err, sentinel)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = callPluginHandlerContext(ctx, func() string { panic("cancelled must not invoke") }, nil)
	require.ErrorIs(t, err, context.Canceled)
	type contextKey struct{}
	ctx = context.WithValue(context.Background(), contextKey{}, "forwarded")
	v, err := callPluginHandlerContext(ctx, func(ctx context.Context, n int64) (interface{}, error) {
		require.Equal(t, "forwarded", ctx.Value(contextKey{}))
		return n, nil
	}, []interface{}{int64(7)})
	require.NoError(t, err)
	require.Equal(t, int64(7), v)
	v, err = callPluginHandler(func(xs []int64) bool { return xs == nil }, []interface{}{nil})
	require.NoError(t, err)
	require.Equal(t, true, v)
	v, err = callPluginHandler(func(m map[string]int64) int64 { return m["x"] }, []interface{}{map[string]interface{}{"x": int64(9)}})
	require.NoError(t, err)
	require.Equal(t, int64(9), v)
}

func TestTypedPluginConversionBoundaries(t *testing.T) {
	type namedText string
	type namedBool bool
	for _, tc := range []struct {
		value, target interface{}
		ok            bool
	}{
		{uint64(9), int64(0), true}, {uint64(math.MaxUint64), int64(0), false},
		{float64(2), int64(0), true}, {"2", int64(0), false},
		{int64(2), uint8(0), true}, {uint64(256), uint8(0), false}, {float64(2), uint64(0), true}, {1.2, uint64(0), false}, {"2", uint64(0), false},
		{uint16(2), uint64(0), true}, {int64(2), float32(0), true}, {uint64(2), float64(0), true}, {float64(2), float32(0), true}, {math.MaxFloat64, float32(0), false}, {"2", float64(0), false},
		{"x", namedText(""), true}, {true, namedBool(false), true},
		{42, namedText(""), false}, {42, namedBool(false), false},
		{[2]int{1, 2}, []int64{}, true}, {42, []int64{}, false}, {[]int(nil), []int64{}, true},
		{map[string]int(nil), map[string]int64{}, true}, {42, map[string]int64{}, false},
		{map[int]int{1: 2}, map[string]int64{}, false}, {map[string]string{"x": "bad"}, map[string]int64{}, false},
		{nil, map[string]int64{}, true}, {nil, []int64{}, true}, {nil, (*int)(nil), true},
		{struct{}{}, int64(0), false},
	} {
		_, ok := convertPluginArgument(reflect.ValueOf(tc.value), reflect.TypeOf(tc.target))
		require.Equal(t, tc.ok, ok, "%T -> %T", tc.value, tc.target)
	}
	var iface interface{}
	v := reflect.ValueOf(&iface).Elem()
	_, ok := convertPluginArgument(v, reflect.TypeOf(map[string]int64{}))
	require.True(t, ok)
	iface = int64(7)
	v = reflect.ValueOf(&iface).Elem()
	converted, ok := convertPluginArgument(v, reflect.TypeOf(int8(0)))
	require.True(t, ok)
	require.Equal(t, int8(7), converted.Interface())
	for _, fn := range []interface{}{func() {}, func() error { return nil }, func() (int64, error) { return 1, nil }, func(xs ...int64) int { return len(xs) }} {
		_, err := callPluginHandler(fn, nil)
		require.NoError(t, err)
	}
	_, err := callPluginHandler(func(x int64, xs ...int64) int { return 0 }, nil)
	require.Error(t, err)
	_, err = callPluginHandler(func() func() { panic("not invoked") }, nil)
	require.Error(t, err)
	_, err = callPluginHandler(func() chan int { panic("not invoked") }, nil)
	require.Error(t, err)
}

func TestTypedPluginPublicContext(t *testing.T) {
	type key struct{}
	withPluginFunctions(t, map[string]interface{}{"zz.ctx": func(ctx context.Context, x int64) interface{} {
		require.Equal(t, "forwarded", ctx.Value(key{}))
		return x
	}})
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_context"))
	ctx := context.WithValue(context.Background(), key{}, "forwarded")
	for _, q := range []string{"RETURN zz.ctx(1) AS v", "UNWIND [1] AS n WITH n WHERE zz.ctx(n)=1 RETURN n"} {
		r, err := e.Execute(ctx, q, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, r.Rows)
	}
}
