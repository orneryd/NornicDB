package cypher

// Author: Codex NOR-184 lane: migrate fixtures to admitted callable publication.

import (
	"context"
	"math"
	"reflect"
	"testing"

	apoccoll "github.com/orneryd/nornicdb/apoc/coll"
	apocconvert "github.com/orneryd/nornicdb/apoc/convert"
	apocmath "github.com/orneryd/nornicdb/apoc/math"
	apocnumber "github.com/orneryd/nornicdb/apoc/number"
	apocstats "github.com/orneryd/nornicdb/apoc/stats"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// The exact same fixture runs on the earlier published PR product and the
// correction. Every case checks its public result before timing, so an old
// silent-discard/error path cannot masquerade as a faster valid baseline.
func BenchmarkCheckedNumericInputs(b *testing.B) {
	handlers := map[string]interface{}{
		"apoc.convert.toIntList": apocconvert.ToIntList, "apoc.convert.toInteger": apocconvert.ToInteger, "apoc.coll.sumLongs": apoccoll.SumLongs,
		"apoc.math.percentile": apocmath.Percentile, "apoc.stats.histogram": apocstats.Histogram, "apoc.number.format": apocnumber.Format,
		"zz.numeric.int32": func(value int32) int64 { return int64(value) },
	}
	withPluginFunctions(b, handlers)
	for _, input := range []struct {
		Name, Expression string
		Value, Want      interface{}
	}{
		{"core/int64", "head(toIntegerList($xs))", []int64{12}, int64(12)},
		{"core/float64", "head(toIntegerList($xs))", []float64{12.75}, int64(12)},
		{"core/float32", "head(toIntegerList($xs))", []float32{12.75}, int64(12)},
		{"core/string-decimal", "head(toIntegerList($xs))", []string{"12.75"}, int64(12)},
		{"apoc/uint64-bound", "apoc.convert.toInteger(head($xs))", []uint64{math.MaxInt64}, int64(math.MaxInt64)},
		{"apoc/uint64-invalid", "apoc.convert.toInteger(head($xs))", []uint64{math.MaxUint64}, int64(0)},
		{"sum/boxed", "apoc.coll.sumLongs($xs)", []interface{}{int64(3), int64(1), int64(3)}, int64(7)},
		{"sum/int64", "apoc.coll.sumLongs($xs)", []int64{3, 1, 3}, int64(7)},
		{"sum/float32", "apoc.coll.sumLongs($xs)", []float32{3, 1, 3}, int64(7)},
		{"vector/boxed", "vector.similarity.cosine($xs,$xs)", []interface{}{int64(3), int64(1), int64(3)}, float64(1)},
		{"vector/int64", "vector.similarity.cosine($xs,$xs)", []int64{3, 1, 3}, float64(1)},
		{"vector/float32", "vector.similarity.cosine($xs,$xs)", []float32{3, 1, 3}, float64(1)},
		{"subscript/boxed", "apoc.map.setKey({},'v',coalesce($xs,[])[0])", []interface{}{int64(3), int64(1), int64(3)}, map[string]interface{}{"v": int64(3)}},
		{"math/percentile", "apoc.math.percentile($xs,50)", []float64{3, 1, 3}, float64(3)},
		{"math/histogram", "head(apoc.stats.histogram($xs,2))", []float64{1, 2, 3}, int64(1)},
		{"format/commas", "apoc.number.format(head($xs),'#,##0.00')", []float64{12345.25}, "12,345.25"},
		{"plugin/uint16-int32", "zz.numeric.int32(head($xs))", []uint16{12}, int64(12)},
	} {
		b.Run(input.Name, func(b *testing.B) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "checked_numeric_bench")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			query := "RETURN " + input.Expression + " AS v"
			params := map[string]interface{}{"xs": input.Value}
			result, err := exec.Execute(context.Background(), query, params)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || !reflect.DeepEqual(input.Want, result.Rows[0][0]) {
				b.Fatalf("invalid benchmark result: want %#v got %#v", input.Want, result.Rows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(context.Background(), query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("unsigned", func(b *testing.B) {
		for _, input := range []struct {
			name  string
			value uint64
		}{{"bound", math.MaxInt64}, {"overflow", math.MaxUint64}} {
			for _, function := range []string{"apoc.convert.toInteger", "apoc.convert.toIntList", "apoc.coll.sumLongs"} {
				b.Run(input.name+"/"+function, func(b *testing.B) {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "unsigned_bench")
					exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
					argument := "$xs"
					if function == "apoc.convert.toInteger" {
						argument = "$value"
					}
					query := "RETURN " + function + "(" + argument + ") AS v"
					params := map[string]interface{}{"value": input.value, "xs": []uint64{input.value}}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := exec.Execute(context.Background(), query, params); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	})
}

func BenchmarkCheckedNumericResults(b *testing.B) {
	for _, input := range []struct {
		Name  string
		Value interface{}
	}{
		{"float32", []float32{12}}, {"uint16", []uint16{12}},
	} {
		b.Run(input.Name, func(b *testing.B) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "numeric_decode_bench")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			params := map[string]interface{}{"xs": input.Value}
			result, err := TypedExecute[[]int32](context.Background(), exec, "RETURN $xs AS v", params)
			if err != nil {
				b.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, [][]int32{{12}}) {
				b.Fatalf("invalid decode benchmark result: %#v", result.Rows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := TypedExecute[[]int32](context.Background(), exec, "RETURN $xs AS v", params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
