package cypher

import (
	"context"
	"math"
	"testing"

	apoccoll "github.com/orneryd/nornicdb/apoc/coll"
	apocconvert "github.com/orneryd/nornicdb/apoc/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkListInputCollections(b *testing.B) {
	for _, input := range []struct {
		name  string
		value interface{}
	}{
		{"boxed", []interface{}{int64(3), int64(1), int64(3)}},
		{"strings", []string{"c", "a", "c"}},
		{"int64", []int64{3, 1, 3}},
	} {
		for _, function := range []string{"union", "frequencies", "toList"} {
			b.Run(input.name+"/"+function, func(b *testing.B) {
				prefix := "apoc.coll."
				if function == "toList" {
					previous := PluginFunctionLookup
					PluginFunctionLookup = func(name string) (interface{}, bool) {
						return apocconvert.ToList, name == "apoc.convert.toList"
					}
					defer func() { PluginFunctionLookup = previous }()
					prefix = "apoc.convert."
				}
				store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "list_bench")
				defer store.Close()
				exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
				query := "RETURN " + prefix + function + "($xs"
				if function == "union" {
					query += ",[9]"
				}
				query += ") AS v"
				params := map[string]interface{}{"xs": input.value}
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
}

func BenchmarkUnsignedIntegerInputs(b *testing.B) {
	previous := PluginFunctionLookup
	handlers := map[string]interface{}{
		"apoc.convert.toInteger": apocconvert.ToInteger,
		"apoc.convert.toIntList": apocconvert.ToIntList,
		"apoc.coll.sumLongs":     apoccoll.SumLongs,
	}
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		handler, ok := handlers[name]
		return handler, ok
	}
	defer func() { PluginFunctionLookup = previous }()
	for _, input := range []struct {
		name  string
		value uint64
	}{{"bound", math.MaxInt64}, {"overflow", math.MaxUint64}} {
		for _, function := range []string{"apoc.convert.toInteger", "apoc.convert.toIntList", "apoc.coll.sumLongs"} {
			b.Run(input.name+"/"+function, func(b *testing.B) {
				store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "unsigned_bench")
				defer store.Close()
				exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
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
}
