package cypher

// Author: Codex NOR-184 lane: migrate fixtures to admitted callable publication.

import (
	"context"
	"testing"

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
					withPluginFunctions(b, map[string]interface{}{"apoc.convert.toList": apocconvert.ToList})
					prefix = "apoc.convert."
				}
				store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "list_bench")
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
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
