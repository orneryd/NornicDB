package cypher

import (
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// NOR-307 representative result-cache workload. The two shapes cover a read
// whose rows are scalars and a read that returns an entity per row; both are
// what an embedded caller repeats through Execute. Put measures publication and
// Get measures a hit, on the paths the executor uses: SmartQueryCache receives
// an already-computed entry key and hot-path trace, and its hit returns them.
const nor307BenchmarkRowCount = 100

func nor307BenchmarkResult(shape string, count int) *ExecuteResult {
	rows := make([][]any, count)
	entities := shape == "entities" || shape == "vectors"
	for i := range rows {
		name := "name-" + strconv.Itoa(i)
		if entities {
			rows[i] = []any{
				name,
				&storage.Node{
					ID:         storage.NodeID("nor307:bench-" + strconv.Itoa(i)),
					Labels:     []string{"Nor307"},
					Properties: map[string]any{"name": name, "seq": int64(i), "tags": []any{"a", "b"}},
				},
			}
			if shape == "vectors" {
				node := rows[i][1].(*storage.Node)
				node.ChunkEmbeddings = make([][]float32, 128)
				for chunk := range node.ChunkEmbeddings {
					node.ChunkEmbeddings[chunk] = make([]float32, 1024)
					node.ChunkEmbeddings[chunk][i%1024] = float32(chunk + 1)
				}
			}
			continue
		}
		rows[i] = []any{name, []any{int64(i), int64(i + 1), int64(i + 2)}}
	}
	if entities {
		return &ExecuteResult{Columns: []string{"name", "node"}, Rows: rows}
	}
	return &ExecuteResult{Columns: []string{"name", "items"}, Rows: rows}
}

var (
	nor307BenchmarkSink        *ExecuteResult
	nor307BenchmarkTraceSink   HotPathTrace
	nor307BenchmarkEntityFound bool
)

func BenchmarkNOR307QueryResultCache(b *testing.B) {
	const (
		query = "MATCH (n:Nor307) RETURN n"
		key   = "nor307:benchmark-entry"
	)
	labels := []string{"Nor307"}

	// Keep the original 100-row names for historical comparison and add the
	// report's large-list/25-hit scales. Vectors are an explicit payload
	// control: mutable vectors requested by a caller still require ownership.
	for _, workload := range []struct {
		name, shape string
		rows        int
	}{
		{"scalars", "scalars", 100},
		{"entities", "entities", 100},
		{"list-21748", "scalars", 21748},
		{"metadata-25", "entities", 25},
		{"vectors-25", "vectors", 25},
	} {
		result := nor307BenchmarkResult(workload.shape, workload.rows)
		b.Run(workload.name, func(b *testing.B) {
			b.Run("query-cache/put", func(b *testing.B) {
				cache := NewQueryCache(nor307BenchmarkRowCount)
				b.ReportAllocs()
				for b.Loop() {
					cache.Put(query, nil, result, time.Hour)
				}
			})
			b.Run("query-cache/hit", func(b *testing.B) {
				cache := NewQueryCache(nor307BenchmarkRowCount)
				cache.Put(query, nil, result, time.Hour)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					got, found := cache.Get(query, nil)
					if !found {
						b.Fatal("cached result missing")
					}
					nor307BenchmarkSink = got
				}
			})
			b.Run("smart-query-cache/put", func(b *testing.B) {
				cache := NewSmartQueryCache(nor307BenchmarkRowCount)
				b.ReportAllocs()
				for b.Loop() {
					cache.putWithLabelsAndTrace(key, result, time.Hour, labels, HotPathTrace{})
				}
			})
			b.Run("smart-query-cache/hit", func(b *testing.B) {
				cache := NewSmartQueryCache(nor307BenchmarkRowCount)
				cache.putWithLabelsAndTrace(key, result, time.Hour, labels, HotPathTrace{})
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					got, trace, entities, found := cache.getWithTraceForEmbedding(key, 0)
					if !found {
						b.Fatal("cached result missing")
					}
					nor307BenchmarkSink, nor307BenchmarkTraceSink, nor307BenchmarkEntityFound = got, trace, entities
				}
			})
		})
	}
}
