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

// nor307WideMapKeys is the property-map width of the report's wide read: every
// row is one 18-key map, the shape a map projection returns.
const nor307WideMapKeys = 18

// nor307WideMapResult builds the reported wide result: count rows, each a
// single map cell carrying nor307WideMapKeys keys. The keys mix the scalar
// widths, a null, and short property lists a map projection of a stored node
// returns, so a copy of it walks the same value kinds a real row does.
func nor307WideMapResult(count int) *ExecuteResult {
	rows := make([][]any, count)
	for i := range rows {
		name := "name-" + strconv.Itoa(i)
		rows[i] = []any{map[string]any{
			"name":    name,
			"seq":     int64(i),
			"score":   float64(i) / 2,
			"rank":    int(i),
			"active":  i%2 == 0,
			"group":   "group-" + strconv.Itoa(i%17),
			"kind":    "kind-" + strconv.Itoa(i%5),
			"owner":   "owner-" + strconv.Itoa(i%31),
			"region":  "region-" + strconv.Itoa(i%7),
			"status":  "status-" + strconv.Itoa(i%3),
			"label":   "label-" + strconv.Itoa(i%11),
			"notes":   nil,
			"unit":    int32(i),
			"weight":  float32(i) / 4,
			"version": uint16(i),
			"tags":    []any{"a", "b"},
			"scores":  []int64{int64(i), int64(i + 1), int64(i + 2)},
			"aliases": []string{"first", "second"},
		}}
	}
	return &ExecuteResult{Columns: []string{"row"}, Rows: rows}
}

func nor307BenchmarkResult(shape string, count int) *ExecuteResult {
	if shape == "maps" {
		return nor307WideMapResult(count)
	}
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
		{"maps-21748", "maps", 21748},
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
