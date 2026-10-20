package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Diagnostic only: identical fixture on the three pinned product trees. Run
// with -benchtime=1x: one process executes a cold original statement, 100 warm
// executions, 20 previously unseen texts, then those same 20 texts again.
// Nonce varies on every call to bypass results while retaining semantic caches.
func BenchmarkColdStatementAttribution(b *testing.B) {
	for _, sample := range []struct {
		name, original, variant string
		rows                    int
	}{
		{"simple_match_limit", "MATCH (n:Person) RETURN n.name LIMIT 10", "MATCH (n%02d:Person) RETURN n%02d.name LIMIT 10", 10},
		{"filter_count", "MATCH (n:Person) WHERE n.age > 30 RETURN count(n)", "MATCH (n%02d:Person) WHERE n%02d.age > 30 RETURN count(n%02d)", 1},
		{"one_hop_limit", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 20", "MATCH (a%02d:Person)-[:KNOWS]->(b%02d:Person) RETURN a%02d.name, b%02d.name LIMIT 20", 20},
	} {
		b.Run(sample.name, func(b *testing.B) {
			if b.N != 1 {
				b.Fatal("diagnostic requires -benchtime=1x")
			}
			b.StopTimer()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
			for index := 0; index < 2000; index++ {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(fmt.Sprintf("person-%d", index)), Labels: []string{"Person"},
					Properties: map[string]interface{}{"id": index, "name": fmt.Sprintf("p%d", index), "age": index % 60},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			for index := 0; index < 500; index++ {
				if err := store.CreateEdge(&storage.Edge{
					ID: storage.EdgeID(fmt.Sprintf("knows-%d", index)), StartNode: storage.NodeID(fmt.Sprintf("person-%d", index)),
					EndNode: storage.NodeID(fmt.Sprintf("person-%d", index+1)), Type: "KNOWS",
				}); err != nil {
					b.Fatal(err)
				}
			}
			executor, ctx := NewStorageExecutor(store), context.Background()
			queries := make([]string, 20)
			for index := range queries {
				switch sample.name {
				case "simple_match_limit":
					queries[index] = fmt.Sprintf(sample.variant, index, index)
				case "filter_count":
					queries[index] = fmt.Sprintf(sample.variant, index, index, index)
				case "one_hop_limit":
					queries[index] = fmt.Sprintf(sample.variant, index, index, index, index)
				}
			}
			type observation struct {
				Phase, Query string
				Index, Calls int
				Ns, Bytes    uint64
				Allocs       uint64
			}
			observations := make([]observation, 0, 42)
			nonce := 0
			measure := func(phase, query string, index, calls int) observation {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				for call := 0; call < calls; call++ {
					nonce++
					result, err := executor.Execute(ctx, query, map[string]interface{}{"nonce": nonce})
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != sample.rows {
						b.Fatalf("%s: got %d rows, want %d", query, len(result.Rows), sample.rows)
					}
					if sample.name == "filter_count" && result.Rows[0][0] != int64(957) {
						b.Fatalf("%s: wrong count %v", query, result.Rows)
					}
				}
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				item := observation{phase, query, index, calls, uint64(elapsed), after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs}
				observations = append(observations, item)
				return item
			}
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			cold := measure("cold_original", sample.original, 0, 1)
			warm := measure("warm100_original", sample.original, 0, 100)
			for index, query := range queries {
				measure("new20", query, index, 1)
			}
			for index, query := range queries {
				measure("repeat20", query, index, 1)
			}
			b.StopTimer()
			for _, phase := range []string{"cold_original", "warm100_original", "new20", "repeat20"} {
				var ns, bytes, allocs uint64
				calls := 0
				for _, item := range observations {
					if item.Phase == phase {
						ns, bytes, allocs, calls = ns+item.Ns, bytes+item.Bytes, allocs+item.Allocs, calls+item.Calls
					}
				}
				b.ReportMetric(float64(ns)/float64(calls), phase+"-ns/query")
				b.ReportMetric(float64(bytes)/float64(calls), phase+"-B/query")
				b.ReportMetric(float64(allocs)/float64(calls), phase+"-allocs/query")
			}
			payload, err := json.Marshal(struct {
				Name         string
				Cold, Warm   observation
				Observations []observation
			}{sample.name, cold, warm, observations})
			if err != nil {
				b.Fatal(err)
			}
			fmt.Printf("COLD_STATEMENT_ATTRIBUTION %s\n", payload)
		})
	}
}
