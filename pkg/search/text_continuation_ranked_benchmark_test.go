package search

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkSearchTextContinuationRankedHNSW pages a ranked vector
// continuation to its end over an HNSW index of 2,000 documents: every
// document (no threshold), and a selective threshold that 11 documents pass.
// Each case runs with the complete-response cache on (repeated identical
// searches are cache hits) and off (every search retrieves).
func BenchmarkSearchTextContinuationRankedHNSW(b *testing.B) {
	const docs = 2000
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	build := func(b *testing.B, cache bool) *Service {
		b.Helper()
		b.Setenv(EnvSearchResultCacheEnabled, fmt.Sprint(cache))
		engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		service := NewServiceWithDimensions(engine, 2)
		b.Cleanup(func() { _ = service.Close() })
		for i := 0; i < docs; i++ {
			angle := math.Pi * float64(i) / float64(docs)
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), ChunkEmbeddings: [][]float32{{float32(math.Cos(angle)), float32(math.Sin(angle))}}}
			if _, err := engine.CreateNode(node); err != nil {
				b.Fatal(err)
			}
			if err := service.IndexNode(node); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := service.getOrCreateHNSWIndex(context.Background(), 2); err != nil {
			b.Fatal(err)
		}
		for service.hnswRebuildInFlight.Load() {
		}
		return service
	}
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	for _, cache := range []bool{true, false} {
		service := build(b, cache)
		for _, tc := range []struct {
			name      string
			threshold float64
		}{{"all", -1}, {"selective", math.Cos(math.Pi * 10 / docs)}} {
			b.Run(fmt.Sprintf("cache=%t/%s", cache, tc.name), func(b *testing.B) {
				threshold := tc.threshold
				returned := 0
				for i := 0; i < b.N; i++ {
					request := SearchContinuationRequest{Owner: "benchmark", N: 50}
					page, err := service.SearchTextContinuation(context.Background(), "", &SearchOptions{Limit: 50, MinSimilarity: &threshold}, request, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
					returned = 0
					for err == nil {
						returned += len(page.Results)
						if !page.HasMore {
							break
						}
						request.QID = page.QID
						page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
					}
					if err != nil {
						b.Fatal(err)
					}
					// The last page carries no qid and the registry keeps the
					// drained stream until it expires; release it through the
					// previous qid.
					if request.QID != "" {
						request.Discard = true
						if _, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{}); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(returned), "docs/op")
			})
		}
	}
}
