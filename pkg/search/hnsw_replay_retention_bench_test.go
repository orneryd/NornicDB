package search

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// This compares two correct construction-replay implementations: retaining
// insertion's temporary normalized copy and retaining the immutable canonical
// record. It measures one generation, not a public write or search latency.
// A GC outside the timer measures heap retained while the unpublished graph and
// its canonical source remain alive. Separate public controls qualify results.
func BenchmarkHNSWReplayCoordinateRetention(b *testing.B) {
	b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	for _, dimensions := range []int{128, 1536} {
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("dimensions=%d/reuse=%t", dimensions, reuse), func(b *testing.B) {
				const count = 512
				var retained int64
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for round := 0; round < b.N; round++ {
					func() {
						engine := fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
						defer func() {
							if err := engine.Close(); err != nil {
								b.Error(err)
							}
						}()
						svc := NewService(engine, dimensions, nil)
						defer func() {
							if err := svc.Close(); err != nil {
								b.Error(err)
							}
						}()
						rng := rand.New(rand.NewSource(71))
						ids := make([]string, count)
						changed := make([][]float32, count)
						for i := range ids {
							ids[i] = fmt.Sprintf("replay-%04d", i)
							original := make([]float32, dimensions)
							changed[i] = make([]float32, dimensions)
							for d := range original {
								original[d] = rng.Float32()*2 - 1
								changed[i][d] = rng.Float32()*2 - 1
							}
							if err := svc.vectorIndex.Add(ids[i], original); err != nil {
								b.Fatal(err)
							}
						}
						graph, _, err := svc.buildHNSWSnapshot(b.Context(), dimensions, svc.hnswConfig(), svc.vectorIndex, nil, nil)
						if err != nil {
							b.Fatal(err)
						}
						coordinates := graph.construction
						defer func() {
							if err := coordinates.Close(); err != nil {
								b.Error(err)
							}
						}()
						runtime.GC()
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						b.StartTimer()
						for i, id := range ids {
							if err := svc.vectorIndex.Add(id, changed[i]); err != nil {
								b.Fatal(err)
							}
							record, ok := svc.vectorIndex.getVectorRef(id)
							if !ok {
								b.Fatal("missing canonical vector")
							}
							if reuse {
								err = applyCatchUpChange(graph, id, record)
							} else {
								err = graph.Update(id, record)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						runtime.GC()
						runtime.ReadMemStats(&after)
						retained += int64(after.HeapAlloc) - int64(before.HeapAlloc)
						runtime.KeepAlive(changed)
						runtime.KeepAlive(graph)
						runtime.KeepAlive(svc)
						if len(graph.construction.applied) != count {
							b.Fatal("incomplete replay")
						}
						for _, id := range ids {
							canonical, ok := svc.vectorIndex.getVectorRef(id)
							if !ok {
								b.Fatal("missing canonical record")
							}
							aliases := &canonical[0] == &coordinates.applied[id][0]
							if aliases != reuse {
								b.Fatal("replay ownership does not match comparison arm")
							}
						}
						if err := graph.construction.Close(); err != nil {
							b.Fatal(err)
						}
						if err := svc.Close(); err != nil {
							b.Fatal(err)
						}
						if err := engine.Close(); err != nil {
							b.Fatal(err)
						}
					}()
				}
				b.ReportMetric(float64(retained)/float64(b.N), "retained-B/generation")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/vector")
			})
		}
	}
}
