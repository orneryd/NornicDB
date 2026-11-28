package storage

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
)

// BenchmarkProjectedReadsWiden measures a batch of narrow cache entries
// widened to realistic large fields. Preparation and cache eviction are not
// included in the reported time; the batch size permits profiling the fill.
func BenchmarkProjectedReadsWiden(b *testing.B) {
	ctx := context.Background()
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	const batchSize = 128
	ids := make([]NodeID, batchSize)
	body := strings.Repeat("documentation body ", 512)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("bench:widen-%d", i))
		_, err := engine.CreateNode(ctx, &Node{ID: ids[i], Labels: []string{"Document"}, Properties: map[string]any{
			"key": string(ids[i]), "path": "/docs/example", "status": "published",
			"body": body, "noise": make([]byte, 4096), "metrics": []int64{1, 2, 3, 4},
		}})
		if err != nil {
			b.Fatal(err)
		}
	}
	narrow := []string{"key", "path", "status"}
	wide := []string{"body", "noise", "metrics"}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		for _, id := range ids {
			engine.cacheDeleteNode(id, nodeCacheAllForms)
			if _, err := engine.GetNodeProjected(ctx, id, narrow); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		pprof.Do(ctx, pprof.Labels("phase", "widen"), func(ctx context.Context) {
			for _, id := range ids {
				if _, err := engine.GetNodeProjected(ctx, id, wide); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.ReportMetric(batchSize, "nodes/op")
}
