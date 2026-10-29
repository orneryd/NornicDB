package linkpredict

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkGraphCacheRead covers a 1,000-node graph with eight neighbors per
// node, including the cache's on-disk decoder and graph reconstruction.
func BenchmarkGraphCacheRead(b *testing.B) {
	cfg := DefaultBuildConfig()
	cfg.CachePath = b.TempDir()
	cfg.CacheTTL = time.Hour
	builder := NewGraphBuilder(nil, cfg)
	graph := make(Graph, 1000)
	for i := 0; i < 1000; i++ {
		neighbors := make(NodeSet, 8)
		for j := 0; j < 8; j++ {
			neighbors[storage.NodeID(fmt.Sprintf("node-%d", (i+j+1)%1000))] = struct{}{}
		}
		graph[storage.NodeID(fmt.Sprintf("node-%d", i))] = neighbors
	}
	if err := builder.saveToCache(graph); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(builder.cacheFilePath())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loaded, err := builder.loadFromCache()
		if err != nil || len(loaded) != len(graph) {
			b.Fatalf("cache read: %v, nodes=%d", err, len(loaded))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(info.Size()), "disk-B")
}
