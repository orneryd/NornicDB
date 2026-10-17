package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkClusterAdmissionWritePath measures the node write path (re-index
// with a changed vector and text) with clustering disabled, with k-means
// routing, and with IVF-HNSW. With clustering disabled the cluster
// admission helpers must cost nothing: allocs/op equal the base tree's.
//
//	go test ./pkg/search -run '^$' -bench BenchmarkClusterAdmissionWritePath -benchmem
func BenchmarkClusterAdmissionWritePath(b *testing.B) {
	for _, mode := range []struct {
		name       string
		clustering bool
		ivf        bool
	}{
		{name: "clustering=off"},
		{name: "clustering=kmeans", clustering: true},
		{name: "clustering=ivf_hnsw", clustering: true, ivf: true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			b.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			b.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
			if mode.ivf {
				b.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
			} else {
				b.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "false")
			}
			const dims, population = 16, 512
			rng := rand.New(rand.NewSource(1372))
			words := []string{"alpha", "beta", "gamma", "delta", "graph", "vector", "cluster", "route"}
			node := func(i, variant int) *storage.Node {
				vec := make([]float32, dims)
				for d := range vec {
					vec[d] = float32(rng.NormFloat64())
				}
				text := fmt.Sprintf("%s %s node %d", words[(i+variant)%len(words)], words[(i*3+variant)%len(words)], i)
				return &storage.Node{
					ID:              storage.NodeID(fmt.Sprintf("bench-%04d", i)),
					Labels:          []string{"Doc"},
					Properties:      map[string]any{"text": text},
					ChunkEmbeddings: [][]float32{vec},
				}
			}
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), dims)
			b.Cleanup(func() { _ = svc.Close() })
			if mode.clustering {
				svc.EnableClustering(nil, 8)
				svc.SetMinEmbeddingsForClustering(1)
			}
			updates := make([]*storage.Node, 0, 2*population)
			for i := 0; i < population; i++ {
				if err := svc.IndexNode(node(i, 0)); err != nil {
					b.Fatal(err)
				}
				updates = append(updates, node(i, 1), node(i, 0))
			}
			if mode.clustering {
				if err := svc.TriggerClustering(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := svc.IndexNode(updates[i%len(updates)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
