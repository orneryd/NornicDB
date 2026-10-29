package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkNodeVectorSearch measures ordinary node vector search through the
// public routes, on single-vector nodes and on chunked nodes (whose later
// chunks are separate vector entries collapsed to their node), plus the
// IndexNode write path that builds the entries.
func BenchmarkNodeVectorSearch(b *testing.B) {
	const dims = 64
	for _, shape := range []struct {
		name   string
		nodes  int
		chunks int
	}{
		{name: "single", nodes: 4000, chunks: 1},
		{name: "chunked", nodes: 1500, chunks: 4},
	} {
		rng := rand.New(rand.NewSource(7))
		randomVector := func() []float32 {
			vector := make([]float32, dims)
			for i := range vector {
				vector[i] = rng.Float32()*2 - 1
			}
			return vector
		}
		nodes := make([]*storage.Node, shape.nodes)
		for i := range nodes {
			chunks := make([][]float32, shape.chunks)
			for c := range chunks {
				chunks[c] = randomVector()
			}
			nodes[i] = &storage.Node{
				ID:              storage.NodeID(fmt.Sprintf("doc-%d", i)),
				Labels:          []string{"Doc"},
				ChunkEmbeddings: chunks,
			}
		}
		engine := storage.NewNamespacedEngine(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
		for _, node := range nodes {
			if _, err := engine.CreateNode(node); err != nil {
				b.Fatal(err)
			}
		}
		svc := NewService(engine, dims, nil)
		svc.resultCache = nil // measure search, not cached responses
		for _, node := range nodes {
			if err := svc.IndexNode(node); err != nil {
				b.Fatal(err)
			}
		}
		queries := make([][]float32, 64)
		for i := range queries {
			queries[i] = randomVector()
		}
		ctx := context.Background()
		options := func() *SearchOptions {
			opts := DefaultSearchOptions()
			opts.Limit = 10
			minSimilarity := -1.0
			opts.MinSimilarity = &minSimilarity
			return opts
		}
		// Build the ANN pipeline before timing.
		if _, err := svc.VectorSearchCandidates(ctx, queries[0], options()); err != nil {
			b.Fatal(err)
		}
		response, err := svc.Search(ctx, "", queries[0], options())
		if err != nil || len(response.Results) == 0 {
			b.Fatalf("benchmark must materialize stored nodes: response=%v err=%v", response, err)
		}
		if svc.EmbeddingCount() != shape.nodes*shape.chunks {
			b.Fatal("benchmark search removed valid vector entries")
		}

		b.Run(shape.name+"/candidates", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := svc.VectorSearchCandidates(ctx, queries[i%len(queries)], options()); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/search", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := svc.Search(ctx, "", queries[i%len(queries)], options()); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/cypher", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := svc.VectorQueryNodes(ctx, queries[i%len(queries)], VectorQuerySpec{Label: "Doc", Limit: 10}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/index_node", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				node := nodes[i%len(nodes)]
				updated := *node
				updated.ChunkEmbeddings = append([][]float32(nil), node.ChunkEmbeddings...)
				updated.ChunkEmbeddings[0] = queries[i%len(queries)] // a real change, not the unchanged fast path
				if err := svc.IndexNode(&updated); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = svc.Close()
		_ = engine.Close()
	}
}
