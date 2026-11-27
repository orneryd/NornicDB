package search

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkFineGrainedKeywordSearch compares the steady-state public search
// path with and without a scoped policy over the same indexed population.
func BenchmarkFineGrainedKeywordSearch(b *testing.B) {
	for _, population := range []int{500, 5000} {
		b.Run(fmt.Sprintf("nodes=%d", population), func(b *testing.B) {
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = base.Close() })
			engine := storage.NewNamespacedEngine(base, "work")
			ctx := context.Background()
			for i := 0; i < population; i++ {
				label := "Public"
				if i%2 == 0 {
					label = "Private"
				}
				_, err := engine.CreateNode(ctx, &storage.Node{
					ID: storage.NodeID(fmt.Sprintf("doc-%06d", i)), Labels: []string{label},
					Properties: map[string]any{"content": "harbor lighthouse search data", "draft": "hidden winter text"},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			service := NewService(engine, 3, nil)
			b.Cleanup(func() { _ = service.Close() })
			service.SetIndexFlags(true, false)
			service.resultCache = nil
			if err := service.BuildIndexes(ctx); err != nil {
				b.Fatal(err)
			}
			policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
				Definitions: []auth.FineGrainedEntitlement{
					{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
					{ID: "public", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Public"}}},
					{ID: "content", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"content"}}},
				},
				Assignments: map[string][]string{"reader": {"access", "public", "content"}},
			})
			if err != nil {
				b.Fatal(err)
			}
			request := &auth.FineGrainedRequest{Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" }, Fingerprint: "reader-policy"}
			restricted := auth.WithFineGrainedRequest(ctx, request)
			for _, mode := range []struct {
				name string
				ctx  context.Context
			}{{"unrestricted", ctx}, {"restricted", restricted}} {
				b.Run(mode.name, func(b *testing.B) {
					if _, err := service.Search(mode.ctx, "lighthouse", nil, &SearchOptions{Limit: 10}); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := service.Search(mode.ctx, "lighthouse", nil, &SearchOptions{Limit: 10}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// BenchmarkFineGrainedVectorSearch measures the shared vector candidate path
// and the cost of filling a visible page after FGAC filters candidates.
func BenchmarkFineGrainedVectorSearch(b *testing.B) {
	benchmarkFineGrainedVectorSearch(b, 1)
}

func BenchmarkFineGrainedVectorSearchFourEntries(b *testing.B) {
	benchmarkFineGrainedVectorSearch(b, 4)
}

func benchmarkFineGrainedVectorSearch(b *testing.B, entriesPerNode int) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000000")
	b.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	const population = 2000
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "work")
	ctx := context.Background()
	for i := 0; i < population; i++ {
		label := "Public"
		if i%2 == 0 {
			label = "Private"
		}
		vectors := make([][]float32, entriesPerNode)
		for entry := range vectors {
			vectors[entry] = []float32{float32(1 + i%17 + entry), float32(1 + i%7), 1}
		}
		_, err := engine.CreateNode(ctx, &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("vec-%06d", i)), Labels: []string{label},
			ChunkEmbeddings: vectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	service := NewService(engine, 3, nil)
	b.Cleanup(func() { _ = service.Close() })
	service.SetIndexFlags(false, true)
	service.resultCache = nil
	if err := service.BuildIndexes(ctx); err != nil {
		b.Fatal(err)
	}
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "public", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Public"}}},
			{ID: "properties", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "embedding", Action: "graph.node.embedding.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
		},
		Assignments: map[string][]string{"reader": {"access", "public", "properties", "embedding"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	request := &auth.FineGrainedRequest{Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" }, Fingerprint: "reader-policy"}
	restricted := auth.WithFineGrainedRequest(ctx, request)
	query := []float32{1, 0, 0}
	for _, mode := range []struct {
		name string
		ctx  context.Context
	}{{"unrestricted", ctx}, {"restricted", restricted}} {
		b.Run(mode.name, func(b *testing.B) {
			options := DefaultSearchOptions()
			options.Limit = 10
			minimum := -1.0
			options.MinSimilarity = &minimum
			if _, err := service.Search(mode.ctx, "", query, options); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := service.Search(mode.ctx, "", query, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
