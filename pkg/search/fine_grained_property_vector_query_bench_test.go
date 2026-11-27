package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkPropertyVectorQueryPropertyIndex measures a Cypher vector query that
// names a property index. The property-vector supplement scores the whole index
// population; a restricted fine-grained scope authorizes the population once
// per graph/policy revision and reuses those admission decisions on warm reads.
func BenchmarkPropertyVectorQueryPropertyIndex(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000000")
	b.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	const (
		dims  = 32
		nodes = 2000
		limit = 10
	)
	rng := rand.New(rand.NewSource(11))
	randomVector := func() []float32 {
		vector := make([]float32, dims)
		for i := range vector {
			vector[i] = rng.Float32()*2 - 1
		}
		return vector
	}
	engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(engine, "work")
	for i := range nodes {
		labels := []string{"Doc"}
		if i%2 == 0 {
			labels = append(labels, "PublicVector")
		}
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("doc-%d", i)),
			Labels:     labels,
			Properties: map[string]any{"vec": randomVector()},
		}
		if _, err := store.CreateNode(b.Context(), node); err != nil {
			b.Fatal(err)
		}
	}
	service := NewService(store, dims, nil)
	b.Cleanup(func() {
		if err := service.Close(); err != nil {
			b.Errorf("closing search service: %v", err)
		}
	})
	service.SetIndexFlags(false, true)
	if err := service.BuildIndexes(b.Context()); err != nil {
		b.Fatal(err)
	}

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "docs", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "vector-source", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}, Properties: []string{"vec"}}},
			{ID: "partial-source", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"PublicVector"}, Properties: []string{"vec"}}},
		},
		Assignments: map[string][]string{
			"reader":  {"access", "docs", "vector-source"},
			"partial": {"access", "docs", "partial-source"},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	request := auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" },
		Fingerprint: "policy-benchmark",
	}
	partialRequest := auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"partial"}), DatabaseID: func(string) string { return "work-id" },
		Fingerprint: "policy-partial-benchmark",
	}
	query := randomVector()
	spec := VectorQuerySpec{Label: "Doc", Property: "vec", Limit: limit}
	for _, variant := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "unscoped", ctx: b.Context()},
		{name: "whole-index", ctx: auth.WithFineGrainedRequest(b.Context(), &request)},
		{name: "restricted-warm", ctx: auth.WithFineGrainedRequest(b.Context(), &partialRequest)},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				hits, err := service.VectorQueryNodes(variant.ctx, query, spec)
				if err != nil {
					b.Fatal(err)
				}
				if len(hits) != limit {
					b.Fatalf("page = %d hits, want %d", len(hits), limit)
				}
			}
		})
	}
}
