package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedManagedRelationshipIndex(t *testing.T) {
	engine := relationshipTestEngine(t)
	edge, err := engine.GetEdge(t.Context(), "r1")
	require.NoError(t, err)
	edge.Properties["editorial"] = "staff notes"
	require.NoError(t, engine.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{After: edge}))
	embed := &countingEmbed{}
	service := startRelationshipService(t, engine, embed.embed, nil)
	waitRelationshipVectors(t, service, 2)

	nodes := auth.FineGrainedScope{Kind: auth.FineGrainedNode}
	edges := auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "db", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "nodes", Action: "graph.node.traverse", Scope: nodes},
			{ID: "edges", Action: "graph.relationship.traverse", Scope: edges},
			{ID: "properties", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{
				Kind: auth.FineGrainedRelationship, Properties: []string{"reason"},
			}},
			{ID: "vectors", Action: "graph.relationship.embedding.read", Scope: edges},
			{ID: "all-source", Action: "graph.relationship.property.read", Scope: edges},
		},
		Assignments: map[string][]string{
			"reader":        {"db", "nodes", "edges", "properties"},
			"vector-reader": {"db", "nodes", "edges", "properties", "vectors"},
			"source-reader": {"db", "nodes", "edges", "all-source", "vectors"},
		},
	})
	require.NoError(t, err)

	minimum := 0.6
	options := &SearchOptions{Limit: 10, MinSimilarity: &minimum}
	query := wordVector("span")
	calls := embed.calls.Load()
	for _, role := range []string{"reader", "vector-reader", "source-reader"} {
		t.Run(role, func(t *testing.T) {
			scope := &auth.FineGrainedRequest{
				Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "test-id" },
			}
			ctx := auth.WithFineGrainedRequest(t.Context(), scope)
			keyword, err := service.SearchRelationships(ctx, "zephyrine", nil, options)
			require.NoError(t, err)
			require.Len(t, keyword.Results, 1)
			require.Equal(t, "r1", keyword.Results[0].ID)
			require.Equal(t, "zephyrine bridge over the river", keyword.Results[0].Properties["reason"])
			if role != "source-reader" {
				require.NotContains(t, keyword.Results[0].Properties, "editorial")
			}
			got, err := service.SearchRelationships(ctx, "", query, options)
			require.NoError(t, err)
			if role != "source-reader" {
				require.Empty(t, got.Results)
				return
			}
			require.Len(t, got.Results, 1)
			require.Equal(t, "r1", got.Results[0].ID)
			require.Equal(t, 1, got.Results[0].VectorRank)
		})
	}
	require.Equal(t, calls, embed.calls.Load(), "queries must reuse existing vectors")
}

func TestFineGrainedSharedPropertyVectors(t *testing.T) {
	ctx := t.Context()
	engine := relationshipTestEngine(t)
	_, err := engine.CreateNode(ctx, &storage.Node{
		ID: "shared-vector", Labels: []string{"Doc"}, Properties: map[string]any{"position": []float32{1, 0, 0}},
	})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &storage.Edge{
		ID: "stored-vector", Type: "CITES", StartNode: "a", EndNode: "b",
		Properties: map[string]any{"position": []float32{1, 0, 0}},
	}))
	service := NewService(engine, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "db", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "nodes", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "edges", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "node-position", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{
				Kind: auth.FineGrainedNode, Properties: []string{"position"},
			}},
			{ID: "edge-position", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{
				Kind: auth.FineGrainedRelationship, Properties: []string{"position"},
			}},
		},
		Assignments: map[string][]string{
			"reader":    {"db", "nodes", "edges"},
			"positions": {"db", "nodes", "edges", "node-position", "edge-position"},
		},
	})
	require.NoError(t, err)

	for _, role := range []string{"positions", "reader", "positions"} {
		scope := &auth.FineGrainedRequest{
			Access: policy.ForRoles([]string{role}), DatabaseID: func(string) string { return "test-id" },
		}
		scoped := auth.WithFineGrainedRequest(ctx, scope)
		for _, exact := range []bool{false, true} {
			hits, err := service.VectorQueryNodes(scoped, []float32{1, 0, 0}, VectorQuerySpec{
				Property: "position", Limit: 1, Exact: exact,
			})
			require.NoError(t, err)
			if role == "positions" {
				require.Len(t, hits, 1)
				require.Equal(t, "shared-vector", hits[0].ID)
			} else {
				require.Empty(t, hits)
			}
		}
		hits, err := service.VectorQueryRelationships(scoped, []float32{1, 0, 0}, RelationshipVectorQuerySpec{
			Type: "CITES", Property: "position", Limit: 1,
		})
		require.NoError(t, err)
		if role == "positions" {
			require.Len(t, hits, 1)
			require.Equal(t, "stored-vector", hits[0].ID)
		} else {
			require.Empty(t, hits)
		}
	}
}
