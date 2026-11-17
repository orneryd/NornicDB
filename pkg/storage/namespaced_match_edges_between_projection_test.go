package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNamespacedMatchEdgesBetweenPredicateProjection is the report53.14
// regression: NamespacedEngine.MatchEdgesBetween copied each candidate's ID and
// endpoints itself and left Database as the inner engine reported it, so the
// predicate saw an identity image that differed from the edges the same call
// returned. The predicate now receives the canonical toUserEdge projection.
func TestNamespacedMatchEdgesBetweenPredicateProjection(t *testing.T) {
	fixtures := []struct {
		name string
		open func(t *testing.T) Engine
	}{
		{name: "memory", open: func(t *testing.T) Engine { return newTestMemoryEngine(t) }},
		{name: "native", open: func(t *testing.T) Engine { return newIsolatedBadgerEngine(t) }},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			inner := fixture.open(t)
			tenant := NewNamespacedEngine(inner, "tenant_a")
			unrelated := NewNamespacedEngine(inner, "tenant_b")
			ctx := t.Context()

			for _, scope := range []*NamespacedEngine{tenant, unrelated} {
				for _, id := range []NodeID{"a", "b"} {
					_, err := scope.CreateNode(ctx, &Node{ID: id})
					require.NoError(t, err)
				}
			}
			require.NoError(t, tenant.CreateEdge(ctx, &Edge{
				ID: "e1", StartNode: "a", EndNode: "b", Type: "LINK",
				Properties: map[string]any{"k": "v"},
			}))
			require.NoError(t, unrelated.CreateEdge(ctx, &Edge{
				ID: "e2", StartNode: "a", EndNode: "b", Type: "LINK",
				Properties: map[string]any{"k": "v"},
			}))

			inputs := []struct {
				name       string
				start, end NodeID
			}{
				{name: "unqualified input", start: "a", end: "b"},
				{name: "qualified input", start: "tenant_a:a", end: "tenant_a:b"},
			}
			for _, input := range inputs {
				t.Run(input.name, func(t *testing.T) {
					var images []Edge
					matched, err := tenant.MatchEdgesBetween(ctx, input.start, input.end, "LINK", []string{"k"}, func(edge *Edge) bool {
						require.NotNil(t, edge)
						images = append(images, *edge)
						return true
					}, SnapshotEdgeMatch)
					require.NoError(t, err)
					require.Len(t, images, 1, "only tenant_a's edge may reach the predicate")
					require.Len(t, matched, 1)

					returned := matched[0]
					image := images[0]
					assert.Equal(t, "tenant_a", returned.Database)
					assert.Equal(t, returned.Database, image.Database,
						"the predicate image carries the same database as the returned projection")
					assert.Equal(t, returned.ID, image.ID)
					assert.Equal(t, returned.StartNode, image.StartNode)
					assert.Equal(t, returned.EndNode, image.EndNode)
					assert.Equal(t, returned.Type, image.Type)
					assert.Equal(t, EdgeID("e1"), image.ID)
					assert.Equal(t, NodeID("a"), image.StartNode)
					assert.Equal(t, NodeID("b"), image.EndNode)
					assert.Equal(t, map[string]any{"k": "v"}, image.Properties)
					assert.Equal(t, EdgeID("e1"), returned.ID)
					assert.Equal(t, NodeID("a"), returned.StartNode)
					assert.Equal(t, NodeID("b"), returned.EndNode)
				})
			}

			// Unrelated namespace control: the same read in tenant_b sees only its
			// own edge and the same canonical database projection.
			var controlImages []Edge
			control, err := unrelated.MatchEdgesBetween(ctx, "a", "b", "LINK", []string{"k"}, func(edge *Edge) bool {
				require.NotNil(t, edge)
				controlImages = append(controlImages, *edge)
				return true
			}, SnapshotEdgeMatch)
			require.NoError(t, err)
			require.Len(t, controlImages, 1)
			require.Len(t, control, 1)
			assert.Equal(t, EdgeID("e2"), control[0].ID)
			assert.Equal(t, "tenant_b", control[0].Database)
			assert.Equal(t, control[0].Database, controlImages[0].Database)
			assert.Equal(t, control[0].ID, controlImages[0].ID)
		})
	}
}
