package storage

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// TestRelationshipPropertyEqualityIndexIsBuiltByItsConstraint covers the read
// side of the relationship equality index the constraint's validation owns: the
// declaration fills it once, and a later exact-value lookup answers from the
// postings — reading the one matching relationship — instead of scanning the
// type's relationships.
func TestRelationshipPropertyEqualityIndexIsBuiltByItsConstraint(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	const namespace = "pd"
	store := NewNamespacedEngine(engine, namespace)
	for _, id := range []string{"n73", "n128561"} {
		_, err := store.CreateNode(ctx, &Node{ID: NodeID(id), Labels: []string{"PDGraphNode"}, Properties: map[string]interface{}{"id": id}})
		require.NoError(t, err)
	}
	createRelationship := func(edgeID, value string) {
		require.NoError(t, store.CreateEdge(ctx, &Edge{
			ID:         EdgeID(edgeID),
			StartNode:  "n73",
			EndNode:    "n128561",
			Type:       "PD_RELATION",
			Properties: map[string]interface{}{"id": value},
		}))
	}
	for i := 0; i < 128; i++ {
		createRelationship(fmt.Sprintf("unrelated%d", i), fmt.Sprintf("x%d", i))
	}
	createRelationship("target", "r289999")

	constraint := Constraint{Name: "pd_relation_id", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "PD_RELATION", Properties: []string{"id"}}
	require.NoError(t, engine.GetSchemaForNamespace(namespace).AddConstraint(constraint, false))

	// A build scans the type's stored relationships; a lookup must not.
	builds := 0
	t.Cleanup(setUniqueConstraintScanHook(func() { builds++ }))
	require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, &WALEngine{engine: store}, constraint))
	require.Equal(t, 1, builds, "the declaration builds the index from the stored relationships")

	lookup, ok := any(store).(RelationshipPropertyEqualityLookup)
	require.True(t, ok)
	edges, handled, err := lookup.LookupEdgesByPropertyEquality(ctx, "PD_RELATION", []string{"id"}, []interface{}{"r289999"})
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("target"), edges[0].ID)
	require.Equal(t, NodeID("n73"), edges[0].StartNode)
	require.Equal(t, NodeID("n128561"), edges[0].EndNode)
	require.Equal(t, 1, builds, "an exact-value lookup reads postings, not the type's relationships")

	// Declaring the same constraint again (IF NOT EXISTS) rebuilds nothing.
	require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, &WALEngine{engine: store}, constraint))
	require.Equal(t, 1, builds)

	// The index follows the stored relationships: a changed value answers only
	// its new value, and a deleted relationship answers nothing.
	edge, err := engine.GetEdge(ctx, EdgeID("pd:target"))
	require.NoError(t, err)
	updated := *edge
	updated.Properties = map[string]interface{}{"id": "moved"}
	require.NoError(t, engine.UpdateEdge(ctx, EdgeUpdateRequest{Before: edge, After: &updated}))
	for value, want := range map[string]int{"r289999": 0, "moved": 1} {
		edges, handled, err := lookup.LookupEdgesByPropertyEquality(ctx, "PD_RELATION", []string{"id"}, []interface{}{value})
		require.NoError(t, err)
		require.True(t, handled)
		require.Len(t, edges, want, "lookup of %q", value)
	}
	require.NoError(t, engine.DeleteEdge(ctx, EdgeID("pd:target")))
	edges, handled, err = lookup.LookupEdgesByPropertyEquality(ctx, "PD_RELATION", []string{"id"}, []interface{}{"moved"})
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, edges)
}

// TestRelationshipPropertyEqualityLookupDeclinesWithoutItsIndex checks that the
// lookup declines invalid or unkeyable requests and a namespace-less engine,
// leaving the complete scan to the caller. Valid native requests may prepare
// a bounded derived index; the Cypher planner selects declared constraints.
func TestRelationshipPropertyEqualityLookupDeclinesUnsupportedRequests(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	store := NewNamespacedEngine(engine, "pd")
	lookup, ok := any(store).(RelationshipPropertyEqualityLookup)
	require.True(t, ok)

	for _, testCase := range []struct {
		name             string
		relationshipType string
		values           []interface{}
	}{
		{"no relationship type", "", []interface{}{"r289999"}},
		{"no properties", "PD_RELATION", nil},
		{"unkeyable value", "PD_RELATION", []interface{}{map[string]interface{}{"nested": "value"}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			properties := []string{"id"}
			_, handled, err := lookup.LookupEdgesByPropertyEquality(ctx, testCase.relationshipType, properties[:min(len(properties), len(testCase.values))], testCase.values)
			require.NoError(t, err)
			require.False(t, handled)
		})
	}

	_, handled, err := engine.LookupEdgesByPropertyEqualityInNamespace(ctx, "", "PD_RELATION", []string{"id"}, []interface{}{"r289999"})
	require.NoError(t, err)
	require.False(t, handled, "a namespace-less store keeps its scan")
}
