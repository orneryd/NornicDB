package storage

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// TestRelationshipPropertyEqualityLookupSeparatesSurrogatePropertyNames is the
// read side of the surrogate-name integrity defect recorded with NOR-42: two
// constraints on distinct escaped surrogate property names shared a lossy JSON
// runtime key, so one property's derived index could answer the other's value.
// relationshipPropertyKey now marshals the ordered names losslessly
// (jsontext.Marshal), and the uniqueness control
// TestRelationshipUniqueIndexSeparatesSurrogatePropertyNames covers the write
// side. This control requires the equality lookup to answer each property from
// its own postings: no cross-property row, and a value only the other property
// carries is not found.
func TestRelationshipPropertyEqualityLookupSeparatesSurrogatePropertyNames(t *testing.T) {
	first, second := "\xed\xa0\x80", "\xed\xb0\x80"
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	const namespace = "pd"
	store := NewNamespacedEngine(engine, namespace)
	for _, id := range []string{"a", "b"} {
		_, err := store.CreateNode(ctx, &Node{ID: NodeID(id)})
		require.NoError(t, err)
	}
	for edgeID, properties := range map[string]map[string]interface{}{
		"first":  {first: "a", second: "x"},
		"second": {first: "b", second: "y"},
	} {
		require.NoError(t, store.CreateEdge(ctx, &Edge{
			ID:         EdgeID(edgeID),
			StartNode:  "a",
			EndNode:    "b",
			Type:       "T",
			Properties: properties,
		}))
	}

	// Each surrogate property has its own UNIQUE constraint and its own
	// declaration-time derived index; a shared runtime key would make the
	// second declaration reuse the first property's index.
	for i, property := range []string{first, second} {
		constraint := Constraint{
			Name:       fmt.Sprintf("surrogate_%d", i),
			Type:       ConstraintUnique,
			EntityType: ConstraintEntityRelationship,
			Label:      "T",
			Properties: []string{property},
		}
		require.NoError(t, engine.GetSchemaForNamespace(namespace).AddConstraint(constraint, false))
		require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, &WALEngine{engine: store}, constraint))
	}

	lookup, ok := any(store).(RelationshipPropertyEqualityLookup)
	require.True(t, ok)
	for _, testCase := range []struct {
		property string
		value    string
		want     []EdgeID
	}{
		{first, "a", []EdgeID{"first"}},
		{first, "b", []EdgeID{"second"}},
		{second, "x", []EdgeID{"first"}},
		{second, "y", []EdgeID{"second"}},
		// These values belong only to the other property's name; the shared-key
		// defect answered them with that other property's relationship.
		{first, "x", nil},
		{second, "a", nil},
	} {
		edges, handled, err := lookup.LookupEdgesByPropertyEquality(ctx, "T", []string{testCase.property}, []interface{}{testCase.value})
		require.NoError(t, err)
		require.True(t, handled)
		ids := make([]EdgeID, 0, len(edges))
		for _, edge := range edges {
			ids = append(ids, edge.ID)
		}
		require.ElementsMatch(t, testCase.want, ids, "lookup of %q under property %q", testCase.value, testCase.property)
	}
}
