package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). The release constraint-snapshot
// controls include metadata emitted by the same publication-key producer.
var capturedPublicationView publicationConstraintView

func TestPublicationConstraintViewCaptureAllocationBudget(t *testing.T) {
	schema := NewSchemaManager()
	require.Zero(t, testing.AllocsPerRun(100, func() {
		var err error
		capturedPublicationView, err = capturePublicationConstraintView(t.Context(), schema)
		require.NoError(t, err)
	}))
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		var err error
		capturedPublicationView, err = capturePublicationConstraintView(t.Context(), schema)
		require.NoError(t, err)
	}))
	require.Empty(t, capturedPublicationView.rest)
	require.Equal(t, "uid", capturedPublicationView.first.property)
}

func TestPublicationConstraintViewPreservesNodeKeysAndNamespace(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	view, viewErr := capturePublicationConstraintView(t.Context(), schema)
	require.NoError(t, viewErr)
	old := &Node{ID: "alpha:n", Labels: []string{"Person"}, Properties: map[string]any{"uid": "old"}}
	written := &Node{ID: old.ID, Labels: old.Labels, Properties: map[string]any{"uid": "new"}}
	want := []any{old.ID,
		AffectedFact{NodeID: old.ID},
		AffectedFact{NodeID: old.ID, Label: "Person", Before: true},
		AffectedFact{NodeID: old.ID, Label: "Person"},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "old"}},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "new"}},
	}
	publicationKeys1, publicationErr := appendNodePublicationKeys(t.Context(), nil, &view, old.ID, old, written)
	require.NoError(t, publicationErr)
	require.Equal(t, want, publicationKeys1)
	unchanged := append([]any(nil), want[:4]...)
	unchanged[3] = AffectedFact{NodeID: old.ID, Label: "Person", Before: true}
	unchanged = append(unchanged, want[4])
	publicationKeys2, publicationErr := appendNodePublicationKeys(t.Context(), nil, &view, old.ID, old, old)
	require.NoError(t, publicationErr)
	require.Equal(t, unchanged, publicationKeys2, "unchanged values have one dependency")
	other := *written
	other.ID = "beta:n"
	publicationKeys3, publicationErr := appendNodePublicationKeys(t.Context(), nil, &view, other.ID, nil, &other)
	require.NoError(t, publicationErr)
	require.Equal(t, []any{other.ID, AffectedFact{NodeID: other.ID}, AffectedFact{NodeID: other.ID, Label: "Person"}, publicationUniqueKey{namespace: "beta", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "new"}}}, publicationKeys3)
	require.NoError(t, schema.DropConstraint("uid"))
	publicationKeys4, publicationErr := appendNodePublicationKeys(t.Context(), nil, &view, old.ID, old, written)
	require.NoError(t, publicationErr)
	require.Equal(t, want, publicationKeys4, "an in-flight snapshot retains its rules")
	fresh, viewErr := capturePublicationConstraintView(t.Context(), schema)
	require.NoError(t, viewErr)
	publicationKeys5, publicationErr := appendNodePublicationKeys(t.Context(), nil, &fresh, old.ID, old, written)
	require.NoError(t, publicationErr)
	require.Equal(t, want[:4], publicationKeys5)
}

func TestPublicationConstraintViewOwnsCompositeRelationshipProperties(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "KNOWS", Properties: []string{"since", "weight"}}))
	view, viewErr := capturePublicationConstraintView(t.Context(), schema)
	require.NoError(t, viewErr)
	old := &Edge{ID: "alpha:e", Type: "KNOWS", Properties: map[string]any{"since": int64(1), "weight": int64(2)}}
	written := &Edge{ID: old.ID, Type: old.Type, Properties: map[string]any{"since": int64(3), "weight": int64(2)}}
	property := relationshipPropertyKey([]string{"since", "weight"})
	oldValues, ok, keyErr := relationshipConstraintValueKeys(t.Context(), []string{"since", "weight"}, old.Properties, true)
	require.NoError(t, keyErr)
	require.True(t, ok)
	newValues, ok, keyErr := relationshipConstraintValueKeys(t.Context(), []string{"since", "weight"}, written.Properties, true)
	require.NoError(t, keyErr)
	require.True(t, ok)
	oldKey, keyErr := NewCompositeKey(t.Context(), oldValues...)
	require.NoError(t, keyErr)
	newKey, keyErr := NewCompositeKey(t.Context(), newValues...)
	require.NoError(t, keyErr)
	want := []any{old.ID,
		AffectedFact{EdgeID: old.ID},
		AffectedFact{EdgeID: old.ID, Type: "KNOWS", Before: true},
		AffectedFact{EdgeID: old.ID, Type: "KNOWS"},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "\x00relationship:KNOWS", property: property, value: oldKey.Hash}},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "\x00relationship:KNOWS", property: property, value: newKey.Hash}},
	}
	schema.mu.Lock()
	schema.constraints["rel"].Properties[0] = "changed"
	schema.mu.Unlock()
	publicationKeys6, publicationErr := appendEdgePublicationKeys(t.Context(), nil, &view, old.ID, old, written)
	require.NoError(t, publicationErr)
	require.Equal(t, want, publicationKeys6, "captured composite property names remain owned")
	unrelated := *written
	unrelated.Type = "OTHER"
	publicationKeys7, publicationErr := appendEdgePublicationKeys(t.Context(), nil, &view, old.ID, nil, &unrelated)
	require.NoError(t, publicationErr)
	require.Equal(t, []any{old.ID, AffectedFact{EdgeID: old.ID}, AffectedFact{EdgeID: old.ID, Type: "OTHER"}}, publicationKeys7)
	node := &Node{ID: "alpha:n", Labels: []string{"Person"}, Properties: map[string]any{"uid": "u"}}
	ops := []Operation{{Type: OpCreateNode, Node: node}, {Type: OpUpdateEdge, OldEdge: old, Edge: written}, {Type: OpDeleteNode, NodeID: "alpha:gone", DeletedEdgeIDs: []EdgeID{"alpha:removed"}}}
	expected := []any{node.ID, AffectedFact{NodeID: node.ID}, AffectedFact{NodeID: node.ID, Label: "Person"}, publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "u"}}}
	expected = append(expected, want...)
	expected = append(expected, NodeID("alpha:gone"), AffectedFact{NodeID: "alpha:gone", Deleted: true}, EdgeID("alpha:removed"), AffectedFact{EdgeID: "alpha:removed", Deleted: true})
	publicationKeys8, publicationErr := appendOperationPublicationKeys(t.Context(), nil, &view, ops)
	require.NoError(t, publicationErr)
	require.Equal(t, expected, publicationKeys8)
}
