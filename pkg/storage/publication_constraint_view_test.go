package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var capturedPublicationView publicationConstraintView

func TestPublicationConstraintViewCaptureAllocationBudget(t *testing.T) {
	schema := NewSchemaManager()
	require.Zero(t, testing.AllocsPerRun(100, func() { capturedPublicationView = capturePublicationConstraintView(schema) }))
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	require.Zero(t, testing.AllocsPerRun(100, func() { capturedPublicationView = capturePublicationConstraintView(schema) }))
	require.Empty(t, capturedPublicationView.rest)
	require.Equal(t, "uid", capturedPublicationView.first.property)
}

func TestPublicationConstraintViewPreservesNodeKeysAndNamespace(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	view := capturePublicationConstraintView(schema)
	old := &Node{ID: "alpha:n", Labels: []string{"Person"}, Properties: map[string]any{"uid": "old"}}
	written := &Node{ID: old.ID, Labels: old.Labels, Properties: map[string]any{"uid": "new"}}
	want := []any{old.ID,
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "old"}},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "new"}},
	}
	require.Equal(t, want, appendNodePublicationKeys(nil, &view, old.ID, old, written))
	require.Equal(t, want[:2], appendNodePublicationKeys(nil, &view, old.ID, old, old), "unchanged values have one dependency")
	other := *written
	other.ID = "beta:n"
	require.Equal(t, []any{other.ID, publicationUniqueKey{namespace: "beta", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "new"}}}, appendNodePublicationKeys(nil, &view, other.ID, nil, &other))
	require.NoError(t, schema.DropConstraint("uid"))
	require.Equal(t, want, appendNodePublicationKeys(nil, &view, old.ID, old, written), "an in-flight snapshot retains its rules")
	fresh := capturePublicationConstraintView(schema)
	require.Equal(t, []any{old.ID}, appendNodePublicationKeys(nil, &fresh, old.ID, old, written))
}

func TestPublicationConstraintViewOwnsCompositeRelationshipProperties(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("uid", "Person", "uid"))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "KNOWS", Properties: []string{"since", "weight"}}))
	view := capturePublicationConstraintView(schema)
	old := &Edge{ID: "alpha:e", Type: "KNOWS", Properties: map[string]any{"since": int64(1), "weight": int64(2)}}
	written := &Edge{ID: old.ID, Type: old.Type, Properties: map[string]any{"since": int64(3), "weight": int64(2)}}
	property := relationshipPropertyKey([]string{"since", "weight"})
	oldValues, ok := relationshipConstraintValueKeys([]string{"since", "weight"}, old.Properties, true)
	require.True(t, ok)
	newValues, ok := relationshipConstraintValueKeys([]string{"since", "weight"}, written.Properties, true)
	require.True(t, ok)
	want := []any{old.ID,
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "\x00relationship:KNOWS", property: property, value: NewCompositeKey(oldValues...).Hash}},
		publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "\x00relationship:KNOWS", property: property, value: NewCompositeKey(newValues...).Hash}},
	}
	schema.mu.Lock()
	schema.constraints["rel"].Properties[0] = "changed"
	schema.mu.Unlock()
	require.Equal(t, want, appendEdgePublicationKeys(nil, &view, old.ID, old, written), "captured composite property names remain owned")
	unrelated := *written
	unrelated.Type = "OTHER"
	require.Equal(t, []any{old.ID}, appendEdgePublicationKeys(nil, &view, old.ID, nil, &unrelated))
	node := &Node{ID: "alpha:n", Labels: []string{"Person"}, Properties: map[string]any{"uid": "u"}}
	ops := []Operation{{Type: OpCreateNode, Node: node}, {Type: OpUpdateEdge, OldEdge: old, Edge: written}, {Type: OpDeleteNode, NodeID: "alpha:gone", DeletedEdgeIDs: []EdgeID{"alpha:removed"}}}
	expected := []any{node.ID, publicationUniqueKey{namespace: "alpha", value: uniqueConstraintLockKey{label: "Person", property: "uid", value: "u"}}}
	expected = append(expected, want...)
	expected = append(expected, NodeID("alpha:gone"), EdgeID("alpha:removed"))
	require.Equal(t, expected, appendOperationPublicationKeys(nil, &view, ops))
}
