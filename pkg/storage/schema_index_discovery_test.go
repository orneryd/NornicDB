package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The index metadata discovery contracts (schema_index_discovery.go): which
// declared equality index each question may return. These pin the selection
// rules the seek, validation, write-maintenance and labelless-lookup paths rely
// on, so a consolidation of the scans cannot quietly widen them.

// A key index is the index on exactly the asked properties in the asked order:
// a reversed or shorter property list is a different index, and the label still
// scopes the answer.
func TestCompositeKeyIndexLockedRequiresExactPropertyOrder(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddCompositeIndex("pair", "Pair", []string{"left", "right"}))

	require.NotNil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Pair", []string{"left", "right"})))
	require.Nil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Pair", []string{"right", "left"})), "property order is part of the index")
	require.Nil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Pair", []string{"left"})), "a prefix is not the declared index")
	require.Nil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Pair", []string{"left", "right", "extra"})))
	require.Nil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Other", []string{"left", "right"})), "the label scopes the index")
}

// A constraint-owned key index answers the same question as a standalone one,
// but validation still waits for its fill.
func TestFilledKeyIndexFollowsTheFillStateOfTheOwnedIndex(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "fact_key",
		Type:       ConstraintNodeKey,
		Label:      "Fact",
		Properties: []string{"left", "right"},
	}))

	require.NotNil(t, checkedIndexRead[*CompositeIndex](t)(sm.compositeKeyIndexLocked(t.Context(), "Fact", []string{"left", "right"})), "the owned index is declared")
	require.Nil(t, checkedIndexRead[*CompositeIndex](t)(sm.filledKeyIndex(t.Context(), "Fact", []string{"left", "right"})), "an unfilled index is not validation's")

	sm.MarkPropertyIndexesFilled()

	require.NotNil(t, checkedIndexRead[*CompositeIndex](t)(sm.filledKeyIndex(t.Context(), "Fact", []string{"left", "right"})), "a filled index is validation's")
}

// The arity-1 question is about the single-property index: a composite index
// that happens to start with the property is not it, for seeks or for write
// maintenance.
func TestArity1PropertyIndexLockedRequiresTheSinglePropertyIndex(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddCompositeIndex("name_age", "Person", []string{"name", "age"}))

	idx, exists, err := sm.arity1PropertyIndexLocked(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.False(t, exists, "an arity-2 index on the label is not the arity-1 index")
	require.Nil(t, idx)
	require.False(t, sm.MaintainsPropertyIndex("Person", "name"), "write maintenance files into arity-1 indexes only")

	require.NoError(t, sm.AddPropertyIndex("person_name", "Person", []string{"name"}))

	idx, exists, err = sm.arity1PropertyIndexLocked(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "person_name", idx.Name)
	require.True(t, sm.MaintainsPropertyIndex("Person", "name"))
}

// The label question counts every declared equality index, whatever its arity,
// and only for the label that declares it.
func TestHasAnyPropertyIndexForLabelCountsEveryArity(t *testing.T) {
	sm := NewSchemaManager()
	require.False(t, sm.HasAnyPropertyIndexForLabel("Pair"))

	require.NoError(t, sm.AddCompositeIndex("pair", "Pair", []string{"left", "right"}))
	require.True(t, sm.HasAnyPropertyIndexForLabel("Pair"), "an arity-2 index is still an index on the label")
	require.False(t, sm.HasAnyPropertyIndexForLabel("Other"))

	require.NoError(t, sm.AddUniqueConstraint("rec_uid", "Rec", "uid"))
	require.True(t, sm.HasAnyPropertyIndexForLabel("Rec"), "an unfilled owned index is declared")
}

// The labelless lookup unions every arity-1 index that declares the property on
// any label, skips a composite index that merely starts with it, and refuses a
// half-filled set.
func TestPropertyIndexLookupAnyLabelUnionsArity1IndexesOnly(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("a_uid", "A", []string{"uid"}))
	require.NoError(t, sm.AddPropertyIndex("b_uid", "B", []string{"uid"}))
	require.NoError(t, sm.AddCompositeIndex("c_uid_ver", "C", []string{"uid", "ver"}))
	require.NoError(t, sm.PropertyIndexInsert("A", "uid", "a1", int64(7)))
	require.NoError(t, sm.PropertyIndexInsert("B", "uid", "b1", int64(7)))
	pair, ok := sm.GetCompositeIndex("c_uid_ver")
	require.True(t, ok)
	require.NoError(t, pair.IndexNode(t.Context(), "c1", map[string]interface{}{"uid": int64(7), "ver": int64(1)}))

	ids := checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookupAnyLabel(t.Context(), "uid", int64(7)))
	require.ElementsMatch(t, []NodeID{"a1", "b1"}, ids, "the union covers every label's arity-1 index and skips the composite one")

	// One unfilled arity-1 index on the same property closes the whole answer:
	// the union may not report a partial set.
	require.NoError(t, sm.AddUniqueConstraint("d_uid", "D", "uid"))
	ids = checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookupAnyLabel(t.Context(), "uid", int64(7)))
	require.Nil(t, ids)
}
