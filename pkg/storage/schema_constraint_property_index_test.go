package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A uniqueness constraint's own property index (#875) serves seeks only once
// it has been filled; until then writes maintain it and seeks scan.
func TestConstraintPropertyIndexSeekableOnceFilled(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddUniqueConstraint("c_uid", "Rec", "uid"))

	idx, owned := sm.ConstraintPropertyIndex("c_uid")
	require.True(t, owned)
	require.Equal(t, "c_uid", idx.OwningConstraint)
	require.Equal(t, "c_uid", idx.Name)
	_, owned = sm.ConstraintPropertyIndex("missing")
	require.False(t, owned)

	_, ok := sm.GetPropertyIndex("Rec", "uid")
	require.False(t, ok)
	require.False(t, sm.HasPropertyIndex("Rec", "uid"))
	require.True(t, sm.MaintainsPropertyIndex("Rec", "uid"))
	require.True(t, sm.HasAnyPropertyIndexForLabel("Rec"))

	require.NoError(t, sm.PropertyIndexInsert("Rec", "uid", "n1", "a"))
	require.Nil(t, sm.PropertyIndexLookup("Rec", "uid", "a"))
	require.Nil(t, sm.PropertyIndexLookupAnyLabel("uid", "a"))
	require.Nil(t, sm.PropertyIndexAllNonNil("Rec", "uid", false))
	require.False(t, sm.VisitPropertyIndexGroups("Rec", "uid", false, func([]NodeID) bool { return true }))

	require.NoError(t, sm.BackfillPropertyIndex("Rec", "uid", map[NodeID]interface{}{"n2": "b"}))
	_, ok = sm.GetPropertyIndex("Rec", "uid")
	require.True(t, ok)
	require.True(t, sm.HasPropertyIndex("Rec", "uid"))
	require.Equal(t, []NodeID{"n1"}, sm.PropertyIndexLookup("Rec", "uid", "a"))
	require.Equal(t, []NodeID{"n2"}, sm.PropertyIndexLookupAnyLabel("uid", "b"))
	require.ElementsMatch(t, []NodeID{"n1", "n2"}, sm.PropertyIndexAllNonNil("Rec", "uid", false))
	require.True(t, sm.VisitPropertyIndexGroups("Rec", "uid", false, func([]NodeID) bool { return true }))
}

// A label-less lookup can't be answered from the indexes while one of the
// indexes on the property is unfilled.
func TestPropertyIndexLookupAnyLabelSkipsWhileAnIndexIsUnfilled(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("i_a", "A", []string{"uid"}))
	require.NoError(t, sm.PropertyIndexInsert("A", "uid", "a1", "x"))
	require.Equal(t, []NodeID{"a1"}, sm.PropertyIndexLookupAnyLabel("uid", "x"))

	require.NoError(t, sm.AddUniqueConstraint("c_b", "B", "uid"))
	require.Nil(t, sm.PropertyIndexLookupAnyLabel("uid", "x"))

	sm.MarkPropertyIndexesFilled()
	require.Equal(t, []NodeID{"a1"}, sm.PropertyIndexLookupAnyLabel("uid", "x"))
}

// A node key constraint owns a property index too; relationship and
// multi-property constraints don't.
func TestConstraintPropertyIndexOwners(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddConstraint(Constraint{Name: "nk", Type: ConstraintNodeKey, Label: "K", Properties: []string{"k"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "nk2", Type: ConstraintNodeKey, Label: "K2", Properties: []string{"a", "b"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "ex", Type: ConstraintExists, Label: "E", Properties: []string{"e"}}))

	_, owned := sm.ConstraintPropertyIndex("nk")
	require.True(t, owned)
	_, owned = sm.ConstraintPropertyIndex("nk2")
	require.False(t, owned)
	_, owned = sm.ConstraintPropertyIndex("ex")
	require.False(t, owned)

	require.False(t, sm.HasPropertyIndex("K", "k"))
	sm.MarkPropertyIndexesFilled()
	require.True(t, sm.HasPropertyIndex("K", "k"))
}

// The owned property index is the constraint's RANGE index: it isn't listed,
// counted, reported or persisted as an index of its own.
func TestConstraintPropertyIndexListedOnlyAsConstraintIndex(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddUniqueConstraint("c_uid", "Rec", "uid"))
	sm.MarkPropertyIndexesFilled()

	named := 0
	for _, raw := range sm.GetIndexes() {
		index := raw.(map[string]interface{})
		require.NotEqual(t, "PROPERTY", index["type"])
		if index["name"] == "c_uid" {
			named++
		}
	}
	require.Equal(t, 1, named)
	indexes, constraints := sm.SchemaObjectCounts()
	require.Zero(t, indexes)
	require.Equal(t, 1, constraints)
	for _, stat := range sm.GetIndexStats() {
		require.NotEqual(t, "PROPERTY", stat.Type)
	}
	require.False(t, sm.isPropertyInStructuralIndex([]string{"Rec"}, "uid"))
	require.Empty(t, sm.ExportDefinition().PropertyIndexes)

	require.NoError(t, sm.AddPropertyIndex("i_name", "Rec", []string{"name"}))
	require.True(t, sm.isPropertyInStructuralIndex([]string{"Rec"}, "name"))
	require.Len(t, sm.ExportDefinition().PropertyIndexes, 1)
}

// Loading a definition recreates the owned index unfilled; a store written
// before the owned index existed may hold an index of its own on the same
// property, which keeps serving the seeks.
func TestConstraintPropertyIndexFromDefinition(t *testing.T) {
	source := NewSchemaManager()
	require.NoError(t, source.AddUniqueConstraint("c_uid", "Rec", "uid"))

	loaded := NewSchemaManager()
	require.NoError(t, loaded.ReplaceFromDefinition(source.ExportDefinition()))
	idx, owned := loaded.ConstraintPropertyIndex("c_uid")
	require.True(t, owned)
	require.True(t, idx.unfilled.Load())
	require.True(t, loaded.MaintainsPropertyIndex("Rec", "uid"))
	require.False(t, loaded.HasPropertyIndex("Rec", "uid"))

	legacy := source.ExportDefinition()
	legacy.PropertyIndexes = []SchemaPropertyIndexDef{{Name: "i_uid", Label: "Rec", Properties: []string{"uid"}}}
	both := NewSchemaManager()
	require.NoError(t, both.ReplaceFromDefinition(legacy))
	_, owned = both.ConstraintPropertyIndex("c_uid")
	require.False(t, owned)
	standalone, ok := both.GetPropertyIndex("Rec", "uid")
	require.True(t, ok)
	require.Equal(t, "i_uid", standalone.Name)
	require.Empty(t, standalone.OwningConstraint)
}

// Neo4j's rules for an index and a constraint on one property (#884).
func TestConstraintPropertyIndexOverlapRules(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("i_id", "U", []string{"id"}))
	for _, ifNotExists := range []bool{false, true} {
		err := sm.AddConstraint(Constraint{Name: "c_id", Type: ConstraintUnique, Label: "U", Properties: []string{"id"}}, ifNotExists)
		var classified *schemaAdmissionError
		require.True(t, errors.As(err, &classified), "ifNotExists=%v: %v", ifNotExists, err)
		require.Equal(t, "Neo.ClientError.Schema.IndexAlreadyExists", classified.BoltErrorCode())
		require.Equal(t, "There already exists an index (:U {id}). A constraint cannot be created until the index has been dropped.", err.Error())
	}

	require.NoError(t, sm.AddUniqueConstraint("c_t", "T", "id"))
	err := sm.DropIndex("c_t")
	var classified *schemaAdmissionError
	require.True(t, errors.As(err, &classified), "%v", err)
	require.Equal(t, "Neo.DatabaseError.Schema.IndexDropFailed", classified.BoltErrorCode())
	require.Equal(t, "Unable to drop index: Index belongs to constraint: `c_t`", err.Error())
	_, owned := sm.ConstraintPropertyIndex("c_t")
	require.True(t, owned)

	// DROP INDEX by name finds an index of its own, never the owned one.
	require.NoError(t, sm.DropIndex("i_id"))
	require.False(t, sm.MaintainsPropertyIndex("U", "id"))
}

// Dropping the constraint drops its property index, and a failed drop keeps
// both.
func TestDropConstraintDropsItsPropertyIndex(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddUniqueConstraint("c_uid", "Rec", "uid"))

	failure := errors.New("persist failed")
	sm.SetPersister(func(*SchemaDefinition) error { return failure })
	require.ErrorIs(t, sm.DropConstraint("c_uid"), failure)
	_, owned := sm.ConstraintPropertyIndex("c_uid")
	require.True(t, owned)

	sm.SetPersister(nil)
	require.NoError(t, sm.DropConstraint("c_uid"))
	_, owned = sm.ConstraintPropertyIndex("c_uid")
	require.False(t, owned)
	require.False(t, sm.MaintainsPropertyIndex("Rec", "uid"))
}

// Node writes maintain an unfilled owned index, and a restart fills it from
// the stored nodes.
func TestBadgerConstraintPropertyIndexMaintainedAndRebuilt(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("c_uid", "Rec", "uid"))

	_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Rec"}, Properties: map[string]interface{}{"uid": "a"}})
	require.NoError(t, err)
	require.Nil(t, schema.PropertyIndexLookup("Rec", "uid", "a"))
	require.NoError(t, schema.BackfillPropertyIndex("Rec", "uid", nil))
	require.Len(t, schema.PropertyIndexLookup("Rec", "uid", "a"), 1)

	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Rec"}, Properties: map[string]interface{}{"uid": "b"}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	schema = reopened.GetSchemaForNamespace("test")
	require.True(t, schema.HasPropertyIndex("Rec", "uid"))
	require.Len(t, schema.PropertyIndexLookup("Rec", "uid", "a"), 1)
	require.Len(t, schema.PropertyIndexLookup("Rec", "uid", "b"), 1)
}
