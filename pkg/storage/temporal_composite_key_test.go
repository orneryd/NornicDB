package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BUG: node TEMPORAL NO OVERLAP constraints only supported exactly three
// properties (key, valid_from, valid_to). Like relationship temporal
// constraints, every property before the trailing (valid_from, valid_to) pair
// now forms a composite grouping key.

func compositeTemporalConstraint() Constraint {
	return Constraint{
		Name:       "e_temporal",
		Type:       ConstraintTemporal,
		Label:      "E",
		Properties: []string{"k1", "k2", "vf", "vt"},
	}
}

func compositeTemporalNode(id string, k1, k2 interface{}, start time.Time, end interface{}) *Node {
	props := map[string]interface{}{"vf": start}
	if k1 != nil {
		props["k1"] = k1
	}
	if k2 != nil {
		props["k2"] = k2
	}
	if end != nil {
		props["vt"] = end
	}
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"E"}, Properties: props}
}

var compositeTemporalBase = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func compositeDay(d int) time.Time { return compositeTemporalBase.AddDate(0, 0, d) }

func TestBug_NodeTemporalCompositeKey_DirectWrites(t *testing.T) {
	engine := createTestBadgerEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(compositeTemporalConstraint()))

	_, err := engine.CreateNode(compositeTemporalNode("v1", "a", "x", compositeDay(0), compositeDay(31)))
	require.NoError(t, err)

	_, err = engine.CreateNode(compositeTemporalNode("v-overlap", "a", "x", compositeDay(14), compositeDay(60)))
	require.Error(t, err, "same composite key with overlapping window")
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintTemporal, violation.Type)

	_, err = engine.CreateNode(compositeTemporalNode("v-other-k2", "a", "y", compositeDay(14), compositeDay(60)))
	require.NoError(t, err, "same k1, different k2")
	_, err = engine.CreateNode(compositeTemporalNode("v-other-k1", "b", "x", compositeDay(14), compositeDay(60)))
	require.NoError(t, err, "different k1, same k2")

	_, err = engine.CreateNode(compositeTemporalNode("v2", "a", "x", compositeDay(31), compositeDay(60)))
	require.NoError(t, err, "adjacent window (vt == next vf)")

	_, err = engine.CreateNode(compositeTemporalNode("v3-open", "a", "x", compositeDay(90), nil))
	require.NoError(t, err, "open-ended window after existing versions")
	_, err = engine.CreateNode(compositeTemporalNode("v-inside-open", "a", "x", compositeDay(120), compositeDay(150)))
	require.Error(t, err, "window inside an open-ended version")
	_, err = engine.CreateNode(compositeTemporalNode("v-before-open", "a", "x", compositeDay(70), compositeDay(100)))
	require.Error(t, err, "window that runs into an open-ended version")

	_, err = engine.CreateNode(compositeTemporalNode("v-null-k2", "a", nil, compositeDay(400), compositeDay(410)))
	require.ErrorContains(t, err, "TEMPORAL key property k2 cannot be null")
	_, err = engine.CreateNode(compositeTemporalNode("v-null-k1", nil, "x", compositeDay(400), compositeDay(410)))
	require.ErrorContains(t, err, "TEMPORAL key property k1 cannot be null")

	// Updating an existing version into an overlap is rejected; updating it
	// within its own slot is allowed (the node excludes itself).
	v2, err := engine.GetNode(NodeID(prefixTestID("v2")))
	require.NoError(t, err)
	v2.Properties["vt"] = compositeDay(61)
	require.NoError(t, engine.UpdateNode(v2))
	v2.Properties["vt"] = compositeDay(95)
	require.Error(t, engine.UpdateNode(v2))

	// The current-version pointer works per composite key.
	open, err := engine.GetNode(NodeID(prefixTestID("v3-open")))
	require.NoError(t, err)
	current, err := engine.IsCurrentTemporalNodeInNamespace("test", open, compositeDay(200))
	require.NoError(t, err)
	require.True(t, current)
	v1, err := engine.GetNode(NodeID(prefixTestID("v1")))
	require.NoError(t, err)
	current, err = engine.IsCurrentTemporalNodeInNamespace("test", v1, compositeDay(200))
	require.NoError(t, err)
	require.False(t, current)
}

func TestBug_NodeTemporalCompositeKey_Transaction(t *testing.T) {
	engine := createTestBadgerEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(compositeTemporalConstraint()))

	t.Run("two overlapping pending nodes", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.CreateNode(compositeTemporalNode("tx-a1", "t", "x", compositeDay(0), compositeDay(31)))
		require.NoError(t, err)
		_, err = tx.CreateNode(compositeTemporalNode("tx-a2", "t", "y", compositeDay(10), compositeDay(40)))
		require.NoError(t, err, "different k2 in the same transaction")
		_, err = tx.CreateNode(compositeTemporalNode("tx-a3", "t", "x", compositeDay(10), compositeDay(40)))
		require.Error(t, err, "overlapping pending node with the same composite key")
	})

	t.Run("pending against committed", func(t *testing.T) {
		_, err := engine.CreateNode(compositeTemporalNode("c1", "c", "x", compositeDay(0), nil))
		require.NoError(t, err)

		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.CreateNode(compositeTemporalNode("c2", "c", "x", compositeDay(10), compositeDay(20)))
		require.Error(t, err, "overlaps committed open-ended version")
		_, err = tx.CreateNode(compositeTemporalNode("c3", "c", "y", compositeDay(10), compositeDay(20)))
		require.NoError(t, err)
		_, err = tx.CreateNode(compositeTemporalNode("c4", "c", nil, compositeDay(10), compositeDay(20)))
		require.ErrorContains(t, err, "TEMPORAL key property k2 cannot be null")
	})

	t.Run("checkTemporalConstraint adjacent windows", func(t *testing.T) {
		_, err := engine.CreateNode(compositeTemporalNode("adj1", "adj", "x", compositeDay(0), compositeDay(10)))
		require.NoError(t, err)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		c := compositeTemporalConstraint()
		require.NoError(t, tx.checkTemporalConstraint(compositeTemporalNode("adj2", "adj", "x", compositeDay(10), compositeDay(20)), c))
		require.Error(t, tx.checkTemporalConstraint(compositeTemporalNode("adj3", "adj", "x", compositeDay(9), compositeDay(20)), c))
	})
}

func TestBug_NodeTemporalCompositeKey_CreationValidation(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(compositeTemporalNode("p1", "a", "x", compositeDay(0), compositeDay(31)))
	require.NoError(t, err)
	_, err = engine.CreateNode(compositeTemporalNode("p2", "a", "y", compositeDay(10), compositeDay(40)))
	require.NoError(t, err)

	c := compositeTemporalConstraint()
	require.NoError(t, ValidateConstraintOnCreationForEngine(engine, c), "different k2 values do not overlap")

	_, err = engine.CreateNode(compositeTemporalNode("p3", "a", "x", compositeDay(20), nil))
	require.NoError(t, err)
	err = ValidateConstraintOnCreationForEngine(engine, c)
	require.Error(t, err, "pre-existing overlap within one composite key")

	_, err = engine.CreateNode(compositeTemporalNode("p-null", "z", nil, compositeDay(500), nil))
	require.NoError(t, err)
	err = ValidateConstraintOnCreationForEngine(engine, Constraint{Type: ConstraintTemporal, Label: "E", Properties: []string{"k1", "k2", "vf", "vt"}})
	require.Error(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{Type: ConstraintTemporal, Label: "E", Properties: []string{"vf", "vt"}})
	require.ErrorContains(t, err, "at least 3 properties")
}

func TestNodeTemporalKey_SingleKeyDescriptorUnchanged(t *testing.T) {
	single := Constraint{Type: ConstraintTemporal, Label: "FactVersion", Properties: []string{"fact_key", "valid_from", "valid_to"}}
	desc := makeTemporalDescriptor("test", single, "k1")
	require.Equal(t, temporalIndexDescriptor{
		namespace: "test",
		label:     "FactVersion",
		keyProp:   "fact_key",
		startProp: "valid_from",
		endProp:   "valid_to",
		keyHash:   NewCompositeKey("k1").Hash,
	}, desc, "single-key descriptors must stay byte-identical to existing persisted indexes")

	composite := compositeTemporalConstraint()
	node := compositeTemporalNode("n", "a", "x", compositeDay(0), nil)
	keyValue, _, _, _, ok := temporalNodeState(node, composite)
	require.True(t, ok)
	cdesc := makeTemporalDescriptor("test", composite, keyValue)
	require.Equal(t, "vf", cdesc.startProp)
	require.Equal(t, "vt", cdesc.endProp)
	require.NotEqual(t, "k1", cdesc.keyProp)

	other := compositeTemporalNode("m", "a", "y", compositeDay(0), nil)
	otherValue, _, _, _, ok := temporalNodeState(other, composite)
	require.True(t, ok)
	require.NotEqual(t, cdesc.keyHash, makeTemporalDescriptor("test", composite, otherValue).keyHash)
	require.Equal(t, cdesc, makeTemporalDescriptor("test", composite, []interface{}{"a", "x"}))
}
