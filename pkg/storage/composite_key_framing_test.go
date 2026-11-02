package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A string value may contain the old separator and the next value's type tag.
// Those bytes belong to the value, not to the tuple framing.
func TestCompositeIndexSeparatesDelimiterShapedValues(t *testing.T) {
	left := []interface{}{"a|string:b", "c"}
	right := []interface{}{"a", "b|string:c"}
	t.Run("key", func(t *testing.T) {
		leftKey, err := NewCompositeKey(t.Context(), left...)
		require.NoError(t, err)
		rightKey, err := NewCompositeKey(t.Context(), right...)
		require.NoError(t, err)
		require.NotEqual(t, leftKey.Hash, rightKey.Hash,
			"distinct property tuples must not alias before HMAC")
	})
	schema := NewSchemaManager()
	require.NoError(t, schema.AddCompositeIndex("tuple", "Pair", []string{"left", "right"}))
	idx, ok := schema.GetCompositeIndex("tuple")
	require.True(t, ok)
	require.NoError(t, idx.IndexNode(t.Context(), "one", map[string]interface{}{"left": left[0], "right": left[1]}))
	require.NoError(t, idx.IndexNode(t.Context(), "two", map[string]interface{}{"left": right[0], "right": right[1]}))
	for _, test := range []struct {
		values []interface{}
		id     NodeID
	}{{left, "one"}, {right, "two"}} {
		t.Run("lookup_"+string(test.id), func(t *testing.T) {
			ids, err := idx.LookupFull(t.Context(), test.values...)
			require.NoError(t, err)
			require.Equal(t, []NodeID{test.id}, ids)
		})
	}
}

func TestBulkNodeKeyAcceptsDistinctDelimiterValues(t *testing.T) {
	engine := createTestBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("tuple")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "pair_key", Type: ConstraintNodeKey, Label: "Pair", Properties: []string{"left", "right"}}))
	nodes := []*Node{
		{ID: "tuple:one", Labels: []string{"Pair"}, Properties: map[string]interface{}{"left": "a|string:b", "right": "c"}},
		{ID: "tuple:two", Labels: []string{"Pair"}, Properties: map[string]interface{}{"left": "a", "right": "b|string:c"}},
	}
	require.NoError(t, engine.BulkCreateNodes(t.Context(), nodes), "distinct tuples are valid even if their text contains a separator/type tag")
	for _, expected := range nodes {
		actual, err := engine.GetNode(t.Context(), expected.ID)
		require.NoError(t, err)
		require.Equal(t, expected.Properties, actual.Properties)
	}
	err := engine.BulkCreateNodes(t.Context(), []*Node{
		{ID: "tuple:three", Labels: []string{"Pair"}, Properties: map[string]interface{}{"left": "same", "right": "value"}},
		{ID: "tuple:four", Labels: []string{"Pair"}, Properties: map[string]interface{}{"left": "same", "right": "value"}},
	})
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintNodeKey, violation.Type)
	for _, id := range []NodeID{"tuple:three", "tuple:four"} {
		_, err := engine.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound, "true duplicate batch must not partially publish")
	}
}
