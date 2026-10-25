package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodedNameSnapshotProjectionIdentity(t *testing.T) {
	engine := txReadFixture(t)
	firstProperty := "x\x001:y"
	secondLabel := "A\x005:x"
	for _, node := range []*Node{
		{ID: "test:identity-a", Labels: []string{"A"}, Properties: map[string]interface{}{firstProperty: int64(1)}},
		{ID: "test:identity-b", Labels: []string{secondLabel}, Properties: map[string]interface{}{"y": int64(2)}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		stored, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		require.Equal(t, node.Labels, stored.Labels)
	}
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, reader.SetNamespace("test"))
	t.Cleanup(func() { _ = reader.Rollback() })
	for _, tc := range []struct {
		label, property string
		id              NodeID
		value           int64
	}{
		{"A", firstProperty, "test:identity-a", 1},
		{secondLabel, "y", "test:identity-b", 2},
		{"A", firstProperty, "test:identity-a", 1},
	} {
		var ids []NodeID
		var values []interface{}
		err := reader.StreamNodesByLabelProjected(tc.label, []string{tc.property}, func(node *Node) error {
			ids = append(ids, node.ID)
			values = append(values, node.Properties[tc.property])
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []NodeID{tc.id}, ids)
		require.Equal(t, []interface{}{tc.value}, values)
	}
}
