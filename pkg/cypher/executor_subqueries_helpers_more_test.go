package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRowMatchesSeedID_MoreBranches(t *testing.T) {
	require.True(t, rowMatchesSeedID(&storage.Node{ID: "n1"}, "n1"))
	require.True(t, rowMatchesSeedID(storage.Node{ID: "n2"}, "n2"))
	require.True(t, rowMatchesSeedID(map[string]interface{}{"id": "tenant:n3"}, "n3"))
	require.True(t, rowMatchesSeedID(map[string]interface{}{"_id": "n4"}, "n4"))
	require.True(t, rowMatchesSeedID(map[string]interface{}{"elementId": "ns:n5"}, "n5"))
	require.False(t, rowMatchesSeedID(map[string]interface{}{"id": 42}, "42"))
	require.True(t, rowMatchesSeedID("ns:n6", "n6"))
	require.False(t, rowMatchesSeedID("n7", "n8"))
}
