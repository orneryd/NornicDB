package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A zero-length variable segment binds the previous segment's intermediate
// node to the chain's final variable, so that node's properties must survive:
// the anonymous intermediate endpoint must load fully, not exist-only (#1110
// conformance regression, Match5.feature scenario 20).
func TestChainedZeroLengthVariableSegmentPreservesEndpointProperties(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
		CREATE (a1:A {name: 'a1'}), (a2:A {name: 'a2'}),
		       (n00:A {name: 'n00'}), (n01:A {name: 'n01'}),
		       (n000:A {name: 'n000'}), (n001:A {name: 'n001'})
		CREATE (a1)-[:LIKES]->(n00), (a1)-[:LIKES]->(n01),
		       (a2)-[:LIKES]->(n000), (a2)-[:LIKES]->(n001)
	`, nil)
	require.NoError(t, err)

	// (a)-[:LIKES]->()-[:LIKES*0]->(c): the anonymous intermediate node IS c.
	result, err := exec.Execute(ctx, "MATCH (a:A) MATCH (a)-[:LIKES]->()-[:LIKES*0]->(c) RETURN c.name AS name ORDER BY name", nil)
	require.NoError(t, err)
	var names []string
	for _, row := range result.Rows {
		names = append(names, row[0].(string))
	}
	require.ElementsMatch(t, []string{"n00", "n01", "n000", "n001"}, names)
}
