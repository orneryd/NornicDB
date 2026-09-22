package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOptionalMatchFiltersBoundRelationshipAndPreservesMiss(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_relationship_filter"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:A {num: 1})-[:REL {name: 'r1'}]->(b:B {num: 2})-[:REL {name: 'r2'}]->(c:C {num: 3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, b, c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	matched := 0
	for _, row := range result.Rows {
		if row[2] != nil {
			matched++
		}
	}
	require.Equal(t, 1, matched)
}

func TestMandatoryMatchDropsCoalescedNullNodeBinding(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "mandatory_coalesced_null"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Single)-[:REL]->(:A)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:Single) OPTIONAL MATCH (a)-->(b:Missing) OPTIONAL MATCH (a)-->(c:Missing) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestOptionalMatchReturnsUndirectedSelfRelationshipOnce(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_self_relationship"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:B)", nil)
	require.NoError(t, err)
	nodes, err := exec.storage.GetNodesByLabel("B")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: "loop", Type: "LOOP", StartNode: nodes[0].ID, EndNode: nodes[0].ID}))

	result, err := exec.Execute(ctx, "MATCH (a:B) OPTIONAL MATCH (a)-[r]-(a) RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.NotNil(t, result.Rows[0][0])
}
