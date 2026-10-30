package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteCreateSet_WithPipelineBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_create_set")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Node) SET n.x = 1 WITH n", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

	_, err = exec.Execute(ctx, "CREATE (n:Node) SET n.x = 1 WITH    RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

	scalar, err := exec.Execute(ctx, "CREATE (n:Node) SET n.x = 1 WITH 1 AS one RETURN one", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, scalar.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, scalar.Rows)

	deleted, err := exec.Execute(ctx, "CREATE (n:Node) SET n.x = 1 DELETE n RETURN n", nil)
	require.NoError(t, err)
	require.Equal(t, 1, deleted.Stats.NodesCreated)
	require.Equal(t, 1, deleted.Stats.NodesDeleted)
	require.Len(t, deleted.Rows, 1)
	require.Len(t, deleted.Rows[0], 1)
	require.IsType(t, &storage.Node{}, deleted.Rows[0][0])

	res, err := exec.Execute(ctx, "CREATE (n:Seed {id:'s1'}) SET n.flag = true WITH n AS seed CREATE (m:Leaf {id:'l1'})-[:LINK]->(seed) RETURN seed.id AS sid, m.id AS mid", nil)
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	require.Equal(t, 2, res.Stats.NodesCreated)
	require.Equal(t, 1, res.Stats.RelationshipsCreated)
	require.Equal(t, []string{"sid", "mid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "s1", res.Rows[0][0])
	require.Equal(t, "l1", res.Rows[0][1])

	verify, err := exec.Execute(ctx, "MATCH (m:Leaf)-[r:LINK]->(s:Seed {id:'s1'}) RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, verify.Rows[0][0])

	// Ensure WITH can forward relationship aliases as well.
	res, err = exec.Execute(ctx, "CREATE (a:Start {id:'a1'})-[r:REL]->(b:End {id:'b1'}) SET r.w = 7 WITH r AS rel RETURN type(rel) AS rt, rel.w AS w", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"rt", "w"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "REL", res.Rows[0][0])
	require.EqualValues(t, 7, res.Rows[0][1])

	// Direct API verification for deterministic assertions.
	edges, err := store.GetEdgesByType("REL")
	require.NoError(t, err)
	require.NotEmpty(t, edges)
	foundWeight := false
	for _, e := range edges {
		if e != nil && e.Properties != nil {
			if w, ok := e.Properties["w"]; ok && w == int64(7) {
				foundWeight = true
				break
			}
		}
	}
	require.True(t, foundWeight)
}
