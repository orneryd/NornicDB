package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG: non-DETACH DELETE of a connected node silently cascade-deletes its edges
// ====================================================================================
// Discovered: eshu #5147
// Impact: `MATCH (n) DELETE n` on a node that still has relationships
//         silently succeeded and cascade-deleted its edges instead of
//         erroring, diverging from openCypher/Neo4j semantics.
// Root Cause: executeDelete's generic path called store.DeleteNode with no
//         adjacency check; the storage engines (e.g. BadgerEngine.DeleteNode)
//         cascade-delete a node's edges unconditionally.
// Behavior change: non-DETACH DELETE of a connected node now fails
//         unconditionally (matching Neo4j). Callers relying on the old
//         silent cascade must switch to DETACH DELETE.
// ====================================================================================

func setupDeleteGuardFixture(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (o:Item {name: 'orphan'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		CREATE (c:Item {name: 'connected'})
		CREATE (p:Item {name: 'peer'})
		CREATE (c)-[:CONTAINS]->(p)
	`, nil)
	require.NoError(t, err)

	return exec, store
}

func countItemsByName(t *testing.T, exec *StorageExecutor, name string) int64 {
	t.Helper()
	result, err := exec.Execute(context.Background(), `MATCH (n:Item {name: $name}) RETURN count(n)`, map[string]interface{}{"name": name})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].(int64)
}

func TestDeleteConnectedNodeWithoutDetach_Errors(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `MATCH (n:Item {name: 'connected'}) DELETE n`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still has relationships")

	assert.Equal(t, int64(1), countItemsByName(t, exec, "connected"), "node must survive a rejected delete")
	assert.Equal(t, int64(1), countItemsByName(t, exec, "peer"), "peer node must survive")

	result, err := exec.Execute(ctx, `MATCH (:Item {name: 'connected'})-[r:CONTAINS]->(:Item {name: 'peer'}) RETURN count(r)`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0][0].(int64), "edge must survive a rejected delete")
}

func TestDeleteOrphanNodeWithoutDetach_Succeeds(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `MATCH (n:Item {name: 'orphan'}) DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, int64(0), countItemsByName(t, exec, "orphan"))
}

func TestDetachDeleteConnectedNode_Succeeds(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `MATCH (n:Item {name: 'connected'}) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.GreaterOrEqual(t, result.Stats.RelationshipsDeleted, 1)
	assert.Equal(t, int64(0), countItemsByName(t, exec, "connected"))
}

func TestDeleteNodeAndItsOwnEdgeTogether_Succeeds(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `MATCH (a:Item {name: 'connected'})-[r:CONTAINS]->(b:Item {name: 'peer'}) DELETE a, r`, nil)
	require.NoError(t, err, "deleting a node together with its own edge in the same statement must succeed")
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, int64(0), countItemsByName(t, exec, "connected"))
	assert.Equal(t, int64(1), countItemsByName(t, exec, "peer"), "peer node itself must survive")
}

func TestDeleteMultipleNodesWithSurvivingEdge_Errors(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	// Deletes both endpoints but does not name the edge between them, so
	// each endpoint still has a "residual" relationship from the
	// statement's point of view -- this must error rather than silently
	// cascade the edge away.
	_, err := exec.Execute(ctx, `MATCH (a:Item {name: 'connected'}), (b:Item {name: 'peer'}) DELETE a, b`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still has relationships")

	assert.Equal(t, int64(1), countItemsByName(t, exec, "connected"))
	assert.Equal(t, int64(1), countItemsByName(t, exec, "peer"))
}

func TestDeleteConnectedNodeParameterizedWhere_Errors(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `MATCH (n:Item) WHERE n.name = $name DELETE n`, map[string]interface{}{"name": "connected"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still has relationships")
	assert.Equal(t, int64(1), countItemsByName(t, exec, "connected"))
}

func TestDeleteConnectedNodeExplicitTransactionRollback_Errors(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `MATCH (n:Item {name: 'connected'}) DELETE n`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still has relationships")

	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	assert.Equal(t, int64(1), countItemsByName(t, exec, "connected"), "node must survive after rollback of a rejected delete")
}

func TestDeleteMultiRowOneConnected_ErrorsAndDeletesNothing(t *testing.T) {
	exec, _ := setupDeleteGuardFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Item {name: 'orphan2'})`, nil)
	require.NoError(t, err)

	// Matches orphan, orphan2, and connected in one statement -- the
	// connected node must abort the whole DELETE. Validation must run for
	// the entire plan before any mutation, so the orphans must not be
	// deleted either (statement atomicity).
	_, err = exec.Execute(ctx, `MATCH (n:Item) WHERE n.name IN ['orphan', 'orphan2', 'connected'] DELETE n`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still has relationships")

	assert.Equal(t, int64(1), countItemsByName(t, exec, "orphan"), "orphan must survive: statement is all-or-nothing")
	assert.Equal(t, int64(1), countItemsByName(t, exec, "orphan2"), "orphan2 must survive: statement is all-or-nothing")
	assert.Equal(t, int64(1), countItemsByName(t, exec, "connected"))
}
