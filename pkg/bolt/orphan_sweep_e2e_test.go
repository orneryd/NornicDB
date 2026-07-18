package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltOrphanSweepRelationshipExistenceAndDeleteGuard drives the three
// proven relationship-existence bugs and the non-DETACH DELETE guard
// through a real in-process Bolt connection (neo4j-go-driver/v5), matching
// eshu #5147: WHERE NOT (n)--() must match only orphans, COUNT { (n)--() }
// = 0 must agree, and a non-DETACH DELETE of a connected node must surface
// a driver-visible error instead of silently cascading its edges.
func TestBoltOrphanSweepRelationshipExistenceAndDeleteGuard(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": storage.NewNamespacedEngine(base, "nornic"),
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		require.NoError(t, session.Close(ctx))
	}()

	_, err = session.Run(ctx, "CREATE (:Item {name: 'orphan'})", nil)
	require.NoError(t, err)
	_, err = session.Run(ctx, `
		CREATE (c:Item {name: 'connected'})
		CREATE (p:Item {name: 'peer'})
		CREATE (c)-[:CONTAINS]->(p)
	`, nil)
	require.NoError(t, err)

	t.Run("WHERE NOT (n)--() matches only the orphan", func(t *testing.T) {
		result, runErr := session.Run(ctx, `MATCH (n:Item) WHERE NOT (n)--() RETURN n.name`, nil)
		require.NoError(t, runErr)
		records, collectErr := result.Collect(ctx)
		require.NoError(t, collectErr)
		require.Len(t, records, 1, "only the orphan should have no relationships")
		require.Equal(t, "orphan", records[0].Values[0])
	})

	t.Run("COUNT subquery bare body DETACH DELETE removes only the orphan", func(t *testing.T) {
		result, runErr := session.Run(ctx, `MATCH (n:Item) WHERE COUNT { (n)--() } = 0 DETACH DELETE n`, nil)
		require.NoError(t, runErr)
		summary, consumeErr := result.Consume(ctx)
		require.NoError(t, consumeErr)
		require.Equal(t, 1, summary.Counters().NodesDeleted(), "exactly one orphan should be deleted")

		countResult, runErr := session.Run(ctx, "MATCH (n:Item) RETURN count(n)", nil)
		require.NoError(t, runErr)
		record, singleErr := countResult.Single(ctx)
		require.NoError(t, singleErr)
		require.Equal(t, int64(2), record.Values[0], "connected and peer must survive")

		edgeResult, runErr := session.Run(ctx, `MATCH (:Item {name: 'connected'})-[r:CONTAINS]->(:Item {name: 'peer'}) RETURN count(r)`, nil)
		require.NoError(t, runErr)
		edgeRecord, singleErr := edgeResult.Single(ctx)
		require.NoError(t, singleErr)
		require.Equal(t, int64(1), edgeRecord.Values[0], "the CONTAINS relationship must survive")
	})

	t.Run("non-DETACH DELETE of a connected node surfaces a driver-visible error", func(t *testing.T) {
		_, runErr := session.Run(ctx, `MATCH (n:Item {name: 'connected'}) DELETE n`, nil)
		if runErr == nil {
			// Some drivers defer errors until the result is consumed.
			result, _ := session.Run(ctx, `MATCH (n:Item {name: 'connected'}) DELETE n`, nil)
			_, runErr = result.Consume(ctx)
		}
		require.Error(t, runErr, "deleting a node that still has relationships without DETACH must fail")
		require.Contains(t, runErr.Error(), "still has relationships")

		countResult, err := session.Run(ctx, "MATCH (n:Item {name: 'connected'}) RETURN count(n)", nil)
		require.NoError(t, err)
		record, err := countResult.Single(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), record.Values[0], "connected node must survive the rejected delete")
	})
}
