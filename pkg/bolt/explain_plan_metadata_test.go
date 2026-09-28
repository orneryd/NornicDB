package bolt

// explain_plan_metadata_test.go — end-to-end plan delivery over Bolt (#744 §2):
// the driver's ResultSummary must expose the plan for EXPLAIN and the profiled
// plan for PROFILE, read straight from the PULL SUCCESS metadata.

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltExplainProfilePlanMetadata(t *testing.T) {
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
	t.Cleanup(func() {
		require.NoError(t, session.Close(ctx))
	})

	seed, err := session.Run(ctx, "CREATE (:Person {name: 'a'}), (:Person {name: 'b'})", nil)
	require.NoError(t, err)
	_, err = seed.Consume(ctx)
	require.NoError(t, err)

	t.Run("EXPLAIN plan", func(t *testing.T) {
		result, err := session.Run(ctx, "EXPLAIN MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		summary, err := result.Consume(ctx)
		require.NoError(t, err)
		plan := summary.Plan()
		require.NotNil(t, plan, "driver must read the plan from PULL SUCCESS metadata")
		require.Equal(t, "ProduceResults", plan.Operator())
		require.NotNil(t, plan.Arguments())
	})

	t.Run("PROFILE profile", func(t *testing.T) {
		result, err := session.Run(ctx, "PROFILE MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		summary, err := result.Consume(ctx)
		require.NoError(t, err)
		profiled := summary.Profile()
		require.NotNil(t, profiled, "driver must read the profile from PULL SUCCESS metadata")
		require.Equal(t, "ProduceResults", profiled.Operator())
		require.NotNil(t, summary.Plan(), "PROFILE also carries the plan key like Neo4j")
	})

	t.Run("ordinary query has no plan", func(t *testing.T) {
		result, err := session.Run(ctx, "MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		summary, err := result.Consume(ctx)
		require.NoError(t, err)
		require.Nil(t, summary.Plan(), "no spurious plan on ordinary queries")
		require.Nil(t, summary.Profile())
	})
}
