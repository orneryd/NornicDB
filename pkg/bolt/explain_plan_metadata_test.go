package bolt

// explain_plan_metadata_test.go — end-to-end plan delivery over Bolt (#744 §2):
// the driver's ResultSummary must expose the plan for EXPLAIN and the profiled
// plan for PROFILE, read straight from the PULL SUCCESS metadata.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newStatementFramingSession(t *testing.T) neo4jdriver.SessionWithContext {
	t.Helper()
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
	return session
}

func TestBoltExplainProfilePlanMetadata(t *testing.T) {
	session := newStatementFramingSession(t)
	ctx := context.Background()

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
		require.Nil(t, summary.Plan(), "PROFILE must publish only the profile key")
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

func TestMonsterBoltStatementBoundaries(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
					for _, test := range []struct{ query, code string }{
						{"CREATE (:Semi); MATCH (n:Semi) RETURN count(n) AS c", "Neo.ClientError.Statement.SyntaxError"},
						{"EXPLAIN PROFILE RETURN 1", "Neo.ClientError.Statement.ArgumentError"},
						{"PROFILE EXPLAIN RETURN 1", "Neo.ClientError.Statement.ArgumentError"},
						{"RETURN 1 AS x UNION FINISH", "Neo.ClientError.Statement.SyntaxError"},
						{"CREATE (:Semi) RETURN 1 AS x UNION FINISH", "Neo.ClientError.Statement.SyntaxError"},
					} {
						t.Run(test.query, func(t *testing.T) {
							ctx := context.Background()
							session := newStatementFramingSession(t)
							var runErr error
							if explicit {
								transaction, err := session.BeginTransaction(ctx)
								require.NoError(t, err)
								_, runErr = transaction.Run(ctx, test.query, nil)
								require.NoError(t, transaction.Rollback(ctx))
							} else {
								_, runErr = session.Run(ctx, test.query, nil)
							}
							var status *neo4jdriver.Neo4jError
							require.True(t, errors.As(runErr, &status), "statement must fail during RUN: %v", runErr)
							require.Equal(t, test.code, status.Code)
							result, err := session.Run(ctx, "MATCH (n:Semi) RETURN count(n) AS c", nil)
							require.NoError(t, err)
							record, err := result.Single(ctx)
							require.NoError(t, err)
							require.Equal(t, int64(0), record.Values[0])
						})
					}
					t.Run("scoped CALL and PROFILE", func(t *testing.T) {
						ctx := context.Background()
						session := newStatementFramingSession(t)
						run := func(ctx context.Context, query string, params map[string]any) (neo4jdriver.ResultWithContext, error) {
							return session.Run(ctx, query, params)
						}
						if explicit {
							transaction, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							t.Cleanup(func() { require.NoError(t, transaction.Rollback(ctx)) })
							run = transaction.Run
						}
						result, err := run(ctx, "UNWIND [1, 2] AS x CALL (x) { RETURN x * 2 AS y } RETURN y ORDER BY y", nil)
						require.NoError(t, err)
						records, err := result.Collect(ctx)
						require.NoError(t, err)
						require.Len(t, records, 2)
						require.Equal(t, []any{int64(2)}, records[0].Values)
						require.Equal(t, []any{int64(4)}, records[1].Values)
						result, err = run(ctx, "PROFILE MATCH (n) RETURN n", nil)
						require.NoError(t, err)
						summary, err := result.Consume(ctx)
						require.NoError(t, err)
						require.NotNil(t, summary.Profile())
						require.Nil(t, summary.Plan())
					})
				})
			}
		})
	}
}
