package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise the actual database-manager view, including the clustered server's
// WAL -> Replicated -> Namespaced -> size-tracking stack and top-level tracing.
// Rows alone cannot distinguish a lost optional capability from a real seek.
func TestRelationshipPropertySeekWrappedEngineStack(t *testing.T) {
	for _, name := range []string{"memory", "wal", "traced_physical", "traced_database", "wal_database", "replicated_database", "production_stack"} {
		t.Run(name, func(t *testing.T) {
			var base storage.Engine
			if name == "memory" {
				base = storage.NewMemoryEngine()
			} else {
				engine, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				base = engine
			}
			physical := base
			t.Cleanup(func() { require.NoError(t, physical.Close()) })
			if name != "memory" {
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, wal.Close()) })
				base = storage.NewWALEngine(base, wal)
			}
			if name == "traced_physical" {
				base = storage.NewTracedEngine(base)
			}
			if name == "production_stack" {
				adapter, err := replication.NewStorageAdapterWithWAL(base, t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, adapter.Close()) })
				config := replication.DefaultConfig()
				config.Mode = replication.ModeStandalone
				config.NodeID = "relationship-seek"
				replicator, err := replication.NewReplicator(config, adapter)
				require.NoError(t, err)
				require.NoError(t, replicator.Start(context.Background()))
				t.Cleanup(func() { require.NoError(t, replicator.Shutdown()) })
				base = replication.NewReplicatedEngine(base, replicator, 0)
			}
			manager, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			for _, database := range []string{"wrapped_a", "wrapped_b"} {
				require.NoError(t, manager.CreateDatabase(database))
				view, err := manager.GetStorage(database)
				require.NoError(t, err)
				executor := cypher.NewStorageExecutor(view)
				for _, query := range []string{
					"CREATE ()-[:KEYED {_ci_key:'hit', tenant:$tenant}]->(), ()-[:KEYED {_ci_key:'other', tenant:$tenant}]->()",
					"CREATE RANGE INDEX wrapped_key IF NOT EXISTS FOR ()-[r:KEYED]-() ON (r._ci_key)",
				} {
					_, err := executor.Execute(context.Background(), query, map[string]interface{}{"tenant": database})
					require.NoError(t, err)
				}
			}
			for _, database := range []string{"wrapped_a", "wrapped_b"} {
				view, err := manager.GetStorage(database)
				require.NoError(t, err)
				switch name {
				case "traced_database", "production_stack":
					view = storage.NewTracedEngine(view)
				case "wal_database":
					wal, err := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, wal.Close()) })
					view = storage.NewWALEngine(view, wal)
				case "replicated_database":
					// The local read facade delegates to the same bound view;
					// fixtures above used the database's actual write stack.
					view = replication.NewReplicatedEngine(view, nil, 0)
				}
				executor := cypher.NewStorageExecutor(view)
				for _, pattern := range []string{"MATCH ()-[r:KEYED {_ci_key:k}]->()", "MATCH ()-[r:KEYED]->() WHERE r._ci_key=k"} {
					result, err := executor.Execute(context.Background(), "PROFILE UNWIND $keys AS k "+pattern+" RETURN r.tenant", map[string]interface{}{"keys": []interface{}{"hit", "missing", "hit"}})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{database}, {database}}, result.Rows)
					plan := result.Metadata["plan"].(*cypher.ExecutionPlan)
					var seeks []*cypher.PlanOperator
					var visit func(*cypher.PlanOperator)
					visit = func(op *cypher.PlanOperator) {
						if op.OperatorType == "RelationshipIndexSeek" { seeks = append(seeks, op) }
						for _, child := range op.Children { visit(child) }
					}
					visit(plan.Root)
					require.Len(t, seeks, 1, "%s/%s must execute a seek", database, pattern)
					seek := seeks[0]
					require.Equal(t, int64(3), seek.Arguments["indexLookups"])
					require.Equal(t, int64(2), seek.Arguments["relationshipCandidates"])
					require.Equal(t, int64(2), seek.ActualRows)
					require.Equal(t, int64(5), seek.DBHits)
					require.Empty(t, seek.Children)
				}
			}
		})
	}
}
