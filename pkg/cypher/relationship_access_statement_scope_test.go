package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const relationshipSuppliedSource = "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS p"

func relationshipSuppliedParams() map[string]interface{} {
	return map[string]interface{}{
		"x": "x", "statement": relationshipSuppliedSource,
		"many":    relationshipSuppliedSource + "; " + relationshipSuppliedSource,
		"action":  "MATCH ()-[r:T]->() WHERE r.p=$p RETURN r.p AS p",
		"limited": relationshipSuppliedSource + " LIMIT 1",
		"prefix":  "MATCH ()-[r:T]->() ", "suffix": "WHERE r.p=$x RETURN r.p AS p",
		"nested":      "CALL apoc.cypher.run($statement,{x:$x}) YIELD value RETURN value.p AS p",
		"callBody":    "CALL { " + relationshipSuppliedSource + " } RETURN p",
		"collectBody": "RETURN head(COLLECT { " + relationshipSuppliedSource + " }) AS p",
	}
}

func setupRelationshipSuppliedFixture(t *testing.T, e *StorageExecutor) {
	t.Helper()
	for _, query := range []string{
		"CREATE (:Key {p:'x'})-[:T {p:'x'}]->()",
		"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
		"CALL apoc.custom.installProcedure('site_install(x :: STRING) :: (p :: STRING)', $body)",
		"CALL apoc.custom.declareProcedure('site_declare(x :: STRING) :: (p :: STRING)', $body)",
		"CREATE PROCEDURE site.ddl($x) MODE READ AS " + relationshipSuppliedSource,
	} {
		_, err := e.Execute(context.Background(), query, map[string]interface{}{"body": relationshipSuppliedSource})
		require.NoError(t, err, query)
	}
}

func assertRelationshipSuppliedProfile(t *testing.T, e *StorageExecutor, ctx context.Context, query string, lookups int64, rows [][]interface{}) {
	t.Helper()
	params := relationshipSuppliedParams()
	observed := withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
	actual, err := e.Execute(observed, query, params)
	require.NoError(t, err, query)
	require.Equal(t, rows, actual.Rows)
	profile := observed.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	require.Empty(t, profile.unregistered)
	seek := profile.seeks["r\x00rel_p"]
	require.NotNil(t, seek)
	require.Equal(t, lookups, seek.lookups)
	require.Equal(t, lookups, seek.rows)
	actual, err = e.Execute(ctx, "PROFILE "+query, params)
	require.NoError(t, err, query)
	require.Equal(t, rows, actual.Rows)
	assertRelationshipSuppliedPlan(t, actual, lookups)
}

func assertRelationshipSuppliedPlan(t *testing.T, result *ExecuteResult, want int64) {
	t.Helper()
	var lookups, rows int64
	var visit func(*PlanOperator)
	visit = func(op *PlanOperator) {
		if op.OperatorType == "RelationshipIndexSeek" && op.Arguments["index"] == "rel_p" {
			lookups += op.Arguments["indexLookups"].(int64)
			rows += op.ActualRows
		}
		for _, child := range op.Children {
			visit(child)
		}
	}
	// Periodic iterate and action are distinct templates, each with its own
	// truthful operator. Check the whole public plan against actual execution.
	visit(result.Metadata["plan"].(*ExecutionPlan).Root)
	require.Equal(t, want, lookups)
	require.Equal(t, want, rows)
}

func TestRelationshipAccessProgramSuppliedStatementScopes(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		for _, scope := range []string{"local", "fabric"} {
			t.Run(fmt.Sprintf("%s/transaction_%t", scope, transaction), func(t *testing.T) {
				ClearUserProcedures()
				t.Cleanup(ClearUserProcedures)
				manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				require.NoError(t, manager.CreateDatabase("statement_part"))
				part, err := manager.GetStorage("statement_part")
				require.NoError(t, err)
				setupRelationshipSuppliedFixture(t, NewStorageExecutor(part))
				require.NoError(t, manager.CreateCompositeDatabase("statement_fabric", []multidb.ConstituentRef{{Alias: "part", DatabaseName: "statement_part", Type: "local", AccessMode: "read_write"}}))
				main, err := manager.GetStorage(manager.DefaultDatabaseName())
				require.NoError(t, err)
				if transaction {
					// BEGIN binds the session database. Select the intended local
					// or composite session before opening that transaction.
					main = part
					if scope == "fabric" {
						main, err = manager.GetStorage("statement_fabric")
						require.NoError(t, err)
					}
				}
				e := NewStorageExecutor(main)
				e.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				ctx := context.Background()
				if transaction {
					_, err = e.Execute(WithTransactionControl(ctx), "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, err := e.Execute(WithTransactionControl(ctx), "ROLLBACK", nil); require.NoError(t, err) })
				}
				for _, tc := range relationshipSuppliedStatementCases() {
					t.Run(tc.name, func(t *testing.T) {
						query := "USE statement_part " + tc.query
						if scope == "fabric" {
							query = "USE statement_fabric CALL { USE statement_fabric.part " + tc.query + " } RETURN p"
						}
						assertRelationshipSuppliedProfile(t, e, ctx, query, tc.lookups, tc.rows)
					})
				}
			})
		}
	}
}

// Keep the real seek capability while counting independent physical reads.
// A positive execution after each EXPLAIN validates the observer on that route.
type relationshipStatementReadCounter struct {
	*storage.NamespacedEngine
	reads atomic.Int64
}

func (s *relationshipStatementReadCounter) AllNodes() ([]*storage.Node, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.AllNodes()
}
func (s *relationshipStatementReadCounter) AllEdges() ([]*storage.Edge, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.AllEdges()
}
func (s *relationshipStatementReadCounter) GetNode(id storage.NodeID) (*storage.Node, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.GetNode(id)
}
func (s *relationshipStatementReadCounter) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.GetEdge(id)
}
func (s *relationshipStatementReadCounter) GetNodesByLabel(label string) ([]*storage.Node, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.GetNodesByLabel(label)
}
func (s *relationshipStatementReadCounter) GetEdgesByType(typ string) ([]*storage.Edge, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.GetEdgesByType(typ)
}
func (s *relationshipStatementReadCounter) SeekRelationships(ctx context.Context, typ string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.SeekRelationships(ctx, typ, properties, values)
}

func TestRelationshipAccessProgramSuppliedExplainPurity(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	store := &relationshipStatementReadCounter{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "supplied_explain")}
	e := NewStorageExecutor(store)
	setupRelationshipSuppliedFixture(t, e)
	for _, tc := range relationshipSuppliedStatementCases() {
		t.Run(tc.name, func(t *testing.T) {
			store.reads.Store(0)
			ctx := withRelationshipSeekProfile(context.Background())
			actual, err := e.Execute(ctx, "EXPLAIN "+tc.query, relationshipSuppliedParams())
			require.NoError(t, err)
			require.Empty(t, actual.Rows)
			require.Zero(t, store.reads.Load(), "EXPLAIN cannot evaluate supplied Cypher")
			require.Empty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).seeks)
			assertRelationshipSuppliedProfile(t, e, context.Background(), tc.query, tc.lookups, tc.rows)
			require.Positive(t, store.reads.Load(), "positive physical observation for the same public route")
		})
	}
}

func TestRelationshipAccessProgramPeriodicStatementSummary(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "periodic_summary"))
	setupRelationshipSuppliedFixture(t, e)
	for _, name := range []string{"apoc.periodic.iterate", "apoc.periodic.rock_n_roll"} {
		t.Run(name, func(t *testing.T) {
			// The standalone result includes the complete execution summary.
			// Inline YIELD follows the procedure's registered output contract.
			actual, err := e.Execute(context.Background(), "PROFILE CALL "+name+"($statement,$action,{batchSize:1})", relationshipSuppliedParams())
			require.NoError(t, err)
			require.Len(t, actual.Rows, 1)
			for column, want := range map[string]int64{"committedOperations": 1, "failedOperations": 0} {
				index := -1
				for i, name := range actual.Columns {
					if name == column {
						index = i
						break
					}
				}
				require.NotEqual(t, -1, index, column)
				require.Equal(t, want, actual.Rows[0][index])
			}
			assertRelationshipSuppliedPlan(t, actual, 2)
		})
	}
}
