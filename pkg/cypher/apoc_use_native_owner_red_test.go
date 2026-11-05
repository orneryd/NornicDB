package cypher

import (
	"context"
	"errors"
	"fmt"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type apocUseAuthProbeManager struct {
	standardDBManager
	otherLookups int
}

func (m *apocUseAuthProbeManager) GetStorageForUse(name, token string) (interface{}, error) {
	resolved, err := m.ResolveDatabase(name)
	if err == nil && resolved == "other" {
		m.otherLookups++
	}
	return m.standardDBManager.GetStorageForUse(name, token)
}
func newApocUseNativeProof(t *testing.T) (*StorageExecutor, *apocUseAuthProbeManager) {
	t.Helper()
	base := newTestMemoryEngine(t)
	home := storage.NewNamespacedEngine(base, "nornic")
	other := storage.NewNamespacedEngine(base, "other")
	for _, x := range []struct {
		engine storage.Engine
		value  string
	}{{home, "home-value"}, {other, "other-only-secret"}} {
		_, err := x.engine.CreateNode(t.Context(), &storage.Node{ID: "secret", Labels: []string{"Secret"}, Properties: map[string]any{"value": x.value}})
		require.NoError(t, err)
	}
	manager := &apocUseAuthProbeManager{standardDBManager: standardDBManager{engines: map[string]storage.Engine{"nornic": home, "other": other}, aliases: map[string]string{"other_alias": "other"}}}
	executor := NewStorageExecutor(home)
	executor.SetDatabaseManager(manager)
	return executor, manager
}

func TestApocUseOtherNativeOwnerPublicContract(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			body := "USE other MATCH (s:Secret) RETURN s.value AS v"
			for _, route := range []struct {
				name, query string
				params      map[string]interface{}
				wrapped     bool
			}{
				{name: "top USE", query: body},
				{name: "CALL subquery", query: "CALL { " + body + " } RETURN v"},
				{name: "APOC literal", query: "CALL apoc.cypher.run('" + body + "', {}) YIELD value RETURN value", wrapped: true},
				{name: "APOC parameter", query: "CALL apoc.cypher.run($body, {}) YIELD value RETURN value", params: map[string]interface{}{"body": body}, wrapped: true},
				{name: "APOC alias", query: "CALL apoc.cypher.run('USE other_alias MATCH (s:Secret) RETURN s.value AS v', {}) YIELD value RETURN value", wrapped: true},
			} {
				t.Run(route.name, func(t *testing.T) {
					for _, denied := range []bool{true, false} {
						mode := "allowed"
						if denied {
							mode = "denied"
						}
						t.Run(mode, func(t *testing.T) {
							executor, manager := newApocUseNativeProof(t)
							var decisions []string
							ctx := WithDatabasePermissionResolver(t.Context(), "nornic", func(db, permission string) bool {
								decisions = append(decisions, db+":"+permission)
								return !denied || !strings.EqualFold(db, "other")
							})
							result, err := executor.Execute(ctx, route.query, route.params)
							if denied {
								var forbidden *PermissionDeniedError
								require.ErrorAs(t, err, &forbidden)
								require.Equal(t, "read", forbidden.Permission)
								require.Zero(t, manager.otherLookups, "authorization must precede target storage acquisition")
								if result != nil {
									require.Empty(t, result.Rows)
								}
								return
							}
							require.NoError(t, err, "autocommit dynamic procedure can choose another standard database without escaping one statement owner")
							require.Contains(t, decisions, "other:read")
							require.Positive(t, manager.otherLookups)
							if route.wrapped {
								require.Equal(t, [][]interface{}{{map[string]interface{}{"v": "other-only-secret"}}}, result.Rows)
							} else {
								require.Equal(t, [][]interface{}{{"other-only-secret"}}, result.Rows)
							}
						})
					}
				})
			}
			t.Run("same database stays healthy", func(t *testing.T) {
				executor, _ := newApocUseNativeProof(t)
				ctx := WithDatabasePermissionResolver(t.Context(), "nornic", func(string, string) bool { return true })
				result, err := executor.Execute(ctx, "CALL apoc.cypher.run('MATCH (s:Secret) RETURN s.value AS v', {}) YIELD value RETURN value", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{map[string]interface{}{"v": "home-value"}}}, result.Rows)
			})
			t.Run("explicit transaction remains single database", func(t *testing.T) {
				executor, _ := newApocUseNativeProof(t)
				ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(string, string) bool { return true })
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, rollbackErr := executor.Execute(ctx, "ROLLBACK", nil); require.NoError(t, rollbackErr) })
				_, err = executor.Execute(ctx, "CALL apoc.cypher.run('"+body+"', {}) YIELD value RETURN value", nil)
				require.ErrorContains(t, err, "Accessing more than one database per transaction is not allowed")
			})
		})
	}
}

func TestStandardCallUseWriteNativeOwnerPublicContract(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, query := range []string{
				"CALL { USE other CREATE (n:Made {id:1}) RETURN n.id AS v } RETURN v",
				"CALL apoc.cypher.run('USE other CREATE (n:Made {id:1}) RETURN n.id AS v',{}) YIELD value RETURN value",
				"MATCH (s:Secret) CALL { USE other CREATE (n:Made {id:1}) RETURN n.id AS v } RETURN v",
				"CALL { USE other MATCH (s:Secret) RETURN s.value AS secret } CREATE (n:HomeMade) RETURN secret",
			} {
				t.Run(query, func(t *testing.T) {
					executor, manager := newApocUseNativeProof(t)
					ctx := WithDatabasePermissionResolver(t.Context(), "nornic", func(string, string) bool { return true })
					contract, contractErr := readStatementExecutionContract(ctx, query)
					require.NoError(t, contractErr)
					t.Logf("contract=%+v fabric=%v", contract, executor.shouldUseFabricPlanner(ctx, query))
					result, err := executor.Execute(ctx, query, nil)
					require.NoError(t, err, "autocommit may read multiple databases and write at most one")
					require.Len(t, result.Rows, 1)
					require.EqualValues(t, 1, result.Stats.NodesCreated)
					made, err := manager.engines["other"].GetNodesByLabel(ctx, "Made")
					require.NoError(t, err)
					home, err := manager.engines["nornic"].GetNodesByLabel(ctx, "HomeMade")
					require.NoError(t, err)
					if strings.Contains(query, "HomeMade") {
						require.Len(t, home, 1)
						require.Empty(t, made)
						require.Equal(t, "other-only-secret", result.Rows[0][0])
					} else {
						require.Empty(t, home)
						require.Len(t, made, 1)
						require.EqualValues(t, 1, made[0].Properties["id"])
					}
				})
			}
		})
	}
}

// An opaque callback owns all children, including direct storage writes, before
// it learns which database a child selects. Failure must not publish a prefix.
func TestNativeDynamicCoordinatorAtomicCallbacks(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, scenario := range []string{"same-database", "cross-database", "direct-write", "fresh-auth", "fresh-cancel", "root-batch-second-writer", "batch-first-writer", "batch-callback-first-writer", "other-batch-rejected"} {
				t.Run(scenario, func(t *testing.T) {
					exec, manager := newApocUseNativeProof(t)
					cleanupAdmissionProcedure(t, "ownerproof.dynamic")
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ctx = WithDatabasePermissionResolver(ctx, "nornic", func(database, permission string) bool { return scenario != "fresh-auth" || database != "other" })
					failure := errors.New("ownerproof deliberately rejected")
					require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.dynamic", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(callbackCtx context.Context, nested *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
						if scenario == "fresh-auth" {
							_, err := nested.Execute(context.Background(), "USE other MATCH (s:Secret) RETURN s.value", nil)
							return nil, err
						}
						if scenario == "fresh-cancel" {
							cancel()
							_, err := nested.Execute(context.Background(), "CREATE (:OwnerProof)", nil)
							require.ErrorIs(t, err, context.Canceled, "issued storage retains request lifetime before child work")
							return nil, err
						}
						if scenario == "direct-write" {
							_, err := nested.storage.CreateNode(t.Context(), &storage.Node{ID: "owner-direct", Labels: []string{"OwnerProof"}})
							if err != nil {
								return nil, err
							}
						} else if scenario != "batch-first-writer" && scenario != "batch-callback-first-writer" {
							_, err := nested.Execute(callbackCtx, "CREATE (:OwnerProof)", nil)
							if err != nil {
								return nil, err
							}
						}
						target := ""
						if scenario == "cross-database" {
							target = "USE other "
						}
						if scenario == "root-batch-second-writer" || scenario == "batch-first-writer" || scenario == "batch-callback-first-writer" {
							action := "CREATE (:OwnerBatch)"
							if scenario == "batch-callback-first-writer" {
								cleanupAdmissionProcedure(t, "ownerproof.batchcreate")
								require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.batchcreate", Mode: ProcedureModeWrite}, func(_ context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
									_, err := e.storage.CreateNode(t.Context(), &storage.Node{ID: "batch-direct", Labels: []string{"OwnerBatch"}})
									return &ExecuteResult{}, err
								}))
								action = "CALL ownerproof.batchcreate()"
							}
							_, err := nested.Execute(callbackCtx, "CALL apoc.periodic.iterate('RETURN 1 AS x','"+action+"',{batchSize:1})", nil)
							if err != nil {
								return nil, err
							}
							_, err = nested.Execute(callbackCtx, "USE other CREATE (:OwnerProof)", nil)
							return nil, err
						}
						if scenario == "other-batch-rejected" {
							_, err := nested.Execute(callbackCtx, "USE other CALL apoc.periodic.iterate('RETURN 1 AS x','CREATE (:OwnerBatch)',{batchSize:1})", nil)
							return nil, err
						}
						_, err := nested.Execute(callbackCtx, target+"CREATE (:OwnerProof)", nil)
						if err != nil {
							return nil, err
						}
						return nil, failure
					}))
					_, err := exec.Execute(ctx, "CALL ownerproof.dynamic() YIELD v RETURN v", nil)
					require.Error(t, err)
					switch scenario {
					case "fresh-auth":
						var denied *PermissionDeniedError
						require.ErrorAs(t, err, &denied)
						require.Zero(t, manager.otherLookups)
					case "fresh-cancel":
						require.ErrorIs(t, err, context.Canceled)
					case "cross-database", "root-batch-second-writer", "batch-first-writer", "batch-callback-first-writer":
						require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
					case "other-batch-rejected":
						require.ErrorContains(t, err, "transaction")
					default:
						require.ErrorContains(t, err, failure.Error())
					}
					for _, db := range []string{"nornic", "other"} {
						nodes, readErr := manager.engines[db].GetNodesByLabel(context.Background(), "OwnerProof")
						require.NoError(t, readErr)
						require.Empty(t, nodes, "failed root must publish no staged child in %s", db)
					}
					for _, db := range []string{"nornic", "other"} {
						nodes, readErr := manager.engines[db].GetNodesByLabel(context.Background(), "OwnerBatch")
						require.NoError(t, readErr)
						if (scenario == "root-batch-second-writer" || scenario == "batch-first-writer" || scenario == "batch-callback-first-writer") && db == "nornic" {
							require.Len(t, nodes, 1, "authorized independent batch retains commit")
						} else {
							require.Empty(t, nodes)
						}
					}
				})
			}
		})
	}
}

func TestNativeDynamicCoordinatorTerminalMetadata(t *testing.T) {
	for _, outcome := range []string{"success", "rollback", "committed-wal-failure"} {
		t.Run(outcome, func(t *testing.T) {
			executor, engine, store := newWALOutcomeExecutor(t)
			failure := errors.New("ownerproof terminal failure")
			cleanupAdmissionProcedure(t, "ownerproof.metadata")
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.metadata", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				wrapper := membershipOfStorage(e.storage).storage
				require.Equal(t, "native-value", wrapper.tx.GetMetadata()["request"])
				result, err := e.Execute(ctx, "CREATE (:WalOutcome) RETURN 1 AS v", nil)
				if err != nil {
					return nil, err
				}
				if outcome == "rollback" {
					require.NoError(t, engine.wal.Close())
					return nil, failure
				}
				if outcome == "committed-wal-failure" {
					engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
						err := tx.CommitContext(ctx)
						if err == nil {
							require.NoError(t, engine.wal.Close())
						}
						return err
					}
				}
				return result, nil
			}))
			result, err := executor.Execute(WithTransactionMetadata(t.Context(), map[string]any{"request": "native-value"}), "CALL ownerproof.metadata() YIELD v RETURN v", nil)
			switch outcome {
			case "success":
				require.NoError(t, err)
				require.NotNil(t, result.Metadata["receipt"])
				require.EqualValues(t, 1, result.Stats.NodesCreated)
				requireWALOutcomeCount(t, store, 1)
			case "rollback":
				require.ErrorIs(t, err, failure)
				require.ErrorIs(t, err, storage.ErrWALClosed)
				requireWALOutcomeCount(t, store, 0)
			case "committed-wal-failure":
				require.ErrorIs(t, err, storage.ErrWALClosed)
				require.False(t, nornicerrors.IsCommitRolledBack(err))
				requireWALOutcomeCount(t, store, 1)
			}
		})
	}
}

func TestNativeStatementOwnerSelectionUsesGrammarClause(t *testing.T) {
	for _, tc := range []struct {
		query string
		owner bool
	}{
		{"CREATE (n {use:1}) RETURN n", false},
		{"CREATE (n:USE) RETURN n", false},
		{"CREATE (`USE`:Owner) RETURN `USE`", false},
		{"CREATE (n {text:'USE other'}) RETURN n", false},
		{"CREATE (n) /* USE other */ RETURN n", false},
		{"CALL { USE other CREATE (n) RETURN n } RETURN n", true},
		{"CALL { USE other RETURN 1 AS v } CREATE (n) RETURN v", true},
		{"CALL { USE other MATCH (n) RETURN n } RETURN n", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			contract, err := readStatementExecutionContract(t.Context(), tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.owner, contract.selectsDatabase)
		})
	}
}

func TestNativeDynamicCoordinatorPreparedRemoteRoute(t *testing.T) {
	for _, scenario := range []string{"read", "write", "rollback", "second-writer"} {
		t.Run(scenario, func(t *testing.T) {
			composite, stores := newRemoteModeExecutor(t, [2]string{"owner", "owner"})
			manager := composite.dbManager.(*testDatabaseManagerAdapter).manager
			native, err := manager.GetStorage(manager.DefaultDatabaseName())
			require.NoError(t, err)
			executor := NewStorageExecutor(native)
			executor.SetDatabaseManager(composite.dbManager)
			body := "USE remote_modes.one RETURN 1 AS v"
			if scenario != "read" {
				body = "USE remote_modes.one CREATE (:NativeRemoteOwner) RETURN 1 AS v"
			}
			if scenario == "second-writer" {
				body = "CREATE (:NativeLocalOwner) WITH 1 AS keep CALL apoc.cypher.run('" + body + "',{}) YIELD value RETURN value"
			}
			query := "CALL apoc.cypher.run($body,{}) YIELD value RETURN value"
			if scenario == "rollback" {
				query += " WITH value RETURN 1 / $zero"
			}
			result, err := executor.Execute(t.Context(), query, map[string]interface{}{"body": body, "zero": int64(0)})
			if scenario == "read" || scenario == "write" {
				require.NoError(t, err)
				require.NotNil(t, result)
			} else {
				require.Error(t, err)
			}
			nodes, err := stores[0].GetNodesByLabel(t.Context(), "NativeRemoteOwner")
			require.NoError(t, err)
			if scenario == "write" {
				require.Len(t, nodes, 1)
			} else {
				require.Empty(t, nodes)
			}
			local, err := native.GetNodesByLabel(t.Context(), "NativeLocalOwner")
			require.NoError(t, err)
			require.Empty(t, local)
		})
	}
}

func TestNativeDynamicCoordinatorCommitCancellation(t *testing.T) {
	for _, phased := range []bool{false, true} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("phased=%t/before=%t", phased, before), func(t *testing.T) {
				executor, engine, store := newWALOutcomeExecutor(t)
				ctx, cancel := context.WithCancel(context.Background())
				if phased {
					cancel()
					ctx, cancel = txphase.WithExecutionDeadline(context.Background(), time.Minute)
				}
				defer cancel()
				type terminalInputKey struct{}
				identity := new(int)
				ctx = context.WithValue(ctx, terminalInputKey{}, identity)
				commits := 0
				engine.commit = func(commitCtx context.Context, tx *storage.BadgerTransaction) error {
					commits++
					require.NoError(t, commitCtx.Err())
					require.Same(t, identity, commitCtx.Value(terminalInputKey{}), "terminal input preserves originating values")
					err := tx.CommitContext(commitCtx)
					if !before {
						cancel()
						if phased {
							require.NoError(t, commitCtx.Err(), "admitted phase retains its detached lifetime")
						} else {
							require.ErrorIs(t, commitCtx.Err(), context.Canceled, "unphased input must not be detached by Fabric")
						}
					}
					return err
				}
				cleanupAdmissionProcedure(t, "ownerproof.commitcancel")
				require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.commitcancel", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
					result, err := e.Execute(ctx, "CREATE (:WalOutcome) RETURN 1 AS v", nil)
					if before {
						cancel()
					}
					return result, err
				}))
				result, err := executor.Execute(ctx, "CALL ownerproof.commitcancel() YIELD v RETURN v", nil)
				if before {
					require.ErrorIs(t, err, context.Canceled)
					require.Zero(t, commits)
					requireWALOutcomeCount(t, store, 0)
					require.False(t, txphase.CommitAdmitted(ctx))
				} else {
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Equal(t, 1, commits)
					requireWALOutcomeCount(t, store, 1)
					require.Equal(t, phased, txphase.CommitAdmitted(ctx))
				}
			})
		}
	}
}

func TestNativeDynamicCoordinatorIssuedStorageSelection(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, scenario := range []string{"read", "write", "write-rollback", "denied"} {
				for _, spoofed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/spoofed=%t", scenario, spoofed), func(t *testing.T) {
						executor, manager := newApocUseNativeProof(t)
						alien := storage.NewNamespacedEngine(newTestMemoryEngine(t), "other")
						_, err := alien.CreateNode(t.Context(), &storage.Node{ID: "secret", Labels: []string{"Secret"}, Properties: map[string]any{"value": "wrong-manager"}})
						require.NoError(t, err)
						replacement := &apocUseAuthProbeManager{standardDBManager: standardDBManager{engines: map[string]storage.Engine{"other": alien, "nornic": manager.engines["nornic"]}}}
						failure := errors.New("issued carrier child rollback")
						cleanupAdmissionProcedure(t, "ownerproof.issued")
						require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.issued", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "ANY"}}}, func(_ context.Context, nested *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
							fresh := NewStorageExecutor(nested.storage)
							if spoofed {
								fresh.SetDatabaseManager(replacement)
							}
							query := "USE other MATCH (s:Secret) RETURN s.value AS v"
							if strings.HasPrefix(scenario, "write") {
								query = "USE other CREATE (:IssuedCarrierMade) RETURN 1 AS v"
							}
							result, err := fresh.Execute(context.Background(), query, nil)
							if scenario != "denied" {
								require.NoError(t, err, "issued storage keeps its authoritative selection owner")
								if err == nil && scenario == "read" {
									require.Equal(t, [][]interface{}{{"other-only-secret"}}, result.Rows)
								}
							}
							if err == nil && scenario == "write-rollback" {
								return nil, failure
							}
							return result, err
						}))
						ctx := WithDatabasePermissionResolver(t.Context(), "nornic", func(database, permission string) bool { return scenario != "denied" || database != "other" })
						result, err := executor.Execute(ctx, "CALL ownerproof.issued() YIELD v RETURN v", nil)
						switch scenario {
						case "denied":
							var denied *PermissionDeniedError
							require.ErrorAs(t, err, &denied)
							require.Zero(t, manager.otherLookups)
						case "write-rollback":
							require.ErrorIs(t, err, failure)
						default:
							require.NoError(t, err)
							require.Len(t, result.Rows, 1)
						}
						require.Zero(t, replacement.otherLookups, "replacement manager cannot acquire a target")
						actual, err := manager.engines["other"].GetNodesByLabel(t.Context(), "IssuedCarrierMade")
						require.NoError(t, err)
						wrong, err := alien.GetNodesByLabel(t.Context(), "IssuedCarrierMade")
						require.NoError(t, err)
						require.Empty(t, wrong)
						if scenario == "write" {
							require.Len(t, actual, 1)
						} else {
							require.Empty(t, actual)
						}
					})
				}
			}
		})
	}
}

func TestNativeDynamicCoordinatorEmptyReaderOwner(t *testing.T) {
	for _, scenario := range []string{"read", "noop-write", "metadata", "empty-wal-error", "completion-action", "knowledge-schema"} {
		t.Run(scenario, func(t *testing.T) {
			executor, engine, store := newWALOutcomeExecutor(t)
			id, err := store.CreateNode(t.Context(), &storage.Node{ID: "seed", Labels: []string{"WalOutcome"}})
			require.NoError(t, err)
			commits := 0
			engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
				commits++
				return tx.CommitContext(ctx)
			}
			var snapshot *storage.BadgerTransaction
			cleanupAdmissionProcedure(t, "ownerproof.empty")
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.empty", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				wrapper := membershipOfStorage(e.storage).storage
				snapshot = wrapper.tx
				query := "MATCH (n:WalOutcome) RETURN count(n) AS v"
				switch scenario {
				case "noop-write":
					query = "MATCH (n:AbsentOwner) SET n.value = 1 RETURN count(n) AS v"
				case "metadata":
					query = "CALL tx.setMetaData({afterRead:'kept-until-end'})"
				case "completion-action":
					query = "MATCH (n:WalOutcome) CALL kp.suppress(n)"
				case "knowledge-schema":
					query = "CREATE PROMOTION PROFILE owner_empty_policy OPTIONS {multiplier:1.25,scope:'NODE',scoreFloor:0.4,scoreCap:0.95}"
				}
				_, err := e.Execute(ctx, query, nil)
				if err != nil {
					return nil, err
				}
				if scenario == "completion-action" || scenario == "knowledge-schema" {
					require.True(t, wrapper.hasPendingCommitEffects())
				} else {
					require.False(t, wrapper.hasPendingCommitEffects())
				}
				if scenario == "metadata" {
					require.Equal(t, "kept-until-end", snapshot.GetMetadata()["afterRead"])
				}
				if scenario == "empty-wal-error" {
					require.NoError(t, engine.wal.Close())
				}
				return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, nil
			}))
			_, err = executor.Execute(t.Context(), "CALL ownerproof.empty() YIELD v RETURN v", nil)
			if scenario == "empty-wal-error" {
				require.ErrorIs(t, err, storage.ErrWALClosed)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
				require.NotContains(t, err.Error(), "no active transaction")
			} else {
				require.NoError(t, err)
			}
			if scenario == "completion-action" || scenario == "knowledge-schema" {
				require.Equal(t, 1, commits, "pending non-entity effects must commit")
			} else {
				require.Zero(t, commits, "empty snapshots must not enter native publication")
			}
			_, readErr := snapshot.GetNode(t.Context(), id)
			require.ErrorIs(t, readErr, storage.ErrTransactionClosed, "terminal owner releases actual snapshot")
			if scenario == "completion-action" {
				_, err = store.GetNode(t.Context(), id)
				require.ErrorIs(t, err, storage.ErrNotFound)
				_, err = storage.GetStoredNode(t.Context(), store, id)
				require.NoError(t, err, "suppression keeps stored entity")
			}
		})
	}
}

func TestNativeDynamicCoordinatorDeferredValidationIsNotEmpty(t *testing.T) {
	forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, executor *StorageExecutor, before *storage.Edge) {
		cleanupAdmissionProcedure(t, "ownerproof.hidden")
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.hidden", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
			_, err := e.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload='stored'", nil)
			if err != nil {
				return nil, err
			}
			wrapper := membershipOfStorage(e.storage).storage
			require.Zero(t, wrapper.tx.OperationCount())
			require.True(t, wrapper.tx.RequiresCommitValidation())
			require.True(t, wrapper.hasPendingCommitEffects())
			return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, nil
		}))
		_, err := executor.Execute(t.Context(), "CALL ownerproof.hidden() YIELD v RETURN v", nil)
		require.NoError(t, err)
		requireHiddenRelationshipUnchanged(t, base, before)
	})
}

func TestNativeDynamicCoordinatorDoesNotGrantManagedBatchException(t *testing.T) {
	for _, managed := range []string{"remote-marker", "fabric-context"} {
		t.Run(managed, func(t *testing.T) {
			executor, manager := newApocUseNativeProof(t)
			calls := 0
			cleanupAdmissionProcedure(t, "ownerproof.managedbatch")
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.managedbatch", Mode: ProcedureModeWrite}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				calls++
				return e.Execute(ctx, "CREATE (:ManagedBatchEscape)", nil)
			}))
			ctx := context.Background()
			if managed == "remote-marker" {
				ctx = storage.WithCoordinatorOwnedTransaction(ctx)
			} else {
				ctx = fabric.WithFabricTransaction(ctx, fabric.NewFabricTransaction("foreign-owner"))
			}
			_, err := executor.Execute(ctx, "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", map[string]interface{}{"body": "CALL apoc.periodic.iterate('RETURN 1 AS x','CALL ownerproof.managedbatch()',{batchSize:1})"})
			require.Error(t, err)
			require.Zero(t, calls, "managed scope must reject before callback")
			nodes, err := manager.engines["nornic"].GetNodesByLabel(t.Context(), "ManagedBatchEscape")
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}

func TestNativeDynamicCoordinatorCommitConflictReturnsNoResult(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			executor, manager := newApocUseNativeProof(t)
			_, err := executor.Execute(t.Context(), "CREATE CONSTRAINT owner_unique FOR (n:OwnerUnique) REQUIRE n.key IS UNIQUE", nil)
			require.NoError(t, err)
			producedRows := 0
			cleanupAdmissionProcedure(t, "ownerproof.conflict")
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownerproof.conflict", Mode: ProcedureModeWrite, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				result, err := e.Execute(ctx, "UNWIND [1,1] AS value CREATE (n:OwnerUnique {key:value}) RETURN count(n) AS v", nil)
				require.NoError(t, err, "deferred native validation follows callback row production")
				if result != nil {
					producedRows = len(result.Rows)
				}
				return result, err
			}))
			result, err := executor.Execute(t.Context(), "CALL ownerproof.conflict() YIELD v RETURN v", nil)
			var violation *storage.ConstraintViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, 1, producedRows)
			require.Nil(t, result, "failed native commit cannot return staged rows")
			require.True(t, nornicerrors.IsCommitRolledBack(err))
			nodes, readErr := manager.engines["nornic"].GetNodesByLabel(t.Context(), "OwnerUnique")
			require.NoError(t, readErr)
			require.Empty(t, nodes)
		})
	}
}
