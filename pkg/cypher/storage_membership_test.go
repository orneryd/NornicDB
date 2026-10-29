package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generate constructions of executors, rather than an enumeration of known
// dispatch entrances. Every construction receives the same storage ownership
// contract across fresh/carried contexts, statement forms and completion modes.
func TestStorageMembershipConstructions(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	ctx := context.Background()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	t.Cleanup(ClearUserProcedures)
	type construction struct {
		name string
		make func(context.Context, *StorageExecutor) (*StorageExecutor, error)
	}
	constructions := []construction{
		{"direct", func(_ context.Context, e *StorageExecutor) (*StorageExecutor, error) { return e, nil }},
		{"clone", func(_ context.Context, e *StorageExecutor) (*StorageExecutor, error) {
			return e.cloneWithStorage(e.storage), nil
		}},
		{"clone_for_storage", func(_ context.Context, e *StorageExecutor) (*StorageExecutor, error) {
			return e.cloneWithStorage(e.storage, selectedDatabaseStorage), nil
		}},
		{"manager", func(ctx context.Context, e *StorageExecutor) (*StorageExecutor, error) {
			selected, _, err := e.useDatabase(ctx, "one", "CREATE (:AcAtom)")
			if err != nil {
				return nil, err
			}
			view, err := selected.FunctionStorage("")
			if err != nil {
				return nil, err
			}
			rebuilt := NewStorageExecutor(view)
			rebuilt.SetDatabaseManager(e.dbManager)
			return rebuilt, nil
		}},
		{"function_catalog", func(_ context.Context, e *StorageExecutor) (*StorageExecutor, error) {
			selected := e.cloneWithStorage(e.storage)
			selected.graphIdentityScope = &graphIdentityScope{database: "one", manager: e.dbManager}
			view, err := selected.FunctionStorage("catalog-selection")
			if err != nil {
				return nil, err
			}
			rebuilt := NewStorageExecutor(view)
			rebuilt.SetDatabaseManager(e.dbManager)
			return rebuilt, nil
		}},
	}
	// All words over the transparent-wrapper alphabet up to depth two. This
	// includes repeated decorators as well as both mixed orders.
	chains := []string{""}
	for depth := 1; depth <= 2; depth++ {
		for bits := 0; bits < 1<<depth; bits++ {
			word := ""
			for bit := 0; bit < depth; bit++ {
				if bits&(1<<bit) == 0 {
					word += "t"
				} else {
					word += "w"
				}
			}
			chains = append(chains, word)
		}
	}
	for _, source := range []string{"storage", "function", "participant", "router"} {
		for _, chain := range chains {
			source, chain := source, chain
			name := source + "_" + chain
			if chain == "" {
				name += "bare"
			}
			constructions = append(constructions, construction{name, func(callCtx context.Context, e *StorageExecutor) (*StorageExecutor, error) {
				view := e.storage
				var err error
				if source == "function" {
					view, err = e.FunctionStorage("")
				}
				if err != nil {
					return nil, err
				}
				if source == "participant" {
					view = e.getStorage(callCtx)
				}
				if source == "router" {
					selected, _, err := e.useDatabase(callCtx, "comp", "RETURN 1")
					if err != nil {
						return nil, err
					}
					view = selected.storage
				}
				for _, wrapper := range chain {
					if wrapper == 't' {
						view = storage.NewTracedEngine(view)
					} else {
						view = storage.NewWALEngine(view, wal)
					}
				}
				nested := NewStorageExecutor(view)
				nested.SetDatabaseManager(e.dbManager)
				return nested, nil
			}})
		}
	}
	var build construction
	var contextKind, form, run string
	var nestedError error
	var calls int
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "membership.construct", Signature: "membership.construct() :: (v :: INTEGER)",
		Mode: ProcedureModeRead, ExecutesStatements: true, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}},
	}, func(callCtx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		calls++
		nested, err := build.make(callCtx, e)
		if err != nil {
			nestedError = err
			return nil, err
		}
		innerCtx := callCtx
		if contextKind != "statement" {
			innerCtx = context.Background()
		}
		if contextKind == "cleared" {
			nested = nested.cloneWithStorage(nested.storage)
			nested.txContext = nil
		}
		write := "CREATE (:AcAtom {run:'" + run + "'}) RETURN 1 AS v"
		query := write
		switch form {
		case "use":
			query = "USE comp.one " + write
		case "script":
			query = "BEGIN " + write + " COMMIT"
		case "case_script":
			query = "BEGIN CALL membership.leaf('" + run + "') YIELD v CASE WHEN false THEN ROLLBACK ELSE RETURN v COMMIT"
		case "call":
			query = "CALL { " + write + " } RETURN v"
		case "procedure":
			query = "CALL membership.leaf('" + run + "') YIELD v RETURN v"
		case "dynamic":
			query = "CALL apoc.cypher.run($body, {}) YIELD value RETURN 1 AS v"
		case "begin", "commit", "rollback":
			query = strings.ToUpper(form)
		}
		if isCompositeRoot(nested.storage) && bareTransactionCommand(query) == "" && !transactionScriptShape(query) && form != "use" {
			query = "USE comp.one " + query
		}
		_, err = nested.Execute(innerCtx, query, map[string]interface{}{"body": write})
		nestedError = err
		return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, err
	}))
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "membership.leaf", Signature: "membership.leaf(run :: STRING) :: (v :: INTEGER)",
		Mode: ProcedureModeRead, ExecutesStatements: true,
		Params: []ProcedureParam{{Name: "run", Type: "STRING"}}, MinArgs: 1, MaxArgs: 1,
		Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}},
	}, func(_ context.Context, e *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		return e.Execute(context.Background(), "CREATE (:AcAtom {run:$run}) RETURN 1 AS v", map[string]interface{}{"run": args[0]})
	}))
	for _, owner := range []string{"native_explicit", "composite_explicit", "native_auto", "composite_auto"} {
		for _, completion := range []string{"keep", "discard"} {
			for _, construction := range constructions {
				if strings.HasPrefix(construction.name, "router_") && !strings.HasPrefix(owner, "composite") {
					continue
				}
				for _, contextName := range []string{"background", "statement", "cleared"} {
					for _, statement := range []string{"data", "use", "script", "case_script", "call", "procedure", "dynamic", "begin", "commit", "rollback"} {
						t.Run(strings.Join([]string{owner, completion, construction.name, contextName, statement}, "/"), func(t *testing.T) {
							build, contextKind, form = construction, contextName, statement
							run = strings.ReplaceAll(t.Name(), "/", "_")
							calls, nestedError = 0, nil
							composite := strings.HasPrefix(owner, "composite")
							explicit := strings.HasSuffix(owner, "explicit")
							store, err := f.mgr.GetStorage("one")
							require.NoError(t, err)
							e := NewStorageExecutor(store)
							e.SetDatabaseManager(f.exec.dbManager)
							if composite {
								e = f.compositeSession(t)
							}
							if explicit {
								_, err = e.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							query := "CALL membership.construct() YIELD v RETURN v"
							if composite {
								query = "USE comp.one " + query
							}
							if !explicit && completion == "discard" {
								query = strings.TrimSuffix(query, "RETURN v") + "RETURN v / 0 AS fail"
							}
							_, execErr := e.Execute(ctx, query, nil)
							control := statement == "script" || statement == "case_script" || statement == "begin" || statement == "commit" || statement == "rollback"
							rejected := control || statement == "use" && !composite
							if explicit {
								end := "COMMIT"
								if completion == "discard" || execErr != nil {
									end = "ROLLBACK"
								}
								_, endErr := e.Execute(ctx, end, nil)
								require.NoError(t, endErr, "the enclosing controller retains terminal ownership")
							}
							want := int64(0)
							if !rejected && completion == "keep" {
								want = 1
							}
							require.Equal(t, want, f.writes(t, "one", run), "effects end only with their owner")
							require.Equal(t, int64(0), f.writes(t, "two", run))
							require.Equal(t, 1, calls, "the generated construction was reached")
							if rejected {
								require.Error(t, nestedError)
								if control {
									require.Contains(t, strings.ToLower(nestedError.Error()), "owned")
								} else {
									require.Contains(t, strings.ToLower(nestedError.Error()), "transaction")
								}
							} else {
								require.NoError(t, nestedError)
								if explicit || completion == "keep" {
									require.NoError(t, execErr)
								} else {
									require.Error(t, execErr)
								}
							}
							// Every assertion above observes the completed owner before cleanup. Keep
							// generated leaves isolated: retaining all successful markers makes later
							// persistence witnesses scan unrelated cases quadratically under -race.
							if want == 1 {
								_, cleanupErr := f.exec.Execute(ctx, "USE one MATCH (n:AcAtom {run:$run}) DETACH DELETE n", map[string]interface{}{"run": run})
								require.NoError(t, cleanupErr)
								require.Zero(t, f.writes(t, "one", run))
							}

						})
					}
				}
			}
		}
	}
	// Unowned storage is an acceptance control: both script forms still work.
	for _, script := range []string{"BEGIN CREATE (:AcAtom {run:'unowned-simple'}) COMMIT", "BEGIN CALL membership.leaf('unowned-conditional') YIELD v CASE WHEN false THEN ROLLBACK ELSE RETURN v COMMIT"} {
		_, err := f.exec.Execute(ctx, script, nil)
		require.NoError(t, err, fmt.Sprintf("unowned control: %s", script))
	}
}

// Transaction roots, routers and remote statement runners are storage issuers
// too. Reconstruct before the first statement and after issuing each view.
func TestStorageMembershipIssuedViews(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	ctx := context.Background()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	wrap := func(view storage.Engine, word string) storage.Engine {
		for _, kind := range word {
			if kind == 't' {
				view = storage.NewTracedEngine(view)
			} else {
				view = storage.NewWALEngine(view, wal)
			}
		}
		return view
	}
	for _, source := range []string{"native_root", "composite_root", "router", "remote"} {
		for _, chain := range []string{"", "t", "w", "tt", "tw", "wt", "ww"} {
			for _, keep := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/keep_%t", source, chain, keep), func(t *testing.T) {
					run := strings.ReplaceAll(t.Name(), "/", "_")
					var original *StorageExecutor
					var coordinator *fabric.FabricTransaction
					var view storage.Engine
					switch source {
					case "native_root", "composite_root":
						store, err := f.mgr.GetStorage("one")
						require.NoError(t, err)
						original = NewStorageExecutor(store)
						original.SetDatabaseManager(f.exec.dbManager)
						if source == "composite_root" {
							original = f.compositeSession(t)
						}
						_, err = original.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						view, err = original.FunctionStorage("")
						require.NoError(t, err)
					case "router", "remote":
						catalog, err := f.exec.buildFabricCatalog()
						require.NoError(t, err)
						coordinator = fabric.NewFabricTransaction(run)
						c := &cypherFabricExecutor{base: f.exec, autoCommit: true, catalog: catalog}
						statement := c.statementFor(ctx, coordinator)
						root, err := f.mgr.GetStorage("comp")
						require.NoError(t, err)
						view = statement.router(root.(*storage.CompositeEngine))
						if source == "remote" {
							// Run the remote engine's operations on an owned participant.
							// No outside engine is ever handed to the nested executor.
							view = storage.NewStatementRemoteEngine(statement, func(ctx context.Context, query string, params map[string]interface{}, _ storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
								store, err := f.mgr.GetStorage("one")
								if err != nil {
									return nil, nil, err
								}
								handle, err := statement.participant("one", store)
								if err != nil {
									return nil, nil, err
								}
								exec := NewStorageExecutor(handle.storage)
								result, err := exec.Execute(ctx, query, params)
								if err != nil {
									return nil, nil, err
								}
								// Match the transport's entity encoding rather than
								// returning the local executor's internal node type.
								for _, row := range result.Rows {
									for i, value := range row {
										if node, ok := value.(*storage.Node); ok {
											row[i] = map[string]interface{}{"elementId": string(node.ID), "labels": node.Labels, "properties": node.Properties}
										}
									}
								}
								return result.Columns, result.Rows, nil
							})
						}
					}
					nested := NewStorageExecutor(wrap(view, chain))
					nested.SetDatabaseManager(f.exec.dbManager)
					query := "CREATE (:AcAtom {run:$run}) RETURN 1 AS v"
					if source == "composite_root" || source == "router" {
						query = "USE comp.one " + query
					}
					_, err = nested.Execute(ctx, query, map[string]interface{}{"run": run})
					require.NoError(t, err)
					require.Zero(t, f.writes(t, "one", run), "no construction commits independently")
					for _, command := range []string{"BEGIN", "COMMIT", "ROLLBACK", "BEGIN CREATE (:AcAtom) COMMIT"} {
						_, err := nested.Execute(ctx, command, nil)
						require.ErrorContains(t, err, "owned")
					}
					if original != nil {
						command := "ROLLBACK"
						if keep {
							command = "COMMIT"
						}
						_, err = original.Execute(ctx, command, nil)
					} else if keep {
						err = coordinator.Commit(nil, nil)
					} else {
						err = coordinator.Rollback(nil)
					}
					require.NoError(t, err)
					want := int64(0)
					if keep {
						want = 1
					}
					require.Equal(t, want, f.writes(t, "one", run))
				})
			}
		}
	}
}

func TestStorageMembershipStorageIsAuthoritative(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	left := NewStorageExecutor(storage.NewNamespacedEngine(base, "left"))
	right := NewStorageExecutor(storage.NewNamespacedEngine(base, "right"))
	_, err := left.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = right.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	// Even a conflicting context is only a hint. A native adapter is the first
	// boundary and must not inherit a different transaction underneath it.
	foreign := context.WithValue(ctx, ctxKeyTxStorage, right.txContext.storageWrapper)
	view, err := left.FunctionStorage("")
	require.NoError(t, err)
	nested := NewStorageExecutor(storage.NewTracedEngine(view))
	_, err = nested.Execute(foreign, "CREATE (:Owned)", nil)
	require.NoError(t, err)
	_, err = left.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	_, err = right.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = nested.Execute(ctx, "BEGIN CREATE (:Escaped) COMMIT", nil)
	require.ErrorContains(t, err, "owned", "an ended view never becomes an unowned base engine")
}

func TestStorageMembershipBatchControl(t *testing.T) {
	ctx := context.Background()
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprint(coordinated), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			owner := NewStorageExecutor(storage.NewNamespacedEngine(base, "one"))
			begin := ctx
			if coordinated {
				begin = storage.WithCoordinatorOwnedTransaction(ctx)
			}
			_, err := owner.Execute(begin, "BEGIN", nil)
			require.NoError(t, err)
			view, err := owner.FunctionStorage("")
			require.NoError(t, err)
			nested := NewStorageExecutor(storage.NewTracedEngine(view))
			_, err = nested.Execute(ctx, "CALL { CREATE (:Batch) } IN TRANSACTIONS", nil)
			require.ErrorContains(t, err, "explicit transaction")
			_, err = nested.Execute(ctx, "CALL apoc.periodic.iterate('RETURN 1 AS v', 'CREATE (:Batch)', {batchSize:1}) YIELD total RETURN total", nil)
			if coordinated {
				require.ErrorContains(t, err, "unsupported query type in transaction")
			} else {
				require.NoError(t, err, "native explicit batches keep their shipped folded semantics")
			}
			_, err = owner.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			count, err := base.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestStorageMembershipControllerFailure(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	owner := NewStorageExecutor(storage.NewNamespacedEngine(base, "one"))
	_, err := owner.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = owner.Execute(ctx, "CREATE (:Pending)", nil)
	require.NoError(t, err)
	_, err = owner.Execute(ctx, "BEGIN", nil)
	require.ErrorContains(t, err, "owned")
	_, err = owner.Execute(ctx, "COMMIT", nil)
	require.ErrorContains(t, err, "was rolled back")
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}
