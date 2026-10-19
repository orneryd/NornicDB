package cypher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newProcedureAdmissionExecutor(t *testing.T, composite bool) (*StorageExecutor, storage.Engine) {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("admission_shard"))
	require.NoError(t, manager.CreateCompositeDatabase("admission_comp", []multidb.ConstituentRef{{
		Alias: "one", DatabaseName: "admission_shard", Type: "local", AccessMode: "read_write",
	}}))
	database := "admission_shard"
	if composite {
		database = "admission_comp"
	}
	store, err := manager.GetStorage(database)
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	exec.SetAllowLocalAPOCFileAccess(true)
	shard, err := manager.GetStorage("admission_shard")
	require.NoError(t, err)
	return exec, shard
}

func admissionMatrixQuery(call string, composite, reject bool) string {
	query := call + " RETURN value"
	if reject {
		query = call + " CALL u5matrix.reject() YIELD rejected RETURN value"
	}
	if composite {
		query = "CALL { USE admission_comp.one " + query + " } RETURN value"
	}
	return query
}

func registerAdmissionMatrixReject(t *testing.T) error {
	t.Helper()
	cleanupAdmissionProcedure(t, "u5matrix.reject")
	failure := errors.New("u5 admission matrix rejected after procedure")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "u5matrix.reject", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "rejected", Type: "INTEGER"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return nil, failure
	}))
	return failure
}

// Restore only the registration owned by this fixture. Other tests may have
// registered procedures whose contracts must remain in the generated inventory.
func cleanupAdmissionProcedure(t *testing.T, name string) {
	t.Helper()
	r := globalProcedureRegistry
	r.mu.RLock()
	previous, existed := r.user[name]
	r.mu.RUnlock()
	t.Cleanup(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if existed {
			r.user[name] = previous
			return
		}
		delete(r.user, name)
		fold := lowerASCII(name)
		for other := range r.user {
			if lowerASCII(other) == fold {
				return
			}
		}
		delete(r.userFold, fold)
	})
}

// Generate the admission grid from every registered contract, rather than a
// list of procedure names. The controlled handlers isolate admission from each
// original handler's argument/setup requirements. Actual shipped dynamic and
// import handlers are exercised separately below; this is not their body test.
func TestRegisteredProcedureAdmissionMatrix(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	origins := ListRegisteredProcedures()
	failure := registerAdmissionMatrixReject(t)
	for index, origin := range origins {
		bodies := []bool{false}
		if origin.ExecutesStatements {
			bodies = []bool{false, true}
		}
		for _, dynamicWrite := range bodies {
			for _, composite := range []bool{false, true} {
				for _, explicit := range []bool{false, true} {
					for _, reject := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/dynamicWrite=%v/fabric=%v/explicit=%v/failure=%v", origin.Name, dynamicWrite, composite, explicit, reject), func(t *testing.T) {
							exec, shard := newProcedureAdmissionExecutor(t, composite)
							alias := fmt.Sprintf("u5matrix.contract%d", index)
							cleanupAdmissionProcedure(t, alias)
							spec := origin
							spec.Name, spec.Signature = alias, alias+"() :: (value :: INTEGER)"
							spec.Params = nil
							spec.Returns = []ProcedureColumn{{Name: "value", Type: "INTEGER"}}
							spec.MinArgs, spec.MaxArgs = 0, 0
							call := "CALL " + alias + "() YIELD value"
							if spec.ExecutesStatements {
								spec.MinArgs, spec.MaxArgs = 2, 2
								spec.Signature = alias + "(statement :: STRING, params :: MAP) :: (value :: INTEGER)"
								spec.Params = []ProcedureParam{{Name: "statement", Type: "STRING"}, {Name: "params", Type: "MAP"}}
								call = "CALL " + alias + "($statement,{}) YIELD value"
							}
							writes := origin.Mode == ProcedureModeWrite || origin.ExecutesStatements && dynamicWrite
							invoked, transactional := false, false
							require.NoError(t, RegisterUserProcedure(spec, func(ctx context.Context, nested *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
								invoked, transactional = true, nested.inTransaction(ctx)
								if origin.ExecutesStatements {
									result, err := nested.Execute(ctx, args[0].(string), nil)
									transactional = nested.inTransaction(ctx)
									return result, err
								}
								if origin.OwnsTransactions {
									return nested.executePeriodicBatch(ctx, origin.Name, func(batchCtx context.Context, batch *StorageExecutor) (*ExecuteResult, error) {
										transactional = batch.inTransaction(batchCtx)
										return batch.Execute(batchCtx, "CREATE (:U5AdmissionMatrix) RETURN 1 AS value", nil)
									})
								}
								if writes {
									return nested.Execute(ctx, "CREATE (:U5AdmissionMatrix) RETURN 1 AS value", nil)
								}
								return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
							}))
							ctx := context.Background()
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							statement := "RETURN 1 AS value"
							if dynamicWrite {
								statement = "CREATE (:U5AdmissionMatrix) RETURN 1 AS value"
							}
							_, err := exec.Execute(ctx, admissionMatrixQuery(call, composite, reject), map[string]interface{}{"statement": statement})
 if origin.OwnsTransactions && composite {
  requireBatchScopeRejected(t,exec,shard,explicit,err)
  require.False(t,invoked,"scope rejection must precede the registered handler")
  return
 }
							if reject {
								require.ErrorContains(t, err, failure.Error())
							} else {
								require.NoError(t, err)
							}
							require.True(t, invoked, "the contract handler must actually run")
							require.Equal(t, explicit || writes || origin.ExecutesStatements && !composite, transactional, "genuine READ contracts must not become write participants")
							if explicit {
								_, err = exec.Execute(ctx, "COMMIT", nil)
								if reject {
									require.Error(t, err)
								} else {
									require.NoError(t, err)
								}
							}
							nodes, err := shard.GetNodesByLabel("U5AdmissionMatrix")
							require.NoError(t, err)
							want := 0
							if writes && (!reject || origin.OwnsTransactions && !explicit) {
								want = 1
							}
							require.Len(t, nodes, want)
						})
					}
				}
			}
		}
		t.Logf("registry matrix origin=%s mode=%s dynamic=%v: success/failure x native/Fabric x auto/explicit", origin.Name, origin.Mode, origin.ExecutesStatements)
	}
}

func TestRegisteredDynamicProcedureBodiesAndImportIDs(t *testing.T) {
	// Fabric participants construct their own executor from runtime settings.
	// Enable local imports for this fixture through that same setting.
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	ensureBuiltInProceduresRegistered()
	failure := registerAdmissionMatrixReject(t)
	profiles := map[string]string{
		"apoc.cypher.run":           "CALL apoc.cypher.run($statement,{}) YIELD value",
		"apoc.cypher.doIt":          "CALL apoc.cypher.doIt($statement,{}) YIELD value",
		"apoc.cypher.doitall":       "CALL apoc.cypher.doitall($statement,{}) YIELD value",
		"apoc.cypher.runMany":       "CALL apoc.cypher.runMany($statement,{}, {statistics:false}) YIELD result AS value",
		"apoc.periodic.iterate":     "CALL apoc.periodic.iterate('RETURN 1 AS input',$statement,{}) YIELD total AS value",
		"apoc.periodic.rock_n_roll": "CALL apoc.periodic.rock_n_roll('RETURN 1 AS input',$statement,{}) YIELD total AS value",
		"apoc.periodic.commit":      "CALL apoc.periodic.commit($merge,{}) YIELD updates AS value",
		"apoc.export.json.query":    "CALL apoc.export.json.query($statement,'',{stream:true}) YIELD rows AS value",
		"apoc.export.csv.query":     "CALL apoc.export.csv.query($statement,'',{stream:true}) YIELD rows AS value",
		"apoc.import.json":          "CALL apoc.import.json($file) YIELD nodes AS value",
	}
	for _, procedure := range ListRegisteredProcedures() {
		call, exists := profiles[procedure.Name]
		if procedure.ExecutesStatements || procedure.OwnsTransactions {
			require.True(t, exists, "missing actual-handler fixture for dynamic registry contract %s", procedure.Name)
		}
		if !exists {
			continue
		}
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, reject := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/fabric=%v/explicit=%v/failure=%v", procedure.Name, composite, explicit, reject), func(t *testing.T) {
						exec, shard := newProcedureAdmissionExecutor(t, composite)
						ctx := context.Background()
						file := filepath.Join(t.TempDir(), "import.json")
						require.NoError(t, os.WriteFile(file, []byte(`{"nodes":[{"id":"imported1","labels":["U5AdmissionMatrix"]},{"id":"4:foreign:opaque","labels":["U5AdmissionMatrix"]}],"relationships":[{"id":"rel:1","type":"R","startNode":"imported1","endNode":"4:foreign:opaque"}]}`), 0600))
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						_, err := exec.Execute(ctx, admissionMatrixQuery(call, composite, reject), map[string]interface{}{
							"statement": "CREATE (:U5AdmissionMatrix) RETURN 1 AS value",
							"merge":     "MERGE (:U5AdmissionMatrix {id:1}) RETURN 1 AS value", "file": file,
						})
						if procedure.OwnsTransactions && composite {
 requireBatchScopeRejected(t,exec,shard,explicit,err)
 return
}
      if reject {
							require.ErrorContains(t, err, failure.Error())
						} else {
							require.NoError(t, err)
						}
						if explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							if reject {
								require.Error(t, err)
							} else {
								require.NoError(t, err)
							}
						}
						nodes, err := shard.GetNodesByLabel("U5AdmissionMatrix")
						require.NoError(t, err)
						want := 1
						if procedure.Name == "apoc.import.json" {
							want = 2
						}
						if reject && (!procedure.OwnsTransactions || explicit) {
							want = 0
						}
						require.Len(t, nodes, want)
						if procedure.Name == "apoc.import.json" && !reject {
							for _, id := range []storage.NodeID{"imported1", "4:foreign:opaque"} {
								_, err := shard.GetNode(id)
								require.NoError(t, err, "supplied node ID %s", id)
							}
							edge, err := shard.GetEdge("rel:1")
							require.NoError(t, err)
							require.Equal(t, storage.NodeID("imported1"), edge.StartNode)
							require.Equal(t, storage.NodeID("4:foreign:opaque"), edge.EndNode)
						}
					})
				}
			}
		}
	}
}

func requireBatchScopeRejected(t *testing.T,exec *StorageExecutor,shard storage.Engine,explicit bool,err error) {
 t.Helper()
 require.ErrorContains(t,err,"unsupported query type in transaction")
 if explicit { _,err=exec.Execute(context.Background(),"ROLLBACK",nil); require.NoError(t,err) }
 nodes,readErr:=shard.GetNodesByLabel("U5AdmissionMatrix"); require.NoError(t,readErr); require.Empty(t,nodes,"scope rejection must precede independent writes")
}