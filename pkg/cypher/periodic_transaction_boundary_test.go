package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Observe committed storage from inside the next public procedure action.
// Reading the caller's pending wrapper would conceal an outer-transaction bug.
func TestPeriodicProceduresPreserveBatchBoundaries(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	for _, contract := range ListRegisteredProcedures() {
		if !contract.OwnsTransactions {
			continue
		}
		name := contract.Name
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, failure := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/fabric=%v/explicit=%v/failure=%v", name, composite, explicit, failure), func(t *testing.T) {
						exec, committed := newProcedureAdmissionExecutor(t, composite)
						cleanupAdmissionProcedure(t, "u5batch.step")
						calls, visible := 0, -1
						require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "u5batch.step", Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
							calls++
							if calls == 1 {
								return caller.Execute(ctx, "CREATE (:U5BatchBoundary) RETURN 1 AS v", nil)
							}
							nodes, err := committed.GetNodesByLabel("U5BatchBoundary")
							if err != nil {
								return nil, err
							}
							visible = len(nodes)
							if failure {
								return nil, errors.New("u5 deliberate later batch failure")
							}
							return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(0)}}, Stats: &QueryStats{}}, nil
						}))
						body := "CALL u5batch.step() YIELD v RETURN * LIMIT 1"
						call := "CALL " + name + "('UNWIND [1,2] AS input RETURN input',$body,{batchSize:1}) YIELD total RETURN total"
						if name == "apoc.periodic.commit" {
							call = "CALL " + name + "($body,{}) YIELD executions RETURN executions"
						}
						if composite {
							call = "CALL { USE admission_comp.one " + call + " } RETURN *"
						}
						if explicit {
							_, err := exec.Execute(context.Background(), "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := exec.Execute(context.Background(), call, map[string]interface{}{"body": body})
						if composite || explicit {
 require.ErrorContains(t,err,"unsupported query type in transaction")
 require.Zero(t,calls,"unsupported managed scope must fail before any batch action")
 if explicit { _,err=exec.Execute(context.Background(),"ROLLBACK",nil); require.NoError(t,err) }
 nodes,err:=committed.GetNodesByLabel("U5BatchBoundary"); require.NoError(t,err); require.Empty(t,nodes)
 return
}
      require.Equal(t, 1, visible, "completed batch must be committed before next action; result=%v error=%v", result, err)
						require.NoError(t, err)
						if explicit {
							_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
							require.NoError(t, err)
						}
						nodes, err := committed.GetNodesByLabel("U5BatchBoundary")
						require.NoError(t, err)
						require.Len(t, nodes, 1, "outer rollback must not undo completed periodic batches")
					})
				}
			}
		}
	}
}

func TestPeriodicBatchRollbackIsAtomic(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	for _, contract := range ListRegisteredProcedures() {
		if !contract.OwnsTransactions || contract.Name == "apoc.periodic.commit" {
			continue
		}
		for _, composite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fabric=%v", contract.Name, composite), func(t *testing.T) {
				exec, committed := newProcedureAdmissionExecutor(t, composite)
				cleanupAdmissionProcedure(t, "u5batch.atomic")
				calls, visible := 0, -1
				require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "u5batch.atomic", Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
					calls++
					if calls == 3 {
						nodes, err := committed.GetNodesByLabel("U5BatchAtomic")
						if err != nil {
							return nil, err
						}
						visible = len(nodes)
					}
					if calls == 4 {
						return nil, errors.New("u5 atomic batch action failure")
					}
					return caller.Execute(ctx, "CREATE (:U5BatchAtomic) RETURN 1 AS v", nil)
				}))
				query := "CALL " + contract.Name + "('UNWIND [1,2,3,4] AS input RETURN input','CALL u5batch.atomic() YIELD v RETURN v',{batchSize:2}) YIELD committedOperations,failedOperations,failedBatches RETURN committedOperations,failedOperations,failedBatches"
				if composite {
					query = "CALL { USE admission_comp.one " + query + " } RETURN *"
				}
				result, err := exec.Execute(context.Background(), query, nil)
				if composite {
 require.ErrorContains(t,err,"unsupported query type in transaction"); require.Zero(t,calls)
 nodes,readErr:=committed.GetNodesByLabel("U5BatchAtomic"); require.NoError(t,readErr); require.Empty(t,nodes); return
 }
				require.NoError(t, err)
				require.Equal(t, 2, visible, "first two-row batch committed before next begins")
				nodes, err := committed.GetNodesByLabel("U5BatchAtomic")
				require.NoError(t, err)
				require.Len(t, nodes, 2, "failed batch rolls back its earlier action, retaining completed batch")
				require.Len(t,result.Rows,1)
    observed:=make(map[string]interface{},len(result.Columns))
    for index,column:=range result.Columns { observed[column]=result.Rows[0][index] }
    require.Equal(t,map[string]interface{}{"committedOperations":int64(2),"failedOperations":int64(2),"failedBatches":int64(1)},observed)
			})
		}
	}
}
