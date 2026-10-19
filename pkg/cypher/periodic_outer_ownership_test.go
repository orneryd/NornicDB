package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeriodicCallerSeesBatchesAndKeepsOuterRollback(t *testing.T) {
	for _, periodic := range []string{"iterate", "commit"} {
		for _, dynamic := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, failure := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/dynamic=%v/explicit=%v/failure=%v", periodic, dynamic, explicit, failure), func(t *testing.T) {
						exec, store := newProcedureAdmissionExecutor(t, false)
						ctx := context.Background()
						_, err := exec.Execute(ctx, "UNWIND [1,2,3] AS x CREATE (:U5PeriodicOuterSeed)", nil); require.NoError(t, err)
						cleanupAdmissionProcedure(t, "u5periodic.observe"); cleanupAdmissionProcedure(t, "u5periodic.abort")
						var observed [][]interface{}
						require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name:"u5periodic.observe", Mode:ProcedureModeRead, Returns:[]ProcedureColumn{{Name:"seen",Type:"INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult,error) {
							result, err := caller.Execute(ctx, "MATCH (n:U5PeriodicOuter) RETURN count(n) AS seen", nil)
							if err != nil { return nil,err }; observed = append(observed,result.Rows...); return result,nil
						}))
						require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name:"u5periodic.abort", Mode:ProcedureModeRead}, func(context.Context,*StorageExecutor,string,[]interface{})(*ExecuteResult,error){return nil,errors.New("deliberate outer failure")}))
						call := "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:U5PeriodicOuterBatch {value:$x})',{batchSize:1}) YIELD total"
						label, wantBatch := "U5PeriodicOuterBatch", 3
						if periodic == "commit" { call = "CALL apoc.periodic.commit('MATCH (n:U5PeriodicOuterSeed) WITH n LIMIT 1 DELETE n RETURN count(n)',{}) YIELD executions"; label, wantBatch = "U5PeriodicOuterSeed", 0 }
						body := "CREATE (:U5PeriodicOuter {stage:'before'}) WITH 1 AS carry " + call +
							" WITH 1 AS carry CALL u5periodic.observe() YIELD seen WITH seen CREATE (:U5PeriodicOuter {stage:'after'}) WITH seen MATCH (b:"+label+") WITH seen,count(b) AS batches CALL u5periodic.observe() YIELD seen AS later RETURN batches,later"
						if failure { body = "CALL { " + body + " } CALL u5periodic.abort() RETURN batches,later" }
						query, params := body, map[string]interface{}{}
						if dynamic { query = "CALL apoc.cypher.run($body,{}) YIELD value RETURN value"; params["body"] = body }
						if explicit { _,err=exec.Execute(ctx,"BEGIN",nil); require.NoError(t,err) }
						result, err := exec.Execute(ctx,query,params)
						if failure { require.ErrorContains(t,err,"deliberate outer failure") } else {
							require.NoError(t,err); require.Len(t,result.Rows,1)
							if !dynamic { require.Equal(t,[][]interface{}{{int64(wantBatch),int64(2)}},result.Rows) }
						}
						require.Equal(t,[][]interface{}{{int64(1)},{int64(2)}},observed,"pending outer writes survive a batch's fresh view")
						if explicit { if failure { _,err=exec.Execute(ctx,"ROLLBACK",nil) } else { _,err=exec.Execute(ctx,"COMMIT",nil) }; require.NoError(t,err) }
						outer,err:=store.GetNodesByLabel("U5PeriodicOuter"); require.NoError(t,err); wantOuter:=2; if failure {wantOuter=0}; require.Len(t,outer,wantOuter)
						batch,err:=store.GetNodesByLabel(label); require.NoError(t,err)
						if explicit && failure { if periodic=="iterate" {wantBatch=0} else {wantBatch=3} }; require.Len(t,batch,wantBatch)
					})
				}
			}
		}
	}
}
