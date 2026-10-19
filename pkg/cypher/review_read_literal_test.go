package cypher

import (
 "context"
 "fmt"
 "testing"

 "github.com/orneryd/nornicdb/pkg/multidb"
 "github.com/stretchr/testify/require"
)

func TestReviewR73ReadLiteralData(t *testing.T) {
 for _,body:=range []string{"RETURN 'hello' AS value","RETURN 'CREATE' AS value"} {for _,explicit:=range []bool{false,true} {
  t.Run(fmt.Sprintf("body=%s/explicit=%v",body,explicit),func(t *testing.T){
   manager,err:=multidb.NewDatabaseManager(newTestMemoryEngine(t),nil);require.NoError(t,err);t.Cleanup(func(){_=manager.Close()})
   require.NoError(t,manager.CreateDatabase("r73one"));require.NoError(t,manager.CreateDatabase("r73two"))
   require.NoError(t,manager.CreateCompositeDatabase("r73comp",[]multidb.ConstituentRef{
    {Alias:"one",DatabaseName:"r73one",Type:"local",AccessMode:"read_write"},
    {Alias:"two",DatabaseName:"r73two",Type:"local",AccessMode:"read_write"},
   }))
   store,err:=manager.GetStorage("r73comp");require.NoError(t,err)
   e:=NewStorageExecutor(store);e.SetDatabaseManager(&testDatabaseManagerAdapter{manager:manager})
   ctx:=context.Background()
   if explicit {_,err=e.Execute(ctx,"BEGIN",nil);require.NoError(t,err)}
   query:="CALL { USE r73comp.one CALL apoc.cypher.run($body,{}) YIELD value RETURN value } RETURN value UNION ALL CALL { USE r73comp.two CALL apoc.cypher.run($body,{}) YIELD value RETURN value } RETURN value"
   result,err:=e.Execute(ctx,query,map[string]interface{}{"body":body})
   t.Logf("pure read dynamic body result=%+v err=%v",result,err)
   require.NoError(t,err,"a string value does not make RETURN a writer")
   require.Len(t,result.Rows,2)
   if explicit {_,err=e.Execute(ctx,"COMMIT",nil);require.NoError(t,err)}
  })
 }}
}
