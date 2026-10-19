package cypher

import (
 "context"
 "encoding/base64"
 "encoding/json"
 "fmt"
 "os"
 "testing"

 "github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
 "github.com/orneryd/nornicdb/pkg/config"
 "github.com/orneryd/nornicdb/pkg/multidb"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/assert"
 "github.com/stretchr/testify/require"
)

func TestFunctionSyntaxGeneratedRoutes(t *testing.T) {
 prior:=config.IsCypherQueryNormalizationEnabled()
 t.Cleanup(func(){config.SetCypherQueryNormalizationEnabled(prior)})
 cases:=functionsyntaxmatrix.Cases()
 for _,normalize:=range []bool{false,true} {for _,explicit:=range []bool{false,true} {for _,route:=range []string{"direct","use","nested-use"} {
  t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s",normalize,explicit,route),func(t *testing.T){
   config.SetCypherQueryNormalizationEnabled(normalize)
   manager,err:=multidb.NewDatabaseManager(storage.NewMemoryEngine(),nil);require.NoError(t,err)
   defer manager.Close()
   require.NoError(t,manager.CreateDatabase("lex_shard"))
   require.NoError(t,manager.CreateCompositeDatabase("lex_cmp",[]multidb.ConstituentRef{{Alias:"sh",DatabaseName:"lex_shard",Type:"local",AccessMode:"read_write"}}))
   shard,err:=manager.GetStorage("lex_shard");require.NoError(t,err)
   composite,err:=manager.GetStorage("lex_cmp");require.NoError(t,err)
   exec:=NewStorageExecutor(shard)
   if route!="direct" {exec=NewStorageExecutor(composite);exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager:manager})}
   ctx:=context.Background()
   for _,sample:=range cases {
    query:=sample.Query
    switch route {case "use":query="USE lex_cmp.sh "+query;case "nested-use":query="CALL { USE lex_cmp.sh "+query+" } RETURN v"}
    if explicit {_,err=exec.Execute(ctx,"BEGIN",nil);require.NoError(t,err)}
    result,queryErr:=exec.Execute(ctx,query,nil)
    if explicit {_,_=exec.Execute(ctx,"ROLLBACK",nil)}
    if assert.NoError(t,queryErr,"%s: %s",sample.ID,query) {
     rows,err:=json.Marshal(result.Rows);require.NoError(t,err)
     assert.JSONEq(t,sample.Rows,string(rows),sample.ID)
    }
   }
  })
 }}}
}

func TestExportFunctionSyntaxMatrixOracle(t *testing.T) {
 if os.Getenv("NORNICDB_EXPORT_FUNCTION_SYNTAX_ORACLE")=="" {t.Skip("explicit oracle export")}
 for _,sample:=range functionsyntaxmatrix.Cases() {
  query:="CALL { "+sample.Query+" } RETURN '"+sample.ID+"' AS id, v;"
  fmt.Println("FUNCTION_SYNTAX_ORACLE "+base64.StdEncoding.EncodeToString([]byte(query)))
  fmt.Printf("FUNCTION_SYNTAX_EXPECTED %s %s\n",sample.ID,sample.Rows)
 }
}
