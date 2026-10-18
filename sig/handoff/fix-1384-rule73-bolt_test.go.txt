package tck

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

func TestRule73PatternFamilyAttack(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" { t.Fatal("live pinned Neo4j is required") }
	output := filepath.Join(t.TempDir(), "matrix.json.gz")
	cmd := exec.Command("python3", "../../../scripts/cypher-tck/differential/rule73_pattern_matrix.py", output)
	if data, err := cmd.CombinedOutput(); err != nil { t.Fatalf("generate: %v %s",err,data) } else { t.Logf("%s",data) }
	corpus, err := differential.LoadSweep(output)
	if err != nil { t.Fatal(err) }
	driver, err := neo4j.NewDriverWithContext(uri,neo4j.NoAuth())
	if err != nil { t.Fatal(err) }
	defer driver.Close(context.Background())
	ctx,cancel := context.WithTimeout(context.Background(),25*time.Minute)
	defer cancel()
	if err:=waitForReference(ctx,driver);err!=nil {t.Fatal(err)}
	session:=driver.NewSession(ctx,neo4j.SessionConfig{DatabaseName:"neo4j"})
	version,err:=session.Run(ctx,"CALL dbms.components() YIELD versions RETURN versions[0] AS version",nil)
	if err!=nil {t.Fatal(err)}
	records,err:=version.Collect(ctx)
	if err!=nil || len(records)!=1 || records[0].Values[0]!="5.26.30" {t.Fatalf("wrong oracle: %v %v",records,err)}
	session.Close(ctx)
	t.Log("R73_ORACLE Neo4j 5.26.30")
	for _,route:=range []struct{name string; mode TransactionMode}{{"bolt-auto",AutocommitMode},{"bolt-tx",ExplicitTransactionMode}} {
		t.Run(route.name,func(t *testing.T) {
			nornic,closeServer:=startConformanceServer(t)
			defer closeServer()
			pair:= &differential.Pair{Neo4j:newBoltDifferentialExecutor(t,driver,"neo4j",route.mode),NornicDB:newBoltDifferentialExecutor(t,nornic,"nornic",route.mode)}
			if os.Getenv("R73_PARAMS")=="1" {
				pair.Neo4j=rule73ParameterExecutor{newBoltDifferentialExecutor(t,driver,"neo4j",route.mode)}
				pair.NornicDB=rule73ParameterExecutor{newBoltDifferentialExecutor(t,nornic,"nornic",route.mode)}
			}
			options:=differential.DefaultOptions
			options.Workers=1
			results,err:=differential.RunSweep(ctx,corpus,pair,options)
			if err!=nil {t.Fatal(err)}
			reportRule73Attack(t,route.name,corpus,results)
		})
	}
}

type rule73ParameterExecutor struct { boltDifferentialExecutor }

func(e rule73ParameterExecutor) Execute(ctx context.Context,query string)(outcome differential.Outcome){
	params:=map[string]interface{}{"target":int64(2),"w":int64(1),"null":nil,"lab":"Q"}
	s:=e.driver.NewSession(ctx,neo4j.SessionConfig{DatabaseName:e.database})
	defer s.Close(ctx)
	collect:=func(r neo4j.ResultWithContext)error{
		keys,err:=r.Keys();if err!=nil{return err}
		records,err:=r.Collect(ctx);if err!=nil{return err}
		outcome.Columns=keys;outcome.Rows=make([][]interface{},len(records))
		for i,record:=range records{row:=make([]interface{},len(record.Values));for j,v:=range record.Values{row[j]=differentialOutcomeValue(v)};outcome.Rows[i]=row}
		return nil
	}
	var err error
	if e.mode==ExplicitTransactionMode{
		var tx neo4j.ExplicitTransaction
		if tx,err=s.BeginTransaction(ctx);err==nil{var r neo4j.ResultWithContext;if r,err=tx.Run(ctx,query,params);err==nil{err=collect(r)};if err==nil{err=tx.Commit(ctx)}else{tx.Rollback(ctx)}}
	}else{var r neo4j.ResultWithContext;if r,err=s.Run(ctx,query,params);err==nil{err=collect(r)}}
	if err!=nil{var failure *neo4j.Neo4jError;if errors.As(err,&failure){return differential.Outcome{Code:failure.Code,Message:failure.Msg}};return differential.Outcome{Code:"client:"+err.Error()}}
	return outcome
}

func reportRule73Attack(t *testing.T,route string,corpus differential.SweepCorpus,results []differential.Result) {
	index:=make(map[string]differential.SweepCase,len(corpus.Cases))
	for _,c:=range corpus.Cases {index[c.ID]=c}
	cells:=map[string][2]int{}
	differences:=0
	for _,result:=range results {
		c:=index[strings.TrimSuffix(result.ID,":graph")]
		key:=c.Family+"/"+c.Position
		cell:=cells[key]
		if result.Match {cell[0]++} else {cell[1]++;differences++;data,_:=json.Marshal(result);t.Logf("R73_DIFF %s %s",route,data)}
		cells[key]=cell
	}
	keys:=make([]string,0,len(cells))
	for key:=range cells {keys=append(keys,key)}
	sort.Strings(keys)
	for _,key:=range keys {c:=cells[key];t.Logf("R73_CELL %s %s match=%d diff=%d",route,key,c[0],c[1])}
	t.Logf("R73_TOTAL %s compared=%d match=%d diff=%d",route,len(results),len(results)-differences,differences)
	if differences>0 {t.Errorf("%d current-head differences from Neo4j; attack evidence, no product changes",differences)}
}
