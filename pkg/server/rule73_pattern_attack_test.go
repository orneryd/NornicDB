package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

func TestRule73PatternFamilyHTTPAttack(t *testing.T) {
	reference:=os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if reference=="" {t.Fatal("live pinned Neo4j is required")}
	output:=filepath.Join(t.TempDir(),"matrix.json.gz")
	cmd:=exec.Command("python3","../../scripts/cypher-tck/differential/rule73_pattern_matrix.py",output)
	if data,err:=cmd.CombinedOutput();err!=nil {t.Fatalf("generate: %v %s",err,data)} else {t.Logf("%s",data)}
	corpus,err:=differential.LoadSweep(output)
	if err!=nil {t.Fatal(err)}
	server,authenticator:=setupTestServer(t)
	local:=httptest.NewServer(server.buildRouter())
	defer local.Close()
	client:=&http.Client{Timeout:differential.DefaultOptions.StatementTimeout}
	if os.Getenv("R73_PARAMS")=="1"{client.Transport=rule73ParameterTransport{}}
	pair:=&differential.Pair{
		Neo4j:httpDifferentialExecutor{client:client,endpoint:reference+"/db/neo4j/tx/commit"},
		NornicDB:httpDifferentialExecutor{client:client,endpoint:local.URL+"/db/nornic/tx/commit",authorization:"Bearer "+getAuthToken(t,authenticator,"admin")},
	}
	options:=differential.DefaultOptions
	options.Workers=1
	ctx,cancel:=context.WithTimeout(context.Background(),25*time.Minute)
	defer cancel()
	results,err:=differential.RunSweep(ctx,corpus,pair,options)
	if err!=nil {t.Fatal(err)}
	index:=make(map[string]differential.SweepCase,len(corpus.Cases))
	for _,c:=range corpus.Cases {index[c.ID]=c}
	cells:=map[string][2]int{}
	differences:=0
	for _,result:=range results {
		c:=index[strings.TrimSuffix(result.ID,":graph")]
		key:=c.Family+"/"+c.Position
		cell:=cells[key]
		if result.Match {cell[0]++} else {cell[1]++;differences++;data,_:=json.Marshal(result);t.Logf("R73_DIFF http %s",data)}
		cells[key]=cell
	}
	keys:=make([]string,0,len(cells))
	for key:=range cells {keys=append(keys,key)}
	sort.Strings(keys)
	for _,key:=range keys {c:=cells[key];t.Logf("R73_CELL http %s match=%d diff=%d",key,c[0],c[1])}
	t.Logf("R73_TOTAL http compared=%d match=%d diff=%d",len(results),len(results)-differences,differences)
	if differences>0 {t.Errorf("%d current-head differences from Neo4j; attack evidence, no product changes",differences)}
}

type rule73ParameterTransport struct{}
func(rule73ParameterTransport) RoundTrip(request *http.Request)(*http.Response,error){
	data,err:=io.ReadAll(request.Body);if err!=nil{return nil,err};request.Body.Close()
	var body map[string]interface{}
	if err=json.Unmarshal(data,&body);err!=nil{return nil,err}
	for _,raw:=range body["statements"].([]interface{}){raw.(map[string]interface{})["parameters"]=map[string]interface{}{"target":int64(2),"w":int64(1),"null":nil,"lab":"Q"}}
	data,err=json.Marshal(body);if err!=nil{return nil,err}
	clone:=request.Clone(request.Context());clone.Body=io.NopCloser(bytes.NewReader(data));clone.ContentLength=int64(len(data))
	return http.DefaultTransport.RoundTrip(clone)
}
