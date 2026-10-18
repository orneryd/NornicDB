package cypher

// Gate-only replay: measures routes reached by the generated public Execute
// population. Semantic evidence comes from the separate live-oracle jobs.
import (
	"compress/gzip"
	
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRule73PatternCoverageReplay(t *testing.T) {
	phases:=[]string{"BASE","PROJECTION","PARAMS","TX_STATE"}
	if phase:=os.Getenv("R73_COVERAGE_PHASE");phase!="" {phases=[]string{phase}}
	for _,phase:=range phases {
		path:=filepath.Join(t.TempDir(),"matrix.json.gz")
		cmd:=exec.Command("python3","../../scripts/cypher-tck/differential/rule73_pattern_matrix.py",path)
		cmd.Env=append(os.Environ(),"R73_PROJECTION=","R73_PARAMS=","R73_TX_STATE=","R73_CODEPATHS=")
		if phase!="BASE" {cmd.Env=append(cmd.Env,"R73_"+phase+"=1")}
		if output,err:=cmd.CombinedOutput();err!=nil {t.Fatalf("generate: %v %s",err,output)}
		file,err:=os.Open(path);if err!=nil {t.Fatal(err)}
		reader,err:=gzip.NewReader(file);if err!=nil {t.Fatal(err)}
		var corpus struct {Setup []string;Cases []struct {Query string;Statements []string;Database string}}
		err=json.NewDecoder(reader).Decode(&corpus);reader.Close();file.Close();if err!=nil {t.Fatal(err)}
		failures:=0
		for _,item:=range corpus.Cases {
			e,ctx:=newUnitExecutor(t)
			if item.Database=="system" {e,_=nestedUseRouterExecutor(t,"system")} else {
				for _,q:=range corpus.Setup {if _,err:=e.Execute(ctx,q,nil);err!=nil {t.Fatal(err)}}
			}
			queries:=item.Statements;if len(queries)==0 {queries=[]string{item.Query}}
			var parameters map[string]interface{}
			if phase=="PARAMS" {parameters=map[string]interface{}{"target":int64(2),"w":int64(1),"null":nil,"lab":"Q"}}
			for _,q:=range queries {if _,err:=e.Execute(ctx,q,parameters);err!=nil {failures++}}
		}
		t.Logf("R73_COVERAGE_REPLAY phase=%s cases=%d errors=%d (errors are recorded, oracle outcomes assessed separately)",phase,len(corpus.Cases),failures)
	}
}

