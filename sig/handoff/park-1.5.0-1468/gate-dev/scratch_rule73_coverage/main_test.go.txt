package rule73coverage

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRule73CoverageDriver(t *testing.T){
	root,err:=filepath.Abs("..");if err!=nil {t.Fatal(err)}
	profile:=filepath.Join(t.TempDir(),"cover.out")
	cmd:=exec.Command("go","test","./pkg/cypher","-run","^TestRule73PatternCoverageReplay$","-count=1","-covermode=atomic","-coverprofile="+profile,"-timeout=15m","-v")
	cmd.Dir=root;cmd.Env=os.Environ()
	output,err:=cmd.CombinedOutput();t.Logf("R73_COVERAGE_CHILD %s",output);if err!=nil{t.Fatalf("coverage replay: %v",err)}
	cmd=exec.Command("go","tool","cover","-func="+profile);cmd.Dir=root
	output,err=cmd.CombinedOutput();if err!=nil {t.Fatalf("cover funcs: %v %s",err,output)}
	interesting:=regexp.MustCompile(`(?i)(pattern|subquer|correlat|bound.*count|bound.*rows|pipeline.*import|pipelineNodePredicateProperties|carry.*import|relationship.*edge|incident.*edge|evaluateRowExpressionShaped|graphReadingExpression)`)
	for _,line:=range strings.Split(string(output),"\n"){if interesting.MatchString(line){t.Logf("R73_COVER_FUNC %s",line)}}
	// Retain all block counts compactly, in case a function percentage conceals
	// an unreached branch. No production source is changed by instrumentation.
	data,err:=os.ReadFile(profile);if err!=nil {t.Fatal(err)}
	for _,line:=range bytes.Split(data,[]byte{'\n'}){if bytes.Contains(line,[]byte("pattern_predicate.go:")) || bytes.Contains(line,[]byte("pattern_expression.go:")) || bytes.Contains(line,[]byte("subquery_value_expression.go:")) || bytes.Contains(line,[]byte("functions_eval_functions.go:")) {t.Logf("R73_COVER_BLOCK %s",line)}}
}
