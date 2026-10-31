package workcensus

// Author: #1667 implementation lane (Codex). Source-derived SSA controls for
// the actual checked-call API, including a retained named-result acquisition.

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestSuccessfulCallEdgeCapturedResult(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"go.mod": "module census.successfixture\n\ngo 1.26\n",
		"fixture.go": `package successfixture
 func acquire()(int,error){return 1,nil}
 func effect(){}
 func held()(err error){defer func(){_ =err}();_,err=acquire();if err!=nil{return};effect();err=nil;return}
 func clobbered()(err error){defer func(){_ =err}();_,err=acquire();err=nil;if err!=nil{return};effect();return}
 func branchClobbered(b bool)(err error){defer func(){_ =err}();_,err=acquire();if b{err=nil};if err!=nil{return};effect();return}
 func unchecked(){acquire();effect()}
 `,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		name string
		want bool
	}{{"held", true}, {"clobbered", false}, {"branchClobbered", false}, {"unchecked", false}} {
		t.Run(probe.name, func(t *testing.T) {
			found := false
			for _, fn := range loaded.Functions {
				if fn.Name() != probe.name {
					continue
				}
				for _, block := range fn.Blocks {
					for _, instruction := range block.Instrs {
						call, ok := instruction.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "acquire" {
							continue
						}
						found = true
						_, success := SuccessfulCallEdge(call, 1)
						if (success != nil) != probe.want {
							t.Fatalf("actual acquisition result checked=%v, want %v", success != nil, probe.want)
						}
					}
				}
			}
			if !found {
				t.Fatal("fixture did not contain the actual acquisition")
			}
		})
	}
}
