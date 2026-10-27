package workcensus

// Author: #1667 implementation lane (Codex). Generic source/call-site facts;
// the fixture has no physical-write names or storage classification policy.

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestSyntheticFunctionSourceSites(t *testing.T) {
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod": "module census.synthetic\n\ngo 1.26\n",
		"source.go": `package synthetic
type target struct{}
func (*target) Action(){}
func invoke(f func()){f()}
func Root(t *target){invoke(t.Action)}
`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	var seeds []*ssa.Function
	for _, fn := range p.Functions {
		if fn.Name() == "Root" || fn.Name() == "invoke" {
			seeds = append(seeds, fn)
		}
	}
	if len(seeds) != 2 {
		t.Fatal("missing source seeds")
	}
	found := false
	for _, fn := range p.IncludeSyntheticFunctions(seeds) {
		if fn.Synthetic == "" || len(fn.FreeVars) == 0 {
			continue
		}
		found = true
		calls := p.SourceCallSites(fn)
		if len(calls) != 1 || calls[0].Parent().Name() != "invoke" || !calls[0].Pos().IsValid() {
			t.Fatalf("nearest real invocation lost: %v", calls)
		}
		targets := p.CallTargets(calls[0])
		if len(targets) != 1 || targets[0] != fn {
			t.Fatalf("wrapper target lost: %v", targets)
		}
		refs := p.SourceReferences(fn)
		if len(refs) != 1 || refs[0].Parent().Name() != "Root" || !refs[0].Pos().IsValid() {
			t.Fatalf("source carrier lost: %v", refs)
		}
		value, ok := refs[0].(ssa.Value)
		if !ok {
			t.Fatal("method-value carrier is not an SSA value")
		}
		if yes, known := MatchesFunctionValue(value, fn); !yes || !known {
			t.Fatal("exact bound target identity lost")
		}
		for _, other := range seeds {
			if yes, known := MatchesFunctionValue(value, other); yes || !known {
				t.Fatal("different function accepted as bound target")
			}
		}
		if _, known := MatchesFunctionValue(calls[0].Common().Value, fn); known {
			t.Fatal("opaque function parameter given a fabricated identity")
		}
	}
	if !found {
		t.Fatal("compiler wrapper omitted from selected body population")
	}
}

func TestSyntheticGlobalInitializerSourceReference(t *testing.T) {
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod":    "module census.syntheticglobal\n\ngo 1.26\n",
		"source.go": "package syntheticglobal\ntype target struct{}\nfunc (*target) Action(){}\nvar t target\nvar Saved=t.Action\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, fn := range p.Functions {
		if fn.Synthetic == "" || len(fn.FreeVars) == 0 {
			continue
		}
		found = true
		refs := p.SourceReferences(fn)
		if len(refs) != 1 || !refs[0].Pos().IsValid() || refs[0].Parent().Name() != "init" {
			t.Fatalf("global initializer source carrier lost: %v", refs)
		}
	}
	if !found {
		t.Fatal("global method-value wrapper missing")
	}
}
