package workcensus

// Author: #1667 implementation lane (Codex). A referenced method value remains
// part of the compiled root population even without a visible invocation.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReachabilityRetainsStoredSyntheticValues(t *testing.T) {
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod":    "module census.syntheticreach\n\ngo 1.26\n",
		"source.go": "package syntheticreach\ntype target struct{}\nfunc (*target) Action(){}\nvar saved func()\nfunc Root(t *target){saved=t.Action}\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.ReachableFrom([]Root{{Package: "census.syntheticreach", Name: "Root", Required: true}})
	if err != nil {
		t.Fatal(err)
	}
	for fn := range r.Functions {
		if fn.Synthetic != "" && len(fn.FreeVars) == 1 && fn.Object() != nil && fn.Object().Name() == "Action" {
			return
		}
	}
	t.Fatal("a stored bound-method wrapper was dropped from the compiled root population")
}
