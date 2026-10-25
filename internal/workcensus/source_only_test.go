package workcensus

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenericCensusSourceOnly(t *testing.T) {
	c := declarationIdentityConfig(t, true, `package sample
import text "strings"
type capability interface { Read(string) string }
type implementation struct{}
func (implementation) Read(s string) string { return s }
var importedAlias = text.TrimSpace
func factory() func(string) string { return importedAlias }
func Unfamiliar(input string, receiver capability) string {
 local := importedAlias
 value := local(input)
 unknown := factory()
 value = unknown(value)
 value = receiver.Read(value)
 return text.TrimSpace(value) + string(input)
}
`)
	c.SourceOnly, c.IncludeSyntax = true, true
	c.Selectors = []Selector{
		{Kind: "declaration", Node: "declaration", Reachability: "all"},
		{Kind: "identifier", Node: "identifier", Reachability: "all"},
		{Kind: "call", Node: "call", Reachability: "all", Packages: []string{"strings"}, Names: []string{"TrimSpace"}},
	}
	inv := mustExtract(t, c)
	if inv.Program != nil || len(inv.Roots) != 0 || !strings.Contains(strings.Join(inv.Limitations, " "), "Source-only mode omits SSA/CHA/VTA") {
		t.Fatal("source-only extraction built or claimed reachability", inv.Roots, inv.Limitations)
	}
	seen := map[string]Member{}
	var value types.Object
	uses := 0
	for _, m := range inv.Members {
		if !m.Typed || m.Syntax == nil || m.Syntax.Info == nil || m.Syntax.Package == nil || m.Reachable {
			t.Fatal("missing typed source facts", m.ID)
		}
		if m.Kind == "call" {
			seen[m.Code] = m
		}
		if id, ok := m.Syntax.Node.(*ast.Ident); ok && id.Name == "value" {
			if obj := m.Syntax.Info.Defs[id]; obj != nil {
				value = obj
			} else if m.Syntax.Info.Uses[id] != nil {
				uses++
			}
		}
	}
	if value == nil || uses < 3 {
		t.Fatal("def/use lost", value, uses)
	}
	for _, code := range []string{"local(input)", "unknown(value)", "receiver.Read(value)", "text.TrimSpace(value)"} {
		m, ok := seen[code]
		if !ok {
			t.Fatal("source-only lost call candidate", code, seen)
		}
		if code != "text.TrimSpace(value)" && !m.UnresolvedTargets {
			t.Fatal("uncertain target claimed exact", m)
		}
	}
	if !contains(seen["local(input)"].Callees, "strings.TrimSpace") || seen["text.TrimSpace(value)"].UnresolvedTargets {
		t.Fatal("direct/assignment alias facts lost", seen)
	}
	if _, exists := seen["string(input)"]; exists {
		t.Fatal("conversion fabricated as unknown function target")
	}
	approval := reviewed(inv)
	p := fixturePolicy
	p.RequireSources = true
	if failures := Validate(inv, approval, p); len(failures) != 0 {
		t.Fatal(failures)
	}
	// The optimized mode uses the real extraction/validation path for a new
	// package as well; it must not silently narrow the reviewed package set.
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "plugins", "newclient", "unfamiliar.go"): []byte("package newclient\nimport \"strings\"\nfunc Added(s string) string { return strings.TrimSpace(s) }\n")}
	mutated := mustExtract(t, c)
	failures := strings.Join(Validate(mutated, approval, p), "\n")
	if !strings.Contains(failures, "UNLISTED plugins/newclient/unfamiliar.go:") || !strings.Contains(failures, "UNLISTED SOURCE plugins/newclient/unfamiliar.go") {
		t.Fatal("new package escaped shared gate", failures)
	}
}

func TestGenericCensusSourceOnlyRefusals(t *testing.T) {
	for _, scenario := range []string{"roots", "program", "reachable", "invalid source", "load program"} {
		t.Run(scenario, func(t *testing.T) {
			c := declarationIdentityConfig(t, true, "package sample\nfunc Existing() {}\n")
			c.SourceOnly = true
			switch scenario {
			case "roots":
				c.Roots = []Root{{Package: "sample", Name: "Existing"}}
			case "program":
				c.IncludeProgram = true
			case "reachable":
				c.Selectors[0].Reachability = ""
			case "invalid source":
				c.Overlay = map[string][]byte{filepath.Join(c.Dir, "source.go"): []byte("package sample\nvar value int = \"wrong\"\n")}
			case "load program":
				if _, err := LoadProgram(c); err == nil {
					t.Fatal("source-only returned an SSA program")
				}
				return
			}
			if _, err := Extract(c); err == nil {
				t.Fatal("invalid source-only request accepted")
			}
		})
	}
}
