package workcensus

import (
	"go/ast"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestGenericCensusPackageScopeMatchesTypeInfo(t *testing.T) {
	for _, sourceOnly := range []bool{false, true} {
		c := declarationIdentityConfig(t, true, "package sample\ntype Thing struct{}\nvar Ordinary Thing\n")
		if err := os.WriteFile(filepath.Join(c.Dir, "source_test.go"), []byte("package sample\nvar TestOnly Thing\n"), 0644); err != nil {
			t.Fatal(err)
		}
		c.Tests, c.IncludeSyntax, c.SourceOnly = true, true, sourceOnly
		c.Packages = []string{"sample"}
		c.Selectors = []Selector{{Kind: "identifier", Node: "identifier", Reachability: "all"}}
		actual := mustExtract(t, c)
		seen := map[string]bool{}
		for _, member := range actual.Members {
			if member.Syntax == nil || member.Syntax.Info == nil || member.Syntax.Package == nil {
				t.Fatal("typed source lost package", member.ID)
			}
			id, ok := member.Syntax.Node.(*ast.Ident)
			if !ok {
				continue
			}
			obj := member.Syntax.Info.Defs[id]
			if obj == nil || obj.Pkg() == nil {
				continue
			}
			if obj.Pkg() != member.Syntax.Package {
				t.Fatalf("sourceOnly=%v %s: package scope and type facts came from different build variants", sourceOnly, member.ID)
			}
			if id.Name == "Ordinary" && member.File == "source.go" {
				seen["ordinary"] = true
				if member.Syntax.Package.Scope().Lookup("TestOnly") != nil {
					t.Fatal("ordinary source borrowed test variant scope")
				}
			}
			if id.Name == "TestOnly" && member.File == "source_test.go" {
				seen["test"] = true
			}
		}
		if len(seen) != 2 {
			t.Fatal("missing ordinary or test source", seen)
		}
	}
}

const declarationIdentitySource = `package sample
type A interface { A() }
type B interface { B() }
type impl struct{}
func (*impl) A() {}
func (*impl) B() {}
var _ A = (*impl)(nil)
var _ B = (*impl)(nil)
func init() { for range 1 {} }
func init() { for range 2 {} }
func _() { for range 3 {} }
func _() { for range 4 {} }
var _ = func() { for range 5 {} }
var _ = func() { for range 6 {} }
`

func declarationIdentityConfig(t *testing.T, typed bool, source string) Config {
	t.Helper()
	dir := t.TempDir()
	for name, raw := range map[string]string{"go.mod": "module sample\n\ngo 1.26\n", "source.go": source} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
	}
	c := Config{Dir: dir, SourceRoots: []SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}}}, IncludeInactive: true, Selectors: []Selector{
		{Kind: "bound-plan-candidate", Node: "declaration", Reachability: "all"},
		{Kind: "loop", Node: "range", Reachability: "all"},
	}}
	if typed {
		c.Patterns = []string{"./..."}
	}
	return c
}

func TestGenericCensusUniqueDeclarationIdentity(t *testing.T) {
	for _, typed := range []bool{false, true} {
		t.Run(map[bool]string{false: "syntax", true: "typed"}[typed], func(t *testing.T) {
			c := declarationIdentityConfig(t, typed, declarationIdentitySource)
			actual := mustExtract(t, c)
			approval := reviewed(actual)
			if failures := Validate(actual, approval, fixturePolicy); len(failures) > 0 {
				t.Fatal(failures)
			}
			ids := func(i Inventory) []string {
				var out []string
				for _, m := range i.Members {
					out = append(out, m.ID)
				}
				sort.Strings(out)
				return out
			}
			path := filepath.Join(c.Dir, "source.go")
			c.Overlay = map[string][]byte{path: []byte(strings.ReplaceAll(declarationIdentitySource, "var _", "\n// moved declaration\nvar _"))}
			moved := mustExtract(t, c)
			namedIDs := func(inv Inventory) []string {
				var out []string
				for _, m := range inv.Members {
					if !strings.Contains(m.Owner, "@source:") {
						out = append(out, m.ID)
					}
				}
				sort.Strings(out)
				return out
			}
			if strings.Join(namedIDs(actual), "\n") != strings.Join(namedIDs(moved), "\n") {
				t.Fatal("comment/line movement changed named declaration identity")
			}
			// The IDs retain declaration positions; swapping the two assertion
			// declarations must change those exact rows rather than collapse them.
			swapped := strings.Replace(declarationIdentitySource, "var _ A = (*impl)(nil)\nvar _ B = (*impl)(nil)", "var _ B = (*impl)(nil)\nvar _ A = (*impl)(nil)", 1)
			c.Overlay[path] = []byte(swapped)
			mutation := mustExtract(t, c)
			if strings.Join(ids(actual), "\n") != strings.Join(ids(mutation), "\n") {
				t.Fatal("swap changed lexical identities")
			}
			failures := Validate(mutation, approval, fixturePolicy)
			assertions := 0
			for _, member := range actual.Members {
				if member.Code == "_ A = (*impl)(nil)" || member.Code == "_ B = (*impl)(nil)" {
					assertions++
					if !contains(failures, "CHANGED "+member.ID) {
						t.Fatal("swapped assertion escaped ordinary validation", member.ID, failures)
					}
				}
			}
			if assertions != 2 {
				t.Fatal("swap control did not select both assertion declarations", assertions)
			}
		})
	}
}

func TestGenericCensusLocalDeclarationKind(t *testing.T) {
	for _, typed := range []bool{false, true} {
		c := declarationIdentityConfig(t, typed, "package sample\nfunc Last(values []interface{}) interface{} { var last interface{}; const empty = 0; _ = empty; for _, value := range values { if value != nil { last = value } }; return last }\n")
		// Typed extraction also covers an inactive source document through the
		// same owner traversal, without fabricated type information.
		if err := os.WriteFile(filepath.Join(c.Dir, "inactive.go"), []byte("//go:build census_identity_inactive\n\npackage sample\nfunc Unused(){var value int;const count=1;_,_=value,count}\n"), 0644); err != nil {
			t.Fatal(err)
		}
		c.Selectors = append(c.Selectors, Selector{Kind: "functions", Node: "function", Reachability: "all"})
		actual := mustExtract(t, c)
		seen := map[string]bool{}
		for _, m := range actual.Members {
			kind := ""
			for fragment, wanted := range map[string]string{"last interface{}": "var", "empty = 0": "const", "value int": "var", "count = 1": "const"} {
				if m.Code == fragment {
					kind = wanted
					seen[fragment] = true
				}
			}
			if kind != "" && (m.Node != kind || m.Declaration != kind || m.Kind == "functions") {
				t.Fatal("local declaration inherited function kind", m)
			}
		}
		if len(seen) != 4 {
			t.Fatal("declaration population incomplete", seen)
		}
		if failures := Validate(actual, reviewed(actual), fixturePolicy); len(failures) > 0 {
			t.Fatal(failures)
		}
	}
}

func TestGenericCensusSyntaxOnlyRepeatedOwners(t *testing.T) {
	c := declarationIdentityConfig(t, false, `package sample
import first "fmt"
import second "fmt"
type Repeated int
type Repeated string
type _ int
type _ string
var repeated int
var repeated string
const _ = 1
const _ = 2
func RepeatedFunction() {}
func RepeatedFunction() {}
`)
	actual := mustExtract(t, c)
	if failures := Validate(actual, reviewed(actual), fixturePolicy); len(failures) > 0 {
		t.Fatal(failures)
	}
}

func TestGenericCensusEmptyPackageTypeScope(t *testing.T) {
	c := declarationIdentityConfig(t, true, "package sample\n")
	if err := os.MkdirAll(filepath.Join(c.Dir, "stub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.Dir, "stub", "stub.go"), []byte("package stub\n"), 0644); err != nil {
		t.Fatal(err)
	}
	c.IncludeSyntax = true
	c.Selectors = []Selector{{Kind: "file", Node: "file", Reachability: "all"}}
	actual := mustExtract(t, c)
	for _, member := range actual.Members {
		if member.File == "stub/stub.go" {
			if member.Syntax == nil || member.Syntax.Package == nil || member.Syntax.Package.Name() != "stub" || member.Syntax.Package.Scope().Len() != 0 {
				t.Fatal("empty compiled package lost its actual type scope", member)
			}
			return
		}
	}
	t.Fatal("empty package source not selected")
}
