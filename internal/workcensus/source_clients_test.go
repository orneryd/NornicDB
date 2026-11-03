package workcensus

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceClientSyntaxAndDefUse(t *testing.T) {
	c := genericConfig(t)
	c.IncludeSyntax = true
	c.Selectors = nil
	for _, node := range []string{"declaration", "assign", "identifier", "call", "binary"} {
		c.Selectors = append(c.Selectors, Selector{Kind: node, Node: node, Reachability: "all"})
	}
	path := filepath.Join(c.Dir, "source.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{path: append(raw, []byte(`
type capability interface { Own() }
type implementation struct{}
func (*implementation) Own() {}
type aliasImplementation = implementation
func domainProbe(s string) bool {
 trim := text.TrimSpace
 value := trim(s)
 return value == "" || "" == value || len(value) == 0
}
`)...)}
	inv := mustExtract(t, c)
	var definition types.Object
	uses := 0
	trimAlias := false
	methodSet := false
	for _, m := range inv.Members {
		if m.Syntax == nil || m.Syntax.Info == nil || m.Syntax.Package == nil || len(m.Syntax.Source) == 0 || m.EndOffset <= m.Offset || m.EndLine < m.Line {
			t.Fatal("missing shared typed/source view", m.ID)
		}
		if id, ok := m.Syntax.Node.(*ast.Ident); ok && id.Name == "value" && strings.Contains(m.Owner, "domainProbe") {
			if obj := m.Syntax.Info.Defs[id]; obj != nil {
				definition = obj
			} else if m.Syntax.Info.Uses[id] != nil {
				uses++
			}
		}
		if m.Node == "call" && strings.HasPrefix(m.Code, "trim(") {
			for _, callee := range m.Callees {
				trimAlias = trimAlias || callee == "strings.TrimSpace"
			}
		}
		if decl, ok := m.Syntax.Node.(*ast.TypeSpec); ok && decl.Name.Name == "aliasImplementation" {
			obj := m.Syntax.Info.Defs[decl.Name]
			scope := obj.Pkg().Scope()
			iface := scope.Lookup("capability").Type().Underlying().(*types.Interface)
			methodSet = types.Implements(types.NewPointer(types.Unalias(obj.Type())), iface)
		}
	}
	if definition == nil || uses != 3 || !trimAlias || !methodSet {
		t.Fatal("domain facts unavailable", definition, uses, trimAlias, methodSet)
	}
	for _, m := range inv.Members {
		if id, ok := m.Syntax.Node.(*ast.Ident); ok && id.Name == "value" && strings.Contains(m.Owner, "domainProbe") && m.Syntax.Info.Uses[id] != nil && m.Syntax.Info.Uses[id] != definition {
			t.Fatal("def/use binding changed")
		}
	}
}

func TestMaintainedSourceRootsAndOutsidePackageControl(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "pkg", "owner"), 0755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "pkg", "owner", "source.go")
	if err := os.WriteFile(base, []byte("package owner\nfunc Existing(s string)bool{return s==\"\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	c := Config{Dir: dir, SourceRoots: []SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}, ExcludeDirs: []string{"vendor"}, ExcludeFiles: []string{"*_test.go"}}}, IncludeSyntax: true, Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}, {Kind: "comparison", Node: "binary", Reachability: "all", Operators: []string{"=="}}}}
	actual := mustExtract(t, c)
	approval := reviewed(actual)
	p := fixturePolicy
	p.RequireSources = true
	if failures := Validate(actual, approval, p); len(failures) > 0 {
		t.Fatal(failures)
	}
	// A new file outside the original package enters the SAME extraction and
	// validation path. It has no known helper name, root or manifest row.
	probe := filepath.Join(dir, "plugins", "newplugin", "unfamiliar.go")
	c.Overlay = map[string][]byte{probe: []byte("package newplugin\nfunc Arbitrary(s string)bool{return s==\"\"||s==\"\"}\n")}
	mutated := mustExtract(t, c)
	failures := strings.Join(Validate(mutated, approval, p), "\n")
	if !strings.Contains(failures, "UNLISTED plugins/newplugin/unfamiliar.go:") || !strings.Contains(failures, "UNLISTED SOURCE plugins/newplugin/unfamiliar.go") {
		t.Fatal("outside-package member escaped", failures)
	}
	var comparisons []Member
	for _, m := range mutated.Members {
		if strings.HasPrefix(m.ID, "plugins/") && m.Kind == "comparison" {
			comparisons = append(comparisons, m)
		}
	}
	if len(comparisons) != 2 || comparisons[0].ID == comparisons[1].ID || comparisons[0].Offset == comparisons[1].Offset || comparisons[0].Line != comparisons[1].Line {
		t.Fatal("same-line identities collapsed", comparisons)
	}
}

func TestLexicalSourcesFactsAndAssignments(t *testing.T) {
	dir := t.TempDir()
	paths := []string{"peer.py", "peer.ps1", "peer.swift", "nogate.rb"}
	for _, path := range paths {
		if err := os.WriteFile(filepath.Join(dir, path), []byte("# .Trim() strip() trimmingCharacters\r\nvalue = alias\r\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	c := Config{Dir: dir, SourceRoots: []SourceRoot{{Path: ".", Languages: map[string]string{".py": "python", ".ps1": "powershell", ".swift": "swift", ".rb": "ruby"}, Mode: "lexical"}}, Selectors: []Selector{{Kind: "trim", Node: "text", Reachability: "all", Pattern: `\.trim\(\)|strip\(\)|trimmingCharacters`, FoldCase: true}}}
	actual := mustExtract(t, c)
	if len(actual.Sources) != 4 || len(actual.Members) != 12 {
		t.Fatal("incomplete lexical peers", len(actual.Sources), len(actual.Members))
	}
	for i := range actual.Members {
		actual.Members[i].Facts = map[string][]string{"origins": {"domain-source"}, "disposition": {"decoded_identity"}}
	}
	approval := reviewed(actual)
	p := fixturePolicy
	p.RequireSources = true
	p.FactKeys = []string{"origins"}
	p.ClassFact = "disposition"
	p.Classes = []string{"decoded_identity", "non_name"}
	p.ClassKinds = map[string]string{"decoded_identity": "review", "non_name": "review"}
	p.ForbiddenClasses = []string{"decoded_identity"}
	for i := range approval.Members {
		a := &approval.Members[i]
		a.Class = "decoded_identity"
		a.Assignment = a.ID
		p.Assignments = append(p.Assignments, Assignment{ID: a.ID, MemberID: a.ID, Digest: a.Digest, Class: a.Class, Owner: "credentials", Reference: "exact-operator-routing"})
	}
	if failures := Validate(actual, approval, p); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, probe := range []struct {
		name, want string
		change     func(*Inventory, *Policy)
	}{
		{"wrong class", "CLASS FACT CONTRADICTION", func(a *Inventory, p *Policy) { a.Members[0].Class = "non_name" }},
		{"missing class", "UNCLASSIFIED", func(a *Inventory, p *Policy) { a.Members[0].Class = "" }},
		{"unassigned", "UNRESOLVED ASSIGNMENT", func(a *Inventory, p *Policy) { a.Members[0].Assignment = "" }},
		{"stale route", "UNRESOLVED ASSIGNMENT", func(a *Inventory, p *Policy) { p.Assignments[0].Digest = "stale" }},
		{"wrong owner reference", "UNRESOLVED ASSIGNMENT", func(a *Inventory, p *Policy) { p.Assignments[0].Reference = "" }},
		{"changed origins", "FACT CHANGED", func(a *Inventory, p *Policy) { a.Members[0].Facts = map[string][]string{"origins": {"other"}} }},
	} {
		t.Run(probe.name, func(t *testing.T) {
			a := approval
			a.Members = append([]Member(nil), approval.Members...)
			policy := p
			policy.Assignments = append([]Assignment(nil), p.Assignments...)
			probe.change(&a, &policy)
			if !strings.Contains(strings.Join(Validate(actual, a, policy), "\n"), probe.want) {
				t.Fatal("contradiction escaped", probe.want)
			}
		})
	}
	path := filepath.Join(dir, "nogate.rb")
	raw, _ := os.ReadFile(path)
	c.Overlay = map[string][]byte{path: []byte(strings.ReplaceAll(string(raw), "\r\n", "\n"))}
	normalized := mustExtract(t, c)
	for i := range normalized.Members {
		normalized.Members[i].Facts = map[string][]string{"origins": {"domain-source"}, "disposition": {"decoded_identity"}}
	}
	if failures := Validate(normalized, approval, p); len(failures) > 0 {
		t.Fatal("CRLF/LF changed canonical identity", failures)
	}
	c.Overlay[path] = append(raw, []byte("new_use = alias.empty?\n")...)
	if !strings.Contains(strings.Join(Validate(mustExtract(t, c), approval, p), "\n"), "SOURCE CHANGED nogate.rb") {
		t.Fatal("changed file without old gate escaped")
	}
}
