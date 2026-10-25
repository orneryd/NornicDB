package workcensus

// Author: #1667 implementation lane (Codex).

import (
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageNilReturnContextProofControls(t *testing.T) {
	for _, tc := range []struct{ name, body, guard, wrongGuard, returned, wrongReturn string }{
		{"parameter", `func owner(ctx,other context.Context)context.Context{if ctx==nil{return context.Background()};return ctx}
func Execute(ctx context.Context)context.Context{return owner(ctx,ctx)}`, "ctx==nil", "ctx!=nil", ";return ctx}", ";return other}"},
		{"field nil first", `type holder struct{ctx,other context.Context}
func(v holder)owner()context.Context{if v.ctx==nil{return context.Background()};return v.ctx}
func Execute(ctx context.Context)context.Context{return (holder{ctx:ctx}).owner()}`, "v.ctx==nil", "v.ctx!=nil", ";return v.ctx}", ";return v.other}"},
		{"field nonnil first", `type holder struct{ctx,other context.Context}
func(v holder)owner()context.Context{if v.ctx!=nil{return v.ctx};return context.Background()}
func Execute(ctx context.Context)context.Context{return (holder{ctx:ctx}).owner()}`, "v.ctx!=nil", "v.ctx==nil", "return v.ctx}", "return v.other}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module nilreturn.fixture\n\ngo 1.26\n"), 0600); err != nil {
				t.Fatal(err)
			}
			raw := "package fixture\nimport \"context\"\n" + tc.body
			extract := func(source string) Inventory {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, "entry.go"), []byte(source), 0600); err != nil {
					t.Fatal(err)
				}
				return mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, IncludeSyntax: true, Roots: []Root{{Package: "nilreturn.fixture", Name: "Execute", Required: true}}, Selectors: []Selector{{Kind: "context-boundary", Node: "call", Packages: []string{"context"}, Names: []string{"Background", "TODO"}}}})
			}
			find := func(inv Inventory) Member {
				t.Helper()
				for _, m := range inv.Members {
					if m.Kind != "context-boundary" || m.Syntax == nil || len(m.Syntax.Ancestors) == 0 {
						continue
					}
					if _, ok := m.Syntax.Ancestors[len(m.Syntax.Ancestors)-1].(*ast.ReturnStmt); ok {
						return m
					}
				}
				t.Fatal("returned context source missing")
				return Member{}
			}
			actual := extract(raw)
			m := find(actual)
			if !storageNilContextDefault(m) {
				t.Fatal("typed nil-only returned context not proved")
			}
			m.Class = nilContextClass
			m.Reason = "Exact typed two-branch nil coalescing return; non-nil context identity retained."
			if failures := validateStorageContextClasses(actual, Inventory{Roots: actual.Roots, Members: []Member{m}}); len(failures) != 0 {
				t.Fatal(failures)
			}
			for _, negative := range []struct{ name, old, replacement string }{
				{"inverted guard", tc.guard, tc.wrongGuard},
				{"different binding", tc.returned, tc.wrongReturn},
			} {
				t.Run(negative.name, func(t *testing.T) {
					changed := strings.Replace(raw, negative.old, negative.replacement, 1)
					if changed == raw {
						t.Fatal("control did not change source")
					}
					inv := extract(changed)
					row := find(inv)
					row.Class = nilContextClass
					row.Reason = "plant: ordinary pins refreshed, returned-context fact contradicted"
					failures := validateStorageContextClasses(inv, Inventory{Roots: inv.Roots, Members: []Member{row}})
					if len(failures) != 1 || !strings.HasPrefix(failures[0], "CLASS FACT CONTRADICTION ") {
						t.Fatal("same production gate missed contradiction", failures)
					}
				})
			}
		})
	}
}
