// Author: L4 implementation lane (Codex), 1 November 2026.
package cleanupcensus_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestLedgerFunctionIdentity(t *testing.T) {
	for _, test := range []struct{ identity, declaration string }{
		{"github.com/orneryd/nornicdb/pkg/storage.TestExample$2$1", "TestExample$2$1"},
		{"(*github.com/orneryd/nornicdb/pkg/storage.First).release$1", "(*First).release$1"},
		{"(github.com/orneryd/nornicdb/pkg/storage.Second).release$3", "(*Second).release$3"},
		{"(*github.com/orneryd/nornicdb/pkg/storage.Generic[github.com/orneryd/nornicdb/pkg/storage.Resource]).release$1", "(*Generic).release$1"},
		{"github.com/orneryd/nornicdb/pkg/storage.visit[github.com/orneryd/nornicdb/pkg/storage.Resource]$1", "visit$1"},
	} {
		if got := ledgerSourceFunction(test.identity); got != test.declaration {
			t.Errorf("%s: got %q, want %q", test.identity, got, test.declaration)
		}
	}
	first := "(*github.com/orneryd/nornicdb/pkg/storage.First).release$1"
	second := "(*github.com/orneryd/nornicdb/pkg/storage.Second).release$1"
	if ledgerSourceFunction(first) == ledgerSourceFunction(second) {
		t.Fatal("a ledger method for one receiver can justify another receiver")
	}
	if ledgerKey("fixture_test.go", first, "r.Close()") == ledgerKey("fixture_test.go", first+"$1", "r.Close()") {
		t.Fatal("SSA member matching lost its complete closure identity")
	}
	if ledgerSourceFunction(first) == ledgerSourceFunction(first+"$1") {
		t.Fatal("AST ledger matching lost its complete closure identity")
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", `package specimen
func TestScopes() {
    first := func() { r.Close(); nested := func() { r.Close() }; _ = nested }
    second := func() { r.Close() }
    _, _ = first, second
}`, 0)
	if err != nil {
		t.Fatal(err)
	}
	var scopes []string
	inspectSourceFunction(file.Decls[0].(*ast.FuncDecl).Body, "TestScopes", func(node ast.Node, scope string) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Close" {
				scopes = append(scopes, scope)
			}
		}
		return true
	})
	want := []string{"TestScopes$1", "TestScopes$1$1", "TestScopes$2"}
	if len(scopes) != len(want) {
		t.Fatalf("source closure population %v, want %v", scopes, want)
	}
	for index := range want {
		if scopes[index] != want[index] {
			t.Fatalf("source closure population %v, want %v", scopes, want)
		}
	}

}
