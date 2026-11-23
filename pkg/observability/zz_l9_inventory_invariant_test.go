package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestL9SinkInventoryDoesNotDiscardReviewedEgress(t *testing.T) {
	// Review the rule itself. No inventories, credential data or network
	// operations are involved in this audit.
	source, err := parser.ParseFile(token.NewFileSet(), "url_sink_structure_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, declaration := range source.Decls {
		f, ok := declaration.(*ast.FuncDecl)
		if !ok || f.Name.Name != "TestCredentialURLSinkStructure" {
			continue
		}
		ast.Inspect(f.Body, func(node ast.Node) bool {
			r, ok := node.(*ast.RangeStmt)
			if !ok {
				return true
			}
			set, ok := r.X.(*ast.Ident)
			if !ok || set.Name != "reviewedNonEgress" {
				return true
			}
			found = true
			for _, s := range r.Body.List {
				expression, ok := s.(*ast.ExprStmt)
				if !ok {
					continue
				}
				call, ok := expression.X.(*ast.CallExpr)
				if !ok || len(call.Args) != 2 {
					continue
				}
				name, nameOK := call.Fun.(*ast.Ident)
				mapName, mapOK := call.Args[0].(*ast.Ident)
				if nameOK && mapOK && name.Name == "delete" && mapName.Name == "reviewed" {
					t.Error("classification loop discards the reviewed egress entry without a conflict check")
				}
			}
			return false
		})
	}
	if !found {
		t.Fatal("the structure audit's classification owner moved")
	}
}
