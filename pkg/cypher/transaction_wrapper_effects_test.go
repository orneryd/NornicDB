// Author: Claude, NOR-180 completeness of the transaction effect signal, 2026-10-31.
package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// The transaction wrapper reaches its underlying engine only to read, so every write
// a statement makes through it goes to the transaction, which counts it. The property
// key registry assigns dictionary tokens, and the access recorder notes reads.
func TestTransactionWrapperReachesItsEngineOnlyToRead(t *testing.T) {
	calls := map[string]bool{"GetInDegree": true, "GetOutDegree": true, "GetSchema": true, "BatchGetNodes": true,
		"NodeCount": true, "EdgeCount": true, "EdgeCountByType": true, "EdgeCountByStartLabel": true, "EdgeCountByEndLabel": true}
	paths, err := filepath.Glob("transaction_storage_wrapper*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("wrapper sources: %v %v", paths, err)
	}
	seen := 0
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			outer, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			inner, ok := outer.X.(*ast.SelectorExpr)
			if !ok || inner.Sel.Name != "underlying" {
				return true
			}
			seen++
			if !calls[outer.Sel.Name] {
				t.Errorf("%s: the wrapper calls %s on its underlying engine", path, outer.Sel.Name)
			}
			return true
		})
	}
	if seen == 0 {
		t.Fatal("the source walk found no underlying call")
	}
}
