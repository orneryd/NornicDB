package storage

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These owners must use withView/withUpdate or the handle returned by
// beginHelperTxn[WithAdmission]. No helper implementation lives here, so
// there are no direct-handle exceptions. A supplied transaction is already
// admitted by its caller and may be used without opening another view.
var labelIndexIOOwnerFiles = map[string]string{
	"badger_label_index_lifecycle.go": "generation markers, readiness and retired dictionary entries",
	"badger_label_index_backfill.go":  "shared startup/recovery admission and full label rebuild",
	"badger_label_index_read.go":      "readiness-aware candidates, fallback and counts",
	"badger_label_count.go":           "public counts, count readiness, initialization and repair",
	"badger_node_index_reader.go":     "snapshot label candidates and dictionary fallback",
}

// Reject the field access itself, including aliases such as db := b.db,
// nested receivers such as r.b.db, method values and function literals. The
// owner files have no other db field; checking its spelling also catches a
// renamed engine receiver without a type-checker dependency.
func labelIndexDirectHandleUses(fset *token.FileSet, file *ast.File) []string {
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "db" {
			violations = append(violations, fmt.Sprintf("%s: direct database field access; use lifetime-admitted helpers", fset.Position(selector.Pos())))
		}
		return true
	})
	return violations
}

func TestLabelIndexIOUsesLifetimeHelpers(t *testing.T) {
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, pkg := range packages {
		for path, file := range pkg.Files {
			name := filepath.Base(path)
			reason, owned := labelIndexIOOwnerFiles[name]
			if !owned {
				if strings.HasPrefix(name, "badger_label") {
					t.Errorf("new label owner %s must be listed with a reason in labelIndexIOOwnerFiles", name)
				}
				continue
			}
			seen[name] = true
			for _, violation := range labelIndexDirectHandleUses(fset, file) {
				t.Errorf("%s (%s)", violation, reason)
			}
		}
	}
	for name := range labelIndexIOOwnerFiles {
		if !seen[name] {
			t.Errorf("label owner file not inspected: %s", name)
		}
	}
}

func TestLabelIndexIOGuardMutation(t *testing.T) {
	t.Run("reintroduced-readiness-probe", func(t *testing.T) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "badger_label_index_lifecycle.go", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		before := len(labelIndexDirectHandleUses(fset, file))
		mutated := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "labelIndexReady" {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "withView" {
					selector.X = &ast.SelectorExpr{X: selector.X, Sel: ast.NewIdent("db")}
					selector.Sel.Name = "View"
					mutated = true
				}
				return true
			})
		}
		if !mutated || len(labelIndexDirectHandleUses(fset, file)) != before+1 {
			t.Fatal("guard failed to reject a direct b.db.View reintroduced into labelIndexReady")
		}
	})
	for _, access := range []string{
		"b.db.View(nil)",
		"b.db.Update(nil)",
		"b.db.NewTransaction(false)",
		"db := b.db; _ = db",
		"r.b.db.View(nil)",
		"func() { b.db.View(nil) }()",
	} {
		t.Run(access, func(t *testing.T) {
			fset := token.NewFileSet()
			// Model reintroducing an unleased probe into an owner. The same
			// detector used by the package guard must reject this mutation.
			file, err := parser.ParseFile(fset, "badger_label_index_lifecycle.go", "package storage; func probe() { "+access+" }", 0)
			if err != nil {
				t.Fatal(err)
			}
			if uses := labelIndexDirectHandleUses(fset, file); len(uses) != 1 {
				t.Fatalf("guard missed reintroduced direct handle use %q: %v", access, uses)
			}
		})
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "admitted.go", "package storage; func probe() { b.withView(nil); db, err := b.beginHelperTxn(); _ = err; defer b.txnWG.Done(); db.View(nil) }", 0)
	if err != nil {
		t.Fatal(err)
	}
	if uses := labelIndexDirectHandleUses(fset, file); len(uses) != 0 {
		t.Fatalf("guard rejected lifetime-admitted helpers: %v", uses)
	}
}
