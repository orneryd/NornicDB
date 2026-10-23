package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The compiler rejects omitted arguments; this source guard prevents restoring
// an optional/default path or manufacturing an undeclared zero scope.
func TestNodeCacheWriterScopeContract(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	required := map[string]bool{
		"issueNodeCacheCommit": false, "cacheCommitNodeWrites": false,
		"newCommitWriter": false, "withUpdate": false, "withUpdateUnits": false,
		"withScopedNodeCacheUpdate": false, "commitEngineWrite": false,
		"commitEngineWriteWithCache": false,
	}
	optional := map[string]bool{
		"withScopedNodeCacheUpdate": true, "withNotifyingUpdate": true,
		"commitEngineWriteHeld": true, "commitEngineWriteWithRead": true,
		"commitWithNodeCache": true, "commitWithCacheScope": true,
		"commitShared": true, "commitSharedWithCacheScope": true, "revokeMaintenancePublishers": true, "dropPrefixWithScope": true, "updateWithScope": true, "forEachStoredRecordInChunks": true,
	}
	// Arbitrary managed callbacks, physical maintenance/rollback, and the offline
	// legacy serializer have no authoritative entity-operation inventory.
	unknownAllowed := map[string]bool{
		"badger_managed.go:Update":        true,
		"badger_managed.go:rollbackSince": true,
		"badger_cache_commit.go:DropAll":  true, "badger_managed.go:DropPrefix": true, "badger_managed.go:Load": true,
		"serializer_migration.go:MigrateBadgerToMsgpack": true,
		"badger_backup.go:deleteNamespaceKeyFamilies":    true,
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			_, must := required[fn.Name.Name]
			if must || optional[fn.Name.Name] || (name == "badger_managed.go" && (fn.Name.Name == "commit" || fn.Name.Name == "NewWriteBatch")) {
				scoped := false
				for _, field := range fn.Type.Params.List {
					if id, ok := field.Type.(*ast.Ident); ok && id.Name == "nodeCacheWriteScope" {
						scoped = true
					}
				}
				if !scoped {
					t.Errorf("%s: %s requires a non-variadic nodeCacheWriteScope argument", fset.Position(fn.Pos()), fn.Name.Name)
				}
				if must {
					required[fn.Name.Name] = true
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.CompositeLit:
					if id, ok := node.Type.(*ast.Ident); ok && id.Name == "nodeCacheWriteScope" && name != "badger_cache_scope.go" {
						t.Errorf("%s: scope must come from the canonical declaration constructors", fset.Position(node.Pos()))
					}
				case *ast.AssignStmt:
					for _, lhs := range node.Lhs {
						if field, ok := lhs.(*ast.SelectorExpr); ok && field.Sel.Name == "cacheScope" {
							t.Errorf("%s: commit-writer scope must be supplied at construction", fset.Position(node.Pos()))
						}
					}
				case *ast.CallExpr:
					if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "unknownNodeCacheWrites" {
						if !unknownAllowed[filepath.Base(name)+":"+fn.Name.Name] {
							t.Errorf("%s: unknown scope is not allowed at %s", fset.Position(node.Pos()), fn.Name.Name)
						}
						if len(node.Args) != 1 {
							t.Errorf("%s: unknown scope must name its entrance", fset.Position(node.Pos()))
						}
					}
				}
				return true
			})
		}
	}
	for name, found := range required {
		if !found {
			t.Errorf("required durable scope entrance missing: %s", name)
		}
	}
}

func receiverType(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	expression := fn.Recv.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	if id, ok := expression.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}
