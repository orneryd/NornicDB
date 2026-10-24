package testfixture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const storageImport = "github.com/orneryd/nornicdb/pkg/storage"
const fixtureImport = "github.com/orneryd/nornicdb/internal/testfixture"

type constructorGap struct {
	position token.Pos
	factory  string
}

// This lint decides only whether a constructor enters the runtime registry.
// It does not inspect bindings, reassignment, Close, defer or control flow.
func constructorRegistrationGaps(set *token.FileSet, file *ast.File, localStorage, allowProcess bool) []constructorGap {
	imports, dots := map[string]string{}, map[string]bool{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := filepath.Base(path)
		if strings.HasPrefix(path, "github.com/dgraph-io/badger/") {
			name = "badger"
		}
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
		if name == "." {
			dots[path] = true
		}
	}
	var gaps []constructorGap
	var stack []ast.Node
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		stack = append(stack, node)
		reference, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		switch reference.(type) {
		case *ast.Ident, *ast.SelectorExpr:
		default:
			return true
		}
		if len(stack) > 1 {
			if selector, ok := stack[len(stack)-2].(*ast.SelectorExpr); ok && selector.Sel == node {
				return true
			}
		}
		path, name := fixtureCallTarget(reference, imports, dots, localStorage)
		factory := fixtureFactory(path, name)
		if factory == "" {
			return true
		}
		call, index := fixtureConstructorCall(stack, reference)
		registered := false
		if call != nil && index > 0 {
			parent := stack[index-1]
			if wrapper, ok := parent.(*ast.CallExpr); ok {
				p, n := fixtureCallTarget(wrapper.Fun, imports, dots, false)
				if p == fixtureImport && len(wrapper.Args) >= 2 && wrapper.Args[1] == call {
					registered = n == "Track"
					if n == "TrackProcess" && allowProcess {
						for _, ancestor := range stack {
							if function, ok := ancestor.(*ast.FuncDecl); ok && function.Name.Name == "TestTestMainRejectsLeakedStore" {
								registered = true
							}
						}
					}
				}
				if preparation, ok := wrapper.Fun.(*ast.CallExpr); ok && len(wrapper.Args) == 1 && wrapper.Args[0] == call {
					p, n = fixtureCallTarget(preparation.Fun, imports, dots, false)
					registered = p == fixtureImport && (n == "TrackResult" || n == "OwnResult" || n == "OwnResult3")
				}
			}
		}
		if !registered {
			gaps = append(gaps, constructorGap{reference.Pos(), factory})
		}
		return true
	})
	return gaps
}

func TestRepositoryStorageFixtureConstructorOwnership(t *testing.T) {
	root := filepath.Join("..", "..")
	set := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		// This package owns the runtime implementation and real leak specimens.
		if strings.HasPrefix(relative, "internal/testfixture/") {
			return nil
		}
		file, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			return err
		}
		local := filepath.ToSlash(filepath.Dir(relative)) == "pkg/storage" && file.Name.Name == "storage"
		process := relative == "pkg/cypher/store_lifecycle_test.go"
		for _, gap := range constructorRegistrationGaps(set, file, local, process) {
			t.Errorf("%s: %s must enter the fixture lifetime owner", set.Position(gap.position), gap.factory)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
