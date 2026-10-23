package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Typed search nodes must keep the source metadata owned by pkg/search. A
// new procedure cannot recreate the old identity/labels/properties projection.
func TestSearchProcedureNodeMaterializationOwner(t *testing.T) {
	for _, directory := range []string{".", filepath.Join("..", "search")} {
		files, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range files {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, entry.Name()), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			usesSearch := directory != "."
			for _, imported := range file.Imports {
				if strings.Trim(imported.Path.Value, "\"") == "github.com/orneryd/nornicdb/pkg/search" {
					usesSearch = true
				}
			}
			if !usesSearch {
				continue
			}
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					continue
				}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					literal, ok := node.(*ast.CompositeLit)
					if !ok {
						return true
					}
					name := ""
					switch kind := literal.Type.(type) {
					case *ast.Ident:
						name = kind.Name
					case *ast.SelectorExpr:
						if qualifier, ok := kind.X.(*ast.Ident); ok {
							name = qualifier.Name + "." + kind.Sel.Name
						}
					}
					if name != "storage.Node" && name != "SearchResult" {
						return true
					}
					if directory != "." && name == "storage.Node" {
						return true
					}
					fields := map[string]bool{}
					for _, element := range literal.Elts {
						if pair, ok := element.(*ast.KeyValueExpr); ok {
							if field, ok := pair.Key.(*ast.Ident); ok {
								fields[field.Name] = true
							}
						}
					}
					if !fields["Labels"] || !fields["Properties"] {
						return true
					}
					if directory != "." && name == "SearchResult" && entry.Name() == "search_result_materialization.go" && function.Name.Name == "searchResultFromNode" {
						return true
					}
					t.Errorf("%s:%s reconstructs a search node; use the shared result materialization owner", entry.Name(), function.Name.Name)
					return true
				})
			}
		}
	}
}
