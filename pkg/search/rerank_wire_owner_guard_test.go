package search

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRerankProvidersDecodeOnlyThroughWireOwner(t *testing.T) {
	const modulePrefix = "github.com/orneryd/nornicdb/"
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	providerPackages := make(map[string]struct{})
	providerNames := make(map[string]map[string]struct{})
	for _, qualified := range discoverProductionRerankerTypes(t) {
		separator := strings.LastIndexByte(qualified, '.')
		if separator < 0 {
			t.Fatalf("invalid discovered reranker type %q", qualified)
		}
		packagePath, typeName := qualified[:separator], qualified[separator+1:]
		providerPackages[packagePath] = struct{}{}
		if providerNames[packagePath] == nil {
			providerNames[packagePath] = make(map[string]struct{})
		}
		providerNames[packagePath][typeName] = struct{}{}
	}
	for packagePath := range providerPackages {
		relative := strings.TrimPrefix(packagePath, modulePrefix)
		if relative == packagePath {
			t.Fatalf("reranker package %q is outside the module", packagePath)
		}
		directory := filepath.Join(moduleRoot, filepath.FromSlash(relative))
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		ownerCalls := 0
		selectedFiles := 0
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			fileSet := token.NewFileSet()
			file, err := parser.ParseFile(fileSet, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !rerankProviderSourceFile(file, providerNames[packagePath]) {
				continue
			}
			selectedFiles++
			jsonAliases, ownerAliases := rerankWireImportAliases(file)
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				if jsonAliases[identifier.Name] && (selector.Sel.Name == "Unmarshal" || selector.Sel.Name == "NewDecoder") {
					t.Errorf("rerank provider decoding bypasses internal/rerankwire: %s:%d %s.%s", path, fileSet.Position(call.Pos()).Line, identifier.Name, selector.Sel.Name)
				}
				if ownerAliases[identifier.Name] && (selector.Sel.Name == "DecodeResponse" || strings.HasPrefix(selector.Sel.Name, "Accept")) {
					ownerCalls++
				}
				return true
			})
		}
		if selectedFiles == 0 {
			t.Errorf("reranker package %s has no discovered provider source file", packagePath)
		}
		if ownerCalls == 0 {
			t.Errorf("reranker package %s has no call into internal/rerankwire", packagePath)
		}
	}
}

func rerankProviderSourceFile(file *ast.File, providerTypes map[string]struct{}) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if strings.Contains(strings.ToLower(function.Name.Name), "rerank") {
			return true
		}
		if function.Recv == nil || len(function.Recv.List) == 0 {
			continue
		}
		receiver := function.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		if identifier, ok := receiver.(*ast.Ident); ok {
			if _, provider := providerTypes[identifier.Name]; provider {
				return true
			}
		}
	}
	return false
}

func rerankWireImportAliases(file *ast.File) (map[string]bool, map[string]bool) {
	jsonAliases := make(map[string]bool)
	ownerAliases := make(map[string]bool)
	for _, imported := range file.Imports {
		path := strings.Trim(imported.Path.Value, `"`)
		alias := ""
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		switch path {
		case "encoding/json":
			if alias == "" {
				alias = "json"
			}
			jsonAliases[alias] = true
		case "github.com/orneryd/nornicdb/internal/rerankwire":
			if alias == "" {
				alias = "rerankwire"
			}
			ownerAliases[alias] = true
		}
	}
	return jsonAliases, ownerAliases
}
