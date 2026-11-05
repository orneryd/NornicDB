package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// Public emitters must name their executor owner (enforced by emit's signature)
// and cross the same row boundary as materialization. Encoder carrier additions
// must also make an explicit visibility decision, including remote provenance.
func TestQueryMemoryOutputBoundaryGuard(t *testing.T) {
	read := func(path string) *ast.File {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		require.NoError(t, err)
		return file
	}
	function := func(file *ast.File, name string) *ast.FuncDecl {
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == name {
				return fn
			}
		}
		t.Fatalf("missing output owner %s", name)
		return nil
	}
	calls := func(fn *ast.FuncDecl, method string) bool {
		found := false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == method {
					found = true
				}
			}
			return true
		})
		return found
	}
	projection := read("query_property_visibility.go")
	require.True(t, calls(function(read("result_stream.go"), "emit"), "publishQueryRow"))
	require.True(t, calls(function(projection, "projectQueryResultProperties"), "publishQueryRow"))
	require.True(t, calls(function(projection, "publishQueryRow"), "releaseRow"))

	// A typed container retains the visibility contract of its entity leaf.
	var carrier func(ast.Expr) string
	carrier = func(expression ast.Expr) string {
		switch expression := expression.(type) {
		case *ast.ArrayType:
			return carrier(expression.Elt)
		case *ast.StarExpr:
			if leaf := carrier(expression.X); leaf != "" {
				return "*" + leaf
			}
		case *ast.SelectorExpr:
			if pkg, ok := expression.X.(*ast.Ident); ok {
				if pkg.Name == "storage" {
					switch expression.Sel.Name {
					case "NodeID", "EdgeID":
						return "" // Scalar identities have no property map.
					}
					return "storage." + expression.Sel.Name
				}
				if pkg.Name == "cypher" && expression.Sel.Name == "PathResult" {
					return "PathResult"
				}
			}
		case *ast.Ident:
			if expression.Name == "PathResult" {
				return expression.Name
			}
		}
		return ""
	}
	carriers := func(root ast.Node) map[string]bool {
		found := map[string]bool{}
		ast.Inspect(root, func(node ast.Node) bool {
			if statement, ok := node.(*ast.TypeSwitchStmt); ok {
				for _, item := range statement.Body.List {
					for _, expression := range item.(*ast.CaseClause).List {
						if name := carrier(expression); name != "" {
							found[name] = true
						}
					}
				}
			}
			return true
		})
		return found
	}
	owned := carriers(function(projection, "projectVisibleQueryValue"))
	for _, path := range []string{"../bolt/packstream.go", "../server/server_db.go"} {
		for name := range carriers(read(path)) {
			require.True(t, owned[name], "%s encodes %s without a visibility decision", path, name)
		}
	}
}
