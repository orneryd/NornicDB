package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every embedder install in the server and the commands uses SetEmbedder's
// result. A closing database refuses the embedder (and closes it); a caller
// that carries on regardless reports a model nobody serves (F-EMBED).
func TestEmbedderInstallsUseTheResult(t *testing.T) {
	dirs, err := filepath.Glob("../../cmd/*")
	require.NoError(t, err)
	dirs = append(dirs, ".")
	fset := token.NewFileSet()
	var discarded []string
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, name, nil, 0)
			require.NoError(t, err)
			ast.Inspect(file, func(n ast.Node) bool {
				var call ast.Expr
				switch stmt := n.(type) {
				case *ast.ExprStmt:
					call = stmt.X
				case *ast.GoStmt:
					call = stmt.Call
				case *ast.DeferStmt:
					call = stmt.Call
				case *ast.AssignStmt:
					blank := true
					for _, lhs := range stmt.Lhs {
						if ident, ok := lhs.(*ast.Ident); !ok || ident.Name != "_" {
							blank = false
						}
					}
					if blank && len(stmt.Rhs) == 1 {
						call = stmt.Rhs[0]
					}
				}
				if c, ok := call.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetEmbedder" {
						discarded = append(discarded, fset.Position(c.Pos()).String())
					}
				}
				return true
			})
		}
	}
	require.Empty(t, discarded, "SetEmbedder's accepted result is ignored")
}
