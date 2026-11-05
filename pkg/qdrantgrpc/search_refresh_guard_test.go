package qdrantgrpc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// Collection points are excluded from the database's storage-event queue, so
// a PointsService method that writes the collection store must refresh search
// for what it wrote (indexPoint or RefreshNode). A writer added without it
// fails here (#1482).
func TestPointWritersRefreshSearch(t *testing.T) {
	writes := map[string]bool{"CreateNode": true, "BulkCreateNodes": true, "UpdateNode": true, "DeleteNode": true}
	file, err := parser.ParseFile(token.NewFileSet(), "points_service.go", nil, 0)
	require.NoError(t, err)
	var missing []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			continue
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok || star.X.(*ast.Ident).Name != "PointsService" {
			continue
		}
		writesStore, refreshes := false, false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch callee := call.Fun.(type) {
			case *ast.Ident:
				refreshes = refreshes || callee.Name == "indexPoint"
			case *ast.SelectorExpr:
				if receiver, ok := callee.X.(*ast.Ident); ok && receiver.Name == "store" && writes[callee.Sel.Name] {
					writesStore = true
				}
				refreshes = refreshes || callee.Sel.Name == "RefreshNode"
			}
			return true
		})
		if writesStore && !refreshes {
			missing = append(missing, fn.Name.Name)
		}
	}
	require.Empty(t, missing, "these PointsService methods write points without refreshing search")
}
