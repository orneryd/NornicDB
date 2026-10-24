package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A transaction wrapper must forward the observed-image capability. Without
// this checked contract the shared helper could silently take the blind path.
var _ storage.ObservedNodeUpdater = (*transactionStorageWrapper)(nil)
var _ storage.ObservedNodeUpdater = (*storage.BadgerTransaction)(nil)

// Observation belongs to mutation, not readers. This census covers future
// procedures and graph operators too: none may write a node through the blind
// Engine.UpdateNode API, or omit the image passed to the shared mutation owner.
func TestNodeMutationObservationOwner(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := ""
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				name = fun.Sel.Name
			case *ast.Ident:
				name = fun.Name
			}
			if name == "UpdateNode" {
				t.Errorf("%s: node mutation bypasses observed-image owner", fset.Position(call.Pos()))
			}
			image := -1
			if name == "UpdateObservedNode" {
				image = 1
			}
			if name == "updateMutatedNode" {
				image = 2
			}
			if image >= 0 {
				require.Greater(t, len(call.Args), image)
				if value, ok := call.Args[image].(*ast.Ident); ok && value.Name == "nil" {
					t.Errorf("%s: node mutation discarded its read image", fset.Position(call.Pos()))
				}
			}
			return true
		})
	}
}
