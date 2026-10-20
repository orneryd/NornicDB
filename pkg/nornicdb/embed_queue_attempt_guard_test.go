package nornicdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is an architectural guard, not an assertion of helper implementation.
// Every worker outcome route must enter the storage acceptance/publication owner.
func TestEmbeddingAttemptOutcomeOwnership(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	writes, publications := 0, 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			workerMethod := false
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					if receiver, ok := pointer.X.(*ast.Ident); ok {
						workerMethod = receiver.Name == "EmbedWorker" || receiver.Name == "EmbedQueue"
					}
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "UpdateNode", "UpdateNodeEmbeddings":
					require.False(t, workerMethod, "%s writes an outcome through the body API", path)
				case "UpdateNodeEmbeddingSidecar":
					writes++
					require.Equal(t, "commitNodeOutcome", fn.Name.Name, "%s bypasses the attempt owner", path)
				case "onEmbedded":
					publications++
					require.Equal(t, "bindNodeAttemptLocked", fn.Name.Name, "%s publishes outside accepted outcome delivery", path)
				case "Add":
					if counter, ok := sel.X.(*ast.SelectorExpr); ok && workerMethod {
						owner := map[string]string{"processed": "bindNodeAttemptLocked", "parked": "bindNodeAttemptLocked", "failed": "recordNodeFailureLocked"}[counter.Sel.Name]
						if owner != "" {
							require.Equal(t, owner, fn.Name.Name, "%s changes outcome statistics outside their owner", path)
						}
					}
				}
				return true
			})
		}
	}
	require.Equal(t, 1, writes)
	require.Equal(t, 1, publications)
}
