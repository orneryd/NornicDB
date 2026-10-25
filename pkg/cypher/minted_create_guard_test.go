package cypher

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The minted-ID creates skip the stored presence read, so only IDs the
// executor minted for the create may reach them: create plans, whose every
// node and relationship takes its ID from generateID, and MERGE's node
// create. Replication apply, WAL replay, imports and remote writes use
// storage.Engine, which has no minted create, so they always read storage.
func TestMintedCreatesTakeOnlyExecutorMintedIDs(t *testing.T) {
	for _, name := range []string{"CreateMintedNode", "BulkCreateMintedNodes", "CreateMintedEdge", "BulkCreateMintedEdges"} {
		_, onEngine := reflect.TypeOf((*storage.Engine)(nil)).Elem().MethodByName(name)
		require.False(t, onEngine, "storage.Engine must not offer %s", name)
	}

	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	helpers := map[string]bool{"createMintedNode": true, "bulkCreateMintedNodes": true, "createMintedEdge": true, "bulkCreateMintedEdges": true}
	methods := map[string]bool{"CreateMintedNode": true, "BulkCreateMintedNodes": true, "CreateMintedEdge": true, "BulkCreateMintedEdges": true}
	allowedCallers := map[string]bool{"StorageExecutor.applyCreatePlan": true, "StorageExecutor.executeMergeWithContext": true}
	allowedMethodFiles := map[string]bool{"minted_create.go": true, "transaction_storage_wrapper.go": true}
	var violations []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			var body strings.Builder
			_ = printer.Fprint(&body, fset, fn.Body)
			source := body.String()
			if strings.Contains(source, "append(plan.nodes") {
				require.Contains(t, source, "storage.NodeID(e.generateID())", "%s plans a node without a minted ID", name)
			}
			if strings.Contains(source, "append(plan.edges") {
				require.Contains(t, source, "storage.EdgeID(e.generateID())", "%s plans a relationship without a minted ID", name)
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if helpers[fun.Name] && !allowedCallers[name] {
						violations = append(violations, name+" calls "+fun.Name)
					}
				case *ast.SelectorExpr:
					if methods[fun.Sel.Name] && !allowedMethodFiles[path] {
						violations = append(violations, path+": "+name+" calls "+fun.Sel.Name)
					}
				}
				return true
			})
			if name == "StorageExecutor.executeMergeWithContext" {
				require.Contains(t, source, "storage.NodeID(e.generateID())", "MERGE creates a node it did not mint")
			}
		}
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}
