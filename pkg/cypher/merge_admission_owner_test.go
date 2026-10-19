package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Inventory the MERGE node producers from source, then require their admission
// call to precede the snapshot lookup. A new producer cannot silently inherit
// the whole-path omission; the public Bolt matrix checks the runtime contract.
func TestMergeNodeProducersAdmitBeforeSnapshot(t *testing.T) {
	want := map[string]string{
		"executeMergeWithContext": "findMergeNode",
		"pipelineMergePath":      "pipelineMatchRows",
		"pipelineApplyMerge":     "findMergeNodesScanned",
	}
	seen := make(map[string]bool)
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || !strings.Contains(fn.Name.Name, "Merge") || fn.Body == nil {
				continue
			}
			calls := make(map[string]token.Pos)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				var called string
				switch target := call.Fun.(type) {
				case *ast.Ident:
					called = target.Name
				case *ast.SelectorExpr:
					called = target.Sel.Name
				}
				if calls[called] == 0 {
					calls[called] = call.Pos()
				}
				return true
			})
			lookup, known := want[fn.Name.Name]
			if calls["CreateNode"] != 0 || calls["pipelineApplyCreateClauses"] != 0 {
				require.True(t, known, "%s: unclassified MERGE node producer %s", name, fn.Name.Name)
			}
			if known {
				seen[fn.Name.Name] = true
				require.NotZero(t, calls["prepareMergeKeys"], "%s must admit keys", fn.Name.Name)
				require.NotZero(t, calls[lookup], "%s lookup moved; update inventory", fn.Name.Name)
				require.Less(t, calls["prepareMergeKeys"], calls[lookup], "%s must admit before snapshot lookup", fn.Name.Name)
			}
		}
	}
	require.Len(t, seen, len(want), "MERGE producers moved; re-establish the inventory")
}
