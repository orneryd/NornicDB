package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The deterministic selector tests prove the capture contract. This guard
// ensures its public consumers keep using that owner as new read paths arrive.
func TestSnapshotOverlayPublicReadsReachCapture(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	calls := map[string][]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			recv := fn.Recv.List[0]
			ptr, ok := recv.Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			typ, ok := ptr.X.(*ast.Ident)
			if !ok || typ.Name != "BadgerEngine" {
				continue
			}
			calls[fn.Name.Name] = nil
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv.Names[0].Name {
					calls[fn.Name.Name] = append(calls[fn.Name.Name], sel.Sel.Name)
				}
				if owner, ok := sel.X.(*ast.SelectorExpr); ok && owner.Sel.Name == "writeBehind" {
					for _, forbidden := range []string{"Overlay", "GetNode", "GetEdge", "LookupNode", "LookupEdge", "NodeDeleted", "EdgeDeleted"} {
						if strings.Contains(sel.Sel.Name, forbidden) {
							t.Errorf("%s.%s bypasses capture with writeBehind.%s", name, fn.Name.Name, sel.Sel.Name)
						}
					}
				}
				return true
			})
		}
	}
	readers := []string{"GetNode", "GetNodeProjected", "GetNodeWithoutEmbeddings", "GetEdge", "BatchGetNodes", "BatchGetNodesWithoutEmbeddings", "HasLabelBatch", "RelationshipEndpointVisible", "GetNodesByLabelInScope", "GetFirstNodeByLabelInScope", "ForEachNodeIDByLabelInScope", "AllNodes", "AllEdges", "GetEdgesByType", "GetOutgoingEdges", "GetIncomingEdges", "GetAdjacentEdges", "OutgoingEdgeHeaders", "IncomingEdgeHeaders", "GetEdgesBetween", "GetEdgeBetween", "MatchEdgesBetween", "StreamNodesWithOptions", "StreamNodesByLabelProjectedInScope", "StreamEdges", "StreamEdgesByTypeInScope", "IterateNodes", "StreamNodeChunks", "GetNodeEmbeddingMeta"}
	if _, ok := calls["GetQueryNode"]; ok {
		readers = append(readers, "GetQueryNode")
	}
	for _, reader := range readers {
		seen := map[string]bool{}
		var reaches func(string) bool
		reaches = func(name string) bool {
			if name == "captureReadView" {
				return true
			}
			if seen[name] {
				return false
			}
			seen[name] = true
			for _, callee := range calls[name] {
				if reaches(callee) {
					return true
				}
			}
			return false
		}
		require.True(t, reaches(reader), "%s must reach the shared snapshot/overlay capture", reader)
	}
}
