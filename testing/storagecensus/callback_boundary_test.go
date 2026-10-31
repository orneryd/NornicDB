package storagecensus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicationCallbackSummaryRejectsOwnerAndBodyDrift(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(root, "pkg/storage/publication_decision.go"))
	require.NoError(t, err)
	parse := func(source []byte) (*token.FileSet, *ast.FuncDecl) {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, "publication_decision.go", source, 0)
		require.NoError(t, err)
		for _, node := range file.Decls {
			if fn, ok := node.(*ast.FuncDecl); ok && fn.Name.Name == "DecidePublication" {
				return set, fn
			}
		}
		t.Fatal("canonical publication owner is missing")
		return nil, nil
	}
	set, declaration := parse(source)
	require.True(t, reviewedCallbackBoundary("pkg/storage.DecidePublication", 1, declaration, set))
	require.False(t, reviewedCallbackBoundary("pkg/other.DecidePublication", 1, declaration, set))
	require.False(t, reviewedCallbackBoundary("pkg/storage.DecidePublication", 0, declaration, set))
	require.False(t, reviewedCallbackBoundary("pkg/storage.DecidePublication", 1, nil, set))
	// An owner that no longer seals retained actions must lose its summary,
	// even while its exported name and callback parameter remain unchanged.
	changed := strings.ReplaceAll(string(source), "sealed = true", "sealed = false")
	require.NotEqual(t, string(source), changed)
	changedSet, changedDeclaration := parse([]byte(changed))
	require.False(t, reviewedCallbackBoundary("pkg/storage.DecidePublication", 1, changedDeclaration, changedSet))
}
