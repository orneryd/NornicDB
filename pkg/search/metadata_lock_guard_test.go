package search

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// indexMuMetadata are the node and relationship metadata maps index writes
// change under indexMu.
var indexMuMetadata = map[string]bool{
	"nodeLabels": true, "nodeTypes": true, "nodeNamedVector": true, "nodePropVector": true,
	"nodeChunkVectors": true, "nodeChunkSpans": true, "edgeTypes": true, "edgePropVector": true,
}

// metadataCallerHoldsIndexMu are methods without the Locked suffix whose
// callers hold indexMu, as each one documents.
var metadataCallerHoldsIndexMu = map[string]bool{
	// Builds a closure; every caller runs it under indexMu.
	"candidate_filter_metadata.go: indexedTypeAdmitter": true,
	// The source-selection owner; its callers hold indexMu.
	"search.go: admitVectorFileStore": true,
}

// metadataField returns the field name of s.f, or "" for any other
// expression.
func metadataField(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); ok && id.Name == "s" {
		return sel.Sel.Name
	}
	return ""
}

// metadataReadersWithoutIndexMu lists the Service methods in file that touch
// an indexMu metadata map without taking indexMu, without the Locked suffix
// that says their caller holds it, and without being listed as such.
func metadataReadersWithoutIndexMu(file *ast.File, name string) []string {
	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) == 0 || types.ExprString(fn.Recv.List[0].Type) != "*Service" {
			continue
		}
		touches, locks := false, false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			expr, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			switch types.ExprString(expr) {
			case "s.indexMu.Lock", "s.indexMu.RLock":
				locks = true
			}
			if indexMuMetadata[metadataField(expr)] {
				touches = true
			}
			return true
		})
		key := name + ": " + fn.Name.Name
		if touches && !locks && !strings.HasSuffix(fn.Name.Name, "Locked") && !metadataCallerHoldsIndexMu[key] {
			out = append(out, key)
		}
	}
	return out
}

// Every reader of the metadata index writes change under indexMu holds
// indexMu. CountPropertyVectorEntries read nodePropVector under mu alone, so
// a Cypher vector query racing a live write crashed with a concurrent map
// read and write.
func TestMetadataReadersHoldIndexMu(t *testing.T) {
	probe := "package search\nfunc (s *Service) leak() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.nodePropVector) }\n"
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "probe.go", probe, 0)
	require.NoError(t, err)
	require.Len(t, metadataReadersWithoutIndexMu(parsed, "probe.go"), 1, "the guard must see a read under mu alone")

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var problems []string
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		problems = append(problems, metadataReadersWithoutIndexMu(file, name)...)
		checked++
	}
	require.Greater(t, checked, 10, "the guard must walk the package sources")
	sort.Strings(problems)
	require.Empty(t, problems, "\n"+strings.Join(problems, "\n"))
}
