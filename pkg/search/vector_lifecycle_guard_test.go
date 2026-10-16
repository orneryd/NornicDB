package search

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// lifecycleWriteRule says which owner file may write a lifecycle field, and
// what counts as a write:
//   - pointer: assigning the field itself (s.f = x);
//   - state: assigning the field or anything inside it (s.f.x = y);
//   - atomic: calling a mutating method on it (s.f.Store/Add/Swap/CompareAndSwap).
type lifecycleWriteRule struct {
	owner string
	kind  string
}

const (
	lifecycleOwnerFile = "vector_lifecycle.go"
	publishOwnerFile   = "vector_lifecycle_publish.go"
	writeOwnerFile     = "vector_lifecycle_write.go"
)

var lifecycleWriteRules = map[string]lifecycleWriteRule{
	"vectorPipeline":       {publishOwnerFile, "pointer"},
	"hnswIndex":            {publishOwnerFile, "pointer"},
	"clusterHNSW":          {publishOwnerFile, "pointer"},
	"ivfpqIndex":           {publishOwnerFile, "pointer"},
	"ivfpqEpoch":           {publishOwnerFile, "pointer"},
	"gpuEmbeddingIndex":    {publishOwnerFile, "pointer"},
	"vectorIndex":          {publishOwnerFile, "pointer"},
	"vectorFileStore":      {publishOwnerFile, "pointer"},
	"nodeSources":          {lifecycleOwnerFile, "state"},
	"life":                 {lifecycleOwnerFile, "state"},
	"hnswGeneration":       {lifecycleOwnerFile, "atomic"},
	"vectorSourceEpoch":    {lifecycleOwnerFile, "atomic"},
	"clusterGeneration":    {lifecycleOwnerFile, "atomic"},
	"hnswVectorsChanging":  {lifecycleOwnerFile, "atomic"},
	"nodePopulationWrites": {writeOwnerFile, "state"},
	"edgePopulationWrites": {writeOwnerFile, "state"},
	"provisionalWrite":     {writeOwnerFile, "pointer"},
}

var atomicMutators = map[string]bool{"Store": true, "Add": true, "Swap": true, "CompareAndSwap": true}

// selectorPath returns the field names of x.a.b.c as [a b c] when the chain is
// rooted at a plain identifier.
func selectorPath(expr ast.Expr) []string {
	var path []string
	for {
		switch e := expr.(type) {
		case *ast.SelectorExpr:
			path = append([]string{e.Sel.Name}, path...)
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.Ident:
			return path
		default:
			return nil
		}
	}
}

// lifecycleWriteViolations reports every write to a lifecycle field made
// outside its owner file.
func lifecycleWriteViolations(fset *token.FileSet, file *ast.File, name string) []string {
	var out []string
	report := func(pos token.Pos, field string) {
		out = append(out, fset.Position(pos).String()+": writes "+field+" outside "+lifecycleWriteRules[field].owner)
	}
	written := func(lhs ast.Expr) {
		path := selectorPath(lhs)
		if len(path) == 0 {
			return
		}
		rule, ok := lifecycleWriteRules[path[0]]
		if !ok || rule.owner == name {
			return
		}
		switch rule.kind {
		case "pointer":
			if len(path) == 1 {
				report(lhs.Pos(), path[0])
			}
		case "state":
			report(lhs.Pos(), path[0])
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				written(lhs)
			}
		case *ast.IncDecStmt:
			written(node.X)
		case *ast.CallExpr:
			sel, ok := node.Fun.(*ast.SelectorExpr)
			if !ok || !atomicMutators[sel.Sel.Name] {
				return true
			}
			path := selectorPath(sel.X)
			if len(path) == 1 {
				if rule, ok := lifecycleWriteRules[path[0]]; ok && rule.kind == "atomic" && rule.owner != name {
					report(node.Pos(), path[0])
				}
			}
		}
		return true
	})
	return out
}

// F-I86 route step 6: lifecycle state has one owner. Every write to the
// derived artifacts, source pointers, completeness record, worker registry,
// generations and the "vectors changing" mark lives in the owner files, so a
// new path cannot publish or retire on its own.
func TestLifecycleFieldsAreWrittenOnlyByTheOwner(t *testing.T) {
	// The checker itself must see a violation.
	probe := "package search\nfunc (s *Service) leak() { s.hnswIndex = nil; s.nodeSources.textComplete = true; s.vectorSourceEpoch.Add(1) }\n"
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "probe.go", probe, 0)
	require.NoError(t, err)
	require.Len(t, lifecycleWriteViolations(fset, parsed, "probe.go"), 3, "the guard must detect pointer, state and atomic writes")

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var violations []string
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		violations = append(violations, lifecycleWriteViolations(fset, file, name)...)
		checked++
	}
	require.Greater(t, checked, 10, "the guard must walk the package sources")
	sort.Strings(violations)
	require.Empty(t, violations, "lifecycle state written outside its owner:\n%s", strings.Join(violations, "\n"))
}
