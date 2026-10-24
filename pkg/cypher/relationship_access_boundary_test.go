package cypher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type relationshipBoundaryListedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	ImportMap  map[string]string
}

type relationshipBoundaryImporter struct {
	fset     *token.FileSet
	packages map[string]*relationshipBoundaryListedPackage
	loaded   map[string]*types.Package
	errors   []error
}

type relationshipBoundaryImportFunc func(string) (*types.Package, error)

func (f relationshipBoundaryImportFunc) Import(path string) (*types.Package, error) { return f(path) }

func (imp *relationshipBoundaryImporter) from(parent *relationshipBoundaryListedPackage) types.Importer {
	return relationshipBoundaryImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := parent.ImportMap[path]; ok {
			path = mapped
		}
		return imp.Import(path)
	})
}

func (imp *relationshipBoundaryImporter) Import(path string) (*types.Package, error) {
	if path == "unsafe" {
		return types.Unsafe, nil
	}
	if loaded := imp.loaded[path]; loaded != nil {
		return loaded, nil
	}
	listed := imp.packages[path]
	if listed == nil {
		return nil, fmt.Errorf("%s is absent from go list -deps", path)
	}
	files, err := relationshipBoundaryParseFiles(imp.fset, listed, "")
	if err != nil {
		return nil, err
	}
	config := types.Config{IgnoreFuncBodies: true, FakeImportC: true, Importer: imp.from(listed), Error: func(err error) {
		imp.errors = append(imp.errors, err)
	}}
	pkg, _ := config.Check(path, imp.fset, files, nil)
	imp.loaded[path] = pkg
	return pkg, nil
}

func relationshipBoundaryParseFiles(fset *token.FileSet, listed *relationshipBoundaryListedPackage, fixture string) ([]*ast.File, error) {
	files := make([]*ast.File, 0, len(listed.GoFiles)+len(listed.CgoFiles)+1)
	for _, name := range append(append([]string{}, listed.GoFiles...), listed.CgoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(listed.Dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	if fixture != "" {
		file, err := parser.ParseFile(fset, "zz_relationship_access_boundary_fixture.go", fixture, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func relationshipBoundaryPackages(t *testing.T) map[string]*relationshipBoundaryListedPackage {
	t.Helper()
	command := exec.Command("go", "list", "-json", "-deps", ".")
	var output, stderr bytes.Buffer
	command.Stdout, command.Stderr = &output, &stderr
	require.NoError(t, command.Run(), stderr.String())
	packages := map[string]*relationshipBoundaryListedPackage{}
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		var listed relationshipBoundaryListedPackage
		require.NoError(t, decoder.Decode(&listed))
		packages[listed.ImportPath] = &listed
	}
	return packages
}

type relationshipBoundaryFunction struct {
	object      *types.Func
	name        string
	position    string
	owner       bool
	adapter     bool
	directIO    bool
	calls       map[*types.Func]bool
	invocations map[*ast.CallExpr]map[*types.Func]bool
	callbacks   map[*ast.CallExpr]map[*types.Func]bool
	body        *ast.BlockStmt
}

func relationshipBoundaryNamedType(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t)
	if pointer, ok := t.(*types.Pointer); ok {
		t = types.Unalias(pointer.Elem())
	}
	if named, ok := t.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/cypher" {
		return named.Obj().Name()
	}
	return ""
}

func relationshipBoundaryFunctionName(function *types.Func) string {
	signature := function.Type().(*types.Signature)
	if signature.Recv() == nil {
		return function.Name()
	}
	return relationshipBoundaryNamedType(signature.Recv().Type()) + "." + function.Name()
}

func relationshipBoundaryStorageAdapter(function *types.Func) bool {
	signature := function.Type().(*types.Signature)
	if signature.Recv() == nil || function.Pkg() == nil {
		return false
	}
	for _, dependency := range function.Pkg().Imports() {
		if dependency.Path() == "github.com/orneryd/nornicdb/pkg/storage" {
			engine := dependency.Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
			return types.Implements(signature.Recv().Type(), engine)
		}
	}
	return false
}

func relationshipStoragePrimitive(function *types.Func) bool {
	if function == nil || function.Pkg() == nil {
		return false
	}
	if function.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/storage" && function.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/search" && !relationshipBoundaryStorageAdapter(function) {
		return false
	}
	name := function.Name()
	if !strings.Contains(name, "Edge") && !strings.Contains(name, "Relationship") && !strings.Contains(name, "Degree") {
		return false
	}
	for _, prefix := range []string{"Get", "All", "Stream", "Seek", "Count", "Has", "Find", "Outgoing", "Incoming", "Incident", "VectorQuery", "EdgeCount", "RelationshipCount"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
func relationshipBoundaryTarget(info *types.Info, expression ast.Expr) *types.Func {
	switch target := expression.(type) {
	case *ast.Ident:
		function, _ := info.Uses[target].(*types.Func)
		return function
	case *ast.SelectorExpr:
		if selection := info.Selections[target]; selection != nil {
			function, _ := selection.Obj().(*types.Func)
			return function
		}
		function, _ := info.Uses[target.Sel].(*types.Func)
		return function
	case *ast.ParenExpr:
		return relationshipBoundaryTarget(info, target.X)
	}
	return nil
}

func relationshipBoundarySource(t *testing.T, fixture string) (*token.FileSet, []*ast.File, *types.Info) {
	t.Helper()
	fset := token.NewFileSet()
	packages := relationshipBoundaryPackages(t)
	listed := packages["github.com/orneryd/nornicdb/pkg/cypher"]
	require.NotNil(t, listed)
	files, err := relationshipBoundaryParseFiles(fset, listed, fixture)
	require.NoError(t, err)
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	imp := &relationshipBoundaryImporter{fset: fset, packages: packages, loaded: map[string]*types.Package{}}
	var typeErrors []error
	config := types.Config{FakeImportC: true, Importer: imp.from(listed), Error: func(err error) { typeErrors = append(typeErrors, err) }}
	_, _ = config.Check(listed.ImportPath, fset, files, info)
	require.Empty(t, append(typeErrors, imp.errors...), "the boundary graph requires a complete compiler call graph")
	return fset, files, info
}

func relationshipBoundaryViolations(t *testing.T, fixture string) []string {
	t.Helper()
	fset, files, info := relationshipBoundarySource(t, fixture)

	functions, structuralViolations := relationshipBoundaryGraph(files, fset, info)

	// Every relationship read is physically implemented by the capability. A
	// storage.Engine implementation is the storage protocol itself, including
	// transaction/snapshot adapters; it must not recursively enter query planning.
	// No call-graph fan-in heuristic can exempt a query-side reader here.
	violations := structuralViolations
	var population []string
	for _, function := range functions {
		if !function.directIO {
			continue
		}
		population = append(population, function.position+" "+function.name)
		if !function.owner && !function.adapter {
			violations = append(violations, fmt.Sprintf("%s: %s reads relationship storage outside relationshipAccessExecution", function.position, function.name))
		}
	}
	sort.Strings(population)
	t.Logf("compiler-resolved relationship storage readers (%d):\n%s", len(population), strings.Join(population, "\n"))
	require.NotEmpty(t, population)
	sort.Strings(violations)
	return violations
}

// Every compiler-resolved relationship storage read belongs to the execution
// capability or to an implementation of the storage protocol. Result types,
// caller counts, clauses and query-reader names cannot exempt a direct read.
func TestRelationshipAccessProgramExecutionBoundary(t *testing.T) {
	require.Empty(t, relationshipBoundaryViolations(t, ""))
}

func TestRelationshipAccessProgramExecutionBoundaryRejectsCapabilityBypass(t *testing.T) {
	fixture := `package cypher
func (e *StorageExecutor) relationshipPathExecutionForFixture(statement admittedRelationshipStatement) relationshipAccessExecution {
 return relationshipAccessExecution{}
}
`
	violations := relationshipBoundaryViolations(t, fixture)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0], "relationshipPathExecutionForFixture constructs relationshipAccessExecution outside the program")
}

func TestRelationshipAccessProgramExecutionBoundaryRejectsStorageBypass(t *testing.T) {
	fixture := `package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) relationshipBoundaryFixture(ctx context.Context, id storage.NodeID) {
	read := e.storage.GetEdge
	_, _ = read(storage.EdgeID(id))
}
`
	violations := relationshipBoundaryViolations(t, fixture)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0], "relationshipBoundaryFixture reads relationship storage outside relationshipAccessExecution")
}

func TestRelationshipAccessProgramExecutionBoundaryStorageSurface(t *testing.T) {
	reads := []string{
		"_, _ = e.storage.GetEdge(edge)",
		"_, _ = e.storage.AllEdges()",
		"_, _ = e.storage.GetEdgesByType(\"T\")",
		"_, _ = e.storage.GetOutgoingEdges(node)",
		"_, _ = e.storage.GetIncomingEdges(node)",
		"_, _ = e.storage.GetEdgesBetween(node, node)",
		"_ = e.storage.GetEdgeBetween(node, node, \"T\")",
		"_ = e.storage.GetInDegree(node)",
		"_ = e.storage.GetOutDegree(node)",
		"_, _ = e.storage.EdgeCount()",
		"_, _ = e.storage.EdgeCountByType(\"T\")",
		"_, _ = e.storage.EdgeCountByStartLabel(\"L\", \"T\")",
		"_, _ = e.storage.EdgeCountByEndLabel(\"L\", \"T\")",
		"_, _ = e.storage.(storage.IncidentEdgeLister).IncidentEdges(node)",
		"_, _, _ = e.storage.(storage.AdjacentEdgesEngine).GetAdjacentEdges(node)",
		"_, _, _ = e.storage.(storage.EdgeHeaderReader).OutgoingEdgeHeaders(node)",
		"_, _, _ = e.storage.(storage.EdgeHeaderReader).IncomingEdgeHeaders(node)",
		"_, _, _ = e.storage.(storage.RelationshipPropertySeeker).SeekRelationships(ctx, \"T\", []string{\"p\"}, []interface{}{1})",
		"_ = storage.StreamEdgesByType(ctx, e.storage, \"T\", func(*storage.Edge) error { return nil })",
		"_ = storage.StreamEdgesWithFallback(ctx, e.storage, 16, func(*storage.Edge) error { return nil })",
	}
	reads = append(reads, "_, _ = e.searchService.VectorQueryRelationships(ctx, []float32{1}, search.RelationshipVectorQuerySpec{})")
	var fixture strings.Builder
	fixture.WriteString("package cypher\nimport (\"context\"; \"github.com/orneryd/nornicdb/pkg/storage\"; \"github.com/orneryd/nornicdb/pkg/search\")\n")
	for index, read := range reads {
		fmt.Fprintf(&fixture, "func(e *StorageExecutor) relationshipPrimitive%d(ctx context.Context, node storage.NodeID, edge storage.EdgeID) *ExecuteResult { %s; return nil }\n", index, read)
	}
	violations := relationshipBoundaryViolations(t, fixture.String())
	require.Len(t, violations, len(reads), "every primitive and every independent caller must be checked")
	for index := range reads {
		require.Contains(t, strings.Join(violations, "\n"), fmt.Sprintf("relationshipPrimitive%d reads relationship storage", index))
	}
}
