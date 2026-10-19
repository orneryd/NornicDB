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
	object   *types.Func
	name     string
	position string
	owner    bool
	directIO bool
	calls    map[*types.Func]bool
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

func relationshipStoragePrimitive(function *types.Func) bool {
	if function == nil || function.Pkg() == nil || function.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/storage" {
		return false
	}
	switch function.Name() {
	case "GetEdge", "AllEdges", "StreamEdgesByType", "GetAdjacentEdges", "GetOutgoingEdges", "GetIncomingEdges", "GetAllEdges", "GetEdgesByType", "SeekRelationships", "EdgeCount", "EdgeCountByType", "EdgeCountByStartLabel", "EdgeCountByEndLabel", "RelationshipCount":
		return true
	}
	return strings.HasPrefix(function.Name(), "CountEdges")
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

func relationshipBoundaryViolations(t *testing.T, fixture string) []string {
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

	functions := map[*types.Func]*relationshipBoundaryFunction{}
	declarations := map[*types.Func]*ast.FuncDecl{}
	var structuralViolations []string
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			object, _ := info.Defs[function.Name].(*types.Func)
			require.NotNil(t, object)
			signature := object.Type().(*types.Signature)
			owner := signature.Recv() != nil && relationshipBoundaryNamedType(signature.Recv().Type()) == "relationshipAccessExecution"
			functions[object] = &relationshipBoundaryFunction{object: object, name: relationshipBoundaryFunctionName(object), position: fset.Position(function.Pos()).String(), owner: owner, calls: map[*types.Func]bool{}}
			declarations[object] = function
		}
	}
	for object, function := range functions {
		ast.Inspect(declarations[object].Body, func(node ast.Node) bool {
			if literal, ok := node.(*ast.CompositeLit); ok && relationshipBoundaryNamedType(info.TypeOf(literal)) == "relationshipAccessExecution" && function.name != "StorageExecutor.relationshipExecutionFor" {
				structuralViolations = append(structuralViolations, fmt.Sprintf("%s: %s constructs relationshipAccessExecution outside the program", fset.Position(literal.Pos()), function.name))
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			target := relationshipBoundaryTarget(info, call.Fun)
			if relationshipStoragePrimitive(target) {
				function.directIO = true
			}
			if functions[target] != nil {
				function.calls[target] = true
			}
			return true
		})
	}

	// Compute storage reachability upwards, treating owner methods as graph
	// cuts. There is no result-type cutoff and no maintained reader/type list.
	reads := map[*types.Func]bool{}
	for object, function := range functions {
		reads[object] = function.directIO
	}
	for changed := true; changed; {
		changed = false
		for object, function := range functions {
			if function.owner || reads[object] {
				continue
			}
			for callee := range function.calls {
				if next := functions[callee]; next != nil && !next.owner && reads[callee] {
					reads[object] = true
					changed = true
					break
				}
			}
		}
	}
	reachesOwner := map[*types.Func]bool{}
	for changed := true; changed; {
		changed = false
		for object, function := range functions {
			if reachesOwner[object] {
				continue
			}
			for callee := range function.calls {
				if functions[callee].owner || reachesOwner[callee] {
					reachesOwner[object] = true
					changed = true
					break
				}
			}
		}
	}

	// The owned kernel is derived by walking down from every method of the
	// capability, restricted only by the storage-rooted reachability above.
	kernel := map[*types.Func]bool{}
	var include func(*types.Func)
	include = func(object *types.Func) {
		if object == nil || kernel[object] || !reads[object] {
			return
		}
		kernel[object] = true
		for callee := range functions[object].calls {
			include(callee)
		}
	}
	for _, function := range functions {
		if function.owner {
			for callee := range function.calls {
				include(callee)
			}
		}
	}
	require.NotEmpty(t, kernel, "no relationship storage kernel is owned by the program")

	var population []string
	for object := range kernel {
		population = append(population, functions[object].position+" "+functions[object].name)
	}
	sort.Strings(population)
	t.Logf("program-owned storage-rooted relationship kernels (%d):\n%s", len(population), strings.Join(population, "\n"))

	violations := structuralViolations
	for object, function := range functions {
		if reads[object] && reachesOwner[object] && !function.owner && !kernel[object] {
			violations = append(violations, fmt.Sprintf("%s: %s reaches relationship storage outside relationshipAccessExecution", function.position, function.name))
		}
		for callee := range function.calls {
			if kernel[callee] && !function.owner && !kernel[object] {
				violations = append(violations, fmt.Sprintf("%s: %s bypasses relationshipAccessExecution via %s", function.position, function.name, functions[callee].name))
			}
		}
	}
	sort.Strings(violations)
	return violations
}

// This guard starts at the compiler-resolved storage methods and walks their
// transitive callers. It therefore covers duplicate method names, interface
// dispatch, string-pattern routes and ExecuteResult producers without a list
// of clauses or reader types.
func TestRelationshipAccessProgramExecutionBoundary(t *testing.T) {
	require.Empty(t, relationshipBoundaryViolations(t, ""))
}

func TestRelationshipAccessProgramExecutionBoundaryRejectsStorageBypass(t *testing.T) {
	fixture := `package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) relationshipBoundaryFixture(ctx context.Context, id storage.NodeID) {
	_, _, _ = incidentEdgeListsContext(ctx, e.storage, id, "both")
}
`
	violations := relationshipBoundaryViolations(t, fixture)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0], "relationshipBoundaryFixture bypasses relationshipAccessExecution via incidentEdgeListsContext")
}
