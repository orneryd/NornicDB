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
	// Collapse the compiler graph's strongly connected components, then grow
	// the kernel only through components whose every production predecessor is
	// already owned. Shared expression/procedure dispatch therefore forms a
	// mechanical boundary, while an exclusive storage reader cannot acquire a
	// new caller without failing this guard.
	indexes, low := map[*types.Func]int{}, map[*types.Func]int{}
	onStack := map[*types.Func]bool{}
	var stack []*types.Func
	var components [][]*types.Func
	nextIndex := 1
	var strongConnect func(*types.Func)
	strongConnect = func(object *types.Func) {
		indexes[object], low[object] = nextIndex, nextIndex
		nextIndex++
		stack = append(stack, object)
		onStack[object] = true
		for callee := range functions[object].calls {
			if !reads[callee] || functions[callee].owner {
				continue
			}
			if indexes[callee] == 0 {
				strongConnect(callee)
				if low[callee] < low[object] {
					low[object] = low[callee]
				}
			} else if onStack[callee] && indexes[callee] < low[object] {
				low[object] = indexes[callee]
			}
		}
		if low[object] != indexes[object] {
			return
		}
		var component []*types.Func
		for {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[last] = false
			component = append(component, last)
			if last == object {
				break
			}
		}
		components = append(components, component)
	}
	for object, function := range functions {
		if reads[object] && !function.owner && indexes[object] == 0 {
			strongConnect(object)
		}
	}
	componentOf := map[*types.Func]int{}
	for component, members := range components {
		for _, member := range members {
			componentOf[member] = component
		}
	}
	predecessors := make([]map[int]bool, len(components))
	ownerPredecessor := make([]bool, len(components))
	for component := range components {
		predecessors[component] = map[int]bool{}
	}
	for caller, function := range functions {
		for callee := range function.calls {
			component, present := componentOf[callee]
			if !present {
				continue
			}
			if function.owner {
				ownerPredecessor[component] = true
				continue
			}
			if predecessor, ok := componentOf[caller]; ok && predecessor != component {
				predecessors[component][predecessor] = true
			}
		}
	}
	included := make([]bool, len(components))
	for changed := true; changed; {
		changed = false
		for component := range components {
			if included[component] {
				continue
			}
			reached, ready := ownerPredecessor[component], true
			for predecessor := range predecessors[component] {
				if included[predecessor] {
					reached = true
				} else {
					ready = false
				}
			}
			if reached && ready {
				included[component] = true
				changed = true
			}
		}
	}
	kernel := map[*types.Func]bool{}
	for component, members := range components {
		if included[component] {
			for _, member := range members {
				kernel[member] = true
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
	for component := range components {
		if included[component] {
			continue
		}
		reached := ownerPredecessor[component]
		var outside []int
		for predecessor := range predecessors[component] {
			if included[predecessor] {
				reached = true
			} else {
				outside = append(outside, predecessor)
			}
		}
		if !reached || len(outside) != 1 {
			continue
		}
		for _, caller := range components[outside[0]] {
			for callee := range functions[caller].calls {
				if componentOf[callee] == component {
					violations = append(violations, fmt.Sprintf("%s: %s bypasses relationshipAccessExecution via %s", functions[caller].position, functions[caller].name, functions[callee].name))
				}
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
