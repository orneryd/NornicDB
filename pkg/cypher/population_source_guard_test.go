package cypher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// storageStreamCallSiteExceptions are the calls to a storage stream or
// visitor API outside storage_stream.go, keyed by file, enclosing function
// and API, with the reason each is not a population the executor consumes.
var storageStreamCallSiteExceptions = map[string]string{
	// The transaction's storage adapter implements these APIs for the
	// executor: it hands the caller's callback to the transaction, and the
	// caller's call went through the scan adapter.
	"transaction_storage_wrapper.go:StreamNodesByLabelProjected:StreamNodesByLabelProjected": "storage adapter: forwards its caller's scan callback",
	"transaction_storage_wrapper.go:StreamNodesWithOptions:StreamNodesWithOptions":           "storage adapter: forwards its caller's scan callback",
	"transaction_storage_wrapper.go:ForEachNodeIDByLabel:ForEachNodeIDByLabel":               "storage adapter: forwards its caller's scan callback",
	"transaction_storage_wrapper.go:MatchEdgesBetween:MatchEdgesBetween":                     "storage adapter: forwards its caller's scan callback",
	"transaction_storage_wrapper_edge_stream.go:StreamEdgesByType:StreamEdgesByType":         "storage adapter: forwards its caller's scan callback",
}

// clientSizedAllocationExceptions are the make lengths and capacities and the
// Grow sizes that are neither a
// constant, the length of an existing value, nor clamped by
// util.SafePreallocCap, keyed by file, enclosing function and capacity
// expression, with the bound that keeps each from following client input.
var clientSizedAllocationExceptions = map[string]string{
	// Configured capacities: a cache's shards, the query cache, the worker pool.
	"bounded_cache.go:newBoundedCache:shardLimit / 4 + 1":                        "a cache shard's configured capacity",
	"bounded_cache.go:put:shardLimit / 4 + 1":                                    "a cache shard's configured capacity",
	"merge_semantic_validation.go:newSemanticValidationCache:shardLimit / 4 + 1": "a cache shard's configured capacity",
	"cache.go:NewQueryCache:maxSize":                                             "the query cache's configured size",
	"parallel.go:NewWorkerPool:numWorkers * 2":                                   "the configured worker count",
	"traversal.go:traverseGraphParallel:numWorkers":                              "the configured worker count",
	"uuid_values.go:init:1 + 8 * vector.Dimension()":                             "a fixed vector dimension, at package initialization",

	// Sized by the query text: labels, list literal items, grouping
	// expressions, the variables a list form binds.
	`label_chain.go:parseLabelChain:strings.Count(chain, ":") + 1`:       "the labels written in the query",
	`label_chain.go:setLabelChainItems:strings.Count(chain, ":") + 1`:    "the labels written in the query",
	`label_chain.go:strictNodeHeadLabels:strings.Count(chain, ":") + 1`:  "the labels written in the query",
	"row_operation_values.go:rowListLiteral:count":                       "the items of a list literal written in the query",
	"value_bindings.go:valueBindingsLayer:len(outer) + extra":            "the outer scope plus the variables a nested scope binds",
	"pipeline_aggregate_stream.go:pipelineAggregateGroups:groupingCount": "the grouping expressions written in the query",
	"row_list_loops.go:rowElementScope:len(values) + extra":              "a row plus the variables a list form binds",

	// Sized by a population the statement already holds.
	"parallel.go:parallelFilterNodes:totalSize":         "the workers' results: the population already collected",
	"parallel.go:parallelCollect:totalSize":             "the workers' results: the population already collected",
	"path_selector_search_walk.go:uniformPair:shortest": "one found path's length",
	"path_selector_search_walk.go:walkHops:known.depth": "one found path's length",

	// Sizes the widened guard (make lengths, Grow, products) reads, by bound.
	"apoc_collections.go:apocCollZip:length":                                    "the shorter of two list values: one value's length",
	"call_apoc_path.go:path:step.depth":                                         "one found path's length",
	"call_apoc_path.go:path:step.depth + 1":                                     "one found path's length",
	"comparison_chain.go:splitComparisonChain:scan.count":                       "the comparisons written in the query",
	"comparison_chain.go:splitComparisonChain:scan.count + 1":                   "the comparisons written in the query",
	"executor_show.go:executeShowFunctions:len(row) * (len(cached) - i)":        "the SHOW FUNCTIONS result's remaining cells",
	"executor_show.go:projectShowColumns:len(result.Rows) * len(columns)":       "a SHOW result's cells: its rows times its columns",
	"fastrp.go:generateFastRPEmbeddings:dim":                                    "the validated embeddingDimension (at most maxFastRPEmbeddingDimension)",
	"fastrp.go:initializeEmbeddingChunk:dim":                                    "the validated embeddingDimension (at most maxFastRPEmbeddingDimension)",
	"fastrp.go:generateFastRPEmbeddings:featureCount":                           "the configured feature properties and one property's dimension",
	"fastrp.go:generateFastRPEmbeddings:propDim":                                "the configured feature properties and one property's dimension",
	"fastrp.go:generateFastRPEmbeddings:size":                                   "the projected graph's node count",
	"gds_louvain.go:callGdsLouvainStream:size":                                  "the projected graph's node count",
	"gds_node_similarity.go:nodeSimilarityComponents:size":                      "the projected graph's node count",
	"functions_cypher25.go:stringArguments:count":                               "the arguments written in the call",
	"gds_java_map_order.go:gdsJavaMapOrder:capacity":                            "a power of two at or above the modelled map's initial capacity: 16, its keys or its contexts",
	"merge_context.go:lookupPatternCandidatesUsingPropertyIndex:covered":        "the index properties the pattern writes",
	"parallel.go:parallelCollect:numWorkers":                                    "the configured worker count",
	"parallel.go:parallelCount:numWorkers":                                      "the configured worker count",
	"parallel.go:parallelFilterNodes:numWorkers":                                "the configured worker count",
	"parallel.go:parallelMap:numWorkers":                                        "the configured worker count",
	"parallel.go:parallelSum:numWorkers":                                        "the configured worker count",
	"pipeline_aggregate_stream.go:tryCartesianAggregatePartitions:workers":      "the configured worker count",
	"path_selector_search_run.go:dominate:nodes":                                "the path search's own state (its product graph)",
	"plugin_typed_invocation.go:invokeTypedPlugin:len(args) + offset":           "a plugin call's arguments",
	"point_values.go:init:length":                                               "one encoded value's length (a registered decoder)",
	"temporal_values.go:decodeCypherDateTime:length":                            "one encoded value's length (a registered decoder)",
	"temporal_values.go:registerTemporalTimeExtension:length":                   "one encoded value's length (a registered decoder)",
	"uuid_values.go:init:length":                                                "one encoded value's length (a registered decoder)",
	"row_value_program.go:compile:scan.count + 1":                               "the parts of an expression written in the query",
	"row_value_program.go:rowLoopScope:loop.slots":                              "a compiled expression's loop slots",
	"traversal.go:findPaths:nextEdgesLen":                                       "one path's length plus its next step",
	"traversal.go:findPaths:nextNodesLen":                                       "one path's length plus its next step",
	"vector_similarity_functions.go:similarityVectorArgument:typed.Dimension()": "one vector value's dimension",
	"vector_values.go:coordinates:v.Dimension()":                                "one vector value's dimension",

	// A value a function returns, not a population.
	"row_extension_expression.go:padRowString:length": "the padded string's requested length: the value the function returns",
}

// The population sources, and what is sized from them, are checked at the
// source: every storage stream or visitor call goes through storage_stream.go
// with a callback that observes its scan first, and no allocation takes its
// capacity from a client's LIMIT, SKIP, k or topK. A new consumer of a storage
// population inherits the scan's cancellation however it is written.
func TestStorageStreamCallSites(t *testing.T) {
	pkg := loadCypherPackageForGuard(t, map[string]string{"population_source_guard_fixture.go": populationSourceGuardFixture})
	violations, used := storageStreamCallSiteViolations(pkg, pkg.files)
	require.Empty(t, violations, "storage stream and visitor calls must go through storage_stream.go's adapter with a callback that observes its scan first")
	require.Empty(t, staleGuardExceptions(storageStreamCallSiteExceptions, used), "exceptions that no longer match a call")

	fixtureViolations, _ := storageStreamCallSiteViolations(pkg, pkg.fixtures)
	require.Equal(t, []string{
		"population_source_guard_fixture.go:13 populationSourceGuardFixture: direct call to StreamNodesByLabelProjected",
		"population_source_guard_fixture.go:14 populationSourceGuardFixture: storageScan built without newStorageScan",
		"population_source_guard_fixture.go:15 populationSourceGuardFixture: streamNodesByLabelProjected's callback does not observe scan first",
		"population_source_guard_fixture.go:16 populationSourceGuardFixture: storageScan built without newStorageScan",
		"population_source_guard_fixture.go:17 populationSourceGuardFixture: streamNodesByLabelProjected's callback does not observe scan first",
		"population_source_guard_fixture.go:29 populationSourceGuardFixture: streamNodesByLabelProjected's callback is not a function literal",
		"population_source_guard_fixture.go:38 populationSourceGuardFixture: scan unclosed is not closed by the next statement's defer",
		"population_source_guard_fixture.go:41 populationSourceGuardFixture: direct call to StreamNodesWithOptions",
	}, fixtureViolations)
}

func TestClientSizedAllocations(t *testing.T) {
	pkg := loadCypherPackageForGuard(t, map[string]string{"population_source_guard_fixture.go": populationSourceGuardFixture})
	violations, used := clientSizedAllocationViolations(pkg, pkg.files)
	require.Empty(t, violations, "a make length or capacity, or a Grow size, must be a constant, an existing value's length or util.SafePreallocCap-clamped")
	require.Empty(t, staleGuardExceptions(clientSizedAllocationExceptions, used), "exceptions that no longer match an allocation")

	fixtureViolations, _ := clientSizedAllocationViolations(pkg, pkg.fixtures)
	require.Equal(t, []string{
		"population_source_guard_fixture.go:32 populationSourceGuardFixture: make capacity limit",
		"population_source_guard_fixture.go:35 populationSourceGuardFixture: make capacity limit * 2",
		"population_source_guard_fixture.go:42 populationSourceGuardFixture: make length limit",
		"population_source_guard_fixture.go:43 populationSourceGuardFixture: make capacity len(nodes) * len(nodes)",
		"population_source_guard_fixture.go:44 populationSourceGuardFixture: grow size limit",
		"population_source_guard_fixture.go:46 populationSourceGuardFixture: grow size limit",
	}, fixtureViolations)
}

const cypherPackagePath = "github.com/orneryd/nornicdb/pkg/cypher"

// isPopulationValue reports a slice or array of population elements, or a
// map whose values are population elements.
func isPopulationValue(typ types.Type) bool {
	if typ == nil {
		return false
	}
	switch u := typ.Underlying().(type) {
	case *types.Slice:
		return isPopulationElem(u.Elem(), 0) || isRowValues(u.Elem())
	case *types.Array:
		return isPopulationElem(u.Elem(), 0) || isRowValues(u.Elem())
	case *types.Map:
		return isPopulationElem(u.Elem(), 0)
	}
	return false
}

// isRowValues reports one result row: its values as a list or a map.
func isRowValues(typ types.Type) bool {
	switch u := typ.(type) {
	case *types.Slice:
		_, values := u.Elem().Underlying().(*types.Interface)
		return values
	case *types.Map:
		_, values := u.Elem().Underlying().(*types.Interface)
		return values
	}
	return false
}

// isPopulationElem reports a population's element: a storage entity or ID,
// a row, a binding, a path, or a struct that holds one (or a list of them).
func isPopulationElem(typ types.Type, depth int) bool {
	if depth > 3 {
		return false
	}
	switch u := typ.(type) {
	case *types.Pointer:
		return isPopulationElem(u.Elem(), depth)
	case *types.Named:
		if object := u.Obj(); object.Pkg() != nil {
			switch object.Pkg().Path() + "." + object.Name() {
			case storagePackagePath + ".Node", storagePackagePath + ".Edge", storagePackagePath + ".NodeID", storagePackagePath + ".EdgeID",
				cypherPackagePath + ".pipelineRow", cypherPackagePath + ".binding", cypherPackagePath + ".relationshipBinding", cypherPackagePath + ".PathResult":
				return true
			}
		}
		if _, isStruct := u.Underlying().(*types.Struct); isStruct {
			return isPopulationElem(u.Underlying(), depth+1)
		}
		return false
	case *types.Struct:
		for index := 0; index < u.NumFields(); index++ {
			field := u.Field(index).Type()
			if isPopulationElem(field, depth+1) {
				return true
			}
			if slice, ok := field.Underlying().(*types.Slice); ok && (isPopulationElem(slice.Elem(), depth+1) || isRowValues(slice.Elem())) {
				return true
			}
		}
	}
	return false
}

func guardTypeString(typ types.Type) string {
	return types.TypeString(typ, func(p *types.Package) string {
		if p.Path() == cypherPackagePath {
			return ""
		}
		return p.Name()
	})
}

const populationSourceGuardFixture = `package cypher

import (
	"context"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

func populationSourceGuardFixture(ctx context.Context, reader storage.ProjectedLabelNodeReader, limit int, nodes []*storage.Node) {
	_ = reader.StreamNodesByLabelProjected("L", nil, func(*storage.Node) error { return nil })
	scan := storageScan{}
	_ = streamNodesByLabelProjected(scan, reader, "L", nil, func(*storage.Node) error { return nil })
	other := storageScan{}
	_ = streamNodesByLabelProjected(scan, reader, "L", nil, func(*storage.Node) error {
		if err := other.observe(); err != nil {
			return err
		}
		return nil
	})
	_ = streamNodesByLabelProjected(scan, reader, "L", nil, func(*storage.Node) error {
		if err := scan.observe(); err != nil {
			return err
		}
		return nil
	})
	_ = streamNodesByLabelProjected(scan, reader, "L", nil, fixtureVisit)
	_ = make([]*storage.Node, 0, len(nodes))
	_ = make([]*storage.Node, 0, min(limit, len(nodes)))
	_ = make([]*storage.Node, 0, limit)
	_ = make([]*storage.Node, 0, util.SafePreallocCap(limit))
	_ = make(map[string]int, rowCancellationInterval)
	_ = make(map[string]int, limit*2)
	closed := newStorageScan(ctx)
	defer closed.close()
	unclosed := newStorageScan(ctx)
	_ = unclosed
	var wrapper *transactionStorageWrapper
	_ = wrapper.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { return nil })
	_ = make([]*storage.Node, limit)
	_ = make([]*storage.Node, 0, len(nodes)*len(nodes))
	_ = slices.Grow(nodes, limit)
	var builder strings.Builder
	builder.Grow(limit)
	_ = make([]*storage.Node, 0, len(nodes)*2)
}

func fixtureVisit(*storage.Node) error { return nil }
`

type guardPackage struct {
	fset     *token.FileSet
	info     *types.Info
	files    []*ast.File
	fixtures []*ast.File
}

func loadCypherPackageForGuard(t *testing.T, fixtures map[string]string) *guardPackage {
	t.Helper()
	return loadPackageForGuard(t, ".", cypherPackagePath, fixtures)
}

// loadPackageForGuard type-checks the package at pattern (relative to this
// package's directory), with fixture files added to it.
func loadPackageForGuard(t *testing.T, pattern, importPath string, fixtures map[string]string) *guardPackage {
	t.Helper()
	listing, err := exec.Command("go", "list", "-deps", "-export", "-json=ImportPath,Dir,GoFiles,Export", pattern).Output()
	require.NoError(t, err)
	exports := map[string]string{}
	var self struct {
		ImportPath, Dir, Export string
		GoFiles                 []string
	}
	for decoder := json.NewDecoder(bytes.NewReader(listing)); decoder.More(); {
		require.NoError(t, decoder.Decode(&self))
		exports[self.ImportPath] = self.Export
	}
	require.Equal(t, importPath, self.ImportPath, "go list -deps lists the package itself last")

	pkg := &guardPackage{fset: token.NewFileSet()}
	for _, name := range self.GoFiles {
		file, err := parser.ParseFile(pkg.fset, filepath.Join(self.Dir, name), nil, 0)
		require.NoError(t, err)
		pkg.files = append(pkg.files, file)
	}
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		file, err := parser.ParseFile(pkg.fset, name, fixtures[name], 0)
		require.NoError(t, err)
		pkg.fixtures = append(pkg.fixtures, file)
	}
	pkg.info = &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Implicits: map[ast.Node]types.Object{}}
	config := types.Config{Importer: importer.ForCompiler(pkg.fset, "gc", func(path string) (io.ReadCloser, error) {
		return os.Open(exports[path])
	})}
	_, err = config.Check(self.ImportPath, pkg.fset, append(append([]*ast.File{}, pkg.files...), pkg.fixtures...), pkg.info)
	require.NoError(t, err)
	return pkg
}

func staleGuardExceptions(exceptions map[string]string, used map[string]bool) []string {
	var stale []string
	for key := range exceptions {
		if !used[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	return stale
}

const storagePackagePath = "github.com/orneryd/nornicdb/pkg/storage"

// isStoragePopulationAPI reports a storage function or method that hands its
// caller a population through a callback: a func parameter whose first
// parameter is a storage entity, ID or record (or a list of them).
func isStoragePopulationAPI(fn *types.Func) bool {
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != storagePackagePath {
		return false
	}
	params := fn.Type().(*types.Signature).Params()
	for index := 0; index < params.Len(); index++ {
		callback, ok := params.At(index).Type().Underlying().(*types.Signature)
		if ok && callback.Params().Len() > 0 && isStorageItem(callback.Params().At(0).Type()) {
			return true
		}
	}
	return false
}

func isStorageItem(typ types.Type) bool {
	for {
		switch t := typ.(type) {
		case *types.Pointer:
			typ = t.Elem()
			continue
		case *types.Slice:
			typ = t.Elem()
			continue
		case *types.Named:
			return t.Obj().Pkg() != nil && t.Obj().Pkg().Path() == storagePackagePath
		}
		return false
	}
}

func calledFunc(info *types.Info, call *ast.CallExpr) *types.Func {
	var ident *ast.Ident
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		ident = fun
	case *ast.SelectorExpr:
		ident = fun.Sel
	case *ast.IndexExpr:
		if selector, ok := fun.X.(*ast.SelectorExpr); ok {
			ident = selector.Sel
		} else if name, ok := fun.X.(*ast.Ident); ok {
			ident = name
		}
	}
	if ident == nil {
		return nil
	}
	fn, _ := info.Uses[ident].(*types.Func)
	return fn
}

func storageStreamCallSiteViolations(pkg *guardPackage, files []*ast.File) ([]string, map[string]bool) {
	var violations []string
	used := map[string]bool{}
	streamNames := storagePopulationMethodNames(pkg.info)
	for _, file := range files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			report := func(node ast.Node, format string, args ...interface{}) {
				violations = append(violations, fmt.Sprintf("%s:%d %s: %s", name, pkg.fset.Position(node.Pos()).Line, function.Name.Name, fmt.Sprintf(format, args...)))
			}
			if name != "storage_stream.go" {
				for _, problem := range storageScanLifecycleProblems(pkg.info, function) {
					report(problem.node, "%s", problem.text)
				}
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				fn := calledFunc(pkg.info, call)
				if fn == nil {
					return true
				}
				if (isStoragePopulationAPI(fn) || isStreamAPIMethod(fn, streamNames)) && name != "storage_stream.go" {
					key := name + ":" + function.Name.Name + ":" + fn.Name()
					if _, excepted := storageStreamCallSiteExceptions[key]; excepted {
						used[key] = true
					} else {
						report(call, "direct call to %s", fn.Name())
					}
				}
				if isStorageScanAdapter(fn) && name != "storage_stream.go" {
					if problem := scanCallbackProblem(function, call); problem != "" {
						report(call, "%s's callback %s", fn.Name(), problem)
					}
				}
				return true
			})
		}
	}
	sort.Strings(violations)
	return violations, used
}

// storagePopulationMethodNames are the names of the storage package's
// population methods (isStoragePopulationAPI), on its interfaces and types.
func storagePopulationMethodNames(info *types.Info) map[string]bool {
	names := map[string]bool{}
	var scope *types.Scope
	for _, object := range info.Uses {
		if object != nil && object.Pkg() != nil && object.Pkg().Path() == storagePackagePath {
			scope = object.Pkg().Scope()
			break
		}
	}
	if scope == nil {
		return names
	}
	for _, typeName := range scope.Names() {
		object, ok := scope.Lookup(typeName).(*types.TypeName)
		if !ok {
			continue
		}
		methods := types.NewMethodSet(types.NewPointer(object.Type()))
		for index := 0; index < methods.Len(); index++ {
			if fn, ok := methods.At(index).Obj().(*types.Func); ok && fn.Exported() && isStoragePopulationAPI(fn) {
				names[fn.Name()] = true
			}
		}
	}
	return names
}

// isStreamAPIMethod reports a method of any other type, such as the
// transaction's storage adapter, that implements a storage population method:
// a call through it hands its caller the same population.
func isStreamAPIMethod(fn *types.Func, names map[string]bool) bool {
	signature := fn.Type().(*types.Signature)
	if signature.Recv() == nil || !names[fn.Name()] {
		return false
	}
	params := signature.Params()
	for index := 0; index < params.Len(); index++ {
		callback, ok := params.At(index).Type().Underlying().(*types.Signature)
		if ok && callback.Params().Len() > 0 && isStorageItem(callback.Params().At(0).Type()) {
			return true
		}
	}
	return false
}

type guardProblem struct {
	node ast.Node
	text string
}

// storageScanLifecycleProblems reports a storageScan built other than by
// newStorageScan, and a newStorageScan whose next statement does not defer
// the scan's close (its pooled checkpoint).
func storageScanLifecycleProblems(info *types.Info, function *ast.FuncDecl) []guardProblem {
	var problems []guardProblem
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CompositeLit:
			if named, ok := info.TypeOf(n).(*types.Named); ok && named.Obj().Name() == "storageScan" {
				problems = append(problems, guardProblem{n, "storageScan built without newStorageScan"})
			}
		case *ast.BlockStmt:
			for index, statement := range n.List {
				assign, ok := statement.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					continue
				}
				call, ok := assign.Rhs[0].(*ast.CallExpr)
				if !ok {
					continue
				}
				if callee, ok := call.Fun.(*ast.Ident); !ok || callee.Name != "newStorageScan" {
					continue
				}
				scan, _ := assign.Lhs[0].(*ast.Ident)
				if scan == nil || index+1 >= len(n.List) || !defersClose(n.List[index+1], scan.Name) {
					name := "_"
					if scan != nil {
						name = scan.Name
					}
					problems = append(problems, guardProblem{assign, "scan " + name + " is not closed by the next statement's defer"})
				}
			}
		}
		return true
	})
	return problems
}

func defersClose(statement ast.Stmt, scan string) bool {
	deferred, ok := statement.(*ast.DeferStmt)
	if !ok {
		return false
	}
	selector, ok := deferred.Call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "close" || len(deferred.Call.Args) != 0 {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == scan
}

// isStorageScanAdapter reports one of storage_stream.go's adapters: a
// function whose first parameter is the storageScan.
func isStorageScanAdapter(fn *types.Func) bool {
	if fn.Pkg() == nil || fn.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
		return false
	}
	signature := fn.Type().(*types.Signature)
	if signature.Recv() != nil || signature.Params().Len() == 0 {
		return false
	}
	named, ok := signature.Params().At(0).Type().(*types.Named)
	return ok && named.Obj().Name() == "storageScan"
}

// scanCallbackProblem reports why an adapter call's callback (its last
// argument) does not start by observing the scan the call passes.
func scanCallbackProblem(function *ast.FuncDecl, call *ast.CallExpr) string {
	scan, ok := call.Args[0].(*ast.Ident)
	if !ok {
		return "is given no scan variable"
	}
	var literal *ast.FuncLit
	switch callback := call.Args[len(call.Args)-1].(type) {
	case *ast.FuncLit:
		literal = callback
	case *ast.Ident:
		literal = assignedFuncLit(function, callback.Name)
	}
	if literal == nil {
		return "is not a function literal"
	}
	if len(literal.Body.List) == 0 || !observesScan(literal.Body.List[0], scan.Name) {
		return "does not observe " + scan.Name + " first"
	}
	return ""
}

// assignedFuncLit is the function literal a function assigns to name, when
// that is name's only assignment.
func assignedFuncLit(function *ast.FuncDecl, name string) *ast.FuncLit {
	var literal *ast.FuncLit
	assignments := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for index, left := range assign.Lhs {
			if ident, ok := left.(*ast.Ident); ok && ident.Name == name {
				assignments++
				if index < len(assign.Rhs) {
					literal, _ = assign.Rhs[index].(*ast.FuncLit)
				}
			}
		}
		return true
	})
	if assignments != 1 {
		return nil
	}
	return literal
}

// observesScan reports `if err := scan.observe(); err != nil { ... return }`
// or `if scan.canceled() { ... return }`.
func observesScan(statement ast.Stmt, scan string) bool {
	branch, ok := statement.(*ast.IfStmt)
	if !ok || len(branch.Body.List) == 0 {
		return false
	}
	if _, returns := branch.Body.List[len(branch.Body.List)-1].(*ast.ReturnStmt); !returns {
		return false
	}
	isScanCall := func(expression ast.Expr, method string) bool {
		call, ok := expression.(*ast.CallExpr)
		if !ok {
			return false
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != method {
			return false
		}
		receiver, ok := selector.X.(*ast.Ident)
		return ok && receiver.Name == scan
	}
	if init, ok := branch.Init.(*ast.AssignStmt); ok && len(init.Rhs) == 1 && isScanCall(init.Rhs[0], "observe") {
		return true
	}
	return branch.Init == nil && isScanCall(branch.Cond, "canceled")
}

func clientSizedAllocationViolations(pkg *guardPackage, files []*ast.File) ([]string, map[string]bool) {
	var violations []string
	used := map[string]bool{}
	for _, file := range files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			check := func(call *ast.CallExpr, size ast.Expr, kind string) {
				if size == nil || boundedCapacity(pkg.info, size) {
					return
				}
				key := name + ":" + function.Name.Name + ":" + types.ExprString(size)
				if _, excepted := clientSizedAllocationExceptions[key]; excepted {
					used[key] = true
					return
				}
				violations = append(violations, fmt.Sprintf("%s:%d %s: %s %s", name, pkg.fset.Position(call.Pos()).Line, function.Name.Name, kind, types.ExprString(size)))
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if size := growSize(pkg.info, call); size != nil {
					check(call, size, "grow size")
					return true
				}
				builtin, ok := call.Fun.(*ast.Ident)
				if !ok || builtin.Name != "make" || pkg.info.Uses[builtin] != types.Universe.Lookup("make") {
					return true
				}
				switch pkg.info.TypeOf(call.Args[0]).Underlying().(type) {
				case *types.Slice:
					if len(call.Args) >= 2 {
						check(call, call.Args[1], "make length")
					}
					if len(call.Args) == 3 {
						check(call, call.Args[2], "make capacity")
					}
				case *types.Map, *types.Chan:
					if len(call.Args) == 2 {
						check(call, call.Args[1], "make capacity")
					}
				}
				return true
			})
		}
	}
	sort.Strings(violations)
	return violations, used
}

// growSize is the size argument of slices.Grow or of a strings.Builder or
// bytes.Buffer Grow, or nil for any other call.
func growSize(info *types.Info, call *ast.CallExpr) ast.Expr {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Grow" {
		return nil
	}
	fn, ok := info.Uses[selector.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return nil
	}
	switch fn.Pkg().Path() {
	case "slices":
		if len(call.Args) == 2 {
			return call.Args[1]
		}
	case "strings", "bytes":
		if len(call.Args) == 1 {
			return call.Args[0]
		}
	}
	return nil
}

// boundedCapacity reports a capacity that cannot follow client input: built
// from constants, the length of an existing value (len, cap, a Len method),
// min with one such bound, or util.SafePreallocCap and its kin.
func constantSize(info *types.Info, expression ast.Expr) bool {
	value, ok := info.Types[expression]
	return ok && value.Value != nil
}

func boundedCapacity(info *types.Info, expression ast.Expr) bool {
	switch e := expression.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		_, constant := info.Uses[e].(*types.Const)
		return constant
	case *ast.SelectorExpr:
		_, constant := info.Uses[e.Sel].(*types.Const)
		return constant
	case *ast.ParenExpr:
		return boundedCapacity(info, e.X)
	case *ast.UnaryExpr:
		return boundedCapacity(info, e.X)
	case *ast.BinaryExpr:
		if !boundedCapacity(info, e.X) || !boundedCapacity(info, e.Y) {
			return false
		}
		// A product of two populations is not bounded by either.
		if e.Op == token.MUL || e.Op == token.SHL {
			return constantSize(info, e.X) || constantSize(info, e.Y)
		}
		return true
	case *ast.CallExpr:
		switch fun := e.Fun.(type) {
		case *ast.Ident:
			switch info.Uses[fun] {
			case types.Universe.Lookup("len"), types.Universe.Lookup("cap"):
				return true
			case types.Universe.Lookup("min"):
				for _, argument := range e.Args {
					if boundedCapacity(info, argument) {
						return true
					}
				}
				return false
			}
			if _, conversion := info.Uses[fun].(*types.TypeName); conversion && len(e.Args) == 1 {
				return boundedCapacity(info, e.Args[0])
			}
		case *ast.SelectorExpr:
			if fun.Sel.Name == "Len" && len(e.Args) == 0 {
				return true
			}
			if fn, ok := info.Uses[fun.Sel].(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == "github.com/orneryd/nornicdb/pkg/util" && strings.HasPrefix(fn.Name(), "SafePrealloc") {
				return true
			}
		}
	}
	return false
}
