package search

// Vector serving publication: every change to what vector search serves is
// published by one owner and advances IndexRevision after it is visible, so no
// answer computed before the change is served after it. Tests synchronize on
// hooks only: the initial build's race replay runs on its own goroutine, and
// its revision advance must not land between a test's two cached searches.

import (
	"bytes"
	"context"
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
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func servingTestEnv(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
}

func addServingNode(t *testing.T, engine storage.Engine, svc *Service, id string, vec []float32) {
	t.Helper()
	node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"text": "text"}, ChunkEmbeddings: [][]float32{vec}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
}

func servingResultIDs(resp *SearchResponse) []string {
	ids := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		ids = append(ids, r.ID)
	}
	return ids
}

// awaitServingReplays returns a wait for the race replay an initial graph
// build schedules: while one is pending (hnswRebuildInFlight), the wait
// blocks on the replay's completion hook.
func awaitServingReplays(t *testing.T) func(svc *Service) {
	done := make(chan struct{}, 64)
	previous := hnswAfterReplayHook
	hnswAfterReplayHook = func() { done <- struct{}{} }
	t.Cleanup(func() { hnswAfterReplayHook = previous })
	return func(svc *Service) {
		for svc.hnswRebuildInFlight.Load() {
			<-done
		}
	}
}

func servingGraph(svc *Service) *HNSWIndex {
	svc.hnswMu.RLock()
	defer svc.hnswMu.RUnlock()
	return svc.servingHNSW()
}

// TestServingPublicationReplayedBuildRaceIsServed: a vector stored while the
// initial graph was being built reaches the graph only when the build's races
// are replayed after installation. An answer cached between installation and
// replay, from the graph without that vector, must not be served after the
// replay.
func TestServingPublicationReplayedBuildRaceIsServed(t *testing.T) {
	servingTestEnv(t)
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { _ = svc.Close() })
	addServingNode(t, engine, svc, "far1", []float32{-1, 0.1})
	addServingNode(t, engine, svc, "far2", []float32{-1, -0.1})
	addServingNode(t, engine, svc, "mid", []float32{0.1, 1})

	built, release := make(chan struct{}), make(chan struct{})
	var builtOnce, releaseOnce sync.Once
	openInstall := func() { releaseOnce.Do(func() { close(release) }) }
	replayReady, replayRelease, replayed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var replayOnce, replayReleaseOnce, replayedOnce sync.Once
	openReplay := func() { replayReleaseOnce.Do(func() { close(replayRelease) }) }
	previousInstall, previousReplay, previousReplayed := hnswBeforeInstallHook, hnswBeforeReplayHook, hnswAfterReplayHook
	hnswBeforeInstallHook = func() { builtOnce.Do(func() { close(built) }); <-release }
	hnswBeforeReplayHook = func() { replayOnce.Do(func() { close(replayReady) }); <-replayRelease }
	hnswAfterReplayHook = func() { replayedOnce.Do(func() { close(replayed) }) }
	t.Cleanup(func() {
		openInstall()
		openReplay()
		hnswBeforeInstallHook, hnswBeforeReplayHook, hnswAfterReplayHook = previousInstall, previousReplay, previousReplayed
	})

	minimum := -1.0
	opts := func() *SearchOptions { return &SearchOptions{Limit: 1, MinSimilarity: &minimum} }
	first := make(chan error, 1)
	go func() {
		_, err := svc.Search(context.Background(), "", []float32{1, 0}, opts())
		first <- err
	}()
	<-built
	// Stored while the graph is built from its snapshot: recorded for replay.
	addServingNode(t, engine, svc, "new", []float32{1, 0})

	// The replay is parked until an answer is cached from the installed graph
	// that does not yet contain "new".
	openInstall()
	require.NoError(t, <-first)
	<-replayReady
	before, err := svc.Search(context.Background(), "", []float32{1, 0}, opts())
	require.NoError(t, err)
	require.NotContains(t, servingResultIDs(before), "new", "precondition: the installed graph does not contain the raced vector yet")
	openReplay()
	<-replayed

	after, err := svc.Search(context.Background(), "", []float32{1, 0}, opts())
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, servingResultIDs(after), "the answer cached before the replay was served after it")
}

// TestServingPublicationRetiresCachedAnswers: on every path that changes
// what vector search serves, an identical query after the change is not the
// cached response from before it.
func TestServingPublicationRetiresCachedAnswers(t *testing.T) {
	ctx := context.Background()
	graphReady := func(t *testing.T, svc *Service) {
		_, err := svc.getOrCreateHNSWIndex(ctx, 2, nil)
		require.NoError(t, err)
	}
	disabledGPU := func(t *testing.T) *gpu.Manager {
		manager, err := gpu.NewManager(&gpu.Config{Enabled: false})
		require.NoError(t, err)
		return manager
	}
	for _, p := range []struct {
		name    string
		setup   func(t *testing.T, svc *Service)
		publish func(t *testing.T, svc *Service)
	}{
		{name: "rebuild_swap", setup: graphReady, publish: func(t *testing.T, svc *Service) {
			old := servingGraph(svc)
			require.NoError(t, svc.maybeRebuildHNSW(ctx, -1, -1, time.Nanosecond))
			require.NotSame(t, old, servingGraph(svc), "precondition: the rebuild swapped the graph")
		}},
		{name: "runtime_transition_swap", setup: graphReady, publish: func(t *testing.T, svc *Service) {
			graph, err := svc.getOrCreateHNSWIndex(ctx, 2, nil)
			require.NoError(t, err)
			svc.indexMu.Lock()
			svc.applyTransitionSwapLocked(strategyModeHNSW, graph, svc.vectorIndex, nil)
			svc.indexMu.Unlock()
		}},
		// The CPU<->GPU brute switch needs a GPU; the CPU-only lab cannot run
		// it. Its publication is covered by TestServingFieldsAreWrittenOnlyByTheirOwner.
		{name: "enable_clustering", setup: graphReady, publish: func(t *testing.T, svc *Service) {
			svc.EnableClustering(nil, 2)
			svc.SetMinEmbeddingsForClustering(1)
			require.NoError(t, svc.TriggerClustering(ctx))
		}},
		// Replacing the cluster index alone, without a new clustering, retires
		// the cached pipeline that captured the replaced index.
		{name: "enable_clustering_alone", setup: graphReady, publish: func(t *testing.T, svc *Service) {
			svc.EnableClustering(nil, 2)
		}},
		{name: "gpu_manager_change", setup: func(t *testing.T, svc *Service) {
			svc.SetGPUManager(disabledGPU(t))
		}, publish: func(t *testing.T, svc *Service) {
			svc.SetGPUManager(disabledGPU(t))
		}},
		{name: "clear", publish: func(t *testing.T, svc *Service) {
			require.NoError(t, svc.ClearVectorIndex())
		}},
	} {
		t.Run(p.name, func(t *testing.T) {
			servingTestEnv(t)
			awaitReplays := awaitServingReplays(t)
			engine := newNamespacedEngine(t)
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { _ = svc.Close() })
			for i, v := range [][]float32{{1, 0.1}, {-1, -0.1}, {0.1, 1}, {-0.1, -1}} {
				addServingNode(t, engine, svc, fmt.Sprintf("n%d", i), v)
			}
			require.NoError(t, svc.BuildIndexes(ctx))
			if p.setup != nil {
				p.setup(t, svc)
			}
			minimum := -1.0
			opts := &SearchOptions{Limit: 2, MinSimilarity: &minimum}
			// A first search may build the graph; its replay advances the
			// revision and must finish before the cached precondition.
			_, err := svc.Search(ctx, "", []float32{1, 1}, opts)
			require.NoError(t, err)
			awaitReplays(svc)
			before, err := svc.Search(ctx, "", []float32{1, 1}, opts)
			require.NoError(t, err)
			again, err := svc.Search(ctx, "", []float32{1, 1}, opts)
			require.NoError(t, err)
			require.Same(t, before, again, "precondition: the answer is cached")
			p.publish(t, svc)
			after, err := svc.Search(ctx, "", []float32{1, 1}, opts)
			require.NoError(t, err)
			require.NotSame(t, before, after, "a cached answer outlived %s", p.name)
		})
	}
}

// vectorServingFields are the structures whose change alters what vector
// search serves outside the cluster generation, which has its own owner and
// guard (cluster_generation_owner_test.go).
var vectorServingFields = map[string]bool{
	"hnswIndex": true, "vectorPipeline": true, "ivfpqIndex": true, "gpuEmbeddingIndex": true,
}

// vectorServingOwners write the serving fields: publication (which advances
// IndexRevision) and the cache of the pipeline derived from the current state
// (which changes nothing a search returns).
var vectorServingOwners = map[string]bool{"publishVectorServing": true, "cacheDerivedVectorPipelineLocked": true}

// vectorServingAccessors read the serving fields for everything else. Each is
// exactly `return s.<field>`.
var vectorServingAccessors = map[string]bool{
	"servingHNSW": true, "servingPipeline": true, "servingIVFPQ": true, "servingGPUIndex": true,
}

// vectorServingHelpers write serving fields on an owner's behalf. They may be
// called only from an owner's install or another helper.
var vectorServingHelpers = map[string]bool{"swapTransitionServingLocked": true}

// vectorServingConstructors allocate a Service; a Service value may appear
// only there.
var vectorServingConstructors = map[string]bool{"NewServiceWithDimensionsAndBM25EngineAndOptions": true}

// servingListedPkg is one package as `go list -json` reports it.
type servingListedPkg struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	IgnoredGoFiles []string
	ImportMap      map[string]string
}

// servingImporter type-checks dependencies from source, exported API only
// (IgnoreFuncBodies): the guard needs their types, not their bodies.
type servingImporter struct {
	fset   *token.FileSet
	listed map[string]*servingListedPkg
	loaded map[string]*types.Package
	errs   []error
}

func (imp *servingImporter) Import(path string) (*types.Package, error) {
	if path == "unsafe" {
		return types.Unsafe, nil
	}
	if pkg := imp.loaded[path]; pkg != nil {
		return pkg, nil
	}
	listed := imp.listed[path]
	if listed == nil {
		return nil, fmt.Errorf("%s: not in go list -deps", path)
	}
	files, err := parseServingFiles(imp.fset, listed)
	if err != nil {
		return nil, err
	}
	conf := types.Config{IgnoreFuncBodies: true, FakeImportC: true, Importer: imp.from(listed),
		Error: func(err error) { imp.errs = append(imp.errs, err) }}
	pkg, _ := conf.Check(path, imp.fset, files, nil)
	imp.loaded[path] = pkg
	return pkg, nil
}

// from resolves imports as written in listed (vendored standard packages).
func (imp *servingImporter) from(listed *servingListedPkg) types.Importer {
	return servingImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[path]; ok {
			path = mapped
		}
		return imp.Import(path)
	})
}

type servingImportFunc func(path string) (*types.Package, error)

func (f servingImportFunc) Import(path string) (*types.Package, error) { return f(path) }

func parseServingFiles(fset *token.FileSet, listed *servingListedPkg) ([]*ast.File, error) {
	var files []*ast.File
	for _, name := range append(append([]string{}, listed.GoFiles...), listed.CgoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(listed.Dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

// servingTypedPkg is a package type-checked without a single error: an
// unresolved expression could hide a selection from the guard.
type servingTypedPkg struct {
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

func checkServingPkg(t *testing.T, imp *servingImporter, path string, files []*ast.File, importer types.Importer) *servingTypedPkg {
	t.Helper()
	var errs []error
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
		Instances: map[*ast.Ident]types.Instance{},
	}
	conf := types.Config{Importer: importer, FakeImportC: true, Error: func(err error) { errs = append(errs, err) }}
	pkg, _ := conf.Check(path, imp.fset, files, info)
	require.Empty(t, append(errs, imp.errs...), "type-check %s", path)
	return &servingTypedPkg{pkg: pkg, files: files, info: info}
}

// servingGuard resolves the serving fields and their owners by object
// identity, so aliases, embedding, generics and shadowing name the same field.
type servingGuard struct {
	info         *types.Info
	service      *types.Named
	fields       map[*types.Var]string
	owner        *types.Func
	bodies       map[*types.Func]bool // owners, accessors and helpers
	accessors    map[*types.Func]bool
	helpers      map[*types.Func]bool
	constructors map[*types.Func]bool
	out          map[string]bool
}

func newServingGuard(t *testing.T, p *servingTypedPkg) *servingGuard {
	t.Helper()
	obj, ok := p.pkg.Scope().Lookup("Service").(*types.TypeName)
	require.True(t, ok, "Service type")
	named := obj.Type().(*types.Named)
	g := &servingGuard{info: p.info, service: named, fields: map[*types.Var]string{},
		bodies: map[*types.Func]bool{}, accessors: map[*types.Func]bool{}, helpers: map[*types.Func]bool{},
		constructors: map[*types.Func]bool{}, out: map[string]bool{}}
	st := named.Underlying().(*types.Struct)
	for i := 0; i < st.NumFields(); i++ {
		if vectorServingFields[st.Field(i).Name()] {
			g.fields[st.Field(i)] = st.Field(i).Name()
		}
	}
	require.Len(t, g.fields, len(vectorServingFields), "every serving field is a field of Service")
	for i := 0; i < named.NumMethods(); i++ {
		m := named.Method(i)
		switch {
		case vectorServingOwners[m.Name()]:
			g.bodies[m] = true
			if m.Name() == "publishVectorServing" {
				g.owner = m
			}
		case vectorServingAccessors[m.Name()]:
			g.bodies[m], g.accessors[m] = true, true
		case vectorServingHelpers[m.Name()]:
			g.bodies[m], g.helpers[m] = true, true
		}
	}
	require.Len(t, g.bodies, len(vectorServingOwners)+len(vectorServingAccessors)+len(vectorServingHelpers), "every owner, accessor and helper is a method of Service")
	for name := range vectorServingConstructors {
		fn, ok := p.pkg.Scope().Lookup(name).(*types.Func)
		require.True(t, ok, "constructor %s", name)
		g.constructors[fn] = true
	}
	return g
}

func (g *servingGuard) report(where, format string, args ...any) {
	g.out[where+": "+fmt.Sprintf(format, args...)] = true
}

// holdsService reports whether t has Service's representation (its underlying
// struct, as a distinct defined type does), or holds one by value (struct
// fields, arrays): memory a pointer to t can overwrite.
func (g *servingGuard) holdsService(t types.Type, seen map[types.Type]bool) bool {
	if types.Identical(t.Underlying(), g.service.Underlying()) {
		return true
	}
	if seen[t] {
		return false
	}
	seen[t] = true
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if g.holdsService(u.Field(i).Type(), seen) {
				return true
			}
		}
	case *types.Array:
		return g.holdsService(u.Elem(), seen)
	}
	return false
}

// exposesService reports whether a value of type t is, or points to, memory
// holding Service's representation.
func (g *servingGuard) exposesService(t types.Type) bool {
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	return g.holdsService(t, map[types.Type]bool{})
}

// reachesService reports whether t reaches Service through any composite or
// pointer: a generic instantiated with it could write a Service unseen.
func (g *servingGuard) reachesService(t types.Type, seen map[types.Type]bool) bool {
	if types.Identical(t, g.service) {
		return true
	}
	if seen[t] {
		return false
	}
	seen[t] = true
	switch u := t.Underlying().(type) {
	case *types.Pointer:
		return g.reachesService(u.Elem(), seen)
	case *types.Slice:
		return g.reachesService(u.Elem(), seen)
	case *types.Array:
		return g.reachesService(u.Elem(), seen)
	case *types.Chan:
		return g.reachesService(u.Elem(), seen)
	case *types.Map:
		return g.reachesService(u.Key(), seen) || g.reachesService(u.Elem(), seen)
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if g.reachesService(u.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

// callee is the function a call statically invokes, or nil.
func (g *servingGuard) callee(call *ast.CallExpr) *types.Func {
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.SelectorExpr:
		if sel := g.info.Selections[fun]; sel != nil {
			fn, _ := sel.Obj().(*types.Func)
			return fn
		}
	case *ast.Ident:
		fn, _ := g.info.Uses[fun].(*types.Func)
		return fn
	}
	return nil
}

// servingScope is the code a node executes in: a declared function, a package
// initializer, or a function literal. Only an owner, accessor or helper body,
// or a literal passed directly as publishVectorServing's argument, may touch
// the serving state. A literal nested in either is a scope of its own, since
// it can run after the publication.
type servingScope struct {
	where       string
	mayTouch    bool
	constructor bool
}

func (g *servingGuard) visit(root ast.Node, scope servingScope) {
	ownerArgs := map[ast.Expr]bool{}
	helperCalls := map[*ast.SelectorExpr]bool{}
	goCalls := map[*ast.CallExpr]bool{}
	ast.Inspect(root, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.FuncLit:
			g.visit(x.Body, servingScope{where: scope.where, mayTouch: ownerArgs[x], constructor: scope.constructor})
			return false
		case *ast.GoStmt:
			goCalls[x.Call] = true
		case *ast.CallExpr:
			fn := g.callee(x)
			if fn != nil && fn == g.owner {
				for _, arg := range x.Args {
					ownerArgs[ast.Unparen(arg)] = true
				}
			}
			if fn != nil && g.helpers[fn] && scope.mayTouch && !goCalls[x] {
				if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok {
					helperCalls[sel] = true
				}
			}
			// A conversion re-types storage: (*mirror)(s), unsafe.Pointer(s) or
			// back exposes the same Service memory under another type.
			if tv, ok := g.info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 && !scope.constructor {
				from := g.info.TypeOf(x.Args[0])
				if !types.Identical(tv.Type, from) && (g.exposesService(tv.Type) || g.exposesService(from)) {
					g.report(scope.where, "converts Service memory to another type")
				}
			}
		case *ast.SelectorExpr:
			sel := g.info.Selections[x]
			if sel == nil {
				break
			}
			if field, ok := sel.Obj().(*types.Var); ok && g.fields[field] != "" && !scope.mayTouch {
				g.report(scope.where, "mentions %s", g.fields[field])
			}
			if fn, ok := sel.Obj().(*types.Func); ok && g.helpers[fn] && !ownerArgs[x] && !helperCalls[x] {
				g.report(scope.where, "uses %s outside an owner", fn.Name())
			}
		case *ast.AssignStmt:
			if x.Tok == token.ASSIGN && !scope.constructor {
				for _, lhs := range x.Lhs {
					if t := g.info.TypeOf(lhs); t != nil && (types.Identical(t, g.service) || types.Identical(t, types.NewPointer(g.service))) {
						g.report(scope.where, "assigns a Service")
					}
				}
			}
		case *ast.InterfaceType:
			for _, method := range x.Methods.List {
				for _, name := range method.Names {
					if vectorServingHelpers[name.Name] {
						g.report(scope.where, "declares an interface reaching %s", name.Name)
					}
				}
			}
		case *ast.Ident:
			if inst, ok := g.info.Instances[x]; ok {
				for i := 0; i < inst.TypeArgs.Len(); i++ {
					if g.reachesService(inst.TypeArgs.At(i), map[types.Type]bool{}) {
						g.report(scope.where, "instantiates %s with Service", x.Name)
					}
				}
			}
		}
		if expr, ok := node.(ast.Expr); ok && !scope.constructor {
			if tv, ok := g.info.Types[expr]; ok && !tv.IsType() && types.Identical(tv.Type, g.service) {
				g.report(scope.where, "uses a Service value")
			}
		}
		return true
	})
}

// check visits every declaration: function bodies, package-level variable
// initializers and type declarations.
func (g *servingGuard) check(t *testing.T, p *servingTypedPkg) []string {
	t.Helper()
	for _, file := range p.files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				fn := g.info.Defs[d.Name].(*types.Func)
				if g.accessors[fn] {
					g.checkAccessor(d)
				}
				g.visit(d, servingScope{where: d.Name.Name, mayTouch: g.bodies[fn], constructor: g.constructors[fn]})
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.ValueSpec:
						for _, value := range s.Values {
							g.visit(value, servingScope{where: "var " + s.Names[0].Name})
						}
					case *ast.TypeSpec:
						g.visit(s.Type, servingScope{where: "type " + s.Name.Name})
					}
				}
			}
		}
	}
	out := make([]string, 0, len(g.out))
	for problem := range g.out {
		out = append(out, problem)
	}
	sort.Strings(out)
	return out
}

// checkAccessor requires an accessor body to be exactly `return s.<field>`.
func (g *servingGuard) checkAccessor(d *ast.FuncDecl) {
	if d.Body != nil && len(d.Body.List) == 1 {
		if ret, ok := d.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
			if sel, ok := ret.Results[0].(*ast.SelectorExpr); ok {
				if recv, ok := sel.X.(*ast.Ident); ok && len(d.Recv.List[0].Names) == 1 && d.Recv.List[0].Names[0].Name == recv.Name {
					if field, ok := g.info.ObjectOf(sel.Sel).(*types.Var); ok && g.fields[field] != "" {
						return
					}
				}
			}
		}
	}
	g.report(d.Name.Name, "is an accessor that is not `return s.<field>`")
}

// servingGuardFixture is a package of its own, type-checked like pkg/search.
// Every leak* declaration, and the malformed accessor servingGPUIndex, must be
// reported; nothing else may be.
const servingGuardFixture = `package fixture

import "unsafe"

type HNSWIndex struct{ nodes map[int]*HNSWIndex }

type Service struct {
	hnswIndex, ivfpqIndex, gpuEmbeddingIndex, vectorPipeline *HNSWIndex
}

func NewServiceWithDimensionsAndBM25EngineAndOptions() *Service {
	s := &Service{hnswIndex: &HNSWIndex{}}
	s.publishVectorServing(func() { s.hnswIndex = nil }, false)
	return s
}

func (s *Service) publishVectorServing(install func(), retire bool) { install(); s.vectorPipeline = nil }
func (s *Service) cacheDerivedVectorPipelineLocked(p *HNSWIndex)     { s.vectorPipeline = p }
func (s *Service) servingHNSW() *HNSWIndex                           { return s.hnswIndex }
func (s *Service) servingPipeline() *HNSWIndex                       { return s.vectorPipeline }
func (s *Service) servingIVFPQ() *HNSWIndex                 { return s.ivfpqIndex }
func (s *Service) servingGPUIndex() *HNSWIndex              { s.gpuEmbeddingIndex = nil; return s.gpuEmbeddingIndex }
func (s *Service) swapTransitionServingLocked()             { s.hnswIndex = nil }

type gen struct{ hnswIndex *HNSWIndex }
type serviceAlias = Service
type servingWrapper struct{ *Service }
type box[T any] struct{ v T }
type other struct{}
type servingMirror Service
type serviceHolder struct{ s Service }
type holderMirror serviceHolder
type genMirror gen

func (other) publishVectorServing(install func(), retire bool) { install() }

func resetGraph(p **HNSWIndex)            { *p = nil }
func zero[T any](p *T)                    { var z T; *p = z }

func (s *Service) leakWrite()                     { s.vectorPipeline = nil }
func (s *Service) leakElement()                   { s.hnswIndex.nodes[0] = nil }
func (s *Service) leakDelete()                    { delete(s.hnswIndex.nodes, 0) }
func (s *Service) leakRead() *HNSWIndex           { return s.ivfpqIndex }
func (s *Service) leakAddress()                   { resetGraph(&s.hnswIndex) }
func (s *Service) leakAlias()                     { m := s.hnswIndex; delete(m.nodes, 0) }
func (s *Service) leakParen()                     { (*s).hnswIndex = nil }
func (s *Service) leakReset()                     { *s = Service{} }
func (s *Service) leakCopy(o *Service)            { *s = *o }
func leakWrapped(o *Service) *Service             { s := &Service{}; s = o; s.ivfpqIndex = nil; return s }
func leakRebound(o *Service)                      { o = &Service{}; _ = o }
func (s *Service) leakHelperValue()               { f := s.swapTransitionServingLocked; f() }
func (s *Service) leakHelperCall()                { s.swapTransitionServingLocked() }
func (s *Service) leakCacheArg()                  { s.cacheDerivedVectorPipelineLocked(s.vectorPipeline) }
func servingHNSW(s *Service) *HNSWIndex           { return s.hnswIndex }
func leakShadow(g *gen, s *Service)               { { g := s; g.hnswIndex = nil } }
func leakShadowCopy(g *gen, s, o *Service)        { { g := s; *g = *o } }
func leakTypeAlias(s *serviceAlias)               { s.hnswIndex = nil }
func leakEmbedded(w *servingWrapper)              { w.hnswIndex = nil }
func (w *servingWrapper) leakEmbeddedMethod()     { w.ivfpqIndex = nil }
func leakGenericField(b *box[*Service])           { b.v.hnswIndex = nil }
func leakGenericWrite(s *Service)                 { zero(s) }
func leakFakeOwner(o other, s *Service)           { o.publishVectorServing(func() { s.hnswIndex = nil }, false) }
func leakUnsafe(s *Service)                       { *(**HNSWIndex)(unsafe.Pointer(s)) = nil }
func leakMirror(s *Service)                       { *(*servingMirror)(s) = servingMirror{} }
func leakMirrorHolder(h *serviceHolder)           { *(*holderMirror)(h) = holderMirror{} }
func unrelatedConversion(g *gen) *genMirror       { return (*genMirror)(g) }

var leakPackageFunc = func(s *Service) { s.hnswIndex = nil }
var leakPackageHelper = (*Service).swapTransitionServingLocked

type leakHelperInterface interface{ swapTransitionServingLocked() }

func (s *Service) leakDelayed() {
	var late func()
	s.publishVectorServing(func() { late = func() { s.hnswIndex = nil } }, false)
	late()
}

func (s *Service) leakSavedHelper() {
	var late func()
	s.publishVectorServing(func() { late = s.swapTransitionServingLocked }, false)
	late()
}

func (s *Service) leakGoroutine() {
	s.publishVectorServing(func() { go s.swapTransitionServingLocked(); go func() { s.hnswIndex = nil }() }, false)
}

func (s *Service) leakEscapingHelperClosure() func() {
	return func() { s.swapTransitionServingLocked() }
}

func (g *gen) unrelated() *HNSWIndex   { g.hnswIndex = nil; return g.hnswIndex }
func (g *gen) unrelatedReset()          { *g = gen{} }
func (s *Service) viaAccessor() int     { _ = s.servingHNSW(); return len(s.servingHNSW().nodes) }
func (w *servingWrapper) viaWrapper()   { w.publishVectorServing(func() { w.hnswIndex = nil }, true) }
func (s *Service) viaMethodValue()      { s.publishVectorServing(s.swapTransitionServingLocked, false) }
func (s *Service) viaOwner() {
	s.publishVectorServing(func() {
		s.hnswIndex = nil
		delete(s.hnswIndex.nodes, 1)
		s.swapTransitionServingLocked()
		defer s.swapTransitionServingLocked()
	}, true)
}
`

func TestServingFieldsAreWrittenOnlyByTheirOwner(t *testing.T) {
	listing, err := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,CgoFiles,IgnoredGoFiles,ImportMap", ".").Output()
	require.NoError(t, err)
	imp := &servingImporter{fset: token.NewFileSet(), listed: map[string]*servingListedPkg{}, loaded: map[string]*types.Package{}}
	var self *servingListedPkg
	for dec := json.NewDecoder(bytes.NewReader(listing)); dec.More(); {
		listed := &servingListedPkg{}
		require.NoError(t, dec.Decode(listed))
		imp.listed[listed.ImportPath] = listed
		self = listed // -deps lists the package itself last
	}
	require.Equal(t, "github.com/orneryd/nornicdb/pkg/search", self.ImportPath)

	fixtureFile, err := parser.ParseFile(imp.fset, "fixture.go", servingGuardFixture, 0)
	require.NoError(t, err)
	fixture := checkServingPkg(t, imp, "fixture", []*ast.File{fixtureFile}, imp.from(self))
	require.Equal(t, []string{
		"leakAddress: mentions hnswIndex",
		"leakAlias: mentions hnswIndex",
		"leakCacheArg: mentions vectorPipeline",
		"leakCopy: assigns a Service",
		"leakCopy: uses a Service value",
		"leakDelayed: mentions hnswIndex",
		"leakDelete: mentions hnswIndex",
		"leakElement: mentions hnswIndex",
		"leakEmbedded: mentions hnswIndex",
		"leakEmbeddedMethod: mentions ivfpqIndex",
		"leakEscapingHelperClosure: uses swapTransitionServingLocked outside an owner",
		"leakFakeOwner: mentions hnswIndex",
		"leakGenericField: instantiates box with Service",
		"leakGenericField: mentions hnswIndex",
		"leakGenericWrite: instantiates zero with Service",
		"leakGoroutine: mentions hnswIndex",
		"leakGoroutine: uses swapTransitionServingLocked outside an owner",
		"leakHelperCall: uses swapTransitionServingLocked outside an owner",
		"leakHelperValue: uses swapTransitionServingLocked outside an owner",
		"leakMirror: converts Service memory to another type",
		"leakMirrorHolder: converts Service memory to another type",
		"leakParen: mentions hnswIndex",
		"leakParen: uses a Service value",
		"leakRead: mentions ivfpqIndex",
		"leakRebound: assigns a Service",
		"leakRebound: uses a Service value",
		"leakReset: assigns a Service",
		"leakReset: uses a Service value",
		"leakSavedHelper: uses swapTransitionServingLocked outside an owner",
		"leakShadow: mentions hnswIndex",
		"leakShadowCopy: assigns a Service",
		"leakShadowCopy: uses a Service value",
		"leakTypeAlias: mentions hnswIndex",
		"leakUnsafe: converts Service memory to another type",
		"leakWrapped: assigns a Service",
		"leakWrapped: mentions ivfpqIndex",
		"leakWrapped: uses a Service value",
		"leakWrite: mentions vectorPipeline",
		"servingGPUIndex: is an accessor that is not `return s.<field>`",
		"servingHNSW: mentions hnswIndex",
		"type leakHelperInterface: declares an interface reaching swapTransitionServingLocked",
		"var leakPackageFunc: mentions hnswIndex",
		"var leakPackageHelper: uses swapTransitionServingLocked outside an owner",
	}, newServingGuard(t, fixture).check(t, fixture), "negative fixture: every leak is reported and nothing else")

	files, err := parseServingFiles(imp.fset, self)
	require.NoError(t, err)
	search := checkServingPkg(t, imp, self.ImportPath, files, imp.from(self))
	problems := newServingGuard(t, search).check(t, search)

	// Files this build excludes are not type-checked; none may touch the
	// serving state by name, or the guard must be run under their tags.
	for _, name := range self.IgnoredGoFiles {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(imp.fset, filepath.Join(self.Dir, name), nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			if sel, ok := node.(*ast.SelectorExpr); ok && (vectorServingFields[sel.Sel.Name] || vectorServingHelpers[sel.Sel.Name]) {
				problems = append(problems, fmt.Sprintf("%s: build-excluded file names %s", name, sel.Sel.Name))
			}
			return true
		})
	}
	require.Empty(t, problems, "serving state touched outside publishVectorServing, the derived-pipeline cache, the accessors and helpers")
}
