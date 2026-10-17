package search

// Vector serving publication: every
// change to what vector search serves is published by one owner and advances
// IndexRevision after it is visible, so no answer computed before the change
// is served after it; and the build's relationship stream never overtakes a
// live edge write.

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
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// lifecycleGateEngine parks the storage reads a lifecycle race depends on,
// only when the targeted caller is on the stack.
type lifecycleGateEngine struct {
	*storage.MemoryEngine
	mu    sync.Mutex
	gates map[string]*lifecycleGate
}

type lifecycleGate struct {
	caller           string
	entered, release chan struct{}
	enterOnce        sync.Once
	releaseOnce      sync.Once
}

func newLifecycleGateEngine(t *testing.T) *lifecycleGateEngine {
	t.Helper()
	e := &lifecycleGateEngine{MemoryEngine: storage.NewMemoryEngine(), gates: map[string]*lifecycleGate{}}
	t.Cleanup(func() { e.releaseAll(); _ = e.MemoryEngine.Close() })
	return e
}

func (e *lifecycleGateEngine) arm(name, caller string) *lifecycleGate {
	g := &lifecycleGate{caller: caller, entered: make(chan struct{}), release: make(chan struct{})}
	e.mu.Lock()
	e.gates[name] = g
	e.mu.Unlock()
	return g
}

func (g *lifecycleGate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

func (e *lifecycleGateEngine) releaseAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, g := range e.gates {
		g.open()
	}
}

func callerOnStack(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

func (e *lifecycleGateEngine) park(name string) {
	e.mu.Lock()
	g := e.gates[name]
	e.mu.Unlock()
	if g == nil || !callerOnStack(g.caller) {
		return
	}
	parked := false
	g.enterOnce.Do(func() { parked = true; close(g.entered) })
	if parked {
		<-g.release
	}
}

// BatchGetNodesWithoutEmbeddings parks a search after scoring, while it
// materializes results and before it caches its answer.
func (e *lifecycleGateEngine) BatchGetNodesWithoutEmbeddings(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	e.park("results")
	return e.MemoryEngine.BatchGetNodesWithoutEmbeddings(ids)
}

// StreamEdges reads a snapshot of the stored edges, then parks before
// applying it: a snapshot iterator that a live write can overtake.
func (e *lifecycleGateEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	var snapshot []*storage.Edge
	if err := e.MemoryEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		copied := *edge
		copied.Properties = make(map[string]any, len(edge.Properties))
		for k, v := range edge.Properties {
			copied.Properties[k] = v
		}
		snapshot = append(snapshot, &copied)
		return nil
	}); err != nil {
		return err
	}
	e.park("edges")
	for _, edge := range snapshot {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not happen", what)
	}
}

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

// TestServingPublicationStaleAnswerIsNotServed: a search that scored with the
// exact fallback before the background graph was published, and caches its
// answer after the publication, must not have that answer served afterwards.
func TestServingPublicationStaleAnswerIsNotServed(t *testing.T) {
	servingTestEnv(t)
	engine := newLifecycleGateEngine(t)
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { _ = svc.Close() })
	addServingNode(t, engine, svc, "nornic:near", []float32{1, 0.1})
	addServingNode(t, engine, svc, "nornic:far", []float32{-1, -0.1})
	buildStarted, buildRelease := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	openBuild := func() { releaseOnce.Do(func() { close(buildRelease) }) }
	previous := hnswBeforeInstallHook
	hnswBeforeInstallHook = func(context.Context) { startOnce.Do(func() { close(buildStarted) }); <-buildRelease }
	t.Cleanup(func() { openBuild(); hnswBeforeInstallHook = previous })

	minimum := -1.0
	opts := func() *SearchOptions { return &SearchOptions{Limit: 1, MinSimilarity: &minimum} }
	gate := engine.arm("results", "(*Service).Search")
	first := make(chan error, 1)
	go func() {
		resp, err := svc.Search(context.Background(), "", []float32{1, 1}, opts())
		if err == nil && resp.Metrics.VectorSearchMethod != "vector_brute" {
			err = fmt.Errorf("the first read was not the exact fallback: %s", resp.Metrics.VectorSearchMethod)
		}
		first <- err
	}()
	waitClosed(t, gate.entered, "the search parking after scoring")
	waitClosed(t, buildStarted, "background construction")
	openBuild()
	_, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	gate.open()
	require.NoError(t, <-first)

	after, err := svc.Search(context.Background(), "", []float32{1, 1}, opts())
	require.NoError(t, err)
	require.Len(t, after.Results, 1)
	require.Equal(t, "vector_hnsw", after.Metrics.VectorSearchMethod, "the build-window answer was served after the graph was published")
}

// TestServingPublicationRetiresCachedAnswers: on every path that changes
// what vector search serves, an identical query after the change is not the
// cached response from before it.
func TestServingPublicationRetiresCachedAnswers(t *testing.T) {
	ctx := context.Background()
	graphReady := func(t *testing.T, svc *Service) {
		_, err := svc.getOrCreateHNSWIndex(ctx, 2)
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
			require.NoError(t, svc.maybeRebuildHNSW(ctx, -1, -1, time.Nanosecond))
		}},
		{name: "runtime_transition_swap", setup: graphReady, publish: func(t *testing.T, svc *Service) {
			graph, err := svc.getOrCreateHNSWIndex(ctx, 2)
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
		{name: "gpu_manager_change", setup: func(t *testing.T, svc *Service) {
			svc.SetGPUManager(disabledGPU(t))
		}, publish: func(t *testing.T, svc *Service) {
			svc.SetGPUManager(disabledGPU(t))
		}},
		{name: "clear", publish: func(t *testing.T, svc *Service) {
			svc.ClearVectorIndex()
		}},
	} {
		t.Run(p.name, func(t *testing.T) {
			servingTestEnv(t)
			engine := newLifecycleGateEngine(t)
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { _ = svc.Close() })
			for i, v := range [][]float32{{1, 0.1}, {-1, -0.1}, {0.1, 1}, {-0.1, -1}} {
				addServingNode(t, engine, svc, fmt.Sprintf("nornic:n%d", i), v)
			}
			require.NoError(t, svc.BuildIndexes(ctx))
			if p.setup != nil {
				p.setup(t, svc)
			}
			minimum := -1.0
			opts := &SearchOptions{Limit: 2, MinSimilarity: &minimum}
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

// TestBuildRelationshipStreamDoesNotOvertakeLiveEdgeWrites: the explicit
// build applies stored edges from a snapshot stream inside its NODE phase, so
// node admission keeps a live edge update or removal out until the stream has
// finished; the live write then lands on top and wins.
func TestBuildRelationshipStreamDoesNotOvertakeLiveEdgeWrites(t *testing.T) {
	ctx := context.Background()
	for _, removal := range []bool{false, true} {
		t.Run(map[bool]string{false: "update_not_lost", true: "removal_not_resurrected"}[removal], func(t *testing.T) {
			servingTestEnv(t)
			engine := newLifecycleGateEngine(t)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), "v2", nil)
			// Open the gates before the service closes, so Close can join the build.
			t.Cleanup(engine.releaseAll)
			for _, id := range []string{"nornic:s", "nornic:e"} {
				_, err := engine.CreateNode(admissionNode(id, "node", []float32{1, 0, 0}, "named"))
				require.NoError(t, err)
			}
			edge := &storage.Edge{ID: "nornic:x", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
			require.NoError(t, engine.CreateEdge(edge))
			gate := engine.arm("edges", "buildRelationshipVectorIndexes")
			done := make(chan error, 1)
			go func() { done <- svc.BuildIndexes(ctx) }()
			waitClosed(t, gate.entered, "the build's relationship stream")
			live, writerID := make(chan error, 1), make(chan string, 1)
			if removal {
				require.NoError(t, engine.DeleteEdge(edge.ID))
				go func() { writerID <- serviceTestGoroutineID(); live <- svc.RemoveEdge(edge.ID) }()
			} else {
				updated := &storage.Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode, Type: edge.Type, Properties: map[string]any{"v": []float32{0, 1, 0}}}
				require.NoError(t, engine.UpdateEdge(updated))
				go func() { writerID <- serviceTestGoroutineID(); live <- svc.IndexEdge(updated) }()
			}
			writer := <-writerID
			require.Eventually(t, func() bool { return serviceStackContains(writer, "awaitNodeAdmission") }, 5*time.Second, time.Millisecond,
				"the live edge write waits for the build's node admission while the stream runs")
			gate.open()
			require.NoError(t, <-done)
			select {
			case err := <-live:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("the live edge write did not finish after the stream")
			}
			svc.mu.RLock()
			got, present := svc.edgePropVector[string(edge.ID)]
			svc.mu.RUnlock()
			if removal {
				require.False(t, present, "a removed edge was resurrected by the build's stream")
				return
			}
			require.True(t, present)
			require.Equal(t, []float32{0, 1, 0}, got["v"], "the live edge update was lost to the build's stream")
		})
	}
}

// vectorServingFields are the structures whose change alters what vector
// search serves.
var vectorServingFields = map[string]bool{
	"hnswIndex": true, "vectorPipeline": true, "clusterIndex": true, "clusterHNSW": true,
	"clusterLexicalProfiles": true, "ivfpqIndex": true, "gpuEmbeddingIndex": true,
}

// vectorServingOwners write the serving fields: publication (which advances
// IndexRevision) and the cache of the pipeline derived from the current state
// (which changes nothing a search returns).
var vectorServingOwners = map[string]bool{"publishVectorServing": true, "cacheDerivedVectorPipelineLocked": true}

// vectorServingAccessors read the serving fields for everything else. Each is
// exactly `return s.<field>`.
var vectorServingAccessors = map[string]bool{
	"servingHNSW": true, "servingPipeline": true, "servingClusterIndex": true, "servingClusterHNSW": true,
	"servingClusterProfiles": true, "servingIVFPQ": true, "servingGPUIndex": true,
}

// vectorServingHelpers write serving fields on an owner's behalf. They may be
// called only from an owner's install or another helper.
var vectorServingHelpers = map[string]bool{
	"invalidateHNSWLocked": true, "clearClusterLexicalProfiles": true,
	"rebuildClusterLexicalProfiles": true, "installClusterIndex": true,
}

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

// holdsService reports whether t is Service, or holds one by value (struct
// fields, arrays): memory a pointer to t can overwrite.
func (g *servingGuard) holdsService(t types.Type, seen map[types.Type]bool) bool {
	if types.Identical(t, g.service) {
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
			if tv, ok := g.info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 {
				if types.Identical(tv.Type, types.Typ[types.UnsafePointer]) {
					if ptr, ok := g.info.TypeOf(x.Args[0]).Underlying().(*types.Pointer); ok && g.holdsService(ptr.Elem(), map[types.Type]bool{}) {
						g.report(scope.where, "converts a pointer to Service memory to unsafe.Pointer")
					}
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

type HNSWIndex struct{}

type Service struct {
	hnswIndex, ivfpqIndex, clusterIndex, gpuEmbeddingIndex *HNSWIndex
	vectorPipeline                                          *HNSWIndex
	clusterHNSW                                             map[int]*HNSWIndex
	clusterLexicalProfiles                                  map[int]map[string]float64
}

func NewServiceWithDimensionsAndBM25EngineAndOptions() *Service {
	s := &Service{clusterHNSW: map[int]*HNSWIndex{}}
	s.publishVectorServing(func() { s.hnswIndex = nil }, false)
	return s
}

func (s *Service) publishVectorServing(install func(), retire bool) { install(); s.vectorPipeline = nil }
func (s *Service) cacheDerivedVectorPipelineLocked(p *HNSWIndex)     { s.vectorPipeline = p }
func (s *Service) servingHNSW() *HNSWIndex                           { return s.hnswIndex }
func (s *Service) servingPipeline() *HNSWIndex                       { return s.vectorPipeline }
func (s *Service) servingClusterIndex() *HNSWIndex                   { return s.clusterIndex }
func (s *Service) servingClusterHNSW() map[int]*HNSWIndex            { return s.clusterHNSW }
func (s *Service) servingClusterProfiles() map[int]map[string]float64 {
	return s.clusterLexicalProfiles
}
func (s *Service) servingIVFPQ() *HNSWIndex                 { return s.ivfpqIndex }
func (s *Service) servingGPUIndex() *HNSWIndex              { s.gpuEmbeddingIndex = nil; return s.gpuEmbeddingIndex }
func (s *Service) invalidateHNSWLocked()                    { s.hnswIndex = nil }
func (s *Service) clearClusterLexicalProfiles()             { clear(s.clusterLexicalProfiles) }
func (s *Service) rebuildClusterLexicalProfiles()           { s.clusterLexicalProfiles = nil }
func (s *Service) installClusterIndex()                     { s.invalidateHNSWLocked(); s.clusterIndex = nil }

type gen struct{ hnswIndex *HNSWIndex }
type serviceAlias = Service
type servingWrapper struct{ *Service }
type box[T any] struct{ v T }
type other struct{}

func (other) publishVectorServing(install func(), retire bool) { install() }

func resetGraph(p **HNSWIndex)            { *p = nil }
func zero[T any](p *T)                    { var z T; *p = z }

func (s *Service) leakWrite()                     { s.vectorPipeline = nil }
func (s *Service) leakElement()                   { s.clusterHNSW[0] = nil }
func (s *Service) leakDelete()                    { delete(s.clusterHNSW, 0) }
func (s *Service) leakRead() *HNSWIndex           { return s.ivfpqIndex }
func (s *Service) leakAddress()                   { resetGraph(&s.hnswIndex) }
func (s *Service) leakAlias()                     { m := s.clusterHNSW; delete(m, 0) }
func (s *Service) leakParen()                     { (*s).hnswIndex = nil }
func (s *Service) leakReset()                     { *s = Service{} }
func (s *Service) leakCopy(o *Service)            { *s = *o }
func leakWrapped(o *Service) *Service             { s := &Service{}; s = o; s.ivfpqIndex = nil; return s }
func leakRebound(o *Service)                      { o = &Service{}; _ = o }
func (s *Service) leakHelperValue()               { f := s.invalidateHNSWLocked; f() }
func (s *Service) leakHelperCall()                { s.invalidateHNSWLocked() }
func (s *Service) leakCacheArg()                  { s.cacheDerivedVectorPipelineLocked(s.vectorPipeline) }
func servingHNSW(s *Service) *HNSWIndex           { return s.hnswIndex }
func leakShadow(g *gen, s *Service)               { { g := s; g.hnswIndex = nil } }
func leakShadowCopy(g *gen, s, o *Service)        { { g := s; *g = *o } }
func leakTypeAlias(s *serviceAlias)               { s.hnswIndex = nil }
func leakEmbedded(w *servingWrapper)              { w.hnswIndex = nil }
func (w *servingWrapper) leakEmbeddedMethod()     { w.clusterHNSW = nil }
func leakGenericField(b *box[*Service])           { b.v.hnswIndex = nil }
func leakGenericWrite(s *Service)                 { zero(s) }
func leakFakeOwner(o other, s *Service)           { o.publishVectorServing(func() { s.hnswIndex = nil }, false) }
func leakUnsafe(s *Service)                       { *(**HNSWIndex)(unsafe.Pointer(s)) = nil }

var leakPackageFunc = func(s *Service) { s.hnswIndex = nil }
var leakPackageHelper = (*Service).invalidateHNSWLocked

type leakHelperInterface interface{ invalidateHNSWLocked() }

func (s *Service) leakDelayed() {
	var late func()
	s.publishVectorServing(func() { late = func() { s.hnswIndex = nil } }, false)
	late()
}

func (s *Service) leakSavedHelper() {
	var late func()
	s.publishVectorServing(func() { late = s.invalidateHNSWLocked }, false)
	late()
}

func (s *Service) leakGoroutine() {
	s.publishVectorServing(func() { go s.invalidateHNSWLocked(); go func() { s.hnswIndex = nil }() }, false)
}

func (s *Service) leakEscapingHelperClosure() func() {
	return func() { s.invalidateHNSWLocked() }
}

func (g *gen) unrelated() *HNSWIndex   { g.hnswIndex = nil; return g.hnswIndex }
func (g *gen) unrelatedReset()          { *g = gen{} }
func (s *Service) viaAccessor() int     { _ = s.servingHNSW(); return len(s.servingClusterHNSW()) }
func (w *servingWrapper) viaWrapper()   { w.publishVectorServing(func() { w.hnswIndex = nil }, true) }
func (s *Service) viaMethodValue()      { s.publishVectorServing(s.clearClusterLexicalProfiles, false) }
func (s *Service) viaOwner() {
	s.publishVectorServing(func() {
		s.hnswIndex = nil
		delete(s.clusterHNSW, 1)
		s.invalidateHNSWLocked()
		defer s.installClusterIndex()
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
		"leakAlias: mentions clusterHNSW",
		"leakCacheArg: mentions vectorPipeline",
		"leakCopy: assigns a Service",
		"leakCopy: uses a Service value",
		"leakDelayed: mentions hnswIndex",
		"leakDelete: mentions clusterHNSW",
		"leakElement: mentions clusterHNSW",
		"leakEmbedded: mentions hnswIndex",
		"leakEmbeddedMethod: mentions clusterHNSW",
		"leakEscapingHelperClosure: uses invalidateHNSWLocked outside an owner",
		"leakFakeOwner: mentions hnswIndex",
		"leakGenericField: instantiates box with Service",
		"leakGenericField: mentions hnswIndex",
		"leakGenericWrite: instantiates zero with Service",
		"leakGoroutine: mentions hnswIndex",
		"leakGoroutine: uses invalidateHNSWLocked outside an owner",
		"leakHelperCall: uses invalidateHNSWLocked outside an owner",
		"leakHelperValue: uses invalidateHNSWLocked outside an owner",
		"leakParen: mentions hnswIndex",
		"leakParen: uses a Service value",
		"leakRead: mentions ivfpqIndex",
		"leakRebound: assigns a Service",
		"leakRebound: uses a Service value",
		"leakReset: assigns a Service",
		"leakReset: uses a Service value",
		"leakSavedHelper: uses invalidateHNSWLocked outside an owner",
		"leakShadow: mentions hnswIndex",
		"leakShadowCopy: assigns a Service",
		"leakShadowCopy: uses a Service value",
		"leakTypeAlias: mentions hnswIndex",
		"leakUnsafe: converts a pointer to Service memory to unsafe.Pointer",
		"leakWrapped: assigns a Service",
		"leakWrapped: mentions ivfpqIndex",
		"leakWrapped: uses a Service value",
		"leakWrite: mentions vectorPipeline",
		"servingGPUIndex: is an accessor that is not `return s.<field>`",
		"servingHNSW: mentions hnswIndex",
		"type leakHelperInterface: declares an interface reaching invalidateHNSWLocked",
		"var leakPackageFunc: mentions hnswIndex",
		"var leakPackageHelper: uses invalidateHNSWLocked outside an owner",
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
