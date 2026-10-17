package search

// Vector serving publication: every
// change to what vector search serves is published by one owner and advances
// IndexRevision after it is visible, so no answer computed before the change
// is served after it; and the build's relationship stream never overtakes a
// live edge write.

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
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

// vectorServingAccessors read the serving fields for everything else.
var vectorServingAccessors = map[string]bool{
	"servingHNSW": true, "servingPipeline": true, "servingClusterIndex": true, "servingClusterHNSW": true,
	"servingClusterProfiles": true, "servingIVFPQ": true, "servingGPUIndex": true,
}

// vectorServingHelpers write serving fields on an owner's behalf. They may be
// referenced only inside an owner's install or another helper.
var vectorServingHelpers = map[string]bool{
	"invalidateHNSWLocked": true, "clearClusterLexicalProfiles": true,
	"rebuildClusterLexicalProfiles": true, "installClusterIndex": true,
}

// namedTypeOf returns the named type an explicit declaration gives: T or *T.
func namedTypeOf(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// unrelatedLocals lists the receiver and parameters fn declares with a named
// type other than Service: a same-named field on them is not a serving field.
func unrelatedLocals(fn *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	var lists []*ast.FieldList
	if fn.Recv != nil {
		lists = append(lists, fn.Recv)
	}
	lists = append(lists, fn.Type.Params)
	for _, list := range lists {
		for _, field := range list.List {
			if typ := namedTypeOf(field.Type); typ != "" && typ != "Service" {
				for _, name := range field.Names {
					out[name.Name] = true
				}
			}
		}
	}
	return out
}

// servingOwnershipViolations makes the ownership rule hold by construction.
// Any mention of a serving field (read, write or address-of, through any
// expression) is reported unless it is inside an owner, accessor or helper
// method of Service, inside a function literal or helper method value handed
// to publishVectorServing, or a field of a receiver or parameter explicitly
// typed as another named type. Keys of a composite literal initialize a value no one has observed and
// are not mentions. Also reported: a serving helper used outside an owner, an
// assignment through a pointer (*x = ...) not provably of another type, and an
// assignment of a whole Service value.
func servingOwnershipViolations(file *ast.File, name string) []string {
	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		fnName := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) == 1 && namedTypeOf(fn.Recv.List[0].Type) == "Service" &&
			(vectorServingOwners[fnName] || vectorServingAccessors[fnName] || vectorServingHelpers[fnName]) {
			continue
		}
		unrelated := unrelatedLocals(fn)
		report := func(format string, args ...any) {
			out = append(out, fmt.Sprintf("%s: %s ", name, fnName)+fmt.Sprintf(format, args...))
		}
		var walk func(n ast.Node, inOwner bool)
		walk = func(n ast.Node, inOwner bool) {
			ast.Inspect(n, func(node ast.Node) bool {
				switch x := node.(type) {
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "publishVectorServing" {
						walk(x.Fun, inOwner)
						for _, arg := range x.Args {
							_, isLit := arg.(*ast.FuncLit)
							sel, isSel := arg.(*ast.SelectorExpr)
							walk(arg, inOwner || isLit || isSel && vectorServingHelpers[sel.Sel.Name])
						}
						return false
					}
				case *ast.SelectorExpr:
					if vectorServingHelpers[x.Sel.Name] && !inOwner {
						report("uses %s outside an owner", x.Sel.Name)
					}
					if vectorServingFields[x.Sel.Name] && !inOwner {
						if id, ok := x.X.(*ast.Ident); !ok || !unrelated[id.Name] {
							report("mentions %s", x.Sel.Name)
						}
					}
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						star, ok := lhs.(*ast.StarExpr)
						if !ok {
							continue
						}
						if id, ok := star.X.(*ast.Ident); ok && unrelated[id.Name] {
							continue
						}
						if len(x.Rhs) == len(x.Lhs) {
							if lit, ok := x.Rhs[i].(*ast.CompositeLit); ok {
								if typ := namedTypeOf(lit.Type); typ != "" && typ != "Service" {
									continue
								}
							}
						}
						report("assigns through a pointer")
					}
					if x.Tok == token.ASSIGN {
						for i, rhs := range x.Rhs {
							if _, deref := x.Lhs[i].(*ast.StarExpr); deref && len(x.Lhs) == len(x.Rhs) {
								continue // reported above
							}
							if unary, ok := rhs.(*ast.UnaryExpr); ok && unary.Op == token.AND {
								rhs = unary.X
							}
							if lit, ok := rhs.(*ast.CompositeLit); ok && namedTypeOf(lit.Type) == "Service" {
								report("assigns a whole Service")
							}
						}
					}
				}
				return true
			})
		}
		walk(fn.Body, false)
	}
	return out
}

func TestServingFieldsAreWrittenOnlyByTheirOwner(t *testing.T) {
	probe, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package search\n"+
		"type gen struct{ hnswIndex *HNSWIndex }\n"+
		"func (s *Service) leak() { s.vectorPipeline = nil }\n"+
		"func (s *Service) leakElement() { s.clusterHNSW[0] = nil }\n"+
		"func (s *Service) leakDelete() { delete(s.clusterHNSW, 0) }\n"+
		"func (s *Service) leakRead() *IVFPQIndex { return s.ivfpqIndex }\n"+
		"func (s *Service) leakAddress() { resetGraph(&s.hnswIndex) }\n"+
		"func (s *Service) leakAlias() { m := s.clusterHNSW; delete(m, 0) }\n"+
		"func (s *Service) leakParen() { (*s).hnswIndex = nil }\n"+
		"func (s *Service) leakReset() { *s = Service{} }\n"+
		"func (s *Service) leakCopy(o *Service) { *s = *o }\n"+
		"func NewWrapped(o *Service) *Service { s := &Service{}; s = o; s.ivfpqIndex = nil; return s }\n"+
		"func NewRebound(o *Service) { o = &Service{}; _ = o }\n"+
		"func (s *Service) leakHelper() { f := s.invalidateHNSWLocked; f() }\n"+
		"func (s *Service) leakCacheArg() { s.cacheDerivedVectorPipelineLocked(s.vectorPipeline) }\n"+
		"func servingHNSW(s *Service) *HNSWIndex { return s.hnswIndex }\n"+
		"func (g *gen) unrelated() *HNSWIndex { g.hnswIndex = nil; return g.hnswIndex }\n"+
		"func (g *gen) unrelatedReset() { *g = gen{} }\n"+
		"func NewFresh() *Service { return &Service{clusterHNSW: map[int]*HNSWIndex{}} }\n"+
		"func (s *Service) viaAccessor() *HNSWIndex { return s.servingHNSW() }\n"+
		"func (s *Service) viaOwner() { s.publishVectorServing(func() { s.hnswIndex = nil; delete(s.clusterHNSW, 1); s.invalidateHNSWLocked() }, true) }\n"+
		"func (s *Service) viaMethodValue() { s.publishVectorServing(s.clearClusterLexicalProfiles, false) }\n"+
		"func (s *Service) servingHNSW() *HNSWIndex { return s.hnswIndex }\n"+
		"func (s *Service) publishVectorServing(install func(), retire bool) { s.vectorPipeline = nil }\n", 0)
	require.NoError(t, err)
	require.Equal(t, []string{
		"probe.go: leak mentions vectorPipeline",
		"probe.go: leakElement mentions clusterHNSW",
		"probe.go: leakDelete mentions clusterHNSW",
		"probe.go: leakRead mentions ivfpqIndex",
		"probe.go: leakAddress mentions hnswIndex",
		"probe.go: leakAlias mentions clusterHNSW",
		"probe.go: leakParen mentions hnswIndex",
		"probe.go: leakReset assigns through a pointer",
		"probe.go: leakCopy assigns through a pointer",
		"probe.go: NewWrapped mentions ivfpqIndex",
		"probe.go: NewRebound assigns a whole Service",
		"probe.go: leakHelper uses invalidateHNSWLocked outside an owner",
		"probe.go: leakCacheArg mentions vectorPipeline",
		"probe.go: servingHNSW mentions hnswIndex",
	}, servingOwnershipViolations(probe, "probe.go"), "negative fixture: every mention outside an owner, accessor or helper is reported; fields of other types, fresh literals, accessor reads and owner installs are admitted")

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var problems []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		problems = append(problems, servingOwnershipViolations(parsed, name)...)
	}
	sort.Strings(problems)
	require.Empty(t, problems, "serving fields mentioned outside publishVectorServing, the derived-pipeline cache, the accessors and helpers")
}
