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
// search serves. Only the serving owners write them.
var vectorServingFields = map[string]bool{
	"hnswIndex": true, "vectorPipeline": true, "clusterIndex": true, "clusterHNSW": true,
	"clusterLexicalProfiles": true, "ivfpqIndex": true, "gpuEmbeddingIndex": true,
}

// vectorServingOwners write the serving fields: publication (which advances
// IndexRevision) and the cache of the pipeline derived from the current state
// (which changes nothing a search returns).
var vectorServingOwners = map[string]bool{"publishVectorServing": true, "cacheDerivedVectorPipelineLocked": true}

// vectorServingHelpers write serving fields on an owner's behalf. They may be
// referenced only inside an owner's install or another helper.
var vectorServingHelpers = map[string]bool{
	"invalidateHNSWLocked": true, "clearClusterLexicalProfiles": true,
	"rebuildClusterLexicalProfiles": true, "installClusterIndex": true,
}

// servingOwnershipViolations lists serving-field writes and helper references
// outside the owners: a write is admitted in an owner, a helper, a constructor
// or a function literal handed to an owner; a helper reference only inside an
// owner's install, as an owner's argument, or inside an owner or helper.
func servingOwnershipViolations(file *ast.File, name string) []string {
	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		fnName := fn.Name.Name
		trusted := vectorServingOwners[fnName] || vectorServingHelpers[fnName]
		if trusted || strings.HasPrefix(fnName, "New") || strings.HasPrefix(fnName, "new") {
			continue
		}
		var walk func(n ast.Node, inOwner bool)
		walk = func(n ast.Node, inOwner bool) {
			ast.Inspect(n, func(node ast.Node) bool {
				switch x := node.(type) {
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok && vectorServingOwners[sel.Sel.Name] {
						walk(x.Fun, inOwner)
						for _, arg := range x.Args {
							walk(arg, true)
						}
						return false
					}
				case *ast.SelectorExpr:
					if vectorServingHelpers[x.Sel.Name] && !inOwner {
						out = append(out, fmt.Sprintf("%s: %s uses %s outside an owner", name, fnName, x.Sel.Name))
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && vectorServingFields[sel.Sel.Name] && !inOwner {
							if _, isIdent := sel.X.(*ast.Ident); isIdent {
								out = append(out, fmt.Sprintf("%s: %s writes %s", name, fnName, sel.Sel.Name))
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
		"func (s *Service) leak() { s.vectorPipeline = nil }\n"+
		"func (s *Service) leakHelper() { s.invalidateHNSWLocked() }\n"+
		"func (s *Service) viaOwner() { s.publishVectorServing(func() { s.hnswIndex = nil; s.invalidateHNSWLocked() }, true) }\n"+
		"func (s *Service) viaMethodValue() { s.publishVectorServing(s.clearClusterLexicalProfiles, false) }\n"+
		"func (s *Service) publishVectorServing(install func(), retire bool) { s.vectorPipeline = nil }\n", 0)
	require.NoError(t, err)
	require.Equal(t, []string{
		"probe.go: leak writes vectorPipeline",
		"probe.go: leakHelper uses invalidateHNSWLocked outside an owner",
	}, servingOwnershipViolations(probe, "probe.go"), "negative fixture: direct writes and helper uses outside an owner are reported, the owner's are admitted")

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
	require.Empty(t, problems, "serving fields written, or their helpers used, outside publishVectorServing")
}
