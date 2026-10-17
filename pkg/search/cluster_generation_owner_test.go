package search

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// F-1372-CLUSTER enumeration: the cluster generation has one owner. These
// source checks type-check pkg/search and pkg/gpu and fail when a function
// outside the owner writes generation state (Service fields, any field of a
// clusterGeneration, gpu membership and centroids), mutates the cluster
// index, a staged clustering, a cluster graph or the lexical terms, or calls
// an owner function from a call site the owner does not list. Values are
// recognised by type, so aliases (gen := s.clusterGen; gen.graphs = nil) and
// graphs reached through a variable, a range or a map parameter are caught.
// Negative fixtures prove each bypass shape is rejected.

// clusterOwnerFuncs may change the cluster generation; methods of
// clusterGeneration and clusterTerms are part of the owner too.
var clusterOwnerFuncs = map[string]bool{
	"Service.applyClusterAdmission":           true,
	"Service.admitLocked":                     true,
	"Service.admitClusterVectorLocked":        true,
	"Service.admitClusterVectorsLocked":       true,
	"Service.admitCanonicalLocked":            true,
	"Service.retireClusterVectorLocked":       true,
	"Service.beginClusterNodeChangeLocked":    true,
	"Service.endClusterNodeChangeLocked":      true,
	"Service.openClusterStaging":              true,
	"Service.closeClusterStaging":             true,
	"Service.publishClusterGeneration":        true,
	"Service.applyClusterPolicyLocked":        true,
	"Service.retireClusterGeneration":         true,
	"Service.publishClusterServing":           true,
	"Service.snapshotClusterGenerationLocked": true,
	"Service.clusterServingLocked":            true,
	"Service.clusterGraphLocked":              true,
	"newClusterGeneration":                    true,
}

// clusterOwnerCallers is the complete call graph into the owner: for each
// owner function, every production function that calls it or takes it as a
// value. A new call site, or one that disappears, fails the guard.
var clusterOwnerCallers = map[string][]string{
	// Owner-internal steps.
	"Service.applyClusterAdmission":    {"Service.admitLocked", "Service.publishClusterGeneration"},
	"Service.admitLocked":              {"Service.admitCanonicalLocked", "Service.endClusterNodeChangeLocked", "Service.retireClusterVectorLocked"},
	"Service.admitCanonicalLocked":     {"Service.admitClusterVectorLocked", "Service.admitClusterVectorsLocked"},
	"Service.applyClusterPolicyLocked": {"Service.publishClusterGeneration"},
	// Admission entries (the admission boundary, indexMu).
	"Service.admitClusterVectorLocked":     {"Service.indexNodeLocked"},
	"Service.admitClusterVectorsLocked":    {"Service.reconcileClusterPopulation"},
	"Service.retireClusterVectorLocked":    {"Service.RemovePropertyVectorIndex", "Service.removeNodeEntriesLocked"},
	"Service.beginClusterNodeChangeLocked": {"Service.RemovePropertyVectorIndex", "Service.indexNodeLocked", "Service.removeNodeLocked"},
	"Service.endClusterNodeChangeLocked":   {"Service.RemovePropertyVectorIndex", "Service.indexNodeLocked", "Service.removeNodeLocked"},
	// Generation construction and publication.
	"newClusterGeneration":        {"Service.buildClusterGeneration"},
	"Service.openClusterStaging":  {"Service.triggerClustering", "Service.tryRestoreClusteredWarmupFromDisk"},
	"Service.closeClusterStaging": {"Service.triggerClustering", "Service.tryRestoreClusteredWarmupFromDisk"},
	"Service.publishClusterGeneration": {"Service.ConfigureHNSWQuantization", "Service.triggerClustering",
		"Service.tryRestoreClusteredWarmupFromDisk"},
	// Retirement, followed by publication of the serving pipeline.
	"Service.retireClusterGeneration": {"Service.clearIndexes", "Service.maybeAutoSetVectorDimensions",
		"Service.replaceClusterIndex", "Service.resetANNForBuild"},
	"Service.publishClusterServing": {"Service.EnableClustering", "Service.maybeAutoSetVectorDimensions",
		"Service.publishClusterGeneration", "Service.resetANNForBuild"},
	// Readers: persistence capture and per-query serving.
	"Service.snapshotClusterGenerationLocked": {"Service.persistIVFHNSWBackground"},
	"Service.clusterServingLocked": {"Service.resolveClusteredVectorStrategy", "Service.resolveStandardVectorStrategyWithGPU",
		"Service.selectHybridClustersLocked"},
	"Service.clusterGraphLocked": {"Service.resolveClusteredVectorStrategy"},
}

// gpuClusterStateWriters may write the gpu cluster state (centroids,
// membership, clustered) or construct its holders.
var gpuClusterStateWriters = map[string]bool{
	"NewClusterIndex": true, "newClustering": true, "newClusterMembership": true,
	"ClusterIndex.PublishClustering": true, "ClusterIndex.Clear": true,
	"ClusterIndex.RestoreClusteringState": true, "ClusterIndex.OnNodeUpdate": true,
	"ClusterIndex.UpdateCentroidsBatch": true, "ClusterIndex.AssignToClustering": true,
	"ClusterIndex.AssignLive": true, "ClusterIndex.Unassign": true, "ClusterIndex.Remove": true,
	"Clustering.Remove": true, "clusterMembership.set": true, "clusterMembership.remove": true,
}

// Service fields that hold the generation; every field of clusterGeneration
// is generation state too.
var serviceGenerationFields = map[string]bool{"clusterIndex": true, "clusterGen": true, "clusterStaging": true}

// gpu fields holding published or staged cluster state.
var gpuGenerationFields = map[string]map[string]bool{
	"ClusterIndex":      {"centroids": true, "membership": true, "clustered": true},
	"Clustering":        {"centroids": true, "membership": true},
	"clusterMembership": {"idToCluster": true, "members": true, "pos": true},
}

// Methods that change state, per receiver type.
var clusterMutatingMethods = map[string]map[string]bool{
	"ClusterIndex": {"Add": true, "AddBatch": true, "Remove": true, "PublishClustering": true, "Clear": true,
		"RestoreClusteringState": true, "ClusterWithContext": true, "Cluster": true, "Release": true,
		"OnNodeUpdate": true, "UpdateCentroidsBatch": true, "AssignToClustering": true, "AssignLive": true, "Unassign": true},
	"Clustering":   {"Remove": true},
	"clusterTerms": {"add": true, "enter": true, "leave": true},
	"HNSWIndex": {"Add": true, "Update": true, "Remove": true, "Clear": true, "SetQuantization": true,
		"SetVectorLookup": true, "SetBuildLexicalHints": true, "setBuildLexicalHint": true,
		"setSearchBeamFactor": true, "addLocked": true, "addWithLevel0Candidates": true, "removeLocked": true},
	"clusterMembership": {"set": true, "remove": true},
}

const clusterGuardFixtureFile = "zz_cluster_owner_guard_fixture.go"

// Bypass shapes the guard must reject: graph mutations through the
// generation, an alias, a range, a map parameter and an owner accessor; a
// write through a generation alias; a generation literal; a direct index
// mutation; lexical terms; a call into an owner step from outside.
const clusterGuardSearchFixture = `package search

func (s *Service) fixtureGraphBypass(id string) { s.clusterGen.graphs[0].Remove(id) }
func (s *Service) fixtureAliasWrite() { gen := s.clusterGen; gen.graphs = nil }
func (s *Service) fixtureRangeGraph(id string) {
	gen := s.clusterGen
	for _, g := range gen.graphs {
		g.Remove(id)
	}
}
func fixtureMapParam(graphs map[int]*HNSWIndex, id string) { graphs[1].Add(id, nil) }
func (s *Service) fixtureAccessorGraph(id string) { s.clusterGraphLocked(0).Remove(id) }
func (s *Service) fixtureLiteral() *clusterGeneration { return &clusterGeneration{} }
func (s *Service) fixtureIndexBypass(id string) { s.clusterIndex.AssignLive(id) }
func (s *Service) fixtureTerms() { s.clusterGen.terms[0].add("x", 1) }
func (s *Service) fixtureDelete() { delete(s.clusterGen.profiles, 0) }
func (s *Service) fixtureHelper() { s.admitLocked(clusterAdmission{}) }
`

const clusterGuardGPUFixture = `package gpu

func (ci *ClusterIndex) fixtureMembership(id string) { ci.membership.set(id, 0) }
func (ci *ClusterIndex) fixtureCentroids() { ci.centroids = nil }
func (c *Clustering) fixtureStaged() { c.centroids[0] = nil }
`

// clusterTypedPkg is one type-checked package.
type clusterTypedPkg struct {
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
	fset  *token.FileSet
}

// clusterSoftImporter resolves only the packages given; every other import
// fails softly, which leaves this package's own types (and gpu's, for
// pkg/search) fully resolved.
type clusterSoftImporter map[string]*types.Package

func (imp clusterSoftImporter) Import(path string) (*types.Package, error) {
	if p, ok := imp[path]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("%s: not loaded by the guard", path)
}

func loadClusterTypedPkg(t *testing.T, dir, path string, fixture string, imports clusterSoftImporter) *clusterTypedPkg {
	t.Helper()
	bp, err := build.Default.ImportDir(dir, 0)
	require.NoError(t, err)
	fset := token.NewFileSet()
	out := &clusterTypedPkg{fset: fset, info: &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}}
	for _, name := range bp.GoFiles {
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		require.NoError(t, err)
		out.files = append(out.files, file)
	}
	if fixture != "" {
		file, err := parser.ParseFile(fset, clusterGuardFixtureFile, fixture, 0)
		require.NoError(t, err)
		out.files = append(out.files, file)
	}
	conf := types.Config{Importer: imports, FakeImportC: true, Error: func(error) {}}
	out.pkg, _ = conf.Check(path, fset, out.files, out.info)
	require.NotNil(t, out.pkg)
	return out
}

// namedOf is the name of t's (pointer-dereferenced) named type in this
// package's or gpu's scope, else "".
func namedOf(t types.Type) string {
	if t == nil {
		return ""
	}
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil {
		switch n.Obj().Pkg().Path() {
		case "github.com/orneryd/nornicdb/pkg/search", "github.com/orneryd/nornicdb/pkg/gpu":
			return n.Obj().Name()
		}
	}
	return ""
}

func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if id, ok := recv.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

type clusterGuard struct {
	p          *clusterTypedPkg
	gpu        bool
	violations map[string][]string // function -> messages
	edges      map[string]map[string]bool
	// derivedFuncs return generation state (a graph, the index, a map of
	// graphs), so their results are generation state at the call site.
	derivedFuncs map[string]bool
}

func (g *clusterGuard) flag(fn, format string, args ...any) {
	g.violations[fn] = append(g.violations[fn], fmt.Sprintf(format, args...))
}

// generationField reports a selection of generation state: a Service
// generation field or any clusterGeneration field (pkg/search), or a gpu
// cluster-state field.
func (g *clusterGuard) generationField(sel *ast.SelectorExpr) (string, bool) {
	s, ok := g.p.info.Selections[sel]
	if !ok || s.Kind() != types.FieldVal {
		return "", false
	}
	owner, field := namedOf(s.Recv()), sel.Sel.Name
	if g.gpu {
		return owner + "." + field, gpuGenerationFields[owner][field]
	}
	return owner + "." + field, (owner == "Service" && serviceGenerationFields[field]) || owner == "clusterGeneration"
}

// writtenField walks a written expression to the generation field it lands
// in (x.f = …, x.f[i] = …, *x.f = …, x.f.g[i] = …).
func (g *clusterGuard) writtenField(expr ast.Expr) (string, bool) {
	for {
		switch e := expr.(type) {
		case *ast.ParenExpr:
			expr = e.X
		case *ast.StarExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.SliceExpr:
			expr = e.X
		case *ast.SelectorExpr:
			if name, ok := g.generationField(e); ok {
				return name, true
			}
			expr = e.X
		default:
			return "", false
		}
	}
}

// derived reports whether expr reaches generation state: it selects a
// generation field, names a variable derived from one, calls an owner
// function, a function returning generation state or a graph lookup, or is
// a map of graphs.
func (g *clusterGuard) derived(expr ast.Expr, tainted map[types.Object]bool) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if found {
			return false
		}
		switch node := n.(type) {
		case *ast.SelectorExpr:
			if _, ok := g.generationField(node); ok {
				found = true
			}
		case *ast.Ident:
			if obj := g.p.info.Uses[node]; obj != nil && tainted[obj] {
				found = true
			}
		case *ast.CallExpr:
			if key := g.calleeKey(node.Fun); key != "" && (clusterOwnerFuncs[key] || g.derivedFuncs[key]) {
				found = true
			}
			if tv, ok := g.p.info.Types[node.Fun]; ok && namedOf(tv.Type) == "clusterHNSWLookup" {
				found = true
			}
		}
		if e, ok := n.(ast.Expr); ok {
			if tv, ok := g.p.info.Types[e]; ok {
				if m, ok := tv.Type.(*types.Map); ok && namedOf(m.Elem()) == "HNSWIndex" {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// calleeKey is the owner-style key of the function fun names, or "".
func (g *clusterGuard) calleeKey(fun ast.Expr) string {
	var id *ast.Ident
	switch f := fun.(type) {
	case *ast.Ident:
		id = f
	case *ast.SelectorExpr:
		id = f.Sel
	default:
		return ""
	}
	fn, ok := g.p.info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() != g.p.pkg {
		return ""
	}
	if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
		return namedOf(recv.Type()) + "." + fn.Name()
	}
	return fn.Name()
}

func (g *clusterGuard) taint(fn *ast.FuncDecl) map[types.Object]bool {
	tainted := map[types.Object]bool{}
	mark := func(lhs ast.Expr) bool {
		// graphs[i] = …, *p = …: the variable holding it.
		for {
			switch e := lhs.(type) {
			case *ast.IndexExpr:
				lhs = e.X
				continue
			case *ast.StarExpr:
				lhs = e.X
				continue
			case *ast.ParenExpr:
				lhs = e.X
				continue
			}
			break
		}
		id, ok := lhs.(*ast.Ident)
		if !ok {
			return false
		}
		obj := g.p.info.Defs[id]
		if obj == nil {
			obj = g.p.info.Uses[id]
		}
		if obj == nil || tainted[obj] {
			return false
		}
		tainted[obj] = true
		return true
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				for i, lhs := range node.Lhs {
					rhs := node.Rhs[0]
					if len(node.Rhs) == len(node.Lhs) {
						rhs = node.Rhs[i]
					}
					if g.derived(rhs, tainted) && mark(lhs) {
						changed = true
					}
				}
			case *ast.ValueSpec:
				for i, name := range node.Names {
					if i < len(node.Values) && g.derived(node.Values[i], tainted) && mark(name) {
						changed = true
					}
				}
			case *ast.RangeStmt:
				if g.derived(node.X, tainted) {
					if node.Key != nil && mark(node.Key) {
						changed = true
					}
					if node.Value != nil && mark(node.Value) {
						changed = true
					}
				}
			}
			return true
		})
	}
	return tainted
}

func (g *clusterGuard) allowed(key string) bool {
	if g.gpu {
		return gpuClusterStateWriters[key]
	}
	if clusterOwnerFuncs[key] {
		return true
	}
	recv, _, _ := strings.Cut(key, ".")
	return strings.Contains(key, ".") && (recv == "clusterGeneration" || recv == "clusterTerms")
}

func (g *clusterGuard) check(fn *ast.FuncDecl) {
	key := funcKey(fn)
	owner := g.allowed(key)
	tainted := g.taint(fn)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				if field, ok := g.writtenField(lhs); ok && !owner {
					g.flag(key, "writes %s", field)
				}
			}
		case *ast.IncDecStmt:
			if field, ok := g.writtenField(node.X); ok && !owner {
				g.flag(key, "writes %s", field)
			}
		case *ast.UnaryExpr:
			if node.Op == token.AND {
				if field, ok := g.writtenField(node.X); ok && !owner {
					g.flag(key, "takes the address of %s", field)
				}
			}
		case *ast.CompositeLit:
			name := namedOf(g.p.info.Types[node].Type)
			if (name == "clusterGeneration" || (g.gpu && gpuGenerationFields[name] != nil)) && !owner {
				g.flag(key, "constructs a %s", name)
			}
		case *ast.CallExpr:
			if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "delete" && len(node.Args) > 0 {
				if field, ok := g.writtenField(node.Args[0]); ok && !owner {
					g.flag(key, "deletes from %s", field)
				}
			}
			if callee := g.calleeKey(node.Fun); callee != "" && clusterOwnerFuncs[callee] {
				g.edge(callee, key)
			}
			sel, ok := node.Fun.(*ast.SelectorExpr)
			if !ok || owner {
				return true
			}
			s, ok := g.p.info.Selections[sel]
			if !ok || s.Kind() != types.MethodVal {
				return true
			}
			recv := namedOf(s.Recv())
			if !clusterMutatingMethods[recv][sel.Sel.Name] {
				return true
			}
			switch {
			case g.gpu:
				if field, ok := g.writtenField(sel.X); ok {
					g.flag(key, "calls %s.%s on %s", recv, sel.Sel.Name, field)
				}
			case recv == "ClusterIndex" || recv == "Clustering":
				g.flag(key, "calls %s.%s", recv, sel.Sel.Name)
			case g.derived(sel.X, tainted):
				g.flag(key, "calls %s.%s on generation state", recv, sel.Sel.Name)
			}
		case *ast.SelectorExpr:
			// A method call or method value (a lookup handed to a generator)
			// of an owner function is a call site.
			if callee := g.calleeKey(node); callee != "" && clusterOwnerFuncs[callee] {
				g.edge(callee, key)
			}
		}
		return true
	})
}

// returnsDerived reports whether fn returns generation state.
func (g *clusterGuard) returnsDerived(fn *ast.FuncDecl) bool {
	tainted := g.taint(fn)
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if ret, ok := n.(*ast.ReturnStmt); ok {
			for _, result := range ret.Results {
				if g.derived(result, tainted) {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

func (g *clusterGuard) edge(callee, caller string) {
	if g.edges[callee] == nil {
		g.edges[callee] = map[string]bool{}
	}
	g.edges[callee][caller] = true
}

func runClusterGuard(p *clusterTypedPkg, gpu bool) *clusterGuard {
	g := &clusterGuard{p: p, gpu: gpu, violations: map[string][]string{}, edges: map[string]map[string]bool{}, derivedFuncs: map[string]bool{}}
	var funcs []*ast.FuncDecl
	for _, file := range p.files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				funcs = append(funcs, fn)
			}
		}
	}
	if !gpu {
		for changed := true; changed; {
			changed = false
			for _, fn := range funcs {
				if key := funcKey(fn); !g.derivedFuncs[key] && g.returnsDerived(fn) {
					g.derivedFuncs[key] = true
					changed = true
				}
			}
		}
	}
	for _, fn := range funcs {
		g.check(fn)
	}
	if gpu {
		return g
	}
	for callee, callers := range clusterOwnerCallers {
		want := map[string]bool{}
		for _, c := range callers {
			want[c] = true
			if !g.edges[callee][c] {
				g.flag(callee, "listed caller %s no longer calls it", c)
			}
		}
		for c := range g.edges[callee] {
			if !want[c] && !(strings.HasPrefix(callee, "Service.") && c == callee) {
				g.flag(c, "calls owner function %s from an unlisted call site", callee)
			}
		}
	}
	for callee := range g.edges {
		if _, ok := clusterOwnerCallers[callee]; !ok {
			g.flag(callee, "owner function has no listed callers")
		}
	}
	return g
}

func fixtureFuncs(src string) map[string]bool {
	out := map[string]bool{}
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", src, 0)
	if err != nil {
		panic(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			out[funcKey(fn)] = true
		}
	}
	return out
}

// requireGuardResult: production code has no violation; every fixture
// function has at least one.
func requireGuardResult(t *testing.T, g *clusterGuard, fixture string) {
	t.Helper()
	fixtures := fixtureFuncs(fixture)
	var production []string
	for fn, msgs := range g.violations {
		if fixtures[fn] {
			continue
		}
		for _, msg := range msgs {
			production = append(production, fn+": "+msg)
		}
	}
	sort.Strings(production)
	require.Empty(t, production, "generation state changed outside the owner")
	var missed []string
	for fn := range fixtures {
		if len(g.violations[fn]) == 0 {
			missed = append(missed, fn)
		}
	}
	sort.Strings(missed)
	require.Empty(t, missed, "bypass fixtures the guard did not reject")
}

func TestClusterGenerationOwnerAllowList(t *testing.T) {
	gpuPkg := loadClusterTypedPkg(t, filepath.Join("..", "gpu"), "github.com/orneryd/nornicdb/pkg/gpu", clusterGuardGPUFixture, clusterSoftImporter{})
	requireGuardResult(t, runClusterGuard(gpuPkg, true), clusterGuardGPUFixture)

	// pkg/search against the clean gpu package (the fixture adds methods only).
	cleanGPU := loadClusterTypedPkg(t, filepath.Join("..", "gpu"), "github.com/orneryd/nornicdb/pkg/gpu", "", clusterSoftImporter{})
	searchPkg := loadClusterTypedPkg(t, ".", "github.com/orneryd/nornicdb/pkg/search", clusterGuardSearchFixture,
		clusterSoftImporter{"github.com/orneryd/nornicdb/pkg/gpu": cleanGPU.pkg})
	requireGuardResult(t, runClusterGuard(searchPkg, false), clusterGuardSearchFixture)
}

type clusterSourceFunc struct {
	file string
	name string
	recv string
	decl *ast.FuncDecl
}

func clusterSourceFuncs(t *testing.T) []clusterSourceFunc {
	t.Helper()
	fset := token.NewFileSet()
	matches, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var funcs []clusterSourceFunc
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(fset, path, src, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			recv := ""
			if key := funcKey(fn); strings.Contains(key, ".") {
				recv, _, _ = strings.Cut(key, ".")
			}
			funcs = append(funcs, clusterSourceFunc{file: path, name: fn.Name.Name, recv: recv, decl: fn})
		}
	}
	return funcs
}

func (f clusterSourceFunc) owner() bool {
	key := f.name
	if f.recv != "" {
		key = f.recv + "." + f.name
	}
	return clusterOwnerFuncs[key] || f.recv == "clusterGeneration"
}

var clusterLockRank = map[string]int{"pipelineMu": 1, "indexMu": 2, "clusterStateMu": 3, "mu": 4}

// Owner entries take pipelineMu -> indexMu -> clusterStateMu -> mu in order
// (statement order; deferred unlocks hold to the end). The one known
// exception is ConfigureHNSWQuantization's indexMu -> pipelineMu
// (setHNSWQuantizationOverride; sol r5).
func TestClusterGenerationOwnerLockOrder(t *testing.T) {
	var violations []string
	for _, fn := range clusterSourceFuncs(t) {
		takesState := false
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "clusterStateMu" {
				takesState = true
			}
			return true
		})
		if !fn.owner() && !takesState {
			continue
		}
		held := map[string]int{}
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			if _, ok := n.(*ast.DeferStmt); ok {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			lock, ok := sel.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if lock.Sel.Name == "mu" {
				if recv, ok := lock.X.(*ast.Ident); !ok || recv.Name != "s" {
					return true
				}
			}
			rank, ok := clusterLockRank[lock.Sel.Name]
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Lock", "RLock":
				for name, r := range held {
					if r > rank && !(fn.name == "setHNSWQuantizationOverride" && name == "indexMu" && lock.Sel.Name == "pipelineMu") {
						violations = append(violations, fmt.Sprintf("%s takes %s while holding %s", fn.file+":"+fn.name, lock.Sel.Name, name))
					}
				}
				held[lock.Sel.Name] = rank
			case "Unlock", "RUnlock":
				delete(held, lock.Sel.Name)
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

// The files #1372 changes decode vector-entry keys through one decoder
// (audit W12) instead of hand-parsing the ambiguous key format.
func TestClusterGenerationKeyDecodingOwner(t *testing.T) {
	var violations []string
	for _, fn := range clusterSourceFuncs(t) {
		switch fn.file {
		case "hybrid_cluster_routing.go", "vector_query_spec.go", "cluster_generation.go":
		default:
			continue
		}
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.BasicLit:
				if node.Kind == token.STRING && (strings.Contains(node.Value, "-chunk-") || strings.Contains(node.Value, "-named-") || strings.Contains(node.Value, "-prop-")) {
					violations = append(violations, fmt.Sprintf("%s parses a vector key literal %s", fn.file+":"+fn.name, node.Value))
				}
			case *ast.CallExpr:
				if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "normalizeVectorResultIDToNodeID" {
					violations = append(violations, fmt.Sprintf("%s hand-decodes a vector key (use decodeVectorKey)", fn.file+":"+fn.name))
				}
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

// The Service fields holding cluster generation state are exactly the
// owner's: the population index, the published generation, the staging log
// and their lock (plus configuration and the run flag).
func TestClusterGenerationOwnerFieldSet(t *testing.T) {
	want := []string{"clusterEnabled", "clusterGen", "clusterIndex", "clusterStaging", "clusterStateMu", "clusterUsesGPU", "minEmbeddingsForClustering"}
	var got []string
	typ := reflect.TypeOf(Service{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name)
		if strings.Contains(name, "cluster") {
			got = append(got, field.Name)
		}
	}
	sort.Strings(got)
	require.Equal(t, want, got)
	gen := reflect.TypeOf(clusterGeneration{})
	var genFields []string
	for i := 0; i < gen.NumField(); i++ {
		genFields = append(genFields, gen.Field(i).Name)
	}
	require.Equal(t, []string{"index", "staged", "graphs", "terms", "profiles", "dirty", "policy"}, genFields,
		"the generation's state; a new field must be added to the owner's checks")
}
