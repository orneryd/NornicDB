package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// F-1372-CLUSTER enumeration: the cluster generation has one owner. The
// guard type-checks pkg/search and pkg/gpu completely (every dependency from
// source, no type error allowed: an unresolved expression could hide a
// selection) and resolves every identifier to its go/types object, so
// selectors, method values, method expressions, promoted fields and aliases
// all name the same protected object:
//   - generation state (the Service fields, every field of clusterGeneration,
//     clusterView, clusterSnapshot and clusterTerms; the gpu routing pointer,
//     centroids and membership) is mentioned only by the owner's bodies, the
//     exact accessor and the routing readers;
//   - generation helpers (clusterGeneration/clusterTerms methods, gpu
//     clusterMembership methods) and the mutators of the cluster index and
//     clusterings are referenced only by the owner;
//   - routing readers (selector, generators) read but call no mutator;
//   - queries route only through a generation's routing, never the index's;
//   - the references into every owner function are exactly the listed ones;
//   - every declaration is visited: functions, package variables and types.
// Negative fixtures prove each bypass shape is reported.

// clusterOwnerFuncs are the owner's bodies: they may read and change
// generation state. Methods of clusterGeneration and clusterTerms are owner
// bodies too.
var clusterOwnerFuncs = map[string]bool{
	"Service.applyClusterAdmission":            true,
	"Service.admitLocked":                      true,
	"Service.admitClusterVectorLocked":         true,
	"Service.admitClusterVectorsLocked":        true,
	"Service.admitCanonicalLocked":             true,
	"Service.retireClusterVectorLocked":        true,
	"Service.beginClusterNodeChangeLocked":     true,
	"Service.endClusterNodeChangeLocked":       true,
	"Service.openClusterStaging":               true,
	"Service.closeClusterStaging":              true,
	"Service.stageStaleClusterMembers":         true,
	"Service.publishClusterGeneration":         true,
	"Service.applyClusterPolicyLocked":         true,
	"Service.retireClusterGeneration":          true,
	"Service.publishClusterServing":            true,
	"Service.snapshotClusterGenerationLocked":  true,
	"Service.acquireClusterSnapshot":           true,
	"Service.clusterGenerationFor":             true,
	"Service.clusterQueryView":                 true,
	"Service.clusterGenerationIn":              true,
	"Service.clusterGenerationPublishedLocked": true,
	"Service.clusterGenerationHasGraphsLocked": true,
	"Service.deriveClusterTerms":               true,
	"pipelineServesGeneration":                 true,
	"clusterProfileFromTerms":                  true,
	"newClusterGeneration":                     true,
	"newClusterTerms":                          true,
}

// clusterOwnerCallers is the complete reference graph into the owner: for
// each owner function, every production function that calls it or takes it
// as a value or method expression. A new reference, or one that disappears,
// fails the guard.
var clusterOwnerCallers = map[string][]string{
	// Owner-internal steps.
	"Service.applyClusterAdmission":    {"Service.admitLocked", "Service.publishClusterGeneration"},
	"Service.admitLocked":              {"Service.admitCanonicalLocked", "Service.endClusterNodeChangeLocked", "Service.retireClusterVectorLocked"},
	"Service.admitCanonicalLocked":     {"Service.admitClusterVectorLocked", "Service.admitClusterVectorsLocked"},
	"Service.applyClusterPolicyLocked": {"Service.publishClusterGeneration"},
	"Service.deriveClusterTerms":       {"Service.buildClusterGeneration", "Service.publishClusterGeneration"},
	"clusterProfileFromTerms":          {"Service.applyClusterAdmission", "Service.publishClusterGeneration"},
	"newClusterTerms":                  {"Service.deriveClusterTerms", "clusterGeneration.addTerms"},
	// Admission entries (the admission boundary, indexMu).
	"Service.admitClusterVectorLocked":     {"Service.indexNodeLocked"},
	"Service.admitClusterVectorsLocked":    {"Service.reconcileClusterPopulation"},
	"Service.retireClusterVectorLocked":    {"Service.RemovePropertyVectorIndex", "Service.removeNodeEntriesLocked"},
	"Service.beginClusterNodeChangeLocked": {"Service.RemovePropertyVectorIndex", "Service.indexNodeLocked", "Service.removeNodeLocked"},
	"Service.endClusterNodeChangeLocked":   {"Service.RemovePropertyVectorIndex", "Service.indexNodeLocked", "Service.removeNodeLocked"},
	// Generation construction and publication.
	"newClusterGeneration":             {"Service.buildClusterGeneration"},
	"Service.openClusterStaging":       {"Service.triggerClustering", "Service.tryRestoreClusteredWarmupFromDisk"},
	"Service.closeClusterStaging":      {"Service.triggerClustering", "Service.tryRestoreClusteredWarmupFromDisk"},
	"Service.stageStaleClusterMembers": {"Service.tryRestoreClusteredWarmupFromDisk"},
	"Service.publishClusterGeneration": {"Service.ConfigureHNSWQuantization", "Service.triggerClustering",
		"Service.tryRestoreClusteredWarmupFromDisk"},
	// Retirement, followed by publication of the serving pipeline.
	"Service.retireClusterGeneration": {"Service.Close", "Service.clearIndexes", "Service.maybeAutoSetVectorDimensions",
		"Service.replaceClusterIndex", "Service.resetANNForBuild"},
	"Service.publishClusterServing": {"Service.Close", "Service.EnableClustering", "Service.maybeAutoSetVectorDimensions",
		"Service.publishClusterGeneration", "Service.resetANNForBuild"},
	// Readers: persistence capture, the generation a query holds, strategy.
	"Service.snapshotClusterGenerationLocked":  {"Service.persistIVFHNSWBackground"},
	"Service.acquireClusterSnapshot":           {"Service.clusterGenerationFor", "Service.vectorPipelineForQuery"},
	"Service.clusterGenerationFor":             {"Service.clusterQueryView"},
	"Service.clusterQueryView":                 {"Service.resolveClusteredVectorStrategy"},
	"Service.clusterGenerationIn":              {"Service.selectHybridClustersLocked"},
	"Service.clusterGenerationPublishedLocked": {"Service.resolveStandardVectorStrategyWithGPU"},
	"Service.clusterGenerationHasGraphsLocked": {"Service.resolveClusteredVectorStrategy"},
	"pipelineServesGeneration":                 {"Service.vectorPipelineForQuery"},
}

// clusterAccessors are exactly `return s.<field>`; any function may call them.
var clusterAccessors = map[string]bool{"Service.currentClusterIndex": true}

// clusterReaders route a query on the generation it holds: they read
// generation and view state but reference no mutator or helper.
var clusterReaders = map[string]bool{
	"Service.selectHybridClustersLocked":     true,
	"IVFHNSWCandidateGen.routedGraphs":       true,
	"KMeansCandidateGen.SearchCandidates":    true,
	"GPUKMeansCandidateGen.SearchCandidates": true,
	"ownClusterView":                         true,
}

// Service fields holding the generation; every field of these types is
// generation state too.
var clusterServiceFields = map[string]bool{"clusterIndex": true, "clusterGen": true, "clusterStaging": true}
var clusterStateTypes = map[string]bool{"clusterGeneration": true, "clusterView": true, "clusterSnapshot": true, "clusterTerms": true}

// clusterHelperTypes: every method is an owner helper.
var clusterHelperTypes = map[string]bool{"clusterGeneration": true, "clusterTerms": true}

// Mutators by receiver type (the selection's receiver, so the separate GPU
// embedding index's own Add is not the cluster index's).
var clusterMutators = map[string]map[string]bool{
	"ClusterIndex": {"Add": true, "AddBatch": true, "Remove": true, "PublishClustering": true, "Clear": true,
		"RestoreClusteringState": true, "ClusterWithContext": true, "Cluster": true, "Release": true,
		"OnNodeUpdate": true, "UpdateCentroidsBatch": true, "AssignToClustering": true},
	"Clustering": {"Remove": true},
}

// clusterPopulationReads are the population handle's methods that code
// outside the owner may reach through the cluster index (reads only); any
// other promoted method, mutating or new, is the owner's.
var clusterPopulationReads = map[string]bool{"Count": true, "Get": true, "ScoreSubset": true, "Search": true}

// clusterGraphMutators may not be referenced by a routing reader.
var clusterGraphMutators = map[string]bool{"Add": true, "Update": true, "Remove": true, "Clear": true,
	"SetQuantization": true, "SetVectorLookup": true, "SetBuildLexicalHints": true, "setBuildLexicalHint": true,
	"setSearchBeamFactor": true, "trackFingerprints": true, "addLocked": true, "addWithLevel0Candidates": true,
	"removeLocked": true}

// Queries route only through a generation's own routing: the index's
// routing readers are referenced nowhere in pkg/search, except its published
// Clustering for a generator built without a serving source.
var clusterIndexRoutingReaders = map[string]bool{
	"FindNearestClusters": true, "FindNearestCentroid": true, "GetClusterMemberIDs": true,
	"GetClusterMemberIDsForCluster": true, "GetClusterMembers": true, "SearchWithClusters": true,
	"ClusterOf": true, "GetCentroids": true, "NumClusters": true, "PublishedClustering": true,
}

// gpu: the published routing pointer, centroids and membership are
// mentioned, and clusterMembership's methods referenced, only by these.
var gpuClusterBodies = map[string]bool{
	"NewClusterIndex":                     true,
	"newClustering":                       true,
	"newClusterMembership":                true,
	"ClusterIndex.AssignToClustering":     true,
	"ClusterIndex.Clear":                  true,
	"ClusterIndex.ClusterStats":           true,
	"ClusterIndex.FindNearestCentroid":    true,
	"ClusterIndex.GetCentroids":           true,
	"ClusterIndex.OnNodeUpdate":           true,
	"ClusterIndex.PublishClustering":      true,
	"ClusterIndex.PublishedClustering":    true,
	"ClusterIndex.RestoreClusteringState": true,
	"ClusterIndex.ShouldRecluster":        true,
	"ClusterIndex.UpdateCentroidsBatch":   true,
	"Clustering.ClusterOf":                true,
	"Clustering.MemberIDs":                true,
	"Clustering.MemberIDsOf":              true,
	"Clustering.NearestClusters":          true,
	"Clustering.NumClusters":              true,
	"Clustering.Remove":                   true,
	"clusterMembership.clusterOf":         true,
	"clusterMembership.ids":               true,
	"clusterMembership.remove":            true,
	"clusterMembership.set":               true,
}
var gpuStateFields = map[string]map[string]bool{
	"ClusterIndex":      {"routing": true},
	"Clustering":        {"centroids": true, "membership": true},
	"clusterMembership": {"idToCluster": true, "members": true, "pos": true},
}

const clusterGuardFixtureFile = "zz_cluster_owner_guard_fixture.go"

// Bypass shapes the guard must report (every fixture* declaration): graph
// mutations through the generation, a range, a query view and a method
// expression; writes through an alias, a map alias and a package variable; a
// generation literal; index mutators by call, method value and method
// expression; a staged clustering mutator; lexical terms; helpers by call and
// method expression; an owner step by call and method value; an interface
// naming an owner step; routing on the index.
const clusterGuardSearchFixture = `package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

func dropFixtureGraph(graphs map[int]*HNSWIndex) { delete(graphs, 0) }

func (s *Service) fixtureGraphBypass(id string) { s.clusterGen.graphs[0].Remove(id) }
func (s *Service) fixtureAliasWrite()            { gen := s.clusterGen; gen.graphs = nil }
func (s *Service) fixtureRangeGraph(id string) {
	for _, g := range s.clusterGen.graphs {
		g.Remove(id)
	}
}
func (s *Service) fixtureMapArgument() { dropFixtureGraph(s.clusterGen.graphs) }
func (s *Service) fixtureViewGraph(ctx context.Context, id string) {
	_, view, done := s.clusterQueryView(ctx)
	defer done()
	view.graph(0).Remove(id)
}
func (s *Service) fixtureLiteral() *clusterGeneration { return &clusterGeneration{} }
func (s *Service) fixtureIndexBypass(id string) {
	s.currentClusterIndex().AssignToClustering(s.currentClusterIndex().PublishedClustering(), id)
}
func (s *Service) fixtureTerms()                         { s.clusterGen.terms[0].add("x", 1) }
func (s *Service) fixtureDelete()                        { delete(s.clusterGen.profiles, 0) }
func (s *Service) fixtureHelper()                        { s.admitLocked(clusterAdmission{}) }
func (s *Service) fixtureIndexRouting(q []float32) []int { return s.currentClusterIndex().FindNearestClusters(q, 1) }
func (s *Service) fixtureHelperBypass(id string)         { s.clusterGen.graphRemove(0, id) }
func (s *Service) fixtureMethodExpression(id string)     { (*HNSWIndex).Remove(s.clusterGen.graphs[0], id) }
func (s *Service) fixtureMapAlias()                      { p := s.clusterGen.profiles; delete(p, 0) }
func (s *Service) fixtureMethodValue(id string)          { f := s.currentClusterIndex().Remove; f(id) }
func (s *Service) fixtureMutatorExpression(id string) {
	(*gpu.ClusterIndex).Remove(s.currentClusterIndex(), id)
}
func fixtureHelperExpression() func(*clusterGeneration, int) { return (*clusterGeneration).touch }
func (s *Service) fixtureOwnerValue() func(clusterAdmission) { return s.admitLocked }
func fixtureStagedRemove(c *gpu.Clustering, id string)       { c.Remove(id) }

var fixtureVar = func(s *Service) { s.clusterGen = nil }

type fixtureHelperInterface interface{ admitLocked(clusterAdmission) }

// Named access (sol r13): a wrapper's promoted mutator and its method value,
// the embedded population handle, and a method expression through an alias.
type clusterWrapper struct{ *gpu.ClusterIndex }
type clusterAlias = gpu.ClusterIndex

func (s *Service) fixturePromotedRemove(id string) { w := clusterWrapper{s.currentClusterIndex()}; w.Remove(id) }
func (s *Service) fixturePromotedMethodValue(id string) {
	w := clusterWrapper{s.currentClusterIndex()}
	f := w.Remove
	f(id)
}
func (s *Service) fixtureEmbeddingRemove(id string)  { s.currentClusterIndex().EmbeddingIndex.Remove(id) }
func (s *Service) fixtureEmbeddingClear()            { s.currentClusterIndex().EmbeddingIndex.Clear() }
func (s *Service) fixtureAliasExpression(id string)  { (*clusterAlias).Remove(s.currentClusterIndex(), id) }
func (s *Service) fixturePromotedAdd(id string)       { w := clusterWrapper{s.currentClusterIndex()}; _ = w.Add(id, nil) }
`

const clusterGuardGPUFixture = `package gpu

func (ci *ClusterIndex) fixtureMembership(id string) { ci.routing.membership.set(id, 0) }
func (ci *ClusterIndex) fixtureRouting()             { ci.routing = nil }
func (c *Clustering) fixtureStaged()                 { c.centroids[0] = nil }
func fixtureLiteral() *Clustering                    { return &Clustering{} }
func fixtureHelperValue() func(string) (int, bool) {
	m := clusterMembership{}
	return m.remove
}
`

// clusterListedPkg is one package as go list -json reports it.
type clusterListedPkg struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	IgnoredGoFiles []string
	ImportMap      map[string]string
}

// clusterImporter type-checks dependencies from source, exported API only
// (IgnoreFuncBodies): the guard needs their types, not their bodies.
type clusterImporter struct {
	fset   *token.FileSet
	listed map[string]*clusterListedPkg
	loaded map[string]*types.Package
	errs   []error
}

func (imp *clusterImporter) Import(path string) (*types.Package, error) {
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
	files, err := parseClusterFiles(imp.fset, listed, "", "")
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
func (imp *clusterImporter) from(listed *clusterListedPkg) types.Importer {
	return clusterImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[path]; ok {
			path = mapped
		}
		return imp.Import(path)
	})
}

type clusterImportFunc func(path string) (*types.Package, error)

func (f clusterImportFunc) Import(path string) (*types.Package, error) { return f(path) }

// parseClusterFiles parses a listed package, plus fixture (named name) when set.
func parseClusterFiles(fset *token.FileSet, listed *clusterListedPkg, name, fixture string) ([]*ast.File, error) {
	var files []*ast.File
	for _, file := range append(append([]string{}, listed.GoFiles...), listed.CgoFiles...) {
		parsed, err := parser.ParseFile(fset, filepath.Join(listed.Dir, file), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, parsed)
	}
	if fixture != "" {
		parsed, err := parser.ParseFile(fset, name, fixture, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, parsed)
	}
	return files, nil
}

// clusterTypedPkg is a package type-checked with bodies and without a single
// error.
type clusterTypedPkg struct {
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

func checkClusterPkg(t *testing.T, imp *clusterImporter, listed *clusterListedPkg, fixture string) *clusterTypedPkg {
	t.Helper()
	files, err := parseClusterFiles(imp.fset, listed, clusterGuardFixtureFile, fixture)
	require.NoError(t, err)
	var errs []error
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: imp.from(listed), FakeImportC: true, Error: func(err error) { errs = append(errs, err) }}
	pkg, _ := conf.Check(listed.ImportPath, imp.fset, files, info)
	require.Empty(t, append(errs, imp.errs...), "type-check %s", listed.ImportPath)
	return &clusterTypedPkg{pkg: pkg, files: files, info: info}
}

// namedOf is the name of t's (pointer-dereferenced) named type in pkg/search
// or pkg/gpu, else "".
func namedOf(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
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

// objKey is the owner-style key of a function or method object.
func objKey(fn *types.Func) string {
	if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
		return namedOf(recv.Type()) + "." + fn.Name()
	}
	return fn.Name()
}

// clusterGuard checks one package by object identity.
type clusterGuard struct {
	p          *clusterTypedPkg
	gpu        bool
	fields     map[*types.Var]string // protected fields
	violations map[string][]string   // declaration -> problems
	edges      map[string]map[string]bool

	// population is the cluster index's embedded EmbeddingIndex field.
	population *types.Var
}

func newClusterGuard(t *testing.T, p *clusterTypedPkg, gpu bool) *clusterGuard {
	t.Helper()
	g := &clusterGuard{p: p, gpu: gpu, fields: map[*types.Var]string{}, violations: map[string][]string{}, edges: map[string]map[string]bool{}}
	protect := func(typeName string, names map[string]bool) {
		obj, ok := p.pkg.Scope().Lookup(typeName).(*types.TypeName)
		require.True(t, ok, "type %s", typeName)
		st := obj.Type().Underlying().(*types.Struct)
		found := 0
		for i := 0; i < st.NumFields(); i++ {
			if names == nil || names[st.Field(i).Name()] {
				g.fields[st.Field(i)] = typeName + "." + st.Field(i).Name()
				found++
			}
		}
		if names != nil {
			require.Equal(t, len(names), found, "every protected field of %s exists", typeName)
		}
	}
	if gpu {
		for typeName, names := range gpuStateFields {
			protect(typeName, names)
		}
		return g
	}
	protect("Service", clusterServiceFields)
	for typeName := range clusterStateTypes {
		protect(typeName, nil)
	}
	for _, imported := range p.pkg.Imports() {
		if imported.Path() != "github.com/orneryd/nornicdb/pkg/gpu" {
			continue
		}
		ci, ok := imported.Scope().Lookup("ClusterIndex").(*types.TypeName)
		require.True(t, ok, "gpu.ClusterIndex")
		st := ci.Type().Underlying().(*types.Struct)
		for i := 0; i < st.NumFields(); i++ {
			if f := st.Field(i); f.Embedded() && f.Name() == "EmbeddingIndex" {
				g.population = f
			}
		}
	}
	require.NotNil(t, g.population, "the cluster index embeds its population handle")
	return g
}

func (g *clusterGuard) flag(where, format string, args ...any) {
	g.violations[where] = append(g.violations[where], fmt.Sprintf(format, args...))
}

func (g *clusterGuard) isOwner(key string) bool {
	if g.gpu {
		return gpuClusterBodies[key]
	}
	recv, _, hasRecv := strings.Cut(key, ".")
	return clusterOwnerFuncs[key] || (hasRecv && clusterHelperTypes[recv])
}

// check visits one declaration's code as where.
func (g *clusterGuard) check(where string, root ast.Node, accessor bool) {
	owner := g.isOwner(where)
	reader := !g.gpu && clusterReaders[where]
	ast.Inspect(root, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.Ident:
			switch obj := g.p.info.Uses[x].(type) {
			case *types.Var:
				if name := g.fields[obj]; name != "" && !owner && !reader && !accessor {
					g.flag(where, "mentions %s", name)
				}
			case *types.Func:
				key := objKey(obj)
				if obj.Pkg() == g.p.pkg && g.isOwnerFuncObject(key) && key != where {
					g.edge(key, where)
				}
				if !owner && g.isHelper(obj) {
					g.flag(where, "references helper %s", key)
				}
			}
		case *ast.SelectorExpr:
			sel := g.p.info.Selections[x]
			if sel == nil || g.gpu {
				break
			}
			fn, isFunc := sel.Obj().(*types.Func)
			// The cluster index's embedded population handle is the owner's:
			// selecting it, or reaching it through embedding, is reported
			// outside the owner, except its read methods.
			if !owner {
				if field, ok := sel.Obj().(*types.Var); ok && field == g.population {
					g.flag(where, "selects the cluster index's population handle")
				} else if g.throughPopulation(sel) && !(isFunc && clusterPopulationReads[fn.Name()]) {
					g.flag(where, "reaches the cluster index's population through %s", sel.Obj().Name())
				}
			}
			if !isFunc {
				break
			}
			// Methods by their declaring object: a wrapper's promoted method,
			// a method value and a method expression through a type alias
			// all name the same function.
			fn = fn.Origin()
			recv, name := namedOf(fn.Type().(*types.Signature).Recv().Type()), fn.Name()
			if clusterMutators[recv][name] && !owner {
				g.flag(where, "references mutator %s.%s", recv, name)
			}
			if reader && recv == "HNSWIndex" && clusterGraphMutators[name] {
				g.flag(where, "routing reader references graph mutator %s", name)
			}
			if recv == "ClusterIndex" && clusterIndexRoutingReaders[name] && !(name == "PublishedClustering" && where == "ownClusterView") {
				g.flag(where, "routes on the cluster index (%s) instead of a generation's routing", name)
			}
		case *ast.CompositeLit:
			name := namedOf(g.p.info.TypeOf(x))
			protected := clusterStateTypes[name]
			if g.gpu {
				protected = name == "Clustering" || name == "clusterMembership" || name == "ClusterIndex"
			}
			if protected && !owner && !(reader && name == "clusterView") {
				g.flag(where, "constructs a %s", name)
			}
		case *ast.InterfaceType:
			for _, method := range x.Methods.List {
				for _, name := range method.Names {
					if g.namesProtectedMethod(name.Name) {
						g.flag(where, "declares an interface naming %s", name.Name)
					}
				}
			}
		}
		return true
	})
}

// isOwnerFuncObject reports whether key is an owner function (not a helper
// type's method: those are covered by isHelper).
func (g *clusterGuard) isOwnerFuncObject(key string) bool {
	return !g.gpu && clusterOwnerFuncs[key]
}

// throughPopulation reports whether sel is promoted through the cluster
// index's embedded population handle (a method or field reached via
// ClusterIndex.EmbeddingIndex, from any wrapper or alias).
func (g *clusterGuard) throughPopulation(sel *types.Selection) bool {
	t, path := sel.Recv(), sel.Index()
	for _, i := range path[:len(path)-1] {
		t = types.Unalias(t)
		if p, ok := t.(*types.Pointer); ok {
			t = types.Unalias(p.Elem())
		}
		st, ok := t.Underlying().(*types.Struct)
		if !ok {
			return false
		}
		field := st.Field(i)
		if field == g.population {
			return true
		}
		t = field.Type()
	}
	return false
}

// isHelper reports a function only the owner may reference.
func (g *clusterGuard) isHelper(fn *types.Func) bool {
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return false
	}
	recv := namedOf(sig.Recv().Type())
	if g.gpu {
		return recv == "clusterMembership"
	}
	return clusterHelperTypes[recv]
}

// namesProtectedMethod reports an owner method name (an interface naming it
// would let a non-owner reach the owner through dynamic dispatch).
func (g *clusterGuard) namesProtectedMethod(name string) bool {
	if g.gpu {
		return false
	}
	for key := range clusterOwnerFuncs {
		if _, method, ok := strings.Cut(key, "."); ok && method == name {
			return true
		}
	}
	return false
}

func (g *clusterGuard) edge(callee, caller string) {
	if g.edges[callee] == nil {
		g.edges[callee] = map[string]bool{}
	}
	g.edges[callee][caller] = true
}

// run visits every declaration, then checks the reference graph and the
// accessors.
func (g *clusterGuard) run(t *testing.T) {
	t.Helper()
	for _, file := range g.p.files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				key := funcKey(d)
				accessor := !g.gpu && clusterAccessors[key]
				if accessor && !clusterIsExactAccessor(g.p.info, d, g.fields) {
					g.flag(key, "is an accessor that is not `return s.<field>`")
				}
				g.check(key, d, accessor)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.ValueSpec:
						for _, value := range s.Values {
							g.check("var "+s.Names[0].Name, value, false)
						}
					case *ast.TypeSpec:
						g.check("type "+s.Name.Name, s.Type, false)
					}
				}
			}
		}
	}
	if g.gpu {
		return
	}
	for callee, callers := range clusterOwnerCallers {
		want := map[string]bool{}
		for _, c := range callers {
			want[c] = true
			if !g.edges[callee][c] {
				g.flag(callee, "listed caller %s no longer references it", c)
			}
		}
		for c := range g.edges[callee] {
			if !want[c] {
				g.flag(c, "references owner function %s from an unlisted site", callee)
			}
		}
	}
	for callee := range g.edges {
		if _, ok := clusterOwnerCallers[callee]; !ok && !g.isOwner(callee) {
			g.flag(callee, "owner function has no listed callers")
		}
	}
	for key := range clusterOwnerFuncs {
		if _, listed := clusterOwnerCallers[key]; !listed && len(g.edges[key]) > 0 {
			g.flag(key, "owner function has no listed callers")
		}
	}
}

// clusterIsExactAccessor reports whether d is `return s.<protected field>`.
func clusterIsExactAccessor(info *types.Info, d *ast.FuncDecl, fields map[*types.Var]string) bool {
	if d.Body == nil || len(d.Body.List) != 1 || d.Recv == nil || len(d.Recv.List[0].Names) != 1 {
		return false
	}
	ret, ok := d.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return false
	}
	sel, ok := ret.Results[0].(*ast.SelectorExpr)
	if !ok {
		return false
	}
	recv, ok := sel.X.(*ast.Ident)
	if !ok || recv.Name != d.Recv.List[0].Names[0].Name {
		return false
	}
	field, ok := info.ObjectOf(sel.Sel).(*types.Var)
	return ok && fields[field] != ""
}

// fixtureFuncs are the declarations a fixture requires reported: fixture*
// functions, and fixture* package variables and types.
func fixtureFuncs(src string) map[string]bool {
	out := map[string]bool{}
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", src, 0)
	if err != nil {
		panic(err)
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if strings.HasPrefix(d.Name.Name, "fixture") {
				out[funcKey(d)] = true
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.ValueSpec:
					if strings.HasPrefix(s.Names[0].Name, "fixture") {
						out["var "+s.Names[0].Name] = true
					}
				case *ast.TypeSpec:
					if strings.HasPrefix(s.Name.Name, "fixture") {
						out["type "+s.Name.Name] = true
					}
				}
			}
		}
	}
	return out
}

// requireGuardResult: production code has no violation; every fixture
// declaration has at least one.
func requireGuardResult(t *testing.T, g *clusterGuard, fixture string) {
	t.Helper()
	fixtures := fixtureFuncs(fixture)
	var production []string
	for where, msgs := range g.violations {
		if fixtures[where] {
			continue
		}
		for _, msg := range msgs {
			production = append(production, where+": "+msg)
		}
	}
	sort.Strings(production)
	require.Empty(t, production, "generation state touched outside the owner")
	var missed []string
	for where := range fixtures {
		if len(g.violations[where]) == 0 {
			missed = append(missed, where)
		}
	}
	sort.Strings(missed)
	require.Empty(t, missed, "bypass fixtures the guard did not report")
}

func TestClusterGenerationOwnerAllowList(t *testing.T) {
	listing, err := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,CgoFiles,IgnoredGoFiles,ImportMap", ".").Output()
	require.NoError(t, err)
	imp := &clusterImporter{fset: token.NewFileSet(), listed: map[string]*clusterListedPkg{}, loaded: map[string]*types.Package{}}
	var self *clusterListedPkg
	for dec := json.NewDecoder(bytes.NewReader(listing)); dec.More(); {
		listed := &clusterListedPkg{}
		require.NoError(t, dec.Decode(listed))
		imp.listed[listed.ImportPath] = listed
		self = listed // -deps lists the package itself last
	}
	require.Equal(t, "github.com/orneryd/nornicdb/pkg/search", self.ImportPath)
	gpuListed := imp.listed["github.com/orneryd/nornicdb/pkg/gpu"]
	require.NotNil(t, gpuListed)

	gpuPkg := checkClusterPkg(t, imp, gpuListed, clusterGuardGPUFixture)
	gpuGuard := newClusterGuard(t, gpuPkg, true)
	gpuGuard.run(t)
	requireGuardResult(t, gpuGuard, clusterGuardGPUFixture)

	searchPkg := checkClusterPkg(t, imp, self, clusterGuardSearchFixture)
	searchGuard := newClusterGuard(t, searchPkg, false)
	searchGuard.run(t)
	requireGuardResult(t, searchGuard, clusterGuardSearchFixture)

	// Files this build excludes are not type-checked; none may name the
	// generation's state or helpers, or the guard must run under their tags.
	for _, listed := range []*clusterListedPkg{self, gpuListed} {
		for _, name := range listed.IgnoredGoFiles {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(imp.fset, filepath.Join(listed.Dir, name), nil, 0)
			require.NoError(t, err)
			ast.Inspect(file, func(node ast.Node) bool {
				if sel, ok := node.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "clusterIndex", "clusterGen", "clusterStaging", "routing", "membership", "centroids":
						t.Errorf("%s: build-excluded file names %s", name, sel.Sel.Name)
					}
				}
				return true
			})
		}
	}
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
	require.Equal(t, []string{"index", "routing", "published", "holders", "graphs", "terms", "profiles", "dirty", "policy"}, genFields,
		"the generation's state; a new field must be added to the owner's checks")
}
