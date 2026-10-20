package search

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

type domainListedPkg struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	IgnoredGoFiles []string
	ImportMap      map[string]string
}

// domainImporter type-checks dependencies from source, exported API only
// (IgnoreFuncBodies): the guard needs their types, not their bodies.
type domainImporter struct {
	fset   *token.FileSet
	listed map[string]*domainListedPkg
	loaded map[string]*types.Package
	errs   []error
}

func (imp *domainImporter) Import(path string) (*types.Package, error) {
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
	files, err := parseDomainFiles(imp.fset, listed, "", "")
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
func (imp *domainImporter) from(listed *domainListedPkg) types.Importer {
	return domainImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[path]; ok {
			path = mapped
		}
		return imp.Import(path)
	})
}

type domainImportFunc func(path string) (*types.Package, error)

func (f domainImportFunc) Import(path string) (*types.Package, error) { return f(path) }

// parseDomainFiles parses a listed package, plus fixture (named name) when set.
func parseDomainFiles(fset *token.FileSet, listed *domainListedPkg, name, fixture string) ([]*ast.File, error) {
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

// domainTypedPkg is a package type-checked with bodies and without a single
// error.
type domainTypedPkg struct {
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

func checkDomainPkg(t *testing.T, imp *domainImporter, listed *domainListedPkg, fixture string) *domainTypedPkg {
	t.Helper()
	files, err := parseDomainFiles(imp.fset, listed, "vector-domain-fixture.go", fixture)
	require.NoError(t, err)
	var errs []error
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: imp.from(listed), FakeImportC: true, Error: func(err error) { errs = append(errs, err) }}
	pkg, _ := conf.Check(listed.ImportPath, imp.fset, files, info)
	require.Empty(t, append(errs, imp.errs...), "type-check %s", listed.ImportPath)
	return &domainTypedPkg{pkg: pkg, files: files, info: info}
}

// The guard derives the population from compiler-resolved production objects,
// not filenames or an rg count. It enumerates every constructor/decoder,
// metadata inventory, hint producer/consumer and vector backend reference.
// String provenance is checked at Service/backend boundaries; hints additionally
// have distinct node/entry key types and conversions are confined to the owner.
type vectorDomain uint8

const (
	domainNode vectorDomain = 1 << iota
	domainEntry
)

type vectorDomainGuard struct {
	p          *domainTypedPkg
	env        map[types.Object]vectorDomain
	keys       map[types.Object]vectorDomain
	aliases    map[types.Object]*types.Func
	returns    map[*types.Func]vectorDomain
	decls      map[*types.Func]*ast.FuncDecl
	violations map[string]bool
	population map[string]bool
}

func domainTypeName(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t)
	if ptr, ok := t.(*types.Pointer); ok {
		t = types.Unalias(ptr.Elem())
	}
	if n, ok := t.(*types.Named); ok {
		if pkg := n.Obj().Pkg(); pkg != nil {
			switch pkg.Path() {
			case "github.com/orneryd/nornicdb/pkg/search", "github.com/orneryd/nornicdb/pkg/storage", "github.com/orneryd/nornicdb/pkg/gpu":
			default:
				// For example voyage.Candidate is a graph rerank record, not
				// search.Candidate. Equal spellings do not establish identity.
				return pkg.Path() + "." + n.Obj().Name()
			}
		}
		return n.Obj().Name()
	}
	return ""
}

func domainFuncName(fn *types.Func) string {
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return fn.Name()
	}
	return domainTypeName(sig.Recv().Type()) + "." + fn.Name()
}

func (g *vectorDomainGuard) object(expr ast.Expr) types.Object {
	switch e := expr.(type) {
	case *ast.Ident:
		if o := g.p.info.Uses[e]; o != nil {
			return o
		}
		return g.p.info.Defs[e]
	case *ast.SelectorExpr:
		if s := g.p.info.Selections[e]; s != nil {
			return s.Obj()
		}
		return g.p.info.Uses[e.Sel]
	case *ast.IndexExpr:
		return g.object(e.X)
	case *ast.IndexListExpr:
		return g.object(e.X)
	}
	return nil
}

func (g *vectorDomainGuard) called(expr ast.Expr) *types.Func {
	obj := g.object(expr)
	if fn, ok := obj.(*types.Func); ok {
		return fn.Origin()
	}
	return g.aliases[obj]
}

func (g *vectorDomainGuard) argumentOffset(call *ast.CallExpr, fn *types.Func) int {
	sig := fn.Type().(*types.Signature)
	if sig.Recv() != nil {
		if actual, ok := g.p.info.TypeOf(call.Fun).Underlying().(*types.Signature); ok && actual.Params().Len() == sig.Params().Len()+1 {
			return 1 // method expression, including an aliased method expression
		}
	}
	return 0
}

func domainBackend(recv string) bool {
	switch recv {
	case "VectorIndex", "VectorFileStore", "HNSWIndex", "EmbeddingIndex", "ClusterIndex", "IVFPQIndex", "vectorLookupGetter", "ivfpqExactOverflow", "annMutationOverlay", "vectorFileReadLease", "lexicalBandIndex", "GPUEmbeddingIndex", "Clustering", "clusterGeneration", "HNSWCandidateGen", "VectorSearchPipeline", "candidateGeneratorWithLexicalEntries":
		return true
	}
	return false
}

// A new backend or read-only getter cannot escape the population by choosing a
// new receiver name. Discover the vector API structurally as well as the current
// concrete stores (including promoted/interface methods).
func domainVectorAPI(recv types.Type) bool {
	for _, method := range []string{"Add", "Update", "GetVector", "GetRawVector"} {
		obj, _, _ := types.LookupFieldOrMethod(recv, true, nil, method)
		fn, ok := obj.(*types.Func)
		if !ok {
			continue
		}
		sig := fn.Type().(*types.Signature)
		if sig.Params().Len() == 0 {
			continue
		}
		id, ok := sig.Params().At(0).Type().Underlying().(*types.Basic)
		if !ok || id.Kind() != types.String {
			continue
		}
		var vector types.Type
		if (method == "Add" || method == "Update") && sig.Params().Len() > 1 {
			vector = sig.Params().At(1).Type()
		}
		if (method == "GetVector" || method == "GetRawVector") && sig.Results().Len() > 0 {
			vector = sig.Results().At(0).Type()
		}
		if vector != nil {
			if slice, ok := vector.Underlying().(*types.Slice); ok {
				if value, ok := slice.Elem().Underlying().(*types.Basic); ok && value.Kind() == types.Float32 {
					return true
				}
			}
		}
	}
	return false
}

// String parameters on a vector backend are entry identities by default. Only
// the existing file-path APIs are exempt. New lookup/admission method names
// therefore cannot silently disappear from the family inventory.
func domainKeyMethod(method string) bool {
	return method != "Save" && method != "Load"
}

// Vector lookup and score-reader callbacks share one entry-key input contract.
// Detect it structurally so aliases and returned reader closures are covered.
func domainEntryReader(t types.Type) bool {
	sig, ok := t.Underlying().(*types.Signature)
	if !ok || sig.Params().Len() != 1 || sig.Results().Len() != 2 {
		return false
	}
	id, ok := sig.Params().At(0).Type().Underlying().(*types.Basic)
	if !ok || id.Kind() != types.String || !types.Identical(sig.Results().At(1).Type(), types.Typ[types.Bool]) {
		return false
	}
	result := sig.Results().At(0).Type().Underlying()
	if basic, ok := result.(*types.Basic); ok && basic.Kind() == types.Float64 {
		return true
	}
	if slice, ok := result.(*types.Slice); ok {
		return types.Identical(slice.Elem(), types.Typ[types.Float32])
	}
	return false
}

// Explicit logical API contracts. Generic backend strings are already entry
// identities; graph/fulltext identities remain nodes. These do not bless call
// sites: every supplied argument is still derived and checked independently.
func domainParam(fn *types.Func, n int) vectorDomain {
	name := domainFuncName(fn)
	if n == 0 {
		switch name {
		case "nodeMainVectorKey", "nodeNamedVectorKey", "nodePropertyVectorKey", "nodeChunkVectorKey", "NodeChunkVectorKey", "vectorNodeIdentity", "vectorEntryChunkIndex",
			"Service.vectorMembersOf", "Service.nodeVectorIDsLocked", "Service.forEachNodeVectorKeyLocked", "Service.removeNodeEntriesLocked", "Service.removeNodeLocked", "Service.RemoveNode", "Service.beginClusterNodeChangeLocked", "Service.endClusterNodeChangeLocked", "Service.bm25SeedEmbeddingIDs":
			return domainNode
		case "vectorEntryOwner", "decodeVectorEntryKey", "vectorEntryChunkIndexForNode", "lexicalHintForVectorID", "vectorIDInSeedNodeSet", "vectorEntryIdentity", "collapseCandidatesByNodeID", "collapseIndexResultsByNodeID", "collapseIndexResultsByNodeIDSlow", "Service.clusterTextTokens",
			"Service.addVectorLocked", "Service.removeVectorLocked", "Service.hnswUpdateLive", "Service.hnswRemoveLive", "Service.storedVectorEqualsLocked", "Service.getVectorForCypher", "Service.getVectorForCypherLocked", "getTransitionDeltaVector",
			"Service.trackIVFPQAdd", "Service.trackIVFPQRemove", "Service.appendStrategyDelta", "Service.admitClusterVectorLocked", "Service.admitClusterVectorsLocked", "Service.admitCanonicalLocked", "Service.retireClusterVectorLocked", "HNSWIndex.setBuildLexicalHint":
			return domainEntry
		}
	}
	if n == 1 {
		switch name {
		case "vectorEntryChunkIndexForNode", "nodeHintForID":
			return domainNode
		case "entryHintForVectorID", "putEntryLexicalHint", "vectorEntryChunkIndex", "recordStageScore":
			return domainEntry
		case "clusterGeneration.graphAdd", "clusterGeneration.graphRemove", "Service.scoreVectorIDDot":
			return domainEntry
		}
	}
	if n == 3 && (name == "BuildIVFPQFromVectorStore" || name == "ivfpqCollectTrainingSample" || name == "Service.getOrBuildIVFPQIndex") {
		sig := fn.Type().(*types.Signature)
		if slice, ok := sig.Params().At(n).Type().Underlying().(*types.Slice); ok {
			if elem, ok := slice.Elem().Underlying().(*types.Basic); ok && elem.Kind() == types.String {
				return domainEntry
			}
		}
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() != nil && (domainBackend(domainTypeName(sig.Recv().Type())) || domainVectorAPI(sig.Recv().Type())) && domainKeyMethod(fn.Name()) && n >= 0 && n < sig.Params().Len() {
		t := sig.Params().At(n).Type().Underlying()
		if basic, ok := t.(*types.Basic); ok && basic.Kind() == types.String {
			return domainEntry
		}
		if slice, ok := t.(*types.Slice); ok {
			if basic, ok := slice.Elem().Underlying().(*types.Basic); ok && basic.Kind() == types.String {
				return domainEntry
			}
		}
	}
	return 0
}

func domainResult(fn *types.Func) vectorDomain {
	switch domainFuncName(fn) {
	case "nodeMainVectorKey", "nodeNamedVectorKey", "nodePropertyVectorKey", "nodeChunkVectorKey", "NodeChunkVectorKey", "encodeVectorEntryKey", "mainKeyForLexicalHint", "Service.nodeVectorIDsLocked", "Service.bm25SeedEmbeddingIDs":
		return domainEntry
	case "vectorEntryOwner", "vectorNodeIdentity", "vectorNodeID.nodeIDString", "collapseCandidatesByNodeID", "collapseIndexResultsByNodeID", "collapseIndexResultsByNodeIDSlow":
		return domainNode
	case "Service.vectorMembersOf", "vectorEntryIdentity", "vectorEntryID.entryKeyString", "VectorFileStore.IDs", "lexicalBandIndex.seeds", "loadIVFClusterMemberIDs", "clusterGeneration.members", "Clustering.MemberIDs", "Clustering.MemberIDsOf", "ClusterIndex.GetClusterMemberIDs", "ClusterIndex.GetClusterMemberIDsForCluster", "VectorIndex.GetAllVectors", "VectorIndex.GetAllIDs", "VectorFileStore.GetAllVectors", "VectorFileStore.GetAllIDs":
		return domainEntry
	}
	if fn.Name() == "ScoreCandidates" {
		return domainEntry
	}
	return 0
}

func (g *vectorDomainGuard) field(expr *ast.SelectorExpr) (string, string) {
	return domainTypeName(g.p.info.TypeOf(expr.X)), expr.Sel.Name
}

func (g *vectorDomainGuard) infer(e ast.Expr) vectorDomain {
	if e == nil {
		return 0
	}
	switch domainTypeName(g.p.info.TypeOf(e)) {
	case "NodeID", "vectorNodeID":
		return domainNode
	case "vectorEntryID":
		return domainEntry
	}
	switch x := e.(type) {
	case *ast.Ident:
		return g.env[g.object(x)]
	case *ast.ParenExpr:
		return g.infer(x.X)
	case *ast.SelectorExpr:
		recv, field := g.field(x)
		switch recv + "." + field {
		case "Node.ID", "vectorEntry.NodeID", "nodeLexicalHint.ID", "LexicalSeedHint.ID", "stageNode.id":
			return domainNode
		case "nodeVectorEntry.key", "hnswBuildPair.id", "HNSWIndex.internalToID", "strategyDelta.id", "strategyDeltaMutation.id", "hnswBuildMutation.id", "annMutation.id", "ScoredCandidate.ID", "Candidate.ID", "ANNResult.ID", "Clustering.Clusters", "annMutationOverlaySnapshot.Tombstones":
			return domainEntry
		case "Service.nodeChunkVectors", "Service.nodeNamedVector", "Service.nodePropVector":
			return domainEntry
		case "vectorQueryCandidateMeta.named", "vectorQueryCandidateMeta.properties", "vectorQueryCandidateMeta.props", "vectorQueryCandidateMeta.chunks", "vectorBuildSeeds.compressedIDs":
			return domainEntry
		}
		if field == "MatchID" {
			return domainEntry
		}
		if field == "ID" || field == "id" {
			if d := g.infer(x.X); d != 0 {
				return d
			}
		}
		return g.env[g.object(x)]
	case *ast.IndexExpr:
		return g.infer(x.X)
	case *ast.SliceExpr:
		return g.infer(x.X)
	case *ast.UnaryExpr:
		return g.infer(x.X)
	case *ast.BinaryExpr:
		return g.infer(x.X) | g.infer(x.Y)
	case *ast.CompositeLit:
		var d vectorDomain
		for _, el := range x.Elts {
			if p, ok := el.(*ast.KeyValueExpr); ok {
				d |= g.infer(p.Value)
			} else {
				d |= g.infer(el)
			}
		}
		return d
	case *ast.CallExpr:
		if id, ok := x.Fun.(*ast.Ident); ok {
			switch id.Name {
			case "append":
				var d vectorDomain
				for _, a := range x.Args {
					d |= g.infer(a)
				}
				return d
			case "string":
				if len(x.Args) == 1 {
					return g.infer(x.Args[0])
				}
			}
		}
		if domainTypeName(g.p.info.TypeOf(x.Fun)) == "vectorMemberResolver" {
			return domainEntry // resolver results contain vector-entry keys
		}
		if f := g.called(x.Fun); f != nil {
			if d := domainResult(f); d != 0 {
				return d
			}
			return g.returns[f.Origin()]
		}
	}
	return 0
}

func (g *vectorDomainGuard) rangeDomains(e ast.Expr) (vectorDomain, vectorDomain) {
	if sel, ok := e.(*ast.SelectorExpr); ok {
		recv, name := g.field(sel)
		if (recv == "VectorIndex" && (name == "vectors" || name == "rawVectors")) || (recv == "HNSWIndex" && name == "idToInternal") || ((recv == "VectorFileStore" || recv == "vectorFileReadLease") && name == "idToOrdinal") {
			return domainEntry, 0
		}
		if recv == "HNSWIndex" && name == "internalToID" {
			return 0, domainEntry
		}
		if recv == "Service" && (name == "nodeChunkVectors" || name == "nodeNamedVector" || name == "nodePropVector") {
			return domainNode, domainEntry
		}
	}
	if typ := g.p.info.TypeOf(e); typ != nil {
		switch typ.Underlying().(type) {
		case *types.Map:
			return g.keys[g.object(e)], g.infer(e)
		case *types.Slice, *types.Array:
			return 0, g.infer(e)
		}
	}
	return 0, 0
}

func (g *vectorDomainGuard) bind(e ast.Expr, d vectorDomain) {
	if e == nil {
		return
	}
	if index, ok := e.(*ast.IndexExpr); ok {
		g.keys[g.object(index.X)] |= g.infer(index.Index)
	}
	if obj, ok := g.object(e).(*types.Var); ok {
		// Fields have their own declared semantic contract; per-object local
		// variables accumulate provenance through assignments and aliases.
		if !obj.IsField() {
			g.env[obj] |= d
		}
	}
}

func (g *vectorDomainGuard) closeDomains() {
	// Propagate through locals, ranges, callbacks and return values to a fixed
	// point. No spelling convention such as nodeID/vectorID establishes trust.
	for round := 0; round < 16; round++ {
		for fn, decl := range g.decls {
			ast.Inspect(decl.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, rhs := range x.Rhs {
						if i < len(x.Lhs) {
							g.bind(x.Lhs[i], g.infer(rhs))
							if f := g.called(rhs); f != nil {
								g.aliases[g.object(x.Lhs[i])] = f
							}
						}
					}
				case *ast.ValueSpec:
					for i, rhs := range x.Values {
						if i < len(x.Names) {
							g.bind(x.Names[i], g.infer(rhs))
						}
					}
				case *ast.RangeStmt:
					k, v := g.rangeDomains(x.X)
					g.bind(x.Key, k)
					g.bind(x.Value, v)
				case *ast.ReturnStmt:
					if domainResult(fn) == 0 && len(x.Results) > 0 {
						g.returns[fn] |= g.infer(x.Results[0])
					}
				case *ast.CallExpr:
					// Function values retain their compiler signature. Derive a
					// local closure parameter from every actual caller too.
					if g.called(x.Fun) == nil && !g.p.info.Types[x.Fun].IsType() {
						if typ := g.p.info.TypeOf(x.Fun); typ != nil {
							if sig, ok := typ.Underlying().(*types.Signature); ok {
								for i, arg := range x.Args {
									if i < sig.Params().Len() {
										g.env[sig.Params().At(i)] |= g.infer(arg)
									}
								}
							}
						}
					}
					if f := g.called(x.Fun); f != nil {
						sig := f.Type().(*types.Signature)
						offset := g.argumentOffset(x, f)
						for i, a := range x.Args {
							if i >= offset && i-offset < sig.Params().Len() && domainParam(f, i-offset) == 0 {
								g.env[sig.Params().At(i-offset)] |= g.infer(a)
							}
						}
						name := domainFuncName(f)
						for _, a := range x.Args {
							lit, ok := a.(*ast.FuncLit)
							if !ok {
								continue
							}
							params := g.p.info.TypeOf(lit).(*types.Signature).Params()
							if (name == "Service.forEachNodeVectorKeyLocked" || name == "Service.nodeVectorIDsLocked") && params.Len() > 0 {
								g.env[params.At(0)] = domainEntry
							}
							if (strings.HasSuffix(name, ".IterateChunked") || strings.HasSuffix(name, ".Iterate") || strings.HasSuffix(name, ".IterateVectors")) && domainBackend(strings.Split(name, ".")[0]) && params.Len() > 0 {
								g.env[params.At(0)] = domainEntry
							}
						}
					}
				case *ast.CompositeLit:
					for _, el := range x.Elts {
						if kv, ok := el.(*ast.KeyValueExpr); ok {
							if field, ok := g.object(kv.Key).(*types.Var); ok && field.IsField() && field.Name() != "ID" && field.Name() != "id" {
								g.env[field] |= g.infer(kv.Value)
							}
						}
					}
				case *ast.FuncLit:
					if domainFuncName(fn) == "Service.memoizedVectorMembers" {
						params := g.p.info.TypeOf(x).(*types.Signature).Params()
						if params.Len() == 1 {
							g.env[params.At(0)] = domainNode
						}
					}
					if domainEntryReader(g.p.info.TypeOf(x)) {
						g.env[g.p.info.TypeOf(x).(*types.Signature).Params().At(0)] = domainEntry
					}
					if domainFuncName(fn) == "hnswVectorChunkIterator" {
						params := g.p.info.TypeOf(x).(*types.Signature).Params()
						if params.Len() == 2 {
							if slice, ok := params.At(0).Type().Underlying().(*types.Slice); ok {
								if elem, ok := slice.Elem().Underlying().(*types.Basic); ok && elem.Kind() == types.String {
									g.env[params.At(0)] = domainEntry
								}
							}
						}
					}
					if domainFuncName(fn) == "Service.getVectorLookup" || domainFuncName(fn) == "Service.captureVectorLookupLocked" {
						params := g.p.info.TypeOf(x).(*types.Signature).Params()
						if params.Len() > 0 {
							g.env[params.At(0)] = domainEntry
						}
					}
				}
				return true
			})
		}
	}
}

func (g *vectorDomainGuard) flag(where, msg string) { g.violations[where+": "+msg] = true }

func (g *vectorDomainGuard) checkIdentitySyntax(where string, root ast.Node, owner bool) {
	ast.Inspect(root, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if tn, ok := g.object(x.Fun).(*types.TypeName); ok && !owner {
				switch domainTypeName(tn.Type()) {
				case "vectorNodeID", "vectorEntryID":
					g.flag(where, "identity conversion outside vector_entry_key.go")
				}
				if basic, ok := tn.Type().Underlying().(*types.Basic); ok && basic.Kind() == types.String && len(x.Args) == 1 {
					switch domainTypeName(g.p.info.TypeOf(x.Args[0])) {
					case "vectorNodeID", "vectorEntryID":
						g.flag(where, "identity erasure outside vector_entry_key.go")
					}
				}
			}
		case *ast.MapType:
			if typ := g.p.info.TypeOf(x); typ != nil {
				m := typ.Underlying().(*types.Map)
				if domainTypeName(m.Elem()) == "LexicalSeedHint" {
					g.flag(where, "raw string hint map reintroduces an ambiguous identity domain")
				}
				if domainTypeName(m.Elem()) == "nodeLexicalHint" {
					key := domainTypeName(m.Key())
					_, parameter := m.Key().(*types.TypeParam)
					if key != "vectorNodeID" && key != "vectorEntryID" && !(owner && parameter) {
						g.flag(where, "internal hint map has no typed key domain")
					}
				}
			}
		case *ast.CompositeLit:
			if !owner {
				recv := domainTypeName(g.p.info.TypeOf(x))
				for _, el := range x.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if ok && recv == "stageNode" && key.Name == "id" && g.infer(kv.Value) != domainNode {
						g.flag(where, "non-node identity assigned to exact-stage owner")
					}
					if ok && ((key.Name == "ID" && (recv == "Candidate" || recv == "ScoredCandidate" || recv == "ANNResult")) || (key.Name == "id" && recv == "hnswBuildPair")) && g.infer(kv.Value)&domainNode != 0 {
						g.flag(where, "document identity assigned to a vector entry record")
					}
				}
			}
			if !owner && domainTypeName(g.p.info.TypeOf(x)) == "nodeLexicalHint" && len(x.Elts) > 0 {
				g.flag(where, "constructs a document hint outside the key owner")
			}
			if !owner && domainTypeName(g.p.info.TypeOf(x)) == "LexicalSeedHint" {
				for _, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok && id.Name == "ID" && g.infer(kv.Value)&domainEntry != 0 {
							g.flag(where, "entry key assigned to a BM25 document hint")
						}
					}
				}
			}
		case *ast.AssignStmt:
			if owner {
				break
			}
			for i, lhs := range x.Lhs {
				if i >= len(x.Rhs) {
					break
				}
				if sel, ok := lhs.(*ast.SelectorExpr); ok {
					recv, field := g.field(sel)
					if recv == "stageNode" && field == "id" && g.infer(x.Rhs[i]) != domainNode {
						g.flag(where, "non-node identity assigned to exact-stage owner")
					}
					if (recv == "LexicalSeedHint" || recv == "nodeLexicalHint") && field == "ID" && g.infer(x.Rhs[i])&domainEntry != 0 {
						g.flag(where, "entry key assigned to a document hint")
					}
				}
			}
		}
		return true
	})
}

func (g *vectorDomainGuard) checkKeyCall(where string, call *ast.CallExpr, owner bool) {
	fn := g.called(call.Fun)
	if g.p.info.Types[call.Fun].IsType() {
		return
	} // a function-type conversion is not an invocation
	if fn == nil {
		if domainTypeName(g.p.info.TypeOf(call.Fun)) == "vectorMemberResolver" {
			g.population[where+" -> VectorMemberResolver"] = true
			if len(call.Args) == 0 || g.infer(call.Args[0]) != domainNode {
				g.flag(where, "member resolver receives an unproved/non-node identity")
			}
		}
		if where == "Service.forEachNodeVectorKeyLocked" && len(call.Args) == 1 {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "visit" && g.infer(call.Args[0]) != domainEntry {
				g.flag(where, "inventory callback receives a non-entry identity")
			}
		}
		// VectorLookup is the backend's typed function contract, including
		// callback values captured without a directly named method object.
		if domainEntryReader(g.p.info.TypeOf(call.Fun)) && len(call.Args) > 0 {
			g.population[where+" -> VectorLookup"] = true
			if g.infer(call.Args[0]) != domainEntry {
				g.flag(where, "VectorLookup receives an unproved/non-entry identity")
			}
		}
		return
	}
	name := domainFuncName(fn)
	if name == "HNSWIndex.SetBuildLexicalHints" {
		g.flag(where, "production hint bridge must use the typed setter")
	}
	if name == "HNSWIndex.setBuildLexicalHints" {
		g.population[where+" -> "+name] = true
	}
	if decl := g.decls[fn]; decl != nil && filepath.Base(gFileName(g, decl)) == "vector_entry_key.go" {
		g.population[where+" -> "+name] = true
	}
	offset := g.argumentOffset(call, fn)
	for i, arg := range call.Args {
		if i < offset {
			continue
		}
		want := domainParam(fn, i-offset)
		if g.p.info.Types[arg].IsNil() {
			continue // an empty optional entry set carries no foreign identity
		}
		if want == 0 {
			continue
		}
		g.population[where+" -> "+name] = true
		if owner {
			continue
		} // only the owner performs domain conversion
		got := g.infer(arg)
		if got != 0 && got != want {
			g.flag(where, fmt.Sprintf("%s argument %d has domain %d, want %d", name, i, got, want))
		}
		if got == 0 && want == domainEntry {
			g.flag(where, fmt.Sprintf("unproved vector key at %s argument %d", name, i))
		}
	}
}

func (g *vectorDomainGuard) run() {
	for _, file := range g.p.files {
		for _, decl := range file.Decls {
			if d, ok := decl.(*ast.FuncDecl); ok {
				if f, ok := g.p.info.Defs[d.Name].(*types.Func); ok {
					g.decls[f.Origin()] = d
					sig := f.Type().(*types.Signature)
					for i := 0; i < sig.Params().Len(); i++ {
						if v := domainParam(f, i); v != 0 {
							g.env[sig.Params().At(i)] = v
						}
					}
				}
			}
		}
	}
	g.closeDomains()
	// Package initializers and type declarations are part of the population too;
	// a new alias or function-valued global must not bypass a function-only scan.
	for _, file := range g.p.files {
		owner := filepath.Base(vectorDomainFileNames[file]) == "vector_entry_key.go"
		for _, decl := range file.Decls {
			if _, ok := decl.(*ast.GenDecl); ok {
				where := "declaration in " + filepath.Base(vectorDomainFileNames[file])
				g.checkIdentitySyntax(where, decl, owner)
				ast.Inspect(decl, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						g.checkKeyCall(where, call, owner)
					}
					return true
				})
			}
		}
	}
	for fn, decl := range g.decls {
		where := domainFuncName(fn)
		owner := filepath.Base(gFileName(g, decl)) == "vector_entry_key.go"
		g.checkIdentitySyntax(where, decl, owner)
		ast.Inspect(decl, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				g.checkKeyCall(where, x, owner)
			case *ast.SelectorExpr:
				recv, field := g.field(x)
				if (recv == "Service" && (field == "nodeChunkVectors" || field == "nodeNamedVector" || field == "nodePropVector")) || (recv == "HNSWIndex" && field == "buildLexicalHints") {
					g.population[where+" -> "+recv+"."+field] = true
				}
			case *ast.CompositeLit:
				if domainTypeName(g.p.info.TypeOf(x)) == "nodeLexicalHint" && len(x.Elts) != 0 && !owner {
					g.flag(where, "constructs a document hint outside the key owner")
				}
			case *ast.ReturnStmt:
				if (where == "Service.nodeVectorIDsLocked" || where == "Service.vectorMembersOf") && len(x.Results) > 0 && g.infer(x.Results[0]) != domainEntry {
					g.flag(where, "inventory returns non-entry identities")
				}
			}
			return true
		})
	}
}

// Resolve filenames through an AST node's own production position. No source
// path allowlist can silently exempt a newly added producer or consumer.
var vectorDomainFileNames map[*ast.File]string

func gFileName(g *vectorDomainGuard, decl *ast.FuncDecl) string {
	for f, name := range vectorDomainFileNames {
		if decl.Pos() >= f.Pos() && decl.End() <= f.End() {
			return name
		}
	}
	return ""
}

func newVectorDomainGuard(t *testing.T, fixture string) *vectorDomainGuard {
	listing, err := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,CgoFiles,IgnoredGoFiles,ImportMap", ".").Output()
	require.NoError(t, err)
	imp := &domainImporter{fset: token.NewFileSet(), listed: map[string]*domainListedPkg{}, loaded: map[string]*types.Package{}}
	var self *domainListedPkg
	for dec := json.NewDecoder(bytes.NewReader(listing)); dec.More(); {
		p := &domainListedPkg{}
		require.NoError(t, dec.Decode(p))
		imp.listed[p.ImportPath] = p
		self = p
	}
	require.Equal(t, "github.com/orneryd/nornicdb/pkg/search", self.ImportPath)
	p := checkDomainPkg(t, imp, self, fixture)
	vectorDomainFileNames = map[*ast.File]string{}
	for _, f := range p.files {
		vectorDomainFileNames[f] = imp.fset.Position(f.Pos()).Filename
	}
	g := &vectorDomainGuard{p: p, env: map[types.Object]vectorDomain{}, keys: map[types.Object]vectorDomain{}, aliases: map[types.Object]*types.Func{}, returns: map[*types.Func]vectorDomain{}, decls: map[*types.Func]*ast.FuncDecl{}, violations: map[string]bool{}, population: map[string]bool{}}
	g.run()
	return g
}

func TestVectorEntryDomainGuard(t *testing.T) {
	g := newVectorDomainGuard(t, "")
	nodeType := g.p.pkg.Scope().Lookup("vectorNodeID").Type()
	entryType := g.p.pkg.Scope().Lookup("vectorEntryID").Type()
	require.False(t, types.AssignableTo(nodeType, entryType), "node/entry identities must not become aliases")
	require.False(t, types.AssignableTo(entryType, nodeType))
	require.False(t, types.AssignableTo(types.Typ[types.String], entryType), "raw strings require the key owner")
	require.False(t, types.AssignableTo(types.Typ[types.String], nodeType))
	var population, violations []string
	for edge := range g.population {
		population = append(population, edge)
	}
	for issue := range g.violations {
		violations = append(violations, issue)
	}
	sort.Strings(population)
	sort.Strings(violations)
	for _, edge := range population {
		t.Log("VECTOR_DOMAIN", edge)
	}
	require.NotEmpty(t, population)
	// Both ordinary and build-queued writes delegate vector production to
	// indexNodeAsLocked; indexNodeLocked only selects the write decision.
	for _, member := range []string{"Service.indexNodeAsLocked", "Service.removeNodeEntriesLocked", "Service.nodeVectorIDsLocked", "forEachNodeVectorEntry", "hnswOrderedVectorIndexPairs", "hnswVectorChunkIterator", "HNSWIndex.buildLexicalHintLocked", "EmbeddingIndex", "VectorIndex", "VectorFileStore"} {
		found := false
		for _, edge := range population {
			if strings.Contains(edge, member) {
				found = true
			}
		}
		require.True(t, found, "missing family producer/consumer %s", member)
	}
	require.Empty(t, violations, "node/vector domains must remain separate across the complete compiler-resolved family")
}

// These compile as ordinary Go. The guard must reject every bypass, including
// aliases and global initializer bodies, rather than only one fixture spelling.
const vectorDomainNegativeFixture = `package search
import "github.com/orneryd/nornicdb/pkg/storage"
type domainNodeAlias = vectorNodeID
func domainFixtureCast(raw string) vectorNodeID { return domainNodeAlias(raw) }
func (s *Service) domainFixtureMethodValue(id storage.NodeID) {
 remove := s.removeVectorLocked
 remove(string(id))
}
func (s *Service) domainFixtureMethodExpression(id storage.NodeID) {
 remove := (*HNSWIndex).Remove
 remove(s.hnswIndex, string(id))
}
func domainFixtureWrongMap() map[string]LexicalSeedHint { return nil }
var domainFixtureGlobal = func(s *Service, id storage.NodeID) { s.removeVectorLocked(string(id)) }
func domainFixtureNodeLookup(pair hnswBuildPair) { nodeHintForID(nil, pair.id) }
func domainFixtureCandidate(id storage.NodeID) Candidate { return Candidate{ID:string(id)} }
func domainFixtureValidRerank(id storage.NodeID) RerankCandidate { return RerankCandidate{ID:string(id)} }
func (s *Service) domainFixtureHintBridge() { s.hnswIndex.SetBuildLexicalHints(nil) }
type domainFixtureBackend struct{}
func (*domainFixtureBackend) Add(string, []float32) error { return nil }
func (*domainFixtureBackend) FutureLookup(string) ([]float32,bool) { return nil,false }
func domainFixtureNewLookup(id storage.NodeID) {
 backend := &domainFixtureBackend{}
 backend.FutureLookup(string(id))
}
`

func TestVectorEntryDomainGuardRejectsBypasses(t *testing.T) {
	g := newVectorDomainGuard(t, vectorDomainNegativeFixture)
	for _, name := range []string{"domainFixtureCast", "domainFixtureMethodValue", "domainFixtureMethodExpression", "domainFixtureWrongMap", "domainFixtureNodeLookup", "domainFixtureCandidate", "domainFixtureHintBridge", "domainFixtureNewLookup", "declaration in vector-domain-fixture.go"} {
		found := false
		for issue := range g.violations {
			if strings.Contains(issue, name) {
				found = true
			}
		}
		require.True(t, found, "guard missed bypass %s", name)
	}
	for issue := range g.violations {
		require.False(t, strings.Contains(issue, "domainFixtureValidRerank"), "an unrelated Candidate type is not a vector entry")
		require.True(t, strings.Contains(issue, "domainFixture") || strings.Contains(issue, "vector-domain-fixture.go"), "production violation: %s", issue)
	}
}
