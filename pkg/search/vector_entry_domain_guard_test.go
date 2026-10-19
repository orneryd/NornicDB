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

func domainBackend(recv string) bool {
	switch recv {
	case "VectorIndex", "VectorFileStore", "HNSWIndex", "EmbeddingIndex", "ClusterIndex", "IVFPQIndex", "vectorLookupGetter", "ivfpqExactOverflow", "annMutationOverlay":
		return true
	}
	return false
}

func domainKeyMethod(method string) bool {
	switch method {
	case "Add", "Update", "Remove", "Get", "GetVector", "GetRawVector", "getVectorRef", "HasVector", "AddBatch", "addLocked", "removeLocked", "addWithLevel0Candidates", "AddNormalized", "addNormalized", "ScoreSubset":
		return true
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
		case "nodeMainVectorKey", "nodeNamedVectorKey", "nodePropertyVectorKey", "nodeChunkVectorKey", "NodeChunkVectorKey",
			"Service.nodeVectorIDsLocked", "Service.forEachNodeVectorKeyLocked", "Service.removeNodeEntriesLocked", "Service.removeNodeLocked", "Service.RemoveNode", "Service.beginClusterNodeChangeLocked", "Service.endClusterNodeChangeLocked", "Service.bm25SeedEmbeddingIDs":
			return domainNode
		case "vectorEntryOwner", "decodeVectorEntryKey", "vectorEntryChunkIndexForNode", "lexicalHintForVectorID", "vectorIDInSeedNodeSet",
			"Service.addVectorLocked", "Service.removeVectorLocked", "Service.hnswUpdateLive", "Service.hnswRemoveLive", "Service.storedVectorEqualsLocked", "Service.getVectorForCypher", "Service.getVectorForCypherLocked", "getTransitionDeltaVector",
			"Service.trackIVFPQAdd", "Service.trackIVFPQRemove", "Service.appendStrategyDelta", "Service.admitClusterVectorLocked", "Service.admitClusterVectorsLocked", "Service.admitCanonicalLocked", "Service.retireClusterVectorLocked", "HNSWIndex.setBuildLexicalHint":
			return domainEntry
		}
	}
	if n == 1 {
		switch name {
		case "vectorEntryChunkIndexForNode":
			return domainNode
		case "entryHintForVectorID", "putEntryLexicalHint":
			return domainEntry
		case "clusterGeneration.graphAdd", "clusterGeneration.graphRemove":
			return domainEntry
		}
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() != nil && domainBackend(domainTypeName(sig.Recv().Type())) && domainKeyMethod(fn.Name()) && n == 0 {
		return domainEntry
	}
	return 0
}

func domainResult(fn *types.Func) vectorDomain {
	switch domainFuncName(fn) {
	case "nodeMainVectorKey", "nodeNamedVectorKey", "nodePropertyVectorKey", "nodeChunkVectorKey", "NodeChunkVectorKey", "encodeVectorEntryKey", "mainKeyForLexicalHint", "Service.nodeVectorIDsLocked", "Service.bm25SeedEmbeddingIDs":
		return domainEntry
	case "vectorEntryOwner":
		return domainNode
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
		case "Node.ID", "vectorEntry.NodeID", "nodeLexicalHint.ID", "LexicalSeedHint.ID":
			return domainNode
		case "nodeVectorEntry.key", "hnswBuildPair.id", "HNSWIndex.internalToID", "strategyDelta.id", "hnswBuildMutation.id", "annMutation.id":
			return domainEntry
		case "Service.nodeChunkVectors", "Service.nodeNamedVector", "Service.nodePropVector":
			return domainEntry
		case "vectorQueryCandidateMeta.named", "vectorQueryCandidateMeta.properties", "vectorQueryCandidateMeta.chunks":
			return domainEntry
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
		if f, ok := g.object(x.Fun).(*types.Func); ok {
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
		if recv == "VectorIndex" && (name == "vectors" || name == "rawVectors") {
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
			return 0, g.infer(e)
		case *types.Slice, *types.Array:
			return 0, g.infer(e)
		}
	}
	return 0, 0
}

func (g *vectorDomainGuard) bind(e ast.Expr, d vectorDomain) {
	if d == 0 || e == nil {
		return
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
					if f, ok := g.object(x.Fun).(*types.Func); ok {
						sig := f.Type().(*types.Signature)
						for i, a := range x.Args {
							if i < sig.Params().Len() && domainParam(f, i) == 0 {
								g.env[sig.Params().At(i)] |= g.infer(a)
							}
						}
						name := domainFuncName(f)
						for _, a := range x.Args {
							lit, ok := a.(*ast.FuncLit)
							if !ok {
								continue
							}
							params := g.p.info.TypeOf(lit).(*types.Signature).Params()
							if name == "Service.forEachNodeVectorKeyLocked" && params.Len() > 0 {
								g.env[params.At(0)] = domainEntry
							}
							if (strings.HasSuffix(name, ".IterateChunked") || strings.HasSuffix(name, ".Iterate") || strings.HasSuffix(name, ".IterateVectors")) && domainBackend(strings.Split(name, ".")[0]) && params.Len() > 0 {
								g.env[params.At(0)] = domainEntry
							}
						}
					}
				case *ast.FuncLit:
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
	for fn, decl := range g.decls {
		where := domainFuncName(fn)
		owner := filepath.Base(gFileName(g, decl)) == "vector_entry_key.go"
		_, service := strings.CutPrefix(where, "Service.")
		ast.Inspect(decl, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				obj := g.object(x.Fun)
				if tn, ok := obj.(*types.TypeName); ok && !owner {
					name := tn.Name()
					if name == "vectorNodeID" || name == "vectorEntryID" {
						g.flag(where, "identity conversion outside vector_entry_key.go")
					}
				}
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "string" && len(x.Args) == 1 && !owner {
					name := domainTypeName(g.p.info.TypeOf(x.Args[0]))
					if name == "vectorNodeID" || name == "vectorEntryID" {
						g.flag(where, "identity erasure outside vector_entry_key.go")
					}
				}
				f, ok := obj.(*types.Func)
				if !ok {
					// The canonical inventory's callback is itself an entry sink.
					if where == "Service.forEachNodeVectorKeyLocked" && len(x.Args) == 1 {
						if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "visit" && g.infer(x.Args[0]) != domainEntry {
							g.flag(where, "inventory callback receives a non-entry identity")
						}
					}
					return true
				}
				name := domainFuncName(f)
				for i, arg := range x.Args {
					want := domainParam(f, i)
					if want == 0 {
						continue
					}
					g.population[where+" -> "+name] = true
					got := g.infer(arg)
					// The codec alone may convert raw bytes between logical domains.
					// Its inventory callback above is not a conversion and is checked.
					if owner {
						continue
					}
					if got != 0 && got != want {
						g.flag(where, fmt.Sprintf("%s argument %d has domain %d, want %d", name, i, got, want))
					}
					if got == 0 && service && want == domainEntry {
						g.flag(where, fmt.Sprintf("unproved vector key at %s argument %d", name, i))
					}
				}
			case *ast.SelectorExpr:
				recv, field := g.field(x)
				if (recv == "Service" && (field == "nodeChunkVectors" || field == "nodeNamedVector" || field == "nodePropVector")) || (recv == "HNSWIndex" && field == "buildLexicalHints") {
					g.population[where+" -> "+recv+"."+field] = true
				}
			case *ast.CompositeLit:
				if domainTypeName(g.p.info.TypeOf(x)) == "nodeLexicalHint" && !owner {
					g.flag(where, "constructs a document hint outside the key owner")
				}
			case *ast.ReturnStmt:
				if where == "Service.nodeVectorIDsLocked" && len(x.Results) > 0 && g.infer(x.Results[0]) != domainEntry {
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

func TestVectorEntryDomainGuard(t *testing.T) {
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
	p := checkDomainPkg(t, imp, self, "")
	vectorDomainFileNames = map[*ast.File]string{}
	for _, f := range p.files {
		vectorDomainFileNames[f] = imp.fset.Position(f.Pos()).Filename
	}
	g := &vectorDomainGuard{p: p, env: map[types.Object]vectorDomain{}, returns: map[*types.Func]vectorDomain{}, decls: map[*types.Func]*ast.FuncDecl{}, violations: map[string]bool{}, population: map[string]bool{}}
	g.run()
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
	require.Empty(t, violations, "node/vector domains must remain separate across the complete compiler-resolved family")
}
