package inference

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Field and package variable kinds. Any other kind is "by <lock>": owned by
// that lock, a path relative to the receiver for fields ("mu", "stats.mu")
// or another package variable for globals.
const (
	kindLock      = "lock"      // a sync.Mutex or sync.RWMutex
	kindAtomic    = "atomic"    // a sync/atomic type
	kindAtomicOps = "atomicops" // a plain integer only accessed through sync/atomic functions
	kindSynced    = "synced"    // a value of a shared type of this package, which locks itself
	kindImmutable = "immutable" // set at construction and never written after
)

// Kinds of the package's other struct types.
const (
	structNested  = "nested"  // a set embedded by value in a shared type; its fields are classified there, as "field.sub"
	structContent = "content" // data held inside a shared type's owned field, protected by that owner
	structValue   = "value"   // configs, keys, results and snapshots, copied or immutable once published
)

// stateTuple is a set of fields read and written as one set: every function
// accesses its fields within one critical section of their owner.
type stateTuple struct {
	typ    string
	fields []string
}

// ownership is the F-INFSTATS inventory (sig/handoff/finfstats-inventory.md):
// every struct type and package variable of pkg/inference, and for each
// shared type every field, with its owner.
type ownership struct {
	fields  map[string]map[string]string // shared type → field path → kind
	structs map[string]string            // every other struct type → its kind
	globals map[string]string            // package variable → kind
	tuples  []stateTuple
}

func by(lock string) string { return "by " + lock }

func inferenceOwnership() ownership {
	return ownership{
		fields: map[string]map[string]string{
			"ClusterIntegration": {
				"config": by("mu"), "clusterIndex": kindImmutable, "mu": kindLock,
				"searches": kindSynced, "lastReclusterAt": by("mu"), "reclusterPending": kindAtomic,
			},
			"searchCounters": {"mu": kindLock, "total": by("mu"), "clustered": by("mu")},
			"CooldownTable":  {"mu": kindLock, "entries": by("mu"), "defaults": by("mu"), "counters": kindSynced},
			"checkCounters":  {"mu": kindLock, "checks": by("mu"), "allowed": by("mu"), "blocked": by("mu")},
			"EvidenceBuffer": {
				"mu": kindLock, "entries": by("mu"), "thresholds": by("mu"),
				"totalAdded": by("mu"), "totalMaterialized": by("mu"), "totalExpired": by("mu"),
			},
			"EdgeDecay": {
				"config": kindImmutable, "storage": kindImmutable,
				"mu": kindLock, "reinforced": by("mu"),
				"lifecycle": kindLock, "cancel": by("lifecycle"), "done": by("lifecycle"),
				"stats.mu": kindLock, "stats.ScansCompleted": by("stats.mu"), "stats.EdgesScanned": by("stats.mu"),
				"stats.EdgesDecayed": by("stats.mu"), "stats.EdgesDeleted": by("stats.mu"),
				"stats.LastScanTime": by("stats.mu"), "stats.LastScanDuration": by("stats.mu"),
			},
			"HeimdallQC": {
				"config": kindImmutable, "heimdall": kindImmutable,
				"cacheMu": kindLock, "cache": by("cacheMu"),
				"stats.mu": kindLock, "stats.BatchesProcessed": by("stats.mu"), "stats.SuggestionsIn": by("stats.mu"),
				"stats.SuggestionsOut": by("stats.mu"), "stats.Augmented": by("stats.mu"), "stats.Skipped": by("stats.mu"),
				"stats.Errors": by("stats.mu"), "stats.CacheHits": by("stats.mu"), "stats.AvgLatencyMs": by("stats.mu"),
				"stats.totalLatencyMs": by("stats.mu"),
			},
			"Engine": {
				"config": kindImmutable, "mu": kindLock,
				"accessHistory": by("mu"), "coAccessCounts": by("mu"),
				"similaritySearch": by("mu"), "topologyIntegration": by("mu"), "clusterIntegration": by("mu"),
				"kalmanAdapter": by("mu"), "cooldownTable": by("mu"), "evidenceBuffer": by("mu"),
				"edgeMetaStore": by("mu"), "nodeConfigStore": by("mu"), "heimdallQC": by("mu"),
			},
			"KalmanAdapter": {
				"mu": kindLock, "config": kindImmutable, "engine": kindImmutable,
				"session": by("mu"), "tracker": by("mu"), "edgeFilters": by("mu"),
				"cachedConfidence": by("mu"), "sessionCoAccess": by("mu"), "stats": by("mu"),
			},
			"TopologyIntegration": {
				"config": kindImmutable, "storage": kindImmutable, "builder": kindImmutable,
				"graphMu": kindLock, "cachedGraph": by("graphMu"),
				"lastBuildTime": by("graphMu"), "totalBuildTime": by("graphMu"), "buildsCompleted": by("graphMu"),
				"predictionCount": kindAtomicOps, "predictionsRun": kindAtomicOps,
				"deltaMu": kindLock, "pendingDelta": by("deltaMu"),
			},
		},
		structs: map[string]string{
			"EdgeDecayStats":  structNested,
			"HeimdallQCStats": structNested,
			// Entries of owned maps and slices: CooldownTable.entries,
			// EvidenceBuffer.entries, HeimdallQC.cache, Engine.accessHistory,
			// KalmanAdapter.cachedConfidence and sessionCoAccess.
			"CooldownEntry": structContent, "Evidence": structContent, "cachedBatchDecision": structContent,
			"accessRecord": structContent, "smoothedConfidence": structContent, "crossSessionData": structContent,
			"ClusterConfig": structValue, "ClusterIntegrationStats": structValue, "CooldownStats": structValue,
			"EdgeDecayConfig": structValue, "EvidenceKey": structValue, "EvidenceThreshold": structValue,
			"EvidenceStats": structValue, "HeimdallQCConfig": structValue, "HeimdallBatchRequest": structValue,
			"NodeSummary": structValue, "CandidateSummary": structValue, "HeimdallBatchResponse": structValue,
			"AugmentedEdge": structValue, "EdgeSuggestion": structValue, "Config": structValue,
			"engineComponents": structValue, "coAccessKey": structValue, "SimilarityResult": structValue,
			"ExistingEdge": structValue, "Stats": structValue, "ProcessSuggestionResult": structValue,
			"KalmanAdapterConfig": structValue, "edgeKey": structValue, "InferenceAdapterStats": structValue,
			"TopologyConfig": structValue, "TopologyStats": structValue,
		},
		globals: map[string]string{
			"DefaultCooldowns": kindImmutable, "DefaultThresholds": kindImmutable, "DefaultEvidenceThreshold": kindImmutable,
			"globalCooldownMu": kindLock, "globalCooldownTable": by("globalCooldownMu"),
			"globalEvidenceMu": kindLock, "globalEvidenceBuffer": by("globalEvidenceMu"),
		},
		tuples: []stateTuple{
			{"checkCounters", []string{"checks", "allowed", "blocked"}},
			{"searchCounters", []string{"total", "clustered"}},
			{"EvidenceBuffer", []string{"totalAdded", "totalMaterialized", "totalExpired"}},
			{"EdgeDecay", []string{"stats.ScansCompleted", "stats.EdgesScanned", "stats.EdgesDecayed", "stats.EdgesDeleted", "stats.LastScanTime", "stats.LastScanDuration"}},
			{"HeimdallQC", []string{"stats.BatchesProcessed", "stats.totalLatencyMs", "stats.AvgLatencyMs"}},
			{"Engine", []string{"similaritySearch", "topologyIntegration", "clusterIntegration", "kalmanAdapter", "cooldownTable", "evidenceBuffer", "edgeMetaStore", "nodeConfigStore", "heimdallQC"}},
			{"TopologyIntegration", []string{"lastBuildTime", "totalBuildTime", "buildsCompleted"}},
		},
	}
}

func (o ownership) clone() ownership {
	c := ownership{fields: map[string]map[string]string{}, structs: map[string]string{}, globals: map[string]string{}, tuples: o.tuples}
	for t, fs := range o.fields {
		c.fields[t] = map[string]string{}
		for f, k := range fs {
			c.fields[t][f] = k
		}
	}
	for t, k := range o.structs {
		c.structs[t] = k
	}
	for v, k := range o.globals {
		c.globals[v] = k
	}
	return c
}

// guardImporter type-checks sync and sync/atomic from source: field kinds
// are checked against their types. Every other import is an empty stub:
// values of other packages' types are dependencies that synchronize
// themselves, and the errors their stubs cause are ignored.
type guardImporter struct {
	std   types.Importer
	stubs map[string]*types.Package
}

func (g *guardImporter) Import(path string) (*types.Package, error) {
	if path == "sync" || path == "sync/atomic" {
		return g.std.Import(path)
	}
	if g.stubs[path] == nil {
		g.stubs[path] = types.NewPackage(path, pathpkg.Base(path))
		g.stubs[path].MarkComplete()
	}
	return g.stubs[path], nil
}

const (
	shared    = 1
	exclusive = 2
)

type span struct {
	from, to  token.Pos
	owner     *ast.FuncLit // innermost closure containing the span's lock, nil for the function body
	exclusive bool
}

type callSite struct {
	callee string
	pos    token.Pos
	async  bool // a method value, not a call: it can run any time
}

// funcInfo is one function's view of the shared state it touches.
type funcInfo struct {
	key      string // "file: Type.Method" or "file: function"
	typ      string // the receiver's shared type, "" otherwise
	name     string
	exported bool
	needs    map[string]int // receiver lock path → the mode its unprotected accesses need
	calls    []callSite     // calls and method values on its own receiver
	held     func(lock string, pos token.Pos) (mode, id int)
	inAsync  func(pos token.Pos) bool // inside a go statement or closure
}

type guard struct {
	fset     *token.FileSet
	info     *types.Info
	pkg      *types.Package
	own      ownership
	problems map[string]bool
	funcs    []*funcInfo
	foreign  []foreignCall
}

// foreignCall is a call to a shared type's method on anything but the
// calling method's own receiver.
type foreignCall struct{ key, typ, callee string }

func (g *guard) report(format string, args ...any) { g.problems[fmt.Sprintf(format, args...)] = true }

func (g *guard) file(pos token.Pos) string { return filepath.Base(g.fset.Position(pos).Filename) }

// namedStruct is the name of e's type (or its pointee) if that is a type of
// this package, else "".
func (g *guard) namedStruct(e ast.Expr) string {
	t := g.info.TypeOf(e)
	if t == nil {
		return ""
	}
	return g.typeName(t)
}

func (g *guard) typeName(t types.Type) string {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() == g.pkg {
		return n.Obj().Name()
	}
	return ""
}

func syncType(t types.Type) (pkg, name string) {
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil {
		if p := n.Obj().Pkg().Path(); p == "sync" || p == "sync/atomic" {
			return p, n.Obj().Name()
		}
	}
	return "", ""
}

// ownershipViolations type-checks files as pkg/inference and returns every
// violation of own: unclassified or misclassified state, and accesses
// outside their owner.
func ownershipViolations(t *testing.T, fset *token.FileSet, imp types.Importer, files map[string]*ast.File, own ownership) []string {
	t.Helper()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	list := make([]*ast.File, 0, len(names))
	for _, name := range names {
		list = append(list, files[name])
	}
	g := &guard{fset: fset, own: own, problems: map[string]bool{}, info: &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}}
	conf := types.Config{Importer: imp, Error: func(err error) {
		// Errors in the package's own files come from the stubbed imports;
		// a probe must type-check.
		if te, ok := err.(types.Error); ok && filepath.Base(te.Fset.Position(te.Pos).Filename) == "probe.go" {
			g.report("probe.go: type error: %s", te.Msg)
		}
	}}
	g.pkg, _ = conf.Check("github.com/orneryd/nornicdb/pkg/inference", fset, list, g.info)
	require.NotNil(t, g.pkg)

	g.classify(list)
	for _, name := range names {
		for _, decl := range files[name].Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				g.analyzeFunc(fn, name)
			}
		}
	}
	g.resolveHelpers()
	out := make([]string, 0, len(g.problems))
	for p := range g.problems {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// classify checks own against the package's declarations: every struct type
// and package variable is classified, every field of every shared type is
// classified with a kind its type allows, and nothing classified is missing.
func (g *guard) classify(files []*ast.File) {
	declared := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gd.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if _, ok := s.Type.(*ast.StructType); ok {
						declared[s.Name.Name] = true
						g.classifyStruct(s.Name)
					}
				case *ast.ValueSpec:
					if gd.Tok != token.VAR {
						continue // constants
					}
					for _, name := range s.Names {
						if name.Name != "_" {
							declared["var "+name.Name] = true
							g.classifyGlobal(name)
						}
					}
				}
			}
		}
	}
	for typ := range g.own.fields {
		if !declared[typ] {
			g.report("%s: classified, but no such struct type", typ)
		}
	}
	for typ := range g.own.structs {
		if !declared[typ] {
			g.report("%s: classified, but no such struct type", typ)
		}
	}
	for name := range g.own.globals {
		if !declared["var "+name] {
			g.report("%s: classified, but no such package variable", name)
		}
	}
}

func (g *guard) classifyStruct(name *ast.Ident) {
	obj, _ := g.info.Defs[name].(*types.TypeName)
	if obj == nil {
		return
	}
	named, _ := obj.Type().(*types.Named)
	st, _ := obj.Type().Underlying().(*types.Struct)
	if named == nil || st == nil {
		return
	}
	file := g.file(name.Pos())
	fields, shared := g.own.fields[name.Name]
	kind := g.own.structs[name.Name]
	hasSync, hasPtrMethods := false, false
	for i := 0; i < st.NumFields(); i++ {
		if p, _ := syncType(st.Field(i).Type()); p != "" {
			hasSync = true
		}
	}
	for i := 0; i < named.NumMethods(); i++ {
		if _, ok := named.Method(i).Type().(*types.Signature).Recv().Type().(*types.Pointer); ok {
			hasPtrMethods = true
		}
	}
	switch {
	case shared && kind != "":
		g.report("%s: %s is classified twice", file, name.Name)
	case !shared && kind == "":
		g.report("%s: unclassified struct type %s", file, name.Name)
	case kind == structValue && hasPtrMethods:
		g.report("%s: %s is a value type with pointer-receiver methods: classify it as shared or content", file, name.Name)
	case (kind == structValue || kind == structContent) && hasSync:
		g.report("%s: %s has synchronization fields: classify it as shared", file, name.Name)
	}
	if !shared {
		return
	}
	// Field paths: nested sets contribute "field.sub" for each of their fields.
	paths := map[string]types.Type{}
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if n, ok := f.Type().(*types.Named); ok && g.typeName(n) != "" && g.own.structs[n.Obj().Name()] == structNested {
			sub := n.Underlying().(*types.Struct)
			for j := 0; j < sub.NumFields(); j++ {
				paths[f.Name()+"."+sub.Field(j).Name()] = sub.Field(j).Type()
			}
			continue
		}
		paths[f.Name()] = f.Type()
	}
	for path, typ := range paths {
		k, ok := fields[path]
		if !ok {
			g.report("%s: %s.%s: unclassified field", file, name.Name, path)
			continue
		}
		pkg, tname := syncType(typ)
		basic, _ := typ.Underlying().(*types.Basic)
		switch {
		case k == kindLock && (pkg != "sync" || (tname != "Mutex" && tname != "RWMutex")):
			g.report("%s: %s.%s: classified lock, but it isn't a sync.Mutex or sync.RWMutex", file, name.Name, path)
		case k == kindAtomic && pkg != "sync/atomic":
			g.report("%s: %s.%s: classified atomic, but it isn't a sync/atomic type", file, name.Name, path)
		case k == kindAtomicOps && (basic == nil || basic.Info()&types.IsInteger == 0):
			g.report("%s: %s.%s: classified atomicops, but it isn't an integer", file, name.Name, path)
		case k == kindSynced && g.own.fields[g.typeName(typ)] == nil:
			g.report("%s: %s.%s: classified synced, but its type isn't a shared type", file, name.Name, path)
		case pkg == "sync" && k != kindLock, pkg == "sync/atomic" && k != kindAtomic:
			g.report("%s: %s.%s: a %s field must be classified %s", file, name.Name, path, pkg, map[string]string{"sync": kindLock, "sync/atomic": kindAtomic}[pkg])
		case strings.HasPrefix(k, "by ") && fields[strings.TrimPrefix(k, "by ")] != kindLock:
			g.report("%s: %s.%s: owned by %s, which isn't a classified lock of %s", file, name.Name, path, strings.TrimPrefix(k, "by "), name.Name)
		}
	}
	for path := range fields {
		if _, ok := paths[path]; !ok {
			g.report("%s.%s: classified, but no such field", name.Name, path)
		}
	}
}

func (g *guard) classifyGlobal(name *ast.Ident) {
	file := g.file(name.Pos())
	k, ok := g.own.globals[name.Name]
	if !ok {
		g.report("%s: unclassified package variable %s", file, name.Name)
		return
	}
	obj := g.info.Defs[name]
	if obj == nil {
		return
	}
	pkg, tname := syncType(obj.Type())
	switch {
	case k == kindLock && (pkg != "sync" || (tname != "Mutex" && tname != "RWMutex")):
		g.report("%s: %s: classified lock, but it isn't a sync.Mutex or sync.RWMutex", file, name.Name)
	case strings.HasPrefix(k, "by ") && g.own.globals[strings.TrimPrefix(k, "by ")] != kindLock:
		g.report("%s: %s: owned by %s, which isn't a classified lock", file, name.Name, strings.TrimPrefix(k, "by "))
	case k != kindLock && k != kindImmutable && !strings.HasPrefix(k, "by "):
		g.report("%s: %s: package variables are lock, immutable or owned by a lock", file, name.Name)
	}
}

// lockCall classifies stmt as lockExpr.Lock ("lock", exclusive) or .RLock
// ("lock", shared), .Unlock/.RUnlock ("unlock") or a deferred unlock
// ("defer").
func lockCall(stmt ast.Stmt, lockExpr string) (string, bool) {
	var call *ast.CallExpr
	deferred := false
	switch st := stmt.(type) {
	case *ast.ExprStmt:
		call, _ = st.X.(*ast.CallExpr)
	case *ast.DeferStmt:
		call, deferred = st.Call, true
	}
	if call == nil || len(call.Args) != 0 {
		return "", false
	}
	switch types.ExprString(call.Fun) {
	case lockExpr + ".Lock", lockExpr + ".RLock":
		if !deferred {
			return "lock", strings.HasSuffix(types.ExprString(call.Fun), ".Lock")
		}
	case lockExpr + ".Unlock", lockExpr + ".RUnlock":
		if deferred {
			return "defer", false
		}
		return "unlock", false
	}
	return "", false
}

// lockSpans finds where lockExpr is held in body.
//
// A lock statement opens a held span in its own statement list, closed by the
// next unlock statement in that list, or at the end of the body once the
// unlock is deferred, or at the end of the list otherwise. An unlock inside a
// nested block ends the protection for the rest of that block. A span only
// protects code in the same closure (or the function body itself): a closure
// can run after the release.
func lockSpans(body *ast.BlockStmt, lockExpr string, ownerOf func(token.Pos) *ast.FuncLit) (held, released []span) {
	ast.Inspect(body, func(n ast.Node) bool {
		var list []ast.Stmt
		var end token.Pos
		switch b := n.(type) {
		case *ast.BlockStmt:
			list, end = b.List, b.End()
		case *ast.CaseClause:
			list, end = b.Body, b.End()
		case *ast.CommClause:
			list, end = b.Body, b.End()
		}
		open, deferred, excl := token.NoPos, false, false
		for _, stmt := range list {
			switch kind, x := lockCall(stmt, lockExpr); kind {
			case "lock":
				if open == token.NoPos {
					open, deferred, excl = stmt.End(), false, x
				}
			case "defer":
				if open != token.NoPos {
					deferred = true
				}
			case "unlock":
				if open != token.NoPos && !deferred {
					held = append(held, span{open, stmt.Pos(), ownerOf(open), excl})
					open = token.NoPos
				} else if open == token.NoPos && n != body {
					released = append(released, span{stmt.Pos(), end, ownerOf(stmt.Pos()), false})
				}
			}
		}
		if open != token.NoPos {
			to := end
			if deferred {
				to = body.End()
			}
			held = append(held, span{open, to, ownerOf(open), excl})
		}
		return true
	})
	return held, released
}

// writes returns the selectors and identifiers fn writes: assigned,
// incremented, deleted from or cleared, address taken (except as an argument
// of a sync/atomic function), or the receiver of a pointer method called on
// an addressable value. Writing x.f[k], x.f.g or *x.f writes x.f.
func (g *guard) writes(body *ast.BlockStmt) (sels map[*ast.SelectorExpr]bool, idents map[*ast.Ident]bool, atomicArgs map[ast.Expr]bool) {
	sels, idents, atomicArgs = map[*ast.SelectorExpr]bool{}, map[*ast.Ident]bool{}, map[ast.Expr]bool{}
	var mark func(e ast.Expr)
	mark = func(e ast.Expr) {
		switch x := ast.Unparen(e).(type) {
		case *ast.SelectorExpr:
			sels[x] = true
			mark(x.X)
		case *ast.IndexExpr:
			mark(x.X)
		case *ast.IndexListExpr:
			mark(x.X)
		case *ast.StarExpr:
			mark(x.X)
		case *ast.Ident:
			idents[x] = true
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok {
					if pn, ok := g.info.Uses[pkg].(*types.PkgName); ok && pn.Imported().Path() == "sync/atomic" {
						for _, arg := range call.Args {
							if u, ok := arg.(*ast.UnaryExpr); ok && u.Op == token.AND {
								atomicArgs[ast.Unparen(u.X)] = true
							}
						}
					}
				}
			}
		}
		return true
	})
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE {
				for _, lhs := range x.Lhs {
					mark(lhs)
				}
			}
		case *ast.IncDecStmt:
			mark(x.X)
		case *ast.RangeStmt:
			if x.Tok == token.ASSIGN {
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if e != nil {
						mark(e)
					}
				}
			}
		case *ast.UnaryExpr:
			if x.Op == token.AND && !atomicArgs[ast.Unparen(x.X)] {
				mark(x.X)
			}
		case *ast.CallExpr:
			if id, ok := ast.Unparen(x.Fun).(*ast.Ident); ok && len(x.Args) > 0 {
				if b, ok := g.info.Uses[id].(*types.Builtin); ok && (b.Name() == "delete" || b.Name() == "clear") {
					mark(x.Args[0])
				}
			}
			if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok {
				if s := g.info.Selections[sel]; s != nil && s.Kind() == types.MethodVal {
					_, ptrRecv := s.Obj().Type().(*types.Signature).Recv().Type().(*types.Pointer)
					_, ptrX := g.info.TypeOf(sel.X).(*types.Pointer)
					if ptrRecv && !ptrX {
						mark(sel.X)
					}
				}
			}
		}
		return true
	})
	return sels, idents, atomicArgs
}

// analyzeFunc checks every access fn makes to shared state.
//
// A field of a shared type is accessed through the receiver of one of that
// type's methods: any other form (a copy of the receiver, a parameter, another
// type's field) can't be tied to a lock and is reported, except in the
// construction functions (New..., With...), which work on values not yet
// published. Reads need the owner held, writes need it held exclusively; a
// method that leaves this to its callers records what it needs (resolved in
// resolveHelpers). Package variables need their lock held in the accessing
// function itself.
func (g *guard) analyzeFunc(fn *ast.FuncDecl, file string) {
	f := &funcInfo{name: fn.Name.Name, exported: fn.Name.IsExported(), needs: map[string]int{}}
	var recvObj types.Object
	recvName := ""
	if fn.Recv != nil && len(fn.Recv.List) == 1 {
		f.typ = g.namedStruct(fn.Recv.List[0].Type)
		f.key = file + ": " + strings.TrimPrefix(types.ExprString(fn.Recv.List[0].Type), "*") + "." + f.name
		if len(fn.Recv.List[0].Names) == 1 {
			recvObj = g.info.Defs[fn.Recv.List[0].Names[0]]
			recvName = fn.Recv.List[0].Names[0].Name
		}
		if g.own.fields[f.typ] == nil {
			f.typ = ""
		}
	} else {
		f.key = file + ": " + f.name
	}
	construction := fn.Recv == nil && (strings.HasPrefix(f.name, "New") || strings.HasPrefix(f.name, "With"))
	g.funcs = append(g.funcs, f)

	var closures []*ast.FuncLit
	var goCalls []span
	calledFuns := map[*ast.SelectorExpr]bool{}
	fieldParents := map[ast.Expr]bool{} // selectors whose field is selected in turn
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			closures = append(closures, x)
		case *ast.GoStmt:
			goCalls = append(goCalls, span{from: x.Pos(), to: x.End()})
		case *ast.CallExpr:
			if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok {
				calledFuns[sel] = true
			}
		case *ast.SelectorExpr:
			if s := g.info.Selections[x]; s != nil && s.Kind() == types.FieldVal {
				fieldParents[ast.Unparen(x.X)] = true
			}
		}
		return true
	})
	ownerOf := func(pos token.Pos) *ast.FuncLit {
		var owner *ast.FuncLit
		for _, c := range closures {
			if pos > c.Pos() && pos < c.End() && (owner == nil || c.Pos() > owner.Pos()) {
				owner = c
			}
		}
		return owner
	}
	f.inAsync = func(pos token.Pos) bool {
		for _, s := range goCalls {
			if pos > s.from && pos < s.to {
				return true
			}
		}
		return ownerOf(pos) != nil
	}

	spans := map[string][2][]span{}
	heldExpr := func(lockExpr string, pos token.Pos) (int, int) {
		s, ok := spans[lockExpr]
		if !ok {
			held, released := lockSpans(fn.Body, lockExpr, ownerOf)
			s = [2][]span{held, released}
			spans[lockExpr] = s
		}
		owner := ownerOf(pos)
		for _, r := range s[1] {
			if pos > r.from && pos < r.to && r.owner == owner {
				return 0, 0
			}
		}
		for i, h := range s[0] {
			if pos > h.from && pos < h.to && h.owner == owner {
				if h.exclusive {
					return exclusive, i + 1
				}
				return shared, i + 1
			}
		}
		return 0, 0
	}
	f.held = func(lock string, pos token.Pos) (int, int) {
		if recvName == "" {
			return 0, 0
		}
		return heldExpr(recvName+"."+lock, pos)
	}
	proper := func(root ast.Expr, typ string) bool {
		id, ok := ast.Unparen(root).(*ast.Ident)
		return ok && recvObj != nil && g.info.Uses[id] == recvObj && f.typ == typ
	}

	written, writtenIdents, atomicArgs := g.writes(fn.Body)
	tupleSpans := map[int]map[int]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			s := g.info.Selections[x]
			if s == nil {
				return true
			}
			if s.Kind() == types.MethodVal {
				typ := g.namedStruct(x.X)
				if g.own.fields[typ] == nil {
					return true
				}
				if proper(x.X, typ) {
					f.calls = append(f.calls, callSite{x.Sel.Name, x.Pos(), !calledFuns[x]})
				} else if !construction {
					g.foreign = append(g.foreign, foreignCall{f.key, typ, x.Sel.Name})
				}
				return true
			}
			if s.Kind() != types.FieldVal {
				return true
			}
			typ, path, root := g.stateAccess(x)
			if typ == "" {
				return true
			}
			kind := g.own.fields[typ][path]
			if g.own.structs[g.namedStruct(x)] == structNested && !isPointer(g.info.TypeOf(x)) {
				if !fieldParents[x] && !construction {
					g.report("%s (uses the nested set %s.%s as a whole)", f.key, typ, path)
				}
				return true
			}
			switch {
			case kind == "" || kind == kindLock || kind == kindAtomic || kind == kindSynced:
			case kind == kindAtomicOps:
				if !atomicArgs[x] {
					g.report("%s (non-atomic access to %s)", f.key, path)
				}
			case kind == kindImmutable:
				if written[x] && !construction {
					g.report("%s (writes the immutable %s.%s)", f.key, typ, path)
				}
			case construction:
			case !proper(root, typ):
				g.report("%s (accesses %s.%s outside its own receiver)", f.key, typ, path)
			default:
				lock := strings.TrimPrefix(kind, "by ")
				need := shared
				if written[x] {
					need = exclusive
				}
				mode, id := f.held(lock, x.Pos())
				if mode < need && f.needs[lock] < need {
					f.needs[lock] = need
				}
				for i, tup := range g.own.tuples {
					if tup.typ != typ {
						continue
					}
					for _, field := range tup.fields {
						if field == path {
							if tupleSpans[i] == nil {
								tupleSpans[i] = map[int]bool{}
							}
							tupleSpans[i][id] = true
						}
					}
				}
			}
		case *ast.Ident:
			obj, _ := g.info.Uses[x].(*types.Var)
			if obj == nil || obj.Parent() != g.pkg.Scope() {
				return true
			}
			switch kind := g.own.globals[obj.Name()]; {
			case kind == kindImmutable:
				if writtenIdents[x] && f.name != "init" {
					g.report("%s (writes the immutable %s)", f.key, obj.Name())
				}
			case strings.HasPrefix(kind, "by "):
				lock := strings.TrimPrefix(kind, "by ")
				if mode, _ := heldExpr(lock, x.Pos()); mode < exclusive && (writtenIdents[x] || mode == 0) {
					g.report("%s (needs %s)", f.key, lock)
				}
			}
		}
		return true
	})
	for i, ids := range tupleSpans {
		if len(ids) > 1 {
			g.report("%s (accesses the set {%s} in %d critical sections)", f.key, strings.Join(g.own.tuples[i].fields, ", "), len(ids))
		}
	}
}

func isPointer(t types.Type) bool {
	_, ok := t.(*types.Pointer)
	return ok
}

// stateAccess resolves x to a classified field: its shared type, its path
// and the expression it is reached through. A nested set's field is resolved
// through the shared field holding it.
func (g *guard) stateAccess(x *ast.SelectorExpr) (typ, path string, root ast.Expr) {
	base := g.namedStruct(x.X)
	if g.own.fields[base] != nil {
		return base, x.Sel.Name, x.X
	}
	if g.own.structs[base] == structNested {
		if outer, ok := ast.Unparen(x.X).(*ast.SelectorExpr); ok {
			if t := g.namedStruct(outer.X); g.own.fields[t] != nil {
				return t, outer.Sel.Name + "." + x.Sel.Name, outer.X
			}
		}
	}
	return "", "", nil // a value type's field, or a copy of a nested set
}

// resolveHelpers decides which methods may leave their locks to their
// callers. A method may only if it is unexported or named ...Locked, and
// every call to it from its own type's methods holds the locks it needs, in
// the mode it needs them (directly, or because the caller is itself such a
// method needing them); calls from a go statement or a closure, and method
// values, never count, since they can run after the release. A method
// without such callers, or an exported method without the Locked suffix,
// must hold the lock itself. Calls to a method that relies on its callers
// from anything but a method of its type are reported.
func (g *guard) resolveHelpers() {
	byName := map[string]*funcInfo{}
	for _, m := range g.funcs {
		if m.typ != "" {
			byName[m.typ+"."+m.name] = m
		}
	}
	mayRely := func(m *funcInfo) bool { return !m.exported || strings.HasSuffix(m.name, "Locked") }
	// A method that calls such a helper synchronously without holding a lock
	// the helper needs (in the mode it needs) needs that lock too.
	for changed := true; changed; {
		changed = false
		for _, m := range g.funcs {
			for _, c := range m.calls {
				callee := byName[m.typ+"."+c.callee]
				if callee == nil || callee == m || !mayRely(callee) || c.async || m.inAsync(c.pos) {
					continue
				}
				for lock, need := range callee.needs {
					if mode, _ := m.held(lock, c.pos); mode < need && m.needs[lock] < need {
						m.needs[lock] = need
						changed = true
					}
				}
			}
		}
	}
	relies := map[*funcInfo]bool{}
	for _, m := range g.funcs {
		if len(m.needs) > 0 && m.typ != "" && mayRely(m) {
			relies[m] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for m := range relies {
			callers, ok := 0, true
			for _, caller := range g.funcs {
				if caller.typ != m.typ {
					continue
				}
				for _, c := range caller.calls {
					if c.callee != m.name {
						continue
					}
					callers++
					for lock, need := range m.needs {
						mode, _ := caller.held(lock, c.pos)
						if c.async || caller.inAsync(c.pos) || (mode < need && !(relies[caller] && caller.needs[lock] >= need)) {
							ok = false
						}
					}
				}
			}
			if callers == 0 || !ok {
				delete(relies, m)
				changed = true
			}
		}
	}
	for _, m := range g.funcs {
		if relies[m] {
			continue
		}
		for lock, need := range m.needs {
			if need == exclusive {
				g.report("%s (needs %s exclusively)", m.key, lock)
			} else {
				g.report("%s (needs %s)", m.key, lock)
			}
		}
	}
	for _, c := range g.foreign {
		if callee := byName[c.typ+"."+c.callee]; callee != nil && relies[callee] {
			g.report("%s (calls %s.%s, which relies on its callers for its locks)", c.key, c.typ, c.callee)
		}
	}
}

// packageFiles parses the package's production sources.
func packageFiles(t *testing.T, fset *token.FileSet) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	files := map[string]*ast.File{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		files[name] = file
	}
	require.GreaterOrEqual(t, len(files), 8, "the guard must walk the package sources")
	return files
}

// Every piece of state in pkg/inference has a classified owner, and every
// access goes through it. The check is structural (go/types over the package
// sources), so it does not depend on scheduling:
//
//   - every struct type and package variable is classified (inferenceOwnership),
//     and every field of every shared type is a lock, atomic, owned by a lock,
//     immutable after construction, or a self-locking shared value; kinds are
//     checked against the field types, and an unclassified type, field or
//     variable fails, so new state can't slip in unowned;
//   - every read of an owned field holds its lock, every write holds it
//     exclusively (Lock, not RLock), either in the method itself or, for an
//     unexported or ...Locked helper, at every call from its type's methods;
//   - owned fields are only reached through the receiver of their own type's
//     methods: aliases, parameters and other types' methods are rejected
//     (construction functions excepted);
//   - each statistics tuple (stateTuples) is accessed within one critical
//     section per function: a snapshot or update split across two sections
//     fails (the B1 and split-counter shapes);
//   - package variables owned by a lock are only accessed holding it, and
//     the read-only defaults are never written.
//
// Limits: objects reached through an owned field (map entries, published
// configurations, confidence records) are covered while they are accessed
// through that field; once a pointer to one is copied out of the critical
// section, the guard doesn't follow it, and the copy helpers (snapshot,
// copyThresholds) carry that contract. Values of other packages' types
// (filters, trackers, storage, the GPU index) are taken to synchronize
// themselves. Construction functions are trusted to work on unpublished
// values. reflect, unsafe and writes from other packages to the exported
// Default* maps are not tracked.
//
// Before the F-INFSTATS route it flagged ClusterIntegration.IsEnabled, the
// eight Engine component readers, EdgeDecay.Start and Stop with the
// unclassified worker fields ctx and wg, and the sync.Once singletons.
func TestSharedStateHoldsItsOwningLock(t *testing.T) {
	fset := token.NewFileSet()
	imp := &guardImporter{std: importer.ForCompiler(fset, "source", nil), stubs: map[string]*types.Package{}}
	files := packageFiles(t, fset)
	own := inferenceOwnership()
	base := ownershipViolations(t, fset, imp, files, own)
	assert.Empty(t, base, "\n"+strings.Join(base, "\n"))

	probes := []struct {
		name, body string
		edit       func(*ownership)
		flagged    []string
	}{
		{"held with defer", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.config != nil }`, nil, nil},
		{"no lock", `func (ci *ClusterIntegration) A() bool { return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu)"}},
		{"another lock", `func (ed *EdgeDecay) A() bool { ed.mu.Lock(); defer ed.mu.Unlock(); return ed.cancel != nil }`, nil, []string{"probe.go: EdgeDecay.A (needs lifecycle)"}},
		{"released before the read", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); ci.mu.RUnlock(); return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu)"}},
		{"lock in a nested block", `func (ed *EdgeDecay) A(ids []int) { for range ids { ed.mu.RLock(); _ = ed.reinforced; ed.mu.RUnlock() } }`, nil, nil},
		{"early release in a branch", `func (ci *ClusterIntegration) A(x bool) bool { ci.mu.RLock(); if x { ci.mu.RUnlock(); return ci.config != nil }; ci.mu.RUnlock(); return false }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu)"}},
		{"read in a closure", `func (ci *ClusterIntegration) A() func() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return func() bool { return ci.config != nil } }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu)"}},
		{"nested lock path", `func (ed *EdgeDecay) A() int64 { ed.stats.mu.Lock(); defer ed.stats.mu.Unlock(); return ed.stats.EdgesScanned }`, nil, nil},
		{"nested set without its lock", `func (ed *EdgeDecay) A() int64 { ed.mu.Lock(); defer ed.mu.Unlock(); return ed.stats.EdgesScanned }`, nil, []string{"probe.go: EdgeDecay.A (needs stats.mu)"}},
		{"nested set used as a whole", `func (ed *EdgeDecay) A() *EdgeDecayStats { return &ed.stats }`, nil, []string{"probe.go: EdgeDecay.A (uses the nested set EdgeDecay.stats as a whole)"}},
		{"helper with locked callers", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil, nil},
		{"helper with one unlocked caller", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h() }
func (ci *ClusterIntegration) B() bool { return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.B (needs mu)", "probe.go: ClusterIntegration.h (needs mu)"}},
		{"helper started by go", `func (ci *ClusterIntegration) A() { ci.mu.Lock(); defer ci.mu.Unlock(); go ci.h() }
func (ci *ClusterIntegration) h() { _ = ci.config }`, nil, []string{"probe.go: ClusterIntegration.h (needs mu)"}},
		{"helper as a method value", `func (ci *ClusterIntegration) A() func() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.h (needs mu)"}},
		{"helper without callers", `func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.h (needs mu)"}},
		{"exported helper", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.H() }
func (ci *ClusterIntegration) H() bool { return ci.config != nil }`, nil, []string{"probe.go: ClusterIntegration.H (needs mu)"}},
		{"exported Locked helper", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.HLocked() }
func (ci *ClusterIntegration) HLocked() bool { return ci.config != nil }`, nil, nil},
		{"helper chain", `func (ci *ClusterIntegration) A() bool { ci.mu.Lock(); defer ci.mu.Unlock(); return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.g() }
func (ci *ClusterIntegration) g() bool { return ci.config != nil }`, nil, nil},
		{"helper called from a free function", `func probeCall(ci *ClusterIntegration) bool { return ci.h() }
func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil, []string{"probe.go: probeCall (calls ClusterIntegration.h, which relies on its callers for its locks)"}},
		{"write under a read lock", `func (ci *ClusterIntegration) A(c *ClusterConfig) { ci.mu.RLock(); defer ci.mu.RUnlock(); ci.config = c }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu exclusively)"}},
		{"map delete under a read lock", `func (ct *CooldownTable) A() { ct.mu.RLock(); defer ct.mu.RUnlock(); delete(ct.entries, "k") }`, nil, []string{"probe.go: CooldownTable.A (needs mu exclusively)"}},
		{"entry write under a read lock", `func (ct *CooldownTable) A() { ct.mu.RLock(); defer ct.mu.RUnlock(); ct.entries["k"] = nil }`, nil, []string{"probe.go: CooldownTable.A (needs mu exclusively)"}},
		{"write under the write lock", `func (ci *ClusterIntegration) A(c *ClusterConfig) { ci.mu.Lock(); defer ci.mu.Unlock(); ci.config = c }`, nil, nil},
		{"helper writing under a caller's read lock", `func (ci *ClusterIntegration) A() { ci.mu.RLock(); defer ci.mu.RUnlock(); ci.h() }
func (ci *ClusterIntegration) h() { ci.lastReclusterAt++ }`, nil, []string{"probe.go: ClusterIntegration.A (needs mu exclusively)", "probe.go: ClusterIntegration.h (needs mu exclusively)"}},
		{"receiver alias", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); x := ci; return x.config != nil }`, nil, []string{"probe.go: ClusterIntegration.A (accesses ClusterIntegration.config outside its own receiver)"}},
		{"free function", `func probeRead(ci *ClusterIntegration) bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.config != nil }`, nil, []string{"probe.go: probeRead (accesses ClusterIntegration.config outside its own receiver)"}},
		{"another type's state", `func (e *Engine) A(ct *CooldownTable) int { return len(ct.entries) }`, nil, []string{"probe.go: Engine.A (accesses CooldownTable.entries outside its own receiver)"}},
		{"construction function", `func NewProbe() *CooldownTable { ct := &CooldownTable{}; ct.entries = nil; return ct }`, nil, nil},
		{"split counter update", `func (c *checkCounters) A() { c.mu.Lock(); c.checks++; c.mu.Unlock(); c.mu.Lock(); c.allowed++; c.mu.Unlock() }`, nil, []string{"probe.go: checkCounters.A (accesses the set {checks, allowed, blocked} in 2 critical sections)"}},
		{"split snapshot", `func (t *TopologyIntegration) A() int64 { t.graphMu.RLock(); n := t.buildsCompleted; t.graphMu.RUnlock(); t.graphMu.RLock(); defer t.graphMu.RUnlock(); _ = t.totalBuildTime; return n }`, nil, []string{"probe.go: TopologyIntegration.A (accesses the set {lastBuildTime, totalBuildTime, buildsCompleted} in 2 critical sections)"}},
		{"B1: build stats read after the release", `func (t *TopologyIntegration) A() int64 { t.graphMu.RLock(); _ = t.cachedGraph; t.graphMu.RUnlock(); return t.buildsCompleted }`, nil, []string{"probe.go: TopologyIntegration.A (needs graphMu)"}},
		{"atomic access", `func (t *TopologyIntegration) A() int64 { return atomic.LoadInt64(&t.predictionCount) }`, nil, nil},
		{"plain access to an atomic counter", `func (t *TopologyIntegration) A() int64 { return t.predictionCount }`, nil, []string{"probe.go: TopologyIntegration.A (non-atomic access to predictionCount)"}},
		{"write to an immutable field", `func (ci *ClusterIntegration) A() { ci.mu.Lock(); defer ci.mu.Unlock(); ci.clusterIndex = nil }`, nil, []string{"probe.go: ClusterIntegration.A (writes the immutable ClusterIntegration.clusterIndex)"}},
		{"global without its lock", `func probeGlobal() *CooldownTable { return globalCooldownTable }`, nil, []string{"probe.go: probeGlobal (needs globalCooldownMu)"}},
		{"global with its lock", `func probeGlobal() *CooldownTable { globalCooldownMu.Lock(); defer globalCooldownMu.Unlock(); return globalCooldownTable }`, nil, nil},
		{"write to a read-only default", `func probeDefaults() { DefaultCooldowns["x"] = 0 }`, nil, []string{"probe.go: probeDefaults (writes the immutable DefaultCooldowns)"}},
		{"unclassified type", `type probeState struct { mu sync.Mutex; n int }`, nil, []string{"probe.go: unclassified struct type probeState"}},
		{"value type holding state", `type probeValue struct{ n int }
func (p *probeValue) Inc() { p.n++ }`, func(o *ownership) { o.structs["probeValue"] = structValue }, []string{"probe.go: probeValue is a value type with pointer-receiver methods: classify it as shared or content"}},
		{"unclassified package variable", `var probeCounter int`, nil, []string{"probe.go: unclassified package variable probeCounter"}},
		{"unclassified field", ``, func(o *ownership) { delete(o.fields["CooldownTable"], "defaults") }, []string{"cooldown.go: CooldownTable.defaults: unclassified field"}},
		{"classified field that doesn't exist", ``, func(o *ownership) { o.fields["CooldownTable"]["gone"] = by("mu") }, []string{"CooldownTable.gone: classified, but no such field"}},
		{"plain field classified as a lock", ``, func(o *ownership) { o.fields["ClusterIntegration"]["lastReclusterAt"] = kindLock }, []string{"cluster_integration.go: ClusterIntegration.lastReclusterAt: classified lock, but it isn't a sync.Mutex or sync.RWMutex"}},
	}
	known := map[string]bool{}
	for _, p := range base {
		known[p] = true
	}
	for _, probe := range probes {
		probeFiles := map[string]*ast.File{}
		for name, file := range files {
			probeFiles[name] = file
		}
		src := "package inference\n\nimport (\n\t\"sync\"\n\t\"sync/atomic\"\n)\n\nvar (\n\t_ sync.Mutex\n\t_ atomic.Bool\n)\n\n" + probe.body + "\n"
		file, err := parser.ParseFile(fset, "probe.go", src, 0)
		require.NoError(t, err, "probe %q", probe.name)
		probeFiles["probe.go"] = file
		o := own.clone()
		if probe.edit != nil {
			probe.edit(&o)
		}
		var got []string
		for _, p := range ownershipViolations(t, fset, imp, probeFiles, o) {
			if !known[p] {
				got = append(got, p)
			}
		}
		assert.Equal(t, probe.flagged, got, "probe %q", probe.name)
	}
}
