package inference

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stateGroup is shared mutable state with one owning lock: every access to
// fields of a typ value (paths relative to the receiver) holds lock, a path
// relative to the receiver too ("mu", "stats.mu", "graphMu").
type stateGroup struct {
	typ    string
	lock   string
	fields []string
}

// stateGroups is the F-INFSTATS inventory (sig/handoff/finfstats-inventory.md):
// every piece of shared mutable state in pkg/inference and its owner.
// Immutable-after-construction fields (configs, injected dependencies) and
// fields with their own synchronization (sync and atomic types, internally
// locked components) are not listed.
var stateGroups = []stateGroup{
	{"ClusterIntegration", "mu", []string{"config", "lastReclusterAt"}},
	{"searchCounters", "mu", []string{"total", "clustered"}},
	{"CooldownTable", "mu", []string{"entries", "defaults"}},
	{"checkCounters", "mu", []string{"checks", "allowed", "blocked"}},
	{"EvidenceBuffer", "mu", []string{"entries", "thresholds", "totalAdded", "totalMaterialized", "totalExpired"}},
	{"EdgeDecay", "mu", []string{"reinforced"}},
	{"EdgeDecay", "lifecycle", []string{"ctx", "cancel", "done"}}, // ctx: the pre-route worker context field
	{"EdgeDecay", "stats.mu", []string{"stats.ScansCompleted", "stats.EdgesScanned", "stats.EdgesDecayed", "stats.EdgesDeleted", "stats.LastScanTime", "stats.LastScanDuration"}},
	{"HeimdallQC", "cacheMu", []string{"cache"}},
	{"HeimdallQC", "stats.mu", []string{"stats.BatchesProcessed", "stats.SuggestionsIn", "stats.SuggestionsOut", "stats.Augmented", "stats.Skipped", "stats.Errors", "stats.CacheHits", "stats.AvgLatencyMs", "stats.totalLatencyMs"}},
	{"Engine", "mu", []string{"accessHistory", "coAccessCounts", "similaritySearch", "topologyIntegration", "clusterIntegration", "kalmanAdapter", "heimdallQC", "cooldownTable", "evidenceBuffer", "edgeMetaStore", "nodeConfigStore"}},
	{"KalmanAdapter", "mu", []string{"edgeFilters", "cachedConfidence", "sessionCoAccess", "stats", "session", "tracker"}},
	{"TopologyIntegration", "graphMu", []string{"cachedGraph", "lastBuildTime", "totalBuildTime", "buildsCompleted"}},
	{"TopologyIntegration", "deltaMu", []string{"pendingDelta"}},
}

// atomicOnlyFields are plain integers accessed only through sync/atomic
// functions (&recv.field as an argument of an atomic.* call).
var atomicOnlyFields = map[string][]string{
	"TopologyIntegration": {"predictionCount", "predictionsRun"},
}

type span struct {
	from, to token.Pos
	owner    *ast.FuncLit // innermost closure containing the span's lock, nil for the method body
}

// lockedMethod is one method's view of its guarded accesses.
type lockedMethod struct {
	key      string // "file: Type.Method"
	typ      string
	name     string
	exported bool
	needs    map[string]bool // locks its unprotected accesses need
	calls    []callSite      // calls recv.m(...) to other methods of its type
	held     func(lock string, pos token.Pos) bool
	problems []string                 // atomic misuse, always reported
	inAsync  func(pos token.Pos) bool // inside a go statement or closure
}

type callSite struct {
	callee string
	pos    token.Pos
}

// lockCall classifies stmt as recv.lock.Lock/RLock ("lock"), Unlock/RUnlock
// ("unlock") or a deferred unlock ("defer").
func lockCall(stmt ast.Stmt, recv, lock string) string {
	var call *ast.CallExpr
	deferred := false
	switch st := stmt.(type) {
	case *ast.ExprStmt:
		call, _ = st.X.(*ast.CallExpr)
	case *ast.DeferStmt:
		call, deferred = st.Call, true
	}
	if call == nil || len(call.Args) != 0 {
		return ""
	}
	switch types.ExprString(call.Fun) {
	case recv + "." + lock + ".Lock", recv + "." + lock + ".RLock":
		if !deferred {
			return "lock"
		}
	case recv + "." + lock + ".Unlock", recv + "." + lock + ".RUnlock":
		if deferred {
			return "defer"
		}
		return "unlock"
	}
	return ""
}

// analyzeMethod finds fn's guarded accesses outside the owning lock.
//
// A lock statement opens a held span in its own statement list, closed by the
// next unlock statement in that list, or at the end of the body once the
// unlock is deferred, or at the end of the list otherwise. An unlock inside a
// nested block ends the protection for the rest of that block. A span only
// protects code in the same closure (or the method body itself): a closure
// can run after the release.
func analyzeMethod(fn *ast.FuncDecl, file string) *lockedMethod {
	if fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return nil
	}
	typ := strings.TrimPrefix(types.ExprString(fn.Recv.List[0].Type), "*")
	recv := fn.Recv.List[0].Names[0].Name
	m := &lockedMethod{key: file + ": " + typ + "." + fn.Name.Name, typ: typ, name: fn.Name.Name, exported: fn.Name.IsExported(), needs: map[string]bool{}}

	var closures []*ast.FuncLit
	var goCalls []posRange
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			closures = append(closures, x)
		case *ast.GoStmt:
			goCalls = append(goCalls, posRange{x.Pos(), x.End()})
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
	m.inAsync = func(pos token.Pos) bool {
		for _, g := range goCalls {
			if pos > g.from && pos < g.to {
				return true
			}
		}
		return ownerOf(pos) != nil
	}

	locks := map[string]bool{}
	for _, g := range stateGroups {
		if g.typ == typ {
			locks[g.lock] = true
		}
	}
	heldBy := map[string][]span{}
	releasedBy := map[string][]span{}
	for lock := range locks {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
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
			open, deferred := token.NoPos, false
			for _, stmt := range list {
				switch lockCall(stmt, recv, lock) {
				case "lock":
					if open == token.NoPos {
						open, deferred = stmt.End(), false
					}
				case "defer":
					if open != token.NoPos {
						deferred = true
					}
				case "unlock":
					if open != token.NoPos && !deferred {
						heldBy[lock] = append(heldBy[lock], span{open, stmt.Pos(), ownerOf(open)})
						open = token.NoPos
					} else if open == token.NoPos && n != fn.Body {
						releasedBy[lock] = append(releasedBy[lock], span{stmt.Pos(), end, ownerOf(stmt.Pos())})
					}
				}
			}
			if open != token.NoPos {
				to := end
				if deferred {
					to = fn.Body.End()
				}
				heldBy[lock] = append(heldBy[lock], span{open, to, ownerOf(open)})
			}
			return true
		})
	}
	m.held = func(lock string, pos token.Pos) bool {
		owner := ownerOf(pos)
		for _, r := range releasedBy[lock] {
			if pos > r.from && pos < r.to && r.owner == owner {
				return false
			}
		}
		for _, h := range heldBy[lock] {
			if pos > h.from && pos < h.to && h.owner == owner {
				return true
			}
		}
		return false
	}

	atomicArgs := map[token.Pos]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "atomic" {
				for _, arg := range call.Args {
					if u, ok := arg.(*ast.UnaryExpr); ok && u.Op == token.AND {
						atomicArgs[u.X.Pos()] = true
					}
				}
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == recv {
				m.calls = append(m.calls, callSite{sel.Sel.Name, call.Pos()})
			}
		}
		return true
	})

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		path := types.ExprString(sel)
		if !strings.HasPrefix(path, recv+".") {
			return true
		}
		path = strings.TrimPrefix(path, recv+".")
		for _, g := range stateGroups {
			if g.typ != typ {
				continue
			}
			for _, f := range g.fields {
				if path == f && !m.held(g.lock, sel.Pos()) {
					m.needs[g.lock] = true
				}
			}
		}
		for _, f := range atomicOnlyFields[typ] {
			if path == f && !atomicArgs[sel.Pos()] {
				m.problems = append(m.problems, m.key+" (non-atomic access to "+f+")")
			}
		}
		return true
	})
	return m
}

type posRange struct{ from, to token.Pos }

// lockOwnershipViolations returns the methods that touch guarded state
// without its owning lock. A method may leave the lock to its callers only if
// it is unexported or named ...Locked, and every call to it from its own
// type's methods holds the locks it needs (directly, or because the caller
// is itself such a method needing them); calls from a go statement or a
// closure never count, since they can run after the release. A method
// without such callers, or an exported method without the Locked suffix, must
// hold the lock itself.
func lockOwnershipViolations(files map[string]*ast.File) []string {
	var methods []*lockedMethod
	for name, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				if m := analyzeMethod(fn, name); m != nil {
					methods = append(methods, m)
				}
			}
		}
	}
	byName := map[string]*lockedMethod{}
	for _, m := range methods {
		byName[m.typ+"."+m.name] = m
	}
	mayRely := func(m *lockedMethod) bool { return !m.exported || strings.HasSuffix(m.name, "Locked") }
	// A method that calls such a helper synchronously without holding a lock
	// the helper needs needs that lock too (its own callers must provide it).
	for changed := true; changed; {
		changed = false
		for _, m := range methods {
			for _, c := range m.calls {
				callee := byName[m.typ+"."+c.callee]
				if callee == nil || callee == m || !mayRely(callee) || m.inAsync(c.pos) {
					continue
				}
				for lock := range callee.needs {
					if !m.held(lock, c.pos) && !m.needs[lock] {
						m.needs[lock] = true
						changed = true
					}
				}
			}
		}
	}
	// Candidates relying on their callers; shrink to the fixpoint.
	relies := map[*lockedMethod]bool{}
	for _, m := range methods {
		if len(m.needs) > 0 && mayRely(m) {
			relies[m] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for m := range relies {
			callers := 0
			ok := true
			for _, caller := range methods {
				if caller.typ != m.typ {
					continue
				}
				for _, c := range caller.calls {
					if c.callee != m.name {
						continue
					}
					callers++
					for lock := range m.needs {
						provided := !caller.inAsync(c.pos) && (caller.held(lock, c.pos) || (relies[caller] && caller.needs[lock]))
						if !provided {
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
	var out []string
	for _, m := range methods {
		out = append(out, m.problems...)
		if len(m.needs) > 0 && !relies[m] {
			out = append(out, m.key)
		}
	}
	sort.Strings(out)
	return out
}

func parseProbe(t *testing.T, fset *token.FileSet, body string) map[string]*ast.File {
	t.Helper()
	src := "package inference\n" + body + "\n"
	file, err := parser.ParseFile(fset, "probe.go", src, 0)
	require.NoError(t, err)
	return map[string]*ast.File{"probe.go": file}
}

// Every access to guarded shared state in pkg/inference holds that state's
// owning lock (stateGroups), and atomic-only counters are only accessed
// atomically. The check is structural, so it does not depend on scheduling.
// Before the F-INFSTATS route it flagged exactly ClusterIntegration.IsEnabled,
// the eight Engine component readers, and EdgeDecay.Start, Stop and worker
// (worker context and cancel state outside one lifecycle owner).
func TestSharedStateHoldsItsOwningLock(t *testing.T) {
	probes := []struct {
		name, body string
		flagged    []string
	}{
		{"held with defer", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.config != nil }`, nil},
		{"no lock", `func (ci *ClusterIntegration) A() bool { return ci.config != nil }`, []string{"probe.go: ClusterIntegration.A"}},
		{"another lock", `func (ed *EdgeDecay) A() bool { ed.mu.Lock(); defer ed.mu.Unlock(); return ed.cancel != nil }`, []string{"probe.go: EdgeDecay.A"}},
		{"released before the read", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); ci.mu.RUnlock(); return ci.config != nil }`, []string{"probe.go: ClusterIntegration.A"}},
		{"lock in a nested block", `func (ed *EdgeDecay) A(ids []int) { for range ids { ed.mu.RLock(); _ = ed.reinforced; ed.mu.RUnlock() } }`, nil},
		{"early release in a branch", `func (ci *ClusterIntegration) A(x bool) bool { ci.mu.RLock(); if x { ci.mu.RUnlock(); return ci.config != nil }; ci.mu.RUnlock(); return false }`, []string{"probe.go: ClusterIntegration.A"}},
		{"read in a closure", `func (ci *ClusterIntegration) A() func() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return func() bool { return ci.config != nil } }`, []string{"probe.go: ClusterIntegration.A"}},
		{"nested lock path", `func (ed *EdgeDecay) A() int64 { ed.stats.mu.Lock(); defer ed.stats.mu.Unlock(); return ed.stats.EdgesScanned }`, nil},
		{"helper with locked callers", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, nil},
		{"helper with one unlocked caller", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.h() }
func (ci *ClusterIntegration) B() bool { return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, []string{"probe.go: ClusterIntegration.B", "probe.go: ClusterIntegration.h"}},
		{"helper started by go", `func (ci *ClusterIntegration) A() { ci.mu.Lock(); defer ci.mu.Unlock(); go ci.h() }
func (ci *ClusterIntegration) h() { _ = ci.config }`, []string{"probe.go: ClusterIntegration.h"}},
		{"helper without callers", `func (ci *ClusterIntegration) h() bool { return ci.config != nil }`, []string{"probe.go: ClusterIntegration.h"}},
		{"exported helper", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.H() }
func (ci *ClusterIntegration) H() bool { return ci.config != nil }`, []string{"probe.go: ClusterIntegration.H"}},
		{"exported Locked helper", `func (ci *ClusterIntegration) A() bool { ci.mu.RLock(); defer ci.mu.RUnlock(); return ci.HLocked() }
func (ci *ClusterIntegration) HLocked() bool { return ci.config != nil }`, nil},
		{"helper chain", `func (ci *ClusterIntegration) A() bool { ci.mu.Lock(); defer ci.mu.Unlock(); return ci.h() }
func (ci *ClusterIntegration) h() bool { return ci.g() }
func (ci *ClusterIntegration) g() bool { return ci.config != nil }`, nil},
		{"atomic access", `func (t *TopologyIntegration) A() int64 { return atomic.LoadInt64(&t.predictionCount) }`, nil},
		{"plain access to an atomic counter", `func (t *TopologyIntegration) A() int64 { return t.predictionCount }`, []string{"probe.go: TopologyIntegration.A (non-atomic access to predictionCount)"}},
	}
	fset := token.NewFileSet()
	for _, probe := range probes {
		got := lockOwnershipViolations(parseProbe(t, fset, probe.body))
		require.Equal(t, probe.flagged, got, "probe %q", probe.name)
	}

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
	problems := lockOwnershipViolations(files)
	require.Empty(t, problems, "\n"+strings.Join(problems, "\n"))
}
