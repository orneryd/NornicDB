package search

// Every exported operation that changes what the search indexes hold, or what
// a write applies to, enters the build sequence (build_write_queue.go), so a
// build overlapping it cannot reorder it. The check is mechanical: each
// exported Service method's in-package call closure is computed from the
// source. Each top-level statement of the method that writes index state
// (through the receiver, or through a relationship-search value) must reach
// the sequence's entry points itself, or follow a statement that calls an
// entry point and returns when the call was queued or failed. A method that
// needs no ordering is listed below with the reason.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// buildSequenceEntries are the entry points into the build sequence.
var buildSequenceEntries = map[string]bool{
	"readLockOrQueue": true, "queueBuildWrite": true, "orderWithBuild": true,
	"orderWithBuildEnd": true, "orderDimensionChange": true, "orderIndexFlags": true, "orderRelationshipUpdate": true,
}

// buildSequenceCacheWriters write only a re-derivable cache and need no
// ordering with the sequence; the guard treats them as ordered.
var buildSequenceCacheWriters = map[string]string{
	"readChunkSpans": "fills the chunk-span cache of a search hit, under indexMu and only while the index revision is unchanged and the node has no entry; indexing or removing the node replaces or drops the entry, so a write the sequence applies later still decides it",
}

// buildSequenceExempt are writers that need no ordering with the sequence.
var buildSequenceExempt = map[string]string{
	"BuildIndexes":             "the build itself, the sequence's owner",
	"ClearVectorIndex":         "takes buildMu exclusively, after the build installed its sequence",
	"ClearIndexes":             "takes buildMu exclusively, after the build installed its sequence",
	"Close":                    "cancels and joins the build, which installs its sequence on exit",
	"PersistIndexesToDisk":     "writes files, not index contents",
	"MarkReadyDisabled":        "service construction, before any build",
	"EnableRelationshipSearch": "service construction, before any build",
	"StartRelationshipIndex":   "starts the relationship loader, whose stored copy yields to every live update and removal made while it runs (rs.live), and the embedding worker, which publishes only for an unchanged source",
}

// buildSequenceState are the Service fields whose change alters what the
// indexes hold or what a write applies to.
var buildSequenceState = map[string]bool{
	"fulltextRef": true, "vectorIndex": true, "vectorFileStore": true,
	"nodeLabels": true, "nodeTypes": true, "nodeNamedVector": true, "nodePropVector": true,
	"nodeChunkVectors": true, "nodeChunkSpans": true, "edgeTypes": true, "edgePropVector": true,
	"relationships": true, "fulltextProperties": true, "relationshipFulltextProperties": true,
	"fulltextLabels": true, "embedTextOptions": true, "embeddingSpace": true, "bm25Enabled": true,
	"vectorEnabled": true, "vectorDimensionsFixed": true, "indexCapacityByNode": true, "indexCapacityByEdge": true,
}

var buildSequenceAtomic = map[string]bool{
	"bm25Enabled": true, "vectorEnabled": true, "vectorDimensionsFixed": true, "embeddingSpace": true,
	"relationships": true, "fulltextRef": true,
}

var buildSequenceMutators = map[string]bool{
	"Add": true, "Remove": true, "Index": true, "IndexBatch": true, "Clear": true, "Store": true, "Swap": true,
	"Update": true, "Load": true, "Delete": true, "Close": true, "Reset": true, "CompareAndSwap": true, "loadSnapshot": true,
}

type buildSequenceFunc struct {
	calls  map[string]bool
	writes bool
	recv   string
	body   *ast.BlockStmt
	rsVars map[string]bool
}

func TestBuildSequenceOrdersEveryIndexWriter(t *testing.T) {
	funcs := parseBuildSequenceFuncs(t)
	guard := newSequenceGuard(funcs)
	var violations []string
	names := make([]string, 0, len(funcs))
	for name := range funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if name[0] < 'A' || name[0] > 'Z' {
			continue
		}
		if _, ok := buildSequenceExempt[name]; ok {
			continue
		}
		if writers := guard.unordered(funcs[name]); len(writers) > 0 {
			violations = append(violations, name+" (writes in "+strings.Join(writers, ", ")+")")
		}
	}
	if len(violations) > 0 {
		t.Fatalf("exported index writers outside the build sequence (route them through readLockOrQueue/orderWithBuild, or list the reason in buildSequenceExempt): %s", strings.Join(violations, ", "))
	}
	for name := range buildSequenceCacheWriters {
		if funcs[name] == nil {
			t.Errorf("cache writer %s no longer exists; remove it from the list", name)
		}
	}
	for name := range buildSequenceExempt {
		if funcs[name] == nil {
			t.Errorf("exempt writer %s no longer exists; remove it from the list", name)
		}
	}
}

// TestBuildSequenceGuardSeesBypasses: the guard reports a writer that
// bypasses the sequence through the receiver, through a relationship-search
// value, before its entry call, in a branch the entry call does not
// dominate, or in code a goto can reach; it accepts the dominated forms.
func TestBuildSequenceGuardSeesBypasses(t *testing.T) {
	analyze := func(src, name string) []string {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package search\n"+src, 0)
		if err != nil {
			t.Fatal(err)
		}
		funcs := map[string]*buildSequenceFunc{}
		collectBuildSequenceFuncs(file, funcs)
		return newSequenceGuard(funcs).unordered(funcs[name])
	}
	for _, src := range []string{
		"func (s *Service) Bypass(id string) { rs := s.relationshipSearch(); rs.fulltext.Remove(id) }",
		"func (s *Service) Bypass(id string) { s.mu.Lock(); delete(s.edgePropVector, id); s.mu.Unlock() }",
		"func (s *Service) Bypass() { s.note(); if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{}); queued { return } }\nfunc (s *Service) note() { delete(s.edgeTypes, \"x\") }",
		"func (s *Service) Bypass(flag bool, id string) { if flag { s.queueBuildWrite(deferredIndexWrite{}) } else { delete(s.edgeTypes, id) } }",
		"func (s *Service) Bypass(k int, id string) { switch k { case 0: s.orderWithBuild(func() {}); case 1: delete(s.edgeTypes, id) } }",
		"func (s *Service) Bypass(id string) { s.queueBuildWrite(deferredIndexWrite{}); goto done; done: delete(s.edgeTypes, id) }",
		"func (s *Service) Bypass(id string) { go func() { delete(s.edgeTypes, id) }(); s.queueBuildWrite(deferredIndexWrite{}) }",
	} {
		if len(analyze(src, "Bypass")) == 0 {
			t.Errorf("the guard missed the bypass in %q", src)
		}
	}
	for _, src := range []string{
		"func (s *Service) Ordered(id string) { if queued, err := s.readLockOrQueue(deferredIndexWrite{}); queued || err != nil { return }; delete(s.edgeTypes, id) }",
		"func (s *Service) Ordered(id string) { queued, _, err := s.queueBuildWrite(deferredIndexWrite{}); if queued || err != nil { return }; delete(s.edgeTypes, id) }",
		"func (s *Service) Ordered(id string) { s.orderWithBuild(func() { delete(s.edgeTypes, id) }) }",
		"func (s *Service) Ordered(flag bool, id string) { if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{}); !queued { if flag { delete(s.edgeTypes, id) } } }",
	} {
		if writers := analyze(src, "Ordered"); len(writers) > 0 {
			t.Errorf("the guard rejected a dominated write in %q: %v", src, writers)
		}
	}
}

// sequenceGuard proves, from the source, that every index-state write of a
// method is dominated by a call to a sequence entry point: on every path
// through the method, the entry call runs before the write. Go without goto
// is structured, so dominance follows the statement tree exactly:
//   - a statement that calls an entry dominates the statements after it in its
//     block;
//   - the init and condition of an if, for or switch dominate its bodies and
//     the statements after it, because they always run; an entry inside one
//     branch dominates only that branch;
//   - a closure passed to an entry call runs where the entry runs it, so it is
//     ordered by that call; any other closure is checked where it appears.
//
// A callee is ordered when every write in it is dominated in it. A goto makes
// the guard fail, since it breaks the structure the proof relies on.
type sequenceGuard struct {
	funcs map[string]*buildSequenceFunc
	memo  map[string]*bool
}

func newSequenceGuard(funcs map[string]*buildSequenceFunc) *sequenceGuard {
	return &sequenceGuard{funcs: funcs, memo: map[string]*bool{}}
}

// ordered reports whether every index-state write of name is dominated by an
// entry call, in name itself or in an ordered callee.
func (g *sequenceGuard) ordered(name string) bool {
	if _, cache := buildSequenceCacheWriters[name]; cache || buildSequenceEntries[name] {
		return true
	}
	fn := g.funcs[name]
	if fn == nil {
		return true // not a Service method of this package
	}
	if v, ok := g.memo[name]; ok {
		return v == nil || *v // in progress (recursion): its writes are checked where it started
	}
	g.memo[name] = nil
	result := len(g.unordered(fn)) == 0
	g.memo[name] = &result
	return result
}

// unordered returns what fn writes, directly or through an unordered callee,
// without an entry call dominating it.
func (g *sequenceGuard) unordered(fn *buildSequenceFunc) []string {
	var out []string
	g.block(fn, fn.body.List, false, &out)
	sort.Strings(out)
	return out
}

func (g *sequenceGuard) block(fn *buildSequenceFunc, stmts []ast.Stmt, dominated bool, out *[]string) {
	for _, stmt := range stmts {
		dominated = g.stmt(fn, stmt, dominated, out)
	}
}

// stmt checks one statement and reports whether an entry call dominates the
// statements after it.
func (g *sequenceGuard) stmt(fn *buildSequenceFunc, stmt ast.Stmt, dominated bool, out *[]string) bool {
	switch x := stmt.(type) {
	case nil:
		return dominated
	case *ast.BranchStmt:
		if x.Tok == token.GOTO {
			*out = append(*out, "goto (the dominance proof needs structured code)")
		}
		return dominated
	case *ast.LabeledStmt:
		return g.stmt(fn, x.Stmt, dominated, out)
	case *ast.BlockStmt:
		g.block(fn, x.List, dominated, out)
		return dominated
	case *ast.IfStmt:
		d := g.stmt(fn, x.Init, dominated, out)
		d = g.simple(fn, x.Cond, d, out)
		g.block(fn, x.Body.List, d, out)
		g.stmt(fn, x.Else, d, out)
		return d
	case *ast.ForStmt:
		d := g.stmt(fn, x.Init, dominated, out)
		if x.Cond != nil {
			d = g.simple(fn, x.Cond, d, out)
		}
		g.stmt(fn, x.Post, d, out)
		g.block(fn, x.Body.List, d, out)
		return d
	case *ast.RangeStmt:
		d := g.simple(fn, x.X, dominated, out)
		g.block(fn, x.Body.List, d, out)
		return d
	case *ast.SwitchStmt:
		d := g.stmt(fn, x.Init, dominated, out)
		if x.Tag != nil {
			d = g.simple(fn, x.Tag, d, out)
		}
		g.clauses(fn, x.Body, d, out)
		return d
	case *ast.TypeSwitchStmt:
		d := g.stmt(fn, x.Init, dominated, out)
		d = g.stmt(fn, x.Assign, d, out)
		g.clauses(fn, x.Body, d, out)
		return d
	case *ast.SelectStmt:
		g.clauses(fn, x.Body, dominated, out)
		return dominated
	default:
		return g.simple(fn, stmt, dominated, out)
	}
}

func (g *sequenceGuard) clauses(fn *buildSequenceFunc, body *ast.BlockStmt, dominated bool, out *[]string) {
	for _, clause := range body.List {
		switch c := clause.(type) {
		case *ast.CaseClause:
			d := dominated
			for _, e := range c.List {
				d = g.simple(fn, e, d, out)
			}
			g.block(fn, c.Body, d, out)
		case *ast.CommClause:
			d := g.stmt(fn, c.Comm, dominated, out)
			g.block(fn, c.Body, d, out)
		}
	}
}

// simple checks a statement or expression with no nested statement list of
// its own (closures aside) and reports whether it calls an entry.
func (g *sequenceGuard) simple(fn *buildSequenceFunc, n ast.Node, dominated bool, out *[]string) bool {
	if n == nil {
		return dominated
	}
	entered := false
	owned := map[*ast.FuncLit]bool{}
	var closures []*ast.FuncLit
	ast.Inspect(n, func(m ast.Node) bool {
		switch x := m.(type) {
		case *ast.FuncLit:
			if !owned[x] {
				closures = append(closures, x)
			}
			return false
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == fn.recv && buildSequenceEntries[sel.Sel.Name] {
					entered = true
					for _, arg := range x.Args {
						if lit, ok := arg.(*ast.FuncLit); ok {
							owned[lit] = true
						}
					}
				}
			}
		}
		return true
	})
	if !dominated {
		summary := summarizeBuildSequenceFlat(n, fn.recv, fn.rsVars)
		if summary.writes {
			*out = append(*out, "a direct write")
		}
		for callee := range summary.calls {
			if !g.ordered(callee) {
				*out = append(*out, callee)
			}
		}
	}
	for _, closure := range closures {
		g.block(fn, closure.Body.List, dominated, out)
	}
	return dominated || entered
}

func parseBuildSequenceFuncs(t *testing.T) map[string]*buildSequenceFunc {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	funcs := map[string]*buildSequenceFunc{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		collectBuildSequenceFuncs(file, funcs)
	}
	return funcs
}

func collectBuildSequenceFuncs(file *ast.File, funcs map[string]*buildSequenceFunc) {
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv == nil || fd.Body == nil || len(fd.Recv.List) != 1 || len(fd.Recv.List[0].Names) != 1 {
			continue
		}
		if star, ok := fd.Recv.List[0].Type.(*ast.StarExpr); !ok {
			continue
		} else if id, ok := star.X.(*ast.Ident); !ok || id.Name != "Service" {
			continue
		}
		recv := fd.Recv.List[0].Names[0].Name
		// The relationship index's values are written like receiver state:
		// parameters of that type, and locals taken from relationshipSearch().
		rsVars := map[string]bool{}
		for _, field := range fd.Type.Params.List {
			if star, ok := field.Type.(*ast.StarExpr); ok {
				if id, ok := star.X.(*ast.Ident); ok && id.Name == "relationshipSearch" {
					for _, n := range field.Names {
						rsVars[n.Name] = true
					}
				}
			}
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == len(as.Rhs) {
				for i, rhs := range as.Rhs {
					if c, ok := rhs.(*ast.CallExpr); ok {
						if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "relationshipSearch" {
							if id, ok := as.Lhs[i].(*ast.Ident); ok {
								rsVars[id.Name] = true
							}
						}
					}
				}
			}
			return true
		})
		fn := summarizeBuildSequence(fd.Body, recv, rsVars)
		fn.recv, fn.body, fn.rsVars = recv, fd.Body, rsVars
		funcs[fd.Name.Name] = fn
	}
}

// summarizeBuildSequence collects the receiver methods n calls and whether n
// writes index state directly.
func summarizeBuildSequence(n ast.Node, recv string, rsVars map[string]bool) *buildSequenceFunc {
	return summarizeBuildSequenceAs(n, recv, rsVars, false)
}

// summarizeBuildSequenceFlat is summarizeBuildSequence without the closures
// in n, which the guard checks where they run.
func summarizeBuildSequenceFlat(n ast.Node, recv string, rsVars map[string]bool) *buildSequenceFunc {
	return summarizeBuildSequenceAs(n, recv, rsVars, true)
}

func summarizeBuildSequenceAs(n ast.Node, recv string, rsVars map[string]bool, flat bool) *buildSequenceFunc {
	fn := &buildSequenceFunc{calls: map[string]bool{}}
	// field returns the receiver field an expression is rooted at, or "rs"
	// for a field of a relationship-search value.
	field := func(e ast.Expr) (string, bool) {
		for {
			switch x := e.(type) {
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok {
					if id.Name == recv {
						return x.Sel.Name, true
					}
					if rsVars[id.Name] {
						return "rs", true
					}
				}
				e = x.X
			case *ast.IndexExpr:
				e = x.X
			case *ast.StarExpr:
				e = x.X
			case *ast.CallExpr:
				e = x.Fun
			default:
				return "", false
			}
		}
	}
	isState := func(name string) bool { return name == "rs" || buildSequenceState[name] }
	ast.Inspect(n, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit && flat {
			return false
		}
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if name, ok := field(l); ok && isState(name) {
					fn.writes = true
				}
			}
		case *ast.IncDecStmt:
			if name, ok := field(x.X); ok && isState(name) {
				fn.writes = true
			}
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && (id.Name == "delete" || id.Name == "clear") && len(x.Args) > 0 {
				if name, ok := field(x.Args[0]); ok && isState(name) {
					fn.writes = true
				}
			}
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv {
				fn.calls[sel.Sel.Name] = true
			}
			if buildSequenceMutators[sel.Sel.Name] || strings.HasPrefix(sel.Sel.Name, "Set") || strings.HasPrefix(sel.Sel.Name, "Remove") {
				if name, ok := field(sel.X); ok && isState(name) && !(sel.Sel.Name == "Load" && buildSequenceAtomic[name]) {
					fn.writes = true
				}
				if inner, ok := sel.X.(*ast.CallExpr); ok {
					if m, ok := inner.Fun.(*ast.SelectorExpr); ok {
						if id, ok := m.X.(*ast.Ident); ok && id.Name == recv && (m.Sel.Name == "fulltext" || m.Sel.Name == "relationshipSearch") {
							fn.writes = true
						}
					}
				}
			}
		}
		return true
	})
	return fn
}
