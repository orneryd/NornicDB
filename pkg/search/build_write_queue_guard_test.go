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
	"orderWithBuildEnd": true, "orderDimensionChange": true, "orderIndexFlags": true,
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
		if writers := unorderedBuildSequenceWrites(funcs, funcs[name]); len(writers) > 0 {
			violations = append(violations, name+" (writes in "+strings.Join(writers, ", ")+")")
		}
	}
	if len(violations) > 0 {
		t.Fatalf("exported index writers outside the build sequence (route them through readLockOrQueue/orderWithBuild, or list the reason in buildSequenceExempt): %s", strings.Join(violations, ", "))
	}
	for name := range buildSequenceExempt {
		if funcs[name] == nil {
			t.Errorf("exempt writer %s no longer exists; remove it from the list", name)
		}
	}
}

// TestBuildSequenceGuardSeesBypasses: the guard reports a writer that
// bypasses the sequence through the receiver, through a relationship-search
// value, or before its entry-point guard.
func TestBuildSequenceGuardSeesBypasses(t *testing.T) {
	for _, src := range []string{
		"func (s *Service) Bypass(id string) { rs := s.relationshipSearch(); rs.fulltext.Remove(id) }",
		"func (s *Service) Bypass(id string) { s.mu.Lock(); delete(s.edgePropVector, id); s.mu.Unlock() }",
		"func (s *Service) Bypass() { s.note(); if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{}); queued { return } }\nfunc (s *Service) note() { delete(s.edgeTypes, \"x\") }",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package search\n"+src, 0)
		if err != nil {
			t.Fatal(err)
		}
		funcs := map[string]*buildSequenceFunc{}
		collectBuildSequenceFuncs(file, funcs)
		if len(unorderedBuildSequenceWrites(funcs, funcs["Bypass"])) == 0 {
			t.Errorf("the guard missed the bypass in %q", src)
		}
	}
	ordered := "func (s *Service) Ordered(id string) { if queued, err := s.readLockOrQueue(deferredIndexWrite{}); queued || err != nil { return }; delete(s.edgeTypes, id) }"
	file, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package search\n"+ordered, 0)
	if err != nil {
		t.Fatal(err)
	}
	funcs := map[string]*buildSequenceFunc{}
	collectBuildSequenceFuncs(file, funcs)
	if writers := unorderedBuildSequenceWrites(funcs, funcs["Ordered"]); len(writers) > 0 {
		t.Errorf("the guard rejected a write after its entry-point guard: %v", writers)
	}
}

// unorderedBuildSequenceWrites returns the writers reached by a top-level
// statement of fn that writes index state before fn has entered the
// sequence: a statement that reaches no entry point, before any statement
// that calls an entry point and returns when the call was queued or failed.
func unorderedBuildSequenceWrites(funcs map[string]*buildSequenceFunc, fn *buildSequenceFunc) []string {
	for i, stmt := range fn.body.List {
		if entryGuard(fn.body.List, i, fn.recv) {
			return nil
		}
		summary := summarizeBuildSequence(stmt, fn.recv, fn.rsVars)
		writes, entered := summary.writes, false
		writers := []string{}
		if summary.writes {
			writers = append(writers, "the method's own statement")
		}
		seen := map[string]bool{}
		var visit func(string)
		visit = func(n string) {
			if seen[n] {
				return
			}
			seen[n] = true
			if buildSequenceEntries[n] {
				entered = true
				return // what the sequence installs is ordered by it
			}
			callee := funcs[n]
			if callee == nil {
				return
			}
			if callee.writes {
				writes = true
				writers = append(writers, n)
			}
			for c := range callee.calls {
				visit(c)
			}
		}
		for c := range summary.calls {
			visit(c)
		}
		if writes && !entered {
			sort.Strings(writers)
			return writers
		}
	}
	return nil
}

// entryGuard reports whether stmts[i] calls an entry point directly and the
// method returns when the call queued the operation or failed: the call in
// an if statement's init whose body returns, or an assignment of the call's
// result followed by an if statement whose body returns.
func entryGuard(stmts []ast.Stmt, i int, recv string) bool {
	callsEntry := func(n ast.Node) bool {
		calls := false
		ast.Inspect(n, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv && buildSequenceEntries[sel.Sel.Name] {
						calls = true
					}
				}
			}
			return true
		})
		return calls
	}
	returns := func(stmt ast.Stmt) bool {
		ifStmt, ok := stmt.(*ast.IfStmt)
		if !ok {
			return false
		}
		for _, s := range ifStmt.Body.List {
			if _, ok := s.(*ast.ReturnStmt); ok {
				return true
			}
		}
		return false
	}
	switch stmt := stmts[i].(type) {
	case *ast.IfStmt:
		return stmt.Init != nil && callsEntry(stmt.Init) && returns(stmt)
	case *ast.AssignStmt:
		return callsEntry(stmt) && i+1 < len(stmts) && returns(stmts[i+1])
	}
	return false
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
