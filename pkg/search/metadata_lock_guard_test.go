package search

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

// indexMuMetadata are the node and relationship metadata maps index writes
// change under indexMu.
var indexMuMetadata = map[string]bool{
	"nodeLabels": true, "nodeTypes": true, "nodeNamedVector": true, "nodePropVector": true,
	"nodeChunkVectors": true, "nodeChunkSpans": true, "edgeTypes": true, "edgePropVector": true,
}

// metadataCallerHoldsIndexMu are methods without the Locked suffix whose
// callers hold indexMu, as each one documents.
var metadataCallerHoldsIndexMu = map[string]bool{
	// Builds a closure; every caller runs it under indexMu.
	"candidate_filter_metadata.go: indexedTypeAdmitter": true,
	// The source-selection owner; its callers hold indexMu.
	"search.go: admitVectorFileStore": true,
}

// indexMuCall classifies a statement as recv.indexMu.Lock/RLock ("lock"),
// recv.indexMu.Unlock/RUnlock ("unlock") or a deferred unlock ("defer").
// Only an actual call on the method's own receiver counts.
func indexMuCall(stmt ast.Stmt, recv string) string {
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
	case recv + ".indexMu.Lock", recv + ".indexMu.RLock":
		if !deferred {
			return "lock"
		}
	case recv + ".indexMu.Unlock", recv + ".indexMu.RUnlock":
		if deferred {
			return "defer"
		}
		return "unlock"
	}
	return ""
}

type posSpan struct{ from, to token.Pos }

func (p posSpan) contains(pos token.Pos) bool { return pos > p.from && pos < p.to }

// metadataReadersWithoutIndexMu lists the Service methods in file with an
// indexMu metadata access outside a span where the method holds indexMu,
// unless the Locked suffix or the documented list says the caller holds it.
//
// A span opens at a recv.indexMu.Lock/RLock call statement of any statement
// list (the body, a nested block, a case or a closure body) and covers the
// statements after it in that list, which the lock dominates. It closes at
// the next Unlock/RUnlock call of the same list, at a label (a goto can enter
// there without the lock), or at the end of the list once the unlock is
// deferred. An unlock inside a nested list ends the protection of every span
// opened before it for the rest of that list. A span protects only accesses
// of its own function: an access inside a closure is never protected by the
// enclosing method's lock, since the closure can run after the release, and
// a lock inside a closure never protects the enclosing method.
func metadataReadersWithoutIndexMu(file *ast.File, name string) []string {
	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) != 1 || types.ExprString(fn.Recv.List[0].Type) != "*Service" {
			continue
		}
		if len(fn.Recv.List[0].Names) != 1 {
			continue // An unnamed receiver cannot reach the metadata.
		}
		recv := fn.Recv.List[0].Names[0].Name
		key := name + ": " + fn.Name.Name
		if strings.HasSuffix(fn.Name.Name, "Locked") || metadataCallerHoldsIndexMu[key] {
			continue
		}

		// owner is the innermost closure holding pos; the zero span is the
		// method itself.
		var closures []posSpan
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if lit, ok := n.(*ast.FuncLit); ok {
				closures = append(closures, posSpan{lit.Pos(), lit.End()})
			}
			return true
		})
		owner := func(pos token.Pos) posSpan {
			var in posSpan
			for _, c := range closures {
				if c.contains(pos) && c.from > in.from {
					in = c
				}
			}
			return in
		}

		type lockSpan struct{ span, owner posSpan }
		var held, released []lockSpan
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
			default:
				return true
			}
			in := owner(n.Pos())
			open, deferred := token.NoPos, false
			closeAt := func(at token.Pos) {
				if open != token.NoPos {
					held = append(held, lockSpan{posSpan{open, at}, in})
					open = token.NoPos
				}
			}
			for _, stmt := range list {
				if _, labeled := stmt.(*ast.LabeledStmt); labeled {
					closeAt(stmt.Pos())
				}
				switch indexMuCall(stmt, recv) {
				case "lock":
					if open == token.NoPos {
						open, deferred = stmt.End(), false
					}
				case "defer":
					if open != token.NoPos {
						deferred = true
					}
				case "unlock":
					released = append(released, lockSpan{posSpan{stmt.Pos(), end}, in})
					if !deferred {
						closeAt(stmt.Pos())
					}
				}
			}
			closeAt(end)
			return true
		})

		protected := func(pos token.Pos) bool {
			in := owner(pos)
			for _, h := range held {
				if h.owner != in || !h.span.contains(pos) {
					continue
				}
				cancelled := false
				for _, r := range released {
					if r.owner == in && r.span.contains(pos) && r.span.from > h.span.from {
						cancelled = true
						break
					}
				}
				if !cancelled {
					return true
				}
			}
			return false
		}
		bad := false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !indexMuMetadata[sel.Sel.Name] {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv && !protected(sel.Pos()) {
				bad = true
			}
			return true
		})
		if bad {
			out = append(out, key)
		}
	}
	return out
}

// Every reader of the metadata index writes change under indexMu holds
// indexMu. CountPropertyVectorEntries read nodePropVector under mu alone, so
// a Cypher vector query racing a live write crashed with a concurrent map
// read and write.
func TestMetadataReadersHoldIndexMu(t *testing.T) {
	probes := []struct {
		name, body string
		flagged    bool
	}{
		{"mu alone", `func (s *Service) f() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.nodePropVector) }`, true},
		{"released before the read", `func (s *Service) f() int { s.indexMu.RLock(); s.indexMu.RUnlock(); return len(s.nodePropVector) }`, true},
		{"read before the lock", `func (s *Service) f() int { n := len(s.nodePropVector); s.indexMu.RLock(); defer s.indexMu.RUnlock(); return n }`, true},
		{"lock mentioned, not called", `func (s *Service) f() int { _ = s.indexMu.RLock; return len(s.nodePropVector) }`, true},
		{"other receiver name", `func (svc *Service) f() int { svc.mu.RLock(); defer svc.mu.RUnlock(); return len(svc.nodePropVector) }`, true},
		{"another service's lock", `func (svc *Service) f(s *Service) int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); return len(svc.nodePropVector) }`, true},
		{"lock inside a conditional", `func (s *Service) f(ok bool) int { if ok { s.indexMu.RLock(); defer s.indexMu.RUnlock() }; return len(s.nodePropVector) }`, true},
		{"lock inside a closure", `func (s *Service) f() int { func() { s.indexMu.RLock() }(); return len(s.nodePropVector) }`, true},
		{"read in an escaping closure", `func (s *Service) f() func() int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); return func() int { return len(s.nodePropVector) } }`, true},
		{"read after an early release", `func (s *Service) f(n int) int { s.indexMu.RLock(); if n > 0 { s.indexMu.RUnlock(); return len(s.nodePropVector) }; n = len(s.nodeLabels); s.indexMu.RUnlock(); return n }`, true},
		{"held with a deferred unlock", `func (s *Service) f() int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); return len(s.nodePropVector) }`, false},
		{"held span", `func (svc *Service) f() int { svc.indexMu.Lock(); n := len(svc.edgeTypes); svc.indexMu.Unlock(); return n }`, false},
		{"read in a conditional while held", `func (s *Service) f(ok bool) int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); if ok { return len(s.nodeLabels) }; return 0 }`, false},
		{"early release that returns", `func (s *Service) f(n int) int { s.indexMu.RLock(); if n < 0 { s.indexMu.RUnlock(); return 0 }; n = len(s.nodeLabels); s.indexMu.RUnlock(); return n }`, false},
		{"Locked suffix", `func (s *Service) fLocked() int { return len(s.nodePropVector) }`, false},
		{"nested lock dominating the write", `func (s *Service) f(ok bool) { if ok { s.indexMu.Lock(); s.mu.Lock(); s.nodePropVector = nil; s.mu.Unlock(); s.indexMu.Unlock() } }`, false},
		{"nested lock with a deferred unlock", `func (s *Service) f(ok bool) int { if ok { s.indexMu.RLock(); defer s.indexMu.RUnlock(); return len(s.nodeLabels) }; return 0 }`, false},
		{"nested lock released before the write", `func (s *Service) f(ok bool) { if ok { s.indexMu.Lock(); s.indexMu.Unlock(); s.nodePropVector = nil } }`, true},
		{"nested lock in a sibling branch", `func (s *Service) f(ok bool) int { if ok { s.indexMu.RLock(); defer s.indexMu.RUnlock() } else { return len(s.nodeLabels) }; return 0 }`, true},
		{"relocked after a release", `func (s *Service) f() int { s.indexMu.RLock(); s.indexMu.RUnlock(); { s.indexMu.RLock(); n := len(s.nodeLabels); s.indexMu.RUnlock(); return n } }`, false},
		{"label after the lock", `func (s *Service) f(ok bool) int { if ok { goto L }; s.indexMu.RLock(); L: n := len(s.nodeLabels); s.indexMu.RUnlock(); return n }`, true},
		{"lock inside the closure that reads", `func (s *Service) f() func() int { return func() int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); return len(s.nodePropVector) } }`, false},
		{"closure lock does not cover the method", `func (s *Service) f() int { func() { s.indexMu.RLock(); defer s.indexMu.RUnlock() }(); return len(s.nodePropVector) }`, true},
	}
	fset := token.NewFileSet()
	for _, probe := range probes {
		parsed, err := parser.ParseFile(fset, "probe.go", "package search\n"+probe.body+"\n", 0)
		require.NoError(t, err, probe.name)
		got := metadataReadersWithoutIndexMu(parsed, "probe.go")
		require.Equal(t, probe.flagged, len(got) == 1, "probe %q: %v", probe.name, got)
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var problems []string
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		problems = append(problems, metadataReadersWithoutIndexMu(file, name)...)
		checked++
	}
	require.Greater(t, checked, 10, "the guard must walk the package sources")
	sort.Strings(problems)
	require.Empty(t, problems, "\n"+strings.Join(problems, "\n"))
}
