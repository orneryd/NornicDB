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
// A span opens at a top-level recv.indexMu.Lock/RLock call statement of the
// method body and closes at the next top-level Unlock/RUnlock call, or at the
// end of the body once the unlock is deferred. A lock taken inside a nested
// block or closure opens no span; an unlock inside a nested block ends the
// protection for the rest of that block; an access inside a closure is never
// protected, since the closure can run after the release.
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

		var held []posSpan
		open, deferred := token.NoPos, false
		for _, stmt := range fn.Body.List {
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
				if open != token.NoPos && !deferred {
					held = append(held, posSpan{open, stmt.Pos()})
					open = token.NoPos
				}
			}
		}
		if open != token.NoPos {
			held = append(held, posSpan{open, fn.Body.End()})
		}

		var released, closures []posSpan
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			var list []ast.Stmt
			var end token.Pos
			switch b := n.(type) {
			case *ast.FuncLit:
				closures = append(closures, posSpan{b.Pos(), b.End()})
			case *ast.BlockStmt:
				if b != fn.Body {
					list, end = b.List, b.End()
				}
			case *ast.CaseClause:
				list, end = b.Body, b.End()
			case *ast.CommClause:
				list, end = b.Body, b.End()
			}
			for _, stmt := range list {
				if indexMuCall(stmt, recv) == "unlock" {
					released = append(released, posSpan{stmt.Pos(), end})
				}
			}
			return true
		})

		protected := func(pos token.Pos) bool {
			for _, c := range closures {
				if c.contains(pos) {
					return false
				}
			}
			for _, r := range released {
				if r.contains(pos) {
					return false
				}
			}
			for _, h := range held {
				if h.contains(pos) {
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
