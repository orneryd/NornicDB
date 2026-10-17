package search

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
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

// goBuiltins are the predeclared functions and types a call can name.
var goBuiltins = map[string]bool{
	"append": true, "cap": true, "clear": true, "close": true, "complex": true, "copy": true, "delete": true,
	"imag": true, "len": true, "make": true, "max": true, "min": true, "new": true, "panic": true,
	"print": true, "println": true, "real": true, "recover": true,
	"any": true, "bool": true, "byte": true, "comparable": true, "complex64": true, "complex128": true,
	"error": true, "float32": true, "float64": true, "int": true, "int8": true, "int16": true, "int32": true,
	"int64": true, "rune": true, "string": true, "uint": true, "uint8": true, "uint16": true, "uint32": true,
	"uint64": true, "uintptr": true,
}

// indexMuMethod classifies fun as X.indexMu.Lock/RLock ("lock") or
// X.indexMu.Unlock/RUnlock ("unlock"). recv restricts X to that identifier;
// "" accepts any X.
func indexMuMethod(fun ast.Expr, recv string) string {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	mu, ok := sel.X.(*ast.SelectorExpr)
	if !ok || mu.Sel.Name != "indexMu" {
		return ""
	}
	if recv != "" {
		if id, ok := mu.X.(*ast.Ident); !ok || id.Name != recv {
			return ""
		}
	}
	switch sel.Sel.Name {
	case "Lock", "RLock":
		return "lock"
	case "Unlock", "RUnlock":
		return "unlock"
	}
	return ""
}

// indexMuStmt classifies stmt as a call statement recv.indexMu.Lock/RLock
// ("lock"), Unlock/RUnlock ("unlock") or a deferred unlock ("defer").
func indexMuStmt(stmt ast.Stmt, recv string) string {
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
	switch kind := indexMuMethod(call.Fun, recv); {
	case kind == "lock" && !deferred:
		return "lock"
	case kind == "unlock" && deferred:
		return "defer"
	case kind == "unlock":
		return "unlock"
	}
	return ""
}

type posSpan struct{ from, to token.Pos }

func (p posSpan) contains(pos token.Pos) bool { return pos > p.from && pos < p.to }

// lockFacts are what the guard knows, package-wide, about the calls that can
// release indexMu.
type lockFacts struct {
	// releasers are the functions and methods, by name, that release indexMu
	// more often than they acquire it: a call releases the caller's lock.
	releasers map[string]bool
	// declared are the package's function, method and type names; a call
	// naming one resolves statically.
	declared map[string]bool
	// funcFields are struct fields of function type; a call through one is
	// dynamic.
	funcFields map[string]bool
	// dynamicRelease reports a release reachable through a function value: a
	// closure that releases indexMu and is not invoked, deferred or started
	// where it is written, or a method value of indexMu's Unlock/RUnlock.
	dynamicRelease bool
	// inertSources are the functions and methods, by name, whose every return
	// gives an inert closure literal (inertClosure): a function value they
	// return cannot release indexMu.
	inertSources map[string]bool
}

// inertClosure reports whether lit cannot release indexMu: it neither calls
// Unlock/RUnlock nor makes a call that may release (callMayRelease, with no
// local variables resolved).
func inertClosure(lit *ast.FuncLit, facts lockFacts, imports map[string]bool) bool {
	inert := true
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if indexMuMethod(call.Fun, "") == "unlock" || callMayRelease(call.Fun, facts, imports, nil) {
				inert = false
			}
		}
		if id, ok := n.(*ast.Ident); ok && facts.releasers[id.Name] {
			inert = false
		}
		return inert
	})
	return inert
}

// returnsInertClosures reports whether every return of fn's own body gives
// one inert closure literal.
func returnsInertClosures(fn *ast.FuncDecl, facts lockFacts, imports map[string]bool) bool {
	returns := 0
	inert := true
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			return false // Returns inside a closure are the closure's.
		case *ast.ReturnStmt:
			returns++
			lit, ok := (ast.Expr)(nil), false
			if len(x.Results) == 1 {
				lit, ok = x.Results[0], true
			}
			fl, isLit := lit.(*ast.FuncLit)
			if !ok || !isLit || !inertClosure(fl, facts, imports) {
				inert = false
			}
		}
		return inert
	})
	return inert && returns > 0
}

// inertLocals are fn's local function variables that cannot release indexMu:
// every assignment to one, anywhere in fn, is an inert closure literal or a
// call to an inert source, and it is never a parameter, a multi-value or range
// target, or address-taken. Names are matched without scopes, so a shadowing
// declaration counts as another assignment that must qualify too.
func inertLocals(fn *ast.FuncDecl, facts lockFacts, imports map[string]bool) map[string]bool {
	unsafe := map[string]bool{}
	sources := map[string][]ast.Expr{}
	params := func(ft *ast.FuncType) {
		for _, list := range []*ast.FieldList{ft.Params, ft.Results} {
			if list == nil {
				continue
			}
			for _, field := range list.List {
				for _, name := range field.Names {
					unsafe[name.Name] = true
				}
			}
		}
	}
	params(fn.Type)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			params(x.Type)
		case *ast.AssignStmt:
			for i, lhs := range x.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok {
					continue
				}
				if len(x.Rhs) != len(x.Lhs) {
					unsafe[id.Name] = true
					continue
				}
				sources[id.Name] = append(sources[id.Name], x.Rhs[i])
			}
		case *ast.ValueSpec:
			for i, id := range x.Names {
				if len(x.Values) != len(x.Names) {
					unsafe[id.Name] = len(x.Values) != 0 || unsafe[id.Name]
					continue
				}
				sources[id.Name] = append(sources[id.Name], x.Values[i])
			}
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{x.Key, x.Value} {
				if id, ok := e.(*ast.Ident); ok {
					unsafe[id.Name] = true
				}
			}
		case *ast.UnaryExpr:
			if id, ok := x.X.(*ast.Ident); ok && x.Op == token.AND {
				unsafe[id.Name] = true
			}
		}
		return true
	})
	locals := map[string]bool{}
	for name, exprs := range sources {
		if unsafe[name] {
			continue
		}
		ok := true
		for _, e := range exprs {
			switch v := e.(type) {
			case *ast.FuncLit:
				ok = ok && inertClosure(v, facts, imports)
			case *ast.CallExpr:
				switch f := v.Fun.(type) {
				case *ast.Ident:
					ok = ok && facts.inertSources[f.Name]
				case *ast.SelectorExpr:
					ok = ok && facts.inertSources[f.Sel.Name]
				default:
					ok = false
				}
			default:
				ok = false
			}
		}
		if ok {
			locals[name] = true
		}
	}
	return locals
}

// indexMuBalance counts the indexMu acquisitions and releases n contains.
func indexMuBalance(n ast.Node) (acquire, release int) {
	ast.Inspect(n, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			switch indexMuMethod(call.Fun, "") {
			case "lock":
				acquire++
			case "unlock":
				release++
			}
		}
		return true
	})
	return acquire, release
}

func newLockFacts(files []*ast.File) lockFacts {
	facts := lockFacts{releasers: map[string]bool{}, declared: map[string]bool{}, funcFields: map[string]bool{}, inertSources: map[string]bool{}}
	for _, file := range files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				facts.declared[d.Name.Name] = true
				if d.Body != nil {
					if acquire, release := indexMuBalance(d.Body); release > acquire {
						facts.releasers[d.Name.Name] = true
					}
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					facts.declared[ts.Name.Name] = true
					if st, ok := ts.Type.(*ast.StructType); ok {
						for _, field := range st.Fields.List {
							if _, ok := field.Type.(*ast.FuncType); ok {
								for _, name := range field.Names {
									facts.funcFields[name.Name] = true
								}
							}
						}
					}
				}
			}
		}
		// A closure that releases indexMu, anywhere but the call, defer or go
		// statement that runs it in place, can release it through any function
		// value; so can a method value of Unlock/RUnlock.
		inPlace := map[*ast.FuncLit]bool{}
		calledMu := map[*ast.SelectorExpr]bool{}
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if lit, ok := call.Fun.(*ast.FuncLit); ok {
					inPlace[lit] = true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					calledMu[sel] = true
				}
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				if acquire, release := indexMuBalance(x.Body); release > acquire && !inPlace[x] {
					facts.dynamicRelease = true
				}
			case *ast.SelectorExpr:
				if indexMuMethod(x, "") == "unlock" && !calledMu[x] {
					facts.dynamicRelease = true
				}
			}
			return true
		})
	}
	// Inert sources need the rest of the facts. Every declaration with the
	// name must qualify, since a call is matched by name only.
	candidate := map[string]bool{}
	for _, file := range files {
		imports := importNames(file)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			inert := returnsInertClosures(fn, facts, imports)
			if prev, seen := candidate[fn.Name.Name]; seen {
				inert = inert && prev
			}
			candidate[fn.Name.Name] = inert
		}
	}
	for name, inert := range candidate {
		if inert {
			facts.inertSources[name] = true
		}
	}
	return facts
}

// importNames are the names file's imports are referred to by.
func importNames(file *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range file.Imports {
		if imp.Name != nil {
			names[imp.Name.Name] = true
			continue
		}
		p, _ := strconv.Unquote(imp.Path.Value)
		base := path.Base(p)
		if len(base) > 1 && base[0] == 'v' && strings.Trim(base[1:], "0123456789") == "" {
			base = path.Base(path.Dir(p)) // A major-version suffix: the package is named by the element before it.
		}
		names[base] = true
		names[strings.ReplaceAll(base, "-", "_")] = true
	}
	return names
}

// metadataReadersWithoutIndexMu lists the Service methods in file with an
// indexMu metadata access the method doesn't provably hold indexMu for,
// unless the Locked suffix or the documented list says the caller holds it.
//
// The rule is deliberately conservative: an access is protected only by a
// recv.indexMu.Lock/RLock call statement earlier in a statement list of the
// same function (method or closure) that contains it, and only when nothing
// from that lock statement through the whole statement holding the access can
// release indexMu or bypass the lock. Any of these voids the protection:
//   - an Unlock/RUnlock of any X.indexMu, wherever it sits in that region, so
//     loops, branches, select and switch arms, invoked closures and goroutines
//     are all covered; the one exception is a release in a block that leaves
//     the function straight after it (only plain statements, then return or
//     panic), which cannot reach an access outside that block;
//   - a deferred release that runs before the access (one inside a closure);
//   - a label (a goto can enter without the lock);
//   - a call that can release indexMu: a package function or method that
//     releases more than it acquires, a reference to one, or, when the
//     package releases indexMu through function values, any call that does
//     not resolve statically. A local function variable resolves statically
//     only when every assignment to it is an inert closure literal or a call
//     to a function whose every return is one (inertLocals).
//
// A method that refers to recv.indexMu in any other way (a method value, an
// alias, a pointer) claims no protection at all. A closure is its own function:
// the enclosing method's lock never protects it, and its lock never protects
// the method. Code the rule cannot prove is flagged and restructured, never
// allow-listed.
func metadataReadersWithoutIndexMu(file *ast.File, name string, facts lockFacts) []string {
	imports := importNames(file)
	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		if id, ok := star.X.(*ast.Ident); !ok || id.Name != "Service" {
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

		locals := inertLocals(fn, facts, imports)

		// Unsupported uses of recv.indexMu void every claim in the method.
		direct := map[ast.Node]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if stmt, ok := n.(ast.Stmt); ok && indexMuStmt(stmt, recv) != "" {
				var call *ast.CallExpr
				switch st := stmt.(type) {
				case *ast.ExprStmt:
					call = st.X.(*ast.CallExpr)
				case *ast.DeferStmt:
					call = st.Call
				}
				direct[call.Fun.(*ast.SelectorExpr).X] = true
			}
			return true
		})
		supported := true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "indexMu" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv && !direct[sel] {
					supported = false
				}
			}
			return true
		})

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

		// The statement lists, with the lock statements in each.
		type stmtList struct {
			stmts []ast.Stmt
			owner posSpan
		}
		var lists []stmtList
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch b := n.(type) {
			case *ast.BlockStmt:
				lists = append(lists, stmtList{b.List, owner(b.Pos())})
			case *ast.CaseClause:
				lists = append(lists, stmtList{b.Body, owner(b.Pos())})
			case *ast.CommClause:
				lists = append(lists, stmtList{b.Body, owner(b.Pos())})
			}
			return true
		})

		// leavesFunction reports whether the release statement at index k of
		// list is followed only by plain statements and then return or panic,
		// so it cannot reach anything after list.
		leavesFunction := func(list []ast.Stmt, k int) bool {
			last := list[len(list)-1]
			switch st := last.(type) {
			case *ast.ReturnStmt:
			case *ast.ExprStmt:
				call, ok := st.X.(*ast.CallExpr)
				if id, isIdent := call.Fun.(*ast.Ident); !ok || !isIdent || id.Name != "panic" {
					return false
				}
			default:
				return false
			}
			plain := true
			for _, stmt := range list[k+1:] {
				ast.Inspect(stmt, func(n ast.Node) bool {
					switch n.(type) {
					case *ast.BranchStmt, *ast.LabeledStmt, *ast.ForStmt, *ast.RangeStmt,
						*ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt, *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt:
						plain = false
					case *ast.CallExpr:
						if indexMuMethod(n.(*ast.CallExpr).Fun, "") != "" {
							plain = false
						}
					}
					return plain
				})
			}
			return plain
		}

		// exempt are the release statements that leave the function straight
		// after them, with the span from each to the end of its block.
		exempt := map[*ast.CallExpr]posSpan{}
		for _, l := range lists {
			for k, stmt := range l.stmts {
				if indexMuStmt(stmt, "") == "unlock" && leavesFunction(l.stmts, k) {
					call := stmt.(*ast.ExprStmt).X.(*ast.CallExpr)
					exempt[call] = posSpan{stmt.Pos(), l.stmts[len(l.stmts)-1].End()}
				}
			}
		}

		// voids reports whether stmts, the region from a lock in a function
		// owned by in up to and including the statement holding the access at
		// pos, can release indexMu or bypass the lock before pos.
		voids := func(stmts []ast.Stmt, in posSpan, pos token.Pos) bool {
			void := false
			for _, stmt := range stmts {
				ast.Inspect(stmt, func(n ast.Node) bool {
					if void {
						return false
					}
					switch x := n.(type) {
					case *ast.LabeledStmt:
						void = true
					case *ast.DeferStmt:
						if indexMuMethod(x.Call.Fun, "") == "unlock" {
							// The caller's own deferred release runs at its exit;
							// a closure's runs before the closure returns.
							void = owner(x.Pos()) != in
							return false
						}
					case *ast.CallExpr:
						switch {
						case indexMuMethod(x.Fun, "") == "unlock":
							if span, ok := exempt[x]; ok && owner(x.Pos()) == in && !span.contains(pos) {
								return true
							}
							void = true
						case indexMuMethod(x.Fun, "") == "lock":
						default:
							void = callMayRelease(x.Fun, facts, imports, locals)
						}
					case *ast.Ident:
						if facts.releasers[x.Name] {
							void = true
						}
					}
					return !void
				})
				if void {
					return true
				}
			}
			return false
		}

		protected := func(pos token.Pos) bool {
			if !supported {
				return false
			}
			in := owner(pos)
			for _, l := range lists {
				if l.owner != in {
					continue
				}
				for i, stmt := range l.stmts {
					if indexMuStmt(stmt, recv) != "lock" {
						continue
					}
					for j := i + 1; j < len(l.stmts); j++ {
						if pos >= l.stmts[j].Pos() && pos < l.stmts[j].End() {
							if !voids(l.stmts[i+1:j+1], in, pos) {
								return true
							}
							break
						}
					}
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

// callMayRelease reports whether a call through fun can release indexMu: a
// releasing package function or method, or, when the package releases indexMu
// through function values, a call that does not resolve statically. locals
// are the calling function's inert local function variables (inertLocals).
// Names are matched without scopes: a local variable is assumed not to
// shadow a package function or type of the same name.
func callMayRelease(fun ast.Expr, facts lockFacts, imports, locals map[string]bool) bool {
	switch f := fun.(type) {
	case *ast.ParenExpr:
		return callMayRelease(f.X, facts, imports, locals)
	case *ast.FuncLit:
		return false // Its body is part of the scanned region.
	case *ast.Ident:
		if facts.releasers[f.Name] {
			return true
		}
		return facts.dynamicRelease && !facts.declared[f.Name] && !goBuiltins[f.Name] && !locals[f.Name]
	case *ast.SelectorExpr:
		if facts.releasers[f.Sel.Name] {
			return true
		}
		if id, ok := f.X.(*ast.Ident); ok && imports[id.Name] {
			return false // Another package can't reach indexMu.
		}
		return facts.dynamicRelease && facts.funcFields[f.Sel.Name]
	case *ast.IndexExpr:
		// An explicit instantiation of a declared generic function, or a call
		// through an element of a slice or map of functions.
		return facts.releasers[exprName(f.X)] || (facts.dynamicRelease && !isTypeOrFuncName(f.X, facts, imports))
	case *ast.IndexListExpr:
		return facts.releasers[exprName(f.X)] || (facts.dynamicRelease && !isTypeOrFuncName(f.X, facts, imports))
	case *ast.StarExpr:
		// A conversion to a pointer type, or a call through a pointer to a
		// function value.
		return facts.dynamicRelease && !isTypeOrFuncName(f.X, facts, imports)
	case *ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.FuncType, *ast.InterfaceType, *ast.StructType:
		return false // A conversion.
	}
	return facts.dynamicRelease
}

// exprName is the name an identifier or selector refers to, or "".
func exprName(x ast.Expr) string {
	switch v := x.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	}
	return ""
}

// isTypeOrFuncName reports whether x names a package-level function or type,
// a predeclared one, or another package's (a qualified identifier).
func isTypeOrFuncName(x ast.Expr, facts lockFacts, imports map[string]bool) bool {
	switch v := x.(type) {
	case *ast.Ident:
		return facts.declared[v.Name] || goBuiltins[v.Name]
	case *ast.SelectorExpr:
		id, ok := v.X.(*ast.Ident)
		return ok && imports[id.Name]
	}
	return false
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
		// sol r1 F1: releases carried through reachable successors.
		{"F1 nested release falls through to the read", `func (s *Service) f(ok, release bool) int { if ok { s.indexMu.RLock(); if release { s.indexMu.RUnlock() }; return len(s.nodeLabels) }; return 0 }`, true},
		{"F1 switch arm releases before the join", `func (s *Service) f(x int) int { s.indexMu.RLock(); switch x { case 1: s.indexMu.RUnlock() }; return len(s.nodeLabels) }`, true},
		{"F1 select arm releases before the join", `func (s *Service) f(c chan int) int { s.indexMu.RLock(); select { case <-c: s.indexMu.RUnlock(); default: }; return len(s.nodeLabels) }`, true},
		{"F1 next iteration after a conditional release", `func (s *Service) f(ok bool) { if ok { s.indexMu.RLock(); for i := 0; i < 2; i++ { _ = len(s.nodeLabels); if i == 0 { s.indexMu.RUnlock() } } } }`, true},
		{"F1 release then conditional continue", `func (s *Service) f(c bool) { s.indexMu.RLock(); for i := 0; i < 2; i++ { _ = len(s.nodeLabels); s.indexMu.RUnlock(); if c { continue }; s.indexMu.RLock() } }`, true},
		{"F1 loop releases and breaks to the read", `func (s *Service) f(x bool) int { s.indexMu.RLock(); for { if x { s.indexMu.RUnlock(); break } }; return len(s.nodeLabels) }`, true},
		{"loop held for every iteration", `func (s *Service) f(n int) (c int) { s.indexMu.RLock(); defer s.indexMu.RUnlock(); for i := 0; i < n; i++ { c += len(s.nodeLabels) }; return c }`, false},
		{"lock per iteration", `func (s *Service) f(n int) (c int) { for i := 0; i < n; i++ { s.indexMu.RLock(); c += len(s.nodeLabels); s.indexMu.RUnlock() }; return c }`, false},
		// sol r1 F2: a closure boundary doesn't erase an executed release.
		{"F2 invoked closure releases", `func (s *Service) f(ok bool) int { if ok { s.indexMu.RLock(); func() { s.indexMu.RUnlock() }(); return len(s.nodeLabels) }; return 0 }`, true},
		{"F2 closure-local deferred release", `func (s *Service) f() int { s.indexMu.RLock(); func() { defer s.indexMu.RUnlock() }(); return len(s.nodeLabels) }`, true},
		{"F2 goroutine releases before the read", `func (s *Service) f() int { s.indexMu.RLock(); done := make(chan struct{}); go func() { s.indexMu.RUnlock(); close(done) }(); <-done; return len(s.nodeLabels) }`, true},
		{"F2 release through a method value", `func (s *Service) f() int { unlock := s.indexMu.RUnlock; s.indexMu.RLock(); unlock(); return len(s.nodeLabels) }`, true},
		{"F2 releasing method", `func (s *Service) release() { s.indexMu.RUnlock() }
func (s *Service) f() int { s.indexMu.RLock(); s.release(); return len(s.nodeLabels) }`, true},
		{"F2 releaser passed as a value", `func (s *Service) release() { s.indexMu.RUnlock() }
func (s *Service) f(run func(func())) int { s.indexMu.RLock(); run(s.release); return len(s.nodeLabels) }`, true},
		{"F2 dynamic call in a package that releases through values", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f(cb func()) int { s.indexMu.RLock(); cb(); return len(s.nodeLabels) }`, true},
		{"F2 call through a slice of functions", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f(fs []func()) int { s.indexMu.RLock(); fs[0](); return len(s.nodeLabels) }`, true},
		{"inert local closure under the lock", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) admitterLocked(types []string) func(string) bool { return func(id string) bool { _, ok := s.nodeTypes[id]; return ok && len(types) > 0 } }
func (s *Service) f(types []string) (n int) { admit := func(string) bool { return true }; s.indexMu.RLock(); defer s.indexMu.RUnlock(); if len(types) > 0 { admit = s.admitterLocked(types) }; for id := range s.nodeLabels { if admit(id) { n++ } }; return n }`, false},
		{"local assigned a parameter", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f(cb func()) int { run := cb; s.indexMu.RLock(); run(); return len(s.nodeLabels) }`, true},
		{"local assigned a releasing source", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f() int { release := s.acquire(); s.indexMu.RLock(); release(); return len(s.nodeLabels) }`, true},
		{"local whose address is taken", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f(set func(*func())) int { run := func() {}; set(&run); s.indexMu.RLock(); run(); return len(s.nodeLabels) }`, true},
		{"call through a pointer to a function", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) f(p *func()) int { s.indexMu.RLock(); (*p)(); return len(s.nodeLabels) }`, true},
		{"static calls under the lock", `func (s *Service) acquire() func() { s.indexMu.Lock(); return func() { s.indexMu.Unlock() } }
func (s *Service) g() int { return 1 }
func (s *Service) f() int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); s.mu.RLock(); defer s.mu.RUnlock(); _ = s.g(); return len(s.nodeLabels) + int(len("x")) }`, false},
		// goto, select, defer and panic/recover.
		{"goto and label inside the locked region", `func (s *Service) f(ok bool) int { s.indexMu.RLock(); goto L; L: return len(s.nodeLabels) }`, true},
		{"select under the lock", `func (s *Service) f(c chan int) int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); select { case <-c: return len(s.nodeLabels); default: }; return 0 }`, false},
		{"deferred release in the method", `func (s *Service) f(ok bool) int { s.indexMu.RLock(); if ok { defer s.indexMu.RUnlock() }; return len(s.nodeLabels) }`, false},
		{"panic under the lock", `func (s *Service) f(bad bool) int { s.indexMu.RLock(); defer s.indexMu.RUnlock(); if bad { panic("x") }; return len(s.nodeLabels) }`, false},
		{"recovered panic after a release", `func (s *Service) f() int { s.indexMu.RLock(); func() { defer func() { _ = recover() }(); s.indexMu.RUnlock(); panic("x") }(); return len(s.nodeLabels) }`, true},
		{"early release that panics", `func (s *Service) f(n int) int { s.indexMu.RLock(); if n < 0 { s.indexMu.RUnlock(); panic("n") }; n = len(s.nodeLabels); s.indexMu.RUnlock(); return n }`, false},
		{"early release that loops before returning", `func (s *Service) f(n int) int { s.indexMu.RLock(); for { if n < 0 { s.indexMu.RUnlock(); for n < 0 { n++ }; return 0 }; break }; return len(s.nodeLabels) }`, true},
	}
	fset := token.NewFileSet()
	for _, probe := range probes {
		parsed, err := parser.ParseFile(fset, "probe.go", "package search\n"+probe.body+"\n", 0)
		require.NoError(t, err, probe.name)
		got := metadataReadersWithoutIndexMu(parsed, "probe.go", newLockFacts([]*ast.File{parsed}))
		require.Equal(t, probe.flagged, len(got) == 1, "probe %q: %v", probe.name, got)
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var files []*ast.File
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		files, names = append(files, file), append(names, name)
	}
	require.Greater(t, len(files), 10, "the guard must walk the package sources")
	facts := newLockFacts(files)
	var problems []string
	for i, file := range files {
		problems = append(problems, metadataReadersWithoutIndexMu(file, names[i], facts)...)
	}
	sort.Strings(problems)
	require.Empty(t, problems, "\n"+strings.Join(problems, "\n"))
}
