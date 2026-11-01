// Author of canonical source-entry correction: L4 lane (Codex), 1 Nov 2026.
// Census v11 independent cross-check: a syntax enumerator that does not use
// the SSA identity rules. For every Go file in the module (overlays included,
// no build-tag filter, internal/testfixture itself excluded) it collects, per
// top-level function, the names that are adopted (an argument of Own or
// OwnWithClose), bound to an owner result, built from an adopted name (a call
// that takes one), aliased, or returned by a same-package helper that returns
// one. Every X.Close( whose receiver path starts with such a name is a hit.
// Each hit must be a release the census saw, or a ledger entry.

package cleanupcensus_test

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

func ledgerPath(t *testing.T) string {
	return filepath.Join(root(t), "internal", "testfixture", "cleanupcensus", "release_ledger.tsv")
}

// plantedLedgerContent is the real ledger plus the matrix's planted entries.
func plantedLedgerContent(t *testing.T) []byte {
	data, _ := os.ReadFile(ledgerPath(t))
	text := strings.TrimRight(string(data), "\r\n")
	if text != "" {
		text += "\n"
	}
	return []byte(text + plantedLedger)
}

// exprPath renders a receiver as a dotted path of identifiers ("f.engine"), or "".
func exprPath(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		if p := exprPath(x.X); p != "" {
			return p + "." + x.Sel.Name
		}
	case *ast.ParenExpr:
		return exprPath(x.X)
	case *ast.StarExpr:
		return exprPath(x.X)
	case *ast.UnaryExpr:
		return exprPath(x.X)
	case *ast.IndexExpr:
		return exprPath(x.X)
	}
	return ""
}

// calleeIdent unwraps generics and a curried constructor invocation.
func calleeIdent(call *ast.CallExpr) string {
	var name func(ast.Expr) string
	name = func(expr ast.Expr) string {
		switch f := expr.(type) {
		case *ast.Ident:
			return f.Name
		case *ast.SelectorExpr:
			return f.Sel.Name
		case *ast.IndexExpr:
			return name(f.X)
		case *ast.IndexListExpr:
			return name(f.X)
		case *ast.CallExpr:
			return name(f.Fun)
		case *ast.ParenExpr:
			return name(f.X)
		}
		return ""
	}
	return name(call.Fun)
}

func isOwnerCall(call *ast.CallExpr, entries map[string]bool) bool {
	return entries[calleeIdent(call)]
}

// ownerEntryNames derives the entry population from the owner source graph.
// The independent AST walk shares the canonical root, not the SSA rules or a
// hand-maintained public API list. Calls inside returned closures count too.
func (a *analysis) ownerEntryNames() map[string]bool {
	entries := map[string]bool{"ownedClose": true}
	bodies := map[string][]*ast.FuncDecl{}
	paths, _ := filepath.Glob(filepath.Join(a.base, "internal", "testfixture", "*.go"))
	for path := range a.overlay {
		if filepath.Dir(path) == filepath.Join(a.base, "internal", "testfixture") {
			paths = append(paths, path)
		}
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		data, ok := a.overlay[path]
		if !ok {
			data, _ = os.ReadFile(path)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
		if err != nil {
			continue
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && fn.Recv == nil {
				bodies[fn.Name.Name] = append(bodies[fn.Name.Name], fn)
			}
		}
	}
	for {
		before := len(entries)
		for name, variants := range bodies {
			for _, fn := range variants {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok && isOwnerCall(call, entries) {
						entries[name] = true
					}
					return true
				})
			}
		}
		if len(entries) == before {
			break
		}
	}
	return entries
}

func knownPrefix(path string, known map[string]bool) bool {
	if path == "" {
		return false
	}
	for p := path; ; {
		if known[p] {
			return true
		}
		i := strings.LastIndexByte(p, '.')
		if i < 0 {
			return false
		}
		p = p[:i]
	}
}

func funcName(decl *ast.FuncDecl) string {
	if decl.Recv != nil && len(decl.Recv.List) > 0 {
		recv := decl.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		if index, ok := recv.(*ast.IndexExpr); ok {
			recv = index.X
		}
		if id, ok := recv.(*ast.Ident); ok {
			return "(*" + id.Name + ")." + decl.Name.Name
		}
	}
	return decl.Name.Name
}

// ledgerSourceFunction compares a full SSA function identity with its source
// function for the AST cross-check. Keep receiver AND closure identity.
// Equal statement text in sibling closures must match their own source entry;
// accepting an arbitrary entry with the same outer declaration leaves other
// live entries stale and can justify a different binding.
func ledgerSourceFunction(name string) string {
	var plain strings.Builder
	depth := 0
	for _, char := range name {
		if char == '[' {
			depth++
		} else if char == ']' {
			depth--
		} else if depth == 0 {
			plain.WriteRune(char)
		}
	}
	name = plain.String()
	if strings.HasPrefix(name, "(") {
		if end := strings.Index(name, ")."); end >= 0 {
			receiver := strings.TrimPrefix(name[1:end], "*")
			receiver = receiver[strings.LastIndexByte(receiver, '/')+1:]
			if dot := strings.IndexByte(receiver, '.'); dot >= 0 {
				receiver = receiver[dot+1:]
			}
			return "(*" + receiver + ")." + name[end+2:]
		}
	}
	name = name[strings.LastIndexByte(name, '/')+1:]
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	return name
}

// inspectSourceFunction gives each source closure its lexical parent and ordinal,
// matching SSA's nested anonymous-function identity without using SSA facts.
// An outer function never consumes the ordinals of a nested closure's children.
func inspectSourceFunction(body ast.Node, name string, visit func(ast.Node, string) bool) {
	children := 0
	ast.Inspect(body, func(node ast.Node) bool {
		if literal, ok := node.(*ast.FuncLit); ok {
			children++
			inspectSourceFunction(literal.Body, name+"$"+itoa(children), visit)
			return false
		}
		return visit(node, name)
	})
}

func knownExpression(e ast.Node, known, returning, ownerEntries map[string]bool) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if found {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok && (isOwnerCall(call, ownerEntries) || returning[calleeIdent(call)]) {
			found = true
			return false
		}
		if expr, ok := n.(ast.Expr); ok && knownPrefix(exprPath(expr), known) {
			found = true
			return false
		}
		return true
	})
	return found
}

// knownNames computes the adopted-derived names of one function body.
func knownNames(body ast.Node, returning map[string]bool, parameters map[string]bool, ownerEntries map[string]bool) map[string]bool {
	known := map[string]bool{}
	for name := range parameters {
		known[name] = true
	}

	for {
		before := len(known)
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if isOwnerCall(x, ownerEntries) {
					for _, arg := range x.Args[1:] {
						if p := exprPath(arg); p != "" {
							known[p] = true
						}
					}
				}
			case *ast.AssignStmt:
				for _, rhs := range x.Rhs {
					if knownExpression(rhs, known, returning, ownerEntries) {
						for _, lhs := range x.Lhs {
							if p := exprPath(lhs); p != "" && p != "_" {
								known[p] = true
							}
						}
					}
				}
			case *ast.ValueSpec:
				for _, rhs := range x.Values {
					if knownExpression(rhs, known, returning, ownerEntries) {
						for _, name := range x.Names {
							if name.Name != "_" {
								known[name.Name] = true
							}
						}
					}
				}
			case *ast.CompositeLit:
				// a struct literal holding a known name holds it
			}
			return true
		})
		if len(known) == before {
			break
		}
	}
	return known
}

// enumerate runs the syntax cross-check and reconciles it with the census.
func (a *analysis) enumerate(c *census) {
	ownerEntries := a.ownerEntryNames()
	fset := token.NewFileSet()
	type parsed struct {
		rel  string
		file *ast.File
		src  []string
	}
	byDir := map[string][]parsed{}
	visit := func(path string, data []byte) {
		rel := relative(a.base, path)
		if !strings.HasSuffix(rel, ".go") || strings.HasPrefix(rel, "internal/testfixture/") && !strings.Contains(strings.TrimPrefix(rel, "internal/testfixture/"), "/") {
			return
		}
		if strings.HasPrefix(rel, "internal/testfixture/cleanupcensus/") || strings.Contains(rel, "/vendor/") || strings.HasPrefix(rel, "vendor/") {
			return
		}
		f, err := parser.ParseFile(fset, path, data, 0)
		if err != nil {
			return
		}
		lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
		byDir[filepath.Dir(rel)] = append(byDir[filepath.Dir(rel)], parsed{rel, f, lines})
	}
	seenOverlay := map[string]bool{}
	for path, data := range a.overlay {
		if strings.HasSuffix(path, ".go") {
			seenOverlay[filepath.Clean(path)] = true
			visit(path, data)
		}
	}
	_ = filepath.WalkDir(a.base, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "node_modules" || name == "vendor" || (strings.HasPrefix(name, ".") && path != a.base) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || seenOverlay[filepath.Clean(path)] {
			return nil
		}
		data, err := os.ReadFile(path)
		if err == nil {
			visit(path, data)
		}
		return nil
	})
	matchLedger := func(file, top, text string) bool {
		for _, e := range c.ledger {
			if e.file != file || e.text != text {
				continue
			}
			if ledgerSourceFunction(e.function) == top {
				e.enumMatched = true
				return true
			}
		}
		return false
	}
	for _, files := range byDir {
		returning := map[string]bool{}
		// Author: L4 lane (Codex), 1 Nov 2026. Syntax-derived names also
		// flow into helper arguments. A release helper need not adopt the
		// resource itself; its consumer can already have adopted it.
		parameters := map[*ast.FuncDecl]map[string]bool{}
		declarations := map[string][]*ast.FuncDecl{}
		for _, p := range files {
			for _, decl := range p.file.Decls {
				if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
					declarations[fd.Name.Name] = append(declarations[fd.Name.Name], fd)
					parameters[fd] = map[string]bool{}
				}
			}
		}
		for {
			before := len(returning)
			for _, names := range parameters {
				before += len(names)
			}
			for _, p := range files {
				for _, decl := range p.file.Decls {
					fd, ok := decl.(*ast.FuncDecl)
					if !ok || fd.Body == nil {
						continue
					}
					known := knownNames(fd.Body, returning, parameters[fd], ownerEntries)
					ast.Inspect(fd.Body, func(n ast.Node) bool {
						if call, ok := n.(*ast.CallExpr); ok {
							for _, target := range declarations[calleeIdent(call)] {
								var names []string
								for _, field := range target.Type.Params.List {
									for _, name := range field.Names {
										names = append(names, name.Name)
									}
								}
								for i, arg := range call.Args {
									knownArgument := knownPrefix(exprPath(arg), known)
									if owner, ok := arg.(*ast.CallExpr); ok {
										knownArgument = knownArgument || isOwnerCall(owner, ownerEntries)
									}
									if i < len(names) && knownArgument {
										parameters[target][names[i]] = true
									}
								}
							}
						}
						if ret, ok := n.(*ast.ReturnStmt); ok {
							for _, r := range ret.Results {
								if knownExpression(r, known, returning, ownerEntries) {
									returning[fd.Name.Name] = true
								}
							}
						}
						return true
					})
				}
			}
			after := len(returning)
			for _, names := range parameters {
				after += len(names)
			}
			if before == after {
				break
			}
		}
		for _, p := range files {
			for _, decl := range p.file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				if fd.Name.Name == "Close" {
					continue // a wrapper's own Close is its implementation, not a test's release
				}
				known := knownNames(fd.Body, returning, parameters[fd], ownerEntries)
				inspectSourceFunction(fd.Body, funcName(fd), func(n ast.Node, top string) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Close" || !knownPrefix(exprPath(sel.X), known) {
						return true
					}
					pos := fset.Position(call.Lparen)
					key := p.rel + ":" + itoa(pos.Line) + ":" + itoa(pos.Column)
					text := ""
					if pos.Line-1 < len(p.src) {
						text = strings.TrimSpace(p.src[pos.Line-1])
					}
					hit := key + " in " + top + " | " + text
					c.enumHits = append(c.enumHits, hit)
					if !c.ownedPositions[key] && !matchLedger(p.rel, top, text) {
						c.enumUnexplained = append(c.enumUnexplained, hit)
					}
					return true
				})
			}
		}
	}
	sort.Strings(c.enumHits)
	sort.Strings(c.enumUnexplained)
	for _, e := range c.ledger {
		if !e.matched && !e.enumMatched {
			c.ledgerStale = append(c.ledgerStale, ledgerKey(e.file, e.function, e.text))
		}
	}
	sort.Strings(c.ledgerStale)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
