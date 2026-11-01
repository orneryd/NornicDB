// Author: #1636 lane (Claude), 30 Oct. Evidence tooling, never product code.
// Compiled census of lifetime-contract copies outside the single owner
// (pkg/lifecycle). It loads every package of the module with full type
// information and reports one line per member, by class:
//
//	K1 accumulator   struct field assigned f(field, ...), f of errors.Join's type
//	K2 admission     struct with a WaitGroup-shaped field (Add/Done/Wait method
//	                 set, concrete or interface) and a bool or atomic.Bool flag
//	K3 completion    struct with a Do(func())-shaped field, chan struct{} and error
//	K4 owned timer   call of any value of time.AfterFunc's type whose callback
//	                 defers a release
//
//	K7 inventory     direct Err()/Done() on a context.Context value, or
//	                 context.Cause, outside pkg/cypher/bulk (not a gate)
//	B  inventory     calls into pkg/cypher/bulk, the statement observation owner
//
// Membership is by type and value, never by name: a primitive reached through
// an interface or a function value is a member.
//
//	K5 no-op accept  return of an empty func literal together with true
//	K6 absent owner  `if x == nil` on a context/lifetime value that returns true
//
// CENSUS_PLANT=1 also loads ./internal/zzlifetimeplant, which holds one
// planted member per class; the test fails unless every planted class is found.
package zzlifetimecensus

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

type member struct{ class, pos, what string }

func moduleRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestLifetimeCensus(t *testing.T) {
	root := moduleRoot(t)
	cfg := &packages.Config{
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:   root,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatal(err)
	}
	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, e.Error())
		}
	})
	if len(loadErrs) > 0 {
		t.Fatalf("census load errors (%d): %s", len(loadErrs), strings.Join(loadErrs[:min(5, len(loadErrs))], "; "))
	}
	var found []member
	for _, p := range pkgs {
		if strings.Contains(p.PkgPath, "zzlifetimecensus") || strings.HasSuffix(p.PkgPath, "/pkg/cypher/bulk") {
			continue
		}
		if strings.HasSuffix(p.PkgPath, "/pkg/lifecycle") {
			for _, f := range p.Syntax {
				if !strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
					found = append(found, scanObservation(p, f)...)
				}
			}
			continue
		}
		for _, f := range p.Syntax {
			name := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			found = append(found, scanFile(p, f)...)
			found = append(found, scanObservation(p, f)...)
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].class != found[j].class {
			return found[i].class < found[j].class
		}
		return found[i].pos < found[j].pos
	})
	counts := map[string]int{}
	plantedCount := map[string]int{}
	for _, m := range found {
		rel := m.pos
		if r, err := filepath.Rel(root, m.pos); err == nil {
			rel = filepath.ToSlash(r)
		}
		fmt.Printf("CENSUS %s %s %s\n", m.class, rel, m.what)
		counts[m.class]++
		if strings.Contains(rel, "zzlifetimeplant") {
			plantedCount[m.class]++
		}
	}
	for _, c := range []string{"K1", "K2", "K3", "K4", "K5", "K6", "K7", "B"} {
		fmt.Printf("CENSUS-COUNT %s %d\n", c, counts[c])
	}
	if os.Getenv("CENSUS_PLANT") != "" {
		// Direct and value/interface shapes: two plants for K1, K2 and K4.
		want := map[string]int{"K1": 2, "K2": 2, "K3": 1, "K4": 2, "K5": 1, "K6": 1}
		for _, c := range []string{"K1", "K2", "K3", "K4", "K5", "K6"} {
			if plantedCount[c] < want[c] {
				t.Errorf("planted control %s: detected %d of %d", c, plantedCount[c], want[c])
			}
		}
	}
}

func scanFile(p *packages.Package, f *ast.File) []member {
	var out []member
	info := p.TypesInfo
	pos := func(n ast.Node) string {
		ps := p.Fset.Position(n.Pos())
		return fmt.Sprintf("%s:%d", ps.Filename, ps.Line)
	}
	isNamed := func(t types.Type, pkg, name string) bool {
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		n, ok := t.(*types.Named)
		return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
	}
	isErrorType := func(t types.Type) bool { return types.Identical(t, types.Universe.Lookup("error").Type()) }
	hasMethods := func(t types.Type, names ...string) bool {
		for _, candidate := range []types.Type{t, types.NewPointer(t)} {
			ms := types.NewMethodSet(candidate)
			all := true
			for _, n := range names {
				if ms.Lookup(nil, n) == nil {
					found := false
					for i := 0; i < ms.Len(); i++ {
						if ms.At(i).Obj().Name() == n {
							found = true
							break
						}
					}
					if !found {
						all = false
						break
					}
				}
			}
			if all {
				return true
			}
		}
		return false
	}
	joinType := types.Universe.Lookup("error").Type()
	isJoinSignature := func(t types.Type) bool {
		sig, ok := t.Underlying().(*types.Signature)
		if !ok || !sig.Variadic() || sig.Params().Len() != 1 || sig.Results().Len() != 1 {
			return false
		}
		slice, ok := sig.Params().At(0).Type().(*types.Slice)
		return ok && types.Identical(slice.Elem(), joinType) && types.Identical(sig.Results().At(0).Type(), joinType)
	}
	isAfterFuncSignature := func(t types.Type) bool {
		sig, ok := t.Underlying().(*types.Signature)
		if !ok || sig.Params().Len() != 2 || sig.Results().Len() != 1 {
			return false
		}
		return isNamed(sig.Params().At(0).Type(), "time", "Duration") && isNamed(sig.Results().At(0).Type(), "time", "Timer")
	}
	isContextOrLifetime := func(t types.Type) bool {
		return isNamed(t, "context", "Context") || isNamed(t, "github.com/orneryd/nornicdb/pkg/lifecycle", "WorkLifetime")
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.TypeSpec:
			st, ok := n.Type.(*ast.StructType)
			if !ok {
				return true
			}
			var wg, flag, once, doneChan, errField bool
			for _, fld := range st.Fields.List {
				t := info.TypeOf(fld.Type)
				if t == nil {
					continue
				}
				if hasMethods(t, "Add", "Done", "Wait") {
					wg = true
				}
				if hasMethods(t, "Do") {
					once = true
				}
				if isErrorType(t) {
					errField = true
				}
				if ch, ok := t.(*types.Chan); ok {
					if s, ok := ch.Elem().(*types.Struct); ok && s.NumFields() == 0 {
						doneChan = true
					}
				}
				if b, ok := t.(*types.Basic); ok && b.Kind() == types.Bool {
					flag = true
				}
				if isNamed(t, "sync/atomic", "Bool") {
					flag = true
				}
			}
			if wg && flag {
				out = append(out, member{"K2", pos(n), n.Name.Name})
			}
			if once && doneChan && errField {
				out = append(out, member{"K3", pos(n), n.Name.Name})
			}
		case *ast.AssignStmt:
			if len(n.Lhs) != 1 || len(n.Rhs) != 1 {
				return true
			}
			sel, ok := n.Lhs[0].(*ast.SelectorExpr)
			if !ok {
				return true
			}
			call, ok := n.Rhs[0].(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			if ft := info.TypeOf(call.Fun); ft == nil || !isJoinSignature(ft) {
				return true
			}
			if first, ok := call.Args[0].(*ast.SelectorExpr); ok && first.Sel.Name == sel.Sel.Name {
				if _, isVar := info.Uses[sel.Sel].(*types.Var); isVar {
					out = append(out, member{"K1", pos(n), sel.Sel.Name})
				}
			}
		case *ast.CallExpr:
			if len(n.Args) != 2 {
				return true
			}
			if ft := info.TypeOf(n.Fun); ft == nil || !isAfterFuncSignature(ft) {
				return true
			}
			if lit, ok := n.Args[1].(*ast.FuncLit); ok {
				for _, st := range lit.Body.List {
					if _, ok := st.(*ast.DeferStmt); ok {
						out = append(out, member{"K4", pos(n), "AfterFunc-typed callback defers a release"})
						break
					}
				}
			}
		case *ast.ReturnStmt:
			var emptyFunc, trueLit bool
			for _, r := range n.Results {
				if lit, ok := r.(*ast.FuncLit); ok && len(lit.Body.List) == 0 && lit.Type.Params.NumFields() == 0 {
					emptyFunc = true
				}
				if id, ok := r.(*ast.Ident); ok && id.Name == "true" {
					trueLit = true
				}
			}
			if emptyFunc && trueLit {
				out = append(out, member{"K5", pos(n), "accepted no-op release"})
			}
		case *ast.IfStmt:
			be, ok := n.Cond.(*ast.BinaryExpr)
			if !ok || be.Op != token.EQL {
				return true
			}
			if id, ok := be.Y.(*ast.Ident); !ok || id.Name != "nil" {
				return true
			}
			if t := info.TypeOf(be.X); t == nil || !isContextOrLifetime(t) {
				return true
			}
			for _, st := range n.Body.List {
				if ret, ok := st.(*ast.ReturnStmt); ok {
					for _, r := range ret.Results {
						if id, ok := r.(*ast.Ident); ok && id.Name == "true" {
							out = append(out, member{"K6", pos(n), "absent owner accepted"})
						}
					}
				}
			}
		}
		return true
	})
	return out
}

// scanObservation inventories how statement and lifetime cancellation is
// observed: directly on a context.Context value (K7), or through bulk (B).
func scanObservation(p *packages.Package, f *ast.File) []member {
	var out []member
	info := p.TypesInfo
	pos := func(n ast.Node) string {
		ps := p.Fset.Position(n.Pos())
		return fmt.Sprintf("%s:%d", ps.Filename, ps.Line)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if fn, ok := info.Uses[sel.Sel].(*types.Func); ok && fn.Pkg() != nil {
			switch {
			case strings.HasSuffix(fn.Pkg().Path(), "/pkg/cypher/bulk"):
				out = append(out, member{"B", pos(call), "bulk." + fn.Name()})
				return true
			case fn.Pkg().Path() == "context" && fn.Name() == "Cause":
				out = append(out, member{"K7", pos(call), "context.Cause"})
				return true
			}
		}
		if sel.Sel.Name != "Err" && sel.Sel.Name != "Done" {
			return true
		}
		t := info.TypeOf(sel.X)
		if t == nil {
			return true
		}
		if named, ok := t.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context" {
			out = append(out, member{"K7", pos(call), "Context." + sel.Sel.Name})
		}
		return true
	})
	return out
}
