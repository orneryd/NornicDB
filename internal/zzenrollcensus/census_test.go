// Author: #1636 lane (Claude), 30 Oct. Evidence tooling, never product code.
// Enrollment-completeness census (Rule 73 course change after the first
// whole-stack review). It loads every package of the module with full type
// information and reports one line per member:
//
//	E1 private lifecycle  a struct that holds a lifecycle owner AND its own
//	                      WaitGroup-shaped field, or a chan struct{} field that
//	                      is closed somewhere (a done/stop channel; wake-up
//	                      channels that are only sent to are not lifecycle)
//	E2 optional owner     in an owner-participating function, a no-op release
//	                      returned with success (true or a nil error), or an
//	                      owner-typed field reset to nil
//	E3 stop bypass        a Close/Shutdown/Stop method of an owner-holding type
//	                      that never calls the owner's Shutdown/WaitStopped
//	E5 foreign under lock in pkg/lifecycle, a call to an interface method, a
//	                      function value, or any function outside the owner
//	                      package (sync/atomic excepted) while a mutex is held
//	E6 dropped outcome    in an owner-participating function,
//	                      `_ = x.Close()/Shutdown()/Stop()` discarding a result
//
// An owner-participating function is a method of an owner-holding type, or a
// function whose body uses the lifecycle package, an owner-holding value, a
// value of a module interface that some owner-holding type implements (such as
// a replicator interface), or an acquire-shaped function value (one returning
// a func() release together with a bool or error).
//
// Membership is by type and value. Distinct populations that legitimately
// keep their own wait are listed in allow.txt with a reason; every listed
// member is printed as ALLOWED so the list stays reviewable.
// CENSUS_PLANT=1 also loads internal/zzenrollplant; the test fails unless each
// class detects its plants.
package zzenrollcensus

import (
	"bufio"
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

const lifecyclePkg = "github.com/orneryd/nornicdb/pkg/lifecycle"

type member struct{ class, pos, what string }

func moduleRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func loadAllow(t *testing.T) map[string]string {
	_, file, _, _ := runtime.Caller(0)
	allow := map[string]string{}
	f, err := os.Open(filepath.Join(filepath.Dir(file), "allow.txt"))
	if err != nil {
		return allow
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, reason, _ := strings.Cut(line, " ")
		allow[key] = strings.TrimSpace(reason)
	}
	return allow
}

func TestEnrollmentCensus(t *testing.T) {
	root := moduleRoot()
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo, Dir: root}
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
	allow := loadAllow(t)
	collectOwnerHolders(pkgs)
	var found []member
	for _, p := range pkgs {
		if strings.Contains(p.PkgPath, "zzenrollcensus") {
			continue
		}
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			found = append(found, scan(p, f)...)
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].class != found[j].class {
			return found[i].class < found[j].class
		}
		return found[i].pos < found[j].pos
	})
	counts, allowed, planted := map[string]int{}, map[string]int{}, map[string]int{}
	for _, m := range found {
		rel := m.pos
		if r, err := filepath.Rel(root, m.pos); err == nil {
			rel = filepath.ToSlash(r)
		}
		file := rel
		if i := strings.LastIndex(rel, ":"); i > 0 {
			file = rel[:i]
		}
		key := m.class + "@" + file + "#" + m.what
		if reason, ok := allow[key]; ok {
			fmt.Printf("ENROLL ALLOWED %s %s %s -- %s\n", m.class, rel, m.what, reason)
			allowed[m.class]++
			continue
		}
		fmt.Printf("ENROLL %s %s %s\n", m.class, rel, m.what)
		counts[m.class]++
		if strings.Contains(rel, "zzenrollplant") {
			planted[m.class]++
		}
	}
	classes := []string{"E1", "E2", "E3", "E5", "E6"}
	for _, c := range classes {
		fmt.Printf("ENROLL-COUNT %s %d (allowed %d)\n", c, counts[c], allowed[c])
	}
	if os.Getenv("CENSUS_PLANT") != "" {
		want := map[string]int{"E1": 2, "E2": 3, "E3": 2, "E5": 2, "E6": 2}
		for _, c := range classes {
			if planted[c] < want[c] {
				t.Errorf("planted control %s: detected %d of %d", c, planted[c], want[c])
			}
		}
		return
	}
	if os.Getenv("CENSUS_GATE") != "" {
		for _, c := range classes {
			if counts[c] != 0 {
				t.Errorf("%s: %d unallowed member(s)", c, counts[c])
			}
		}
	}
}

func isNamed(t types.Type, pkg, name string) bool {
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
}

func isOwner(t types.Type) bool {
	return isNamed(t, lifecyclePkg, "WorkLifetime")
}

func hasMethods(t types.Type, names ...string) bool {
	for _, c := range []types.Type{t, types.NewPointer(t)} {
		ms := types.NewMethodSet(c)
		ok := true
		for _, n := range names {
			hit := false
			for i := 0; i < ms.Len(); i++ {
				if ms.At(i).Obj().Name() == n {
					hit = true
					break
				}
			}
			if !hit {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func isEmptyStructChan(t types.Type) bool {
	ch, ok := t.Underlying().(*types.Chan)
	if !ok {
		return false
	}
	s, ok := ch.Elem().Underlying().(*types.Struct)
	return ok && s.NumFields() == 0
}

// ownerHolding reports whether a named type's struct holds an owner field.
func ownerHolding(t types.Type) bool {
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return false
	}
	for i := 0; i < st.NumFields(); i++ {
		if isOwner(st.Field(i).Type()) {
			return true
		}
	}
	return false
}

// closedFields lists the struct fields of a package that are closed somewhere.
func closedFields(p *packages.Package) map[*types.Var]bool {
	closed := map[*types.Var]bool{}
	for _, f := range p.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "close" {
				return true
			}
			if sel, ok := call.Args[0].(*ast.SelectorExpr); ok {
				if v, ok := p.TypesInfo.Uses[sel.Sel].(*types.Var); ok && v.IsField() {
					closed[v] = true
				}
			}
			return true
		})
	}
	return closed
}

var closedCache = map[*packages.Package]map[*types.Var]bool{}

// ownerHolders are the module's named types whose struct holds an owner.
var ownerHolders []types.Type

func collectOwnerHolders(pkgs []*packages.Package) {
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if p.Types == nil || !strings.HasPrefix(p.PkgPath, "github.com/orneryd/nornicdb") {
			return
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			if ownerHolding(tn.Type()) {
				ownerHolders = append(ownerHolders, tn.Type())
			}
		}
	})
}

// ownerInterface reports a module interface that an owner-holding type implements.
func ownerInterface(t types.Type) bool {
	n, ok := t.(*types.Named)
	if !ok || n.Obj().Pkg() == nil || !strings.HasPrefix(n.Obj().Pkg().Path(), "github.com/orneryd/nornicdb") {
		return false
	}
	iface, ok := n.Underlying().(*types.Interface)
	if !ok || iface.NumMethods() == 0 {
		return false
	}
	for _, h := range ownerHolders {
		if types.Implements(h, iface) || types.Implements(types.NewPointer(h), iface) {
			return true
		}
	}
	return false
}

func scan(p *packages.Package, f *ast.File) []member {
	var out []member
	info := p.TypesInfo
	if closedCache[p] == nil {
		closedCache[p] = closedFields(p)
	}
	closed := closedCache[p]
	pos := func(n ast.Node) string {
		ps := p.Fset.Position(n.Pos())
		return fmt.Sprintf("%s:%d", ps.Filename, ps.Line)
	}
	inLifecycle := p.PkgPath == lifecyclePkg || strings.HasSuffix(p.PkgPath, "/zzenrollplant/lock")
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if inLifecycle {
				continue
			}
			for _, spec := range d.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				var owner, private bool
				var privateFields []string
				for _, fld := range st.Fields.List {
					ft := info.TypeOf(fld.Type)
					if ft == nil {
						continue
					}
					if isOwner(ft) {
						owner = true
						continue
					}
					if hasMethods(ft, "Add", "Done", "Wait") {
						private = true
						for _, n := range fld.Names {
							privateFields = append(privateFields, n.Name)
						}
					} else if isEmptyStructChan(ft) {
						for _, n := range fld.Names {
							if v, ok := info.Defs[n].(*types.Var); ok && closed[v] {
								private = true
								privateFields = append(privateFields, n.Name)
							}
						}
					}
				}
				if owner && private {
					out = append(out, member{"E1", pos(ts), ts.Name.Name + "{" + strings.Join(privateFields, ",") + "}"})
				}
			}
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			out = append(out, scanFunc(p, d, inLifecycle, pos)...)
		}
	}
	return out
}

// acquireShaped reports a signature returning a func() release together with a
// bool or error: the shape of an admission.
func acquireShaped(t types.Type) bool {
	sig, ok := t.Underlying().(*types.Signature)
	if !ok {
		return false
	}
	var release, verdict bool
	for i := 0; i < sig.Results().Len(); i++ {
		rt := sig.Results().At(i).Type()
		if rs, ok := rt.Underlying().(*types.Signature); ok && rs.Params().Len() == 0 && rs.Results().Len() == 0 {
			release = true
		}
		if b, ok := rt.Underlying().(*types.Basic); ok && b.Kind() == types.Bool {
			verdict = true
		}
		if types.Identical(rt, types.Universe.Lookup("error").Type()) {
			verdict = true
		}
	}
	return release && verdict
}

func ownerParticipating(info *types.Info, d *ast.FuncDecl) bool {
	if d.Recv != nil && len(d.Recv.List) == 1 {
		if rt := info.TypeOf(d.Recv.List[0].Type); rt != nil && ownerHolding(rt) {
			return true
		}
	}
	found := false
	ast.Inspect(d.Body, func(n ast.Node) bool {
		if found {
			return false
		}
		switch x := n.(type) {
		case *ast.Ident:
			if obj := info.Uses[x]; obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == lifecyclePkg {
				found = true
			}
			if t := info.TypeOf(x); t != nil && (isOwner(t) || ownerHolding(t) || ownerInterface(t)) {
				found = true
			}
		case *ast.SelectorExpr:
			if t := info.TypeOf(x); t != nil {
				if isOwner(t) || ownerHolding(t) {
					found = true
				}
				if sel, ok := info.Selections[x]; ok && sel.Kind() == types.FieldVal && acquireShaped(t) {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

func scanFunc(p *packages.Package, d *ast.FuncDecl, inLifecycle bool, pos func(ast.Node) string) []member {
	var out []member
	info := p.TypesInfo
	participating := inLifecycle || ownerParticipating(info, d)
	name := d.Name.Name
	if d.Recv != nil && len(d.Recv.List) == 1 {
		if rt := info.TypeOf(d.Recv.List[0].Type); rt != nil {
			if n, ok := deref(rt).(*types.Named); ok {
				name = n.Obj().Name() + "." + d.Name.Name
			}
		}
	}
	// E3: a stop method of an owner-holding type that never joins the owner.
	if !inLifecycle && d.Recv != nil && (d.Name.Name == "Close" || d.Name.Name == "Shutdown" || d.Name.Name == "Stop") {
		if rt := info.TypeOf(d.Recv.List[0].Type); rt != nil && ownerHolding(rt) {
			joins := false
			ast.Inspect(d.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if fn, ok := info.Uses[sel.Sel].(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == lifecyclePkg {
						if fn.Name() == "Shutdown" || fn.Name() == "WaitStopped" {
							joins = true
						}
					}
					// Delegating to the type's own Shutdown/Close counts once that one joins.
					if fn, ok := info.Uses[sel.Sel].(*types.Func); ok && fn.Pkg() == p.Types && (fn.Name() == "Shutdown" || fn.Name() == "Close") && fn.Name() != d.Name.Name {
						joins = true
					}
				}
				return true
			})
			if !joins {
				out = append(out, member{"E3", pos(d), name})
			}
		}
	}
	ast.Inspect(d.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.ReturnStmt:
			if inLifecycle {
				return true
			}
			var empty, success bool
			for _, r := range n.Results {
				if lit, ok := r.(*ast.FuncLit); ok && len(lit.Body.List) == 0 && lit.Type.Params.NumFields() == 0 {
					empty = true
				}
				if id, ok := r.(*ast.Ident); ok && (id.Name == "true" || id.Name == "nil") {
					success = true
				}
			}
			if empty && success && participating && resultsHaveFunc(info, d) {
				out = append(out, member{"E2", pos(n), name + " returns an accepted no-op release"})
			}
		case *ast.AssignStmt:
			// E2: owner identity reset to nil.
			if !inLifecycle && len(n.Lhs) == len(n.Rhs) {
				for i, l := range n.Lhs {
					if id, ok := n.Rhs[i].(*ast.Ident); ok && id.Name == "nil" {
						if lt := info.TypeOf(l); lt != nil && isOwner(lt) {
							out = append(out, member{"E2", pos(n), name + " resets an owner to nil"})
						}
					}
				}
			}
			// E6: discarded stop outcome.
			if participating && len(n.Lhs) == 1 && len(n.Rhs) == 1 {
				if id, ok := n.Lhs[0].(*ast.Ident); ok && id.Name == "_" {
					if call, ok := n.Rhs[0].(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Close" || sel.Sel.Name == "Shutdown" || sel.Sel.Name == "Stop") {
							out = append(out, member{"E6", pos(n), name + " discards " + sel.Sel.Name + "()"})
						}
					}
				}
			}
		}
		return true
	})
	if inLifecycle {
		out = append(out, scanLocked(p, d, name, pos)...)
	}
	return out
}

func deref(t types.Type) types.Type {
	if ptr, ok := t.(*types.Pointer); ok {
		return ptr.Elem()
	}
	return t
}

func resultsHaveFunc(info *types.Info, d *ast.FuncDecl) bool {
	if d.Type.Results == nil {
		return false
	}
	for _, r := range d.Type.Results.List {
		if t := info.TypeOf(r.Type); t != nil {
			if _, ok := t.Underlying().(*types.Signature); ok {
				return true
			}
		}
	}
	return false
}

// scanLocked walks each block in order and flags calls made while a mutex taken
// in that block is held: interface method calls and calls through function values.
func scanLocked(p *packages.Package, d *ast.FuncDecl, name string, pos func(ast.Node) string) []member {
	var out []member
	info := p.TypesInfo
	var walk func(stmts []ast.Stmt, held bool)
	isLockCall := func(s ast.Stmt, method string) bool {
		es, ok := s.(*ast.ExprStmt)
		if !ok {
			return false
		}
		call, ok := es.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != method {
			return false
		}
		t := info.TypeOf(sel.X)
		return t != nil && (isNamed(t, "sync", "Mutex") || isNamed(t, "sync", "RWMutex"))
	}
	isDeferUnlock := func(s ast.Stmt) bool {
		ds, ok := s.(*ast.DeferStmt)
		if !ok {
			return false
		}
		sel, ok := ds.Call.Fun.(*ast.SelectorExpr)
		return ok && (sel.Sel.Name == "Unlock" || sel.Sel.Name == "RUnlock")
	}
	flagCalls := func(n ast.Node) {
		ast.Inspect(n, func(x ast.Node) bool {
			if _, ok := x.(*ast.FuncLit); ok {
				return false
			}
			call, ok := x.(*ast.CallExpr)
			if !ok {
				return true
			}
			if tv, ok := info.Types[call.Fun]; ok && tv.IsType() {
				return true // conversion
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				if fn, ok := info.Uses[fun.Sel].(*types.Func); ok && fn.Pkg() != nil && fn.Pkg() != p.Types && info.Selections[fun] == nil {
					if path := fn.Pkg().Path(); path != "sync" && path != "sync/atomic" {
						out = append(out, member{"E5", pos(call), name + " calls " + fn.Pkg().Name() + "." + fn.Name() + " under lock"})
					}
				}
				if s, ok := info.Selections[fun]; ok && s.Kind() == types.MethodVal {
					if _, iface := s.Recv().Underlying().(*types.Interface); iface {
						out = append(out, member{"E5", pos(call), name + " calls interface method " + fun.Sel.Name + " under lock"})
					}
				} else if s, ok := info.Selections[fun]; ok && s.Kind() == types.FieldVal {
					out = append(out, member{"E5", pos(call), name + " calls function value " + fun.Sel.Name + " under lock"})
				}
			case *ast.Ident:
				if fn, ok := info.Uses[fun].(*types.Func); ok && fn.Pkg() != nil && fn.Pkg() != p.Types {
					out = append(out, member{"E5", pos(call), name + " calls " + fn.Pkg().Name() + "." + fn.Name() + " under lock"})
				}
				if v, ok := info.Uses[fun].(*types.Var); ok {
					if _, sig := v.Type().Underlying().(*types.Signature); sig {
						out = append(out, member{"E5", pos(call), name + " calls function value " + fun.Name + " under lock"})
					}
				}
			}
			return true
		})
	}
	walk = func(stmts []ast.Stmt, held bool) {
		for _, s := range stmts {
			switch {
			case isLockCall(s, "Lock") || isLockCall(s, "RLock"):
				held = true
				continue
			case isLockCall(s, "Unlock") || isLockCall(s, "RUnlock"):
				held = false
				continue
			case isDeferUnlock(s):
				continue
			}
			if held {
				flagCalls(s)
			} else if b, ok := s.(*ast.BlockStmt); ok {
				walk(b.List, held)
			} else if ifs, ok := s.(*ast.IfStmt); ok {
				walk(ifs.Body.List, held)
			}
		}
	}
	walk(d.Body.List, false)
	_ = token.NoPos
	return out
}
