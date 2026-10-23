package storagecensus

import (
	"fmt"
	"go/ast"
	"go/types"
	"regexp"
	"sort"
	"strings"
)

// Class is a kind of effect on stored entities.
type Class string

const (
	// Write is a physical Badger write: a write transaction, batch, commit,
	// prefix drop or load.
	Write Class = "write"
	// Counter is a change to a node or relationship count, a derived count,
	// or quota usage: an update of a counting field, or a call of a
	// counting function.
	Counter Class = "counter"
	// Hidden is a hidden-entity decision taken next to an entity write: a
	// function that tests whether an entity is hidden or suppressed, or
	// reads its stored image, and stages or performs a write itself.
	Hidden Class = "hidden"
	// Total is a read of an integrity or quota total: a node or
	// relationship count, or a stored size.
	Total Class = "total"
)

// Classes are the effect classes, in report order.
var Classes = []Class{Write, Counter, Hidden, Total}

// badgerWrites are the Badger methods that write.
var badgerWrites = map[string]bool{
	"Update": true, "NewWriteBatch": true, "NewWriteBatchAt": true, "NewManagedWriteBatch": true,
	"DropPrefix": true, "DropAll": true, "Load": true, "Commit": true, "CommitAt": true, "CommitWith": true,
}

// badgerStages are the Badger methods that stage a write in a transaction or
// batch.
var badgerStages = map[string]bool{"Set": true, "SetEntry": true, "Delete": true}

var (
	// counterField names a struct field that holds a count or a size.
	counterField = regexp.MustCompile(`(?i)(count|size|usage|reserv|quota)`)
	// counterFunc names a function that changes a count, a size or usage.
	counterFunc = regexp.MustCompile(`(?i)^(adjust|increment|decrement|apply|add|publish|fold|reset|set|charge|release|reserve|commit)[A-Za-z]*(count|size|usage|reserv|quota|delta)[A-Za-z]*$`)
	// totalFunc names a function that reads an integrity or quota total.
	totalFunc = regexp.MustCompile(`^(NodeCount|EdgeCount|[A-Za-z]*NodeCount[A-Za-z]*|[A-Za-z]*EdgeCount[A-Za-z]*|GetStorageSize|getCurrentStorageSize|calculate[A-Za-z]*Size[A-Za-z]*|storageSize[A-Za-z]*)$`)
	// hiddenFunc names a function that decides or reads hidden state, or a
	// stored image that includes hidden entities.
	hiddenFunc = regexp.MustCompile(`^(hides(Node|Edge)|manuallySuppressed|evaluate(Node|Edge)Suppression[A-Za-z]*|read(Node|Edge)SuppressionState|[A-Za-z]*[Hh]idden[A-Za-z]*|Stored(Node|Edge)Image)$`)
	// stageFunc names an own function that stages entity bytes in a
	// transaction or commit writer.
	stageFunc = regexp.MustCompile(`^(bufferSet|bufferDelete|write|writeOnce|writeUnits)$`)
	// counterMutators are the methods of atomic counters that change them.
	counterMutators = map[string]bool{"Add": true, "Store": true, "Swap": true, "CompareAndSwap": true, "And": true, "Or": true}
)

// fn is one function, method or go statement of the analysed packages.
type fn struct {
	key    string
	pkg    string
	file   string
	entry  bool
	calls  map[string]bool
	iface  map[ifaceCall]bool
	sites  map[Class]map[string]bool
	hidden bool
	stages bool
}

type ifaceCall struct {
	iface  *types.Interface
	method string
}

// graph is the analysed call graph.
type graph struct {
	prog *program
	fns  map[string]*fn
	// named are the counter and total targets the patterns found.
	named map[string]bool
}

func (g *graph) node(key string) *fn {
	f := g.fns[key]
	if f == nil {
		f = &fn{key: key, calls: map[string]bool{}, iface: map[ifaceCall]bool{}, sites: map[Class]map[string]bool{}}
		g.fns[key] = f
	}
	return f
}

func (f *fn) site(c Class, what string) {
	if f.sites[c] == nil {
		f.sites[c] = map[string]bool{}
	}
	f.sites[c][what] = true
}

// shortOf names an analysed package by its last element, or "".
func (g *graph) shortOf(p *types.Package) string {
	if p == nil {
		return ""
	}
	for _, l := range g.prog.pkgs {
		if l.pkg == p {
			return l.short
		}
	}
	return ""
}

// funcKey names a function "pkg.Name" and a method "pkg.Type.Name".
func (g *graph) funcKey(f *types.Func) string {
	short := g.shortOf(f.Pkg())
	if short == "" {
		return ""
	}
	sig, ok := f.Type().(*types.Signature)
	if !ok {
		return ""
	}
	recv := sig.Recv()
	if recv == nil {
		return short + "." + f.Name()
	}
	named := namedOf(recv.Type())
	if named == nil {
		return ""
	}
	return short + "." + named.Obj().Name() + "." + f.Name()
}

func namedOf(t types.Type) *types.Named {
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, _ := t.(*types.Named)
	return named
}

func receiverName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return receiverName(e.X)
	case *ast.IndexListExpr:
		return receiverName(e.X)
	}
	return ""
}

// build analyses every function body of the program.
func build(prog *program) *graph {
	g := &graph{prog: prog, fns: map[string]*fn{}}
	for _, l := range prog.pkgs {
		for _, file := range l.files {
			name := prog.fset.Position(file.Package).Filename
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				// An entry point is an exported function or an exported
				// method of an exported type; a method of an unexported type
				// is reached from its callers, through an interface too.
				key := l.short + "." + fd.Name.Name
				exported := ast.IsExported(fd.Name.Name)
				if fd.Recv != nil && len(fd.Recv.List) == 1 {
					recv := receiverName(fd.Recv.List[0].Type)
					key = l.short + "." + recv + "." + fd.Name.Name
					exported = exported && ast.IsExported(recv)
				}
				g.visit(l, name, key, exported, fd.Body)
			}
		}
	}
	g.resolveInterfaces()
	return g
}

// visit records body's calls and effect sites as key's. A function a go
// statement starts runs after its parent returned: it is an entry point of
// its own, "<parent>$go<n>".
func (g *graph) visit(l *loaded, file, key string, entry bool, body ast.Node) {
	f := g.node(key)
	f.pkg, f.file, f.entry = l.short, file, f.entry || entry
	info := l.info
	spawned := 0
	link := func(obj types.Object) {
		fo, ok := obj.(*types.Func)
		if !ok {
			return
		}
		if target := g.funcKey(fo); target != "" && target != key {
			f.calls[target] = true
		}
	}
	ownField := func(e ast.Expr) (string, bool) {
		sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		s := info.Selections[sel]
		if s == nil || s.Kind() != types.FieldVal {
			return "", false
		}
		v, ok := s.Obj().(*types.Var)
		if !ok || g.shortOf(v.Pkg()) == "" || !counterField.MatchString(v.Name()) {
			return "", false
		}
		owner := ""
		if n := namedOf(s.Recv()); n != nil {
			owner = n.Obj().Name() + "."
		}
		return g.shortOf(v.Pkg()) + "." + owner + v.Name(), true
	}
	counterTarget := func(e ast.Expr) (string, bool) {
		e = ast.Unparen(e)
		if ix, ok := e.(*ast.IndexExpr); ok {
			e = ix.X
		}
		if star, ok := e.(*ast.StarExpr); ok {
			e = star.X
		}
		return ownField(e)
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			spawned++
			g.visit(l, file, fmt.Sprintf("%s$go%d", key, spawned), true, x.Call)
			return false
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				if field, ok := counterTarget(lhs); ok {
					f.site(Counter, "assign "+field)
				}
			}
		case *ast.IncDecStmt:
			if field, ok := counterTarget(x.X); ok {
				f.site(Counter, "assign "+field)
			}
		case *ast.CallExpr:
			if id, ok := ast.Unparen(x.Fun).(*ast.Ident); ok && id.Name == "delete" && len(x.Args) > 0 {
				if field, ok := ownField(x.Args[0]); ok {
					f.site(Counter, "delete "+field)
				}
			}
			if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok {
				if counterMutators[sel.Sel.Name] {
					if field, ok := ownField(sel.X); ok {
						f.site(Counter, sel.Sel.Name+" "+field)
					}
				}
				// NewTransaction(update=true) opens a write transaction.
				if (sel.Sel.Name == "NewTransaction" || sel.Sel.Name == "NewTransactionAt") && len(x.Args) > 0 {
					if id, ok := x.Args[len(x.Args)-1].(*ast.Ident); ok && id.Name == "true" {
						if s := info.Selections[sel]; s != nil && isBadger(s.Obj().Pkg()) {
							f.site(Write, "badger."+sel.Sel.Name+"(update)")
							f.stages = true
						}
					}
				}
			}
		case *ast.SelectorExpr:
			s := info.Selections[x]
			if s == nil {
				link(info.Uses[x.Sel])
				break
			}
			if s.Kind() != types.MethodVal && s.Kind() != types.MethodExpr {
				break
			}
			fo, ok := s.Obj().(*types.Func)
			if !ok {
				break
			}
			if isBadger(fo.Pkg()) {
				if badgerWrites[fo.Name()] {
					f.site(Write, "badger."+fo.Name())
					f.stages = true
				}
				if badgerStages[fo.Name()] {
					f.stages = true
				}
				break
			}
			if g.shortOf(fo.Pkg()) == "" {
				break
			}
			if iface, ok := s.Recv().Underlying().(*types.Interface); ok {
				f.iface[ifaceCall{iface: iface, method: fo.Name()}] = true
				g.classifyCall(f, g.ifaceName(s.Recv(), fo.Name()), fo.Name())
				break
			}
			link(fo)
			g.classifyCall(f, g.funcKey(fo), fo.Name())
		case *ast.Ident:
			if fo, ok := info.Uses[x].(*types.Func); ok {
				link(fo)
				if sig, ok := fo.Type().(*types.Signature); ok && sig.Recv() == nil {
					g.classifyCall(f, g.funcKey(fo), fo.Name())
				}
			}
		}
		return true
	})
}

func (g *graph) ifaceName(t types.Type, method string) string {
	if n := namedOf(t); n != nil {
		if short := g.shortOf(n.Obj().Pkg()); short != "" {
			return short + "." + n.Obj().Name() + "." + method
		}
		return n.Obj().Name() + "." + method
	}
	return "interface." + method
}

func isBadger(p *types.Package) bool {
	return p != nil && strings.HasPrefix(p.Path(), "github.com/dgraph-io/badger")
}

// classifyCall records the effect sites a call of name (callee key target)
// makes in f.
func (g *graph) classifyCall(f *fn, target, name string) {
	if target == "" {
		return
	}
	if counterFunc.MatchString(name) {
		f.site(Counter, "call "+target)
	} else if totalFunc.MatchString(name) {
		f.site(Total, "call "+target)
	}
	if hiddenFunc.MatchString(name) {
		f.hidden = true
	}
	if stageFunc.MatchString(name) {
		f.stages = true
	}
}

// resolveInterfaces adds, for every call through an interface, an edge to
// each analysed concrete type's method that implements it (class
// hierarchy analysis).
func (g *graph) resolveInterfaces() {
	var concrete []*types.Named
	for _, l := range g.prog.pkgs {
		scope := l.pkg.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			n, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			if _, isIface := n.Underlying().(*types.Interface); isIface {
				continue
			}
			concrete = append(concrete, n)
		}
	}
	keys := make([]string, 0, len(g.fns))
	for k := range g.fns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		f := g.fns[k]
		for call := range f.iface {
			for _, n := range concrete {
				ptr := types.NewPointer(n)
				if !types.Implements(ptr, call.iface) {
					continue
				}
				obj, _, _ := types.LookupFieldOrMethod(ptr, true, n.Obj().Pkg(), call.method)
				if fo, ok := obj.(*types.Func); ok {
					if target := g.funcKey(fo); target != "" && target != k {
						f.calls[target] = true
					}
				}
			}
		}
	}
}
