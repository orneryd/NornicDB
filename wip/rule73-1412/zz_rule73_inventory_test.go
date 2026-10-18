//go:build rule73probe

package cypher

// Rule 73 inventory extractor for #1412 (probe; not product code). It
// type-checks pkg/cypher and lists every structure that can hold entities
// read from storage, mechanically:
//   S  struct types with a field whose type holds entities (caches, buffers,
//      traversal and operator state);
//   G  package-level variables whose type holds entities;
//   R  storage reads: calls of a pkg/storage function or method (or of an
//      interface method declared there) returning entities in bulk, with
//      the enclosing function and whether a held* call wraps it;
//   V  storage visitors: calls of a pkg/storage function or method taking
//      an entity callback;
//   P  producers: functions of this package returning a type that holds
//      entities, with whether they read storage (transitively);
//   L  locals: variables of a type that holds entities, declared in a
//      function that reads storage or calls a producer, with how they get
//      their value (read / producer / built);
//   C  context values: context.WithValue whose value holds entities.
// Each line is tab-separated. Run: go test -run TestZZRule73Inventory -v.

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"
	"testing"
)

const storagePath = "github.com/orneryd/nornicdb/pkg/storage"

func zzHoldsEntities(t types.Type, depth int, seen map[types.Type]bool) bool {
	if t == nil || depth > 6 || seen[t] {
		return false
	}
	seen[t] = true
	defer delete(seen, t)
	switch u := t.(type) {
	case *types.Named:
		obj := u.Obj()
		if obj.Pkg() != nil && obj.Pkg().Path() == storagePath {
			switch obj.Name() {
			case "Node", "Edge", "NodeID", "EdgeID":
				return true
			}
		}
		if s, ok := u.Underlying().(*types.Struct); ok {
			return zzStructHolds(s, depth, seen)
		}
		return zzHoldsEntities(u.Underlying(), depth+1, seen)
	case *types.Pointer:
		return zzHoldsEntities(u.Elem(), depth+1, seen)
	case *types.Slice:
		return zzHoldsEntities(u.Elem(), depth+1, seen)
	case *types.Array:
		return zzHoldsEntities(u.Elem(), depth+1, seen)
	case *types.Map:
		return zzHoldsEntities(u.Key(), depth+1, seen) || zzHoldsEntities(u.Elem(), depth+1, seen)
	case *types.Chan:
		return zzHoldsEntities(u.Elem(), depth+1, seen)
	case *types.Struct:
		return zzStructHolds(u, depth, seen)
	}
	return false
}

func zzStructHolds(s *types.Struct, depth int, seen map[types.Type]bool) bool {
	for i := 0; i < s.NumFields(); i++ {
		if zzHoldsEntities(s.Field(i).Type(), depth+1, seen) {
			return true
		}
	}
	return false
}

// zzBulk reports whether t holds entities in bulk: a container (slice, array,
// map, chan) of entities, or a struct/named type that has one.
func zzBulk(t types.Type) bool { return zzBulkAt(t, map[types.Type]bool{}) }

func zzBulkAt(t types.Type, seen map[types.Type]bool) bool {
	if seen[t] || len(seen) > 8 {
		return false
	}
	seen[t] = true
	defer delete(seen, t)
	switch u := t.Underlying().(type) {
	case *types.Slice, *types.Array, *types.Map, *types.Chan:
		return zzHoldsEntities(u, 0, map[types.Type]bool{})
	case *types.Pointer:
		return zzBulkAt(u.Elem(), seen)
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if zzBulkAt(u.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func zzResultsBulk(sig *types.Signature) bool {
	for i := 0; i < sig.Results().Len(); i++ {
		if zzBulk(sig.Results().At(i).Type()) {
			return true
		}
	}
	return false
}

func zzTakesEntityCallback(sig *types.Signature) bool {
	for i := 0; i < sig.Params().Len(); i++ {
		if fn, ok := sig.Params().At(i).Type().Underlying().(*types.Signature); ok {
			for j := 0; j < fn.Params().Len(); j++ {
				if zzHoldsEntities(fn.Params().At(j).Type(), 0, map[types.Type]bool{}) {
					return true
				}
			}
		}
	}
	return false
}

func TestZZRule73Inventory(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil), Error: func(error) {}}
	if _, err := conf.Check("github.com/orneryd/nornicdb/pkg/cypher", fset, files, info); err != nil {
		t.Logf("type check (continuing): %v", err)
	}
	var lines []string
	emit := func(format string, args ...interface{}) { lines = append(lines, fmt.Sprintf(format, args...)) }
	pos := func(p token.Pos) string { position := fset.Position(p); return fmt.Sprintf("%s:%d", position.Filename, position.Line) }

	// S and G.
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					obj := info.Defs[s.Name]
					if obj == nil {
						continue
					}
					st, ok := obj.Type().Underlying().(*types.Struct)
					if !ok {
						continue
					}
					for i := 0; i < st.NumFields(); i++ {
						field := st.Field(i)
						if zzBulk(field.Type()) {
							emit("S\t%s\t%s.%s\t%s", pos(field.Pos()), s.Name.Name, field.Name(), types.TypeString(field.Type(), types.RelativeTo(obj.Pkg())))
						}
					}
				case *ast.ValueSpec:
					if gen.Tok != token.VAR {
						continue
					}
					for _, name := range s.Names {
						if obj := info.Defs[name]; obj != nil && zzBulk(obj.Type()) {
							emit("G\t%s\t%s\t%s", pos(name.Pos()), name.Name, obj.Type())
						}
					}
				}
			}
		}
	}

	// Producers (P), reads (R), visitors (V), locals (L), context values (C).
	type fnInfo struct {
		decl     *ast.FuncDecl
		reads    bool
		calls    map[string]bool
		producer bool
	}
	funcs := map[string]*fnInfo{}
	funcName := func(d *ast.FuncDecl) string {
		if d.Recv != nil && len(d.Recv.List) > 0 {
			recv := d.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			if id, ok := recv.(*ast.Ident); ok {
				return id.Name + "." + d.Name.Name
			}
		}
		return d.Name.Name
	}
	calleeName := func(call *ast.CallExpr) (types.Object, string) {
		switch f := call.Fun.(type) {
		case *ast.Ident:
			obj := info.Uses[f]
			return obj, f.Name
		case *ast.SelectorExpr:
			if sel := info.Selections[f]; sel != nil {
				return sel.Obj(), f.Sel.Name
			}
			obj := info.Uses[f.Sel]
			return obj, f.Sel.Name
		}
		return nil, ""
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || d.Body == nil {
				continue
			}
			fi := &fnInfo{decl: d, calls: map[string]bool{}}
			name := funcName(d)
			funcs[name] = fi
			if obj := info.Defs[d.Name]; obj != nil {
				if sig, ok := obj.Type().(*types.Signature); ok && zzResultsBulk(sig) {
					fi.producer = true
				}
			}
			var stack []ast.Node
			ast.Inspect(d.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				defer func() { stack = append(stack, n) }()
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				obj, short := calleeName(call)
				fn, _ := obj.(*types.Func)
				if fn == nil {
					return true
				}
				sig, _ := fn.Type().(*types.Signature)
				if sig == nil {
					return true
				}
				if fn.Pkg() != nil && fn.Pkg().Path() == storagePath {
					if zzResultsBulk(sig) {
						fi.reads = true
						held := false
						if parent, ok := stack[len(stack)-1].(*ast.CallExpr); ok {
							if sel, ok := parent.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(sel.Sel.Name, "held") {
								held = true
							}
						}
						emit("R\t%s\t%s\t%s\theld=%v", pos(call.Pos()), name, short, held)
					} else if zzTakesEntityCallback(sig) {
						emit("V\t%s\t%s\t%s", pos(call.Pos()), name, short)
					}
				} else if fn.Pkg() != nil && fn.Pkg().Path() == "github.com/orneryd/nornicdb/pkg/cypher" {
					recv := ""
					if r := sig.Recv(); r != nil {
						rt := r.Type()
						if p, ok := rt.(*types.Pointer); ok {
							rt = p.Elem()
						}
						if nt, ok := rt.(*types.Named); ok {
							recv = nt.Obj().Name() + "."
						}
					}
					fi.calls[recv+fn.Name()] = true
				}
				if short == "WithValue" && len(call.Args) == 3 {
					if tv, ok := info.Types[call.Args[2]]; ok && zzBulk(tv.Type) {
						emit("C\t%s\t%s\t%s", pos(call.Pos()), name, tv.Type)
					}
				}
				return true
			})
		}
	}
	// Typed use edges (E): every call of, or reference to, a function or
	// method of this package from a function's body, closures and function
	// values included (procedure registrations, plan tables).
	for _, file := range files {
		for _, decl := range file.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || d.Body == nil {
				continue
			}
			from := funcName(d)
			seenUse := map[string]bool{}
			ast.Inspect(d.Body, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				fn, ok := info.Uses[id].(*types.Func)
				if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
					return true
				}
				to := fn.Name()
				if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
					rt := sig.Recv().Type()
					if p, ok := rt.(*types.Pointer); ok {
						rt = p.Elem()
					}
					if nt, ok := rt.(*types.Named); ok {
						to = nt.Obj().Name() + "." + to
					}
				}
				if !seenUse[to] {
					seenUse[to] = true
					emit("E	%s	%s", from, to)
				}
				return true
			})
		}
	}
	// Transitive reads.
	for changed := true; changed; {
		changed = false
		for _, fi := range funcs {
			if fi.reads {
				continue
			}
			for callee := range fi.calls {
				if c := funcs[callee]; c != nil && c.reads {
					fi.reads, changed = true, true
					break
				}
			}
		}
	}
	names := make([]string, 0, len(funcs))
	for name := range funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fi := funcs[name]
		if fi.producer {
			emit("P\t%s\t%s\treadsStorage=%v", pos(fi.decl.Pos()), name, fi.reads)
		}
		if !fi.reads {
			continue
		}
		// Locals holding entities in a function that reads storage.
		ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
			var idents []*ast.Ident
			var values []ast.Expr
			switch s := n.(type) {
			case *ast.AssignStmt:
				if s.Tok != token.DEFINE {
					return true
				}
				for i, lhs := range s.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						idents = append(idents, id)
						if len(s.Rhs) == len(s.Lhs) {
							values = append(values, s.Rhs[i])
						} else {
							values = append(values, s.Rhs[0])
						}
					}
				}
			case *ast.ValueSpec:
				for i, id := range s.Names {
					idents = append(idents, id)
					if i < len(s.Values) {
						values = append(values, s.Values[i])
					} else {
						values = append(values, nil)
					}
				}
			default:
				return true
			}
			for i, id := range idents {
				obj := info.Defs[id]
				if obj == nil || !zzBulk(obj.Type()) {
					continue
				}
				kind := "declared"
				if v := values[i]; v != nil {
					kind = "built"
					ast.Inspect(v, func(m ast.Node) bool {
						if call, ok := m.(*ast.CallExpr); ok {
							o, short := calleeName(call)
							if f, ok := o.(*types.Func); ok && f.Pkg() != nil {
								switch {
								case f.Pkg().Path() == storagePath:
									kind = "read:" + short
								case strings.HasPrefix(short, "held"):
								case kind == "built":
									kind = "call:" + short
								}
							}
						}
						return true
					})
				}
				emit("L\t%s\t%s\t%s\t%s\t%s", pos(id.Pos()), name, id.Name, kind, types.TypeString(obj.Type(), func(p *types.Package) string { return p.Name() }))
			}
			return true
		})
	}
	sort.Strings(lines)
	counts := map[string]int{}
	for _, line := range lines {
		counts[line[:1]]++
		fmt.Println("INV\t" + line)
	}
	fmt.Printf("INVCOUNT\t%v\n", counts)
}
