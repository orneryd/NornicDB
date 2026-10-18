// zzinventory: test-only diagnostic (never committed). Mechanical inventory of
// every loop and bulk native operation in the given packages, classified by
// what it iterates, and whether it observes the statement's cancellation.
//
//	go run ./zzinventory pkg/cypher pkg/cypher/fn
//
// Output (TSV): LOOP|BULK|STAGE  file:line  func  domain  type  checked  inDataLoop
package main

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var fset = token.NewFileSet()

// clientUses: the objects of the analysed packages that ZZ_CLIENTS (comma
// separated package dirs: pkg/bolt, pkg/server, ...) use, by FullName.
var clientUses = map[string]string{}

func main() {
	build.Default.BuildTags = []string{"noui", "nolocalllm"}
	imp := importer.ForCompiler(fset, "source", nil)
	for _, dir := range strings.Split(os.Getenv("ZZ_CLIENTS"), ",") {
		if dir != "" {
			collectClientUses(dir, imp)
		}
	}
	for _, dir := range os.Args[1:] {
		analyse(dir, imp)
	}
}

func collectClientUses(dir string, imp types.Importer) {
	bp, err := build.ImportDir(dir, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client", dir, err)
		return
	}
	var files []*ast.File
	for _, name := range bp.GoFiles {
		if f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0); err == nil {
			files = append(files, f)
		}
	}
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	var first error
	conf := types.Config{Importer: imp, Error: func(err error) {
		if first == nil {
			first = err
		}
	}}
	_, _ = conf.Check(bp.ImportPath, fset, files, info)
	found := 0
	for _, obj := range info.Uses {
		if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil && strings.Contains(fn.Pkg().Path(), "/pkg/cypher") {
			clientUses[fn.Origin().FullName()] = dir
			found++
		}
	}
	fmt.Fprintf(os.Stderr, "client %s: %d files, %d uses, %d pkg/cypher func uses, first error: %v\n", dir, len(files), len(info.Uses), found, first)
}

type fnInfo struct {
	decl   *ast.FuncDecl
	obj    *types.Func
	direct bool
	calls  []*types.Func
	refs   []*types.Func
	// ifaceCalls: method names called through an interface.
	ifaceCalls []string
	checks bool
	// reach: referenced, transitively, from StorageExecutor.Execute (or,
	// in a package without it, from any exported function).
	reach bool
}

func analyse(dir string, imp types.Importer) {
	bp, err := build.ImportDir(dir, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var files []*ast.File
	for _, name := range bp.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		files = append(files, f)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{Importer: imp, Error: func(error) {}}
	if _, err := conf.Check("github.com/orneryd/nornicdb/"+filepath.ToSlash(dir), fset, files, info); err != nil {
		fmt.Fprintln(os.Stderr, "typecheck:", err)
	}
	a := &analyser{info: info, fns: map[*types.Func]*fnInfo{}}
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				obj, _ := info.Defs[fd.Name].(*types.Func)
				fi := &fnInfo{decl: fd, obj: obj}
				a.fns[obj] = fi
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if a.isDirectCheck(call) {
							fi.direct = true
						}
						if callee := a.callee(call); callee != nil {
							fi.calls = append(fi.calls, callee)
						}
					}
					if sel, ok := n.(*ast.SelectStmt); ok && a.selectsDone(sel) {
						fi.direct = true
					}
					if id, ok := n.(*ast.Ident); ok {
						if ref, ok := info.Uses[id].(*types.Func); ok {
							fi.refs = append(fi.refs, ref.Origin())
						}
					}
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if s := info.Selections[sel]; s != nil && types.IsInterface(s.Recv()) {
							fi.ifaceCalls = append(fi.ifaceCalls, sel.Sel.Name)
						}
					}
					return true
				})
			}
		}
	}
	// Methods by name, for calls through an interface.
	methodsByName := map[string][]*fnInfo{}
	for _, fi := range a.fns {
		if fi.decl.Recv != nil {
			methodsByName[fi.decl.Name.Name] = append(methodsByName[fi.decl.Name.Name], fi)
		}
	}
	for _, fi := range a.fns {
		for _, name := range fi.ifaceCalls {
			for _, m := range methodsByName[name] {
				fi.refs = append(fi.refs, m.obj)
			}
		}
	}
	// Package-level initializers register handlers (procedure tables).
	var initRefs []*types.Func
	for _, f := range files {
		for _, d := range f.Decls {
			if gd, ok := d.(*ast.GenDecl); ok && gd.Tok == token.VAR {
				ast.Inspect(gd, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok {
						if ref, ok := info.Uses[id].(*types.Func); ok {
							initRefs = append(initRefs, ref.Origin())
						}
					}
					return true
				})
			}
		}
	}
	// checks: a direct check, or a static call to a function that checks.
	for _, fi := range a.fns {
		fi.checks = fi.direct
	}
	for changed := true; changed; {
		changed = false
		for _, fi := range a.fns {
			if fi.checks {
				continue
			}
			for _, c := range fi.calls {
				if g := a.fns[c]; g != nil && g.checks {
					fi.checks, changed = true, true
					break
				}
			}
		}
	}
	var roots []*fnInfo
	rootKinds := map[string]int{}
	for _, fi := range a.fns {
		if fi.obj == nil {
			continue
		}
		switch {
		case funcName(fi.decl) == "StorageExecutor.Execute":
			roots = append(roots, fi)
			rootKinds["Execute"]++
		case clientUses[fi.obj.FullName()] != "":
			roots = append(roots, fi)
			rootKinds["client:"+clientUses[fi.obj.FullName()]]++
			fmt.Fprintf(os.Stderr, "ROOT\t%s\t%s\t%s\n", clientUses[fi.obj.FullName()], pos(fi.decl.Pos()), funcName(fi.decl))
		case fi.decl.Recv == nil && fi.decl.Name.Name == "init":
			roots = append(roots, fi)
			rootKinds["init"]++
		}
	}
	for _, r := range initRefs {
		if g := a.fns[r]; g != nil {
			roots = append(roots, g)
			rootKinds["var-initializer"]++
		}
	}
	fmt.Fprintln(os.Stderr, "roots", dir, rootKinds)
	if len(roots) == 0 {
		for _, fi := range a.fns {
			if fi.obj != nil && fi.obj.Exported() {
				roots = append(roots, fi)
			}
		}
	}
	for len(roots) > 0 {
		fi := roots[len(roots)-1]
		roots = roots[:len(roots)-1]
		if fi.reach {
			continue
		}
		fi.reach = true
		for _, r := range fi.refs {
			if g := a.fns[r]; g != nil && !g.reach {
				roots = append(roots, g)
			}
		}
	}
	var out []string
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			name := funcName(fd)
			if obj, ok := a.info.Defs[fd.Name].(*types.Func); ok && a.fns[obj] != nil {
				a.reach = a.fns[obj].reach
			}
			if a.isStage(fd) {
				fi := a.fns[a.info.Defs[fd.Name].(*types.Func)]
				out = append(out, fmt.Sprintf("STAGE\t%s\t%s\t-\t-\t%v\t-\t%v", pos(fd.Pos()), name, fi != nil && fi.checks, a.reach))
			}
			a.walk(fd.Body, name, 0, &out)
		}
	}
	sort.Strings(out)
	for _, line := range out {
		fmt.Println(line)
	}
}

type analyser struct {
	info  *types.Info
	reach bool
	fns   map[*types.Func]*fnInfo
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		t := fd.Recv.List[0].Type
		if s, ok := t.(*ast.StarExpr); ok {
			t = s.X
		}
		if ix, ok := t.(*ast.IndexExpr); ok {
			t = ix.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

func pos(p token.Pos) string {
	position := fset.Position(p)
	return fmt.Sprintf("%s:%d", filepath.ToSlash(position.Filename), position.Line)
}

// walk visits loops; depth counts enclosing data loops.
func (a *analyser) walk(n ast.Node, fn string, depth int, out *[]string) {
	ast.Inspect(n, func(m ast.Node) bool {
		switch s := m.(type) {
		case *ast.RangeStmt:
			domain, typ := a.rangeDomain(s.X)
			a.emitLoop(s, s.Body, fn, domain, typ, depth, out)
			next := depth
			if domain == "DATA" {
				next++
			}
			a.walk(s.Body, fn, next, out)
			return false
		case *ast.ForStmt:
			domain, typ := a.condDomain(s)
			a.emitLoop(s, s.Body, fn, domain, typ, depth, out)
			next := depth
			if domain == "DATA" {
				next++
			}
			a.walk(s.Body, fn, next, out)
			return false
		case *ast.CallExpr:
			if kind, typ, ok := a.bulk(s); ok {
				*out = append(*out, fmt.Sprintf("BULK\t%s\t%s\t%s\t%s\t-\t%v\t%v", pos(s.Pos()), fn, kind, typ, depth > 0, a.reach))
			}
		}
		return true
	})
}

func (a *analyser) emitLoop(n ast.Node, body *ast.BlockStmt, fn, domain, typ string, depth int, out *[]string) {
	*out = append(*out, fmt.Sprintf("LOOP\t%s\t%s\t%s\t%s\t%v\t%v\t%v", pos(n.Pos()), fn, domain, typ, a.bodyChecks(body), depth > 0, a.reach))
}

// bodyChecks: the loop body observes cancellation per iteration, directly or
// through a function that checks, or by passing a probe on.
func (a *analyser) bodyChecks(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		switch x := n.(type) {
		case *ast.CallExpr:
			if a.isDirectCheck(x) || a.passesProbe(x) {
				found = true
			}
			if c := a.callee(x); c != nil {
				if g := a.fns[c]; g != nil && g.checks {
					found = true
				}
			}
		case *ast.SelectStmt:
			if a.selectsDone(x) {
				found = true
			}
		}
		return true
	})
	return found
}

func (a *analyser) callee(call *ast.CallExpr) *types.Func {
	switch f := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		if fn, ok := a.info.Uses[f].(*types.Func); ok {
			return fn.Origin()
		}
	case *ast.SelectorExpr:
		if fn, ok := a.info.Uses[f.Sel].(*types.Func); ok {
			return fn.Origin()
		}
	case *ast.IndexExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			if fn, ok := a.info.Uses[id].(*types.Func); ok {
				return fn.Origin()
			}
		}
	}
	return nil
}

var probeTypes = []string{"pipelineCancellation", "Cancellation", "Work"}

func isProbeType(t types.Type) bool {
	if t == nil {
		return false
	}
	s := t.String()
	for _, name := range probeTypes {
		if strings.HasSuffix(s, "."+name) || strings.HasSuffix(s, "*"+name) {
			return true
		}
	}
	return strings.HasSuffix(s, ".pipelineCancellation")
}

func (a *analyser) passesProbe(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		if isProbeType(a.info.TypeOf(arg)) {
			if id, ok := ast.Unparen(arg).(*ast.Ident); ok && id.Name == "nil" {
				continue
			}
			return true
		}
	}
	return false
}

func (a *analyser) isDirectCheck(call *ast.CallExpr) bool {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	recv := a.info.TypeOf(sel.X)
	switch sel.Sel.Name {
	case "check", "poll", "chargeText", "Continue":
		return isProbeType(recv)
	case "Err":
		return recv != nil && (strings.HasSuffix(recv.String(), "context.Context") || strings.Contains(recv.String(), "Context"))
	case "traversalCancelled":
		return true
	}
	return false
}

func (a *analyser) selectsDone(sel *ast.SelectStmt) bool {
	found := false
	ast.Inspect(sel, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Done" {
				found = true
			}
		}
		return !found
	})
	return found
}

// isStage: a function that takes rows, a row source or result rows.
func (a *analyser) isStage(fd *ast.FuncDecl) bool {
	for _, field := range fd.Type.Params.List {
		s := a.info.TypeOf(field.Type)
		if s == nil {
			continue
		}
		ts := s.String()
		if strings.HasSuffix(ts, "[]github.com/orneryd/nornicdb/pkg/cypher.pipelineRow") || strings.HasSuffix(ts, ".pipelineRowSource") || ts == "[][]interface{}" || ts == "[][]any" {
			return true
		}
	}
	return false
}

func short(t types.Type) string {
	s := t.String()
	s = strings.ReplaceAll(s, "github.com/orneryd/nornicdb/pkg/", "")
	return s
}

// stmtNamed are statement-structure types: their size is the statement's.
var stmtNamed = []string{"pipelineClause", "returnProjection", "orderByTerm", "returnItem", "token", "Token", "clause", "Clause", "Projection", "projection", "pattern", "Pattern", "segment", "span", "Span", "keyword", "argument", "operand", "Operand", "Node", "Item", "item", "part", "Part", "plan", "Plan", "term", "Term", "selector", "check", "step", "Step", "spec", "Spec", "Prop", "label", "boundary", "Boundary"}

func classify(t types.Type) string {
	if t == nil {
		return "AMBIG"
	}
	switch u := t.Underlying().(type) {
	case *types.Basic:
		if u.Info()&types.IsString != 0 {
			return "STMT"
		}
		if u.Info()&types.IsInteger != 0 {
			return "INT"
		}
	case *types.Chan, *types.Signature:
		return "DATA"
	}
	s := t.String()
	switch {
	case strings.HasSuffix(s, ".pipelineRow"):
		return "STMT" // a row's keys are the statement's variables
	case strings.Contains(s, "pipelineRow"), strings.Contains(s, "storage.Node"), strings.Contains(s, "storage.Edge"),
		strings.Contains(s, "storage.NodeID"), strings.Contains(s, "storage.EdgeID"), strings.Contains(s, "PathResult"),
		s == "[]interface{}", s == "[]any", s == "[][]interface{}", s == "[][]any", s == "map[string]interface{}", s == "map[string]any",
		strings.HasPrefix(s, "[]map[string]"), strings.HasPrefix(s, "map[interface{}]"), strings.HasPrefix(s, "[]float"),
		strings.HasPrefix(s, "[]int64"), strings.HasPrefix(s, "[]time."), strings.HasPrefix(s, "[]*github.com/orneryd/nornicdb/pkg/storage"):
		return "DATA"
	case s == "[]byte" || s == "[]rune" || s == "[]uint8" || s == "[]int32":
		return "STMT"
	}
	for _, n := range stmtNamed {
		if strings.Contains(s, n) {
			return "STMT"
		}
	}
	return "AMBIG"
}

func (a *analyser) rangeDomain(x ast.Expr) (string, string) {
	t := a.info.TypeOf(x)
	if t == nil {
		return "AMBIG", "?"
	}
	if b, ok := t.Underlying().(*types.Basic); ok && b.Info()&types.IsInteger != 0 {
		// range over n: classify by what n measures (len(x)).
		if call, ok := ast.Unparen(x).(*ast.CallExpr); ok && len(call.Args) == 1 {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "len" {
				at := a.info.TypeOf(call.Args[0])
				return classify(at), "len(" + shortOrQ(at) + ")"
			}
		}
		return "INT", short(t)
	}
	return classify(t), short(t)
}

func shortOrQ(t types.Type) string {
	if t == nil {
		return "?"
	}
	return short(t)
}

// condDomain: a for-cond loop is classified by the operand of len() in its
// condition, else by the types of the identifiers compared.
func (a *analyser) condDomain(s *ast.ForStmt) (string, string) {
	if s.Cond == nil {
		return "INF", "for{}"
	}
	domain, typ := "", ""
	ast.Inspect(s.Cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && len(call.Args) == 1 {
			if id, ok := call.Fun.(*ast.Ident); ok && (id.Name == "len" || id.Name == "cap") {
				t := a.info.TypeOf(call.Args[0])
				domain, typ = classify(t), "len("+shortOrQ(t)+")"
				return false
			}
		}
		return true
	})
	if domain != "" {
		return domain, typ
	}
	return "INT", "cond"
}

// bulk: a native operation whose cost is its argument's size.
func (a *analyser) bulk(call *ast.CallExpr) (string, string, bool) {
	name := ""
	switch f := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		name = f.Name
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			name = id.Name + "." + f.Sel.Name
		}
	}
	switch name {
	case "sort.Slice", "sort.SliceStable", "sort.Sort", "sort.Stable", "slices.Sort", "slices.SortFunc", "slices.SortStableFunc",
		"copy", "slices.Clone", "maps.Copy", "maps.Clone", "strings.Join", "slices.Equal", "reflect.DeepEqual", "fmt.Sprintf", "fmt.Sprint", "json.Marshal":
	case "append":
		if !call.Ellipsis.IsValid() {
			return "", "", false
		}
	default:
		return "", "", false
	}
	if len(call.Args) == 0 {
		return "", "", false
	}
	arg := call.Args[0]
	if name == "append" || name == "copy" {
		arg = call.Args[len(call.Args)-1]
	}
	t := a.info.TypeOf(arg)
	d := classify(t)
	if d != "DATA" && d != "AMBIG" {
		return "", "", false
	}
	return name + ":" + d, shortOrQ(t), true
}
