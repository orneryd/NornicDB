package cypher

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
	"testing"
)

// The statement's end (its deadline, a client cancel, TERMINATE) has one
// owner: the statement probe (contextPipelineProbe; carried in the
// registered statement context; a goroutine forks it with
// pipelineWorkerContext; statementDeadlineOutcome settles the result).
//
// The contract this test enforces, over a type-checked inventory of the
// package: every loop a statement can reach whose trip count grows with
// data (rows, result rows, values, lists, maps, nodes, relationships,
// vectors, iterators) observes the probe on every iteration, and so does
// every bulk native operation over data. Work bounded by the statement's own
// text (its characters, clauses, a row's variables) takes no probe: that
// work goes to the native, cached readers, and preparation checks the probe
// between its steps.
//
// Reachable: from StorageExecutor.Execute, from every function or method of
// the package another package of the server uses (Bolt, HTTP, transactions,
// the database, MCP, the command), from init and from package-level tables
// (procedures and functions register there), with a call through an
// interface reaching every method of that name.
//
// Rules (a finding is fixed or listed in cancellationContractAllowlist with a
// one-line reason; a stale entry fails too):
//
//	R1 a reachable data loop or bulk data operation that doesn't observe
//	   the probe.
//	R2 a probe-taking twin (XWithCancellation / XProbed beside X) that does
//	   no data work: statement-text work takes no probe.
//	R3 context.Background() / context.TODO() in a reachable function: a fresh
//	   context has no deadline.
//	R4 a reachable function that holds a context or a probe calling a form
//	   that drops it (a wrapper that forwards a nil probe or a background
//	   context).
//	R5 a goroutine started by a reachable function without forking the probe
//	   (pipelineWorkerContext) and recovering into it (recoverWorkerPanic).
func TestCancellationOwnerStaticInventory(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the package and its clients from source")
	}
	findings := collectCancellationContractFindings(t)
	seen := map[string]bool{}
	var open []string
	counts := map[string]int{}
	for _, finding := range findings {
		counts[finding.rule]++
		seen[finding.key] = true
		if _, ok := cancellationContractAllowlist[finding.key]; !ok {
			open = append(open, finding.key+"  @ "+finding.where)
		}
	}
	var stale []string
	for key := range cancellationContractAllowlist {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(open)
	sort.Strings(stale)
	t.Logf("findings by rule %v; allowlisted %d", counts, len(cancellationContractAllowlist))
	if os.Getenv("CANCELLATION_INVENTORY") != "" {
		for _, line := range open {
			t.Logf("OPEN %s", line)
		}
	}
	if len(open) > 0 || len(stale) > 0 {
		t.Fatalf("%d findings (CANCELLATION_INVENTORY=1 lists them); %d stale allowlist entries: %v", len(open), len(stale), stale)
	}
}

type contractFinding struct{ rule, key, where string }

// The packages whose use of this one makes a function reachable.
var cancellationContractClients = []string{"../bolt", "../server", "../txsession", "../nornicdb", "../fabric", "../mcp", "../../cmd/nornicdb"}

type contractFunc struct {
	decl       *ast.FuncDecl
	obj        *types.Func
	file       string
	direct     bool
	checks     bool
	data       bool // has a data loop or a bulk data operation
	refs       []*types.Func
	calls      []*types.Func
	ifaceCalls []string
	reach      bool
}

type contractAnalysis struct {
	fset *token.FileSet
	info *types.Info
	fns  map[*types.Func]*contractFunc
}

func collectCancellationContractFindings(t *testing.T) []contractFinding {
	ctxt := build.Default
	ctxt.BuildTags = []string{"noui", "nolocalllm"}
	build.Default = ctxt
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "source", nil)
	const module = "github.com/orneryd/nornicdb/pkg/cypher"

	clientUses := map[string]bool{}
	for _, dir := range cancellationContractClients {
		files := parsePackageFiles(t, fset, dir, false)
		if files == nil {
			continue
		}
		info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
		conf := types.Config{Importer: imp, Error: func(error) {}}
		_, _ = conf.Check("client/"+filepath.Base(dir), fset, files, info)
		for _, obj := range info.Uses {
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == module {
				clientUses[fn.Origin().FullName()] = true
			}
		}
	}
	if len(clientUses) == 0 {
		t.Fatal("no client package uses this package: the clients did not type-check")
	}

	files := parsePackageFiles(t, fset, ".", true)
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var typeErrors []error
	conf := types.Config{Importer: imp, Error: func(err error) { typeErrors = append(typeErrors, err) }}
	_, _ = conf.Check(module, fset, files, info)
	if len(typeErrors) > 0 {
		t.Fatalf("the package does not type-check: %v", typeErrors[0])
	}
	a := &contractAnalysis{fset: fset, info: info, fns: map[*types.Func]*contractFunc{}}
	var initRefs []*types.Func
	for _, f := range files {
		name := fset.Position(f.Pos()).Filename
		for _, d := range f.Decls {
			switch decl := d.(type) {
			case *ast.FuncDecl:
				if decl.Body == nil {
					continue
				}
				obj, _ := info.Defs[decl.Name].(*types.Func)
				if obj == nil {
					continue
				}
				fn := &contractFunc{decl: decl, obj: obj, file: name}
				a.fns[obj] = fn
				ast.Inspect(decl.Body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.CallExpr:
						if a.isDirectCheck(x) {
							fn.direct = true
						}
						if callee := a.callee(x); callee != nil {
							fn.calls = append(fn.calls, callee)
						}
					case *ast.SelectStmt:
						if selectsDone(x) {
							fn.direct = true
						}
					case *ast.Ident:
						if ref, ok := info.Uses[x].(*types.Func); ok {
							fn.refs = append(fn.refs, ref.Origin())
						}
					case *ast.SelectorExpr:
						if s := info.Selections[x]; s != nil && types.IsInterface(s.Recv()) {
							fn.ifaceCalls = append(fn.ifaceCalls, x.Sel.Name)
						}
					}
					return true
				})
			case *ast.GenDecl:
				if decl.Tok == token.VAR {
					ast.Inspect(decl, func(n ast.Node) bool {
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
	}
	methodsByName := map[string][]*contractFunc{}
	for _, fn := range a.fns {
		if fn.decl.Recv != nil {
			methodsByName[fn.decl.Name.Name] = append(methodsByName[fn.decl.Name.Name], fn)
		}
	}
	for _, fn := range a.fns {
		for _, name := range fn.ifaceCalls {
			for _, m := range methodsByName[name] {
				fn.refs = append(fn.refs, m.obj)
			}
		}
	}
	// checks: observes the probe itself or through a function it calls.
	for _, fn := range a.fns {
		fn.checks = fn.direct
	}
	fixedPoint(a.fns, func(fn *contractFunc) bool {
		for _, c := range fn.calls {
			if g := a.fns[c]; g != nil && g.checks {
				fn.checks = true
				return true
			}
		}
		return false
	}, func(fn *contractFunc) bool { return fn.checks })
	// Reachability.
	var stack []*contractFunc
	for _, fn := range a.fns {
		if contractFuncName(fn.decl) == "StorageExecutor.Execute" || clientUses[fn.obj.FullName()] || (fn.decl.Recv == nil && fn.decl.Name.Name == "init") {
			stack = append(stack, fn)
		}
	}
	for _, ref := range initRefs {
		if fn := a.fns[ref]; fn != nil {
			stack = append(stack, fn)
		}
	}
	for len(stack) > 0 {
		fn := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if fn.reach {
			continue
		}
		fn.reach = true
		for _, ref := range fn.refs {
			if g := a.fns[ref]; g != nil && !g.reach {
				stack = append(stack, g)
			}
		}
	}

	var findings []contractFinding
	forwarders := a.forwarders()
	for _, fn := range a.fns {
		name := contractFuncName(fn.decl)
		// R1: loops and bulk operations.
		ordinal := 0
		a.walkLoops(fn.decl.Body, func(n ast.Node, body *ast.BlockStmt, domain, typ string) {
			ordinal++
			if domain != "DATA" {
				return
			}
			fn.data = true
			if !fn.reach {
				return
			}
			if body != nil && a.bodyObserves(body) {
				return
			}
			kind := "loop"
			if body == nil {
				kind = "bulk"
			}
			findings = append(findings, contractFinding{"R1", fmt.Sprintf("R1 %s %s#%d %s", name, kind, ordinal, typ), a.where(n.Pos())})
		})
		if !fn.reach {
			continue
		}
		ctxName, probeName := cancellationScope(fn.decl)
		holdsOwner := ctxName != "" || probeName != "" || fn.decl.Body != nil && localProbe(fn.decl, fn.decl.Body.End()) != ""
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				// R3.
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO") {
						findings = append(findings, contractFinding{"R3", fmt.Sprintf("R3 %s %s", name, sel.Sel.Name), a.where(x.Pos())})
					}
				}
				// R4.
				if holdsOwner {
					if callee := a.callee(x); callee != nil && forwarders[callee] != "" {
						findings = append(findings, contractFinding{"R4", fmt.Sprintf("R4 %s -> %s", name, contractFuncName(a.fns[callee].decl)), a.where(x.Pos())})
					}
				}
			case *ast.GoStmt:
				// R5.
				forks, recovers := false, false
				ast.Inspect(fn.decl.Body, func(m ast.Node) bool {
					if call, ok := m.(*ast.CallExpr); ok {
						switch callName(call) {
						case "pipelineWorkerContext":
							forks = true
						case "recoverWorkerPanic":
							recovers = true
						}
					}
					return true
				})
				if !forks || !recovers {
					findings = append(findings, contractFinding{"R5", fmt.Sprintf("R5 %s go", name), a.where(x.Pos())})
				}
			}
			return true
		})
	}
	// data: the function, or one it calls, does data work.
	fixedPoint(a.fns, func(fn *contractFunc) bool {
		for _, c := range fn.calls {
			if g := a.fns[c]; g != nil && g.data {
				fn.data = true
				return true
			}
		}
		return false
	}, func(fn *contractFunc) bool { return fn.data })
	// R2: statement-text twins.
	byName := map[string]*contractFunc{}
	for _, fn := range a.fns {
		byName[contractFuncName(fn.decl)] = fn
	}
	for key, fn := range byName {
		for _, suffix := range []string{"WithCancellation", "Probed"} {
			if strings.HasSuffix(key, suffix) && byName[strings.TrimSuffix(key, suffix)] != nil && !fn.data {
				findings = append(findings, contractFinding{"R2", "R2 " + key, a.where(fn.decl.Pos())})
			}
		}
	}
	return findings
}

func fixedPoint(fns map[*types.Func]*contractFunc, step func(*contractFunc) bool, done func(*contractFunc) bool) {
	for changed := true; changed; {
		changed = false
		for _, fn := range fns {
			if !done(fn) && step(fn) {
				changed = true
			}
		}
	}
}

func parsePackageFiles(t *testing.T, fset *token.FileSet, dir string, required bool) []*ast.File {
	pkg, err := build.ImportDir(dir, 0)
	if err != nil {
		if required {
			t.Fatal(err)
		}
		return nil
	}
	var files []*ast.File
	for _, name := range pkg.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

func contractFuncName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		t := fd.Recv.List[0].Type
		if s, ok := t.(*ast.StarExpr); ok {
			t = s.X
		}
		if ix, ok := t.(*ast.IndexExpr); ok {
			t = ix.X
		}
		if ix, ok := t.(*ast.IndexListExpr); ok {
			t = ix.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

func callName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.IndexExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""
}

func (a *contractAnalysis) where(p token.Pos) string {
	position := a.fset.Position(p)
	return fmt.Sprintf("%s:%d", filepath.Base(position.Filename), position.Line)
}

func (a *contractAnalysis) callee(call *ast.CallExpr) *types.Func {
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

func isProbeType(t types.Type) bool {
	if t == nil {
		return false
	}
	s := t.String()
	return strings.HasSuffix(s, ".pipelineCancellation") || strings.HasSuffix(s, "fn.Cancellation") || strings.HasSuffix(s, "text.Work")
}

func isContextType(t types.Type) bool {
	return t != nil && t.String() == "context.Context"
}

func (a *contractAnalysis) isDirectCheck(call *ast.CallExpr) bool {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return false
	}
	recv := a.info.TypeOf(sel.X)
	switch sel.Sel.Name {
	case "check", "poll", "stopped", "chargeText", "Continue", "sort":
		return isProbeType(recv)
	case "Err":
		return isContextType(recv)
	case "traversalCancelled":
		return true
	}
	return false
}

func selectsDone(sel *ast.SelectStmt) bool {
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

// bodyObserves: the body checks the probe, passes it on, or calls a
// function that checks it.
func (a *contractAnalysis) bodyObserves(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		switch x := n.(type) {
		case *ast.CallExpr:
			if a.isDirectCheck(x) {
				found = true
			}
			for _, arg := range x.Args {
				if id, ok := ast.Unparen(arg).(*ast.Ident); ok && id.Name == "nil" {
					continue
				}
				if t := a.info.TypeOf(arg); isProbeType(t) || isContextType(t) {
					found = true
				}
			}
			if c := a.callee(x); c != nil {
				if g := a.fns[c]; g != nil && g.checks {
					found = true
				}
			}
		case *ast.SelectStmt:
			if selectsDone(x) {
				found = true
			}
		}
		return true
	})
	return found
}

// forwarders are the functions whose whole body hands a nil probe or a
// background context to another function: the forms that drop the owner.
func (a *contractAnalysis) forwarders() map[*types.Func]string {
	out := map[*types.Func]string{}
	for obj, fn := range a.fns {
		if len(fn.decl.Body.List) != 1 {
			continue
		}
		var call *ast.CallExpr
		switch s := fn.decl.Body.List[0].(type) {
		case *ast.ReturnStmt:
			if len(s.Results) == 1 {
				call, _ = s.Results[0].(*ast.CallExpr)
			}
		case *ast.ExprStmt:
			call, _ = s.X.(*ast.CallExpr)
		}
		if call == nil {
			continue
		}
		sig, _ := a.info.TypeOf(call.Fun).(*types.Signature)
		for i, arg := range call.Args {
			if sig == nil || i >= sig.Params().Len() {
				break
			}
			param := sig.Params().At(i).Type()
			if id, ok := ast.Unparen(arg).(*ast.Ident); ok && id.Name == "nil" && isProbeType(param) {
				out[obj] = "nil probe"
			}
			if c, ok := ast.Unparen(arg).(*ast.CallExpr); ok && isContextType(param) {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO") {
					out[obj] = "background context"
				}
			}
		}
	}
	return out
}

// walkLoops reports every loop (with its body) and every bulk native
// operation (body nil) of n, with the domain of what it iterates.
func (a *contractAnalysis) walkLoops(n ast.Node, visit func(n ast.Node, body *ast.BlockStmt, domain, typ string)) {
	ast.Inspect(n, func(m ast.Node) bool {
		switch s := m.(type) {
		case *ast.RangeStmt:
			domain, typ := a.rangeDomain(s.X)
			visit(s, s.Body, domain, typ)
		case *ast.ForStmt:
			domain, typ := a.condDomain(s)
			visit(s, s.Body, domain, typ)
		case *ast.CallExpr:
			if kind, typ, ok := a.bulk(s); ok {
				visit(s, nil, "DATA", kind+" "+typ)
			}
		}
		return true
	})
}

func contractShort(t types.Type) string {
	if t == nil {
		return "?"
	}
	return strings.ReplaceAll(t.String(), "github.com/orneryd/nornicdb/pkg/", "")
}

// contractClassify is the domain of what a value of type t holds:
// STMT (the statement's own text and structures, a row's variables), DATA
// (rows, values, lists, maps, graph entities, iterators) or OTHER.
func contractClassify(t types.Type) string {
	if t == nil {
		return "OTHER"
	}
	switch u := t.Underlying().(type) {
	case *types.Basic:
		if u.Info()&types.IsString != 0 {
			return "STMT"
		}
		return "OTHER"
	case *types.Chan, *types.Signature:
		return "DATA"
	}
	s := t.String()
	switch {
	case strings.HasSuffix(s, ".pipelineRow"), strings.HasPrefix(s, "map[string]*github.com/orneryd/nornicdb/pkg/storage."):
		return "STMT" // a row's (or a binding scope's) variables
	case strings.Contains(s, "pipelineRow"), strings.Contains(s, "storage.Node"), strings.Contains(s, "storage.Edge"),
		strings.Contains(s, "storage.NodeID"), strings.Contains(s, "storage.EdgeID"), strings.Contains(s, "PathResult"),
		s == "[]interface{}", s == "[]any", s == "[][]interface{}", s == "[][]any", s == "map[string]interface{}", s == "map[string]any",
		strings.HasPrefix(s, "[]map[string]"), strings.HasPrefix(s, "map[interface{}]"), strings.HasPrefix(s, "[]float"),
		strings.HasPrefix(s, "[]int64"), strings.HasPrefix(s, "[]time."):
		return "DATA"
	}
	return "OTHER"
}

func (a *contractAnalysis) rangeDomain(x ast.Expr) (string, string) {
	t := a.info.TypeOf(x)
	if t == nil {
		return "OTHER", "?"
	}
	if b, ok := t.Underlying().(*types.Basic); ok && b.Info()&types.IsInteger != 0 {
		if call, ok := ast.Unparen(x).(*ast.CallExpr); ok && len(call.Args) == 1 && callName(call) == "len" {
			at := a.info.TypeOf(call.Args[0])
			return contractClassify(at), "len(" + contractShort(at) + ")"
		}
		return "OTHER", contractShort(t)
	}
	return contractClassify(t), contractShort(t)
}

func (a *contractAnalysis) condDomain(s *ast.ForStmt) (string, string) {
	if s.Cond == nil {
		return "OTHER", "for{}"
	}
	domain, typ := "OTHER", "cond"
	ast.Inspect(s.Cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && len(call.Args) == 1 && (callName(call) == "len" || callName(call) == "cap") {
			t := a.info.TypeOf(call.Args[0])
			domain, typ = contractClassify(t), "len("+contractShort(t)+")"
			return false
		}
		return true
	})
	return domain, typ
}

// bulk: a native operation whose cost is the size of a data value.
func (a *contractAnalysis) bulk(call *ast.CallExpr) (string, string, bool) {
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
		"copy", "slices.Clone", "maps.Copy", "maps.Clone", "strings.Join", "reflect.DeepEqual":
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
	if contractClassify(t) != "DATA" {
		return "", "", false
	}
	return name, contractShort(t), true
}

// cancellationScope names fn's context and probe parameters.
func cancellationScope(fn *ast.FuncDecl) (ctxName, probeName string) {
	if fn.Type.Params == nil {
		return "", ""
	}
	for _, field := range fn.Type.Params.List {
		for _, name := range field.Names {
			switch typ := field.Type.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := typ.X.(*ast.Ident); ok && pkg.Name == "context" && typ.Sel.Name == "Context" && ctxName == "" && name.Name != "_" {
					ctxName = name.Name
				}
			case *ast.StarExpr:
				if id, ok := typ.X.(*ast.Ident); ok && id.Name == "pipelineCancellation" && probeName == "" && name.Name != "_" {
					probeName = name.Name
				}
			}
		}
	}
	return ctxName, probeName
}

// localProbe is a probe variable fn declares from one of the owner's
// accessors before pos.
func localProbe(fn *ast.FuncDecl, pos token.Pos) string {
	found := ""
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Pos() >= pos || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
			switch callName(call) {
			case "pipelineCancellationForContext", "contextPipelineProbe", "parameterCancellationForContext", "rowPipelineCancellation":
				if id, ok := assign.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
					found = id.Name
				}
			}
		}
		return true
	})
	return found
}
