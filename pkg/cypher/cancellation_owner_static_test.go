package cypher

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// The statement's deadline has one owner, the statement context's probe
// (contextPipelineProbe; a goroutine forks it with pipelineWorkerContext, and
// statementCancellationErr makes the deadline win at commit and at Execute's
// return). This test enumerates, over every non-test file of the package,
// every way an evaluator or stage can lose that deadline, so a new entry
// cannot drop it unnoticed:
//
//	A. context.Background() / context.TODO(): a fresh context has no deadline.
//	B. a call to a no-probe wrapper (X forwarding to XWithCancellation or
//	   XProbed with a nil probe) from a function that has a context or probe.
//	C. a literal nil probe passed to a *WithCancellation / *Probed function
//	   from a function that has a context or probe.
//	D. a compiled node's eval that receives the probe and never reads it.
//	E. a goroutine that uses the statement context without its own fork
//	   (pipelineWorkerContext).
//
// Each finding is converted or listed in cancellationOwnerAllowlist with a
// one-line reason; a stale allowlist entry fails as well.

type cancellationFinding struct {
	rule, key, where string
	rewrite          string
}

type cancellationSource struct {
	name string
	src  []byte
	file *ast.File
}

type cancellationWrapper struct {
	params   []string // the wrapper's parameter names, in order
	variadic bool     // its last parameter is variadic
	target   string   // the probe form it forwards to
	args     []ast.Expr
	method   bool
	context  bool // forwards context.Background() rather than a nil probe
}

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

// localProbe is a probe variable declared in fn before pos from one of the
// owner's accessors.
func localProbe(fn *ast.FuncDecl, pos token.Pos) string {
	found := ""
	var blocks []*ast.BlockStmt
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if block, ok := node.(*ast.BlockStmt); ok {
			blocks = append(blocks, block)
		}
		return true
	})
	// innermost is the smallest block holding node: a variable declared in
	// it is in scope at pos only if that block holds pos too.
	innermost := func(node ast.Node) *ast.BlockStmt {
		var best *ast.BlockStmt
		for _, block := range blocks {
			if block.Pos() <= node.Pos() && node.End() <= block.End() && (best == nil || block.End()-block.Pos() < best.End()-best.Pos()) {
				best = block
			}
		}
		return best
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Pos() >= pos || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		if block := innermost(assign); block == nil || pos < block.Pos() || pos >= block.End() {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		switch callName(call) {
		case "pipelineCancellationForContext", "contextPipelineProbe", "parameterCancellationForContext", "rowPipelineCancellation":
			if id, ok := assign.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
				found = id.Name
			}
		}
		return true
	})
	return found
}

func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv != nil && len(fn.Recv.List) == 1 {
		typ := fn.Recv.List[0].Type
		if star, ok := typ.(*ast.StarExpr); ok {
			typ = star.X
		}
		if index, ok := typ.(*ast.IndexExpr); ok {
			typ = index.X
		}
		if index, ok := typ.(*ast.IndexListExpr); ok {
			typ = index.X
		}
		if id, ok := typ.(*ast.Ident); ok {
			return id.Name + "." + fn.Name.Name
		}
	}
	return fn.Name.Name
}

func callName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	case *ast.IndexExpr:
		switch x := fun.X.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			return x.Sel.Name
		}
	}
	return ""
}

// callIsLocal: a call of a package-level function or of a method (not a
// qualified call into another package).
func callIsLocal(call *ast.CallExpr, imports map[string]bool) bool {
	fun := call.Fun
	if index, ok := fun.(*ast.IndexExpr); ok {
		fun = index.X
	}
	if sel, ok := fun.(*ast.SelectorExpr); ok {
		if pkg, ok := sel.X.(*ast.Ident); ok && imports[pkg.Name] && pkg.Obj == nil {
			return false
		}
	}
	return true
}

func probeForm(name string) bool {
	return strings.HasSuffix(name, "WithCancellation") || strings.HasSuffix(name, "Probed")
}

// backgroundArg is a context.Background() or context.TODO() argument.
func backgroundArg(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "context" && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO")
}

func nilArg(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == "nil"
}

func hasNilArg(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		if nilArg(arg) {
			return true
		}
	}
	return false
}

func fileImports(file *ast.File) map[string]bool {
	out := map[string]bool{}
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		out[name] = true
	}
	return out
}

func loadCancellationSources(t *testing.T) (*token.FileSet, []cancellationSource) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []cancellationSource
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, cancellationSource{name, src, file})
	}
	return fset, files
}

// wrapperTemplate is how a wrapper forwards: its parameters, in order, plus a
// nil probe at one position. Wrappers that forward anything else are not
// rewritten automatically.
func rewriteCall(fset *token.FileSet, src []byte, call *ast.CallExpr, wrapper cancellationWrapper, probe, ctxName string) (string, bool) {
	text := func(node ast.Node) string {
		return string(src[fset.Position(node.Pos()).Offset:fset.Position(node.End()).Offset])
	}
	var args []string
	for index, arg := range wrapper.args {
		switch {
		case wrapper.context && backgroundArg(arg):
			if ctxName == "" {
				return "", false
			}
			args = append(args, ctxName)
		case !wrapper.context && nilArg(arg):
			args = append(args, probe)
		default:
			id, isIdent := arg.(*ast.Ident)
			last := index == len(wrapper.args)-1
			if !isIdent {
				return "", false
			}
			position := -1
			for p, name := range wrapper.params {
				if name == id.Name {
					position = p
				}
			}
			if position < 0 {
				return "", false
			}
			if wrapper.variadic && position == len(wrapper.params)-1 {
				// The variadic parameter: forwards the caller's remaining arguments.
				if !last {
					return "", false
				}
				rest := call.Args[min(position, len(call.Args)):]
				for _, r := range rest {
					args = append(args, text(r))
				}
				if call.Ellipsis.IsValid() && len(args) > 0 {
					args[len(args)-1] += "..."
				}
				continue
			}
			if position >= len(call.Args) {
				return "", false
			}
			args = append(args, text(call.Args[position]))
		}
	}
	fun := text(call.Fun)
	name := callName(call)
	target := wrapper.target
	index := strings.LastIndex(fun, name)
	if index < 0 {
		return "", false
	}
	fun = fun[:index] + target + fun[index+len(name):]
	return fun + "(" + strings.Join(args, ", ") + ")", true
}

func collectCancellationFindings(t *testing.T) ([]cancellationFinding, map[string]int) {
	t.Helper()
	fset, files := loadCancellationSources(t)
	// Pass 1: the wrappers, and how many methods share each method name.
	wrappers := map[string]cancellationWrapper{} // function name, or "." + method name
	methodWrappers := map[string]cancellationWrapper{} // "Type.method"
	probeIndex := map[string]int{}            // probe form name -> index of its probe parameter
	methodNames := map[string]int{}
	for _, f := range files {
		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Recv != nil {
				methodNames[fn.Name.Name]++
			}
			if probeForm(fn.Name.Name) && fn.Type.Params != nil {
				index := 0
				for _, field := range fn.Type.Params.List {
					names := len(field.Names)
					if names == 0 {
						names = 1
					}
					if star, ok := field.Type.(*ast.StarExpr); ok {
						if id, ok := star.X.(*ast.Ident); ok && id.Name == "pipelineCancellation" {
							probeIndex[fn.Name.Name] = index
						}
					}
					index += names
				}
			}
			if fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := callName(call)
				background := false
				if len(fn.Body.List) == 1 && name != "" && name != fn.Name.Name {
					for _, arg := range call.Args {
						background = background || backgroundArg(arg)
					}
				}
				if (name == fn.Name.Name+"WithCancellation" || name == fn.Name.Name+"Probed") && hasNilArg(call) || background && node == cancellationForwardingCall(fn) {
					wrapper := cancellationWrapper{target: name, args: call.Args, method: fn.Recv != nil, context: background}
					for _, field := range fn.Type.Params.List {
						if _, ok := field.Type.(*ast.Ellipsis); ok {
							wrapper.variadic = true
						}
						for _, n := range field.Names {
							wrapper.params = append(wrapper.params, n.Name)
						}
					}
					key := fn.Name.Name
					if fn.Recv != nil {
						key = "." + key
					}
					wrappers[key] = wrapper
					if fn.Recv != nil {
						methodWrappers[funcKey(fn)] = wrapper
					}
				}
				return true
			})
		}
	}
	counts := map[string]int{"wrappers": len(wrappers)}
	var findings []cancellationFinding
	for _, f := range files {
		imports := fileImports(f.file)
		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := funcKey(fn)
			ctxName, probeName := cancellationScope(fn)
			scoped := ctxName != "" || probeName != ""
			varTypes := cancellationVarTypes(fn)
			selfKey := fn.Name.Name
			if fn.Recv != nil {
				selfKey = "." + selfKey
			}
			// Outermost rewritable calls first; nested ones are found on the next run.
			var emitted []ast.Node
			inside := func(node ast.Node) bool {
				for _, outer := range emitted {
					if node.Pos() >= outer.Pos() && node.End() <= outer.End() {
						return true
					}
				}
				return false
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if g, ok := node.(*ast.GoStmt); ok {
					lit, ok := g.Call.Fun.(*ast.FuncLit)
					usesCtx, forks := false, false
					var body ast.Node = g.Call
					if ok {
						body = lit.Body
					}
					ast.Inspect(body, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok && ctxName != "" && id.Name == ctxName {
							usesCtx = true
						}
						if call, ok := n.(*ast.CallExpr); ok && callName(call) == "pipelineWorkerContext" {
							forks = true
						}
						return true
					})
					if usesCtx && !forks {
						counts["E"]++
						findings = append(findings, cancellationFinding{rule: "E", key: key + " go", where: fmt.Sprintf("%s:%d", f.name, fset.Position(g.Pos()).Line)})
					}
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := callName(call)
				position := fset.Position(call.Pos())
				where := fmt.Sprintf("%s:%d", f.name, position.Line)
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO") {
						counts["A"]++
						label := key + " context." + sel.Sel.Name
						if self, ok := wrappers[selfKey]; ok && self.context {
							label += " (context-free wrapper of " + self.target + ")"
						}
						findings = append(findings, cancellationFinding{rule: "A", key: label, where: where})
					}
				}
				if !scoped || !callIsLocal(call, imports) {
					return true
				}
				if index, known := probeIndex[name]; known && index < len(call.Args) && nilArg(call.Args[index]) && !(name == fn.Name.Name+"WithCancellation" || name == fn.Name.Name+"Probed") {
					counts["C"]++
					findings = append(findings, cancellationFinding{rule: "C", key: key + " -> " + name + "(…, nil)", where: where})
				}
				wrapperKey := name
				if _, isSel := call.Fun.(*ast.SelectorExpr); isSel {
					wrapperKey = "." + name
				}
				wrapper, isWrapper := wrappers[wrapperKey]
				resolved := false
				if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && isWrapper {
					if recv, ok := sel.X.(*ast.Ident); ok && varTypes[recv.Name] != "" {
						resolved = true
						wrapper, isWrapper = methodWrappers[varTypes[recv.Name]+"."+name]
					}
				}
				if !isWrapper || wrapperKey == selfKey && !resolved || resolved && varTypes[call.Fun.(*ast.SelectorExpr).X.(*ast.Ident).Name]+"."+name == key {
					return true
				}
				counts["B"]++
				finding := cancellationFinding{rule: "B", key: key + " -> " + name, where: where}
				if wrapper.method && methodNames[name] > 1 && !resolved {
					finding.key += " (ambiguous method)"
				} else if !inside(call) {
					probe := probeName
					if local := localProbe(fn, call.Pos()); local != "" {
						probe = local
					}
					if probe == "" {
						probe = "contextPipelineProbe(" + ctxName + ")"
					}
					if text, ok := rewriteCall(fset, f.src, call, wrapper, probe, ctxName); ok {
						end := fset.Position(call.End())
						finding.rewrite = fmt.Sprintf("REWRITE %s %d %d %d %d %s", f.name, position.Line, position.Column, end.Line, end.Column, base64.StdEncoding.EncodeToString([]byte(text)))
						emitted = append(emitted, call)
					}
				}
				findings = append(findings, finding)
				return true
			})
			// D: a compiled node eval that takes the probe and never reads it.
			if fn.Name.Name == "eval" && fn.Recv != nil && fn.Type.Params != nil {
				for _, field := range fn.Type.Params.List {
					star, ok := field.Type.(*ast.StarExpr)
					if !ok {
						continue
					}
					if id, ok := star.X.(*ast.Ident); !ok || id.Name != "pipelineCancellation" {
						continue
					}
					used := false
					for _, param := range field.Names {
						ast.Inspect(fn.Body, func(node ast.Node) bool {
							if id, ok := node.(*ast.Ident); ok && id.Name == param.Name {
								used = true
							}
							return !used
						})
					}
					if !used {
						counts["D"]++
						findings = append(findings, cancellationFinding{rule: "D", key: key, where: f.name})
					}
				}
			}
		}
	}
	return findings, counts
}

func TestCancellationOwnerStaticInventory(t *testing.T) {
	findings, counts := collectCancellationFindings(t)
	t.Logf("no-probe wrappers %d; findings A=%d B=%d C=%d D=%d E=%d",
		counts["wrappers"], counts["A"], counts["B"], counts["C"], counts["D"], counts["E"])
	seen := map[string]bool{}
	var open []string
	var rewrites bytes.Buffer
	for _, finding := range findings {
		id := finding.rule + " " + finding.key
		seen[id] = true
		if _, ok := cancellationOwnerAllowlist[id]; ok {
			continue
		}
		open = append(open, id+"  @ "+finding.where)
		if finding.rewrite != "" {
			rewrites.WriteString(finding.rewrite + "\n")
		}
	}
	var stale []string
	for id := range cancellationOwnerAllowlist {
		if !seen[id] {
			stale = append(stale, id)
		}
	}
	sort.Strings(open)
	sort.Strings(stale)
	if os.Getenv("CANCELLATION_INVENTORY") != "" {
		for _, line := range open {
			t.Logf("OPEN %s", line)
		}
		for _, line := range strings.Split(strings.TrimSpace(rewrites.String()), "\n") {
			if line != "" {
				t.Logf("%s", line)
			}
		}
	}
	if len(open) > 0 || len(stale) > 0 {
		t.Fatalf("%d findings drop the statement deadline (convert them or allowlist with a reason; CANCELLATION_INVENTORY=1 lists them); %d stale allowlist entries: %v", len(open), len(stale), stale)
	}
}

// cancellationVarTypes names the package type of the variables in fn whose
// type is plain to read: its receiver, its parameters, `var x T` and
// `x := T{…}` / `x := &T{…}`. A method call on one of them resolves to that
// type's method; a call on anything else stays ambiguous.
func cancellationVarTypes(fn *ast.FuncDecl) map[string]string {
	types := map[string]string{}
	typeName := func(expr ast.Expr) string {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		if index, ok := expr.(*ast.IndexExpr); ok {
			expr = index.X
		}
		if id, ok := expr.(*ast.Ident); ok {
			return id.Name
		}
		return ""
	}
	fields := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				if typ := typeName(field.Type); typ != "" {
					types[name.Name] = typ
				}
			}
		}
	}
	fields(fn.Recv)
	fields(fn.Type.Params)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.ValueSpec:
			if typ := typeName(node.Type); typ != "" {
				for _, name := range node.Names {
					types[name.Name] = typ
				}
			}
		case *ast.AssignStmt:
			if len(node.Lhs) == 1 && len(node.Rhs) == 1 {
				rhs := node.Rhs[0]
				if unary, ok := rhs.(*ast.UnaryExpr); ok && unary.Op == token.AND {
					rhs = unary.X
				}
				if lit, ok := rhs.(*ast.CompositeLit); ok {
					if id, ok := node.Lhs[0].(*ast.Ident); ok {
						if typ := typeName(lit.Type); typ != "" {
							types[id.Name] = typ
						}
					}
				}
			}
		}
		return true
	})
	return types
}

// cancellationForwardingCall is the call a one-statement function forwards
// to (`return f(…)` or `f(…)`), or nil.
func cancellationForwardingCall(fn *ast.FuncDecl) ast.Node {
	if fn.Body == nil || len(fn.Body.List) != 1 {
		return nil
	}
	switch stmt := fn.Body.List[0].(type) {
	case *ast.ReturnStmt:
		if len(stmt.Results) == 1 {
			if call, ok := stmt.Results[0].(*ast.CallExpr); ok {
				return call
			}
		}
	case *ast.ExprStmt:
		if call, ok := stmt.X.(*ast.CallExpr); ok {
			return call
		}
	}
	return nil
}
