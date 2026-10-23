package cypher

import (
	"go/ast"
	"go/types"
)

// Value provenance: which values may hold the query's data. A value parsed
// from the statement's text (a literal, a pattern's properties, a parsed
// argument list) is as large as the text the client wrote, which the request
// limits bound: a pass over it is part of reading the statement, like a pass
// over the text itself. Data comes from outside the statement's text: storage
// reads, parameters, rows, evaluated expressions, results of other packages.
//
// The analysis is a least fixed point over the package's dataflow:
//   - a variable holds data when an assignment to it, or a write into it (a
//     field, an element, a channel send, a callee writing into a parameter it
//     was handed), is data;
//   - a parameter holds data when a call hands it data, and always when the
//     package cannot see all of its calls: an exported function, a method an
//     interface may dispatch to, a function used as a value or never called,
//     a function literal's parameter;
//   - a package function's result holds data when one of its returns does;
//   - a call into another package (other than the statement's parsers), a
//     call through a function value or an interface returns data, and a data
//     argument writes data into the receiver and every reference argument;
//   - a conversion, field, index, slice, dereference or assertion holds what
//     its operand holds, a composite literal what its elements hold, append
//     what its arguments hold; a constant, nil or function literal holds none.
//
// Strings are not tracked here: data strings are the string taint's
// (dataExpr), which reads provenance for the Cypher values it starts from.
// The analysis only removes findings: every rule that reads it also required
// the value's type before.

// boundaryStatementPackages parse the statement's text: their results are
// the statement, not data.
var boundaryStatementPackages = map[string]bool{
	cypherPackagePath + "/antlr":    true,
	"github.com/antlr4-go/antlr/v4": true,
}

type valueProvenance struct {
	info *types.Info
	// path is the package's import path.
	path string
	// decls are the package's functions (production and fixtures).
	decls map[*types.Func]*ast.FuncDecl
	// data are the variables that may hold data.
	data map[types.Object]bool
	// results are the functions whose results may hold data.
	results map[*types.Func]bool
	// sinks are, per function, the parameters it writes data into (-1 is
	// the receiver).
	sinks map[*types.Func]map[int]bool
	flows []provenanceFlow
	// aliases are, per reference variable, the variables whose value it was
	// assigned a part of (x := y.f): a write through x writes into y too.
	aliases map[types.Object][]types.Object
	// taint is the string taint (dataExpr), consulted for strings.
	taint func(ast.Expr) bool
}

// provenanceFlow carries data from an expression (or a callee's write into a
// parameter) into a variable, a function's results, or a parameter sink.
type provenanceFlow struct {
	from ast.Expr
	// fromSink, when set, is a callee and parameter: data when the callee
	// writes data into that parameter.
	fromSink  *types.Func
	sinkIndex int

	variable types.Object
	result   *types.Func
	sink     *types.Func
	sinkAt   int
}

// tracked reports a type whose values the analysis follows: everything but
// scalars and strings.
func provenanceTracked(typ types.Type) bool {
	if typ == nil {
		return false
	}
	_, basic := typ.Underlying().(*types.Basic)
	return !basic
}

func newValueProvenance(info *types.Info, decls map[*types.Func]*ast.FuncDecl, files []*ast.File, taint func(ast.Expr) bool) *valueProvenance {
	p := &valueProvenance{info: info, decls: decls, data: map[types.Object]bool{}, results: map[*types.Func]bool{},
		sinks: map[*types.Func]map[int]bool{}, aliases: map[types.Object][]types.Object{}, taint: taint}
	for f := range decls {
		if f.Pkg() != nil {
			p.path = f.Pkg().Path()
			break
		}
	}
	open := p.openFunctions(files)
	for f, decl := range decls {
		params := p.params(decl)
		if !open[f] {
			continue
		}
		for _, param := range params {
			if param != nil {
				p.seed(param)
			}
		}
		if recv := p.receiver(decl); recv != nil {
			p.seed(recv)
		}
	}
	for _, file := range files {
		p.collect(file)
	}
	return p
}

func (p *valueProvenance) seed(obj types.Object) {
	if obj != nil && provenanceTracked(obj.Type()) {
		p.data[obj] = true
	}
}

// params are a declaration's parameter objects by position (nil when unnamed).
func (p *valueProvenance) params(decl *ast.FuncDecl) []types.Object {
	var out []types.Object
	for _, field := range decl.Type.Params.List {
		if len(field.Names) == 0 {
			out = append(out, nil)
			continue
		}
		for _, name := range field.Names {
			out = append(out, p.info.Defs[name])
		}
	}
	return out
}

func (p *valueProvenance) receiver(decl *ast.FuncDecl) types.Object {
	if decl.Recv == nil || len(decl.Recv.List) == 0 || len(decl.Recv.List[0].Names) == 0 {
		return nil
	}
	return p.info.Defs[decl.Recv.List[0].Names[0]]
}

// openFunctions are the functions whose calls the package cannot all see.
func (p *valueProvenance) openFunctions(files []*ast.File) map[*types.Func]bool {
	interfaceMethods := map[string]bool{}
	callFuns := map[*ast.Ident]bool{}
	calls := map[*types.Func]int{}
	callees := map[*types.Func][]*types.Func{}
	for _, file := range files {
		var caller *types.Func
		var inspect func(n ast.Node) bool
		inspect = func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncDecl:
				outer := caller
				caller, _ = p.info.Defs[x.Name].(*types.Func)
				if x.Body != nil {
					ast.Inspect(x.Body, inspect)
				}
				caller = outer
				return false
			case *ast.InterfaceType:
				for _, method := range x.Methods.List {
					for _, name := range method.Names {
						interfaceMethods[name.Name] = true
					}
				}
			case *ast.CallExpr:
				if ident := provenanceCallIdent(x); ident != nil {
					callFuns[ident] = true
					if f, ok := p.info.Uses[ident].(*types.Func); ok {
						calls[f.Origin()]++
						if caller != nil {
							callees[caller] = append(callees[caller], f.Origin())
						}
					}
				}
			}
			return true
		}
		ast.Inspect(file, inspect)
	}
	used := map[*types.Func]bool{}
	for ident, obj := range p.info.Uses {
		if f, ok := obj.(*types.Func); ok && !callFuns[ident] && p.decls[f.Origin()] != nil {
			used[f.Origin()] = true
		}
	}
	open := map[*types.Func]bool{}
	for f, decl := range p.decls {
		method := decl.Recv != nil
		switch {
		case decl.Name.IsExported(), decl.Name.Name == "init", used[f], calls[f] == 0,
			method && interfaceMethods[decl.Name.Name]:
			open[f] = true
		}
	}
	// A function no open function reaches (dead code, or a cycle only its
	// own members call) is called from outside the analysis: open too.
	reached := map[*types.Func]bool{}
	var queue []*types.Func
	for f := range open {
		reached[f] = true
		queue = append(queue, f)
	}
	for len(queue) > 0 {
		f := queue[0]
		queue = queue[1:]
		for _, g := range callees[f] {
			if !reached[g] {
				reached[g] = true
				queue = append(queue, g)
			}
		}
	}
	for f := range p.decls {
		if !reached[f] {
			open[f] = true
		}
	}
	return open
}

// provenanceCallIdent is the identifier a call names (f, x.f, f[T]).
func provenanceCallIdent(call *ast.CallExpr) *ast.Ident {
	fun := call.Fun
	if index, ok := fun.(*ast.IndexExpr); ok {
		fun = index.X
	}
	if index, ok := fun.(*ast.IndexListExpr); ok {
		fun = index.X
	}
	switch x := fun.(type) {
	case *ast.Ident:
		return x
	case *ast.SelectorExpr:
		return x.Sel
	}
	return nil
}

// root is the variable an expression writes into or reads from: x for x,
// x.f, x[i], x[i:j], *x.
func (p *valueProvenance) root(expr ast.Expr) types.Object {
	for {
		switch x := expr.(type) {
		case *ast.Ident:
			if obj, ok := p.info.Uses[x].(*types.Var); ok {
				return obj
			}
			if obj, ok := p.info.Defs[x].(*types.Var); ok {
				return obj
			}
			return nil
		case *ast.ParenExpr:
			expr = x.X
		case *ast.StarExpr:
			expr = x.X
		case *ast.UnaryExpr:
			expr = x.X
		case *ast.IndexExpr:
			expr = x.X
		case *ast.SliceExpr:
			expr = x.X
		case *ast.SelectorExpr:
			if _, field := p.info.Selections[x]; !field {
				// A package's variable (pkg.V).
				if obj, ok := p.info.Uses[x.Sel].(*types.Var); ok {
					return obj
				}
				return nil
			}
			expr = x.X
		default:
			return nil
		}
	}
}

// rootsOf are obj and, transitively, the variables it aliases.
func (p *valueProvenance) rootsOf(obj types.Object) []types.Object {
	if obj == nil {
		return nil
	}
	out := []types.Object{obj}
	seen := map[types.Object]bool{obj: true}
	for i := 0; i < len(out); i++ {
		for _, next := range p.aliases[out[i]] {
			if !seen[next] {
				seen[next] = true
				out = append(out, next)
			}
		}
	}
	return out
}

// collect records a file's flows.
func (p *valueProvenance) collect(file *ast.File) {
	var stack []*ast.FuncDecl
	var lits []*ast.FuncLit
	// declaration is the function declaration the code is in (a function
	// literal in it writes into its parameters too); enclosing is the
	// function its returns return from (none inside a function literal).
	declaration := func() (*types.Func, *ast.FuncDecl) {
		if len(stack) == 0 {
			return nil, nil
		}
		decl := stack[len(stack)-1]
		f, _ := p.info.Defs[decl.Name].(*types.Func)
		return f, decl
	}
	enclosing := func() (*types.Func, *ast.FuncDecl) {
		if len(lits) > 0 {
			return nil, nil
		}
		return declaration()
	}
	// write records data written into lhs: a variable, or (through a field,
	// element or dereference) the variable it is rooted at, and a parameter
	// sink when that variable is a parameter.
	write := func(lhs, from ast.Expr) {
		if ident, ok := lhs.(*ast.Ident); ok {
			obj := p.info.Defs[ident]
			if obj == nil {
				obj = p.info.Uses[ident]
			}
			if obj != nil && provenanceTracked(obj.Type()) {
				p.flows = append(p.flows, provenanceFlow{from: from, variable: obj})
				if provenanceReference(obj.Type()) {
					if source := p.root(from); source != nil && source != obj {
						p.aliases[obj] = append(p.aliases[obj], source)
					}
				}
			}
			return
		}
		for _, obj := range p.rootsOf(p.root(lhs)) {
			p.flows = append(p.flows, provenanceFlow{from: from, variable: obj})
			if f, decl := declaration(); f != nil {
				for i, param := range p.params(decl) {
					if param == obj {
						p.flows = append(p.flows, provenanceFlow{from: from, sink: f, sinkAt: i})
					}
				}
				if p.receiver(decl) == obj {
					p.flows = append(p.flows, provenanceFlow{from: from, sink: f, sinkAt: -1})
				}
			}
		}
	}
	var visit func(n ast.Node) bool
	visit = func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			stack = append(stack, x)
			if x.Body != nil {
				ast.Inspect(x.Body, visit)
			}
			stack = stack[:len(stack)-1]
			return false
		case *ast.FuncLit:
			for _, field := range x.Type.Params.List {
				for _, name := range field.Names {
					p.seed(p.info.Defs[name])
				}
			}
			lits = append(lits, x)
			ast.Inspect(x.Body, visit)
			lits = lits[:len(lits)-1]
			return false
		case *ast.AssignStmt:
			for i, lhs := range x.Lhs {
				rhs := x.Rhs[0]
				if len(x.Rhs) == len(x.Lhs) {
					rhs = x.Rhs[i]
				}
				write(lhs, rhs)
			}
		case *ast.ValueSpec:
			for i, name := range x.Names {
				if i < len(x.Values) {
					write(name, x.Values[i])
				} else if len(x.Values) == 1 {
					write(name, x.Values[0])
				}
			}
		case *ast.RangeStmt:
			if x.Key != nil {
				write(x.Key, x.X)
			}
			if x.Value != nil {
				write(x.Value, x.X)
			}
		case *ast.SendStmt:
			write(x.Chan, x.Value)
		case *ast.ReturnStmt:
			if f, decl := enclosing(); f != nil {
				results := x.Results
				if len(results) == 0 && decl.Type.Results != nil {
					// A bare return returns the named results.
					for _, field := range decl.Type.Results.List {
						for _, name := range field.Names {
							results = append(results, name)
						}
					}
				}
				for _, result := range results {
					p.flows = append(p.flows, provenanceFlow{from: result, result: f})
				}
			}
		case *ast.CallExpr:
			p.collectCall(x, write)
		}
		return true
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			visit(d)
		case *ast.GenDecl:
			ast.Inspect(d, visit)
		}
	}
}

// collectCall records a call's flows: arguments into a package function's
// parameters, a callee's writes into its parameters back into the
// arguments' variables, and, for any other call, data arguments into the
// receiver and the reference arguments.
func (p *valueProvenance) collectCall(call *ast.CallExpr, write func(lhs, from ast.Expr)) {
	if tv, ok := p.info.Types[call.Fun]; ok && tv.IsType() {
		return
	}
	if ident, ok := call.Fun.(*ast.Ident); ok {
		if builtin, ok := p.info.Uses[ident].(*types.Builtin); ok {
			if builtin.Name() == "copy" && len(call.Args) == 2 {
				write(call.Args[0], call.Args[1])
			}
			return
		}
	}
	var receiver ast.Expr
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		if _, method := p.info.Selections[selector]; method {
			receiver = selector.X
		}
	}
	callee := calledFunc(p.info, call)
	if callee != nil {
		callee = callee.Origin()
	}
	decl := p.decls[callee]
	if decl == nil {
		if callee != nil && callee.Pkg() != nil && boundaryStatementPackages[callee.Pkg().Path()] {
			return
		}
		// Another package, a function value or an interface: any data
		// argument may be written into what the call can reach.
		for _, arg := range call.Args {
			if receiver != nil {
				for _, obj := range p.rootsOf(p.root(receiver)) {
					p.flows = append(p.flows, provenanceFlow{from: arg, variable: obj})
				}
			}
			for _, other := range call.Args {
				if other != arg && provenanceReference(p.info.TypeOf(other)) {
					for _, obj := range p.rootsOf(p.root(other)) {
						p.flows = append(p.flows, provenanceFlow{from: arg, variable: obj})
					}
				}
			}
		}
		return
	}
	params := p.params(decl)
	variadic := false
	if sig, ok := callee.Type().(*types.Signature); ok {
		variadic = sig.Variadic()
	}
	for i, arg := range call.Args {
		at := i
		if at >= len(params) {
			if !variadic || len(params) == 0 {
				break
			}
			at = len(params) - 1
		}
		if param := params[at]; param != nil && provenanceTracked(param.Type()) {
			p.flows = append(p.flows, provenanceFlow{from: arg, variable: param})
		}
		for _, obj := range p.rootsOf(p.root(arg)) {
			p.flows = append(p.flows, provenanceFlow{fromSink: callee, sinkIndex: at, variable: obj})
		}
	}
	if receiver != nil {
		if recv := p.receiver(decl); recv != nil && provenanceTracked(recv.Type()) {
			p.flows = append(p.flows, provenanceFlow{from: receiver, variable: recv})
		}
		for _, obj := range p.rootsOf(p.root(receiver)) {
			p.flows = append(p.flows, provenanceFlow{fromSink: callee, sinkIndex: -1, variable: obj})
		}
	}
}

// provenanceReference reports a type a callee can write through: a pointer,
// map, slice, channel or interface.
func provenanceReference(typ types.Type) bool {
	if typ == nil {
		return false
	}
	switch typ.Underlying().(type) {
	case *types.Pointer, *types.Map, *types.Slice, *types.Chan, *types.Interface:
		return true
	}
	return false
}

// solve runs the flows to their fixed point; it reports whether any mark
// changed.
func (p *valueProvenance) solve() bool {
	grew := false
	for changed := true; changed; {
		changed = false
		for i := range p.flows {
			flow := &p.flows[i]
			if flow.variable == nil && flow.result == nil && flow.sink == nil {
				continue
			}
			if p.flowDone(flow) {
				continue
			}
			data := false
			if flow.fromSink != nil {
				data = p.sinks[flow.fromSink][flow.sinkIndex]
			} else {
				data = p.holds(flow.from)
			}
			if !data {
				continue
			}
			switch {
			case flow.variable != nil:
				p.data[flow.variable] = true
			case flow.result != nil:
				p.results[flow.result] = true
			default:
				if p.sinks[flow.sink] == nil {
					p.sinks[flow.sink] = map[int]bool{}
				}
				p.sinks[flow.sink][flow.sinkAt] = true
			}
			changed, grew = true, true
		}
	}
	return grew
}

func (p *valueProvenance) flowDone(flow *provenanceFlow) bool {
	switch {
	case flow.variable != nil:
		return p.data[flow.variable]
	case flow.result != nil:
		return p.results[flow.result]
	default:
		return p.sinks[flow.sink][flow.sinkAt]
	}
}

// holds reports an expression that may hold the query's data.
func (p *valueProvenance) holds(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	if tv, ok := p.info.Types[expr]; ok {
		if tv.Value != nil || tv.IsNil() || tv.IsType() {
			return false
		}
		if basic, ok := tv.Type.Underlying().(*types.Basic); ok {
			if basic.Info()&types.IsString != 0 && p.taint != nil {
				return p.taint(expr)
			}
			return false
		}
		if stringLike(tv.Type) && p.taint != nil {
			// []string, []byte: the string taint's.
			return p.taint(expr)
		}
	}
	switch x := expr.(type) {
	case *ast.Ident:
		switch obj := p.info.Uses[x].(type) {
		case *types.Var:
			if obj.Pkg() != nil && obj.Pkg().Path() != p.path {
				// Another package's variable.
				return true
			}
			return p.data[obj]
		case nil:
			if def, ok := p.info.Defs[x].(*types.Var); ok {
				return p.data[def]
			}
		}
		return false
	case *ast.BasicLit, *ast.FuncLit:
		return false
	case *ast.CompositeLit:
		for _, element := range x.Elts {
			if p.holds(element) {
				return true
			}
		}
		return false
	case *ast.KeyValueExpr:
		return p.holds(x.Key) || p.holds(x.Value)
	case *ast.ParenExpr:
		return p.holds(x.X)
	case *ast.StarExpr:
		return p.holds(x.X)
	case *ast.UnaryExpr:
		return p.holds(x.X)
	case *ast.BinaryExpr:
		return p.holds(x.X) || p.holds(x.Y)
	case *ast.SelectorExpr:
		if _, field := p.info.Selections[x]; field {
			return p.holds(x.X)
		}
		return p.holds(x.Sel)
	case *ast.IndexExpr:
		return p.holds(x.X)
	case *ast.IndexListExpr:
		return p.holds(x.X)
	case *ast.SliceExpr:
		return p.holds(x.X)
	case *ast.TypeAssertExpr:
		return p.holds(x.X)
	case *ast.CallExpr:
		if tv, ok := p.info.Types[x.Fun]; ok && tv.IsType() {
			return len(x.Args) == 1 && p.holds(x.Args[0])
		}
		if ident, ok := x.Fun.(*ast.Ident); ok {
			if builtin, ok := p.info.Uses[ident].(*types.Builtin); ok {
				if builtin.Name() == "append" {
					for _, arg := range x.Args {
						if p.holds(arg) {
							return true
						}
					}
				}
				return false
			}
		}
		callee := calledFunc(p.info, x)
		if callee != nil {
			callee = callee.Origin()
			if p.decls[callee] != nil {
				return p.results[callee]
			}
			if callee.Pkg() != nil && boundaryStatementPackages[callee.Pkg().Path()] {
				return false
			}
		}
		return true
	}
	return true
}

// holds reports an expression that may hold the query's data; without a
// provenance (the remediation inventories) every value may.
func (a *boundaryAnalysis) holds(expr ast.Expr) bool {
	if a.provenance == nil {
		return true
	}
	return a.provenance.holds(expr)
}

// dataValue reports an argument whose type can hold data (a Cypher value, a
// population, a list or map) and whose provenance may be data, or a data
// string.
func (a *boundaryAnalysis) dataValue(arg ast.Expr) bool {
	typ := a.pkg.info.TypeOf(arg)
	return (cypherValueType(typ) || isQuerySizedValue(typ)) && a.holds(arg) || a.dataSized(arg)
}

// solveProvenance runs provenance and the string taint together to their
// common fixed point: each reads the other.
func (a *boundaryAnalysis) solveProvenance(files []*ast.File) {
	a.provenance = newValueProvenance(a.pkg.info, a.decls, files, a.dataExpr)
	for {
		a.provenance.solve()
		before := len(a.tainted)
		a.propagate(files)
		if len(a.tainted) == before {
			return
		}
	}
}
