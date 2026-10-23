package cypher

import (
	"encoding/json"
	"go/ast"
	"go/constant"
	"go/types"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// The guard's second set of rules: observation must dominate a pass's
// progress (not merely occur somewhere in it), every loop form and recursion
// counts, whole-value library work counts, the owner's stop is recovered only
// at a boundary, workers own their checkpoints, markers state a checkable
// bound, and an owner's job is never implemented a second time.

// boundaryOwnerIterators are the cancellation owner's iterators: a loop
// ranging over one is observed by it.
var boundaryOwnerIterators = map[string]bool{
	"observedItems": true, "observedEntries": true, "observedRunes": true, "observedBytes": true,
	"observedCount": true, "materializedBatches": true,
}

// boundaryRecoveries are the functions that are the owner's boundaries: they
// alone recover its stop (besides goroutines, which recover their own).
var boundaryRecoveries = map[string]bool{
	// Package cypher.
	"row_cancellation.go:insideStopBoundary":                                true,
	"executor_entry.go:*StorageExecutor.execute":                            true,
	"executor_internal.go:*StorageExecutor.executeInternal":                 true,
	"executor.go:*StorageExecutor.executeWithImplicitTransactionCallback":   true,
	"procedure_statement_stats.go:*StorageExecutor.executeProcedureHandler": true,
	"executor_fabric.go:*cypherFabricExecutor.ExecuteRecordQuery":           true,
	"executor_fabric.go:*cypherFabricExecutor.ExecuteQueryWithRecord":       true,
	"optimized_executors.go:*StorageExecutor.ExecuteOptimized":              true,
	"cache.go:*QueryCache.cacheKey":                                         true,
	"cache.go:resultCacheEntryKey":                                          true,
	// The statement's completion runs after execute's boundary returned.
	"node_helpers.go:*StorageExecutor.recordMaterializedResultAccess": true,
	// Package fn.
	"registry.go:callFunction":        true,
	"runtime_value.go:FunctionResult": true,
}

// boundaryStatementEntries are where statement text enters execution, held
// to a stated limit: the client's statement to the request limits (HTTP
// MaxRequestSize, Bolt's message admission), text a procedure runs to
// DynamicStatementLimit (requiredStatementText). Text does not taint past
// them: passes over statement text are bounded by those limits.
var boundaryStatementEntries = map[string]bool{
	"executor_internal.go:*StorageExecutor.executeUserStatement": true,
	"executor_internal.go:*StorageExecutor.executeInternal":      true,
	"executor_entry.go:*StorageExecutor.Execute":                 true,
	"executor_entry.go:*StorageExecutor.execute":                 true,
}

// boundaryStopAPI are the owner's own primitives other packages call inside
// their boundaries: they stop by design (bulk.Checkpoint.Check's contract).
var boundaryStopAPI = map[string]bool{
	"value_walk.go:TransformValue": true, "value_walk.go:VisitValue": true, "value_walk.go:ProjectValue": true,
	"value_walk.go:IdenticalValues": true, "value_walk.go:VisitValueDepth": true,
}

// boundaryLimitations are library calls no checkpoint can interrupt: listed
// as the PR's stated limitations, recorded in the ledger, not findings.
var boundaryLimitations = map[string]string{
	"regexp":                                "RE2 matching runs to its end",
	"golang.org/x/text/unicode/norm":        "Unicode normalization runs to its end",
	"github.com/orneryd/nornicdb/apoc/util": "the bundled APOC library's digest",
}

// isCheckpointType reports the owner's checkpoint, or a pointer to it, under
// any alias (materializedRowCheckpoint).
func isCheckpointType(typ types.Type) bool {
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == cypherPackagePath+"/bulk" && named.Obj().Name() == "Checkpoint"
}

// dominates reports a loop body whose first statement observes the
// statement: it runs on every iteration before any continue, early return,
// rejected element or empty inner loop.
func (a *boundaryAnalysis) dominates(body *ast.BlockStmt) bool {
	return body != nil && len(body.List) > 0 && a.observation(body.List[0])
}

// observation reports a statement that observes the statement: the owner's
// Check, or an if whose condition or init observes (an error-returning
// observation, a context's Err).
func (a *boundaryAnalysis) observation(statement ast.Stmt) bool {
	observes := func(node ast.Node) bool {
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					switch selector.Sel.Name {
					case "Check", "CheckBytes", "Observe", "ObserveSteps", "observe", "canceled", "Err":
						found = true
					}
				}
			}
			return !found
		})
		return found
	}
	switch s := statement.(type) {
	case *ast.ExprStmt:
		if call, ok := s.X.(*ast.CallExpr); ok {
			if callee := calledFunc(a.pkg.info, call); callee != nil && callee.FullName() == "(*"+cypherPackagePath+"/bulk.Checkpoint).Check" {
				return true
			}
		}
	case *ast.IfStmt:
		return (s.Init != nil && observes(s.Init)) || observes(s.Cond)
	}
	return false
}

// ownerIteration reports a range over one of the owner's iterators.
func (a *boundaryAnalysis) ownerIteration(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	fun := call.Fun
	if index, ok := fun.(*ast.IndexExpr); ok {
		fun = index.X
	}
	if index, ok := fun.(*ast.IndexListExpr); ok {
		fun = index.X
	}
	if ident, ok := fun.(*ast.Ident); ok && boundaryOwnerIterators[ident.Name] {
		return true
	}
	return false
}

// rangeSize reports a range over data: a population, a list, a map, a data
// string, or a data-sized count (range n).
func (a *boundaryAnalysis) rangeSize(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	if _, literal := expr.(*ast.CompositeLit); literal {
		// A composite literal's length is fixed by the source.
		return false
	}
	if a.dataSized(expr) {
		return true
	}
	if basic, ok := a.pkg.info.TypeOf(expr).Underlying().(*types.Basic); ok && basic.Info()&types.IsInteger != 0 {
		return a.dataCount(expr)
	}
	return false
}

// dataCount reports an integer sized by data: the length of a data-sized
// value, or a variable assigned one.
func (a *boundaryAnalysis) dataCount(expr ast.Node) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if ident, ok := x.Fun.(*ast.Ident); ok && (ident.Name == "len" || ident.Name == "cap") && len(x.Args) == 1 && a.dataSized(x.Args[0]) {
				found = true
			}
		case *ast.Ident:
			if obj := a.pkg.info.Uses[x]; obj != nil && a.lengths[obj] {
				found = true
			}
		case *ast.TypeAssertExpr:
			// A count read from a Cypher value (a scalar-driven output).
			if x.Type != nil && cypherValueType(a.pkg.info.TypeOf(x.X)) && a.holds(x.X) {
				if basic, ok := a.pkg.info.TypeOf(x.Type).Underlying().(*types.Basic); ok && basic.Info()&types.IsInteger != 0 {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// forSize reports a for loop whose progress is data-sized: a condition or
// init on a data length (forward, reverse or aliased), an iterator's Next, or
// a conditionless loop over data.
func (a *boundaryAnalysis) forSize(loop *ast.ForStmt) (string, bool) {
	if loop.Cond == nil {
		data := false
		ast.Inspect(loop.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.IndexExpr:
				data = data || a.dataSized(x.X)
			case *ast.SliceExpr:
				data = data || a.dataSized(x.X)
			}
			return !data
		})
		return "{}", data
	}
	detail := bulkWorkExpr(loop.Cond)
	iterator := false
	ast.Inspect(loop.Cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				switch selector.Sel.Name {
				case "Next", "Scan", "More":
					iterator = true
				}
			}
		}
		return !iterator
	})
	if iterator {
		return detail, true
	}
	if a.dataCount(loop.Cond) || (loop.Init != nil && a.dataCount(loop.Init)) {
		return detail, true
	}
	return detail, false
}

// noteLengths records, to a fixed point, the integer variables assigned a
// data length (n := len(list), last := n - 1).
func (a *boundaryAnalysis) noteLengths(files []*ast.File) {
	a.lengths = map[types.Object]bool{}
	for changed := true; changed; {
		changed = false
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				assign, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, lhs := range assign.Lhs {
					if i >= len(assign.Rhs) {
						break
					}
					ident, ok := lhs.(*ast.Ident)
					if !ok {
						continue
					}
					obj := a.pkg.info.Defs[ident]
					if obj == nil {
						obj = a.pkg.info.Uses[ident]
					}
					if obj == nil || a.lengths[obj] {
						continue
					}
					if basic, ok := obj.Type().Underlying().(*types.Basic); !ok || basic.Info()&types.IsInteger == 0 {
						continue
					}
					if a.dataCount(assign.Rhs[i]) {
						a.lengths[obj] = true
						changed = true
					}
				}
				return true
			})
		}
	}
}

// recursionFindings reports a recursive function over data (a cycle of the
// package's calls whose member takes a Cypher value, a population or a data
// string) whose body does not start with an observation: each level is one
// step of a pass the owner must observe.
func (a *boundaryAnalysis) recursionFindings(files []*ast.File) []boundaryFinding {
	calls := map[*types.Func][]*types.Func{}
	for f, decl := range a.decls {
		ast.Inspect(decl.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if callee := calledFunc(a.pkg.info, call); callee != nil && a.decls[callee] != nil {
					calls[f] = append(calls[f], callee)
				}
			}
			return true
		})
	}
	// Tarjan's strongly connected components.
	index, low, onStack := map[*types.Func]int{}, map[*types.Func]int{}, map[*types.Func]bool{}
	var stack []*types.Func
	recursive := map[*types.Func]bool{}
	components := map[*types.Func][]*types.Func{}
	next := 0
	var connect func(f *types.Func)
	connect = func(f *types.Func) {
		index[f], low[f] = next, next
		next++
		stack = append(stack, f)
		onStack[f] = true
		for _, g := range calls[f] {
			if _, seen := index[g]; !seen {
				connect(g)
				low[f] = min(low[f], low[g])
			} else if onStack[g] {
				low[f] = min(low[f], index[g])
			}
		}
		if low[f] == index[f] {
			var component []*types.Func
			for {
				g := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[g] = false
				component = append(component, g)
				if g == f {
					break
				}
			}
			self := false
			for _, g := range calls[f] {
				self = self || g == f
			}
			if len(component) > 1 || self {
				for _, g := range component {
					recursive[g] = true
					components[g] = component
				}
			}
		}
	}
	for f := range a.decls {
		if _, seen := index[f]; !seen {
			connect(f)
		}
	}
	inFiles := map[*ast.FuncDecl]string{}
	for _, file := range files {
		name := filepath.Base(a.pkg.fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				inFiles[fn] = name
			}
		}
	}
	var findings []boundaryFinding
	for f := range recursive {
		decl := a.decls[f]
		name, ok := inFiles[decl]
		if !ok || strings.HasSuffix(name, "_test.go") || !a.recursesIntoData(f, decl) || a.dominates(decl.Body) {
			continue
		}
		if !a.cyclesUnobserved(f, components[f], calls) {
			// Every cycle back to f passes a level that starts with an
			// observation (the owner's entry, say): each level observes.
			continue
		}
		findings = append(findings, boundaryFinding{File: name, Function: decl.Name.Name, Key: funcKey(a.pkg, f), Line: a.pkg.fset.Position(decl.Pos()).Line,
			Kind: "recursion", Detail: "a recursive pass over data whose levels do not start with an observation", Scope: a.scope(decl), Observe: a.observeExpr(decl)})
	}
	return findings
}

// fmtTypeOnlyArguments are the arguments of a fmt call with a constant
// format that only a %T or %p verb formats (the type's name, a pointer): no
// pass over the value. An explicit argument index makes every argument
// count (none is reported).
func fmtTypeOnlyArguments(info *types.Info, f *types.Func, call *ast.CallExpr) map[int]bool {
	formatAt := -1
	switch f.Name() {
	case "Sprintf", "Errorf", "Printf":
		formatAt = 0
	case "Fprintf":
		formatAt = 1
	}
	if f.Pkg().Path() != "fmt" || formatAt < 0 || len(call.Args) <= formatAt {
		return nil
	}
	tv, ok := info.Types[call.Args[formatAt]]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return nil
	}
	format := constant.StringVal(tv.Value)
	verbs := map[int][]byte{}
	next := formatAt + 1
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		i++
		for i < len(format) && strings.IndexByte("+-# 0123456789.*", format[i]) >= 0 {
			if format[i] == '*' {
				next++
			}
			i++
		}
		if i >= len(format) {
			break
		}
		switch format[i] {
		case '%':
			continue
		case '[':
			return nil
		}
		verbs[next] = append(verbs[next], format[i])
		next++
	}
	out := map[int]bool{}
	for index, used := range verbs {
		typeOnly := true
		for _, verb := range used {
			typeOnly = typeOnly && (verb == 'T' || verb == 'p')
		}
		out[index] = typeOnly
	}
	return out
}

// cyclesUnobserved reports a cycle from f back to f, within its recursive
// component, that passes no function whose body starts with an observation.
func (a *boundaryAnalysis) cyclesUnobserved(f *types.Func, component []*types.Func, calls map[*types.Func][]*types.Func) bool {
	member := map[*types.Func]bool{}
	for _, g := range component {
		member[g] = g == f || !a.dominates(a.decls[g].Body)
	}
	seen := map[*types.Func]bool{}
	var reaches func(g *types.Func) bool
	reaches = func(g *types.Func) bool {
		for _, h := range calls[g] {
			if h == f {
				return true
			}
			if member[h] && !seen[h] {
				seen[h] = true
				if reaches(h) {
					return true
				}
			}
		}
		return false
	}
	return reaches(f)
}

// summarizePasses finds, to a fixed point, the parameters each package
// function makes an unobserved pass over: a loop on the parameter's length
// or over it, a whole-text library call or conversion of it, or a hand-off
// of it to a function that passes over it.
func (a *boundaryAnalysis) summarizePasses() {
	a.passParams = map[*types.Func]map[int]bool{}
	a.passLoops = map[*types.Func]map[int]bool{}
	a.passHandoffs = map[*types.Func]map[int]*types.Func{}
	paramIndex := func(decl *ast.FuncDecl) map[types.Object]int {
		index := map[types.Object]int{}
		at := 0
		for _, field := range decl.Type.Params.List {
			names := field.Names
			if len(names) == 0 {
				at++
				continue
			}
			for _, name := range names {
				// A row's values (values, row, scope, bindings) are keyed by
				// the statement's variables: the row scope, not data.
				if obj := a.pkg.info.Defs[name]; obj != nil && !boundaryRowScopeParameter(name.Name, obj.Type()) {
					index[obj] = at
				}
				at++
			}
		}
		return index
	}
	paramOf := func(index map[types.Object]int, expr ast.Expr) (int, bool) {
		ident, ok := expr.(*ast.Ident)
		if !ok {
			return 0, false
		}
		i, ok := index[a.pkg.info.Uses[ident]]
		return i, ok
	}
	lengthParam := func(index map[types.Object]int, node ast.Node) (int, bool) {
		at, found := 0, false
		ast.Inspect(node, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && !found {
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "len" && len(call.Args) == 1 {
					at, found = paramOf(index, call.Args[0])
				}
			}
			return !found
		})
		return at, found
	}
	for changed := true; changed; {
		changed = false
		for f, decl := range a.decls {
			index := paramIndex(decl)
			if len(index) == 0 {
				continue
			}
			mark := func(i int, at ast.Node) {
				if a.passParams[f] == nil {
					a.passParams[f] = map[int]bool{}
				}
				if a.passLoops[f] == nil {
					a.passLoops[f] = map[int]bool{}
				}
				if _, loop := at.(ast.Stmt); loop {
					a.passLoops[f][a.pkg.fset.Position(at.Pos()).Line] = true
				}
				if !a.passParams[f][i] {
					a.passParams[f][i] = true
					changed = true
				}
			}
			ast.Inspect(decl.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.FuncLit:
					return false
				case *ast.RangeStmt:
					if i, ok := paramOf(index, x.X); ok && !a.dominates(x.Body) && !a.ownerIteration(x.X) {
						if t := a.pkg.info.TypeOf(x.X); stringLike(t) || isQuerySizedValue(t) || cypherValueType(t) {
							mark(i, x)
						}
					}
				case *ast.ForStmt:
					if !a.dominates(x.Body) {
						if x.Cond == nil {
							break
						}
						if i, ok := lengthParam(index, x.Cond); ok {
							mark(i, x)
						} else if x.Init != nil {
							if i, ok := lengthParam(index, x.Init); ok {
								mark(i, x)
							}
						}
					}
				case *ast.CallExpr:
					callee := calledFunc(a.pkg.info, x)
					if tv, ok := a.pkg.info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 && stringLike(tv.Type) {
						if i, ok := paramOf(index, x.Args[0]); ok && !(basicString(tv.Type) && basicString(a.pkg.info.TypeOf(x.Args[0]))) {
							mark(i, x)
						}
						return true
					}
					if callee == nil || callee.Pkg() == nil {
						return true
					}
					whole := callee.Pkg().Path() == "strings" && boundaryStringPasses[callee.Name()] ||
						callee.Pkg().Path() == "fmt" ||
						strings.HasPrefix(callee.FullName(), "(*strings.Builder).Write") || strings.HasPrefix(callee.FullName(), "(*bytes.Buffer).Write")
					for j, arg := range x.Args {
						i, ok := paramOf(index, arg)
						if !ok {
							continue
						}
						if whole || a.passParams[callee][j] {
							mark(i, x)
						}
						if !whole && a.passParams[callee][j] {
							if a.passHandoffs[f] == nil {
								a.passHandoffs[f] = map[int]*types.Func{}
							}
							a.passHandoffs[f][a.pkg.fset.Position(x.Pos()).Line] = callee
						}
					}
				}
				return true
			})
		}
	}
}

// spawnsWorkers reports a coordinator's spawn loop: its body starts a
// goroutine. It runs once per worker, whose count the coordinator chooses;
// each worker observes the statement itself (one checkpoint per worker,
// see goroutines), so the loop is not a pass over data.
func spawnsWorkers(body *ast.BlockStmt) bool {
	for _, statement := range body.List {
		if _, ok := statement.(*ast.GoStmt); ok {
			return true
		}
	}
	return false
}

// recursesIntoData reports a function that calls itself (or its cycle)
// on a member of data: inside a pass over a data-sized value, or with an
// element read from one. Recursion over statement text is a parse, bounded
// by the statement.
func (a *boundaryAnalysis) recursesIntoData(f *types.Func, decl *ast.FuncDecl) bool {
	found := false
	var visit func(node ast.Node, inDataPass bool)
	visit = func(node ast.Node, inDataPass bool) {
		ast.Inspect(node, func(n ast.Node) bool {
			if found {
				return false
			}
			switch x := n.(type) {
			case *ast.RangeStmt:
				if x != node {
					if a.dominates(x.Body) || a.ownerIteration(x.X) {
						// Each recursive call in it follows the iteration's
						// observation: those levels are observed.
						return false
					}
					visit(x.Body, inDataPass || a.rangeSize(x.X))
					return false
				}
			case *ast.ForStmt:
				if x != node && a.dominates(x.Body) {
					return false
				}
			case *ast.CallExpr:
				callee := calledFunc(a.pkg.info, x)
				if callee == nil || a.decls[callee] == nil {
					return true
				}
				if callee != f && !a.inCycle(f, callee) {
					return true
				}
				if inDataPass {
					found = true
					return false
				}
				for _, arg := range x.Args {
					if index, ok := arg.(*ast.IndexExpr); ok && a.dataSized(index.X) {
						found = true
					}
				}
			}
			return !found
		})
	}
	visit(decl.Body, false)
	return found && a.dataParameter(decl)
}

// inCycle reports g calling back to f (a recursive cycle), to a small depth.
func (a *boundaryAnalysis) inCycle(f, g *types.Func) bool {
	seen := map[*types.Func]bool{}
	var reaches func(h *types.Func, depth int) bool
	reaches = func(h *types.Func, depth int) bool {
		if h == f {
			return true
		}
		if depth == 0 || seen[h] || a.decls[h] == nil {
			return false
		}
		seen[h] = true
		hit := false
		ast.Inspect(a.decls[h].Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && !hit {
				if callee := calledFunc(a.pkg.info, call); callee != nil && reaches(callee, depth-1) {
					hit = true
				}
			}
			return !hit
		})
		return hit
	}
	return reaches(g, 3)
}

// dataParameter reports a function taking a Cypher value, a population or a
// data string.
func (a *boundaryAnalysis) dataParameter(decl *ast.FuncDecl) bool {
	for _, field := range decl.Type.Params.List {
		typ := a.pkg.info.TypeOf(field.Type)
		for _, name := range field.Names {
			obj := a.pkg.info.Defs[name]
			if obj == nil {
				continue
			}
			// A value parameter some data reaches (see provenance).
			if (cypherValueType(typ) || isQuerySizedValue(typ)) && (a.provenance == nil || a.provenance.data[obj]) {
				return true
			}
			if a.tainted[obj] {
				return true
			}
		}
	}
	return false
}

// recoveryFindings reports a recovery of the owner's stop outside its
// boundaries (a helper that recovers returns a partial result's error, or
// swallows the stop): RecoverStop deferred, or insideStopBoundary called,
// in a function that is not a boundary, outside a goroutine.
func (a *boundaryAnalysis) recoveryFindings(name string, fn *ast.FuncDecl) []boundaryFinding {
	f, _ := a.pkg.info.Defs[fn.Name].(*types.Func)
	key := funcKey(a.pkg, f)
	// An exported function is a boundary of public execution: its stop must
	// not leave the package (stopFindings), so it recovers it.
	if boundaryRecoveries[key] || fn.Name.IsExported() {
		return nil
	}
	var findings []boundaryFinding
	var visit func(node ast.Node)
	visit = func(node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GoStmt:
				return false
			case *ast.CallExpr:
				callee := calledFunc(a.pkg.info, x)
				if callee == nil || callee.Pkg() == nil {
					return true
				}
				recover := callee.Pkg().Path() == cypherPackagePath+"/bulk" && callee.Name() == "RecoverStop"
				boundary := callee.Pkg().Path() == cypherPackagePath && callee.Name() == "insideStopBoundary"
				if recover || boundary {
					findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Key: key, Line: a.pkg.fset.Position(x.Pos()).Line,
						Kind: "recover", Detail: "the owner's stop recovered outside a boundary (" + callee.Name() + ")", Scope: a.scope(fn)})
				}
			}
			return true
		})
	}
	visit(fn.Body)
	return findings
}

// workerCheckpointFindings reports a goroutine that checks a checkpoint it
// shares with another goroutine: each worker owns its own state under the
// same statement.
func (a *boundaryAnalysis) workerCheckpointFindings(name string, fn *ast.FuncDecl) []boundaryFinding {
	var findings []boundaryFinding
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		statement, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		lit, ok := statement.Call.Fun.(*ast.FuncLit)
		if !ok {
			return true
		}
		ast.Inspect(lit.Body, func(m ast.Node) bool {
			ident, ok := m.(*ast.Ident)
			if !ok {
				return true
			}
			obj, ok := a.pkg.info.Uses[ident].(*types.Var)
			if !ok || !isCheckpointType(obj.Type()) {
				return true
			}
			if obj.Pos() < lit.Pos() || obj.Pos() > lit.End() {
				findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Line: a.pkg.fset.Position(ident.Pos()).Line,
					Kind: "go", Detail: "a worker checks a checkpoint it shares: " + ident.Name, Scope: a.scope(fn)})
			}
			return true
		})
		return true
	})
	return findings
}

// boundaryBoundPattern is the bound a marker states: a number, or a named
// constant of the package.
var boundaryBoundPattern = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*|[0-9]+`)

// markerBound reports whether a marker's reason names a checkable bound: an
// integer, or an integer constant of the package.
func (a *boundaryAnalysis) markerBound(reason string) bool {
	for _, word := range boundaryBoundPattern.FindAllString(reason, -1) {
		if word[0] >= '0' && word[0] <= '9' {
			return true
		}
		for ident, obj := range a.pkg.info.Defs {
			if ident.Name != word {
				continue
			}
			if c, ok := obj.(*types.Const); ok && c.Val().Kind() == constant.Int {
				return true
			}
		}
	}
	return false
}

// duplicateOwnerFindings reports a function that does an owner's job itself:
// a recursive walk of Cypher values (a type switch over lists and maps that
// calls itself), a deep comparison of values (reflect.DeepEqual), an
// encoding of a value (encoding/json), outside the owners.
func (a *boundaryAnalysis) duplicateOwnerFindings(name string, fn *ast.FuncDecl) []boundaryFinding {
	f, _ := a.pkg.info.Defs[fn.Name].(*types.Func)
	if f == nil || boundaryValueOwners[funcKey(a.pkg, f)] {
		return nil
	}
	var findings []boundaryFinding
	report := func(at ast.Node, detail string) {
		findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Key: funcKey(a.pkg, f), Line: a.pkg.fset.Position(at.Pos()).Line,
			Kind: "duplicate", Detail: detail, Scope: a.scope(fn), Observe: a.observeExpr(fn)})
	}
	recursive := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && calledFunc(a.pkg.info, call) == f {
			recursive = true
		}
		return !recursive
	})
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.TypeSwitchStmt:
			if !recursive {
				return true
			}
			list, mapping := false, false
			for _, clause := range x.Body.List {
				for _, typ := range clause.(*ast.CaseClause).List {
					switch types.ExprString(typ) {
					case "[]interface{}", "[]any":
						list = true
					case "map[string]interface{}", "map[string]any":
						mapping = true
					}
				}
			}
			if list && mapping {
				report(x, "a recursive walk of Cypher values outside the value walker (fn.TransformValue / VisitValue / ProjectValue)")
			}
		case *ast.CallExpr:
			callee := calledFunc(a.pkg.info, x)
			if callee == nil || callee.Pkg() == nil {
				return true
			}
			switch callee.Pkg().Path() + "." + callee.Name() {
			case "reflect.DeepEqual":
				for _, arg := range x.Args {
					if typ := a.pkg.info.TypeOf(arg); cypherValueType(typ) || isQuerySizedValue(typ) {
						report(x, "a deep comparison of values outside the equality owner (reflect.DeepEqual)")
						break
					}
				}
			}
		}
		return true
	})
	return findings
}

// boundaryValueOwners are the owners of value walks, keys, text and
// comparisons: their own recursion is the one implementation.
var boundaryValueOwners = map[string]bool{
	"value_walk.go:TransformValue": true, "value_walk.go:VisitValue": true, "value_walk.go:ProjectValue": true,
	"value_walk.go:IdenticalValues": true, "value_walk.go:identicalMembers": true, "value_walk.go:visitValueAt": true,
	"cache_key.go:writeCacheKeyValue": true, "value_equivalence.go:appendValueKey": true,
	"json_value_encoder.go:writeJSONValue": true, "value_text.go:writeValueText": true,
}

// passLoopInventory lists each function's summarized parameter passes (its
// key, file, lines, scope and statement expression) as JSON, for remediation.
func passLoopInventory(pkg *guardPackage) []string {
	a := &boundaryAnalysis{pkg: pkg, tainted: map[types.Object]bool{}, decls: map[*types.Func]*ast.FuncDecl{}}
	for _, file := range pkg.files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				if obj, ok := pkg.info.Defs[fn.Name].(*types.Func); ok {
					a.decls[obj] = fn
				}
			}
		}
	}
	a.propagate(pkg.files)
	a.noteLengths(pkg.files)
	a.summarizePasses()
	var out []string
	for f, lines := range a.passLoops {
		decl := a.decls[f]
		file := filepath.Base(pkg.fset.Position(decl.Pos()).Filename)
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		var at []int
		for line := range lines {
			at = append(at, line)
		}
		handoffs := map[string]string{}
		for line, callee := range a.passHandoffs[f] {
			handoffs[strconv.Itoa(line)] = funcKey(pkg, callee)
		}
		encoded, _ := json.Marshal(map[string]interface{}{"key": funcKey(pkg, f), "file": file, "lines": at, "handoffs": handoffs, "scope": a.scope(decl), "observe": a.observeExpr(decl)})
		out = append(out, string(encoded))
	}
	return out
}

// boundaryRowScopeParameter reports a row's values map parameter (the scope
// a row expression reads its variables from): keyed by the statement's
// variables, as the row scope (scope) reads it.
func boundaryRowScopeParameter(name string, typ types.Type) bool {
	m, ok := typ.Underlying().(*types.Map)
	if !ok || !cypherValueType(m.Elem()) {
		return false
	}
	switch name {
	case "values", "row", "scope", "bindings", "vals", "nodeCtx", "nodeContext":
		return true
	}
	return false
}
