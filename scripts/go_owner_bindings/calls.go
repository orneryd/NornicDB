package main

import (
	"go/ast"
	"go/token"
	"go/types"
)

// collector walks one function body and records call metadata in source order.
type collector struct {
	a    *analyzer
	pkg  *types.Package
	info *types.Info
	f    sourceFile
	out  []callInfo

	// litStack tracks the function literals containing the current node.
	litStack []*ast.FuncLit
	// parents records the direct parent node of every node in the body.
	parents map[ast.Node]ast.Node
	// litCall records the outer call that directly receives a literal as an
	// argument, keyed by the literal.
	litCall map[*ast.FuncLit]*ast.CallExpr
	litSlot map[*ast.FuncLit]int
}

// calls collects call metadata for one function body in source order.
func (a *analyzer) calls(pkg *types.Package, info *types.Info, f sourceFile, body *ast.BlockStmt) []callInfo {
	c := &collector{
		a:       a,
		pkg:     pkg,
		info:    info,
		f:       f,
		out:     []callInfo{},
		parents: map[ast.Node]ast.Node{},
		litCall: map[*ast.FuncLit]*ast.CallExpr{},
		litSlot: map[*ast.FuncLit]int{},
	}
	indexParents(body, c.parents)
	c.walk(body)
	return c.out
}

// indexParents records the direct parent of every node beneath n.
func indexParents(n ast.Node, parents map[ast.Node]ast.Node) {
	ast.Inspect(n, func(node ast.Node) bool {
		if node == nil || node == n {
			return true
		}
		parents[node] = n
		indexParents(node, parents)
		return false
	})
}

// walk visits n in source order, descending into function literals with a
// nested literal context.
func (c *collector) walk(n ast.Node) {
	ast.Inspect(n, func(node ast.Node) bool {
		if node == nil {
			return true
		}
		switch x := node.(type) {
		case *ast.FuncLit:
			c.enterLiteral(x)
			c.walk(x.Body)
			c.litStack = c.litStack[:len(c.litStack)-1]
			return false
		case *ast.CallExpr:
			c.out = append(c.out, c.callInfo(x))
			return true
		}
		return true
	})
}

// enterLiteral records how a literal is nested and pushes it.
//
// A literal reaches an enclosing call either as an argument or as the callee
// itself (`go func() { ... }()`). Only the argument case records an outer
// call slot.
func (c *collector) enterLiteral(lit *ast.FuncLit) {
	c.litStack = append(c.litStack, lit)
	var node ast.Node = lit
	for {
		p, ok := c.parents[node]
		if !ok {
			return
		}
		switch par := p.(type) {
		case *ast.ParenExpr:
			node = par
			continue
		case *ast.CallExpr:
			if unwrapParen(par.Fun) == ast.Expr(lit) {
				// The literal is the callee, not an argument.
				return
			}
			for i, arg := range par.Args {
				if unwrapParen(arg) == ast.Expr(lit) {
					c.litCall[lit] = par
					c.litSlot[lit] = i
				}
			}
			return
		default:
			return
		}
	}
}

// unwrapParen removes redundant parentheses.
func unwrapParen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// unwrapInstantiation removes generic type arguments from a call target so
// that F[int](...) and F[int, string](...) resolve to F's object.
func unwrapInstantiation(e ast.Expr) ast.Expr {
	for {
		switch x := unwrapParen(e).(type) {
		case *ast.IndexExpr:
			e = x.X
		case *ast.IndexListExpr:
			e = x.X
		default:
			return unwrapParen(e)
		}
	}
}

// callInfo builds metadata for one call expression.
func (c *collector) callInfo(call *ast.CallExpr) callInfo {
	ci := callInfo{
		Line:     c.a.fset.Position(call.Lparen).Line,
		ArgCount: len(call.Args),
		Args:     []objID{},
		Assigned: []objID{},
		Ellipsis: call.Ellipsis.IsValid(),
	}
	ci.InFuncLiteral = len(c.litStack) > 0

	for _, arg := range call.Args {
		ci.Args = appendObjID(ci.Args, c.a.objectID(c.argObject(arg)))
		if _, isLit := unwrapParen(arg).(*ast.FuncLit); isLit {
			ci.FuncLiteralArg = true
		}
	}
	for _, o := range c.assignedObjects(call) {
		ci.Assigned = appendObjID(ci.Assigned, c.a.objectID(o))
	}

	if len(c.litStack) > 0 {
		lit := c.litStack[len(c.litStack)-1]
		ci.FuncLiteralLine = c.a.fset.Position(lit.Pos()).Line
		if outer, ok := c.litCall[lit]; ok {
			idx := c.litSlot[lit]
			ci.OuterCallLine = c.a.fset.Position(outer.Lparen).Line
			ci.OuterCallArgIdx = &idx
		}
	}

	fun := unwrapInstantiation(call.Fun)
	switch fn := fun.(type) {
	case *ast.Ident:
		c.bindIdent(&ci, fn)
	case *ast.SelectorExpr:
		c.bindSelector(&ci, fn)
	default:
		ci.Kind = kindUnknown
		ci.Target = nil
	}
	return ci
}

// appendObjID appends a resolved identity, skipping unresolved entries.
func appendObjID(list []objID, id objID) []objID {
	if id.File == "" && id.Pos == 0 {
		return list
	}
	return append(list, id)
}

// argObject returns the object an argument expression refers to when the
// argument is a plain identifier bound to a variable or constant.
func (c *collector) argObject(arg ast.Expr) types.Object {
	id, ok := unwrapParen(arg).(*ast.Ident)
	if !ok {
		return nil
	}
	obj := c.info.Uses[id]
	if obj == nil {
		obj = c.info.Defs[id]
	}
	switch obj.(type) {
	case *types.Var, *types.Const:
		return obj
	}
	return nil
}

// assignedObjects returns the objects that receive the call's results.
func (c *collector) assignedObjects(call *ast.CallExpr) []types.Object {
	var node ast.Node = call
	for {
		p, ok := c.parents[node]
		if !ok {
			return nil
		}
		if par, isParen := p.(*ast.ParenExpr); isParen {
			node = par
			continue
		}
		switch stmt := p.(type) {
		case *ast.AssignStmt:
			return c.assignTargets(stmt, node)
		case *ast.ValueSpec:
			return c.valueSpecTargets(stmt, node)
		}
		return nil
	}
}

// assignTargets resolves the objects assigned by an assignment statement.
//
// A call whose result list is a single value assigns the identifier named at
// its position: `x := f()`, `a, b := f(), 2` and `a, b := f()` (a tuple) all
// assign the first identifier. Special forms are excluded rather than
// guessed: the comma-ok form (v, ok := m[k]) and a swap (x, y = y, x) leave
// Assigned empty.
func (c *collector) assignTargets(stmt *ast.AssignStmt, node ast.Node) []types.Object {
	idx := exprIndex(stmt.Rhs, node)
	if idx < 0 {
		return nil
	}
	define := stmt.Tok == token.DEFINE
	if len(stmt.Lhs) == 1 {
		if len(stmt.Rhs) != 1 {
			return nil
		}
		// a, b := f(): one identifier receives the call's tuple.
		return c.lhsObjectAt(stmt.Lhs, 0, define)
	}
	if len(stmt.Rhs) == 1 {
		if tuple, ok := c.info.TypeOf(stmt.Rhs[0]).(*types.Tuple); ok && tuple.Len() == len(stmt.Lhs) {
			objects := make([]types.Object, len(stmt.Lhs))
			for i, lhs := range stmt.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name != "_" {
					objects[i] = c.identObject(id, define)
				}
			}
			return objects
		}
	}
	if len(stmt.Lhs) != len(stmt.Rhs) {
		return nil
	}
	for i := range stmt.Lhs {
		id, ok := stmt.Lhs[i].(*ast.Ident)
		if !ok {
			// Index, field or dereference targets are not bound here.
			return nil
		}
		if i == idx || id.Name == "_" {
			continue
		}
		// Equal arity means every right-hand expression is single-valued, so
		// an identifier naming a left-hand not written after the comma is a
		// reason to stop rather than guess.
		if other, isIdent := unwrapParen(stmt.Rhs[i]).(*ast.Ident); isIdent && definesName(stmt.Lhs, other.Name) {
			return nil
		}
	}
	return c.lhsObjectAt(stmt.Lhs, idx, define)
}

// definesName reports whether lhs contains an identifier with that name.
func definesName(lhs []ast.Expr, name string) bool {
	for _, e := range lhs {
		if id, ok := e.(*ast.Ident); ok && id.Name == name {
			return true
		}
	}
	return false
}

// valueSpecTargets resolves the objects declared by a value specification.
func (c *collector) valueSpecTargets(spec *ast.ValueSpec, node ast.Node) []types.Object {
	idx := exprIndex(spec.Values, node)
	if idx < 0 {
		return nil
	}
	if len(spec.Names) == 1 {
		// var a, b = f() distributes one call's results over two names.
		return nil
	}
	if len(spec.Names) != len(spec.Values) {
		return nil
	}
	return c.defObjectAt(spec.Names, idx)
}

// lhsObjectAt resolves the identifier object at lhs[idx].
func (c *collector) lhsObjectAt(lhs []ast.Expr, idx int, define bool) []types.Object {
	if idx < 0 || idx >= len(lhs) {
		return nil
	}
	id, ok := lhs[idx].(*ast.Ident)
	if !ok || id.Name == "_" {
		return nil
	}
	if o := c.identObject(id, define); o != nil {
		return []types.Object{o}
	}
	return nil
}

// defObjectAt resolves the defined object at names[idx].
func (c *collector) defObjectAt(names []*ast.Ident, idx int) []types.Object {
	if idx < 0 || idx >= len(names) {
		return nil
	}
	if o := c.info.Defs[names[idx]]; o != nil {
		return []types.Object{o}
	}
	return nil
}

// identObject resolves an assignment target identifier.
func (c *collector) identObject(id *ast.Ident, define bool) types.Object {
	if define {
		if o := c.info.Defs[id]; o != nil {
			return o
		}
	}
	if o := c.info.Uses[id]; o != nil {
		return o
	}
	return c.info.Defs[id]
}

// exprIndex returns the index of node among exprs, unwrapping parentheses.
func exprIndex(exprs []ast.Expr, node ast.Node) int {
	for i, e := range exprs {
		if ast.Node(unwrapParen(e)) == node {
			return i
		}
	}
	return -1
}

// bindIdent binds f in f(...).
func (c *collector) bindIdent(ci *callInfo, id *ast.Ident) {
	obj := c.info.Uses[id]
	if obj == nil {
		obj = c.info.Defs[id]
	}
	switch o := obj.(type) {
	case *types.Func:
		if isMethodFunc(o) {
			// Method value or method expression: not a free call binding.
			ci.Kind = kindMethod
			ci.Target = c.a.targetOf(o, true, "")
			return
		}
		ci.Kind = kindFree
		ci.Target = c.a.targetOf(o, false, "")
	case *types.Builtin:
		ci.Kind = kindBuiltin
		ci.Target = nil
		ci.ImportPath = "builtin"
	default:
		// Unresolved, or a variable/callback: never guess a target.
		ci.Kind = kindUnknown
		ci.Target = nil
	}
}

// isMethodFunc reports whether fn declares a receiver.
func isMethodFunc(fn *types.Func) bool {
	sig, ok := fn.Type().(*types.Signature)
	return ok && sig.Recv() != nil
}

// bindSelector binds x.f(...) using package names and recorded selections.
func (c *collector) bindSelector(ci *callInfo, sel *ast.SelectorExpr) {
	base := sel.X
	if inst, ok := unwrapParen(base).(*ast.IndexExpr); ok {
		// Method on an instantiated generic type: v.M[int](...).
		base = unwrapParen(inst.X)
	}
	if pn, ok := c.pkgName(base); ok {
		ci.ImportPath = pn.Imported().Path()
		if obj, ok := c.info.Uses[sel.Sel].(*types.Func); ok {
			if isMethodFunc(obj) {
				ci.Kind = kindMethod
				ci.Target = c.a.targetOf(obj, true, "")
				return
			}
			ci.Kind = kindFree
			ci.Target = c.a.targetOf(obj, false, "")
			return
		}
		// The import path is bound, but this symbol is not a function, so no
		// target is asserted.
		ci.Kind = kindPkgUnres
		ci.Target = nil
		return
	}

	if s, ok := c.info.Selections[sel]; ok {
		if fn, ok := s.Obj().(*types.Func); ok && isMethodFunc(fn) {
			// The selected receiver is the type used at the call site,
			// including promoted methods.
			ci.Kind = kindMethod
			ci.Target = c.a.targetOf(fn, true, selectionReceiver(s))
			return
		}
	}
	// Field selection, shadowed receiver or unresolved selector.
	ci.Kind = kindUnknown
	ci.Target = nil
}

// selectionReceiver renders the receiver type named by a selection.
func selectionReceiver(s *types.Selection) string {
	if name, ok := typeName(s.Recv()); ok {
		return name
	}
	return ""
}

// pkgName resolves e to an imported package name object.
func (c *collector) pkgName(e ast.Expr) (*types.PkgName, bool) {
	id, ok := e.(*ast.Ident)
	if !ok {
		return nil, false
	}
	obj := c.info.Uses[id]
	if obj == nil {
		obj = c.info.Defs[id]
	}
	pn, ok := obj.(*types.PkgName)
	return pn, ok
}
