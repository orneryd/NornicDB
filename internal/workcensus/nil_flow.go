package workcensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"fmt"
	"go/token"
	"go/types"
	"sort"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// MayBeNil reports whether value may be nil where at uses it, with a witness
// path (nil when it cannot). It follows SSA def/use within functions,
// parameters to their visible call sites, closure free variables to their
// bindings, phi arms, the returns of static callees, local cells, and struct
// fields to every store and to every allocation that leaves the field zero. A
// value used only after the non-nil edge of its own nil test is non-nil there.
// Results of external or context-package code, and of calls with no visible
// target, are non-nil (Go's non-nil interface convention); a nil visible behind
// a static, interface or function-value call is never assumed away, except on
// an error return when the use is reached only after that error tested nil,
// a lazily initialized field or cell, and a Recover block no deferred call can
// enter.
func (p *Program) MayBeNil(value ssa.Value, at ssa.Instruction) []string {
	witness, _ := p.MayBeNilWhy(value, at)
	return witness
}

// MayBeNilWhy is MayBeNil with the reasons for a non-nil verdict: every proof
// rule (a nil test, a checked error, a non-nilable type, a cycle) and every
// model convention (an external or unresolved result, a parameter with no
// visible caller, an unmodelled value) that removed a path to nil. A non-nil
// verdict with no reason is one the model reached without deciding anything.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func (p *Program) MayBeNilWhy(value ssa.Value, at ssa.Instruction) (witness, why []string) {
	if p.nilFlow == nil {
		p.nilFlow = newNilFlow(p)
	}
	f := p.nilFlow
	f.trail = f.trail[:0]
	witness = f.use(value, at)
	if witness == nil {
		why = dedupe(f.trail)
	}
	return witness, why
}

// discharge records one reason a path to nil was removed.
func (f *nilFlow) discharge(rule string) { f.trail = append(f.trail, rule) }

func dedupe(rules []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, rule := range rules {
		if !seen[rule] {
			seen[rule] = true
			out = append(out, rule)
		}
	}
	sort.Strings(out)
	return out
}

func newNilFlow(p *Program) *nilFlow {
	return &nilFlow{p: p, values: map[ssa.Value][]string{}, fields: map[*types.Var][]string{}, active: map[any]bool{}, recovers: map[*ssa.Function]bool{}, why: map[ssa.Value][]string{}, fieldWhy: map[*types.Var][]string{}, external: map[string]string{}, bodies: map[string]map[string]*ssa.Function{}}
}

type nilFlow struct {
	p        *Program
	values   map[ssa.Value][]string
	fields   map[*types.Var][]string
	active   map[any]bool
	recovers map[*ssa.Function]bool
	why      map[ssa.Value][]string              // the reasons behind each cached non-nil verdict
	fieldWhy map[*types.Var][]string             // the same for field verdicts
	trail    []string                            // reasons recorded during the current query
	external map[string]string                   // deferred callee -> verdict from its package's source
	bodies   map[string]map[string]*ssa.Function // package path -> its functions, loaded on demand
}

// use applies the guard at this use, then the value's own may-nil result.
func (f *nilFlow) use(v ssa.Value, at ssa.Instruction) []string {
	if v == nil {
		return nil
	}
	if at != nil && NonNilAt(v, at.Block()) {
		f.discharge("proof: nil-test guard")
		return nil
	}
	// Go's (value, error) convention: a result used only after its sibling
	// error tested nil comes from the callee's success returns.
	if x, ok := v.(*ssa.Extract); ok && at != nil {
		if call, ok := x.Tuple.(*ssa.Call); ok && errorChecked(call, at.Block()) {
			return f.resultOf(call, x.Index, true)
		}
	}
	return f.value(v)
}

// callSuccess returns the block that runs only where call's trailing error
// result is nil, from the engine's one call-success analysis
// (SuccessfulCallEdge, which also follows the error through a cell it is
// stored in, only while that cell still holds it), or nil.
func callSuccess(call *ssa.Call) *ssa.BasicBlock {
	tuple, ok := call.Type().(*types.Tuple)
	if !ok || tuple.Len() < 2 || !types.Identical(tuple.At(tuple.Len()-1).Type(), types.Universe.Lookup("error").Type()) {
		return nil
	}
	_, success := SuccessfulCallEdge(call, tuple.Len()-1)
	return success
}

// errorChecked reports that block runs only where call's trailing error result
// is nil.
func errorChecked(call *ssa.Call, block *ssa.BasicBlock) bool {
	success := callSuccess(call)
	return success != nil && success.Dominates(block)
}

// sameLocation reports two addresses of one variable: the same cell, or the
// same field of the same base object (cellObject).
func sameLocation(a, b ssa.Value) bool {
	fa, okA := a.(*ssa.FieldAddr)
	fb, okB := b.(*ssa.FieldAddr)
	if okA || okB {
		return okA && okB && cellObject(fa.X) == cellObject(fb.X) && fa.Field == fb.Field
	}
	return a == b
}

// cellObject names the object a value refers to. A load of an immutable cell
// (one store that dominates the load, ImmutableCellValueAt) names the stored
// value: a variable captured by a closure lives in such a cell, and each use
// loads it again, but every load reads the same object.
func cellObject(v ssa.Value) ssa.Value {
	for depth := 0; depth < 8; depth++ {
		load, ok := v.(*ssa.UnOp)
		if !ok || load.Op != token.MUL {
			return v
		}
		stored, ok := ImmutableCellValueAt(load.X, load)
		if !ok {
			return v
		}
		v = stored
	}
	return v
}

// recoverReachable reports whether fn's Recover block can run: some deferred
// call may recover a panic. It is unreachable only when every deferred call has
// a static callee (a function, closure or method) and none of those callees can
// reach the builtin recover through the call graph. A deferred interface call,
// a function value without a static callee, or a call in that walk with no
// visible target counts as possibly recovering. A deferred callee from a
// package loaded without bodies is judged by externalMayRecover.
func (f *nilFlow) recoverReachable(fn *ssa.Function) bool {
	if fn.Recover == nil {
		return false
	}
	if live, done := f.recovers[fn]; done {
		return live
	}
	live := false
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			deferred, ok := ins.(*ssa.Defer)
			if !ok {
				continue
			}
			callee := deferred.Common().StaticCallee()
			if callee == nil {
				live = true
				continue
			}
			if len(callee.Blocks) == 0 {
				if f.externalMayRecover(callee) {
					live = true
				}
				continue
			}
			if f.mayCallRecover(callee, map[*ssa.Function]bool{}) {
				live = true
			}
		}
	}
	f.recovers[fn] = live
	return live
}

// externalMayRecover judges a deferred callee whose package was loaded without
// bodies (a dependency such as sync): it loads that package's source on its own,
// with the program's build settings, and reports whether the callee's body calls
// the builtin recover. Go stops a panic only in a recover called directly by the
// deferred function, so that body is the one that matters. A callee that still
// has no body (assembly), or that cannot be loaded or found, may recover. Each
// verdict is recorded in ExternalDeferred.
func (f *nilFlow) externalMayRecover(callee *ssa.Function) bool {
	key := callee.String()
	if verdict, done := f.external[key]; done {
		return verdict != "no recover call in its body"
	}
	verdict := "no loadable body"
	if body := f.externalBody(callee); body != nil {
		verdict = "no recover call in its body"
		for _, block := range body.Blocks {
			for _, ins := range block.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					if builtin, ok := call.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "recover" {
						verdict = "calls recover"
					}
				}
			}
		}
	}
	f.external[key] = verdict
	return verdict != "no recover call in its body"
}

// externalBody returns the built body of callee from a separate source load of
// its package, or nil.
func (f *nilFlow) externalBody(callee *ssa.Function) *ssa.Function {
	obj := callee.Object()
	if obj == nil || obj.Pkg() == nil {
		return nil
	}
	path := obj.Pkg().Path()
	functions, loaded := f.bodies[path]
	if !loaded {
		functions = map[string]*ssa.Function{}
		f.bodies[path] = functions
		cfg := f.p.load
		cfg.Mode = packages.LoadSyntax
		cfg.Fset = token.NewFileSet()
		pkgs, err := packages.Load(&cfg, path)
		if err != nil || len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
			return nil
		}
		prog, _ := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
		prog.Build()
		for fn := range sourceFunctions(prog, pkgs) {
			if fn.Pkg != nil && fn.Pkg.Pkg.Path() == path {
				functions[fn.String()] = fn
			}
		}
	}
	if body := functions[callee.String()]; body != nil && len(body.Blocks) > 0 {
		return body
	}
	return nil
}

// ExternalDeferred lists each deferred callee judged from a separate source load
// of its package, with its verdict.
func (p *Program) ExternalDeferred() map[string]string {
	if p.nilFlow == nil {
		return nil
	}
	return p.nilFlow.external
}

func (f *nilFlow) mayCallRecover(fn *ssa.Function, seen map[*ssa.Function]bool) bool {
	if seen[fn] {
		return false
	}
	seen[fn] = true
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			if builtin, ok := call.Common().Value.(*ssa.Builtin); ok {
				if builtin.Name() == "recover" {
					return true
				}
				continue
			}
			targets := f.p.CallTargets(call)
			if len(targets) == 0 {
				return true // an unresolved call may recover
			}
			for _, target := range targets {
				if f.mayCallRecover(target, seen) {
					return true
				}
			}
		}
	}
	return false
}

// nonNilEdge reports that arm reaches succ from pred only along the non-nil
// edge of arm's own nil test, as in "if ctx == nil { ctx = ... }".
func nonNilEdge(arm ssa.Value, pred, succ *ssa.BasicBlock) bool {
	if arm.Referrers() == nil {
		return false
	}
	iff, nilSucc := NilSuccessEdge(arm)
	return iff != nil && iff.Block() == pred && succ != nilSucc
}

func (f *nilFlow) position(fn *ssa.Function, pos token.Pos) string {
	if fn == nil || !pos.IsValid() {
		return ""
	}
	at := f.p.FileSet.PositionFor(pos, false)
	return fmt.Sprintf("%s:%d", at.Filename, at.Line)
}

func (f *nilFlow) value(v ssa.Value) (witness []string) {
	if w, done := f.values[v]; done {
		if w == nil {
			f.trail = append(f.trail, f.why[v]...)
		}
		return w
	}
	if f.active[v] {
		f.discharge("proof: cycle adds no new source")
		return nil // a cycle adds no new source of nil
	}
	f.active[v] = true
	start := len(f.trail)
	defer func() {
		delete(f.active, v)
		f.values[v] = witness
		if witness == nil {
			f.why[v] = dedupe(f.trail[start:])
		}
	}()
	switch v := v.(type) {
	case *ssa.Const:
		if v.IsNil() {
			if at := f.position(v.Parent(), v.Pos()); at != "" {
				return []string{"nil " + v.Type().String() + " at " + at}
			}
			return []string{"nil " + v.Type().String()}
		}
		return nil // a non-nil constant
	case *ssa.Parameter:
		return f.parameter(v)
	case *ssa.FreeVar:
		return f.freeVar(v)
	case *ssa.Phi:
		for i, arm := range v.Edges {
			pred := v.Block().Preds[i]
			if nonNilEdge(arm, pred, v.Block()) {
				f.discharge("proof: nil-test edge")
				continue
			}
			if w := f.use(arm, pred.Instrs[len(pred.Instrs)-1]); w != nil {
				return w
			}
		}
	case *ssa.Call:
		return f.result(v, -1)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return f.result(call, v.Index)
		}
		if lookup, ok := v.Tuple.(*ssa.Lookup); ok && v.Index == 0 {
			return []string{"absent map value at " + f.position(lookup.Parent(), lookup.Pos())}
		}
	case *ssa.Lookup:
		return []string{"absent map value at " + f.position(v.Parent(), v.Pos())}
	case *ssa.ChangeInterface:
		return f.value(v.X)
	case *ssa.ChangeType:
		return f.value(v.X)
	case *ssa.MakeInterface:
		if structOf(v.X.Type()) != nil && !isPointer(v.X.Type()) {
			return f.embedded(v.X)
		}
		if !nilable(v.X.Type()) {
			f.discharge("proof: non-nilable value")
			return nil // an interface holding a non-nilable value is never nil
		}
		return f.value(v.X)
	case *ssa.Field:
		return f.field(structField(v.X.Type(), v.Field))
	case *ssa.UnOp:
		if v.Op == token.MUL {
			return f.load(v)
		}
		f.discharge("convention: unmodelled value " + v.Op.String())
		return nil
	case *ssa.Alloc, *ssa.MakeClosure, *ssa.MakeMap, *ssa.MakeChan, *ssa.MakeSlice, *ssa.Function, *ssa.Global, *ssa.FieldAddr, *ssa.IndexAddr:
		// Structurally non-nil (an address or a made value); recorded, so
		// every non-nil verdict carries its reason.
		f.discharge(fmt.Sprintf("proof: structurally non-nil %T", v))
		return nil
	}
	f.discharge(fmt.Sprintf("convention: unmodelled value %T", v))
	return nil
}

func (f *nilFlow) parameter(param *ssa.Parameter) []string {
	fn := param.Parent()
	index := -1
	for i, candidate := range fn.Params {
		if candidate == param {
			index = i
		}
	}
	sites := f.p.SourceCallSites(fn)
	if len(sites) == 0 {
		f.discharge("convention: parameter without visible call sites: " + fn.String())
	}
	for _, site := range sites {
		// An interface or bound-method call passes no receiver argument: align
		// by the difference, and skip a site whose argument is not this
		// parameter's type (a call attributed through a compiler wrapper).
		common := site.Common()
		i := index - (len(fn.Params) - len(common.Args))
		if i < 0 || i >= len(common.Args) || !types.AssignableTo(common.Args[i].Type(), param.Type()) {
			continue
		}
		if w := f.use(common.Args[i], site); w != nil {
			return append(w, fmt.Sprintf("-> parameter %s of %s (call at %s)", param.Name(), fn.String(), f.position(site.Parent(), site.Pos())))
		}
	}
	return nil
}

func (f *nilFlow) freeVar(free *ssa.FreeVar) []string {
	binding := freeVarBinding(free)
	if binding == nil {
		f.discharge("convention: unbound free variable of " + free.Parent().String())
		return nil
	}
	if w := f.value(binding); w != nil {
		return append(w, "-> captured "+free.Name()+" of "+free.Parent().String())
	}
	return nil
}

// result follows the returns of every call-graph target of call (the static
// callee, or the targets of an interface or function-value call), so a nil
// produced behind a dynamic call is not assumed away. Context-package and
// bodiless (external) targets produce non-nil results.
func (f *nilFlow) result(call ssa.CallInstruction, index int) []string {
	return f.resultOf(call, index, false)
}

// resultOf follows the returns of call's targets; successOnly skips a return
// whose trailing error result is not the nil constant (an error return).
func (f *nilFlow) resultOf(call ssa.CallInstruction, index int, successOnly bool) []string {
	targets := f.p.CallTargets(call)
	if len(targets) == 0 {
		f.discharge("convention: unresolved call target at " + f.position(call.Parent(), call.Pos()))
	}
	for _, callee := range targets {
		switch {
		case callee.Pkg != nil && callee.Pkg.Pkg.Path() == "context":
			f.discharge("convention: context-package result " + callee.String())
			continue
		case callee.Blocks == nil || callee.Pkg == nil:
			f.discharge("convention: external result " + callee.String())
			continue
		}
		for _, block := range callee.Blocks {
			ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
			if !ok {
				continue
			}
			if block == callee.Recover && !f.recoverReachable(callee) {
				f.discharge("proof: unreachable Recover block of " + callee.String())
				continue
			}
			i := index
			if i < 0 {
				i = 0
			}
			if i >= len(ret.Results) {
				continue
			}
			// A checked caller sees only returns whose error can be nil; a
			// return is dropped only when its error is proven non-nil.
			if successOnly && f.use(ret.Results[len(ret.Results)-1], ret) == nil {
				f.discharge("proof: return with a non-nil error excluded by the caller's check")
				continue
			}
			if w := f.use(ret.Results[i], ret); w != nil {
				return append(w, "-> result of "+callee.String())
			}
		}
	}
	return nil
}

// embedded judges a struct value converted to an interface: the interface is
// never nil, but the methods it promotes forward to its embedded interface
// fields, so each of those is judged by projectField where the value is read.
func (f *nilFlow) embedded(x ssa.Value) []string {
	st := structOf(x.Type())
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		if !field.Anonymous() || !types.IsInterface(field.Type()) {
			continue
		}
		if w := f.projectField(x, field); w != nil {
			return append(w[:len(w):len(w)], "-> promoted through embedded field "+field.Name()+" of "+x.Type().String())
		}
	}
	f.discharge("proof: struct value, every embedded interface judged")
	return nil
}

// projectField judges the value of field inside the struct value x (field is
// x's own field or nested inline in it): zero in a zero constant; through
// reaching definitions when x is loaded from a local composite literal, or
// zero when that literal never sets it; otherwise the field's possible values
// anywhere. Embedded interfaces and whole-value stores share this projection.
func (f *nilFlow) projectField(x ssa.Value, field *types.Var) []string {
	name := "field " + field.Name()
	if _, ok := x.(*ssa.Const); ok {
		return []string{"zero " + name + " of " + x.Type().String()}
	}
	index := fieldIndex(x.Type(), field)
	if load, ok := x.(*ssa.UnOp); ok && load.Op == token.MUL && index >= 0 {
		if cell, ok := load.X.(*ssa.Alloc); ok && cell.Referrers() != nil {
			var addr *ssa.FieldAddr
			for _, ref := range *cell.Referrers() {
				if fa, ok := ref.(*ssa.FieldAddr); ok && fa.X == cell && fa.Field == index {
					addr = fa
				}
			}
			if addr == nil {
				return []string{"zero " + name + " of " + x.Type().String()}
			}
			return f.definedRead(load, addr, name)
		}
	}
	return f.field(field)
}

// nilable reports a type whose values can be nil.
func nilable(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Kind() == types.UnsafePointer || u.Kind() == types.UntypedNil
	case *types.Struct, *types.Array:
		return false
	}
	return true
}

func isPointer(t types.Type) bool {
	_, ok := t.Underlying().(*types.Pointer)
	return ok
}

func (f *nilFlow) load(load *ssa.UnOp) []string {
	switch addr := load.X.(type) {
	case *ssa.FieldAddr:
		return f.definedRead(load, addr, "field "+structField(addr.X.Type(), addr.Field).Name())
	case *ssa.Alloc:
		if stored, ok := ImmutableCellValueAt(addr, load); ok {
			return f.use(stored, load)
		}
		if localCell(addr) {
			return f.definedRead(load, addr, "local "+addr.Comment)
		}
		return f.stores(addr, "local "+addr.Comment)
	case *ssa.Global:
		return f.stores(addr, "global "+addr.Name())
	case *ssa.FreeVar:
		// A captured variable: the closure loads its enclosing function's cell.
		if cell, ok := freeVarBinding(addr).(*ssa.Alloc); ok {
			return f.stores(cell, "captured "+addr.Name())
		}
	}
	f.discharge(fmt.Sprintf("convention: unmodelled load through %T", load.X))
	return nil
}

func freeVarBinding(free *ssa.FreeVar) ssa.Value {
	if closure, index := freeVarClosure(free); closure != nil {
		return closure.Bindings[index]
	}
	return nil
}

// freeVarClosure returns the enclosing function's MakeClosure that binds a
// closure's free variable, with the binding's index, or nil: the one
// closure-binding resolution (freeVarBinding and DerivedOnlyFrom use it).
// FreeVarClosure is freeVarClosure for the census policy packages.
func FreeVarClosure(free *ssa.FreeVar) (*ssa.MakeClosure, int) { return freeVarClosure(free) }

func freeVarClosure(free *ssa.FreeVar) (*ssa.MakeClosure, int) {
	fn := free.Parent()
	for i, candidate := range fn.FreeVars {
		if candidate != free || fn.Parent() == nil {
			continue
		}
		for _, block := range fn.Parent().Blocks {
			for _, ins := range block.Instrs {
				if closure, ok := ins.(*ssa.MakeClosure); ok && closure.Fn == fn && i < len(closure.Bindings) {
					return closure, i
				}
			}
		}
	}
	return nil, 0
}

// localCell reports a cell used only by direct loads and stores, in its own
// function and in every closure that captures it (at any depth): its address
// escapes to no call or other value anywhere, so its loads and stores are all
// its reads and writes.
func localCell(cell *ssa.Alloc) bool {
	return localCellUses(cell, map[ssa.Value]bool{})
}

// LocalCell is localCell for the census policy packages.
func LocalCell(cell *ssa.Alloc) bool { return localCell(cell) }

func localCellUses(cell ssa.Value, seen map[ssa.Value]bool) bool {
	if seen[cell] {
		return true
	}
	seen[cell] = true
	if cell.Referrers() == nil {
		return false
	}
	for _, ref := range *cell.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Store:
			if ref.Addr != cell || ref.Val == cell {
				return false
			}
		case *ssa.UnOp:
			if ref.Op != token.MUL || ref.X != cell {
				return false
			}
		case *ssa.MakeClosure:
			fn, ok := ref.Fn.(*ssa.Function)
			if !ok {
				return false
			}
			for i, binding := range ref.Bindings {
				if binding == cell && (i >= len(fn.FreeVars) || !localCellUses(fn.FreeVars[i], seen)) {
					return false
				}
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// CapturedStores is capturedStores for the census policy packages.
func CapturedStores(cell ssa.Value) []*ssa.Store { return capturedStores(cell) }

// capturedStores are the stores a capturing closure (or one nested in it)
// makes to cell; they may run at any time relative to the enclosing function.
func capturedStores(cell ssa.Value) []*ssa.Store {
	var out []*ssa.Store
	if cell.Referrers() == nil {
		return nil
	}
	for _, ref := range *cell.Referrers() {
		closure, ok := ref.(*ssa.MakeClosure)
		if !ok {
			continue
		}
		fn := closure.Fn.(*ssa.Function)
		for i, binding := range closure.Bindings {
			if binding != cell || i >= len(fn.FreeVars) || fn.FreeVars[i].Referrers() == nil {
				continue
			}
			free := fn.FreeVars[i]
			for _, use := range *free.Referrers() {
				if store, ok := use.(*ssa.Store); ok && store.Addr == free {
					out = append(out, store)
				}
			}
			out = append(out, capturedStores(free)...)
		}
	}
	return out
}

// definedRead judges a load of a struct field or a local cell by the
// definitions that reach it within its function: on every path backwards from
// the load, the nearest store to the location, a write that may reach it
// through another reference, the branch of a nil test of the same location
// (whose result is then known), or the location's start. A later store kills
// an earlier one, so a value initialized and then overwritten is judged by the
// overwrite. A field's start or unknown write stands for its possible values
// anywhere (every store and zero allocation); a cell's start for its zero
// value. Stores by capturing closures may run at any time and always count.
func (f *nilFlow) definedRead(load *ssa.UnOp, addr ssa.Value, name string) []string {
	var field *types.Var
	if fa, ok := addr.(*ssa.FieldAddr); ok {
		field = structField(fa.X.Type(), fa.Field)
	}
	everywhere := func() []string {
		if field != nil {
			return f.field(field)
		}
		return []string{"zero " + name}
	}
	if cell, ok := addr.(*ssa.Alloc); ok {
		for _, store := range capturedStores(cell) {
			if w := f.use(store.Val, store); w != nil {
				return append(w[:len(w):len(w)], "-> stored in "+name+" by a closure")
			}
		}
	}
	for _, d := range reachingDefinitions(load, addr, field) {
		var w []string
		switch {
		case d.store != nil:
			if w = f.storedValue(d.store, load); w != nil {
				w = append(w[:len(w):len(w)], "-> stored in "+name)
			}
		case d.unknown:
			if w = everywhere(); w != nil {
				w = append(w[:len(w):len(w)], "-> after a write that may reach "+name)
			}
		case d.nilBranch:
			w = []string{"nil on the nil branch of a test of " + name}
		case d.nonNilBranch:
			f.discharge("proof: non-nil branch of a test of the location")
		default: // the location's start: function entry or its creation
			w = everywhere()
		}
		if w != nil {
			return w
		}
	}
	return nil
}

// storedValue judges a stored value at the load that reads it; a stored call
// result whose error is tested nil before the load (on the load's block, or on
// every path from the store to the load) comes from the callee's success
// returns.
func (f *nilFlow) storedValue(store *ssa.Store, load *ssa.UnOp) []string {
	if x, ok := store.Val.(*ssa.Extract); ok {
		if call, ok := x.Tuple.(*ssa.Call); ok && (errorChecked(call, load.Block()) || errorCheckedBetween(call, store, load)) {
			return f.resultOf(call, x.Index, true)
		}
	}
	return f.use(store.Val, store)
}

type definition struct {
	store                   *ssa.Store // a store to this very location
	unknown                 bool       // a write that may change it to any of its possible values
	nilBranch, nonNilBranch bool       // the known result of a nil test of it
	tested                  *ssa.UnOp  // the tested load, for a nil-test result
	// none of these: the location's start (function entry, or where its
	// cell or base object is created)
}

// locationBase returns the value naming addr's object: the cell itself, or the
// base of a field address. The walk stops at its definition, so every match
// "same location" found before it concerns the same object (a loop iteration
// that redefines the base or reallocates the cell is never crossed).
func locationBase(addr ssa.Value) ssa.Value {
	if fa, ok := addr.(*ssa.FieldAddr); ok {
		return cellObject(fa.X)
	}
	return addr
}

// writesLocation reports an instruction that defines addr's location: the
// creation of its cell or base object, a store to the same location, or, for a
// field, a write that may reach it through another reference (a store to the
// same field of another base, a store of a whole struct that contains the
// field, or a non-builtin call, which may do either). Concurrent writes are out
// of scope: a read racing a write is a data race, and the census assumes the
// race-free program the -race test runs check.
func writesLocation(ins ssa.Instruction, addr ssa.Value, field *types.Var) (definition, bool) {
	if v, ok := ins.(ssa.Value); ok && v == locationBase(addr) {
		return definition{}, true
	}
	switch ins := ins.(type) {
	case *ssa.Store:
		if sameLocation(ins.Addr, addr) {
			return definition{store: ins}, true
		}
		if field == nil {
			return definition{}, false
		}
		if fa, ok := ins.Addr.(*ssa.FieldAddr); ok && structField(fa.X.Type(), fa.Field) == field {
			return definition{unknown: true}, true
		}
		if ptr, ok := ins.Addr.Type().Underlying().(*types.Pointer); ok && containsField(ptr.Elem(), field, 0) {
			return definition{unknown: true}, true
		}
	case *ssa.Call:
		if _, builtin := ins.Call.Value.(*ssa.Builtin); field != nil && !builtin {
			return definition{unknown: true}, true
		}
	case *ssa.Go, *ssa.RunDefers:
		if field != nil {
			return definition{unknown: true}, true
		}
	}
	return definition{}, false
}

// containsField reports that a value of type t holds field inline (not behind
// a pointer), at any depth.
func containsField(t types.Type, field *types.Var, depth int) bool {
	s := structOf(t)
	if s == nil || depth > 8 {
		return false
	}
	if _, isPtr := t.Underlying().(*types.Pointer); isPtr {
		return false
	}
	for i := 0; i < s.NumFields(); i++ {
		if s.Field(i) == field || containsField(s.Field(i).Type(), field, depth+1) {
			return true
		}
	}
	return false
}

// testedEdge reports the known result on the edge pred->succ when pred ends in
// a nil test of a load of the same location (same cell, or same field of the
// same base) and nothing writes the location between that load and the test.
func testedEdge(pred, succ *ssa.BasicBlock, addr ssa.Value, field *types.Var) (definition, bool) {
	iff, ok := pred.Instrs[len(pred.Instrs)-1].(*ssa.If)
	if !ok {
		return definition{}, false
	}
	tested, nilIndex, ok := NilComparison(iff.Cond)
	if !ok {
		return definition{}, false
	}
	load, ok := tested.(*ssa.UnOp)
	if !ok || load.Op != token.MUL || load.Block() != pred || !sameLocation(load.X, addr) {
		return definition{}, false
	}
	after := false
	for _, ins := range pred.Instrs {
		if ins == load {
			after = true
			continue
		}
		if _, writes := writesLocation(ins, addr, field); after && writes {
			return definition{}, false
		}
	}
	nilSucc := pred.Succs[nilIndex]
	if pred.Succs[0] == pred.Succs[1] {
		return definition{}, false
	}
	if succ == nilSucc {
		return definition{nilBranch: true, tested: load}, true
	}
	return definition{nonNilBranch: true, tested: load}, true
}

// reachingDefinitions walks backwards from load and returns, for every path,
// the nearest definition of its location.
func reachingDefinitions(load *ssa.UnOp, addr ssa.Value, field *types.Var) []definition {
	var out []definition
	scanned := map[*ssa.BasicBlock]bool{}
	var scan func(block *ssa.BasicBlock, end int)
	scan = func(block *ssa.BasicBlock, end int) {
		for i := end - 1; i >= 0; i-- {
			if d, ok := writesLocation(block.Instrs[i], addr, field); ok {
				out = append(out, d)
				return
			}
		}
		if len(block.Preds) == 0 {
			out = append(out, definition{})
			return
		}
		for _, pred := range block.Preds {
			if d, ok := testedEdge(pred, block, addr, field); ok {
				out = append(out, d)
				continue
			}
			if !scanned[pred] {
				scanned[pred] = true
				scan(pred, len(pred.Instrs))
			}
		}
	}
	block := load.Block()
	for i, ins := range block.Instrs {
		if ins == load {
			scan(block, i)
			break
		}
	}
	return out
}

// errorCheckedBetween reports that every path from store to load passes the
// success block of call (callSuccess). The success block must be entered only
// by that edge.
func errorCheckedBetween(call *ssa.Call, store *ssa.Store, load *ssa.UnOp) bool {
	success := callSuccess(call)
	if success == nil || len(success.Preds) != 1 || success == store.Block() {
		return false
	}
	return !reachesAvoiding(store, load, success)
}

// reachesAvoiding reports a control-flow path from store to load that never
// enters the block avoid.
func reachesAvoiding(store, load ssa.Instruction, avoid *ssa.BasicBlock) bool {
	from, to := store.Block(), load.Block()
	if from == to {
		for _, ins := range from.Instrs {
			if ins == store {
				return true
			}
			if ins == load {
				break
			}
		}
	}
	seen := map[*ssa.BasicBlock]bool{}
	queue := append([]*ssa.BasicBlock(nil), from.Succs...)
	for len(queue) > 0 {
		block := queue[0]
		queue = queue[1:]
		if block == avoid || seen[block] {
			continue
		}
		if block == to {
			return true
		}
		seen[block] = true
		queue = append(queue, block.Succs...)
	}
	return false
}

// stores reports a nil reaching any store to cell, by its own function or a
// capturing closure, or its zero value when no store exists. A local cell
// whose address escapes (to a call, or stored as a value) can be written by
// code the analysis does not follow, so any value may reach it.
func (f *nilFlow) stores(cell ssa.Value, name string) []string {
	if cell.Referrers() == nil {
		return nil
	}
	if alloc, ok := cell.(*ssa.Alloc); ok && !localCell(alloc) {
		return []string{"the address of " + name + " escapes to code that may write it"}
	}
	stored := false
	all := capturedStores(cell)
	for _, ref := range *cell.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == cell {
			all = append(all, store)
		}
	}
	for _, store := range all {
		stored = true
		if w := f.use(store.Val, store); w != nil {
			return append(w, "-> stored in "+name)
		}
	}
	if !stored {
		return []string{"zero " + name}
	}
	return nil
}

func (f *nilFlow) field(field *types.Var) (witness []string) {
	if field == nil {
		return nil
	}
	if w, done := f.fields[field]; done {
		if w == nil {
			f.trail = append(f.trail, f.fieldWhy[field]...)
		}
		return w
	}
	if f.active[field] {
		f.discharge("proof: cycle adds no new source")
		return nil
	}
	f.active[field] = true
	start := len(f.trail)
	defer func() {
		delete(f.active, field)
		f.fields[field] = witness
		if witness == nil {
			f.fieldWhy[field] = dedupe(f.trail[start:])
		}
	}()
	for _, fn := range f.p.Functions {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				switch ins := ins.(type) {
				case *ssa.Store:
					if addr, ok := ins.Addr.(*ssa.FieldAddr); ok && structField(addr.X.Type(), addr.Field) == field {
						if w := f.use(ins.Val, ins); w != nil {
							return append(w, "-> stored in field "+field.Name())
						}
					} else if containsField(ins.Val.Type(), field, 0) {
						// A whole value stored over a struct sets this field
						// to the stored value's own field.
						if w := f.projectField(ins.Val, field); w != nil {
							return append(w[:len(w):len(w)], "-> stored whole at "+f.position(fn, ins.Pos()))
						}
					}
				case *ssa.Alloc:
					if index := fieldIndex(ins.Type(), field); index >= 0 {
						if !storesField(ins, index) {
							return []string{fmt.Sprintf("zero field %s at %s", field.Name(), f.position(fn, ins.Pos()))}
						}
						f.discharge("proof: the allocating function stores the field")
					}
				}
			}
		}
	}
	return nil
}

func structOf(t types.Type) *types.Struct {
	if ptr, ok := types.Unalias(t).Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	st, _ := types.Unalias(t).Underlying().(*types.Struct)
	return st
}

func structField(t types.Type, index int) *types.Var {
	if st := structOf(t); st != nil && index < st.NumFields() {
		return st.Field(index)
	}
	return nil
}

func fieldIndex(t types.Type, field *types.Var) int {
	if st := structOf(t); st != nil {
		for i := 0; i < st.NumFields(); i++ {
			if st.Field(i) == field {
				return i
			}
		}
	}
	return -1
}

// storesField reports whether the allocating function stores the field of
// that allocation, as a composite literal or a later assignment does, also
// through loads of an immutable cell holding it (cellObject). A whole value
// stored into the allocation (a struct built elsewhere and assigned)
// initializes every field; that value's fields are judged where it was built.
// The zero value a partial composite literal stores first sets no field.
func storesField(alloc *ssa.Alloc, index int) bool {
	if alloc.Referrers() == nil {
		return false
	}
	bases := []ssa.Value{alloc}
	for _, ref := range *alloc.Referrers() {
		held, ok := ref.(*ssa.Store)
		if !ok || held.Val != alloc || held.Addr.Referrers() == nil {
			continue
		}
		for _, use := range *held.Addr.Referrers() {
			if load, ok := use.(*ssa.UnOp); ok && load.Op == token.MUL && cellObject(load) == ssa.Value(alloc) {
				bases = append(bases, load)
			}
		}
	}
	for _, base := range bases {
		if addressSetsField(base, index) {
			return true
		}
	}
	return false
}

// addressSetsField reports that the function stores field index of the
// struct at addr: a store to that field's address (fieldStoredAt), or a whole
// value other than the zero value stored at addr, whose own fields are judged
// where it was built (a partial composite literal assigned over a variable
// stores the zero value first and then only the fields it names).
func addressSetsField(addr ssa.Value, index int) bool {
	if fieldStoredAt(addr, index) {
		return true
	}
	if addr.Referrers() == nil {
		return false
	}
	for _, ref := range *addr.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == addr {
			if _, zero := store.Val.(*ssa.Const); !zero {
				return true
			}
		}
	}
	return false
}

// fieldStoredAt reports a store to the address of field index of the struct
// at addr.
func fieldStoredAt(addr ssa.Value, index int) bool {
	if addr.Referrers() == nil {
		return false
	}
	for _, ref := range *addr.Referrers() {
		field, ok := ref.(*ssa.FieldAddr)
		if !ok || field.X != addr || field.Field != index || field.Referrers() == nil {
			continue
		}
		for _, use := range *field.Referrers() {
			if store, ok := use.(*ssa.Store); ok && store.Addr == field {
				return true
			}
		}
	}
	return false
}
